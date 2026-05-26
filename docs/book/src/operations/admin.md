# `dyn-admin`: cluster admin CLI

`dyn-admin` is the operator-facing CLI that ships alongside
`dynomited`. It is the Dynomite Rust port's answer to Riak's
`riak-admin` tool: a single binary an operator points at one running
node and asks structured questions of. The subcommand surface is
intentionally narrow in v0; mutating operations land in v1 once the
gossip substrate exposes the necessary admin-only DNODE messages.

## Connecting

Every subcommand reads from a running node over one of two ports:

| Port | Default | Used by |
|---|---|---|
| PBC (Riak Protocol Buffers) | `127.0.0.1:8087` | `ping`, `status`, `ring-status`, `cluster-list` |
| Stats / metrics HTTP | `127.0.0.1:22222` | `stats`, `metrics` (and `status`/`ring-status` augmentation) |

Override the address with `--node <host:port>` (or `--seed` for
`cluster-list`). `status` and `ring-status` accept `--stats-node`
to reach a stats endpoint that lives on a different host or port,
and `--no-stats` to skip the HTTP fetch entirely.

## Subcommands

### `ping`

Sends `RpbPingReq` and times the round-trip:

```sh
$ dyn-admin ping
PONG from 127.0.0.1:8087 (1.234 ms)
$ dyn-admin ping --json | jq .rtt_us
1234
```

Exit code `0` on success. Non-zero exit and an error printed on
stderr otherwise.

### `status`

Reads `RpbGetServerInfoResp` from the PBC port and combines it with
a slice of the `/stats` JSON snapshot:

```sh
$ dyn-admin status
node: 127.0.0.1:8087
server_node: dyn-riak
server_version: dyn-riak 0.0.1
engine_source: node-a
engine_version: 0.0.1
datacenter: dc1
rack: r1
uptime_seconds: 7
pool: dyn_o_mite
```

When the stats endpoint is unreachable, the human formatter notes
`stats: unavailable (...)` and the JSON formatter sets
`stats: null` plus `stats_error: "..."`.

### `ring-status`

Best-effort ring/topology view. The substrate does not yet expose a
multi-peer ring map over PBC, so the v0 output is a single-row
table for the contacted node:

```sh
$ dyn-admin ring-status
Ring status (queried 127.0.0.1:8087)

node                          dc          rack        state   version               token
----------------------------  ----------  ----------  ------  --------------------  --------
node-a                        dc1         r1          up      dyn-riak 0.0.1        <unset>
```

`token` and richer per-vnode state become populated once the
substrate gossips a peer table over PBC. Until then the column is
present so the output shape is forward-compatible.

### `stats`

Pretty-prints the high-value fields from the `/stats` JSON
snapshot. `--json` echoes the raw snapshot byte-for-byte so
downstream `jq` filters keep working:

```sh
$ dyn-admin stats
engine_source: node-a
engine_version: 0.0.1
datacenter: dc1
rack: r1
uptime_seconds: 7
pool: dyn_o_mite
latency_p99_us: 1234
latency_max_us: 2345
alloc_msgs: 10
free_msgs: 5

$ dyn-admin stats --json | jq '.dyn_o_mite.client_eof'
0
```

### `metrics`

Forwards the `/metrics` Prometheus text endpoint verbatim:

```sh
$ dyn-admin metrics | head -3
# HELP dynomite_uptime_seconds Uptime in seconds.
# TYPE dynomite_uptime_seconds gauge
dynomite_uptime_seconds 7
```

A trailing newline is appended when the upstream body lacks one,
so the output is always safe to pipe into line-oriented tools.

### `cluster-list`

Reports what the seed knows about itself, plus a note recording
the deferred multi-peer discovery. Once peer-list messages land,
the loop here grows naturally:

```sh
$ dyn-admin cluster-list --seed 127.0.0.1:8087
Cluster (seed 127.0.0.1:8087)

addr                      node                          state   version
------------------------  ----------------------------  ------  --------------------
127.0.0.1:8087            dyn-riak                      up      dyn-riak 0.0.1

note: multi-peer discovery deferred: substrate does not yet expose a peer-list message
      over PBC; reporting only the contacted seed
```

### `bucket-props`

Inspects or updates a bucket's `RpbBucketProps` over PBC. Two
actions:

* `get <bucket>` issues an `RpbGetBucketReq` and pretty-prints the
  reply.
* `set <bucket> [flags]` reads the current properties first, applies
  the named overrides on top, and submits an `RpbSetBucketReq`.
  Fields the operator does not name are sent back unchanged so the
  update is always a partial overlay.

```sh
$ dyn-admin bucket-props get users
Bucket properties for users via 127.0.0.1:8087
  n_val: 3
  keyfun: std
  replication_strategy: successors

$ dyn-admin bucket-props set users --n-val 5 --keyfun bucketonly
Updated bucket properties for users via 127.0.0.1:8087
  n_val: 5
  keyfun: bucketonly
  replication_strategy: successors

$ dyn-admin bucket-props get users --json | jq '.props.n_val'
5
```

Flags accepted by `set`:

| Flag | Values | Meaning |
|---|---|---|
| `--n-val N` | non-negative integer | Replication factor. |
| `--read-consistency C` | `one`, `quorum`, `all`, `default`, integer | Default replica-read quorum (Riak `r`). |
| `--write-consistency C` | same as `--read-consistency` | Default replica-write quorum (Riak `w`). |
| `--keyfun KF` | `std`, `bucketonly` | Pre-hash strategy. |
| `--replication-strategy STRAT` | `topology`, `successors` | Replica-target selection strategy. |

The symbolic quorum names map to Riak's published magic uint32
values so a Riak client and `dyn-admin` agree on the semantics.
`set` rejects an empty flag list with a hard error rather than a
no-op round-trip.

See [`riak.md`](riak.md) for the bucket-property semantics and the
locations in the registry where the values are stored.

## Output formats

* Default: human-readable. One `key: value` pair per line for the
  scalar subcommands; bordered tables for `ring-status` and
  `cluster-list`.
* `--json`: pretty-printed JSON. The schema is stable across patch
  releases of the v0 line; breaking changes ship under a major
  bump and are recorded in `docs/journal/`.

## Exit codes

* `0`: success.
* `1`: any subcommand-level failure (connection refused, timeout,
  server error, malformed response). The reason is written to
  stderr as `dyn-admin: <error>`.
* `2`: bootstrap failure (tokio runtime). Rare.

## Deferred subcommands

The following `riak-admin` mutating commands are listed in
`docs/riak-compat-plan.md` Section 5 but absent in v0:

* `cluster-join`
* `cluster-leave`
* `cluster-plan`
* `cluster-commit`

They are not stubbed; clap rejects them as unknown. The journal
entry `docs/journal/2026-05-24-dyn-admin-v0.md` captures the
sequencing: each one needs an admin-only DNODE message that the
substrate has not yet exposed, plus a confirmation prompt and a
dry-run `cluster-plan` step before any state mutation.
