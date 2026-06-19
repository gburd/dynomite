# Configuration

This page covers configuration knobs that go beyond the basic
pool stanza. Refer to the inline rustdoc on
[`dynomite::conf::ConfPool`] for the exhaustive field list; the
pages here describe the operator-facing surface in more detail.

## Endpoints and Unix sockets

The `listen:` (client plane), `dyn_listen:` (peer plane), and
`stats_listen:` (HTTP stats) directives take a `host:port`
address (IPv4 or IPv6). A value that begins with `/` is treated
as a Unix domain socket path instead:

```yaml
dyn_o_mite:
  listen: /var/run/dynomite/client.sock
  dyn_listen: 127.0.0.1:8101
  stats_listen: 127.0.0.1:22222
```

The `servers:` entries (the backend datastore) accept the same
shape: `host:port:weight [name]` or, for a Unix-socket backend,
`/path/to/socket:weight [name]`. The port is reported as `0` for
Unix-socket entries.

```yaml
  servers:
  - /var/run/valkey/valkey.sock:1 valkey-local
```

## Backend authentication

* `redis_requirepass` (string, default unset): when set, the
  password is sent as `AUTH <pw>` on every backend connection
  immediately after the TCP handshake. It mirrors the Valkey /
  Redis `requirepass` server option. Memcache backends are not
  authenticated (`AUTH` is RESP-specific; memcache binary SASL
  is not implemented), so the knob is ignored for a memcache
  `data_store`.

## Dyniak store

* `noxu_path` (path): filesystem directory the in-process Noxu
  DB environment opens at. **Required** when `data_store:
  dyniak` is selected and ignored otherwise. The directory must
  be writable; an existing environment is reused, otherwise one
  is created. A `dyniak` pool serving the Riak surface needs a
  binary built with `--features riak`.

## Bucket types

A *bucket type* is a named bundle of routing properties that
applies to every key whose on-the-wire form starts with the
bucket prefix. Operators use bucket types to give different key
classes different SLAs from inside a single pool: cache-style
keys can sit on `DC_ONE` while transactional keys ride
`DC_EACH_SAFE_QUORUM`, and the dispatcher swaps in the right
settings on a per-request basis without needing more pools, more
listeners, or client-side routing.

The wire convention is intentionally simple. The bucket name is
the byte sequence before the first `/` in the key; everything
else (including the slash itself) is the user-visible key body.
A key with no `/` has no bucket, and the dispatcher falls back
to the pool defaults (or to `default_bucket_type`, when one is
named).

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  bucket_types:
  - name: sessions
    read_consistency: DC_QUORUM
    write_consistency: DC_EACH_SAFE_QUORUM
    n_val: 3
  - name: cache
    read_consistency: DC_ONE
    write_consistency: DC_ONE
    n_val: 1
  default_bucket_type: cache
```

With this stanza:

* `GET sessions/abc123` is planned with `DC_QUORUM`; writes use
  `DC_EACH_SAFE_QUORUM` and may fan out across DCs.
* `GET cache/u/9000` uses `DC_ONE` reads and writes.
* `GET plain-key` (no slash) falls through to
  `default_bucket_type: cache` and inherits its `DC_ONE`
  routing.
* Removing `default_bucket_type` makes the slashless and
  unknown-prefix cases inherit the pool-level defaults.

`n_val` caps the number of replicas a single request fans out
to. `0` means "no cap" (the default; the consistency level
alone decides fan-out). A positive `n_val` truncates the plan
to its first `n_val` targets, where rack-local replicas are
already ordered first.

Validation enforces unique bucket-type names, valid consistency
strings, and that `default_bucket_type` (when set) names an
entry in `bucket_types`. Invalid stanzas are caught by
`dynomited --test-conf` before the daemon starts.

## Hinted handoff

Hinted handoff is the data-availability tier that lets writes
succeed even when one of their target peers is unreachable. The
dispatcher records the on-the-wire request bytes plus the
intended peer index in a node-local *hint store*; a background
drainer periodically replays the hints once the peer returns to
the gossip-defined `Normal` state.

The feature is **off by default**. Enable it per-pool in YAML:

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
  read_consistency: DC_QUORUM
  write_consistency: DC_QUORUM
  enable_hinted_handoff: true
  hint_ttl_seconds: 86400
  hint_store_max_bytes: 67108864
  hint_drain_interval_ms: 30000
  hint_dir: /var/lib/dynomite/hints
```

Knobs:

* `enable_hinted_handoff` (bool, default `false`): master
  switch. When `false` the dispatcher's behaviour is unchanged
  and a Down or unreachable target is silently skipped.
* `hint_ttl_seconds` (uint, default `86400` / 24 hours):
  per-hint expiry. Hints older than this are dropped during the
  next drainer sweep so the store stays bounded.
* `hint_store_max_bytes` (uint, default `67108864` / 64 MiB):
  upper bound on the cumulative payload bytes of pending hints.
  Once the store is full the dispatcher falls back to its
  non-handoff error path
  (`DynomiteNoQuorumAchieved`) and the next drainer sweep
  reclaims space when peers come back online.
* `hint_drain_interval_ms` (uint, default `30000`): cadence of
  the drainer sweep. Every tick (a) drops expired hints and
  (b) replays the queued hints for every peer that has
  transitioned to `Normal`.
* `hint_dir` (path, default unset): when set, the hint store is
  durable. It keeps one append-only segment file per peer under
  this directory and replays them at startup, so hints queued
  for a temporarily down peer survive a coordinator restart.
  When unset the hint store is RAM-only and queued hints are
  lost on restart. Ignored when `enable_hinted_handoff` is
  `false`.

Operational notes:

* Hinted handoff applies only to **writes**. Reads against a
  Down peer remain a no-data-to-hint situation and continue to
  follow the legacy "skip the target and fall through to the
  consistency check" path.
* The hint store is RAM-only by default. Set `hint_dir` to make
  it durable: the store then writes one append-only segment
  file per peer under that directory and replays them at
  startup, so pending hints survive a node restart. Without
  `hint_dir`, a node restart drops every pending hint, so size
  `hint_store_max_bytes` and the drainer cadence conservatively.
* The synthesised reply that the dispatcher feeds the
  coalescer on a hinted target's behalf is `+OK\r\n`. This is
  the correct shape for `SET`-style writes (the dominant
  case) but may show up as a "divergent target" for
  request types whose reply is an integer or a multibulk
  (e.g. `DEL`). The hint is still recorded and replayed; the
  surviving real replies determine the coalesced answer the
  client sees, and read-repair (which only fires on `GET`)
  is not affected.
* The `hint_drain_interval_ms` value should be small enough to
  notice peer recoveries promptly but large enough that the
  drainer does not dominate the per-tick scheduling cost when
  many peers come back at once. The default 30 seconds matches
  the gossip period.

## Search index persistence

The optional RediSearch FT.* surface (`crates/dynomite-search`)
is wired into `dynomited` behind the `search` Cargo feature.
By default the FT.* index registry is purely in-memory: index
definitions, indexed documents, text fields, and suggestion
dictionaries are lost on a process restart and the client must
recreate them.

* `search_index_dir` (path, default unset): when set, the
  search registry snapshots its full state (index schemas,
  indexed documents, per-`TEXT`-field contents, and FT.SUG*
  suggestion dictionaries) to a CBOR file under this directory
  and reloads it on restart. A clean checkout, a process kill,
  or a chaos restart no longer drops the FT.* surface: the
  snapshot survives on disk and the next startup reloads every
  index without the client re-issuing `FT.CREATE` or re-feeding
  data.

Operational notes:

* The snapshot is written atomically (write to a sibling
  `*.tmp`, flush + fsync, rename over the live file), so a
  crash mid-write never leaves a half-written snapshot; the
  prior good snapshot survives. A stray `*.tmp` from an
  interrupted write is ignored on load and overwritten on the
  next save.
* Snapshots are taken periodically (every five seconds) and
  once more on clean shutdown. A crash between snapshots loses
  at most the un-snapshotted delta; the FT.* workload tolerates
  re-creation, so the recreate-on-miss path becomes a fallback
  rather than the primary recovery once persistence is on.
* When `search_index_dir` is unset the registry never touches
  disk; behaviour is identical to releases before this knob
  existed.

## Riak mode

The optional Riak protocol surface (`crates/dyniak`) is
wired into `dynomited` behind the `riak` Cargo feature. When
the binary is built with `--features riak`, the `riak:` block
of the pool body controls the PBC listener, the HTTP gateway,
and the optional active-anti-entropy (AAE) scheduler. The
block is parsed and validated even under the default build so
YAML files authored against the Riak-enabled binary still
validate without the feature flag; under the default build the
fields are inert at run time.

```yaml
my_pool:
  ...
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
    quic_listen: 127.0.0.1:8089
    aae_enabled: true
    aae_full_sweep_interval_seconds: 86400
    aae_segment_interval_seconds: 60
    tls_cert: /etc/dynomited/riak.crt
    tls_key: /etc/dynomited/riak.key
    tls_ca: /etc/dynomited/ca.pem
    wasm_modules:
    - id: wordcount
      path: /etc/dynomited/wasm/wordcount.wasm
```

Every field is optional. Setting only `pbc_listen` enables the
PBC listener with no HTTP gateway; setting only `http_listen`
runs HTTP without PBC. The two listeners share a single
backing `Datastore` so request accounting accumulates in one
place.

| Key | Type | Meaning |
| --- | --- | --- |
| `pbc_listen` | `host:port` | Riak PBC listener bind address (TCP). |
| `http_listen` | `host:port` | Riak HTTP gateway bind address. |
| `quic_listen` | `host:port` | Riak PBC listener bind address over QUIC (a UDP socket; same framing as `pbc_listen`). Requires `tls_cert` + `tls_key` and a binary built with the `quic` feature; rejected otherwise. |
| `aae_enabled` | bool | Spawn the AAE scheduler. Default `false`. |
| `aae_full_sweep_interval_seconds` | u64 | Cadence over which one full sweep across every peer pair completes. Default 86400. |
| `aae_segment_interval_seconds` | u64 | Cadence of one (peer, time-bucket) exchange tick. Default 60. Must be <= `aae_full_sweep_interval_seconds`. |
| `tls_cert` | path | PEM certificate for the Riak listeners. When `tls_cert` + `tls_key` are both set the listeners terminate TLS; both absent runs plaintext. Setting one without the other is rejected. |
| `tls_key` | path | PEM private key matching `tls_cert`. |
| `tls_ca` | path | Optional PEM CA bundle. When set, inbound clients must present a cert signed by a CA in the bundle (mutual TLS). |
| `wasm_modules` | list | Wasm map/reduce modules registered with the MapReduce executor at startup. Each entry is `{id, path}` where `path` points at a `.wasm` or `.wat` file. Loaded only when the binary is built with the `wasm` feature; otherwise parsed and validated but a `Phase::WasmModule` submission returns a `WasmNotImplemented` error. Every `id` must be unique and every `path` must exist at validation time. |

The CLI offers three matching overrides for the same knobs:
`--riak-pbc-listen=HOST:PORT`, `--riak-http-listen=HOST:PORT`,
and `--riak-aae-enabled`. They are visible in
`dynomited --help` only when the binary was built with
`--features riak`. See [Riak mode](./operations/riak.md) for
operator-facing details.

## Distribution modes

The pool's `distribution:` directive selects the algorithm that
maps a hashed key to one of the rack's peers. Two modes are
first-class and supported indefinitely:

| `distribution:` | Behaviour |
|---|---|
| `vnode` | Per-rack continuum keyed by per-peer `tokens:` lists. The historical default. |
| `random_slicing` | Small, gap-free `(name, size)` partition table over the 64-bit hash space. New in this slice. |

`random_slicing` is the recommended mode for new deployments
that do not need byte-identical compatibility with an existing
operator-managed token plan. The technique is described in
detail in `docs/design/random-slicing-integration.md`; the
operator-facing pitch is "you cannot accidentally configure a
hole in the ring", which is the failure mode that bit chaos
pass-3 (3-of-4 hosts running with a 4-host token plan, 25% of
the ring orphaned).

The legacy aliases `ketama`, `modula`, and `random` are accepted
for backward compatibility with the C reference's vocabulary.
They emit a `tracing::warn!` line at config-load time and
collapse to `vnode` at runtime.

### Default per binary build

* **Default builds** keep `vnode` as the default. Existing YAML
  files validate and run unchanged.
* **`--features riak` builds** that configure a Riak listener
  (`riak.pbc_listen` or `riak.http_listen`) flip the default to
  `random_slicing`. Riak-shaped deployments inherit Riak's
  full-coverage partition invariant by default. Operators can
  still set `distribution: vnode` explicitly to override.

### Migration: shadow mode

A `--distribution-shadow=<vnode|random_slicing>` CLI flag (and
the matching `distribution_shadow:` YAML key) lets an operator
run both modes simultaneously: routing follows the live
`distribution:`, but the dispatcher also computes the would-be
peer for the shadow mode and bumps the
`distribution_shadow_disagreement_total` Prometheus counter when
they disagree. Use this to validate a migration before flipping
the YAML.

The `dyn-admin distribution-dump --node <host:stats-port>`
subcommand pretty-prints the configured live and shadow modes
plus the cumulative disagreement counter, so an operator can
verify each node's view from one place.

```yaml
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:6379:1
  data_store: 0
  distribution: random_slicing       # gap-free partition table
  distribution_shadow: vnode         # validate the new mode
                                     # against the old one
```
