# dyn-admin

Cluster admin CLI for the Dynomite Rust port. Modelled on Riak's
`riak-admin` tool: a single binary an operator points at one node and
asks structured questions of.

`dyn-admin` is a thin wrapper around the existing protocol surfaces
shipped by `dyniak` (PBC) and `dynomite` (the `/stats` and
`/metrics` HTTP endpoints). It does not run a daemon and does not
hold any state of its own.

## Subcommands

```text
dyn-admin <COMMAND>

Commands:
  status           Print node identity (PBC) plus a stats summary (HTTP)
  ring-status      Print the ring/topology view
  stats            Pretty-print key metrics from /stats
  metrics          Print the Prometheus text endpoint verbatim
  ping             Send a PBC ping and report RTT
  cluster-list     List cluster peers reachable through the seed
  cluster-join     Stage a peer-join
  cluster-leave    Stage a peer-leave
  cluster-plan     Show staged-but-uncommitted cluster changes
  cluster-commit   Commit every staged cluster change
```

Every subcommand accepts `--node <host:port>` (PBC by default,
HTTP for `stats` and `metrics`) and, where it makes sense,
`--json` for machine output. The `cluster-list` subcommand uses
`--seed` instead of `--node` to make the seed-discovery semantics
explicit. The defaults match the `dynomited` binary shipped in
this workspace:

* PBC: `127.0.0.1:8087`
* Stats / metrics: `127.0.0.1:22222`

## Examples

```sh
# Liveness check.
$ dyn-admin ping
PONG from 127.0.0.1:8087 (1.234 ms)

# Identity + stats summary.
$ dyn-admin status
node: 127.0.0.1:8087
server_node: dyniak
server_version: dyniak 0.0.1
engine_source: node-a
engine_version: 0.0.1
datacenter: dc1
rack: r1
uptime_seconds: 7
pool: dyn_o_mite

# Pipe the stats JSON through jq.
$ dyn-admin stats --json | jq '.dyn_o_mite.client_eof'
0

# Forward Prometheus output to a scraper.
$ dyn-admin metrics > /tmp/scrape.prom
```

## Cluster mutation flow

The four cluster-mutation commands follow Riak's standard
stage-then-commit ritual:

```sh
$ dyn-admin cluster-join 10.0.0.5:8101
Staged cluster-join via 127.0.0.1:8087
  target: 10.0.0.5:8101
  kind: add
  peer:
    host: 10.0.0.5
    port: 8101
    dc: dc1
    rack: r1
    tokens: 3829148213

Run `dyn-admin cluster-commit` to apply.

$ dyn-admin cluster-plan
Pending staged changes on 127.0.0.1:8087

  [0]
    kind: add
    peer:
      host: 10.0.0.5
      ...

1 change(s) staged.

$ dyn-admin cluster-commit
Committed 1 staged change(s) on 127.0.0.1:8087
```

`cluster-leave <peer-idx>` stages a removal the same way; the
operator runs `cluster-commit` to apply both adds and removes
atomically under the gossip mutex. The commit fans the change
out via the existing gossip protocol so the rest of the cluster
converges on the new ring.

When the substrate is run with `dynomite::cluster::NoopClusterAdmin`
(the default for embeddings that do not opt in) every mutation
is rejected at the server with `cluster admin RPC not configured
on this node`.

## Architecture

```text
   dyn-admin <subcommand>
        |
   commands/<subcommand>.rs
        |
   client::PbcClient        <- wraps dyniak::proto::pb::framer
   client::http_get         <- 60-line tokio HTTP/1.1 GET
        |
   PBC port (8087)        Stats port (22222)
```

The crate is the smallest possible CLI: one module per subcommand,
one shared client helper, one shared output formatter. The
top-level error type ([`AdminError`]) carries enough context for
`main` to render every failure as a single line on stderr.
