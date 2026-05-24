# dyn-admin

Cluster admin CLI for the Dynomite Rust port. Modelled on Riak's
`riak-admin` tool: a single binary an operator points at one node and
asks structured questions of.

`dyn-admin` is a thin wrapper around the existing protocol surfaces
shipped by `dyn-riak` (PBC) and `dynomite` (the `/stats` and
`/metrics` HTTP endpoints). It does not run a daemon and does not
hold any state of its own.

## Subcommands (v0)

```text
dyn-admin <COMMAND>

Commands:
  status         Print node identity (PBC) plus a stats summary (HTTP)
  ring-status    Print the ring/topology view
  stats          Pretty-print key metrics from /stats
  metrics        Print the Prometheus text endpoint verbatim
  ping           Send a PBC ping and report RTT
  cluster-list   List cluster peers reachable through the seed
```

Every subcommand accepts `--node <host:port>` (PBC by default,
HTTP for `stats` and `metrics`) and, where it makes sense,
`--json` for machine output. The defaults match the `dynomited`
binary shipped in this workspace:

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
server_node: dyn-riak
server_version: dyn-riak 0.0.1
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

## Deferred subcommands

The following `riak-admin` mutating commands are documented in
`docs/riak-compat-plan.md` Section 5 but intentionally absent in
v0; the substrate does not yet expose the gossip-state mutations
they require:

* `cluster-join`
* `cluster-leave`
* `cluster-plan`
* `cluster-commit`

Invoking any of these names is rejected by clap as an unknown
subcommand. Once the substrate ships the underlying admin-only
DNODE message family, each is added as a fresh `clap` variant.
See `docs/journal/2026-05-24-dyn-admin-v0.md` for the full plan.

## Architecture

```text
   dyn-admin <subcommand>
        |
   commands/<subcommand>.rs
        |
   client::PbcClient        <- wraps dyn-riak::proto::pb::framer
   client::http_get         <- 60-line tokio HTTP/1.1 GET
        |
   PBC port (8087)        Stats port (22222)
```

The crate is the smallest possible CLI: one module per subcommand,
one shared client helper, one shared output formatter. The
top-level error type ([`AdminError`]) carries enough context for
`main` to render every failure as a single line on stderr.
