# Dynomite

[![Codeberg CI](https://codeberg.org/gregburd/dynomite/actions/workflows/ci.yml/badge.svg)](https://codeberg.org/gregburd/dynomite/actions)
[![GitHub CI](https://github.com/gburd/dynomite/actions/workflows/ci.yml/badge.svg)](https://github.com/gburd/dynomite/actions/workflows/ci.yml)
[![Docs](https://github.com/gburd/dynomite/actions/workflows/docs.yml/badge.svg)](https://gburd.github.io/dynomite/)
[![crates.io](https://img.shields.io/crates/v/dynomite-engine.svg)](https://crates.io/crates/dynomite-engine)
[![docs.rs](https://img.shields.io/docsrs/dynomite-engine.svg)](https://docs.rs/dynomite-engine)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](./LICENSE)

**Reference manual and getting-started guide:**
[gburd.github.io/dynomite](https://gburd.github.io/dynomite/)
(mirror: [gregburd.codeberg.page/dynomite](https://gregburd.codeberg.page/dynomite/)).

Inspired by [Dynamo whitepaper](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf),
is a thin, distributed dynamo layer for different storage engines and
protocols. Currently these include [Valkey](https://github.com/valkey-io/valkey)
(an open source fork of Redis), [MemcacheD](http://www.memcached.org/), and
[Noxu](https://codeberg.org/gregburd/noxu) a Rust re-write of [Berkeley DB Java
Edition](https://github.com/berkeleydb/je) which is used to provide a
[Riak](https://github.com/basho/riak)-like API/feature set called "Dyniak".

Dynomite supports multi-datacenter replication and is designed for high availability.
When using Dyniak there is support for transactional multi-key updates that depend
on the distributed transaction (XA) support in Noxu based on 2PC.

The ultimate goal with Dynomite is to be able to implement high availability and
cross-datacenter replication on storage engines that do not inherently provide that
functionality. The design aims for few moving parts and a predictable p99; see the
[Status](#status) section below for what is and is not yet demonstrated in practice.

Dynomite, in this Rust form, can be deployed as a server fronting another
engine as is the case with Valkey and MemcacheD or it can be embedded and used
as the dynamo-style distributed k/v storage orchestration layer for your project.

It can be used both as:

* a standalone server binary (`dynomited`), and
* a library crate published on crates.io as `dynomite-engine` (its
  library name is `dynomite`) that can be embedded directly in another
  Rust program through a stable, documented API.

## Origin Story

This project is a from-scratch Rust port of
[Netflix Dynomite](https://github.com/Netflix/dynomite) that aims to be
functionally identical to the original C codebase.

## Data stores

Each node fronts one backend, selected by the `data_store:` key in the
YAML config (or the integer form):

* `valkey` (alias `redis`, integer `0`) -- the Valkey / RESP wire
  protocol. The `redis` spelling is accepted for back-compat and maps
  to the same backend.
* `memcache` (alias `memcached`, integer `1`) -- the Memcached ASCII
  protocol.
* `dyniak` (integer `2`) -- the built-in Riak-compatible store. It
  serves the Riak PBC and HTTP surfaces from an embedded transactional
  Noxu environment and requires `dynomited` built with `--features riak`
  plus a `noxu_path:` knob. A dyniak pool does not run a RESP client
  proxy.

## Capabilities

What the engine does today:

* Consistent-hash sharding and multi-data-center replication on a
  shared-nothing architecture with no single point of failure.
* Gossip-based cluster membership and topology discovery.
* Tunable quorum reads and writes.
* Hinted handoff (durable, persisted under `hint_dir:`) for writes to
  temporarily unavailable peers.
* Read repair on divergent replicas.
* Active anti-entropy (Merkle-tree) reconciliation.
* For dyniak: cross-node XA transactions, object links and link
  walking, secondary indexes (2i), MapReduce (with optional WASM map /
  reduce phases, gated on the `wasm` feature), and `FT.*` full-text /
  vector / regex search with a durable index.

Network communication between Dynomite nodes can run over TCP (default,
matching the original) or QUIC (via the `quiche` crate, gated on the
`quic` feature). Both transports support IPv4 and IPv6.

## Status

This is an in-progress port and a young system. The version number
(currently 1.x) reflects internal milestone cadence, not field-proven
maturity: the project has extensive self-verification (a symbol-level
parity ledger, deterministic-simulation and Elle history checks as merge
gates, a multi-DC chaos gate, fuzzing, property tests) but no production
hours and no independent adversarial audit yet. Treat it as a rigorously
engineered, extensively self-tested young system, not as a drop-in
replacement with a decade of field hardening behind it.

Scope and non-goals worth stating up front:

* **Consistency**: tunable quorum in the Dynamo tradition (eventual
  consistency; quorum overlap is not linearizability). There is no
  strong-consistency / consensus mode (the Riak `riak_ensemble`
  analogue is out of scope).
* **Cross-DC replication**: multi-datacenter replication is supported,
  but there is no realtime cross-DC replication product analogous to
  Riak Enterprise `riak_repl`.
* **Dyniak search**: the `FT.*` surface is RediSearch-shaped
  (vector / trigram-text / regex), served per-node, not
  Yokozuna/Solr-compatible Riak Search; cluster-wide distributed search
  fan-out is not yet the default.
* **Mixed clusters**: interoperating a live Netflix Dynomite (C) cluster
  with these Rust nodes in one ring is **not** a tested configuration.
  The supported migration story is whole-cluster replacement, not
  rolling node-by-node replacement of a running C cluster.

See `PLAN.md` for the staged roadmap, `docs/parity.md` for the live
C-to-Rust mapping (including intentional deviations and ambiguities),
and `AGENTS.md` for the contributor operating manual.

## Quick start

```
nix develop
cargo build --workspace
cargo nextest run --workspace
```

The Nix flake pins every tool needed to build, test, fuzz, bench, and
package the project.

Run the server against a config file:

```
cargo run -p dynomited -- --conf-file conf/dynomite.yml
```

Validate a config without starting the server with `--test-conf`, and
see `dynomited --help` (or the `dynomited.8` man page) for the full flag
list.

For a ten-minute walk-through that takes a fresh checkout to a running
search stack with vector, text, and regex queries over `valkey-cli`,
see [`docs/book/src/tutorial-search.md`](./docs/book/src/tutorial-search.md).

## Embedding

Add the engine to another Rust project:

```
cargo add dynomite-engine
```

The crate is imported as `dynomite`. Build a server with `ServerBuilder`
and drive it via the returned handle:

```rust,no_run
use dynomite::{Server, ServerBuilder};
use dynomite::conf::DataStore;

#[tokio::main]
async fn main() {
    let server = ServerBuilder::new("dyn_o_mite")
        .listen("127.0.0.1:0".parse().unwrap())
        .dyn_listen("127.0.0.1:0".parse().unwrap())
        .data_store(DataStore::Valkey)
        .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
        .tokens_str("0")
        .build()
        .unwrap();
    let handle = server.start().await.unwrap();
    handle.shutdown().await.unwrap();
}
```

The full embedding cookbook lives in the `dynomite::embed` module docs
and in [`docs/book/`](./docs/book/); runnable examples are under
[`crates/dynomite/examples/`](./crates/dynomite/examples/).

## Acknowledgements

This Rust implementation is a port of the original
[Dynomite](https://github.com/Netflix/dynomite) project by Netflix, Inc.,
which itself extended Twitter's `twemproxy`. Both projects are licensed
under the Apache License 2.0; their notices are preserved in `NOTICE`.

## License

Apache License 2.0. See `LICENSE`.
