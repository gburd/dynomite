# dynomite

[![Crates.io](https://img.shields.io/crates/v/dynomite.svg)](https://crates.io/crates/dynomite)
[![Docs.rs](https://docs.rs/dynomite/badge.svg)](https://docs.rs/dynomite)
[![License](https://img.shields.io/crates/l/dynomite.svg)](https://github.com/gburd/dynomite/blob/main/LICENSE)

An embeddable distributed-replication engine for Rust. `dynomite` gives
you the substrate to build your own Dynamo-style distributed system: a
gossip-driven cluster, ring-based key partitioning across racks and
datacenters, tunable consistency, hinted handoff, active anti-entropy,
optional QUIC peer transport, and a RESP client surface (the
protocol Valkey speaks) including the RediSearch FT.* command family.

You wire in the things only you can decide: how to store a key on a
single node and how a peer is discovered. The engine handles the rest.

## Why use it

* **Library-first.** `dynomite` is a crate; you embed it. The same code
  drives the `dynomited` server binary, so the embed surface is
  exercised in production rather than as an afterthought.
* **Open replication strategy.** Rack- and datacenter-aware token rings,
  consistent hashing across thirteen hash functions, random slicing,
  configurable replica counts, hinted handoff, and merkle-tree
  anti-entropy are all in the box.
* **Stable embedding API.** Five pluggable traits cover datastore,
  service discovery, transport, crypto, and metrics. Every trait ships
  with at least one in-crate default so a one-page program is enough
  to bring up a node.

## Quick start

```rust,no_run
use dynomite::conf::{ConfServer, DataStore};
use dynomite::embed::{Server, ServerBuilder};

# tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
let handle = ServerBuilder::new("my_pool")
    .listen("127.0.0.1:8102".parse().unwrap())
    .dyn_listen("127.0.0.1:8101".parse().unwrap())
    .data_store(DataStore::Valkey)
    .servers(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()])
    .tokens_str("0")
    .build().unwrap()
    .start().await.unwrap();

// ... your application runs ...

handle.shutdown().await.unwrap();
# });
```

## What you build vs what dynomite handles

You bring the per-node truth; the engine takes care of the cluster.

| You implement (hooks)    | The engine handles                       |
|--------------------------|------------------------------------------|
| `Datastore`              | Token-ring routing, replica selection,   |
|                          | quorum, retries, request coalescing      |
| `SeedsProvider`          | Gossip, peer-state machine, handoff      |
| `Transport`              | TCP/QUIC reactor, framing, back-pressure |
| `CryptoProvider`         | DNODE peer-encryption handshake          |
| `MetricsSink`            | Per-pool, per-server, per-peer counters  |

Every hook is `Send + Sync` and object-safe. Async methods return
`BoxFuture` so adapters can be moved across tokio tasks freely.

## Public surface

The crate root re-exports the most common embedding handles:

```rust,ignore
use dynomite::{Server, ServerBuilder, ServerHandle};
use dynomite::embed::{Datastore, SeedsProvider, CryptoProvider, MetricsSink};
use dynomite::embed::{ConnRole, Transport};
use dynomite::events::{ClusterEvent, EventManager, PeerId};
use dynomite::vector::VectorRegistry;
```

See the [`embed`](https://docs.rs/dynomite/latest/dynomite/embed/) module
for the full embedding cookbook with worked examples for:

* a single-node embedding behind a memory datastore,
* a three-node in-process cluster,
* a custom transport sketch,
* the demo vector + trigram-text indexing pipeline,
* a random-slicing distribution example.

## Features

| Feature        | Default | What it adds                                 |
|----------------|---------|----------------------------------------------|
| `tcp`          | yes     | TCP peer transport (default)                 |
| `tls`          | yes     | rustls-backed peer-plane TLS                 |
| `quic`         | no      | QUIC peer transport via `quiche`             |
| `riak-storage` | no      | Riak-compatible API (Noxu storage substrate) |

## Status

This crate is published as a work-in-progress. The embedding API is
expected to evolve until 0.1.0; see
[`PLAN.md`](https://github.com/gburd/dynomite/blob/main/PLAN.md) for the
staged roadmap and `docs/parity.md` for the live capability matrix. The
Stage 16 chaos suite is the production-readiness gate; reports live
under `dist/chaos-reports/` in the repository.

## Acknowledgements

The architecture and many invariants of `dynomite` are derived from
[Netflix's Dynomite](https://github.com/Netflix/dynomite), itself
descended from Twitter's `twemproxy` and inspired by the
[Amazon Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).
Both upstream projects ship under Apache-2.0; their notices are
preserved in the workspace `NOTICE` file.

## License

Apache-2.0. See `LICENSE`.
