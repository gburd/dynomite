# dynomite-search

RediSearch FT.* command surface for the
[Dynomite](https://crates.io/crates/dynomite-engine) cluster engine.
Provides:

* a per-server vector index registry,
* the FT.CREATE / FT.SEARCH / FT.INFO / FT.LIST / FT.DROPINDEX
  / FT.AGGREGATE / FT.EXPLAIN / FT.ALTER / FT.REGEX dispatch
  layer over the parsed RESP arguments,
* the cluster-coordinated k-NN broadcast FSM and on-the-wire
  codec the engine's DNODE plane uses to fan a query out to
  every primary peer covering the index's key range,
* an `install` helper that wires the FT.* surface into a
  [`dynomite_engine::embed::ServerBuilder`] via the
  `CommandExtension` hook.

## Why split it out

The Dynomite engine ships a generic Dynamo-style cluster
substrate (token-ring partitioning, gossip, hinted handoff,
anti-entropy) that is independently useful as the foundation
for any replicated key/value system. Search is a layered
surface on top: vectors, an HNSW index, a trigram + bloom
text index, and the RediSearch dispatch glue. Splitting the
two crates lets embedders pull in the cluster substrate
without paying for the search machinery.

## Quickstart (embedding)

```toml
[dependencies]
dynomite-engine = "0.0.1"
dynomite-search = "0.0.1"
```

```rust,no_run
use dynomite::embed::ServerBuilder;
use dynomite::conf::DataStore;
# tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
let mut builder = ServerBuilder::new("dyn_o_mite")
    .listen("127.0.0.1:0".parse().unwrap())
    .dyn_listen("127.0.0.1:0".parse().unwrap())
    .data_store(DataStore::Redis)
    .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
    .tokens_str("0");
let registry = dynomite_search::install(&mut builder);
let handle = builder.build().unwrap().start().await.unwrap();
// `registry` now reflects every FT.CREATE landed by the
// running server; share clones liberally.
let _ = registry;
handle.shutdown().await.unwrap();
# });
```

License: Apache-2.0.
