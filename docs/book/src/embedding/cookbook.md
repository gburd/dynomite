# Embedding cookbook

This page is the mainstream reference for embedding Dynomite as a
library inside another Rust program. It complements
[Server lifecycle](./server.md), [Hooks and traits](./hooks.md),
and [Examples](../examples/index.md): the lifecycle and hooks pages pin
the API surface, the examples page sketches three end-to-end
scenarios, and the cookbook below answers the common questions
embedders ask in order.

## When to embed vs daemon

Dynomite ships in two equally first-class shapes:

* `dynomited` (the binary, in `crates/dynomited/`) - reads a
  YAML file, runs the same `dynomite::embed::Server` engine
  under its own `#[tokio::main]`, exposes a stats HTTP endpoint
  and a SIGHUP reload hook, and is what a typical operations
  team installs as a service. Choose the daemon when you want
  observability and lifecycle isolation from your application
  process and when you are happy proxying through a TCP socket.
* `dynomite` (the library, in `crates/dynomite/`) - exposes the
  same engine through the typed [`embed`](./index.md)
  API. The host program owns the tokio runtime, plugs custom
  hooks, drives the engine through a [`ServerHandle`], and reads
  metrics either by holding the live `Arc<Stats>` or by plugging
  a custom [`MetricsSink`]. Choose embedding when:
    * the storage layer is already in-process (an in-memory
      B-Tree, an `sled`, a `RocksDB`, ... ) and you want
      Dynomite-style ring routing and quorum without serialising
      to a wire format;
    * you want to drive Dynomite from your application's own
      lifecycle / shutdown signals (and to avoid double-running
      tokio runtimes);
    * you want bespoke metrics integration (a Prometheus
      registry your application already exposes, an OTLP
      pipeline, a custom Grafana dashboard adapter) without
      mounting a second HTTP listener;
    * your transport is non-TCP: Unix domain sockets, in-memory
      pipes for tests, mutual-TLS, or the upcoming QUIC variant.

The library and the daemon share the same engine and the same
public API; choosing one over the other is purely an operational
decision. This entire page assumes the library shape.

## Smallest possible embedded server

The five-line example below is the canonical smallest embedding.
It binds two ephemeral ports (the `:0` syntax asks the kernel for
a free port), spawns the engine, prints the post-bind addresses,
and shuts down. The cookbook references this exact body as the
"five-line embedded server"; the runnable form lives in
[`embedded_minimal.rs`](https://codeberg.org/gregburd/dynomite/src/branch/main/crates/dynomite/examples/embedded_minimal.rs).

```rust,no_run
use dynomite::embed::{Server, ServerBuilder};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let handle = Server::start_with(
        ServerBuilder::new("dyn_o_mite")
            .listen("127.0.0.1:0".parse()?)
            .dyn_listen("127.0.0.1:0".parse()?),
    ).await?;

    eprintln!("up on {:?}", handle.listen_addr());
    handle.shutdown().await?;
    Ok(())
}
```

Run it:

```text
cargo run --example embedded_minimal
embedded dynomite up; client listen=Some(127.0.0.1:NN) dnode listen=Some(127.0.0.1:NN)
shutdown ok
```

What the defaults give you when you do not call any other
setter:

* one pool named `"dyn_o_mite"` (any string works);
* one stub backend so validation passes (override with
  `.servers(...)` or `.datastore(...)`);
* a single token (`0`) on a single rack in a single datacenter
  (the local node);
* gossip off, retry timeout, mbuf budget, and stats interval at
  the same defaults `dynomited` would apply via `apply_defaults`;
* the in-crate `MemoryDatastore` standing in for the wire-level
  Redis bridge.

The defaults exist so the cookbook's "smallest" example fits in
five lines; production embeddings always customise the backend
and the topology.

## Custom Datastore: plug a non-Redis store

Dynomite's gossip / ring / quorum layer is independent of the
backing store. The default in-crate `RedisDatastore` and
`MemcacheDatastore` front the two protocols Dynomite was born to
proxy, but any type that implements
[`dynomite::embed::Datastore`](./hooks.md#datastore)
slots in. Common targets:

* `RocksDB`, `sled`, `redb`, in-process B-Trees - host the
  storage layer in the same process as Dynomite to remove the
  socket round-trip and avoid serialising structured values to
  RESP / Memcache wire format.
* HTTP-fronted KV stores - call out via `reqwest` or `hyper`.
* The Riak K/V protocol - the workspace ships
  [`crates/dyniak`](https://codeberg.org/gregburd/dynomite/src/branch/main/crates/dyniak)
  on top of the same trait.

The trait surface (verbatim from
[`hooks.rs`](./hooks.md#datastore)):

```rust,no_run
# use dynomite::embed::hooks::{BoxFuture, Datastore, DatastoreError, Protocol};
# use dynomite::msg::{Msg, MsgType};
# use std::sync::Arc;
# use parking_lot::Mutex;
#[derive(Default, Clone)]
struct InMemoryDatastore {
    map: Arc<Mutex<std::collections::BTreeMap<u64, MsgType>>>,
}

impl Datastore for InMemoryDatastore {
    fn protocol(&self) -> Protocol { Protocol::Custom }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        let map = self.map.clone();
        Box::pin(async move {
            // Application logic here. SET stores; GET / anything
            // else replies with the previously stored kind.
            let stored = {
                let mut g = map.lock();
                if matches!(req.ty(), MsgType::ReqRedisSet) {
                    g.insert(req.id(), MsgType::RspRedisStatus);
                }
                g.get(&req.id()).copied().unwrap_or(MsgType::RspRedisStatus)
            };
            let mut rsp = Msg::new(req.id(), stored, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}
```

Plug it via `ServerBuilder::datastore`:

```rust,no_run
# use dynomite::embed::{Server, ServerBuilder};
# use dynomite::embed::hooks::{BoxFuture, Datastore, DatastoreError, Protocol};
# use dynomite::msg::{Msg, MsgType};
# #[derive(Default, Clone)]
# struct InMemoryDatastore;
# impl Datastore for InMemoryDatastore {
#     fn protocol(&self) -> Protocol { Protocol::Custom }
#     fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
#         Box::pin(async move {
#             let mut rsp = Msg::new(req.id(), MsgType::RspRedisStatus, false);
#             rsp.set_parent_id(req.id());
#             Ok(rsp)
#         })
#     }
# }
# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
let store = InMemoryDatastore::default();
let handle = Server::start_with(
    ServerBuilder::new("dyn_o_mite")
        .listen("127.0.0.1:0".parse().unwrap())
        .dyn_listen("127.0.0.1:0".parse().unwrap())
        .datastore(Box::new(store)),
).await.unwrap();
# handle.shutdown().await.unwrap();
# });
```

Drive traffic with `ServerHandle::inject_request` (in-process
fast path) or via the bound listen address (cross-process).
`crates/dynomite/tests/embed_api.rs` shows the request-shape
round-trip end to end, and `embedded_cluster3.rs` shows the same
custom datastore behind three nodes.

## Custom Transport: swap TCP for QUIC or anything else

Dynomite's `Transport` trait is the per-connection abstraction:
any `AsyncRead + AsyncWrite + Send + Unpin` byte stream tagged
with a `ConnRole` is a valid transport. The two ready-made
implementations are `TcpTransport` (always available) and the
QUIC variant (gated behind the `quic` feature).

What ships today, and where the boundary is:

* The trait shape is stable. `crates/dynomite/examples/embedded_custom_transport_sketch.rs`
  shows how to wrap a `tokio::io::DuplexStream`, a
  `tokio_rustls::TlsStream`, or a Unix domain socket into a
  `Transport` impl. The wrapper carries a `ConnRole` tag and
  reports a synthetic `peer_addr`.
* Plugging a *custom listener* (a factory that yields these
  transports as connections arrive) into `ServerBuilder` is
  tracked as a follow-up: the builder does not yet expose a
  `transport_listener` setter. The embedded server serves the
  client plane over TCP on its bound `listen:` socket today
  (parse -> dispatcher -> the configured `Datastore` hook), and
  `ServerHandle::inject_request` drives in-process traffic;
  custom transports beyond TCP are available by driving
  `Proxy` / `QuicProxy` directly, as the `dynomited` binary
  does. Cross-process peer-plane traffic is served by
  `dynomited`.

In short: write your `Transport` impl today (the shape is
permanent); wire a custom listener through `Proxy` / `QuicProxy`
until a `ServerBuilder` `transport_listener` setter lands. The
embedded sketch double-checks the API contract for you.

## Subscribing to cluster events

The engine publishes cluster-wide events on two complementary
buses:

1. `ServerHandle::events() -> Arc<EventManager>` - the
   structured `ClusterEvent` broadcast (`PeerUp`, `PeerDown`,
   `GossipRoundComplete`, `AaeExchangeStarted`, `RingChanged`,
   ...). Mirrors the C reference's diagnostic logging in typed,
   matchable form. Use this for application-facing observability.
2. `ServerHandle::subscribe_events() -> EventStream` - the
   lower-level `ServerEvent` broadcast (`ConnectionAccepted`,
   `ConnectionClosed`, `ConfigReloaded`, `Lagged`, ...). Use this
   for connection-level tooling.

The two streams are independent; subscribe to whichever (or
both) suits your integration. Lagging consumers receive a
`ClusterEvent`-side lag (handled by your `match` arm via the
`#[non_exhaustive]` wildcard) or a `ServerEvent::Lagged`
payload, respectively, so a slow consumer never silently drops
events.

```rust,no_run
# use dynomite::embed::{Server, ServerBuilder};
# use dynomite::events::ClusterEvent;
# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
let handle = Server::start_with(
    ServerBuilder::new("p")
        .listen("127.0.0.1:0".parse().unwrap())
        .dyn_listen("127.0.0.1:0".parse().unwrap()),
).await.unwrap();

let events = handle.events();
let mut sub = events.subscribe();
tokio::spawn(async move {
    while let Ok(evt) = sub.recv().await {
        match evt {
            ClusterEvent::PeerUp { peer_id, dc, .. } => {
                eprintln!("peer {peer_id} up in {dc}");
            }
            ClusterEvent::PeerDown { peer_id, .. } => {
                eprintln!("peer {peer_id} down");
            }
            _ => {}
        }
    }
});
# handle.shutdown().await.unwrap();
# });
```

`ClusterEvent` is `#[non_exhaustive]`; your `match` must include
a wildcard arm so future variants stay non-breaking. The same
rule applies to `ServerEvent`.

## Reading metrics from inside the embedder

Dynomite exposes its metrics surface through three mechanisms,
each suited to a different integration shape:

1. **Pull, lock-free**: `ServerHandle::stats_handle() ->
   Arc<Stats>` returns a clone-cheap handle to the live
   aggregator. Read it whenever you want; the snapshot it
   returns is a value type that is safe to hold across awaits.
   Use this for pull-model exporters (Prometheus scrape
   handlers, OpenTelemetry pull readers) that want to read
   current counters without going through a periodic flush.

2. **Pull, snapshot-only**: `ServerHandle::stats() -> Snapshot`
   is the simpler shape - it returns the latest snapshot the
   stats aggregator computed. Cheaper to call than
   `stats_handle().snapshot()` because the runtime caches the
   most recent snapshot under a lock; identical content, lower
   contention.

3. **Push**: implement
   [`MetricsSink`](./hooks.md#metricssink) and plug
   it via `ServerBuilder::metrics_sink`. The runtime calls
   `MetricsSink::emit(&snapshot)` on the cadence the trait
   reports through `flush_interval`. Use this to forward the
   snapshot to OTLP, statsd, a custom dashboard adapter, or any
   other push-based collector.

```rust,no_run
# use std::sync::Arc;
# use dynomite::embed::{Server, ServerBuilder};
# use dynomite::stats::Stats;
# tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
let handle = Server::start_with(
    ServerBuilder::new("p")
        .listen("127.0.0.1:0".parse().unwrap())
        .dyn_listen("127.0.0.1:0".parse().unwrap()),
).await.unwrap();

// Pull model.
let stats: Arc<Stats> = handle.stats_handle();
let snap = stats.snapshot();
println!("pool: {}", snap.pool.name);

// Or use the cached snapshot accessor.
let snap = handle.stats();
println!("pool again: {}", snap.pool.name);
# handle.shutdown().await.unwrap();
# });
```

The `describe_stats` accessor on the handle returns the
manifest of every metric the engine emits, which is what a
typical exporter wires into its registry at startup.

## Graceful shutdown patterns

Two sanctioned shapes integrate Dynomite with your application's
own lifecycle:

### Pattern A: app-driven shutdown

Your application owns the shutdown signal (a `ctrl_c`, a kill
switch from your supervisor, an internal "drain" command). Park
the embedder on `ServerHandle::join` and trigger the cancel
from a side task that watches the signal. `join` returns when
the engine's background tasks finish.

```rust,no_run
use dynomite::embed::{Server, ServerBuilder};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let handle = Server::start_with(
        ServerBuilder::new("dyn_o_mite")
            .listen("127.0.0.1:0".parse()?)
            .dyn_listen("127.0.0.1:0".parse()?),
    ).await?;

    let shutdown = handle.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown.shutdown().await;
    });

    handle.join().await;
    Ok(())
}
```

### Pattern B: timeout-bounded shutdown

In the daemon shape, `dynomited` puts a wall-clock budget on
shutdown so a stuck task does not block the process. The same
pattern works in an embedder:

```rust,no_run
# use std::time::Duration;
# use dynomite::embed::{Server, ServerBuilder};
# tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
let handle = Server::start_with(
    ServerBuilder::new("p")
        .listen("127.0.0.1:0".parse().unwrap())
        .dyn_listen("127.0.0.1:0".parse().unwrap()),
).await.unwrap();

// Fire shutdown then wait at most 5 seconds for the join set.
let h = handle.clone();
tokio::spawn(async move { let _ = h.shutdown().await; });
let _ = tokio::time::timeout(Duration::from_secs(5), handle.join()).await;
# });
```

Two important guarantees from the embed API:

* `ServerHandle::shutdown` is idempotent. Calling it twice is
  safe; the second call is a no-op and returns immediately.
* `ServerHandle::join` after a `shutdown` returns immediately
  because the task set is drained on the first `shutdown`.

## Production readiness

Dynomite as a library has been exercised under multi-host chaos
across all four supported backends:

* TCP + Redis;
* TCP + Memcache;
* TCP + Riak (via `dyniak`);
* peer-plane TLS variants of each.

The committed reports under
[`dist/chaos-reports/v0.1.0/`](https://codeberg.org/gregburd/dynomite/src/branch/main/dist/chaos-reports/v0.1.0)
record seven independent multi-host chaos passes against four
EC2 hosts at a time, exercising:

* gossip convergence under partition / merge cycles;
* per-DC quorum read repair under packet loss / corruption /
  reorder;
* hinted handoff drain during a sustained peer outage;
* anti-entropy reconciliation across rolling restarts;
* peer-plane TLS handshake under clock skew.

Every committed pass shows zero invariant violations, the
post-test sweep confirms the host is left in its pre-test state,
and the report itself is the production sign-off for the
matching backend / transport pair. Embedders that ship a
distinct backend should run the same harness against their own
deployment shape; the shape lives in
[`crates/dynomite/tests/stage_16_chaos.rs`](https://codeberg.org/gregburd/dynomite/src/branch/main/crates/dynomite/tests/stage_16_chaos.rs)
and is documented in
[Operations / Chaos test](../operations/chaos.md).

The library surface itself is covered by
`crates/dynomite/tests/embed_api.rs` (the API smoke tests this
cookbook references) and the broader
`crates/dynomite/tests/stage_13_embed.rs` integration suite.
Both suites run in CI on every push.

[`MetricsSink`]: ./hooks.md#metricssink
[`ServerHandle`]: https://docs.rs/dynomite-engine/latest/dynomite/embed/struct.ServerHandle.html
