# Examples

Three illustrative scenarios that exercise the API shape pinned in
[Server lifecycle](./server.md) and [Hooks and traits](./hooks.md).
The code on this page is a sketch; the real, runnable versions land
under `crates/dynomite/examples/` in Stage 13.

## 1. Single-node embedded Dynomite in front of an existing Redis

The simplest case: one process, one Redis backend, no peers, no
custom hooks. The embedding program is responsible for the tokio
runtime.

```rust
use dynomite::embed::{Server, ServerHandle};
use dynomite::conf::{DataStore, ConfServer, Servers};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Server::builder()
        .listen("127.0.0.1:8102".parse()?)
        .stats_listen("127.0.0.1:22222".parse()?)
        .data_store(DataStore::Valkey)
        .servers(Servers::single(ConfServer::parse(
            "127.0.0.1:6379:1 backend",
        )?))
        .datacenter("dc-local")
        .rack("rack-local")
        .enable_gossip(false)
        .build()?;

    let handle: ServerHandle = server.start();

    tokio::signal::ctrl_c().await?;
    handle.shutdown().await?;
    Ok(())
}
```

Hooks used: all defaults. Listener is `TcpTransport`, datastore is
`RedisDatastore`, seeds provider is `SimpleSeedsProvider` over an
empty seed list, crypto is `OpensslCryptoProvider`, metrics sink is
`RestMetricsSink`.

## 2. 3-node in-process cluster for testing

Builds three `Server` instances inside one process, plugs in a
`MockDatastore` and `MockSeedsProvider`, and drives traffic through
`inject_request` so no kernel sockets are involved. This is the
shape `tests/embed_cluster.rs` will use.

```rust
use dynomite::embed::{Server, ServerHandle};
use dynomite::embed::testing::{MockDatastore, MockSeedsProvider};
use dynomite::msg::{Msg, MsgType};

async fn spawn_node(idx: usize, seeds: MockSeedsProvider) -> ServerHandle {
    Server::builder()
        .datacenter("dc-test")
        .rack(format!("rack-{idx}"))
        .tokens(format!("{}", idx * 1431655765).parse().unwrap())
        .datastore(Box::new(MockDatastore::default()))
        .seeds_provider(Box::new(seeds))
        .enable_gossip(true)
        .gos_interval_ms(100)
        .build()
        .unwrap()
        .start()
}

#[tokio::test]
async fn three_node_cluster_routes_by_token() {
    let seeds = MockSeedsProvider::with_three_nodes();
    let nodes = futures::future::join_all((0..3).map(|i| spawn_node(i, seeds.clone()))).await;

    // Wait for gossip convergence.
    let mut events = nodes[0].subscribe_events();
    while let Some(evt) = events.next().await {
        if matches!(evt, ServerEvent::PeerUp(_)) && nodes[0].peers().len() == 2 {
            break;
        }
    }

    // Drive a request through node 0; assert it landed on the right primary.
    let req = Msg::request(MsgType::ReqRedisGet, b"some-key");
    let rsp = nodes[0].inject_request(req).await.unwrap();
    assert_eq!(rsp.msg_type(), MsgType::RspRedisBulk);

    for n in nodes { n.shutdown().await.unwrap(); }
}
```

Hooks used: `MockDatastore` (custom `Datastore` from
[Hooks](./hooks.md#datastore)), `MockSeedsProvider` (custom
`SeedsProvider`). Transport stays `TcpTransport` because the dnode
listeners still need a real socket pair to gossip; `inject_request`
bypasses the proxy listener so the client side is socket-free.

## 3. Production embedding: custom Transport (mTLS) and custom MetricsSink (Prometheus)

A real-world shape. The embedding program runs as a sidecar in
front of an in-process B-Tree datastore, accepts client connections
over mutually-authenticated TLS, and exports metrics to a
Prometheus scrape endpoint.

```rust
use std::sync::Arc;
use dynomite::embed::{Server, ServerHandle};
use dynomite::embed::transport::TransportListener;
use dynomite::embed::metrics::MetricsSink;

struct MutualTlsListener { /* tokio_rustls + client cert verifier */ }
struct InMemoryBTreeDatastore { /* Arc<RwLock<BTreeMap<Bytes, Bytes>>> */ }
struct PrometheusMetricsSink { /* prometheus::Registry */ }

// impl TransportListener for MutualTlsListener { ... }
// impl Datastore for InMemoryBTreeDatastore { ... }
// impl MetricsSink for PrometheusMetricsSink { ... }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tls = Arc::new(MutualTlsListener::new("certs/")?);
    let store = Box::new(InMemoryBTreeDatastore::new());
    let prom = Box::new(PrometheusMetricsSink::with_registry(/* ... */));

    let handle: ServerHandle = Server::builder()
        .from_yaml_file("/etc/dynomite/node.yml")?
        .transport_listener(tls)
        .datastore(store)
        .metrics_sink(prom)
        .build()?
        .start();

    let mut events = handle.subscribe_events();
    tokio::spawn(async move {
        while let Some(evt) = events.next().await {
            tracing::info!(?evt, "server event");
        }
    });

    tokio::signal::ctrl_c().await?;
    handle.shutdown().await?;
    Ok(())
}
```

Hooks used:

* `MutualTlsListener` - custom `TransportListener` from
  [Hooks](./hooks.md#transport).
* `InMemoryBTreeDatastore` - custom `Datastore`.
* `PrometheusMetricsSink` - custom `MetricsSink`.

The YAML at `/etc/dynomite/node.yml` carries every other knob
(seeds, consistency, timeouts, rack and datacenter identity). The
typed setters override only the slots that have no YAML form.
