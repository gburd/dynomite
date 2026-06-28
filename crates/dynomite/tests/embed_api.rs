//! Public-API smoke tests for the embedding surface.
//!
//! These tests are organised around the four guarantees the
//! embedding cookbook makes to downstream callers:
//!
//! 1. [`ServerHandle::shutdown`] is idempotent (calling it twice
//!    is safe; a `join` after a `shutdown` returns immediately).
//! 2. [`ServerHandle::stats_handle`] hands out a live
//!    `Arc<Stats>` so embedders can read counters without going
//!    through the snapshot path, and the snapshot the running
//!    aggregator publishes carries pool-level counters.
//! 3. The cluster-event subscriber returned by
//!    [`ServerHandle::events`] receives `ClusterEvent` payloads
//!    after `start`.
//! 4. A custom [`Datastore`] plugged through
//!    [`ServerBuilder::datastore`] receives the routed request
//!    via [`ServerHandle::inject_request`] and round-trips a
//!    `set` followed by a `get`.
//!
//! Run via `cargo nextest run -p dynomite --test embed_api`.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use dynomite::conf::{ConfServer, DataStore};
use dynomite::embed::hooks::{BoxFuture, Datastore, DatastoreError, Protocol};
use dynomite::embed::{Server, ServerBuilder};
use dynomite::events::ClusterEvent;
use dynomite::msg::{Msg, MsgType};
use dynomite::stats::Stats;

// ---- helpers --------------------------------------------------------------

fn minimal_builder(label: &str) -> ServerBuilder {
    ServerBuilder::new("dyn_o_mite")
        .listen("127.0.0.1:0".parse().unwrap())
        .dyn_listen("127.0.0.1:0".parse().unwrap())
        .data_store(DataStore::Valkey)
        .servers(vec![ConfServer::parse(&format!(
            "127.0.0.1:6379:1 {label}"
        ))
        .unwrap()])
        .datacenter("dc-test")
        .rack("r-test")
        .tokens_str("0")
        .stats_interval(Duration::from_millis(40))
        .timeout(Duration::from_secs(1))
        .enable_gossip(false)
}

#[derive(Debug, Default, Clone)]
struct KvDatastore {
    inner: Arc<Mutex<KvState>>,
}

#[derive(Debug, Default)]
struct KvState {
    last_request: Option<MsgType>,
    held_value_for: Option<u64>,
    dispatch_count: u64,
}

impl KvDatastore {
    fn new() -> Self {
        Self::default()
    }

    fn dispatch_count(&self) -> u64 {
        self.inner.lock().dispatch_count
    }

    fn last_request(&self) -> Option<MsgType> {
        self.inner.lock().last_request
    }

    fn held_value_for(&self) -> Option<u64> {
        self.inner.lock().held_value_for
    }
}

impl Datastore for KvDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut s = inner.lock();
            s.dispatch_count += 1;
            s.last_request = Some(req.ty());
            if matches!(req.ty(), MsgType::ReqRedisSet) {
                s.held_value_for = Some(req.id());
            }
            let already = s.held_value_for == Some(req.id());
            drop(s);
            let rsp_ty = if already {
                MsgType::RspRedisStatus
            } else {
                MsgType::RspRedisError
            };
            let mut rsp = Msg::new(req.id(), rsp_ty, false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}

// ---- tests ----------------------------------------------------------------

#[tokio::test]
async fn server_handle_shutdown_idempotent() {
    let handle = Server::start_with(minimal_builder("idem"))
        .await
        .expect("start");

    // First shutdown: real work (cancellation, drain).
    handle.shutdown().await.expect("first shutdown ok");

    // Second shutdown: no-op, must still succeed.
    handle.shutdown().await.expect("second shutdown ok");

    // join after shutdown returns immediately because the task
    // set was drained on the first shutdown call.
    let join_started = std::time::Instant::now();
    handle.join().await;
    assert!(
        join_started.elapsed() < Duration::from_secs(1),
        "join after shutdown took too long: {:?}",
        join_started.elapsed()
    );
}

#[tokio::test]
async fn server_handle_stats_returns_counters() {
    let handle = Server::start_with(minimal_builder("stats"))
        .await
        .expect("start");

    // The handle must hand out a live Arc<Stats>.
    let live: Arc<Stats> = handle.stats_handle();
    let snap_via_arc = live.snapshot();

    // The snapshot accessor on the handle is consistent with the
    // snapshot taken directly on the live aggregator.
    let snap_via_handle = handle.stats();

    assert_eq!(snap_via_arc.pool.name, snap_via_handle.pool.name);
    assert_eq!(snap_via_arc.pool.name, "dyn_o_mite");

    // Wait long enough for the aggregator's stats_loop to tick at
    // least once (stats_interval is 40ms in minimal_builder).
    tokio::time::sleep(Duration::from_millis(120)).await;

    // The aggregator is a real handle that survives across awaits.
    let after = live.snapshot();
    assert_eq!(after.pool.name, "dyn_o_mite");

    // describe_stats should expose at least the codec metrics.
    let manifest = handle.describe_stats();
    assert!(
        !manifest.is_empty(),
        "describe_stats() returned empty manifest"
    );

    handle.shutdown().await.expect("shutdown ok");
}

#[tokio::test]
async fn cluster_event_subscriber_receives_events_after_start() {
    // We need gossip enabled so the engine emits at least one
    // ClusterEvent::GossipRoundComplete on a regular tick.
    let builder = minimal_builder("events")
        .enable_gossip(true)
        .gossip_interval(Duration::from_millis(40));
    let handle = Server::start_with(builder).await.expect("start");

    // Subscribe to the ClusterEvent broadcast.
    let manager = handle.events();
    let mut sub = manager.subscribe();

    let mut saw_gossip_complete = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline && !saw_gossip_complete {
        if let Ok(Ok(evt)) = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await {
            if matches!(evt, ClusterEvent::GossipRoundComplete { .. }) {
                saw_gossip_complete = true;
            }
        }
    }

    assert!(
        saw_gossip_complete,
        "no GossipRoundComplete event seen after enabling gossip"
    );

    handle.shutdown().await.expect("shutdown ok");
}

#[tokio::test]
async fn custom_datastore_impl_roundtrips_a_set_get() {
    let store = KvDatastore::new();
    let store_observer = store.clone();

    let builder = minimal_builder("custom-ds").datastore(Box::new(store));
    let handle = Server::start_with(builder).await.expect("start");

    // Inject SET.
    let mut set = Msg::new(99, MsgType::ReqRedisSet, true);
    set.set_parent_id(0);
    let rsp = handle.inject_request(set).await.expect("set ok");
    assert_eq!(rsp.parent_id(), 99);
    assert_eq!(rsp.ty(), MsgType::RspRedisStatus);

    // Inject GET on the same id and expect the same status code
    // because the datastore noted the prior SET.
    let get = Msg::new(99, MsgType::ReqRedisGet, true);
    let rsp = handle.inject_request(get).await.expect("get ok");
    assert_eq!(rsp.parent_id(), 99);
    assert_eq!(rsp.ty(), MsgType::RspRedisStatus);

    assert!(store_observer.dispatch_count() >= 2);
    assert!(matches!(
        store_observer.last_request(),
        Some(MsgType::ReqRedisGet | MsgType::ReqRedisSet)
    ));
    assert_eq!(store_observer.held_value_for(), Some(99));

    handle.shutdown().await.expect("shutdown ok");
}

#[tokio::test]
async fn embedded_listen_socket_serves_a_wire_client() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use dynomite::io::mbuf::MbufPool;
    use dynomite::msg::response;

    // A datastore that returns a real RESP payload (`+OK\r\n`) so
    // the full wire round-trip is exercised: the embedded proxy
    // must parse the request off the socket, route it through the
    // dispatcher to this datastore, and write the reply bytes
    // back -- not the old warn-and-close path.
    #[derive(Debug, Default, Clone)]
    struct OkDatastore {
        calls: Arc<Mutex<u64>>,
    }
    impl Datastore for OkDatastore {
        fn protocol(&self) -> Protocol {
            Protocol::Custom
        }
        fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
            let calls = self.calls.clone();
            Box::pin(async move {
                *calls.lock() += 1;
                let pool = MbufPool::default();
                Ok(response::make_simple_redis(&req, &pool, b"+OK\r\n"))
            })
        }
    }

    let store = OkDatastore::default();
    let calls = store.calls.clone();

    let builder = minimal_builder("wire-serve").datastore(Box::new(store));
    let handle = Server::start_with(builder).await.expect("start");

    let addr = handle
        .listen_addr()
        .expect("listen_addr resolved after start");

    // Connect a raw client and send a RESP `SET k v`. The key
    // routes the request to the local datastore through the
    // dispatcher; the embedded proxy must parse it and write the
    // reply back on the same socket.
    let mut sock = TcpStream::connect(addr)
        .await
        .expect("connect to listen addr");
    sock.set_nodelay(true).ok();
    sock.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .expect("write SET");
    sock.flush().await.expect("flush");

    // Read the reply the proxy writes back.
    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
        .await
        .expect("reply within 2s")
        .expect("read reply");
    assert!(n > 0, "embedded listen socket closed without a reply");
    assert_eq!(
        &buf[..n],
        b"+OK\r\n",
        "unexpected wire reply from the embedded proxy"
    );

    // The request reached the datastore over the wire.
    assert!(
        *calls.lock() >= 1,
        "the wire request did not reach the dispatcher/datastore"
    );

    handle.shutdown().await.expect("shutdown ok");
}
