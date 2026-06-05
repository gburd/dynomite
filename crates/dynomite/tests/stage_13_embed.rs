//! Stage 13: embedding-API integration tests.
//!
//! Exercises the public surface in `dynomite::embed`:
//!
//! * 3-node in-process cluster driven via `inject_request` only
//!   (no kernel sockets).
//! * Pluggable `Datastore`, `SeedsProvider`, `MetricsSink` hooks
//!   asserting that the dispatcher / gossip / aggregator paths
//!   call into the supplied implementations.
//! * Real cluster bound on `127.0.0.1:<port>` exchanging gossip
//!   through the public API and converging on
//!   `peers().len() == 3`.
//! * Event stream subscription asserting `PeerUp` /
//!   `GossipRound` semantics.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use dynomite::cluster::peer::PeerState;
use dynomite::conf::{ConfDynSeed, ConfServer, ConsistencyLevel, DataStore};
use dynomite::embed::events::{PeerDownReason, ServerEvent};
use dynomite::embed::hooks::{
    BoxFuture, Datastore, DatastoreError, LoggingMetricsSink, MetricsError, MetricsSink, Protocol,
    SeedsProvider, SimpleSeedsProvider,
};
use dynomite::embed::{Server, ServerBuilder, ServerHandle};
use dynomite::msg::{Msg, MsgType};
use dynomite::seeds::SeedsError;
use dynomite::stats::Snapshot;

// ----- Mocks --------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct MockDatastore {
    inner: Arc<Mutex<MockState>>,
}

#[derive(Debug, Default)]
struct MockState {
    requests: Vec<MsgType>,
    kv: std::collections::HashMap<u64, MsgType>,
}

impl MockDatastore {
    fn new() -> Self {
        Self::default()
    }

    fn count(&self) -> usize {
        self.inner.lock().requests.len()
    }
}

impl Datastore for MockDatastore {
    fn protocol(&self) -> Protocol {
        Protocol::Custom
    }

    fn dispatch(&self, req: Msg) -> BoxFuture<'_, Result<Msg, DatastoreError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut s = inner.lock();
            s.requests.push(req.ty());
            if matches!(req.ty(), MsgType::ReqRedisSet) {
                s.kv.insert(req.id(), MsgType::RspRedisStatus);
            }
            let stored = s.kv.get(&req.id()).copied();
            drop(s);
            let mut rsp = Msg::new(req.id(), stored.unwrap_or(MsgType::RspRedisStatus), false);
            rsp.set_parent_id(req.id());
            Ok(rsp)
        })
    }
}

#[derive(Debug, Clone)]
struct MockSeedsProvider {
    seeds: Vec<ConfDynSeed>,
    fetches: Arc<Mutex<u64>>,
}

impl MockSeedsProvider {
    fn new(seeds: Vec<ConfDynSeed>) -> Self {
        Self {
            seeds,
            fetches: Arc::new(Mutex::new(0)),
        }
    }

    fn fetch_count(&self) -> u64 {
        *self.fetches.lock()
    }
}

impl SeedsProvider for MockSeedsProvider {
    fn fetch(&self) -> Result<Vec<ConfDynSeed>, SeedsError> {
        *self.fetches.lock() += 1;
        Ok(self.seeds.clone())
    }

    fn refresh_interval(&self) -> Duration {
        Duration::from_millis(50)
    }
}

#[derive(Debug, Default, Clone)]
struct MockMetricsSink {
    inner: Arc<Mutex<Vec<String>>>,
}

impl MockMetricsSink {
    fn new() -> Self {
        Self::default()
    }

    fn emit_count(&self) -> usize {
        self.inner.lock().len()
    }
}

impl MetricsSink for MockMetricsSink {
    fn emit<'a>(&'a self, snapshot: &'a Snapshot) -> BoxFuture<'a, Result<(), MetricsError>> {
        let inner = self.inner.clone();
        let pool = snapshot.pool.name.clone();
        Box::pin(async move {
            inner.lock().push(pool);
            Ok(())
        })
    }

    fn flush_interval(&self) -> Duration {
        Duration::from_millis(50)
    }
}

// ----- Helpers ------------------------------------------------------------

struct NodeSpec<'a> {
    pool_name: &'a str,
    rack: &'a str,
    listen: &'a str,
    dyn_listen: &'a str,
    tokens: &'a str,
    seeds: Vec<ConfDynSeed>,
    datastore: Option<Box<dyn Datastore>>,
    seeds_provider: Option<Box<dyn SeedsProvider>>,
    enable_gossip: bool,
}

fn build_node(spec: NodeSpec<'_>) -> Server {
    let mut b = ServerBuilder::new(spec.pool_name)
        .listen(spec.listen.parse().unwrap())
        .dyn_listen(spec.dyn_listen.parse().unwrap())
        .data_store(DataStore::Valkey)
        .servers(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()])
        .datacenter("dc-test")
        .rack(spec.rack)
        .tokens_str(spec.tokens)
        .read_consistency(ConsistencyLevel::DcOne)
        .write_consistency(ConsistencyLevel::DcOne)
        .gossip_interval(Duration::from_millis(80))
        .enable_gossip(spec.enable_gossip)
        .dyn_seeds(spec.seeds)
        .timeout(Duration::from_secs(1));
    if let Some(ds) = spec.datastore {
        b = b.datastore(ds);
    }
    if let Some(sp) = spec.seeds_provider {
        b = b.seeds_provider(sp);
    }
    b.build().unwrap()
}

fn seed(s: &str) -> ConfDynSeed {
    ConfDynSeed::parse(s).unwrap()
}

// ----- Tests --------------------------------------------------------------

#[tokio::test]
async fn three_node_cluster_inject_request_round_trip() {
    let shared_ds = MockDatastore::new();

    let entries = [
        "127.0.0.1:18301:rA:dc-test:101134286",
        "127.0.0.1:18302:rB:dc-test:1431655765",
        "127.0.0.1:18303:rC:dc-test:2863311530",
    ];
    let seeds_for = |skip: usize| -> Vec<ConfDynSeed> {
        entries
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != skip)
            .map(|(_, s)| seed(s))
            .collect()
    };

    let n0 = build_node(NodeSpec {
        pool_name: "p",
        rack: "rA",
        listen: "127.0.0.1:18001",
        dyn_listen: "127.0.0.1:18301",
        tokens: "101134286",
        seeds: seeds_for(0),
        datastore: Some(Box::new(shared_ds.clone())),
        seeds_provider: None,
        enable_gossip: false,
    });
    let n1 = build_node(NodeSpec {
        pool_name: "p",
        rack: "rB",
        listen: "127.0.0.1:18002",
        dyn_listen: "127.0.0.1:18302",
        tokens: "1431655765",
        seeds: seeds_for(1),
        datastore: Some(Box::new(shared_ds.clone())),
        seeds_provider: None,
        enable_gossip: false,
    });
    let n2 = build_node(NodeSpec {
        pool_name: "p",
        rack: "rC",
        listen: "127.0.0.1:18003",
        dyn_listen: "127.0.0.1:18303",
        tokens: "2863311530",
        seeds: seeds_for(2),
        datastore: Some(Box::new(shared_ds.clone())),
        seeds_provider: None,
        enable_gossip: false,
    });

    let h0 = n0.start().await.unwrap();
    let h1 = n1.start().await.unwrap();
    let h2 = n2.start().await.unwrap();

    // Write through node 0.
    let mut req = Msg::new(42, MsgType::ReqRedisSet, true);
    req.set_parent_id(0);
    let rsp = h0.inject_request(req).await.unwrap();
    assert_eq!(rsp.parent_id(), 42);

    // Read from node 1 and node 2 (same id -> same shared kv slot).
    let req = Msg::new(42, MsgType::ReqRedisGet, true);
    let rsp = h1.inject_request(req).await.unwrap();
    assert_eq!(rsp.parent_id(), 42);
    assert_eq!(rsp.ty(), MsgType::RspRedisStatus);

    let req = Msg::new(42, MsgType::ReqRedisGet, true);
    let rsp = h2.inject_request(req).await.unwrap();
    assert_eq!(rsp.parent_id(), 42);

    assert!(shared_ds.count() >= 3);

    h0.shutdown().await.unwrap();
    h1.shutdown().await.unwrap();
    h2.shutdown().await.unwrap();
}

#[tokio::test]
async fn mock_datastore_records_dispatcher_fanout() {
    let ds = MockDatastore::new();
    let server = build_node(NodeSpec {
        pool_name: "p",
        rack: "rA",
        listen: "127.0.0.1:18011",
        dyn_listen: "127.0.0.1:18311",
        tokens: "0",
        seeds: Vec::new(),
        datastore: Some(Box::new(ds.clone())),
        seeds_provider: None,
        enable_gossip: false,
    });
    let h = server.start().await.unwrap();
    for i in 0..5u64 {
        let req = Msg::new(i, MsgType::ReqRedisGet, true);
        let _ = h.inject_request(req).await.unwrap();
    }
    h.shutdown().await.unwrap();
    assert!(
        ds.count() >= 5,
        "expected >=5 dispatch calls, got {}",
        ds.count()
    );
}

#[tokio::test]
async fn mock_seeds_provider_drives_gossip_convergence() {
    let seeds = vec![
        seed("127.0.0.1:18331:rB:dc-test:1431655765"),
        seed("127.0.0.1:18332:rC:dc-test:2863311530"),
    ];
    let mock = MockSeedsProvider::new(seeds);
    let mock_clone = mock.clone();
    let server = build_node(NodeSpec {
        pool_name: "p",
        rack: "rA",
        listen: "127.0.0.1:18021",
        dyn_listen: "127.0.0.1:18321",
        tokens: "101134286",
        seeds: Vec::new(),
        datastore: None,
        seeds_provider: Some(Box::new(mock)),
        enable_gossip: true,
    });
    let h = server.start().await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        if h.peers().len() >= 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert!(h.peers().len() >= 3, "got {}", h.peers().len());
    assert!(
        mock_clone.fetch_count() >= 1,
        "seeds provider was never polled"
    );
    h.shutdown().await.unwrap();
}

#[tokio::test]
async fn mock_metrics_sink_observes_aggregator() {
    let sink = MockMetricsSink::new();
    let sink_clone = sink.clone();
    let b = ServerBuilder::new("p")
        .listen("127.0.0.1:18031".parse().unwrap())
        .dyn_listen("127.0.0.1:18331".parse().unwrap())
        .data_store(DataStore::Valkey)
        .servers(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()])
        .datacenter("dc-test")
        .rack("rA")
        .tokens_str("0")
        .stats_interval(Duration::from_millis(40))
        .timeout(Duration::from_secs(1))
        .metrics_sink(Box::new(sink));
    let h = b.build().unwrap().start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    h.shutdown().await.unwrap();
    assert!(
        sink_clone.emit_count() >= 1,
        "sink never received a snapshot"
    );
}

#[tokio::test]
async fn three_node_cluster_real_bind_converges() {
    let n0 = build_node(NodeSpec {
        pool_name: "p",
        rack: "rA",
        listen: "127.0.0.1:18041",
        dyn_listen: "127.0.0.1:18341",
        tokens: "101134286",
        seeds: vec![],
        datastore: None,
        seeds_provider: Some(Box::new(SimpleSeedsProvider::new(vec![
            seed("127.0.0.1:18342:rB:dc-test:1431655765"),
            seed("127.0.0.1:18343:rC:dc-test:2863311530"),
        ]))),
        enable_gossip: true,
    });
    let n1 = build_node(NodeSpec {
        pool_name: "p",
        rack: "rB",
        listen: "127.0.0.1:18042",
        dyn_listen: "127.0.0.1:18342",
        tokens: "1431655765",
        seeds: vec![],
        datastore: None,
        seeds_provider: Some(Box::new(SimpleSeedsProvider::new(vec![
            seed("127.0.0.1:18341:rA:dc-test:101134286"),
            seed("127.0.0.1:18343:rC:dc-test:2863311530"),
        ]))),
        enable_gossip: true,
    });
    let n2 = build_node(NodeSpec {
        pool_name: "p",
        rack: "rC",
        listen: "127.0.0.1:18043",
        dyn_listen: "127.0.0.1:18343",
        tokens: "2863311530",
        seeds: vec![],
        datastore: None,
        seeds_provider: Some(Box::new(SimpleSeedsProvider::new(vec![
            seed("127.0.0.1:18341:rA:dc-test:101134286"),
            seed("127.0.0.1:18342:rB:dc-test:1431655765"),
        ]))),
        enable_gossip: true,
    });

    let h0 = n0.start().await.unwrap();
    let h1 = n1.start().await.unwrap();
    let h2 = n2.start().await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        if h0.peers().len() == 3 && h1.peers().len() == 3 && h2.peers().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    for h in [&h0, &h1, &h2] {
        assert_eq!(
            h.peers().len(),
            3,
            "node never converged: {}",
            h.peers().len()
        );
    }

    assert!(h0.listen_addr().is_some());
    assert!(h0.dyn_listen_addr().is_some());

    h0.shutdown().await.unwrap();
    h1.shutdown().await.unwrap();
    h2.shutdown().await.unwrap();
}

#[tokio::test]
async fn event_stream_observes_peer_up_and_gossip_rounds() {
    let seeds = vec![seed("127.0.0.1:18351:rB:dc-test:1431655765")];
    let server = build_node(NodeSpec {
        pool_name: "p",
        rack: "rA",
        listen: "127.0.0.1:18051",
        dyn_listen: "127.0.0.1:18353",
        tokens: "101134286",
        seeds: Vec::new(),
        datastore: None,
        seeds_provider: Some(Box::new(SimpleSeedsProvider::new(seeds))),
        enable_gossip: true,
    });
    let h = server.start().await.unwrap();
    let mut stream = h.subscribe_events();

    let mut saw_round = false;
    let mut saw_peer_up = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline && !(saw_round && saw_peer_up) {
        if let Some(evt) = tokio::time::timeout(Duration::from_millis(200), stream.recv())
            .await
            .ok()
            .flatten()
        {
            match evt {
                ServerEvent::GossipRound { .. } => saw_round = true,
                ServerEvent::PeerUp(_) => saw_peer_up = true,
                _ => {}
            }
        }
    }
    assert!(saw_round, "no GossipRound event seen");
    assert!(saw_peer_up, "no PeerUp event seen");
    h.shutdown().await.unwrap();
}

#[tokio::test]
async fn handle_reload_emits_config_reloaded_event() {
    let server = build_node(NodeSpec {
        pool_name: "p",
        rack: "rA",
        listen: "127.0.0.1:18061",
        dyn_listen: "127.0.0.1:18361",
        tokens: "0",
        seeds: Vec::new(),
        datastore: None,
        seeds_provider: None,
        enable_gossip: false,
    });
    let h = server.start().await.unwrap();
    let mut stream = h.subscribe_events();

    let yaml = "p:\n  listen: 127.0.0.1:18061\n  dyn_listen: 127.0.0.1:18361\n  tokens: '0'\n  servers:\n  - 127.0.0.1:6379:1\n  data_store: 0\n";
    let cfg = dynomite::conf::Config::parse_str(yaml).unwrap();
    h.reload(cfg).await.unwrap();

    let evt = tokio::time::timeout(Duration::from_secs(1), stream.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(evt, ServerEvent::ConfigReloaded { .. }));
    h.shutdown().await.unwrap();
}

#[test]
fn peer_state_and_down_reason_constants_are_public() {
    assert_eq!(PeerState::Normal.name(), "NORMAL");
    assert!(matches!(
        PeerDownReason::FailureDetector,
        PeerDownReason::FailureDetector
    ));
}

#[tokio::test]
async fn logging_metrics_sink_emits_without_panic() {
    let sink = LoggingMetricsSink::new("test");
    let snap = Snapshot::default();
    sink.emit(&snap).await.unwrap();
    assert_eq!(sink.flush_count(), 1);
}

fn assert_handle_traits<T: Clone + Send + Sync + 'static>(_: &T) {}

#[tokio::test]
async fn server_handle_is_clone_send_sync() {
    let server = build_node(NodeSpec {
        pool_name: "p",
        rack: "rA",
        listen: "127.0.0.1:18071",
        dyn_listen: "127.0.0.1:18371",
        tokens: "0",
        seeds: Vec::new(),
        datastore: None,
        seeds_provider: None,
        enable_gossip: false,
    });
    let h: ServerHandle = server.start().await.unwrap();
    assert_handle_traits(&h);
    let cloned = h.clone();
    drop(cloned);
    h.shutdown().await.unwrap();
}
