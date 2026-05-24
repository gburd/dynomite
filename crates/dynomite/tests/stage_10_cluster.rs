//! Stage 10 integration tests: cluster pools, peers, gossip,
//! seeds providers, and the cluster-aware
//! [`Dispatcher`](dynomite::net::Dispatcher).

use std::sync::Arc;

use dynomite::cluster::dispatch::{ClusterDispatcher, DispatchPlan, ReplicaTarget};
use dynomite::cluster::gossip::{
    parse_seed_blob, parse_seed_node, GossipNode, GossipState, GossipStep,
};
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::cluster::snitch::{pick_target_rack, rack_distance, RackDistance};
use dynomite::cluster::vnode;
use dynomite::conf::{ConfDynSeed, DataStore, HashType};
use dynomite::hashkit::DynToken;
use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
use dynomite::net::auto_eject::{AutoEject, AutoEjectState};
use dynomite::seeds::dns::{DnsSeedsProvider, ResolvedSeeds, Resolver};
use dynomite::seeds::florida::FloridaSeedsProvider;
use dynomite::seeds::simple::SimpleSeedsProvider;
use dynomite::seeds::SeedsProvider;

fn cfg(dc: &str, rack: &str, read: ConsistencyLevel, write: ConsistencyLevel) -> PoolConfig {
    PoolConfig {
        name: "p".into(),
        dc: dc.into(),
        rack: rack.into(),
        data_store: DataStore::Redis,
        hash: HashType::Murmur,
        read_consistency: read,
        write_consistency: write,
        timeout_ms: 5_000,
        server_retry_timeout_ms: 30_000,
        server_failure_limit: 2,
        auto_eject_hosts: false,
        enable_gossip: false,
        bucket_types: Vec::new(),
        default_bucket_type: None,
    }
}

fn peer_at(idx: u32, dc: &str, rack: &str, tok: u32, is_local: bool, is_same_dc: bool) -> Peer {
    let mut p = Peer::new(
        idx,
        PeerEndpoint::tcp(format!("10.0.{idx}.1"), 8101),
        rack.into(),
        dc.into(),
        vec![DynToken::from_u32(tok)],
        is_local,
        is_same_dc,
        false,
    );
    p.set_state(PeerState::Normal, 0);
    p
}

#[test]
fn three_node_single_dc_routing_matches_ring_math() {
    // 3 peers, single DC, single rack, distinct tokens.
    let peers = vec![
        peer_at(0, "dc1", "r1", 1_000, true, true),
        peer_at(1, "dc1", "r1", 2_000, false, true),
        peer_at(2, "dc1", "r1", 3_000, false, true),
    ];
    let pool = ServerPool::new(
        cfg(
            "dc1",
            "r1",
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
        ),
        peers,
    );
    let dcs = pool.datacenters().read();
    let rack = &dcs[0].racks()[0];
    // For a corpus of 1000 keys, every key should resolve via
    // vnode_dispatch and land on one of the three peers.
    for i in 0..1000u32 {
        let key = format!("key-{i}");
        let token = dynomite::hashkit::hash(dynomite::hashkit::HashType::Murmur, key.as_bytes());
        let idx = vnode::dispatch(rack.continuums(), &token).unwrap();
        assert!(idx < 3);
    }
}

#[test]
fn dispatch_dc_one_picks_local_rack_when_present() {
    let peers = vec![
        peer_at(0, "dc1", "rA", 1_000, true, true),
        peer_at(1, "dc1", "rB", 2_000, false, true),
        peer_at(2, "dc2", "rA", 3_000, false, false),
    ];
    let pool = Arc::new(ServerPool::new(
        cfg(
            "dc1",
            "rA",
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
        ),
        peers,
    ));
    let disp = ClusterDispatcher::new(pool);
    let req = Msg::new(1, MsgType::ReqRedisGet, true);
    // Local rack peer is the local node, so the plan must short-circuit to LocalDatastore.
    let plan = disp.plan(&req, b"x");
    assert!(matches!(plan, DispatchPlan::LocalDatastore));
}

#[test]
fn dispatch_dc_quorum_fanout_local_dc_only() {
    let peers = vec![
        peer_at(0, "dc1", "rA", 1_000, true, true),
        peer_at(1, "dc1", "rB", 2_000, false, true),
        peer_at(2, "dc2", "rA", 3_000, false, false),
        peer_at(3, "dc2", "rB", 4_000, false, false),
    ];
    let pool = Arc::new(ServerPool::new(
        cfg(
            "dc1",
            "rA",
            ConsistencyLevel::DcQuorum,
            ConsistencyLevel::DcQuorum,
        ),
        peers,
    ));
    let disp = ClusterDispatcher::new(pool);
    let req = Msg::new(1, MsgType::ReqRedisGet, true);
    let plan = disp.plan(&req, b"x");
    match plan {
        DispatchPlan::Replicas { targets: rs, .. } => {
            assert_eq!(rs.len(), 2);
            for r in &rs {
                assert_eq!(r.dc, "dc1");
            }
        }
        other => panic!("expected Replicas, got {other:?}"),
    }
}

#[test]
fn dispatch_dc_safe_quorum_includes_local_dc_peers() {
    let peers = vec![
        peer_at(0, "dc1", "rA", 1_000, true, true),
        peer_at(1, "dc1", "rB", 2_000, false, true),
        peer_at(2, "dc2", "rA", 3_000, false, false),
    ];
    let pool = Arc::new(ServerPool::new(
        cfg(
            "dc1",
            "rA",
            ConsistencyLevel::DcSafeQuorum,
            ConsistencyLevel::DcSafeQuorum,
        ),
        peers,
    ));
    let disp = ClusterDispatcher::new(pool);
    let req = Msg::new(1, MsgType::ReqRedisGet, true);
    let plan = disp.plan(&req, b"x");
    match plan {
        DispatchPlan::Replicas { targets: rs, .. } => {
            assert!(rs.iter().all(|r: &ReplicaTarget| r.dc == "dc1"));
        }
        DispatchPlan::LocalDatastore => {}
        other => panic!("unexpected plan: {other:?}"),
    }
}

#[test]
fn dispatch_dc_each_safe_quorum_fans_out_per_dc() {
    let peers = vec![
        peer_at(0, "dc1", "rA", 1_000, true, true),
        peer_at(1, "dc2", "rA", 2_000, false, false),
    ];
    let pool = Arc::new(ServerPool::new(
        cfg(
            "dc1",
            "rA",
            ConsistencyLevel::DcEachSafeQuorum,
            ConsistencyLevel::DcEachSafeQuorum,
        ),
        peers,
    ));
    let disp = ClusterDispatcher::new(pool);
    let req = Msg::new(1, MsgType::ReqRedisGet, true);
    let plan = disp.plan(&req, b"x");
    let dcs: Vec<String> = match plan {
        DispatchPlan::Replicas { targets: rs, .. } => rs.into_iter().map(|r| r.dc).collect(),
        DispatchPlan::LocalDatastore => vec!["dc1".into()],
        other => panic!("unexpected: {other:?}"),
    };
    assert!(dcs.iter().any(|d| d == "dc2"));
}

#[test]
fn snitch_pick_target_rack_prefers_local_rack() {
    let cands = [("dc1", "rA"), ("dc1", "rB"), ("dc2", "rA")];
    assert_eq!(pick_target_rack("dc1", "rA", &cands), Some(("dc1", "rA")));
    assert_eq!(pick_target_rack("dc1", "rZ", &cands), Some(("dc1", "rA")));
    assert_eq!(
        rack_distance("dc1", "rA", "dc2", "rZ"),
        RackDistance::Remote
    );
}

#[test]
fn gossip_converges_within_n_rounds() {
    let mut state = GossipState::new();
    let seeds = parse_seed_blob(
        "10.0.0.1:8101:rA:dc1:1000|10.0.0.2:8101:rA:dc1:2000|10.0.0.3:8101:rA:dc1:3000",
    )
    .unwrap();
    // Round 1: import every seed.
    for s in &seeds {
        let n = GossipNode {
            dc: s.dc.clone(),
            rack: s.rack.clone(),
            host: s.host.clone(),
            port: s.port,
            tokens: s.tokens.clone(),
            state: PeerState::Normal,
            ts_secs: 1,
            is_local: false,
        };
        assert_eq!(state.add_or_update(n), GossipStep::Added);
    }
    // Round 2: same seeds with stale ts -> Unchanged.
    for s in &seeds {
        let n = GossipNode {
            dc: s.dc.clone(),
            rack: s.rack.clone(),
            host: s.host.clone(),
            port: s.port,
            tokens: s.tokens.clone(),
            state: PeerState::Normal,
            ts_secs: 1,
            is_local: false,
        };
        assert_eq!(state.add_or_update(n), GossipStep::Unchanged);
    }
    // Round 3: new state with newer ts -> StateChanged.
    for s in &seeds {
        let n = GossipNode {
            dc: s.dc.clone(),
            rack: s.rack.clone(),
            host: s.host.clone(),
            port: s.port,
            tokens: s.tokens.clone(),
            state: PeerState::Down,
            ts_secs: 5,
            is_local: false,
        };
        assert_eq!(state.add_or_update(n), GossipStep::StateChanged);
    }
    assert_eq!(state.node_count(), 3);
    // Failure detector ages stale entries: with delta = 40s for
    // interval 1000ms, now=200 should keep them in sync.
    state.run_failure_detector(200, 1000);
    assert!(state.nodes().all(|n| n.state == PeerState::Down));
}

#[test]
fn gossip_seed_node_parser_round_trips() {
    let r = parse_seed_node("h:1:r:d:7,8,9").unwrap();
    assert_eq!(r.tokens.len(), 3);
    assert!(parse_seed_node("h:1:r:d").is_err());
    assert!(parse_seed_node("h:0:r:d:1").is_err());
}

#[test]
fn auto_eject_under_simulated_failures() {
    use std::time::{Duration, Instant};

    let mut ae = AutoEject::new(true, 3, Duration::from_millis(50));
    let now = Instant::now();
    assert_eq!(ae.record_attempt(now), AutoEjectState::Reachable);
    assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
    assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
    assert_eq!(ae.record_failure(now), AutoEjectState::Ejected);
    assert_eq!(ae.record_attempt(now), AutoEjectState::Ejected);
    let later = now + Duration::from_millis(60);
    assert_eq!(ae.record_attempt(later), AutoEjectState::Reachable);
    ae.record_success(later);
    assert_eq!(ae.failure_count(), 0);
}

#[test]
fn simple_seeds_round_trip() {
    let seeds = vec![
        ConfDynSeed::parse("10.0.0.1:8101:rA:dc1:1").unwrap(),
        ConfDynSeed::parse("10.0.0.2:8101:rA:dc1:2").unwrap(),
    ];
    let p = SimpleSeedsProvider::new(seeds);
    let v = p.get_seeds().unwrap();
    assert_eq!(v.len(), 2);
    assert_eq!(v[0].dc(), "dc1");
}

struct LoopbackResolver;
impl Resolver for LoopbackResolver {
    fn resolve(&self, _name: &str) -> Result<ResolvedSeeds, dynomite::seeds::SeedsError> {
        Ok(ResolvedSeeds::A {
            ips: vec!["127.0.0.1".into(), "127.0.0.2".into()],
            port: 8101,
            rack: "rA".into(),
            dc: "dc1".into(),
            tokens: "1".into(),
        })
    }
}

#[test]
fn dns_seeds_synthesises_loopback() {
    let p = DnsSeedsProvider::new("_dynomite.test".into(), Box::new(LoopbackResolver));
    let v = p.get_seeds().unwrap();
    assert_eq!(v.len(), 2);
    for s in &v {
        assert_eq!(s.port(), 8101);
    }
}

#[tokio::test]
async fn florida_seeds_via_canned_listener() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let mut tmp = [0u8; 1024];
        let _ = s.read(&mut tmp).await;
        let body = b"10.0.0.5:8101:rA:dc1:42";
        let header = b"HTTP/1.0 200 OK\r\nContent-Type: text/plain\r\n\r\n";
        let _ = s.write_all(header).await;
        let _ = s.write_all(body).await;
        let _ = s.shutdown().await;
    });
    let p = FloridaSeedsProvider::new("127.0.0.1".into(), port);
    let v = p.fetch().await.unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].host(), "10.0.0.5");
    assert_eq!(v[0].port(), 8101);
    assert_eq!(v[0].dc(), "dc1");
}

#[test]
fn pool_init_response_mgrs_one_per_dc() {
    let peers = vec![
        peer_at(0, "dc1", "r1", 10, true, true),
        peer_at(1, "dc2", "r1", 20, false, false),
        peer_at(2, "dc3", "r1", 30, false, false),
    ];
    let pool = ServerPool::new(
        cfg(
            "dc1",
            "r1",
            ConsistencyLevel::DcEachSafeQuorum,
            ConsistencyLevel::DcEachSafeQuorum,
        ),
        peers,
    );
    let req = Msg::new(1, MsgType::ReqRedisGet, true);
    let mgrs = pool.init_response_mgrs(&req);
    assert_eq!(mgrs.len(), 3);
}

#[test]
fn preselect_remote_rack_is_deterministic() {
    let peers = vec![
        peer_at(0, "dc1", "rA", 1, true, true),
        peer_at(1, "dc1", "rB", 2, false, true),
        peer_at(2, "dc2", "rA", 3, false, false),
        peer_at(3, "dc2", "rB", 4, false, false),
    ];
    let pool = ServerPool::new(
        cfg(
            "dc1",
            "rB",
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
        ),
        peers,
    );
    pool.preselect_remote_racks();
    let topology = pool.datacenters().read();
    let dc2 = topology.iter().find(|d| d.name() == "dc2").unwrap();
    // Local rack rB sorts to index 1; dc2 has 2 racks: 1 % 2 = 1 -> rB.
    assert_eq!(
        dc2.preselected_rack().map(dynomite::cluster::Rack::name),
        Some("rB"),
    );
}

#[test]
fn down_peer_is_skipped_by_dispatcher() {
    let mut p0 = peer_at(0, "dc1", "rA", 10, true, true);
    p0.set_state(PeerState::Down, 0);
    let pool = Arc::new(ServerPool::new(
        cfg(
            "dc1",
            "rA",
            ConsistencyLevel::DcQuorum,
            ConsistencyLevel::DcQuorum,
        ),
        vec![p0],
    ));
    let disp = ClusterDispatcher::new(pool);
    let req = Msg::new(1, MsgType::ReqRedisGet, true);
    let plan = disp.plan(&req, b"k");
    assert_eq!(plan, DispatchPlan::NoTargets);
}
