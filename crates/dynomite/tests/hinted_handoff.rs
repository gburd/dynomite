//! Hinted-handoff integration tests.
//!
//! Three-replica in-process cluster under `DC_QUORUM`. One
//! replica starts in [`PeerState::Down`] so the dispatcher
//! routes the SET to a hint instead of the wire. Bringing the
//! replica back to [`PeerState::Normal`] and running the
//! drainer once causes the hint to be delivered.

use std::sync::Arc;
use std::time::Duration;

use dynomite::cluster::dispatch::ClusterDispatcher;
use dynomite::cluster::hints::HintStore;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::hashkit::DynToken;
use dynomite::io::mbuf::MbufPool;
use dynomite::msg::keypos::KeyPos;
use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
use dynomite::net::dispatcher::{Dispatcher, OutboundEnvelope};
use dynomite::net::server::OutboundRequest;
use dynomite::proto::dnode::DmsgType;

use tokio::sync::mpsc;

/// Spawn one fake replica that records every observed request
/// onto `recorded` and replies with `+OK\r\n` for SET / DEL.
fn spawn_fake_replica(
    peer_idx: u32,
    rx: mpsc::Receiver<OutboundRequest>,
    recorded: mpsc::Sender<(u32, DmsgType, Vec<u8>)>,
) -> tokio::task::JoinHandle<()> {
    let mut rx = rx;
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let _ = recorded.send((peer_idx, req.ty, req.bytes.clone())).await;
            let pool = MbufPool::default();
            let mut rsp = Msg::new(req.req_id, MsgType::RspRedisStatus, false);
            rsp.set_parent_id(req.req_id);
            let mut buf = pool.get();
            buf.recv(b"+OK\r\n");
            rsp.mbufs_mut().push_back(buf);
            rsp.recompute_mlen();
            let env = OutboundEnvelope {
                req_id: req.req_id,
                rsp,
                span: req.span,
                source_peer_idx: req.target_peer_idx,
            };
            let _ = req.responder.send(env).await;
        }
    })
}

fn build_peer(idx: u32, rack: &str, tok: u32, state: PeerState) -> Peer {
    let mut p = Peer::new(
        idx,
        PeerEndpoint::tcp(
            format!("10.0.0.{idx}"),
            8101 + u16::try_from(idx).unwrap_or(0),
        ),
        rack.into(),
        "dc1".into(),
        vec![DynToken::from_u32(tok)],
        false,
        true,
        false,
    );
    p.set_state(state, 0);
    p
}

fn pool(handoff: bool, peer_states: [PeerState; 3]) -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "rA".into(),
        read_consistency: ConsistencyLevel::DcQuorum,
        write_consistency: ConsistencyLevel::DcQuorum,
        enable_hinted_handoff: handoff,
        hint_ttl_seconds: 3_600,
        hint_store_max_bytes: 1024 * 1024,
        hint_drain_interval_ms: 1_000,
        ..PoolConfig::default()
    };
    let peers = vec![
        build_peer(0, "rA", 1_000, peer_states[0]),
        build_peer(1, "rB", 2_000, peer_states[1]),
        build_peer(2, "rC", 3_000, peer_states[2]),
    ];
    let p = ServerPool::new(cfg, peers);
    p.preselect_remote_racks();
    Arc::new(p)
}

fn build_set_req(req_id: u64, key: &[u8], value: &[u8]) -> Msg {
    let pool = MbufPool::default();
    let mut req = Msg::new(req_id, MsgType::ReqRedisSet, true);
    req.flags_mut().is_read = false;
    req.push_key(KeyPos::without_tag(key.to_vec()));
    let mut wire = Vec::new();
    wire.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$");
    wire.extend_from_slice(key.len().to_string().as_bytes());
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(key);
    wire.extend_from_slice(b"\r\n$");
    wire.extend_from_slice(value.len().to_string().as_bytes());
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(value);
    wire.extend_from_slice(b"\r\n");
    let mut mb = pool.get();
    mb.recv(&wire);
    req.mbufs_mut().push_back(mb);
    req.recompute_mlen();
    req
}

/// Drain the recorder for a brief window and assert peer 2
/// did not see a SET (it was Down throughout the SET fan-out).
async fn assert_peer2_silent_during_outage(
    recorded_rx: &mut mpsc::Receiver<(u32, DmsgType, Vec<u8>)>,
) {
    let mut peer2_saw_set = false;
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), recorded_rx.recv()).await {
            Ok(Some((peer, _, bytes))) => {
                if peer == 2 {
                    let upper: Vec<u8> = bytes.iter().map(u8::to_ascii_uppercase).collect();
                    if upper.windows(3).any(|w| w == b"SET") {
                        peer2_saw_set = true;
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }
    assert!(
        !peer2_saw_set,
        "peer 2 should not see the SET while in Down state"
    );
}

async fn await_hint_replay(recorded_rx: &mut mpsc::Receiver<(u32, DmsgType, Vec<u8>)>) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), recorded_rx.recv()).await {
            Ok(Some((peer, ty, bytes))) => {
                if peer == 2 && matches!(ty, DmsgType::ReqForward) {
                    let upper: Vec<u8> = bytes.iter().map(u8::to_ascii_uppercase).collect();
                    if upper.windows(3).any(|w| w == b"SET")
                        && bytes.windows(3).any(|w| w == b"hot")
                        && bytes.windows(2).any(|w| w == b"v1")
                    {
                        return true;
                    }
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dc_quorum_set_with_one_down_replica_stores_and_drains_hint() {
    let pool = pool(
        true,
        [PeerState::Normal, PeerState::Normal, PeerState::Down],
    );
    let store = Arc::new(HintStore::new(1024 * 1024));

    let (peer0_tx, peer0_rx) = mpsc::channel::<OutboundRequest>(32);
    let (peer1_tx, peer1_rx) = mpsc::channel::<OutboundRequest>(32);
    let (peer2_tx, peer2_rx) = mpsc::channel::<OutboundRequest>(32);

    let (recorded_tx, mut recorded_rx) = mpsc::channel::<(u32, DmsgType, Vec<u8>)>(64);
    let h0 = spawn_fake_replica(0, peer0_rx, recorded_tx.clone());
    let h1 = spawn_fake_replica(1, peer1_rx, recorded_tx.clone());
    let h2 = spawn_fake_replica(2, peer2_rx, recorded_tx.clone());
    drop(recorded_tx);

    let disp = ClusterDispatcher::new(pool.clone())
        .with_hint_store(store.clone())
        .with_peer_backend(0, peer0_tx)
        .with_peer_backend(1, peer1_tx)
        .with_peer_backend(2, peer2_tx.clone());

    // Issue a SET against the cluster.
    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(8);
    let req = build_set_req(42, b"hot", b"v1");
    let _ = disp.dispatch(req, client_tx);

    // Client should see `+OK\r\n` (the SET coalesces over peers
    // 0 + 1 plus the synth hint reply for peer 2).
    let env = tokio::time::timeout(Duration::from_secs(2), client_rx.recv())
        .await
        .expect("client reply timeout")
        .expect("client reply missing");
    let bytes: Vec<u8> = env
        .rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(bytes, b"+OK\r\n");

    // The hint store must hold exactly one hint for peer 2.
    assert_eq!(store.len_for(2), 1);
    assert_eq!(store.len_for(0), 0);
    assert_eq!(store.len_for(1), 0);

    assert_peer2_silent_during_outage(&mut recorded_rx).await;

    // Bring peer 2 back to Normal and drain hints.
    {
        let mut peers = pool.peers().write();
        if let Some(p) = peers.get_mut(2) {
            p.set_state(PeerState::Normal, 1);
        }
    }

    // Run one drainer pass: take hints for peer 2 and ship them
    // to peer2_tx.
    let drained = store.take_for(2);
    assert_eq!(drained.len(), 1);
    for h in drained {
        let (rsp_tx, _rsp_rx) = mpsc::channel::<OutboundEnvelope>(1);
        let req = OutboundRequest {
            bytes: h.payload,
            req_id: 99,
            responder: rsp_tx,
            span: tracing::Span::none(),
            ty: DmsgType::ReqForward,
            target_peer_idx: Some(2),
        };
        peer2_tx.try_send(req).expect("hint replay channel full");
    }

    assert!(
        await_hint_replay(&mut recorded_rx).await,
        "drained hint never reached peer 2"
    );

    // Hint store is now empty.
    assert_eq!(store.len_for(2), 0);
    assert_eq!(store.total_len(), 0);

    drop(disp);
    drop(peer2_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = h0.await;
        let _ = h1.await;
        let _ = h2.await;
    })
    .await;
}

/// Regression: with `enable_hinted_handoff: false` the
/// dispatcher behaviour is unchanged. Down peers are filtered
/// out of the routable set and the request fails with
/// no-quorum when the surviving replicas cannot meet the
/// consistency level.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handoff_off_preserves_legacy_behaviour() {
    let pool = pool(
        false,
        [PeerState::Normal, PeerState::Normal, PeerState::Down],
    );
    let (peer0_tx, peer0_rx) = mpsc::channel::<OutboundRequest>(32);
    let (peer1_tx, peer1_rx) = mpsc::channel::<OutboundRequest>(32);
    let (peer2_tx, peer2_rx) = mpsc::channel::<OutboundRequest>(32);
    let (recorded_tx, mut recorded_rx) = mpsc::channel::<(u32, DmsgType, Vec<u8>)>(64);
    let h0 = spawn_fake_replica(0, peer0_rx, recorded_tx.clone());
    let h1 = spawn_fake_replica(1, peer1_rx, recorded_tx.clone());
    let h2 = spawn_fake_replica(2, peer2_rx, recorded_tx.clone());
    drop(recorded_tx);

    // No hint store wired even though one is built. The
    // pool config has `enable_hinted_handoff: false` so the
    // dispatcher's `hinted_handoff_active()` is false; no
    // hint path is exercised.
    let disp = ClusterDispatcher::new(pool)
        .with_peer_backend(0, peer0_tx)
        .with_peer_backend(1, peer1_tx)
        .with_peer_backend(2, peer2_tx);

    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
    let req = build_set_req(43, b"k", b"v");
    let _ = disp.dispatch(req, client_tx);

    // The plan reaches peers 0 and 1 only (peer 2 is Down and
    // filtered out). DC_QUORUM with 2 reachable replicas needs
    // both to ack; both fake replicas reply +OK so the client
    // still sees +OK.
    let env = tokio::time::timeout(Duration::from_secs(2), client_rx.recv())
        .await
        .expect("client reply timeout")
        .expect("client reply missing");
    let bytes: Vec<u8> = env
        .rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(bytes, b"+OK\r\n");

    // Peer 2 must NOT have received any traffic.
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(50), recorded_rx.recv()).await {
            Ok(Some((peer, _, _))) => {
                assert_ne!(peer, 2, "peer 2 must not see traffic with handoff off");
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }

    drop(disp);
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = h0.await;
        let _ = h1.await;
        let _ = h2.await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_with_one_down_replica_does_not_hint() {
    // Reads must not trigger the hint path even when handoff is
    // enabled: there is no value to hint, and the spec restricts\n    // hinting to writes.
    let pool = pool(
        true,
        [PeerState::Normal, PeerState::Normal, PeerState::Down],
    );
    let store = Arc::new(HintStore::new(1024 * 1024));

    let (peer0_tx, peer0_rx) = mpsc::channel::<OutboundRequest>(32);
    let (peer1_tx, peer1_rx) = mpsc::channel::<OutboundRequest>(32);
    let (peer2_tx, peer2_rx) = mpsc::channel::<OutboundRequest>(32);
    let (recorded_tx, _recorded_rx) = mpsc::channel::<(u32, DmsgType, Vec<u8>)>(64);
    let h0 = spawn_fake_replica(0, peer0_rx, recorded_tx.clone());
    let h1 = spawn_fake_replica(1, peer1_rx, recorded_tx.clone());
    let h2 = spawn_fake_replica(2, peer2_rx, recorded_tx);

    let disp = ClusterDispatcher::new(pool)
        .with_hint_store(store.clone())
        .with_peer_backend(0, peer0_tx)
        .with_peer_backend(1, peer1_tx)
        .with_peer_backend(2, peer2_tx);

    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
    // Build a GET request manually.
    let mbuf_pool = MbufPool::default();
    let mut req = Msg::new(7, MsgType::ReqRedisGet, true);
    req.flags_mut().is_read = true;
    req.push_key(KeyPos::without_tag(b"hot".to_vec()));
    let mut wire = Vec::new();
    wire.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nhot\r\n");
    let mut mb = mbuf_pool.get();
    mb.recv(&wire);
    req.mbufs_mut().push_back(mb);
    req.recompute_mlen();
    let _ = disp.dispatch(req, client_tx);

    let _ = tokio::time::timeout(Duration::from_secs(2), client_rx.recv()).await;

    // No hint should have been recorded on the read path.
    assert_eq!(store.total_len(), 0);

    drop(disp);
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = h0.await;
        let _ = h1.await;
        let _ = h2.await;
    })
    .await;
}

/// `try_send` failure to a Normal-state peer must fall back to
/// the hint path when handoff is enabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_send_failure_falls_back_to_hint() {
    let pool = pool(
        true,
        [PeerState::Normal, PeerState::Normal, PeerState::Normal],
    );
    let store = Arc::new(HintStore::new(1024 * 1024));

    let (peer0_tx, peer0_rx) = mpsc::channel::<OutboundRequest>(32);
    let (peer1_tx, peer1_rx) = mpsc::channel::<OutboundRequest>(32);
    // Peer 2 channel is closed up front so try_send fails.
    let (peer2_tx, peer2_rx) = mpsc::channel::<OutboundRequest>(32);
    drop(peer2_rx);

    let (recorded_tx, mut _recorded_rx) = mpsc::channel::<(u32, DmsgType, Vec<u8>)>(64);
    let h0 = spawn_fake_replica(0, peer0_rx, recorded_tx.clone());
    let h1 = spawn_fake_replica(1, peer1_rx, recorded_tx);

    let disp = ClusterDispatcher::new(pool)
        .with_hint_store(store.clone())
        .with_peer_backend(0, peer0_tx)
        .with_peer_backend(1, peer1_tx)
        .with_peer_backend(2, peer2_tx);

    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
    let req = build_set_req(101, b"foo", b"bar");
    let _ = disp.dispatch(req, client_tx);

    // Client still sees +OK because the hint counts toward the
    // quorum.
    let env = tokio::time::timeout(Duration::from_secs(2), client_rx.recv())
        .await
        .expect("client reply timeout")
        .expect("client reply missing");
    let bytes: Vec<u8> = env
        .rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(bytes, b"+OK\r\n");

    // Hint stored against peer 2.
    assert_eq!(store.len_for(2), 1);

    drop(disp);
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = h0.await;
        let _ = h1.await;
    })
    .await;
}
