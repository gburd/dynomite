//! Read-repair integration tests.
//!
//! Three-replica in-process cluster. Each replica is modelled as
//! a tokio task that pulls outbound requests off a per-peer
//! channel, decodes them, and synthesises a Redis reply. The
//! dispatcher fans the request to all three replicas, the
//! coalescer picks a winner, and the test asserts that:
//!
//! * the client receives the quorum-winning reply;
//! * after a short delay the divergent replica has received a
//!   `SET key <winner>` repair write tagged with
//!   `DmsgType::ReqForward`.

use std::sync::Arc;
use std::time::Duration;

use dynomite::cluster::dispatch::ClusterDispatcher;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::conf::{DataStore, HashType};
use dynomite::hashkit::DynToken;
use dynomite::msg::keypos::KeyPos;
use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
use dynomite::net::dispatcher::{Dispatcher, OutboundEnvelope};
use dynomite::net::server::OutboundRequest;
use dynomite::proto::dnode::DmsgType;

use tokio::sync::mpsc;

/// Spawn one fake replica that replies with a fixed bulk string
/// to any GET, swallows any SET/DEL by recording the bytes onto
/// the supplied `recorded_writes` channel, and answers SET / DEL
/// repair traffic with a `+OK\r\n` simple-string reply.
fn spawn_fake_replica(
    peer_idx: u32,
    fixed_get_value: Option<Vec<u8>>,
    rx: &mut mpsc::Receiver<OutboundRequest>,
    recorded: mpsc::Sender<(u32, DmsgType, Vec<u8>)>,
) -> tokio::task::JoinHandle<()> {
    use dynomite::io::mbuf::MbufPool;
    let mut rx = std::mem::replace(rx, {
        let (_t, r) = mpsc::channel(1);
        r
    });
    tokio::spawn(async move {
        while let Some(req) = rx.recv().await {
            let _ = recorded.send((peer_idx, req.ty, req.bytes.clone())).await;
            let pool = MbufPool::default();
            let mut rsp_msg = Msg::new(req.req_id, MsgType::RspRedisBulk, false);
            rsp_msg.set_parent_id(req.req_id);
            // Decide reply shape from the request bytes.
            let bytes_upper: Vec<u8> = req.bytes.iter().map(u8::to_ascii_uppercase).collect();
            let is_get = bytes_upper.windows(3).any(|w| w == b"GET");
            let is_set = bytes_upper.windows(3).any(|w| w == b"SET");
            let is_del = bytes_upper.windows(3).any(|w| w == b"DEL");
            let payload: Vec<u8> = if is_get {
                match fixed_get_value.as_deref() {
                    Some(v) => {
                        let mut out = Vec::new();
                        out.extend_from_slice(b"$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(v);
                        out.extend_from_slice(b"\r\n");
                        out
                    }
                    None => b"$-1\r\n".to_vec(),
                }
            } else if is_set || is_del {
                rsp_msg = Msg::new(req.req_id, MsgType::RspRedisStatus, false);
                rsp_msg.set_parent_id(req.req_id);
                b"+OK\r\n".to_vec()
            } else {
                b"+OK\r\n".to_vec()
            };
            let mut buf = pool.get();
            buf.recv(&payload);
            rsp_msg.mbufs_mut().push_back(buf);
            rsp_msg.recompute_mlen();
            let env = OutboundEnvelope {
                req_id: req.req_id,
                rsp: rsp_msg,
                span: req.span,
                source_peer_idx: req.target_peer_idx,
            };
            let _ = req.responder.send(env).await;
        }
    })
}

fn local_peer(idx: u32, rack: &str, tok: u32, is_local: bool) -> Peer {
    let mut p = Peer::new(
        idx,
        PeerEndpoint::tcp(
            format!("10.0.0.{idx}"),
            8101 + u16::try_from(idx).unwrap_or(0),
        ),
        rack.into(),
        "dc1".into(),
        vec![DynToken::from_u32(tok)],
        is_local,
        true,
        false,
    );
    p.set_state(PeerState::Normal, 0);
    p
}

fn pool_three_replicas() -> Arc<ServerPool> {
    let cfg = PoolConfig {
        name: "p".into(),
        dc: "dc1".into(),
        rack: "rA".into(),
        data_store: DataStore::Redis,
        hash: HashType::Murmur,
        read_consistency: ConsistencyLevel::DcQuorum,
        write_consistency: ConsistencyLevel::DcQuorum,
        timeout_ms: 5_000,
        server_retry_timeout_ms: 30_000,
        server_failure_limit: 2,
        auto_eject_hosts: false,
        enable_gossip: false,
        bucket_types: Vec::new(),
        default_bucket_type: None,
    };
    let peers = vec![
        // Make peer 0 NON-local so the dispatcher does NOT
        // short-circuit a single-rack DC_QUORUM into the local
        // datastore branch. With three peers across three racks
        // the planner produces a Replicas plan with three
        // targets.
        local_peer(0, "rA", 1_000, false),
        local_peer(1, "rB", 2_000, false),
        local_peer(2, "rC", 3_000, false),
    ];
    let pool = ServerPool::new(cfg, peers);
    pool.preselect_remote_racks();
    Arc::new(pool)
}

/// Build a request with a single key and the wire bytes for a
/// `GET key` so the dispatcher can clone the bytes onto every
/// per-target outbound.
fn build_get_req(req_id: u64, key: &[u8]) -> Msg {
    use dynomite::io::mbuf::MbufPool;
    let pool = MbufPool::default();
    let mut req = Msg::new(req_id, MsgType::ReqRedisGet, true);
    req.flags_mut().is_read = true;
    req.push_key(KeyPos::without_tag(key.to_vec()));
    let mut wire = Vec::new();
    wire.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$");
    wire.extend_from_slice(key.len().to_string().as_bytes());
    wire.extend_from_slice(b"\r\n");
    wire.extend_from_slice(key);
    wire.extend_from_slice(b"\r\n");
    let mut mb = pool.get();
    mb.recv(&wire);
    req.mbufs_mut().push_back(mb);
    req.recompute_mlen();
    req
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dc_quorum_get_returns_majority_value_and_repairs_divergent_replica() {
    let pool = pool_three_replicas();
    // Three per-peer channels.
    let (peer0_tx, mut peer0_rx) = mpsc::channel::<OutboundRequest>(8);
    let (peer1_tx, mut peer1_rx) = mpsc::channel::<OutboundRequest>(8);
    let (peer2_tx, mut peer2_rx) = mpsc::channel::<OutboundRequest>(8);
    // Recorder channel: every replica reports the requests it
    // observed.
    let (recorded_tx, mut recorded_rx) = mpsc::channel::<(u32, DmsgType, Vec<u8>)>(64);
    // Replica 0 returns "v1"; replicas 1 and 2 return "v2".
    let h0 = spawn_fake_replica(0, Some(b"v1".to_vec()), &mut peer0_rx, recorded_tx.clone());
    let h1 = spawn_fake_replica(1, Some(b"v2".to_vec()), &mut peer1_rx, recorded_tx.clone());
    let h2 = spawn_fake_replica(2, Some(b"v2".to_vec()), &mut peer2_rx, recorded_tx.clone());
    drop(recorded_tx);

    let disp = ClusterDispatcher::new(pool)
        .with_peer_backend(0, peer0_tx)
        .with_peer_backend(1, peer1_tx)
        .with_peer_backend(2, peer2_tx);

    // Client responder.
    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
    let req = build_get_req(7, b"hot");
    let _ = disp.dispatch(req, client_tx);

    // Read the coalesced reply: should be "v2".
    let env = tokio::time::timeout(Duration::from_secs(2), client_rx.recv())
        .await
        .expect("timed out waiting for coalesced reply")
        .expect("no reply produced");
    let bytes: Vec<u8> = env
        .rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(bytes, b"$2\r\nv2\r\n");

    // The coalescer should now have shipped a SET hot v2 with
    // `DmsgType::ReqForward` to peer 0 (the divergent replica).
    // Drain the recorder; ignore the original GETs (peers 0/1/2)
    // and any +OK / repair acks; assert we observe the SET to
    // peer 0.
    let mut got_repair = false;
    let mut last_recv = std::time::Instant::now();
    while last_recv.elapsed() < Duration::from_secs(2) {
        match tokio::time::timeout(Duration::from_millis(100), recorded_rx.recv()).await {
            Ok(Some((peer, ty, bytes))) => {
                last_recv = std::time::Instant::now();
                let upper: Vec<u8> = bytes.iter().map(u8::to_ascii_uppercase).collect();
                let is_set = upper.windows(3).any(|w| w == b"SET");
                if peer == 0 && matches!(ty, DmsgType::ReqForward) && is_set {
                    // Confirm payload contains both the key and
                    // the winning value.
                    assert!(bytes.windows(3).any(|w| w == b"hot"));
                    assert!(bytes.windows(2).any(|w| w == b"v2"));
                    got_repair = true;
                    break;
                }
            }
            Ok(None) => break,
            Err(_) => {}
        }
    }
    assert!(
        got_repair,
        "expected a ReqForward SET hot v2 to peer 0 (divergent replica)"
    );

    // Cleanup: drop dispatcher senders -> replica RX closes ->
    // tasks exit.
    drop(disp);
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        let _ = h0.await;
        let _ = h1.await;
        let _ = h2.await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dc_quorum_get_unanimous_no_repair() {
    let pool = pool_three_replicas();
    let (peer0_tx, mut peer0_rx) = mpsc::channel::<OutboundRequest>(8);
    let (peer1_tx, mut peer1_rx) = mpsc::channel::<OutboundRequest>(8);
    let (peer2_tx, mut peer2_rx) = mpsc::channel::<OutboundRequest>(8);
    let (recorded_tx, mut recorded_rx) = mpsc::channel::<(u32, DmsgType, Vec<u8>)>(64);
    let h0 = spawn_fake_replica(0, Some(b"q".to_vec()), &mut peer0_rx, recorded_tx.clone());
    let h1 = spawn_fake_replica(1, Some(b"q".to_vec()), &mut peer1_rx, recorded_tx.clone());
    let h2 = spawn_fake_replica(2, Some(b"q".to_vec()), &mut peer2_rx, recorded_tx.clone());
    drop(recorded_tx);

    let disp = ClusterDispatcher::new(pool)
        .with_peer_backend(0, peer0_tx)
        .with_peer_backend(1, peer1_tx)
        .with_peer_backend(2, peer2_tx);

    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
    let req = build_get_req(11, b"k");
    let _ = disp.dispatch(req, client_tx);

    let env = tokio::time::timeout(Duration::from_secs(2), client_rx.recv())
        .await
        .expect("timeout")
        .expect("no reply");
    let bytes: Vec<u8> = env
        .rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    assert_eq!(bytes, b"$1\r\nq\r\n");

    // Drain a brief window: there must be NO ReqForward SET on
    // any peer.
    let deadline = std::time::Instant::now() + Duration::from_millis(300);
    while std::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(100), recorded_rx.recv()).await {
            Ok(Some((_peer, ty, bytes))) => {
                let upper: Vec<u8> = bytes.iter().map(u8::to_ascii_uppercase).collect();
                let is_set = upper.windows(3).any(|w| w == b"SET");
                let is_del = upper.windows(3).any(|w| w == b"DEL");
                assert!(
                    !(matches!(ty, DmsgType::ReqForward) && (is_set || is_del)),
                    "no read-repair should fire when replicas agree"
                );
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
