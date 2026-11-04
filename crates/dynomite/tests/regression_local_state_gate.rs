//! The node's own state gates what it hands its local datastore.
//!
//! Regression: the dispatcher forwarded to the local datastore in every
//! state. STANDBY refuses every local request and WRITES_ONLY / RESUMING
//! refuse local reads, with `Dynomite: Dynomite's current state does not
//! allow this request`; requests a peer owns are unaffected. A request the
//! node accepts but may not send yet (a write under WRITES_ONLY; anything
//! under JOINING) is queued for the datastore unsent, and goes out once a
//! request arrives under RESUMING; NORMAL sends only when nothing is
//! queued ahead.

use std::sync::Arc;

use dynomite::{
    cluster::{
        dispatch::ClusterDispatcher,
        peer::{Peer, PeerEndpoint, PeerState},
        pool::{DynState, PoolConfig, ServerPool},
    },
    conf::{Distribution, HashType},
    hashkit::DynToken,
    io::mbuf::MbufPool,
    msg::{Msg, MsgType},
    net::{
        dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope},
        server::OutboundRequest,
    },
    proto::redis::redis_parse_req,
};
use tokio::sync::mpsc;

const INVALID_STATE: &[u8] =
    b"-ERR Dynomite: Dynomite's current state does not allow this request\r\n";

fn pool() -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        hash: HashType::Murmur,
        distribution: Distribution::Vnode,
        ..PoolConfig::default()
    };
    let peers: Vec<Peer> = (0..2u32)
        .map(|i| {
            Peer::new(
                i,
                PeerEndpoint::tcp("127.0.0.1".into(), 8101 + u16::try_from(i).unwrap()),
                "r1".into(),
                "dc1".into(),
                vec![DynToken::from_u32(i * (u32::MAX / 2))],
                i == 0,
                true,
                false,
            )
        })
        .collect();
    let pool = Arc::new(ServerPool::new(cfg, peers));
    for p in pool.peers().write().iter_mut() {
        p.set_state(PeerState::Normal, 0);
    }
    pool.rebuild_ring();
    pool
}

fn resp(args: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn parse(id: u64, wire: &[u8]) -> Msg {
    let mut m = Msg::new(id, MsgType::Unknown, true);
    redis_parse_req(&mut m, wire);
    m.append_bytes(&MbufPool::default(), wire);
    m
}

fn bytes(m: &Msg) -> Vec<u8> {
    m.mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect()
}

struct Rig {
    pool: Arc<ServerPool>,
    disp: ClusterDispatcher,
    local_rx: mpsc::Receiver<OutboundRequest>,
    peer_rx: mpsc::Receiver<OutboundRequest>,
    local_key: Vec<u8>,
    peer_key: Vec<u8>,
}

fn rig() -> Rig {
    let pool = pool();
    let (local_tx, local_rx) = mpsc::channel::<OutboundRequest>(16);
    let (peer_tx, peer_rx) = mpsc::channel::<OutboundRequest>(16);
    let disp = ClusterDispatcher::new(pool.clone())
        .with_backend(local_tx)
        .with_peer_backend(1, peer_tx);
    let key_on = |owner: u32| {
        (0..500)
            .map(|i| format!("key:{i}").into_bytes())
            .find(|k| disp.rack_owner(k) == Some(owner))
            .expect("a key for each owner")
    };
    let local_key = key_on(0);
    let peer_key = key_on(1);
    Rig {
        pool,
        disp,
        local_rx,
        peer_rx,
        local_key,
        peer_key,
    }
}

/// Request ids the local datastore channel has received, in order.
fn drain(rx: &mut mpsc::Receiver<OutboundRequest>) -> Vec<u64> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .map(|o| o.req_id)
        .collect()
}

/// Dispatch `wire` and return the immediate error reply, if any.
fn dispatch(r: &Rig, id: u64, wire: &[u8]) -> Option<Vec<u8>> {
    let (tx, _rx) = mpsc::channel::<OutboundEnvelope>(4);
    match r.disp.dispatch(parse(id, wire), tx) {
        DispatchOutcome::Error(rsp) | DispatchOutcome::Inline(rsp) => Some(bytes(&rsp)),
        DispatchOutcome::Pending | DispatchOutcome::Drop => None,
    }
}

#[tokio::test]
async fn standby_refuses_every_local_request_and_serves_peer_keys() {
    let mut r = rig();
    r.pool.set_local_state(DynState::Standby);
    let get = resp(&[b"GET", &r.local_key]);
    let set = resp(&[b"SET", &r.local_key, b"v"]);
    assert_eq!(dispatch(&r, 1, &get).as_deref(), Some(INVALID_STATE));
    assert_eq!(dispatch(&r, 2, &set).as_deref(), Some(INVALID_STATE));
    assert_eq!(
        dispatch(&r, 3, &resp(&[b"PING"])).as_deref(),
        Some(INVALID_STATE)
    );
    assert!(
        r.local_rx.try_recv().is_err(),
        "nothing reaches the datastore"
    );
    // A key the peer owns still goes to the peer.
    assert_eq!(dispatch(&r, 4, &resp(&[b"GET", &r.peer_key])), None);
    assert_eq!(r.peer_rx.try_recv().unwrap().req_id, 4);
}

#[tokio::test]
async fn writes_only_refuses_reads_and_holds_writes_until_resuming() {
    let mut r = rig();
    r.pool.set_local_state(DynState::WritesOnly);
    let get = resp(&[b"GET", &r.local_key]);
    let set = resp(&[b"SET", &r.local_key, b"v"]);
    assert_eq!(dispatch(&r, 10, &get).as_deref(), Some(INVALID_STATE));
    // The write is accepted but not sent.
    assert_eq!(dispatch(&r, 11, &set), None);
    assert!(r.local_rx.try_recv().is_err(), "write held");
    r.pool.set_local_state(DynState::Resuming);
    // RESUMING still refuses reads, and releases the held write
    // together with the new one, in order.
    assert_eq!(dispatch(&r, 12, &get).as_deref(), Some(INVALID_STATE));
    assert_eq!(dispatch(&r, 13, &set), None);
    let forwarded = drain(&mut r.local_rx);
    assert_eq!(forwarded, vec![11, 13]);
}

#[tokio::test]
async fn joining_holds_reads_and_writes() {
    let mut r = rig();
    r.pool.set_local_state(DynState::Joining);
    let get = resp(&[b"GET", &r.local_key]);
    let set = resp(&[b"SET", &r.local_key, b"v"]);
    assert_eq!(dispatch(&r, 30, &get), None);
    assert_eq!(dispatch(&r, 31, &set), None);
    assert_eq!(dispatch(&r, 32, &resp(&[b"PING"])), None);
    assert!(r.local_rx.try_recv().is_err(), "nothing sent while joining");
    r.pool.set_local_state(DynState::Resuming);
    assert_eq!(dispatch(&r, 33, &set), None);
    let forwarded = drain(&mut r.local_rx);
    assert_eq!(forwarded, vec![30, 31, 32, 33]);
}

#[tokio::test]
async fn normal_does_not_release_a_held_write() {
    let mut r = rig();
    r.pool.set_local_state(DynState::WritesOnly);
    let set = resp(&[b"SET", &r.local_key, b"v"]);
    assert_eq!(dispatch(&r, 20, &set), None);
    r.pool.set_local_state(DynState::Normal);
    assert_eq!(dispatch(&r, 21, &set), None);
    // NORMAL sends the new write only when nothing is queued ahead of
    // it; the held one keeps it waiting until a RESUMING request
    // flushes the queue.
    assert!(r.local_rx.try_recv().is_err());
    r.pool.set_local_state(DynState::Resuming);
    assert_eq!(dispatch(&r, 22, &set), None);
    let forwarded = drain(&mut r.local_rx);
    assert_eq!(forwarded, vec![20, 21, 22]);
}
