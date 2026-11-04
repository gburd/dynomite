//! Multi-key requests whose keys live on different nodes of the local rack
//! are split per owner and the replies merged in the client's key order.
//!
//! Regression: the dispatcher routed `MGET` / `MSET` / `DEL` / `EXISTS` by
//! the first key only, so with a multi-node rack every other key was read
//! from (or written to) the first key's owner.

use std::{collections::HashMap, sync::Arc, time::Duration};

use dynomite::{
    cluster::{
        dispatch::ClusterDispatcher,
        peer::{Peer, PeerEndpoint, PeerState},
        pool::{PoolConfig, ServerPool},
    },
    conf::{Distribution, HashType},
    hashkit::DynToken,
    io::mbuf::MbufPool,
    msg::{Msg, MsgType},
    net::{
        dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope},
        server::OutboundRequest,
    },
    proto::redis::{
        redis_coalesce_fragments, redis_fragment, redis_parse_req, FRAGMENT_ERROR_REPLY,
    },
};
use tokio::sync::mpsc;

const PEERS: u32 = 3;

fn pool() -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        hash: HashType::Murmur,
        distribution: Distribution::Vnode,
        ..PoolConfig::default()
    };
    let peers: Vec<Peer> = (0..PEERS)
        .map(|i| {
            Peer::new(
                i,
                PeerEndpoint::tcp("127.0.0.1".into(), 8101 + u16::try_from(i).unwrap()),
                "r1".into(),
                "dc1".into(),
                vec![DynToken::from_u32(i * (u32::MAX / PEERS))],
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

/// Keys spread over all three owners, in a fixed order.
fn spread_keys(disp: &ClusterDispatcher) -> Vec<Vec<u8>> {
    let mut by_owner: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();
    for i in 0..200 {
        let k = format!("key:{i}").into_bytes();
        by_owner
            .entry(disp.rack_owner(&k).unwrap())
            .or_default()
            .push(k);
    }
    assert_eq!(
        by_owner.len(),
        PEERS as usize,
        "keys must reach every owner"
    );
    // Interleave owners: o0, o1, o2, o0, o1, o2.
    let keys: Vec<Vec<u8>> = (0..2)
        .flat_map(|round| (0..PEERS).map(move |o| (o, round)))
        .map(|(o, round)| by_owner[&o][round].clone())
        .collect();
    keys
}

#[test]
fn mget_splits_per_owner_in_key_order() {
    let disp = ClusterDispatcher::new(pool());
    let keys = spread_keys(&disp);
    let mut args: Vec<&[u8]> = vec![b"MGET"];
    args.extend(keys.iter().map(Vec::as_slice));
    let mut req = parse(7, &resp(&args));
    let out = redis_fragment(&mut req, &disp, &[], &MbufPool::default())
        .unwrap()
        .unwrap();
    assert_eq!(out.fragments.len(), 3);
    for (f, frag) in out.fragments.iter().enumerate() {
        let mine: Vec<&[u8]> = keys
            .iter()
            .zip(&out.shard_for_key)
            .filter(|(_, s)| **s == out.shard_for_key[f])
            .map(|(k, _)| k.as_slice())
            .collect();
        let mut want: Vec<&[u8]> = vec![b"mget"];
        want.extend(mine);
        assert_eq!(bytes(frag), resp(&want), "fragment {f}");
        assert_eq!(frag.frag_id(), out.frag_id);
    }
}

/// One owner for every key still goes through the split path as a single
/// fragment (only a one-key request is left whole).
#[test]
fn single_owner_mget_is_one_fragment() {
    let disp = ClusterDispatcher::new(pool());
    let k = b"key:1".to_vec();
    let mut req = parse(1, &resp(&[b"MGET", &k, &k]));
    let out = redis_fragment(&mut req, &disp, &[], &MbufPool::default())
        .unwrap()
        .unwrap();
    assert_eq!(out.fragments.len(), 1);
    assert_eq!(bytes(&out.fragments[0]), resp(&[b"mget", &k, &k]));
    let mut one = parse(2, &resp(&[b"MGET", &k]));
    assert!(redis_fragment(&mut one, &disp, &[], &MbufPool::default())
        .unwrap()
        .is_none());
}

#[test]
fn merge_restores_client_order_and_errors_whole_reply() {
    // MGET a b c d: a,c on fragment 1; b on 0; d on 2.
    let replies = vec![
        Some(b"*1\r\n$1\r\nB\r\n".to_vec()),
        Some(b"*2\r\n$1\r\nA\r\n$-1\r\n".to_vec()),
        Some(b"*1\r\n$2\r\nDD\r\n".to_vec()),
    ];
    assert_eq!(
        redis_coalesce_fragments(MsgType::ReqRedisMget, &[1, 0, 1, 2], &replies),
        b"*4\r\n$1\r\nA\r\n$1\r\nB\r\n$-1\r\n$2\r\nDD\r\n"
    );
    let mut bad = replies.clone();
    bad[2] = Some(b"-ERR boom\r\n".to_vec());
    assert_eq!(
        redis_coalesce_fragments(MsgType::ReqRedisMget, &[1, 0, 1, 2], &bad),
        FRAGMENT_ERROR_REPLY
    );
    bad[2] = None;
    assert_eq!(
        redis_coalesce_fragments(MsgType::ReqRedisMget, &[1, 0, 1, 2], &bad),
        FRAGMENT_ERROR_REPLY
    );
}

/// The full dispatcher path: every owner gets its sub-request and the
/// client gets one merged reply in key order.
#[tokio::test]
async fn dispatcher_fans_fragments_out_and_merges() {
    let (local_tx, mut local_rx) = mpsc::channel::<OutboundRequest>(8);
    let mut disp = ClusterDispatcher::new(pool()).with_backend(local_tx);
    let mut peer_rx = HashMap::new();
    for i in 1..PEERS {
        let (tx, rx) = mpsc::channel::<OutboundRequest>(8);
        disp = disp.with_peer_backend(i, tx);
        peer_rx.insert(i, rx);
    }
    let keys = spread_keys(&disp);
    let mut args: Vec<&[u8]> = vec![b"MGET"];
    args.extend(keys.iter().map(Vec::as_slice));
    let req = parse(42, &resp(&args));
    let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
    assert!(matches!(
        disp.dispatch(req, client_tx),
        DispatchOutcome::Pending
    ));

    // Each owner answers its keys with the key upper-cased as the value.
    let answer = |out: OutboundRequest| {
        let mut sub = Msg::new(0, MsgType::Unknown, true);
        redis_parse_req(&mut sub, &out.bytes);
        let sub_keys: Vec<Vec<u8>> = sub.keys().iter().map(|k| k.key().to_vec()).collect();
        let mut body = format!("*{}\r\n", sub_keys.len()).into_bytes();
        for k in &sub_keys {
            let v = k.to_ascii_uppercase();
            body.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
            body.extend_from_slice(&v);
            body.extend_from_slice(b"\r\n");
        }
        let mut rsp = Msg::new(out.req_id, MsgType::Unknown, false);
        rsp.append_bytes(&MbufPool::default(), &body);
        tokio::spawn(async move {
            let _ = out
                .responder
                .send(OutboundEnvelope {
                    req_id: out.req_id,
                    rsp,
                    span: tracing::Span::none(),
                    source_peer_idx: out.target_peer_idx,
                })
                .await;
        });
        sub_keys.len()
    };
    let mut answered = answer(local_rx.recv().await.expect("local fragment"));
    for i in 1..PEERS {
        answered += answer(
            peer_rx
                .get_mut(&i)
                .unwrap()
                .recv()
                .await
                .expect("peer fragment"),
        );
    }
    assert_eq!(answered, keys.len());

    let env = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
        .await
        .expect("merged reply")
        .expect("reply");
    assert_eq!(env.req_id, 42);
    let mut want = format!("*{}\r\n", keys.len()).into_bytes();
    for k in &keys {
        let v = k.to_ascii_uppercase();
        want.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
        want.extend_from_slice(&v);
        want.extend_from_slice(b"\r\n");
    }
    assert_eq!(bytes(&env.rsp), want);
}
