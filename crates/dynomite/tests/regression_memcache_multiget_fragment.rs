//! Memcache `get` / `gets` requests whose keys live on different nodes of
//! the local rack are split per owner and the replies merged in the
//! client's key order.
//!
//! Regression: the dispatcher routed a memcache retrieval by its first key
//! only, so with a multi-node rack every other key was read from the first
//! key's owner and came back as a miss.

use std::{collections::HashMap, sync::Arc, time::Duration};

use dynomite::{
    cluster::{
        dispatch::ClusterDispatcher,
        peer::{Peer, PeerEndpoint, PeerState},
        pool::{PoolConfig, ServerPool},
    },
    conf::{DataStore, Distribution, HashType},
    hashkit::DynToken,
    io::mbuf::MbufPool,
    msg::{Msg, MsgType},
    net::{
        dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope},
        server::OutboundRequest,
    },
    proto::memcache::{
        memcache_coalesce_fragments, memcache_fragment, memcache_parse_req,
        MEMCACHE_FRAGMENT_ERROR_REPLY,
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
        data_store: DataStore::Memcache,
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

fn parse(id: u64, wire: &[u8]) -> Msg {
    let mut m = Msg::new(id, MsgType::Unknown, true);
    memcache_parse_req(&mut m, wire);
    m.append_bytes(&MbufPool::default(), wire);
    m
}

fn bytes(m: &Msg) -> Vec<u8> {
    m.mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect()
}

fn request(verb: &str, keys: &[Vec<u8>]) -> Vec<u8> {
    let mut out = verb.as_bytes().to_vec();
    for k in keys {
        out.push(b' ');
        out.extend_from_slice(k);
    }
    out.extend_from_slice(b"\r\n");
    out
}

/// Keys spread over all three owners, interleaved o0, o1, o2, o0, o1, o2.
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
    (0..2)
        .flat_map(|round| (0..PEERS).map(move |o| (o, round)))
        .map(|(o, round)| by_owner[&o][round].clone())
        .collect()
}

/// The VALUE block a backend returns for `key` (value = key upper-cased).
fn value_block(key: &[u8], cas: Option<u64>) -> Vec<u8> {
    let v = key.to_ascii_uppercase();
    let mut out = b"VALUE ".to_vec();
    out.extend_from_slice(key);
    out.extend_from_slice(format!(" 0 {}", v.len()).as_bytes());
    if let Some(c) = cas {
        out.extend_from_slice(format!(" {c}").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&v);
    out.extend_from_slice(b"\r\n");
    out
}

#[test]
fn get_splits_per_owner_in_key_order() {
    let disp = ClusterDispatcher::new(pool());
    let keys = spread_keys(&disp);
    for verb in ["get", "gets"] {
        let mut req = parse(7, &request(verb, &keys));
        let out = memcache_fragment(&mut req, &disp, &MbufPool::default())
            .unwrap()
            .unwrap();
        assert_eq!(out.fragments.len(), 3);
        for (f, frag) in out.fragments.iter().enumerate() {
            // Each key is followed by one space, then CRLF.
            let mut want = format!("{verb} ").into_bytes();
            for (k, s) in keys.iter().zip(&out.shard_for_key) {
                if *s == out.shard_for_key[f] {
                    want.extend_from_slice(k);
                    want.push(b' ');
                }
            }
            want.extend_from_slice(b"\r\n");
            assert_eq!(bytes(frag), want, "{verb} fragment {f}");
            assert_eq!(frag.ty(), req.ty());
            assert_eq!(frag.frag_id(), out.frag_id);
            assert_eq!(frag.id(), 7);
        }
    }
}

/// Each key takes the next VALUE block of its fragment's reply, so a miss
/// followed by a hit in the same fragment hands the hit to the missed
/// key's slot.
#[test]
fn merge_takes_next_block_per_key_and_appends_end() {
    // get a b c d: a,c on fragment 0; b on 1; d on 2.
    let a = value_block(b"a", None);
    let b = value_block(b"b", None);
    let c = value_block(b"c", None);
    let all_hit = vec![
        Some([a.clone(), c.clone(), b"END\r\n".to_vec()].concat()),
        Some([b.clone(), b"END\r\n".to_vec()].concat()),
        Some(b"END\r\n".to_vec()),
    ];
    assert_eq!(
        memcache_coalesce_fragments(&[0, 1, 0, 2], &all_hit),
        [a.clone(), b.clone(), c.clone(), b"END\r\n".to_vec()].concat()
    );
    // a misses: c's block fills a's slot and lands before b.
    let a_miss = vec![
        Some([c.clone(), b"END\r\n".to_vec()].concat()),
        Some([b.clone(), b"END\r\n".to_vec()].concat()),
        Some(b"END\r\n".to_vec()),
    ];
    assert_eq!(
        memcache_coalesce_fragments(&[0, 1, 0, 2], &a_miss),
        [c, b, b"END\r\n".to_vec()].concat()
    );
    // Every key missing: just END.
    let none = vec![Some(b"END\r\n".to_vec()); 3];
    assert_eq!(
        memcache_coalesce_fragments(&[0, 1, 0, 2], &none),
        b"END\r\n"
    );
}

/// Any fragment without a reply, with a reply that is not VALUE / END, or
/// with an unreadable VALUE block fails the whole request with the
/// generic error.
#[test]
fn merge_errors_whole_reply_on_bad_fragment() {
    let ok = Some([value_block(b"a", None), b"END\r\n".to_vec()].concat());
    for bad in [
        None,
        Some(b"SERVER_ERROR out of memory\r\n".to_vec()),
        Some(b"ERROR\r\n".to_vec()),
        Some(b"STORED\r\n".to_vec()),
        Some(b"VALUE a 0 5\r\nab\r\nEND\r\n".to_vec()),
    ] {
        let replies = vec![ok.clone(), bad.clone()];
        assert_eq!(
            memcache_coalesce_fragments(&[0, 1], &replies),
            MEMCACHE_FRAGMENT_ERROR_REPLY,
            "{bad:?}"
        );
    }
}

/// The full dispatcher path: every owner gets its sub-request and the
/// client gets one merged reply in key order, misses omitted.
#[tokio::test]
async fn dispatcher_fans_fragments_out_and_merges() {
    for (verb, cas) in [("get", None), ("gets", Some(17u64))] {
        let (local_tx, mut local_rx) = mpsc::channel::<OutboundRequest>(8);
        let mut disp = ClusterDispatcher::new(pool()).with_backend(local_tx);
        let mut peer_rx = HashMap::new();
        for i in 1..PEERS {
            let (tx, rx) = mpsc::channel::<OutboundRequest>(8);
            disp = disp.with_peer_backend(i, tx);
            peer_rx.insert(i, rx);
        }
        let keys = spread_keys(&disp);
        // The last key of every fragment misses.
        let missing: Vec<Vec<u8>> = keys[3..].to_vec();
        let req = parse(42, &request(verb, &keys));
        let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
        assert!(matches!(
            disp.dispatch(req, client_tx),
            DispatchOutcome::Pending
        ));

        let answer = |out: OutboundRequest| {
            let mut sub = Msg::new(0, MsgType::Unknown, true);
            memcache_parse_req(&mut sub, &out.bytes);
            assert!(out.bytes.starts_with(format!("{verb} ").as_bytes()));
            let sub_keys: Vec<Vec<u8>> = sub.keys().iter().map(|k| k.key().to_vec()).collect();
            let mut body = Vec::new();
            for k in sub_keys.iter().filter(|k| !missing.contains(k)) {
                body.extend_from_slice(&value_block(k, cas));
            }
            body.extend_from_slice(b"END\r\n");
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
        let mut want = Vec::new();
        for k in &keys[..3] {
            want.extend_from_slice(&value_block(k, cas));
        }
        want.extend_from_slice(b"END\r\n");
        assert_eq!(bytes(&env.rsp), want, "{verb}");
    }
}

/// An error from one owner, or an owner that never answers, fails the
/// whole request with the generic error.
#[tokio::test]
async fn dispatcher_failed_fragment_errors_whole_reply() {
    for last in [Some(&b"SERVER_ERROR out of memory\r\n"[..]), None] {
        let (local_tx, mut local_rx) = mpsc::channel::<OutboundRequest>(8);
        let mut disp = ClusterDispatcher::new(pool()).with_backend(local_tx);
        let mut peer_rx = HashMap::new();
        for i in 1..PEERS {
            let (tx, rx) = mpsc::channel::<OutboundRequest>(8);
            disp = disp.with_peer_backend(i, tx);
            peer_rx.insert(i, rx);
        }
        let keys = spread_keys(&disp);
        let req = parse(9, &request("get", &keys));
        let (client_tx, mut client_rx) = mpsc::channel::<OutboundEnvelope>(4);
        assert!(matches!(
            disp.dispatch(req, client_tx),
            DispatchOutcome::Pending
        ));
        let reply = |out: OutboundRequest, body: Option<&'static [u8]>| {
            // No body: the request (and its responder) is dropped unanswered.
            let Some(body) = body else { return };
            let mut rsp = Msg::new(out.req_id, MsgType::Unknown, false);
            rsp.append_bytes(&MbufPool::default(), body);
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
        };
        reply(local_rx.recv().await.unwrap(), Some(&b"END\r\n"[..]));
        reply(
            peer_rx.get_mut(&1).unwrap().recv().await.unwrap(),
            Some(&b"END\r\n"[..]),
        );
        reply(peer_rx.get_mut(&2).unwrap().recv().await.unwrap(), last);
        let env = tokio::time::timeout(Duration::from_secs(5), client_rx.recv())
            .await
            .expect("merged reply")
            .expect("reply");
        assert_eq!(env.req_id, 9);
        assert_eq!(bytes(&env.rsp), MEMCACHE_FRAGMENT_ERROR_REPLY, "{last:?}");
    }
}
