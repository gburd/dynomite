//! Quorum enforcement over the PBC put/get path.
//!
//! A write must gather `W` acks and a read `R` responses from the
//! key's replicas; below that the operation fails rather than succeed
//! with insufficient replication. This drives both the success and the
//! below-quorum failure with a fixture whose reply behavior is
//! controllable per peer.

#![cfg(feature = "noxu")]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use dyniak::bucket_props::{BucketProps, BucketPropsRegistry};
use dyniak::proto::pb::{MessageCode, RpbContent, RpbGetReq, RpbPutReq};
use dyniak::quorum::{QUORUM_ALL, QUORUM_ONE};
use dyniak::replication::{RingPoint, RingView};
use dyniak::router::{
    BucketRouter, PeerOp, PeerOutbound, RoutingHooks, ACK_STORED, ACK_STORED_DURABLE,
};
use dynomite::embed::hooks::BoxFuture;
use dynomite::embed::Datastore;
use dynomite::hashkit::HashType;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Per-peer stored SiblingSet bytes, keyed by (peer, key).
type StoredMap = std::collections::HashMap<(u32, Vec<u8>), Vec<u8>>;

/// An outbound that acks (replies to a request) only for peers in
/// `acking`, and stores what it is asked to store per peer. Peers not
/// in `acking` return `None` from `request` (unreachable / no ack).
/// Peers in `acking` but not in `durable` ack with [`ACK_STORED`]
/// (landed, durability unconfirmed) rather than
/// [`ACK_STORED_DURABLE`], so a test can exercise the DW quorum
/// distinctly from W.
struct ControllableOutbound {
    acking: HashSet<u32>,
    durable: HashSet<u32>,
    // Records the last stored SiblingSet per (peer, key) for reads.
    stored: Mutex<StoredMap>,
}

impl std::fmt::Debug for ControllableOutbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllableOutbound")
            .finish_non_exhaustive()
    }
}

impl PeerOutbound for ControllableOutbound {
    fn dispatch(&self, _peer_idx: u32, _op: PeerOp) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    fn request(&self, peer_idx: u32, op: PeerOp) -> BoxFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move {
            if !self.acking.contains(&peer_idx) {
                // Unreachable / no ack.
                return None;
            }
            match op {
                PeerOp::RepairPut { key, storage, .. } => {
                    self.stored
                        .lock()
                        .expect("lock")
                        .insert((peer_idx, key), storage);
                    let ack = if self.durable.contains(&peer_idx) {
                        ACK_STORED_DURABLE
                    } else {
                        ACK_STORED
                    };
                    Some(vec![ack])
                }
                PeerOp::Get { key, .. } => Some(
                    self.stored
                        .lock()
                        .expect("lock")
                        .get(&(peer_idx, key))
                        .cloned()
                        .unwrap_or_default(),
                ),
                _ => None,
            }
        })
    }
}

async fn send_frame(stream: &mut TcpStream, code: u8, body: &[u8]) {
    let len = u32::try_from(body.len() + 1).expect("len");
    stream.write_all(&len.to_be_bytes()).await.expect("len");
    stream.write_all(&[code]).await.expect("code");
    stream.write_all(body).await.expect("body");
}

async fn recv_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.expect("len");
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.expect("frame");
    (buf[0], buf[1..].to_vec())
}

/// Build a 3-peer cluster (n_val=3, coordinator peer 0) whose replicas
/// `acking` will ack writes and answer reads.
/// Build a 3-peer cluster (n_val=3, coordinator peer 0) whose replicas
/// `acking` will ack writes and answer reads. Every acking peer acks
/// durably ([`ACK_STORED_DURABLE`]); see [`spawn_with_durability`] for
/// control over which acks are durable.
async fn spawn(
    acking: Vec<u32>,
    props: BucketProps,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let durable = acking.clone();
    spawn_with_durability(acking, durable, props).await
}

/// Like [`spawn`], but only replicas in `durable` (a subset of
/// `acking`) reply [`ACK_STORED_DURABLE`]; the rest of `acking` reply
/// plain [`ACK_STORED`] (landed, durability unconfirmed). Lets a test
/// drive the durable-write quorum DW distinctly from the write quorum
/// W.
async fn spawn_with_durability(
    acking: Vec<u32>,
    durable: Vec<u32>,
    props: BucketProps,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let dir = tempfile::tempdir().expect("tempdir");
    // Leak the tempdir so the noxu store outlives the test.
    let path = dir.keep();
    let ds: Arc<dyn Datastore> =
        Arc::new(dyniak::datastore::NoxuDatastore::open_transactional(&path).expect("noxu"));
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(b"", b"b", props);
    let span = u64::from(u32::MAX);
    let pts: Vec<RingPoint> = (0..3u32)
        .map(|i| RingPoint::new(u64::from(i) * span / 3, i, "dc1", "r1"))
        .collect();
    let router = Arc::new(BucketRouter::new(
        registry,
        Arc::new(RingView::new(pts)),
        HashType::Murmur,
    ));
    let hooks = RoutingHooks {
        router,
        outbound: Arc::new(ControllableOutbound {
            acking: acking.into_iter().collect(),
            durable: durable.into_iter().collect(),
            stored: Mutex::new(StoredMap::new()),
        }) as Arc<dyn PeerOutbound>,
        local_actor: dyniak::datatypes::ActorId::new("dc1", "n0"),
        local_peer_idx: 0,
        precommit: None,
        postcommit: None,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let admin = Arc::new(dynomite::cluster::admin_rpc::NoopClusterAdmin);
    let server = tokio::spawn(async move {
        let _ = dyniak::server::serve_pbc_with_routing(listener, ds, admin, hooks).await;
    });
    (addr, server)
}

/// A liveness source that reports every peer in `down` as unreachable
/// and every other peer as up.
struct DownPeers(HashSet<u32>);

impl dyniak::replication::ReplicaLiveness for DownPeers {
    fn is_up(&self, peer_idx: u32) -> bool {
        !self.0.contains(&peer_idx)
    }
}

impl std::fmt::Debug for DownPeers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownPeers").finish_non_exhaustive()
    }
}

/// Build a 4-peer cluster (n_val=3, coordinator peer 0) whose ring
/// walk names peers [0, 1, 2] as primaries and peer 3 as the ring's
/// only fallback candidate. `down` marks primary-window peers as
/// known-unreachable so [`BucketRouter::with_liveness`]'s planner
/// substitutes peer 3 in their place (marked
/// [`dynomite::cluster::ReplicaTarget::is_fallback`]). `acking` /
/// `durable` behave as in [`spawn_with_durability`].
async fn spawn_with_liveness(
    down: Vec<u32>,
    acking: Vec<u32>,
    durable: Vec<u32>,
    props: BucketProps,
) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.keep();
    let ds: Arc<dyn Datastore> =
        Arc::new(dyniak::datastore::NoxuDatastore::open_transactional(&path).expect("noxu"));
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(b"", b"b", props);
    let span = u64::from(u32::MAX);
    let pts: Vec<RingPoint> = (0..4u32)
        .map(|i| RingPoint::new(u64::from(i) * span / 4, i, "dc1", "r1"))
        .collect();
    let router = Arc::new(
        BucketRouter::new(registry, Arc::new(RingView::new(pts)), HashType::Murmur)
            .with_liveness(Arc::new(DownPeers(down.into_iter().collect()))),
    );
    let hooks = RoutingHooks {
        router,
        outbound: Arc::new(ControllableOutbound {
            acking: acking.into_iter().collect(),
            durable: durable.into_iter().collect(),
            stored: Mutex::new(StoredMap::new()),
        }) as Arc<dyn PeerOutbound>,
        local_actor: dyniak::datatypes::ActorId::new("dc1", "n0"),
        local_peer_idx: 0,
        precommit: None,
        postcommit: None,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let admin = Arc::new(dynomite::cluster::admin_rpc::NoopClusterAdmin);
    let server = tokio::spawn(async move {
        let _ = dyniak::server::serve_pbc_with_routing(listener, ds, admin, hooks).await;
    });
    (addr, server)
}
async fn put(c: &mut TcpStream, w: Option<u32>) -> u8 {
    put_with(c, w, None, None).await
}

async fn put_with(c: &mut TcpStream, w: Option<u32>, pw: Option<u32>, dw: Option<u32>) -> u8 {
    put_key_with(c, b"k", w, pw, dw).await
}

async fn put_key_with(
    c: &mut TcpStream,
    key: &[u8],
    w: Option<u32>,
    pw: Option<u32>,
    dw: Option<u32>,
) -> u8 {
    let req = RpbPutReq {
        bucket: b"b".to_vec(),
        key: Some(key.to_vec()),
        w,
        pw,
        dw,
        content: Some(RpbContent {
            value: b"v".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    send_frame(c, MessageCode::PutReq.as_u8(), &req.encode_to_vec()).await;
    recv_frame(c).await.0
}

#[tokio::test]
async fn write_quorum_all_fails_when_a_replica_does_not_ack() {
    // n_val=3, W=all=3. Coordinator (peer 0) + only peer 1 ack; peer 2
    // does not. Local + 1 ack = 2 < 3 -> the write fails.
    let (addr, server) = spawn(
        vec![1],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put(&mut c, Some(QUORUM_ALL)).await;
    assert_eq!(
        code,
        MessageCode::ErrorResp.as_u8(),
        "W=all must fail when a replica does not ack"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn write_quorum_is_met_when_enough_replicas_ack() {
    // W=all=3, all three ack (peer 0 local + peers 1,2) -> success.
    let (addr, server) = spawn(
        vec![1, 2],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put(&mut c, Some(QUORUM_ALL)).await;
    assert_eq!(
        code,
        MessageCode::PutResp.as_u8(),
        "W=all succeeds when every replica acks"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn write_quorum_one_succeeds_on_local_alone() {
    // W=one: the local store alone satisfies it even if no replica acks.
    let (addr, server) = spawn(
        vec![],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put(&mut c, Some(QUORUM_ONE)).await;
    assert_eq!(
        code,
        MessageCode::PutResp.as_u8(),
        "W=one is satisfied by the local write"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn read_quorum_all_fails_below_quorum() {
    // R=all=3, only peer 1 answers (peer 2 unreachable). local + 1 = 2
    // responses < 3 -> read fails.
    let (addr, server) = spawn(
        vec![1],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let get = RpbGetReq {
        bucket: b"b".to_vec(),
        key: b"k".to_vec(),
        r: Some(QUORUM_ALL),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, _) = recv_frame(&mut c).await;
    assert_eq!(
        code,
        MessageCode::ErrorResp.as_u8(),
        "R=all must fail when fewer than N replicas respond"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn durable_write_quorum_fails_when_acks_are_not_durable() {
    // n_val=3, DW=all=3. All three peers ack (W=3 satisfied), but peer
    // 2's ack is not durable (plain ACK_STORED). Local write is
    // durable (real noxu commit) and peer 1 acks durably, so durable
    // acks = 2 < 3 -> DW fails even though W is fully satisfied.
    let (addr, server) = spawn_with_durability(
        vec![1, 2],
        vec![1],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put_with(&mut c, Some(QUORUM_ALL), None, Some(QUORUM_ALL)).await;
    assert_eq!(
        code,
        MessageCode::ErrorResp.as_u8(),
        "DW=all must fail when a replica's ack is not confirmed durable"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn durable_write_quorum_succeeds_when_every_ack_is_durable() {
    // Same as above but both remote peers ack durably -> DW=all met.
    let (addr, server) = spawn_with_durability(
        vec![1, 2],
        vec![1, 2],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put_with(&mut c, Some(QUORUM_ALL), None, Some(QUORUM_ALL)).await;
    assert_eq!(
        code,
        MessageCode::PutResp.as_u8(),
        "DW=all succeeds when every ack is confirmed durable"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn primary_write_quorum_fails_when_only_a_fallback_acks() {
    // 4-peer ring, n_val=3, key = "key8": peer 1 is marked down, so the
    // liveness-aware planner substitutes peer 3 as a fallback for it,
    // leaving primaries [local peer 0, peer 2] and fallback [peer 3].
    // Only peer 3 (the fallback) acks; the other primary, peer 2, does
    // not. PW=2 needs 2 PRIMARY acks (local + peer 2), but only the
    // local primary ack landed -- the fallback ack must NOT count
    // toward PW. W is satisfied (local + fallback = 2 of the reachable
    // 3 targets), but PW is not.
    let (addr, server) = spawn_with_liveness(
        vec![1],
        vec![3],
        vec![3],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put_key_with(&mut c, b"key8", Some(QUORUM_ONE), Some(2), None).await;
    assert_eq!(
        code,
        MessageCode::ErrorResp.as_u8(),
        "PW=2 must fail when only a fallback (not a second primary) acks"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn primary_write_quorum_succeeds_when_enough_primaries_ack() {
    // Same topology, but peer 2 (a real primary) also acks. PW=2 is
    // now met by the local primary ack + peer 2's primary ack.
    let (addr, server) = spawn_with_liveness(
        vec![1],
        vec![2, 3],
        vec![2, 3],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put_key_with(&mut c, b"key8", Some(QUORUM_ONE), Some(2), None).await;
    assert_eq!(
        code,
        MessageCode::PutResp.as_u8(),
        "PW=2 succeeds once two primaries ack, even with a fallback in play"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn primary_read_quorum_fails_when_a_primary_is_silent_even_if_a_fallback_answers() {
    // Same topology (key = "key8", peer 1 down, peer 3 stands in as a
    // fallback, primaries = [local peer 0, peer 2]). Only peer 3 (the
    // fallback) is reachable; peer 2 (a real primary) is silent. A
    // read with PR=2 must fail: local (one primary) plus peer 3's
    // FALLBACK response is not two PRIMARY responses, and a fallback
    // response may never count toward PR.
    let (addr, server) = spawn_with_liveness(
        vec![1],
        vec![3],
        vec![3],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    // Put once (local + fallback ack) so the coordinator holds the
    // object locally for the read below.
    let code = put_key_with(&mut c, b"key8", Some(QUORUM_ONE), None, None).await;
    assert_eq!(code, MessageCode::PutResp.as_u8(), "seed put must succeed");
    let get = RpbGetReq {
        bucket: b"b".to_vec(),
        key: b"key8".to_vec(),
        pr: Some(2),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, _) = recv_frame(&mut c).await;
    assert_eq!(
        code,
        MessageCode::ErrorResp.as_u8(),
        "PR=2 must fail when only the fallback (peer 3), not a second \
         primary, answers the read"
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn primary_read_quorum_succeeds_when_enough_primaries_answer() {
    // Same topology, but peer 2 (a real primary) also answers reads.
    // PR=2 is met by the local primary response + peer 2's primary
    // response.
    let (addr, server) = spawn_with_liveness(
        vec![1],
        vec![2, 3],
        vec![2, 3],
        BucketProps {
            n_val: Some(3),
            ..BucketProps::default()
        },
    )
    .await;
    let mut c = TcpStream::connect(addr).await.expect("connect");
    let code = put_key_with(&mut c, b"key8", Some(QUORUM_ALL), None, None).await;
    assert_eq!(code, MessageCode::PutResp.as_u8(), "seed put must succeed");
    let get = RpbGetReq {
        bucket: b"b".to_vec(),
        key: b"key8".to_vec(),
        pr: Some(2),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, _) = recv_frame(&mut c).await;
    assert_eq!(
        code,
        MessageCode::GetResp.as_u8(),
        "PR=2 succeeds once two primaries answer, even with a fallback in play"
    );
    server.abort();
    let _ = server.await;
}
