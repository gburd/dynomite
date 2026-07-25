//! Per-object causal context (vclock) round-trip over the PBC wire.
//!
//! A PUT returns the object's advanced causal context in
//! `RpbPutResp.vclock`; a GET returns the stored context in
//! `RpbGetResp.vclock`; a second PUT advances the context so it
//! strictly dominates the first (proving each write tracks causality).

#![cfg(feature = "noxu")]

use std::sync::Arc;

use dyniak::proto::pb::{MessageCode, RpbContent, RpbGetReq, RpbGetResp, RpbPutReq, RpbPutResp};
use dyniak::server::serve_pbc;
use dyniak::vclock::VClock;
use dynomite::embed::Datastore;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn send_frame(stream: &mut TcpStream, code: u8, body: &[u8]) {
    let len = u32::try_from(body.len() + 1).expect("frame length fits u32");
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
    let code = buf[0];
    (code, buf[1..].to_vec())
}

#[tokio::test]
async fn put_returns_vclock_get_returns_it_and_writes_advance_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ds: Arc<dyn Datastore> =
        Arc::new(dyniak::datastore::NoxuDatastore::open_transactional(dir.path()).expect("noxu"));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = serve_pbc(listener, ds).await;
    });

    let mut c = TcpStream::connect(addr).await.expect("connect");

    // First PUT (no client vclock -> a new object).
    let put1 = RpbPutReq {
        bucket: b"cart".to_vec(),
        key: Some(b"k1".to_vec()),
        content: Some(RpbContent {
            value: b"v1".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    send_frame(&mut c, MessageCode::PutReq.as_u8(), &put1.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(code, MessageCode::PutResp.as_u8());
    let resp1 = RpbPutResp::decode(body.as_slice()).expect("put resp");
    let vclock1 = resp1.vclock.expect("put returns a vclock");
    assert!(!vclock1.is_empty(), "first write has a non-empty context");
    let clock1 = VClock::decode(&vclock1);

    // GET returns the same context.
    let get = RpbGetReq {
        bucket: b"cart".to_vec(),
        key: b"k1".to_vec(),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(code, MessageCode::GetResp.as_u8());
    let gresp = RpbGetResp::decode(body.as_slice()).expect("get resp");
    assert_eq!(
        gresp.vclock.as_deref(),
        Some(vclock1.as_slice()),
        "GET returns the stored context"
    );

    // Second PUT carrying the read context: the new context must
    // strictly dominate the first.
    let put2 = RpbPutReq {
        bucket: b"cart".to_vec(),
        key: Some(b"k1".to_vec()),
        vclock: Some(vclock1.clone()),
        content: Some(RpbContent {
            value: b"v2".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    send_frame(&mut c, MessageCode::PutReq.as_u8(), &put2.encode_to_vec()).await;
    let (_code, body) = recv_frame(&mut c).await;
    let resp2 = RpbPutResp::decode(body.as_slice()).expect("put resp 2");
    let vclock2 = resp2.vclock.expect("put 2 returns a vclock");
    let clock2 = VClock::decode(&vclock2);
    assert_eq!(
        clock2.partial_cmp(&clock1),
        Some(std::cmp::Ordering::Greater),
        "the second write's context strictly dominates the first"
    );

    server.abort();
    let _ = server.await;
}

/// A no-op outbound: this test exercises local sibling storage, so the
/// replica fan is a sink.
#[derive(Debug)]
struct NoopOutbound;

impl dyniak::router::PeerOutbound for NoopOutbound {
    fn dispatch(
        &self,
        _peer_idx: u32,
        _op: dyniak::router::PeerOp,
    ) -> dynomite::embed::hooks::BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[tokio::test]
async fn concurrent_writes_surface_as_siblings_under_allow_mult() {
    use dyniak::bucket_props::{BucketProps, BucketPropsRegistry};
    use dyniak::replication::{RingPoint, RingView};
    use dyniak::router::{BucketRouter, RoutingHooks};
    use dyniak::server::serve_pbc_with_routing;
    use dynomite::cluster::admin_rpc::NoopClusterAdmin;
    use dynomite::hashkit::HashType;

    let dir = tempfile::tempdir().expect("tempdir");
    let ds: Arc<dyn Datastore> =
        Arc::new(dyniak::datastore::NoxuDatastore::open_transactional(dir.path()).expect("noxu"));

    // Registry with allow_mult = true for the default bucket type.
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"",
        b"cart",
        BucketProps {
            allow_mult: Some(true),
            ..BucketProps::default()
        },
    );
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
        outbound: Arc::new(NoopOutbound) as Arc<dyn dyniak::router::PeerOutbound>,
        local_actor: dyniak::datatypes::ActorId::new("dc1", "local"),
        local_peer_idx: 0,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let admin = Arc::new(NoopClusterAdmin);
    let server = tokio::spawn(async move {
        let _ = serve_pbc_with_routing(listener, ds, admin, hooks).await;
    });
    let mut c = TcpStream::connect(addr).await.expect("connect");

    // Two writes that BOTH read no prior context (both blind) are
    // concurrent. Under allow_mult a GET must surface both siblings.
    for v in [b"red".as_slice(), b"blue".as_slice()] {
        let put = RpbPutReq {
            bucket: b"cart".to_vec(),
            key: Some(b"k".to_vec()),
            content: Some(RpbContent {
                value: v.to_vec(),
                ..RpbContent::default()
            }),
            ..RpbPutReq::default()
        };
        send_frame(&mut c, MessageCode::PutReq.as_u8(), &put.encode_to_vec()).await;
        let (code, _) = recv_frame(&mut c).await;
        assert_eq!(code, MessageCode::PutResp.as_u8());
    }

    let get = RpbGetReq {
        bucket: b"cart".to_vec(),
        key: b"k".to_vec(),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(code, MessageCode::GetResp.as_u8());
    let resp = RpbGetResp::decode(body.as_slice()).expect("get resp");
    assert_eq!(
        resp.content.len(),
        2,
        "two concurrent writes surface as two siblings under allow_mult"
    );
    let values: std::collections::BTreeSet<Vec<u8>> =
        resp.content.iter().map(|c| c.value.clone()).collect();
    assert!(values.contains(b"red".as_slice()));
    assert!(values.contains(b"blue".as_slice()));

    server.abort();
    let _ = server.await;
}

/// An outbound backed by one datastore per peer, answering `Get`
/// read-coordination queries and applying `RepairPut`s. Lets a
/// coordinated read merge sibling sets held on distinct replicas.
#[derive(Clone)]
struct PerPeerStores {
    stores: std::sync::Arc<std::collections::HashMap<u32, Arc<dyniak::datastore::NoxuDatastore>>>,
}

impl std::fmt::Debug for PerPeerStores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PerPeerStores").finish_non_exhaustive()
    }
}

impl dyniak::router::PeerOutbound for PerPeerStores {
    fn dispatch(
        &self,
        peer_idx: u32,
        op: dyniak::router::PeerOp,
    ) -> dynomite::embed::hooks::BoxFuture<'_, ()> {
        Box::pin(async move {
            if let dyniak::router::PeerOp::RepairPut {
                bucket,
                key,
                storage,
                ..
            } = op
            {
                if let Some(ds) = self.stores.get(&peer_idx) {
                    let _ = ds.put_object(&bucket, &key, &storage, &[]);
                }
            }
        })
    }

    fn request(
        &self,
        peer_idx: u32,
        op: dyniak::router::PeerOp,
    ) -> dynomite::embed::hooks::BoxFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move {
            let dyniak::router::PeerOp::Get { bucket, key, .. } = op else {
                return None;
            };
            let ds = self.stores.get(&peer_idx)?;
            match ds.get_object(&bucket, &key) {
                Ok(Some(b)) => Some(b),
                _ => Some(Vec::new()),
            }
        })
    }
}

#[tokio::test]
async fn coordinated_read_merges_sibling_sets_across_replicas() {
    use dyniak::bucket_props::{BucketProps, BucketPropsRegistry};
    use dyniak::proto::http::object::{HttpObject, SiblingSet};
    use dyniak::replication::{RingPoint, RingView};
    use dyniak::router::{BucketRouter, RoutingHooks};
    use dynomite::hashkit::HashType;

    // Three peers, each with its own store. Peers 1 and 2 hold DIFFERENT
    // concurrent siblings of the same key; peer 0 (the coordinator)
    // holds neither. A coordinated read at peer 0 must fan Get to the
    // replica set, merge the two siblings, and return both.
    let dirs: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let mut map = std::collections::HashMap::new();
    for (i, d) in dirs.iter().enumerate() {
        let ds = dyniak::datastore::NoxuDatastore::open_transactional(d.path()).expect("noxu");
        map.insert(u32::try_from(i).expect("peer index fits u32"), Arc::new(ds));
    }
    let stores = PerPeerStores {
        stores: std::sync::Arc::new(map),
    };

    // Seed peer 1 with sibling "red" (context {n1:1}) and peer 2 with
    // sibling "blue" (context {n2:1}) -- concurrent.
    let mk = |val: &[u8], actor: &[u8]| -> Vec<u8> {
        let ctx = dyniak::vclock::VClock::decode(&[]);
        let mut ctx = ctx;
        ctx.advance(actor);
        SiblingSet::single(HttpObject {
            value: val.to_vec(),
            context: ctx.encode(),
            ..HttpObject::default()
        })
        .to_storage_bytes()
    };
    stores.stores[&1]
        .put_object(b"cart", b"k", &mk(b"red", b"n1"), &[])
        .expect("seed n1");
    stores.stores[&2]
        .put_object(b"cart", b"k", &mk(b"blue", b"n2"), &[])
        .expect("seed n2");

    // Coordinator = peer 0, its own (empty) store is the serve_pbc ds.
    let coord_ds: Arc<dyn Datastore> = stores.stores[&0].clone();
    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"",
        b"cart",
        BucketProps {
            allow_mult: Some(true),
            n_val: Some(3),
            ..BucketProps::default()
        },
    );
    // Degenerate ring: all three peers at distinct tokens; n_val=3 puts
    // every key on all three.
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
        outbound: Arc::new(stores.clone()) as Arc<dyn dyniak::router::PeerOutbound>,
        local_actor: dyniak::datatypes::ActorId::new("dc1", "n0"),
        local_peer_idx: 0,
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let admin = Arc::new(dynomite::cluster::admin_rpc::NoopClusterAdmin);
    let server = tokio::spawn(async move {
        let _ = dyniak::server::serve_pbc_with_routing(listener, coord_ds, admin, hooks).await;
    });
    let mut c = TcpStream::connect(addr).await.expect("connect");

    let get = RpbGetReq {
        bucket: b"cart".to_vec(),
        key: b"k".to_vec(),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(code, MessageCode::GetResp.as_u8());
    let resp = RpbGetResp::decode(body.as_slice()).expect("get resp");
    let values: std::collections::BTreeSet<Vec<u8>> =
        resp.content.iter().map(|c| c.value.clone()).collect();
    assert_eq!(
        resp.content.len(),
        2,
        "coordinated read merges the two replicas' concurrent siblings"
    );
    assert!(values.contains(b"red".as_slice()));
    assert!(values.contains(b"blue".as_slice()));

    server.abort();
    let _ = server.await;
}
