//! End-to-end integration tests for the bucket-property knobs:
//!
//! * `chash_keyfun: BUCKETONLY` -- two PUTs to the same bucket
//!   but different keys land on the same primary peer.
//! * `replication_strategy: SUCCESSORS` with `n_val = 3` --
//!   one PUT fans out to exactly 3 distinct peer outbound
//!   channels.
//!
//! The tests spin up [`dyniak::serve_pbc_with_routing`] over a
//! real `tokio::net::TcpListener`, drive PBC frames through a
//! `tokio::net::TcpStream`, and observe per-peer outbound
//! dispatches via a fixture [`PeerOutbound`] implementation that
//! records calls into per-peer `tokio::sync::mpsc::UnboundedSender`s.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message as _;
use tokio::io::{ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use dynomite::cluster::admin_rpc::NoopClusterAdmin;
use dynomite::embed::hooks::BoxFuture;
use dynomite::embed::{Datastore, MemoryDatastore};
use dynomite::hashkit::HashType;

use dyniak::bucket_props::{BucketProps, BucketPropsRegistry};
use dyniak::datatypes::keyfun::KeyFun;
use dyniak::error::RiakError;
use dyniak::proto::pb::{
    read_frame, write_frame, Frame, MessageCode, RpbBucketProps, RpbContent, RpbGetBucketReq,
    RpbGetBucketResp, RpbPutReq, RpbPutResp, RpbSetBucketReq, RpbSetBucketResp,
    CHASH_KEYFUN_BUCKETONLY, REPLICATION_STRATEGY_SUCCESSORS,
};
#[cfg(feature = "wasm")]
use dyniak::proto::pb::{RpbErrorResp, CHASH_KEYFUN_CUSTOM};
use dyniak::replication::{ReplicationStrategy, RingPoint, RingView};
use dyniak::router::{BucketRouter, PeerOp, PeerOutbound, RoutingHooks};
use dyniak::serve_pbc_with_routing;

/// Optional keyfun store handed to the harness. With the `wasm`
/// feature it carries a real store; without it the parameter is a
/// unit placeholder so the storeless harness still compiles.
#[cfg(feature = "wasm")]
type KeyfunStoreParam = Option<dyniak::datatypes::keyfun_wasm::WasmKeyfunStore>;
#[cfg(not(feature = "wasm"))]
type KeyfunStoreParam = Option<()>;

type Reader = ReadHalf<TcpStream>;
type Writer = WriteHalf<TcpStream>;

/// Fixture [`PeerOutbound`] that pushes every dispatch onto a
/// per-peer unbounded `mpsc` channel held in a shared map. The
/// test driver reads from each channel to assert which peers
/// saw which ops.
#[derive(Debug)]
struct RecordingOutbound {
    senders: Mutex<HashMap<u32, mpsc::UnboundedSender<PeerOp>>>,
}

impl RecordingOutbound {
    fn new(peer_count: u32) -> (Arc<Self>, HashMap<u32, mpsc::UnboundedReceiver<PeerOp>>) {
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();
        for i in 0..peer_count {
            let (tx, rx) = mpsc::unbounded_channel();
            senders.insert(i, tx);
            receivers.insert(i, rx);
        }
        (
            Arc::new(Self {
                senders: Mutex::new(senders),
            }),
            receivers,
        )
    }
}

impl PeerOutbound for RecordingOutbound {
    fn dispatch(&self, peer_idx: u32, op: PeerOp) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let senders = self.senders.lock().expect("recording-outbound poisoned");
            if let Some(tx) = senders.get(&peer_idx) {
                let _ = tx.send(op);
            }
        })
    }
}

fn five_peer_ring() -> Arc<RingView> {
    let span = u64::from(u32::MAX);
    let pts: Vec<RingPoint> = (0..5u32)
        .map(|i| RingPoint::new(u64::from(i) * span / 5, i, "dc1", "r1"))
        .collect();
    Arc::new(RingView::new(pts))
}

struct Harness {
    server: JoinHandle<Result<(), RiakError>>,
    reader: Reader,
    writer: Writer,
    receivers: HashMap<u32, mpsc::UnboundedReceiver<PeerOp>>,
    _registry: Arc<BucketPropsRegistry>,
}

impl Harness {
    async fn start(props: BucketProps) -> Self {
        Self::start_inner(props, None).await
    }

    /// As [`Self::start`], but attaches a custom-keyfun WASM store
    /// (carrying one WAT module registered under `keyfun_id`) so
    /// the CUSTOM bucket-props validation path can be exercised.
    #[cfg(feature = "wasm")]
    async fn start_with_keyfun(props: BucketProps, keyfun_id: &str) -> Self {
        // A trivial bucket-only keyfun WAT: returns the bucket
        // bytes. Enough for the validation path (registration
        // presence) without compiling Rust.
        const BUCKET_KEYFUN_WAT: &str = r#"
            (module
              (memory (export "memory") 1)
              (global $top (mut i32) (i32.const 1024))
              (func $alloc (param $len i32) (result i32)
                (local $p i32)
                (local.set $p (global.get $top))
                (global.set $top (i32.add (global.get $top) (local.get $len)))
                (local.get $p))
              (func (export "keyfun_alloc") (param $len i32) (result i32)
                (call $alloc (local.get $len)))
              (func (export "keyfun_route")
                (param $in_ptr i32) (param $in_len i32)
                (param $out_ptr_ptr i32) (param $out_len_ptr i32) (result i32)
                (local $blen i32) (local $out i32) (local $i i32)
                (local.set $blen (i32.load (local.get $in_ptr)))
                (local.set $out (call $alloc (local.get $blen)))
                (local.set $i (i32.const 0))
                (block $done (loop $loop
                  (br_if $done (i32.ge_s (local.get $i) (local.get $blen)))
                  (i32.store8 (i32.add (local.get $out) (local.get $i))
                    (i32.load8_u (i32.add (i32.add (local.get $in_ptr) (i32.const 4)) (local.get $i))))
                  (local.set $i (i32.add (local.get $i) (i32.const 1)))
                  (br $loop)))
                (i32.store (local.get $out_ptr_ptr) (local.get $out))
                (i32.store (local.get $out_len_ptr) (local.get $blen))
                (i32.const 0)))
        "#;
        let store = dyniak::datatypes::keyfun_wasm::WasmKeyfunStore::new().expect("keyfun store");
        store
            .register(keyfun_id, BUCKET_KEYFUN_WAT.as_bytes())
            .expect("register keyfun wat");
        let router_store = Some(store);
        Self::start_inner(props, router_store).await
    }

    async fn start_inner(props: BucketProps, keyfun: KeyfunStoreParam) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
        registry.set(b"default", b"users", props);

        // Use 32-bit hash so the hash output stays within the
        // 5-peer u32 token range for deterministic routing.
        let router = BucketRouter::new(registry.clone(), five_peer_ring(), HashType::Murmur);
        #[cfg(feature = "wasm")]
        let router = match keyfun {
            Some(store) => router.with_keyfun_store(store),
            None => router,
        };
        #[cfg(not(feature = "wasm"))]
        let _ = keyfun;
        let router = Arc::new(router);

        let (outbound, receivers) = RecordingOutbound::new(5);
        let hooks = RoutingHooks {
            router,
            outbound: outbound as Arc<dyn PeerOutbound>,
        };

        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let admin = Arc::new(NoopClusterAdmin);
        let server =
            tokio::spawn(async move { serve_pbc_with_routing(listener, ds, admin, hooks).await });

        // Give tokio a chance to enter the accept loop.
        tokio::time::sleep(Duration::from_millis(10)).await;

        let stream = TcpStream::connect(addr).await.expect("connect");
        let (reader, writer) = tokio::io::split(stream);
        Self {
            server,
            reader,
            writer,
            receivers,
            _registry: registry,
        }
    }

    async fn shutdown(self) {
        drop(self.reader);
        drop(self.writer);
        self.server.abort();
        let _ = self.server.await;
    }

    async fn put(&mut self, bucket: &[u8], key: &[u8], value: &[u8]) {
        let body = RpbPutReq {
            bucket: bucket.to_vec(),
            key: Some(key.to_vec()),
            vclock: None,
            content: Some(RpbContent {
                value: value.to_vec(),
                ..RpbContent::default()
            }),
            r#type: None,
            ..RpbPutReq::default()
        }
        .encode_to_vec();
        let frame = Frame::new(MessageCode::PutReq.as_u8(), body);
        write_frame(&mut self.writer, &frame).await.expect("write");
        // Read the put response so the reply does not pile up
        // on the listener side and confuse the next op.
        let resp = read_frame(&mut self.reader).await.expect("put resp");
        assert_eq!(resp.code, MessageCode::PutResp.as_u8());
        let _ = RpbPutResp::decode(resp.body.as_slice()).expect("decode put resp");
    }

    fn drain_dispatches(&mut self) -> HashMap<u32, Vec<PeerOp>> {
        let mut out: HashMap<u32, Vec<PeerOp>> = HashMap::new();
        for (peer, rx) in &mut self.receivers {
            let mut bucket = Vec::new();
            while let Ok(op) = rx.try_recv() {
                bucket.push(op);
            }
            if !bucket.is_empty() {
                out.insert(*peer, bucket);
            }
        }
        out
    }
}

#[tokio::test]
async fn bucketonly_keyfun_routes_two_keys_to_same_primary() {
    let mut h = Harness::start(BucketProps {
        keyfun: Some(KeyFun::BucketOnly),
        strategy: Some(ReplicationStrategy::Successors),
        // n_val = 1 so we can read off the primary directly
        // without inspecting successors.
        n_val: Some(1),
        ..BucketProps::default()
    })
    .await;

    h.put(b"users", b"alice", b"alice-value").await;
    h.put(b"users", b"bob", b"bob-value").await;

    let dispatches = h.drain_dispatches();
    assert_eq!(
        dispatches.len(),
        1,
        "BUCKETONLY routes both keys to one peer; saw {dispatches:?}"
    );
    let (_peer, ops) = dispatches.into_iter().next().expect("one entry");
    assert_eq!(ops.len(), 2, "two PUTs land on the same peer");
    for op in &ops {
        match op {
            PeerOp::Put { bucket, .. } => assert_eq!(bucket, b"users"),
            other => panic!("expected Put op, got {other:?}"),
        }
    }
    h.shutdown().await;
}

#[tokio::test]
async fn successors_strategy_fans_one_put_out_to_three_peers() {
    let mut h = Harness::start(BucketProps {
        keyfun: Some(KeyFun::Std),
        strategy: Some(ReplicationStrategy::Successors),
        n_val: Some(3),
        ..BucketProps::default()
    })
    .await;

    h.put(b"users", b"alice", b"alice-value").await;

    let dispatches = h.drain_dispatches();
    assert_eq!(
        dispatches.len(),
        3,
        "Successors n_val=3 fans out to 3 distinct peers; saw {dispatches:?}"
    );
    for (peer, ops) in &dispatches {
        assert_eq!(ops.len(), 1, "peer {peer} sees exactly one op");
        match &ops[0] {
            PeerOp::Put {
                bucket, key, value, ..
            } => {
                assert_eq!(bucket, b"users");
                assert_eq!(key, b"alice");
                assert_eq!(value, b"alice-value");
            }
            other => panic!("expected Put op, got {other:?}"),
        }
    }
    h.shutdown().await;
}

#[tokio::test]
async fn topology_strategy_does_not_fan_out_via_successors_path() {
    // With Topology strategy, the router returns an empty
    // replica list; the existing topology pipeline (not
    // exercised in this test) is the source of truth. The
    // recording outbound therefore sees zero dispatches.
    let mut h = Harness::start(BucketProps {
        keyfun: Some(KeyFun::Std),
        strategy: Some(ReplicationStrategy::Topology),
        n_val: Some(3),
        ..BucketProps::default()
    })
    .await;

    h.put(b"users", b"alice", b"alice-value").await;

    let dispatches = h.drain_dispatches();
    assert!(
        dispatches.is_empty(),
        "Topology mode skips the per-replica fan-out hook; saw {dispatches:?}"
    );
    h.shutdown().await;
}

#[tokio::test]
async fn set_bucket_round_trips_through_get_bucket() {
    let mut h = Harness::start(BucketProps::default()).await;

    // GET-BUCKET: the registry was configured Riak-defaults so
    // Successors should be reflected back to the client.
    let body = RpbGetBucketReq {
        bucket: b"users".to_vec(),
        r#type: Some(b"default".to_vec()),
    }
    .encode_to_vec();
    let frame = Frame::new(MessageCode::GetBucketReq.as_u8(), body);
    write_frame(&mut h.writer, &frame).await.expect("write");
    let resp = read_frame(&mut h.reader).await.expect("read");
    let resp = RpbGetBucketResp::decode(resp.body.as_slice()).expect("decode");
    let props = resp.props.expect("props present");
    assert_eq!(
        props.replication_strategy,
        Some(REPLICATION_STRATEGY_SUCCESSORS),
        "Riak-mode default is Successors"
    );

    h.shutdown().await;
}

#[tokio::test]
async fn pbc_set_bucket_persists_chash_keyfun() {
    let mut h = Harness::start(BucketProps::default()).await;

    // SET-BUCKET to BUCKETONLY.
    let body = RpbSetBucketReq {
        bucket: b"users".to_vec(),
        props: Some(RpbBucketProps {
            chash_keyfun: Some(CHASH_KEYFUN_BUCKETONLY),
            n_val: Some(3),
            ..RpbBucketProps::default()
        }),
        r#type: Some(b"default".to_vec()),
    }
    .encode_to_vec();
    let frame = Frame::new(MessageCode::SetBucketReq.as_u8(), body);
    write_frame(&mut h.writer, &frame).await.expect("write");
    let resp = read_frame(&mut h.reader).await.expect("read");
    let _ = RpbSetBucketResp::decode(resp.body.as_slice()).expect("decode set-bucket resp");

    // GET-BUCKET reads the persisted choice back.
    let body = RpbGetBucketReq {
        bucket: b"users".to_vec(),
        r#type: Some(b"default".to_vec()),
    }
    .encode_to_vec();
    let frame = Frame::new(MessageCode::GetBucketReq.as_u8(), body);
    write_frame(&mut h.writer, &frame).await.expect("write");
    let resp = read_frame(&mut h.reader).await.expect("read");
    let resp = RpbGetBucketResp::decode(resp.body.as_slice()).expect("decode");
    let props = resp.props.expect("props present");
    assert_eq!(props.chash_keyfun, Some(CHASH_KEYFUN_BUCKETONLY));

    h.shutdown().await;
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn pbc_set_bucket_custom_unregistered_module_is_rejected() {
    // A keyfun store is wired but the named module is NOT
    // registered. The SET-BUCKET write must be rejected with a
    // clean error frame (not silently accepted, never a panic),
    // so routing never changes to a missing keyfun.
    let mut h = Harness::start_with_keyfun(BucketProps::default(), "present").await;
    let body = RpbSetBucketReq {
        bucket: b"users".to_vec(),
        props: Some(RpbBucketProps {
            chash_keyfun: Some(CHASH_KEYFUN_CUSTOM),
            chash_keyfun_module: Some(b"absent".to_vec()),
            n_val: Some(3),
            ..RpbBucketProps::default()
        }),
        r#type: Some(b"default".to_vec()),
    }
    .encode_to_vec();
    let frame = Frame::new(MessageCode::SetBucketReq.as_u8(), body);
    write_frame(&mut h.writer, &frame).await.expect("write");
    let resp = read_frame(&mut h.reader).await.expect("read");
    assert_eq!(
        resp.code,
        MessageCode::ErrorResp.as_u8(),
        "CUSTOM naming an unregistered module is rejected"
    );
    let err = RpbErrorResp::decode(resp.body.as_slice()).expect("decode error");
    let msg = String::from_utf8_lossy(&err.errmsg);
    assert!(msg.contains("not registered"), "unexpected error: {msg}");

    h.shutdown().await;
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn pbc_set_bucket_custom_empty_module_is_rejected() {
    // CUSTOM with no module id named at all is rejected too.
    let mut h = Harness::start_with_keyfun(BucketProps::default(), "present").await;
    let body = RpbSetBucketReq {
        bucket: b"users".to_vec(),
        props: Some(RpbBucketProps {
            chash_keyfun: Some(CHASH_KEYFUN_CUSTOM),
            n_val: Some(3),
            ..RpbBucketProps::default()
        }),
        r#type: Some(b"default".to_vec()),
    }
    .encode_to_vec();
    let frame = Frame::new(MessageCode::SetBucketReq.as_u8(), body);
    write_frame(&mut h.writer, &frame).await.expect("write");
    let resp = read_frame(&mut h.reader).await.expect("read");
    assert_eq!(resp.code, MessageCode::ErrorResp.as_u8());

    h.shutdown().await;
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn pbc_set_bucket_custom_registered_module_round_trips() {
    // CUSTOM naming a registered module is accepted and the
    // selector + module id round-trip through GET-BUCKET.
    let mut h = Harness::start_with_keyfun(BucketProps::default(), "present").await;
    let body = RpbSetBucketReq {
        bucket: b"users".to_vec(),
        props: Some(RpbBucketProps {
            chash_keyfun: Some(CHASH_KEYFUN_CUSTOM),
            chash_keyfun_module: Some(b"present".to_vec()),
            n_val: Some(3),
            ..RpbBucketProps::default()
        }),
        r#type: Some(b"default".to_vec()),
    }
    .encode_to_vec();
    let frame = Frame::new(MessageCode::SetBucketReq.as_u8(), body);
    write_frame(&mut h.writer, &frame).await.expect("write");
    let resp = read_frame(&mut h.reader).await.expect("read");
    assert_eq!(
        resp.code,
        MessageCode::SetBucketResp.as_u8(),
        "CUSTOM with a registered module is accepted"
    );

    let body = RpbGetBucketReq {
        bucket: b"users".to_vec(),
        r#type: Some(b"default".to_vec()),
    }
    .encode_to_vec();
    let frame = Frame::new(MessageCode::GetBucketReq.as_u8(), body);
    write_frame(&mut h.writer, &frame).await.expect("write");
    let resp = read_frame(&mut h.reader).await.expect("read");
    let resp = RpbGetBucketResp::decode(resp.body.as_slice()).expect("decode");
    let props = resp.props.expect("props present");
    assert_eq!(props.chash_keyfun, Some(CHASH_KEYFUN_CUSTOM));
    assert_eq!(props.chash_keyfun_module.as_deref(), Some(&b"present"[..]));

    h.shutdown().await;
}
