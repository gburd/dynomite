//! Postcommit-hook notification over the PBC put path.
//!
//! A bucket with a `postcommit_module` runs each committed write
//! through a WASM hook, fire-and-forget: the hook's result never
//! affects the put response. This drives the notification path end to
//! end, including the case where the hook itself fails.

#![cfg(all(feature = "noxu", feature = "wasm"))]

use std::sync::{Arc, Mutex};

use dyniak::bucket_props::{BucketProps, BucketPropsRegistry};
use dyniak::mapreduce::wasm::WasmModuleStore;
use dyniak::precommit::PostcommitHooks;
use dyniak::proto::pb::{MessageCode, RpbContent, RpbGetReq, RpbGetResp, RpbPutReq};
use dyniak::replication::{RingPoint, RingView};
use dyniak::router::{BucketRouter, PostcommitRunner, RoutingHooks};
use dynomite::embed::Datastore;
use dynomite::hashkit::HashType;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A postcommit hook that returns 0 (success) and echoes the value.
const NOTIFY_OK_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $heap_top (mut i32) (i32.const 1024))
      (func $alloc_inner (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap_top))
        (global.set $heap_top (i32.add (global.get $heap_top) (local.get $len)))
        (local.get $ptr))
      (func (export "hook_alloc") (param $len i32) (result i32)
        (call $alloc_inner (local.get $len)))
      (func (export "postcommit")
        (param $in_ptr i32) (param $in_len i32)
        (param $out_ptr_ptr i32) (param $out_len_ptr i32)
        (result i32)
        (local $out_buf i32)
        (local.set $out_buf (call $alloc_inner (local.get $in_len)))
        (memory.copy (local.get $out_buf) (local.get $in_ptr) (local.get $in_len))
        (i32.store (local.get $out_ptr_ptr) (local.get $out_buf))
        (i32.store (local.get $out_len_ptr) (local.get $in_len))
        (i32.const 0)))
"#;

/// A postcommit hook that always fails (returns a non-zero status).
/// Since postcommit is fire-and-forget, this must not affect the put.
const NOTIFY_FAIL_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (data (i32.const 2048) "side effect failed")
      (global $heap_top (mut i32) (i32.const 1024))
      (func (export "hook_alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap_top))
        (global.set $heap_top (i32.add (global.get $heap_top) (local.get $len)))
        (local.get $ptr))
      (func (export "postcommit")
        (param $in_ptr i32) (param $in_len i32)
        (param $out_ptr_ptr i32) (param $out_len_ptr i32)
        (result i32)
        (i32.store (local.get $out_ptr_ptr) (i32.const 2048))
        (i32.store (local.get $out_len_ptr) (i32.const 19))
        (i32.const 1)))
"#;

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

/// Wraps a real [`PostcommitHooks`] engine and records every value it
/// was asked to run, so the test can observe that the hook actually
/// ran over the committed value without needing the WASM module
/// itself to call back into the host.
#[derive(Debug)]
struct RecordingPostcommit {
    inner: PostcommitHooks,
    seen: Mutex<Vec<Vec<u8>>>,
}

impl PostcommitRunner for RecordingPostcommit {
    fn run(&self, module_id: &str, value: &[u8]) {
        self.seen.lock().expect("mutex").push(value.to_vec());
        self.inner.run(module_id, value);
    }
}

fn one_peer_router(registry: Arc<BucketPropsRegistry>) -> Arc<BucketRouter> {
    let span = u64::from(u32::MAX);
    let pts: Vec<RingPoint> = (0..1u32)
        .map(|i| RingPoint::new(u64::from(i) * span, i, "dc1", "r1"))
        .collect();
    Arc::new(BucketRouter::new(
        registry,
        Arc::new(RingView::new(pts)),
        HashType::Murmur,
    ))
}

#[tokio::test]
async fn postcommit_hook_runs_after_a_successful_put() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ds: Arc<dyn Datastore> =
        Arc::new(dyniak::datastore::NoxuDatastore::open_transactional(dir.path()).expect("noxu"));

    let wasm = Arc::new(WasmModuleStore::new().expect("wasm store"));
    let postcommit = PostcommitHooks::new(wasm);
    postcommit
        .register("notify", NOTIFY_OK_WAT.as_bytes())
        .expect("register");
    let recorder = Arc::new(RecordingPostcommit {
        inner: postcommit,
        seen: Mutex::new(Vec::new()),
    });

    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"",
        b"notified",
        BucketProps {
            postcommit_module: Some("notify".to_string()),
            ..BucketProps::default()
        },
    );
    let router = one_peer_router(registry);
    let hooks = RoutingHooks {
        router,
        outbound: Arc::new(NoopOutbound) as Arc<dyn dyniak::router::PeerOutbound>,
        local_actor: dyniak::datatypes::ActorId::new("dc1", "n0"),
        local_peer_idx: 0,
        precommit: None,
        postcommit: Some(recorder.clone() as Arc<dyn PostcommitRunner>),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let admin = Arc::new(dynomite::cluster::admin_rpc::NoopClusterAdmin);
    let server = tokio::spawn(async move {
        let _ = dyniak::server::serve_pbc_with_routing(listener, ds, admin, hooks).await;
    });
    let mut c = TcpStream::connect(addr).await.expect("connect");

    // A put to the notified bucket succeeds: PutResp, not an error.
    let put = RpbPutReq {
        bucket: b"notified".to_vec(),
        key: Some(b"k".to_vec()),
        content: Some(RpbContent {
            value: b"hello".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    send_frame(&mut c, MessageCode::PutReq.as_u8(), &put.encode_to_vec()).await;
    let (code, _body) = recv_frame(&mut c).await;
    assert_eq!(
        code,
        MessageCode::PutResp.as_u8(),
        "a put with a postcommit hook still succeeds"
    );

    // The write actually committed: a get finds the stored value.
    let get = RpbGetReq {
        bucket: b"notified".to_vec(),
        key: b"k".to_vec(),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(code, MessageCode::GetResp.as_u8());
    let resp = RpbGetResp::decode(body.as_slice()).expect("get resp");
    assert_eq!(resp.content.len(), 1);
    assert_eq!(resp.content[0].value, b"hello");

    // The hook observed the committed value (proving it ran, and ran
    // over the value that was actually stored, not some earlier or
    // transformed copy).
    let seen = {
        let guard = recorder.seen.lock().expect("mutex");
        guard.clone()
    };
    assert_eq!(seen.as_slice(), &[b"hello".to_vec()]);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn a_failing_postcommit_hook_never_fails_the_put() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ds: Arc<dyn Datastore> =
        Arc::new(dyniak::datastore::NoxuDatastore::open_transactional(dir.path()).expect("noxu"));

    let wasm = Arc::new(WasmModuleStore::new().expect("wasm store"));
    let postcommit = PostcommitHooks::new(wasm);
    postcommit
        .register("flaky", NOTIFY_FAIL_WAT.as_bytes())
        .expect("register");

    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"",
        b"flaky-bucket",
        BucketProps {
            postcommit_module: Some("flaky".to_string()),
            ..BucketProps::default()
        },
    );
    let router = one_peer_router(registry);
    let hooks = RoutingHooks {
        router,
        outbound: Arc::new(NoopOutbound) as Arc<dyn dyniak::router::PeerOutbound>,
        local_actor: dyniak::datatypes::ActorId::new("dc1", "n0"),
        local_peer_idx: 0,
        precommit: None,
        postcommit: Some(Arc::new(postcommit) as Arc<dyn PostcommitRunner>),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let admin = Arc::new(dynomite::cluster::admin_rpc::NoopClusterAdmin);
    let server = tokio::spawn(async move {
        let _ = dyniak::server::serve_pbc_with_routing(listener, ds, admin, hooks).await;
    });
    let mut c = TcpStream::connect(addr).await.expect("connect");

    let put = RpbPutReq {
        bucket: b"flaky-bucket".to_vec(),
        key: Some(b"k".to_vec()),
        content: Some(RpbContent {
            value: b"v".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    send_frame(&mut c, MessageCode::PutReq.as_u8(), &put.encode_to_vec()).await;
    let (code, _body) = recv_frame(&mut c).await;
    assert_eq!(
        code,
        MessageCode::PutResp.as_u8(),
        "a failing postcommit hook is logged, not surfaced to the client"
    );

    // The write committed regardless of the hook's failure.
    let get = RpbGetReq {
        bucket: b"flaky-bucket".to_vec(),
        key: b"k".to_vec(),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(code, MessageCode::GetResp.as_u8());
    let resp = RpbGetResp::decode(body.as_slice()).expect("get resp");
    assert_eq!(resp.content.len(), 1);
    assert_eq!(resp.content[0].value, b"v");

    server.abort();
    let _ = server.await;
}

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
