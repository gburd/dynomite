//! Precommit-hook veto over the PBC put path.
//!
//! A bucket with a `precommit_module` runs each object write through a
//! WASM hook before it commits: the hook may accept (store the value)
//! or reject (veto the write). This drives the veto path end to end.

#![cfg(all(feature = "noxu", feature = "wasm"))]

use std::sync::Arc;

use dyniak::bucket_props::{BucketProps, BucketPropsRegistry};
use dyniak::mapreduce::wasm::WasmModuleStore;
use dyniak::precommit::PrecommitHooks;
use dyniak::proto::pb::{MessageCode, RpbContent, RpbGetReq, RpbGetResp, RpbPutReq};
use dyniak::replication::{RingPoint, RingView};
use dyniak::router::{BucketRouter, PrecommitRunner, RoutingHooks};
use dynomite::embed::Datastore;
use dynomite::hashkit::HashType;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A precommit hook that vetoes every write with reason "denied".
const REJECT_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (data (i32.const 2048) "denied")
      (global $heap_top (mut i32) (i32.const 1024))
      (func (export "hook_alloc") (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap_top))
        (global.set $heap_top (i32.add (global.get $heap_top) (local.get $len)))
        (local.get $ptr))
      (func (export "precommit")
        (param $in_ptr i32) (param $in_len i32)
        (param $out_ptr_ptr i32) (param $out_len_ptr i32)
        (result i32)
        (i32.store (local.get $out_ptr_ptr) (i32.const 2048))
        (i32.store (local.get $out_len_ptr) (i32.const 6))
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

#[tokio::test]
async fn precommit_hook_vetoes_the_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ds: Arc<dyn Datastore> =
        Arc::new(dyniak::datastore::NoxuDatastore::open_transactional(dir.path()).expect("noxu"));

    // Register the veto hook and name it on the bucket.
    let wasm = Arc::new(WasmModuleStore::new().expect("wasm store"));
    let precommit = PrecommitHooks::new(wasm);
    precommit
        .register("veto", REJECT_WAT.as_bytes())
        .expect("register");

    let registry = Arc::new(BucketPropsRegistry::new_riak_defaults());
    registry.set(
        b"",
        b"guarded",
        BucketProps {
            precommit_module: Some("veto".to_string()),
            ..BucketProps::default()
        },
    );
    let span = u64::from(u32::MAX);
    let pts: Vec<RingPoint> = (0..1u32)
        .map(|i| RingPoint::new(u64::from(i) * span, i, "dc1", "r1"))
        .collect();
    let router = Arc::new(BucketRouter::new(
        registry,
        Arc::new(RingView::new(pts)),
        HashType::Murmur,
    ));
    let hooks = RoutingHooks {
        router,
        outbound: Arc::new(NoopOutbound) as Arc<dyn dyniak::router::PeerOutbound>,
        local_actor: dyniak::datatypes::ActorId::new("dc1", "n0"),
        local_peer_idx: 0,
        precommit: Some(Arc::new(precommit) as Arc<dyn PrecommitRunner>),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let admin = Arc::new(dynomite::cluster::admin_rpc::NoopClusterAdmin);
    let server = tokio::spawn(async move {
        let _ = dyniak::server::serve_pbc_with_routing(listener, ds, admin, hooks).await;
    });
    let mut c = TcpStream::connect(addr).await.expect("connect");

    // A put to the guarded bucket is vetoed: the response is an error
    // frame, not a PutResp, and a subsequent get finds nothing.
    let put = RpbPutReq {
        bucket: b"guarded".to_vec(),
        key: Some(b"k".to_vec()),
        content: Some(RpbContent {
            value: b"v".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    send_frame(&mut c, MessageCode::PutReq.as_u8(), &put.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(
        code,
        MessageCode::ErrorResp.as_u8(),
        "a vetoed put returns an error frame"
    );
    let msg = String::from_utf8_lossy(&body);
    assert!(
        msg.contains("denied") || msg.contains("rejected"),
        "the veto reason is surfaced: {msg}"
    );

    // The put did not store anything.
    let get = RpbGetReq {
        bucket: b"guarded".to_vec(),
        key: b"k".to_vec(),
        ..RpbGetReq::default()
    };
    send_frame(&mut c, MessageCode::GetReq.as_u8(), &get.encode_to_vec()).await;
    let (code, body) = recv_frame(&mut c).await;
    assert_eq!(code, MessageCode::GetResp.as_u8());
    let resp = RpbGetResp::decode(body.as_slice()).expect("get resp");
    assert!(resp.content.is_empty(), "the vetoed write stored nothing");

    // A put to an unguarded bucket (no precommit_module) succeeds.
    let ok = RpbPutReq {
        bucket: b"free".to_vec(),
        key: Some(b"k".to_vec()),
        content: Some(RpbContent {
            value: b"v".to_vec(),
            ..RpbContent::default()
        }),
        ..RpbPutReq::default()
    };
    send_frame(&mut c, MessageCode::PutReq.as_u8(), &ok.encode_to_vec()).await;
    let (code, _body) = recv_frame(&mut c).await;
    assert_eq!(
        code,
        MessageCode::PutResp.as_u8(),
        "an unguarded put is not vetoed"
    );

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
