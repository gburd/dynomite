//! Per-object causal context (vclock) round-trip over the PBC wire.
//!
//! A PUT returns the object's advanced causal context in
//! `RpbPutResp.vclock`; a GET returns the stored context in
//! `RpbGetResp.vclock`; a second PUT advances the context so it
//! strictly dominates the first (proving each write tracks causality).

#![cfg(feature = "noxu")]

use std::sync::Arc;

use dyniak::datatypes::Itc;
use dyniak::proto::pb::{MessageCode, RpbContent, RpbGetReq, RpbGetResp, RpbPutReq, RpbPutResp};
use dyniak::server::serve_pbc;
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
    let clock1 = Itc::decode(&vclock1).expect("vclock1 decodes as ITC");

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
    let clock2 = Itc::decode(&vclock2).expect("vclock2 decodes as ITC");
    assert_eq!(
        clock2.partial_cmp_event(&clock1),
        Some(std::cmp::Ordering::Greater),
        "the second write's context strictly dominates the first"
    );

    server.abort();
    let _ = server.await;
}
