//! End-to-end PBC round-trip integration test.
//!
//! Spins up [`dyn_riak::serve_pbc`] over a real `tokio::net::TcpListener`
//! bound to localhost, then drives Ping / Put / Get / Del frames over
//! a [`tokio::net::TcpStream`] client. Asserts that each request
//! returns the expected response code and that the in-memory
//! datastore observed the dispatch.
//!
//! The test deliberately uses [`dynomite::embed::MemoryDatastore`]
//! rather than [`dyn_riak::datastore::NoxuDatastore`] so it runs
//! without the lamdb checkout next door.

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};

use dyn_riak::proto::pb::{
    read_frame, write_frame, Frame, MessageCode, RpbDelReq, RpbGetReq, RpbGetResp, RpbPutReq,
    RpbPutResp,
};
use dyn_riak::serve_pbc;
use dynomite::embed::{Datastore, MemoryDatastore};

#[tokio::test]
async fn pbc_ping_put_get_del_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let ds = Arc::new(MemoryDatastore::new());
    let ds_for_server: Arc<dyn Datastore> = ds.clone();
    let server = tokio::spawn(async move { serve_pbc(listener, ds_for_server).await });

    // Give tokio a chance to enter the accept loop.
    tokio::time::sleep(Duration::from_millis(10)).await;

    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut reader, mut writer) = tokio::io::split(stream);

    // ---------- Ping ----------------------------------------------------
    write_frame(
        &mut writer,
        &Frame::new(MessageCode::PingReq.as_u8(), Vec::new()),
    )
    .await
    .expect("write ping");
    let resp = read_frame(&mut reader).await.expect("read ping resp");
    assert_eq!(resp.code, MessageCode::PingResp.as_u8());
    assert!(resp.body.is_empty());

    // ---------- Put -----------------------------------------------------
    let put = RpbPutReq {
        bucket: b"users".to_vec(),
        key: Some(b"alice".to_vec()),
        value: b"hello".to_vec(),
        ..RpbPutReq::default()
    };
    write_frame(
        &mut writer,
        &Frame::new(MessageCode::PutReq.as_u8(), put.encode_to_vec()),
    )
    .await
    .expect("write put");
    let resp = read_frame(&mut reader).await.expect("read put resp");
    assert_eq!(resp.code, MessageCode::PutResp.as_u8());
    let put_resp = RpbPutResp::decode(resp.body.as_slice()).expect("decode put resp");
    assert!(put_resp.content.is_empty());
    assert!(put_resp.vclock.is_none());

    // ---------- Get -----------------------------------------------------
    let get = RpbGetReq {
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        ..RpbGetReq::default()
    };
    write_frame(
        &mut writer,
        &Frame::new(MessageCode::GetReq.as_u8(), get.encode_to_vec()),
    )
    .await
    .expect("write get");
    let resp = read_frame(&mut reader).await.expect("read get resp");
    assert_eq!(resp.code, MessageCode::GetResp.as_u8());
    let get_resp = RpbGetResp::decode(resp.body.as_slice()).expect("decode get resp");
    // The v0.0.1 slice's MemoryDatastore-backed server replies with
    // an empty content list; full sibling materialisation lands with
    // the RiakObject schema in the next slice.
    assert!(get_resp.content.is_empty());

    // ---------- Del -----------------------------------------------------
    let del = RpbDelReq {
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        ..RpbDelReq::default()
    };
    write_frame(
        &mut writer,
        &Frame::new(MessageCode::DelReq.as_u8(), del.encode_to_vec()),
    )
    .await
    .expect("write del");
    let resp = read_frame(&mut reader).await.expect("read del resp");
    assert_eq!(resp.code, MessageCode::DelResp.as_u8());
    assert!(resp.body.is_empty());

    // The substrate's accounting should have ticked once per
    // K/V op (Put, Get, Del). Ping does not reach the datastore.
    assert_eq!(ds.dispatch_count(), 3);

    drop(reader);
    drop(writer);
    // serve_pbc keeps looping; aborting the spawned task is the
    // standard graceful-shutdown shape for this test.
    server.abort();
    let _ = server.await;
}
