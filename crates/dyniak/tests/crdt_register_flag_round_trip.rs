//! End-to-end served CRDT (data-type) integration tests for the
//! Register (LWW) and Flag (enable-wins) types.
//!
//! Gated on the `noxu` feature; without it the file compiles to
//! nothing (the served CRDT path is backed by `NoxuDatastore`).
//!
//! Mirrors `crdt_pbc_round_trip.rs`: starts a real `serve_pbc`
//! listener backed by [`NoxuDatastore`], drives `DtUpdateReq` /
//! `DtFetchReq` over TCP, and asserts the served CRDT path converges:
//!
//! * a register assigned over the wire reads back the assigned value;
//! * a flag enabled over the wire reads back `true`.

#![cfg(feature = "noxu")]

use std::sync::Arc;

use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};

use dyniak::datastore::NoxuDatastore;
use dyniak::proto::pb::{
    read_frame, write_frame, DtFetchReq, DtFetchResp, DtOp, DtUpdateReq, DtUpdateResp, FlagOp,
    Frame, MessageCode, RegisterOp,
};
use dyniak::serve_pbc;
use dynomite::embed::Datastore;
use tempfile::TempDir;

async fn spawn_server() -> (TempDir, std::net::SocketAddr) {
    let dir = TempDir::new().expect("tempdir");
    let noxu = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = serve_pbc(listener, ds).await;
    });
    (dir, addr)
}

async fn dt_update_register(
    stream: &mut TcpStream,
    bucket: &[u8],
    key: &[u8],
    value: &[u8],
) -> Option<Vec<u8>> {
    let req = DtUpdateReq {
        bucket: bucket.to_vec(),
        key: Some(key.to_vec()),
        r#type: b"registers".to_vec(),
        op: Some(DtOp {
            register_op: Some(RegisterOp {
                value: value.to_vec(),
                ts_micros: None,
            }),
            ..DtOp::default()
        }),
        ..DtUpdateReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtUpdateReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_update");
    let resp = read_frame(&mut r).await.expect("recv dt_update");
    assert_eq!(
        resp.code,
        MessageCode::DtUpdateResp.as_u8(),
        "dt_update must return DtUpdateResp, got code {}",
        resp.code
    );
    DtUpdateResp::decode(resp.body.as_slice())
        .expect("decode")
        .register_value
}

async fn dt_fetch_register(stream: &mut TcpStream, bucket: &[u8], key: &[u8]) -> Option<Vec<u8>> {
    let req = DtFetchReq {
        bucket: bucket.to_vec(),
        key: key.to_vec(),
        r#type: b"registers".to_vec(),
        ..DtFetchReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtFetchReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_fetch");
    let resp = read_frame(&mut r).await.expect("recv dt_fetch");
    assert_eq!(resp.code, MessageCode::DtFetchResp.as_u8());
    DtFetchResp::decode(resp.body.as_slice())
        .expect("decode")
        .value
        .and_then(|v| v.register_value)
}

async fn dt_update_flag(
    stream: &mut TcpStream,
    bucket: &[u8],
    key: &[u8],
    enable: bool,
) -> Option<bool> {
    let req = DtUpdateReq {
        bucket: bucket.to_vec(),
        key: Some(key.to_vec()),
        r#type: b"flags".to_vec(),
        op: Some(DtOp {
            flag_op: Some(FlagOp { enable }),
            ..DtOp::default()
        }),
        ..DtUpdateReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtUpdateReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_update");
    let resp = read_frame(&mut r).await.expect("recv dt_update");
    assert_eq!(resp.code, MessageCode::DtUpdateResp.as_u8());
    DtUpdateResp::decode(resp.body.as_slice())
        .expect("decode")
        .flag_value
}

async fn dt_fetch_flag(stream: &mut TcpStream, bucket: &[u8], key: &[u8]) -> Option<bool> {
    let req = DtFetchReq {
        bucket: bucket.to_vec(),
        key: key.to_vec(),
        r#type: b"flags".to_vec(),
        ..DtFetchReq::default()
    };
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(
        &mut w,
        &Frame::new(MessageCode::DtFetchReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("send dt_fetch");
    let resp = read_frame(&mut r).await.expect("recv dt_fetch");
    assert_eq!(resp.code, MessageCode::DtFetchResp.as_u8());
    DtFetchResp::decode(resp.body.as_slice())
        .expect("decode")
        .value
        .and_then(|v| v.flag_value)
}

#[tokio::test]
async fn register_assign_reads_back_the_value() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let updated = dt_update_register(&mut s, b"profiles", b"name", b"alice").await;
    assert_eq!(updated, Some(b"alice".to_vec()));
    let fetched = dt_fetch_register(&mut s, b"profiles", b"name").await;
    assert_eq!(fetched, Some(b"alice".to_vec()));
}

#[tokio::test]
async fn register_reassign_converges_to_the_latest_value() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");
    dt_update_register(&mut s, b"profiles", b"name", b"alice").await;
    let updated = dt_update_register(&mut s, b"profiles", b"name", b"bob").await;
    assert_eq!(updated, Some(b"bob".to_vec()));
    let fetched = dt_fetch_register(&mut s, b"profiles", b"name").await;
    assert_eq!(fetched, Some(b"bob".to_vec()));
}

#[tokio::test]
async fn flag_enable_reads_back_true() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");
    let updated = dt_update_flag(&mut s, b"toggles", b"beta", true).await;
    assert_eq!(updated, Some(true));
    let fetched = dt_fetch_flag(&mut s, b"toggles", b"beta").await;
    assert_eq!(fetched, Some(true));
}

#[tokio::test]
async fn flag_disable_after_enable_reads_back_false() {
    let (_dir, addr) = spawn_server().await;
    let mut s = TcpStream::connect(addr).await.expect("connect");
    dt_update_flag(&mut s, b"toggles", b"beta", true).await;
    let updated = dt_update_flag(&mut s, b"toggles", b"beta", false).await;
    assert_eq!(updated, Some(false));
    let fetched = dt_fetch_flag(&mut s, b"toggles", b"beta").await;
    assert_eq!(fetched, Some(false));
}
