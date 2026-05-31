//! End-to-end PBC integration tests against [`NoxuDatastore`].
//!
//! These tests open a real Noxu environment in a tempdir,
//! install it as the backing [`Datastore`], drive PBC requests
//! over a TCP loopback connection, and assert that puts with
//! 2i indexes are queryable via [`RpbIndexReq`].
//!
//! Gated on the `noxu` feature; without it the file compiles to
//! an empty module.

#![cfg(feature = "noxu")]

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::net::{TcpListener, TcpStream};

use dyniak::datastore::NoxuDatastore;
use dyniak::proto::pb::{
    read_frame, write_frame, Frame, MessageCode, RpbDelReq, RpbGetReq, RpbGetResp, RpbIndexReq,
    RpbIndexResp, RpbPair, RpbPutReq, RpbPutResp, INDEX_QUERY_TYPE_EQ, INDEX_QUERY_TYPE_RANGE,
};
use dyniak::serve_pbc;
use dynomite::embed::Datastore;
use tempfile::TempDir;

async fn start_with_noxu() -> (TempDir, TcpStream) {
    let dir = TempDir::new().expect("tempdir");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let noxu = NoxuDatastore::open_in(dir.path()).expect("open noxu");
    let ds: Arc<dyn Datastore> = Arc::new(noxu);
    tokio::spawn(async move {
        let _ = serve_pbc(listener, ds).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    let stream = TcpStream::connect(addr).await.expect("connect");
    (dir, stream)
}

async fn put_with_indexes(
    stream: &mut TcpStream,
    bucket: &[u8],
    key: &[u8],
    value: &[u8],
    indexes: &[(&[u8], &[u8])],
) {
    let rpb_indexes: Vec<RpbPair> = indexes
        .iter()
        .map(|(name, val)| RpbPair {
            key: name.to_vec(),
            value: Some(val.to_vec()),
        })
        .collect();
    let req = RpbPutReq {
        bucket: bucket.to_vec(),
        key: Some(key.to_vec()),
        value: value.to_vec(),
        indexes: rpb_indexes,
        ..RpbPutReq::default()
    };
    let frame = Frame::new(MessageCode::PutReq.as_u8(), req.encode_to_vec());
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(&mut w, &frame).await.expect("send put");
    let resp = read_frame(&mut r).await.expect("recv put");
    assert_eq!(resp.code, MessageCode::PutResp.as_u8());
    let _ = RpbPutResp::decode(resp.body.as_slice()).expect("decode put resp");
}

async fn index_eq(
    stream: &mut TcpStream,
    bucket: &[u8],
    index: &[u8],
    value: &[u8],
) -> Vec<Vec<u8>> {
    let req = RpbIndexReq {
        bucket: bucket.to_vec(),
        index: index.to_vec(),
        qtype: INDEX_QUERY_TYPE_EQ,
        key: Some(value.to_vec()),
        ..RpbIndexReq::default()
    };
    let frame = Frame::new(MessageCode::IndexReq.as_u8(), req.encode_to_vec());
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(&mut w, &frame).await.expect("send index");
    drain_index_stream(&mut r).await
}

async fn index_range(
    stream: &mut TcpStream,
    bucket: &[u8],
    index: &[u8],
    min: &[u8],
    max: &[u8],
) -> Vec<Vec<u8>> {
    let req = RpbIndexReq {
        bucket: bucket.to_vec(),
        index: index.to_vec(),
        qtype: INDEX_QUERY_TYPE_RANGE,
        range_min: Some(min.to_vec()),
        range_max: Some(max.to_vec()),
        ..RpbIndexReq::default()
    };
    let frame = Frame::new(MessageCode::IndexReq.as_u8(), req.encode_to_vec());
    let (mut r, mut w) = tokio::io::split(stream);
    write_frame(&mut w, &frame).await.expect("send index");
    drain_index_stream(&mut r).await
}

/// Read [`RpbIndexResp`] frames from `r` until one carries
/// `done = Some(true)`, concatenating the keys field across
/// chunks. The terminator carries no keys.
async fn drain_index_stream<R>(r: &mut R) -> Vec<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut out = Vec::new();
    loop {
        let resp = read_frame(r).await.expect("recv index");
        assert_eq!(resp.code, MessageCode::IndexResp.as_u8());
        let body = RpbIndexResp::decode(resp.body.as_slice()).expect("decode index resp");
        out.extend(body.keys);
        if body.done == Some(true) {
            break;
        }
    }
    out
}

#[tokio::test]
async fn put_get_del_round_trip_against_noxu() {
    let (_dir, mut stream) = start_with_noxu().await;
    put_with_indexes(&mut stream, b"users", b"alice", b"hello", &[]).await;

    // Read it back.
    let req = RpbGetReq {
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        ..RpbGetReq::default()
    };
    let frame = Frame::new(MessageCode::GetReq.as_u8(), req.encode_to_vec());
    let (mut r, mut w) = tokio::io::split(&mut stream);
    write_frame(&mut w, &frame).await.expect("send get");
    let resp = read_frame(&mut r).await.expect("recv get");
    assert_eq!(resp.code, MessageCode::GetResp.as_u8());
    let body = RpbGetResp::decode(resp.body.as_slice()).expect("decode get");
    assert_eq!(body.content, vec![b"hello".to_vec()]);

    // Delete it.
    let dreq = RpbDelReq {
        bucket: b"users".to_vec(),
        key: b"alice".to_vec(),
        ..RpbDelReq::default()
    };
    let dframe = Frame::new(MessageCode::DelReq.as_u8(), dreq.encode_to_vec());
    write_frame(&mut w, &dframe).await.expect("send del");
    let dresp = read_frame(&mut r).await.expect("recv del");
    assert_eq!(dresp.code, MessageCode::DelResp.as_u8());
    assert!(dresp.body.is_empty());

    // Now get returns empty content.
    let frame = Frame::new(MessageCode::GetReq.as_u8(), req.encode_to_vec());
    write_frame(&mut w, &frame).await.expect("send get");
    let resp = read_frame(&mut r).await.expect("recv get");
    let body = RpbGetResp::decode(resp.body.as_slice()).expect("decode");
    assert!(body.content.is_empty());
}

#[tokio::test]
async fn pbc_index_eq_returns_matching_keys() {
    let (_dir, mut stream) = start_with_noxu().await;
    put_with_indexes(
        &mut stream,
        b"users",
        b"alice",
        b"v1",
        &[(b"age_int", b"42")],
    )
    .await;
    put_with_indexes(&mut stream, b"users", b"bob", b"v2", &[(b"age_int", b"42")]).await;
    put_with_indexes(
        &mut stream,
        b"users",
        b"carol",
        b"v3",
        &[(b"age_int", b"99")],
    )
    .await;

    let mut hits = index_eq(&mut stream, b"users", b"age_int", b"42").await;
    hits.sort();
    assert_eq!(hits, vec![b"alice".to_vec(), b"bob".to_vec()]);

    let hits99 = index_eq(&mut stream, b"users", b"age_int", b"99").await;
    assert_eq!(hits99, vec![b"carol".to_vec()]);
}

#[tokio::test]
async fn pbc_index_range_returns_keys_in_bounds() {
    let (_dir, mut stream) = start_with_noxu().await;
    for (key, age) in [
        (&b"alice"[..], "10"),
        (&b"bob"[..], "15"),
        (&b"carol"[..], "20"),
        (&b"dave"[..], "25"),
        (&b"erin"[..], "30"),
    ] {
        put_with_indexes(
            &mut stream,
            b"users",
            key,
            b"v",
            &[(b"age_int", age.as_bytes())],
        )
        .await;
    }
    let mut hits = index_range(&mut stream, b"users", b"age_int", b"15", b"25").await;
    hits.sort();
    assert_eq!(
        hits,
        vec![b"bob".to_vec(), b"carol".to_vec(), b"dave".to_vec()]
    );
}

#[tokio::test]
async fn pbc_index_eq_after_update_only_returns_new_value() {
    let (_dir, mut stream) = start_with_noxu().await;
    put_with_indexes(
        &mut stream,
        b"users",
        b"alice",
        b"v1",
        &[(b"age_int", b"30")],
    )
    .await;
    put_with_indexes(
        &mut stream,
        b"users",
        b"alice",
        b"v2",
        &[(b"age_int", b"31")],
    )
    .await;
    let hits_old = index_eq(&mut stream, b"users", b"age_int", b"30").await;
    assert!(hits_old.is_empty());
    let hits_new = index_eq(&mut stream, b"users", b"age_int", b"31").await;
    assert_eq!(hits_new, vec![b"alice".to_vec()]);
}
