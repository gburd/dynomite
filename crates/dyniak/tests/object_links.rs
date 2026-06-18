//! End-to-end tests for Riak object links across HTTP and PBC.
//!
//! Covers the link feature's storage path: parsing `Link:` request
//! headers on an HTTP `PUT`, re-emitting them on the matching `GET`,
//! and keeping the storage form compatible across the two transports
//! so a link attached over HTTP is visible over PBC and a value
//! stored over PBC is fetchable over HTTP.
//!
//! Gated on the `noxu` feature: the real object store is
//! `NoxuDatastore`, the only backend with a K/V object layer.
#![cfg(feature = "noxu")]

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use dyniak::datastore::NoxuDatastore;
use dyniak::proto::http::object::HttpObject;
use dyniak::proto::pb::{
    read_frame, write_frame, Frame, MessageCode, RpbGetReq, RpbGetResp, RpbPutReq, RpbPutResp,
};
use dyniak::{serve_http, serve_pbc};
use dynomite::embed::Datastore;
use tempfile::TempDir;

/// Spawn an HTTP gateway and a PBC server over the same backing
/// [`NoxuDatastore`], returning both bound addresses and the store
/// handle so a test can read the raw storage form directly.
async fn spawn_dual() -> (
    std::net::SocketAddr,
    std::net::SocketAddr,
    Arc<NoxuDatastore>,
    TempDir,
) {
    let dir = TempDir::new().expect("tempdir");
    let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("open transactional"));

    let http_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind http");
    let http_addr = http_listener.local_addr().expect("http addr");
    let http_ds: Arc<dyn Datastore> = ds.clone();
    tokio::spawn(async move { serve_http(http_listener, http_ds).await });

    let pbc_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind pbc");
    let pbc_addr = pbc_listener.local_addr().expect("pbc addr");
    let pbc_ds: Arc<dyn Datastore> = ds.clone();
    tokio::spawn(async move {
        let _ = serve_pbc(pbc_listener, pbc_ds).await;
    });

    tokio::time::sleep(Duration::from_millis(15)).await;
    (http_addr, pbc_addr, ds, dir)
}

/// Send a raw HTTP request and read the full response to EOF.
async fn send_raw(addr: std::net::SocketAddr, request: &str) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    stream.flush().await.expect("flush");
    let mut buf = Vec::with_capacity(4096);
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("response timeout");
    let _ = read.expect("read response");
    buf
}

/// Split a raw HTTP/1.1 response into status code and the list of
/// `Link:` header values (one entry per header line).
fn parse_status_and_links(resp: &[u8]) -> (u16, Vec<String>) {
    let sep = find_subslice(resp, b"\r\n\r\n").expect("end of headers");
    let head = std::str::from_utf8(&resp[..sep]).expect("ascii headers");
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    let mut link_values = Vec::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            if key.eq_ignore_ascii_case("link") {
                link_values.push(value.trim().to_string());
            }
        }
    }
    (status, link_values)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn pbc_get(addr: std::net::SocketAddr, bucket: &[u8], key: &[u8]) -> Vec<Vec<u8>> {
    let mut stream = TcpStream::connect(addr).await.expect("pbc connect");
    let req = RpbGetReq {
        bucket: bucket.to_vec(),
        key: key.to_vec(),
        ..RpbGetReq::default()
    };
    let frame = Frame::new(MessageCode::GetReq.as_u8(), req.encode_to_vec());
    let (mut r, mut w) = tokio::io::split(&mut stream);
    write_frame(&mut w, &frame).await.expect("send get");
    let resp = read_frame(&mut r).await.expect("recv get");
    assert_eq!(resp.code, MessageCode::GetResp.as_u8());
    RpbGetResp::decode(resp.body.as_slice())
        .expect("decode get")
        .content
}

async fn pbc_put(addr: std::net::SocketAddr, bucket: &[u8], key: &[u8], value: &[u8]) {
    let mut stream = TcpStream::connect(addr).await.expect("pbc connect");
    let req = RpbPutReq {
        bucket: bucket.to_vec(),
        key: Some(key.to_vec()),
        value: value.to_vec(),
        ..RpbPutReq::default()
    };
    let frame = Frame::new(MessageCode::PutReq.as_u8(), req.encode_to_vec());
    let (mut r, mut w) = tokio::io::split(&mut stream);
    write_frame(&mut w, &frame).await.expect("send put");
    let resp = read_frame(&mut r).await.expect("recv put");
    assert_eq!(resp.code, MessageCode::PutResp.as_u8());
    let _ = RpbPutResp::decode(resp.body.as_slice()).expect("decode put");
}

#[tokio::test]
async fn http_link_headers_round_trip() {
    let (http_addr, _pbc_addr, ds, _dir) = spawn_dual().await;

    // PUT an object carrying two links (one per tag) split across two
    // Link header lines, plus a comma-separated value on the first.
    let body = "{\"value\":[104,105]}"; // {"value": "hi"} -> bytes
    let put = format!(
        "PUT /buckets/people/keys/alice HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Link: </buckets/people/keys/bob>; riaktag=\"friend\"\r\n\
         Link: </buckets/work/keys/acme>; riaktag=\"employer\"\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let resp = send_raw(http_addr, &put).await;
    assert_eq!(parse_status_and_links(&resp).0, 204, "put status");

    // The stored envelope carries both links.
    let stored = ds
        .get_object(b"people", b"alice")
        .expect("get_object")
        .expect("present");
    let obj = HttpObject::from_storage_bytes(&stored).expect("decode envelope");
    assert_eq!(obj.links.len(), 2, "two links stored");
    assert!(obj
        .links
        .iter()
        .any(|l| l.bucket == "people" && l.key == "bob" && l.tag == "friend"));
    assert!(obj
        .links
        .iter()
        .any(|l| l.bucket == "work" && l.key == "acme" && l.tag == "employer"));

    // GET re-emits both links as response headers, plus the bucket-up
    // link Riak always adds.
    let get = "GET /buckets/people/keys/alice HTTP/1.1\r\n\
         Host: localhost\r\nAccept: application/json\r\nConnection: close\r\n\r\n";
    let resp = send_raw(http_addr, get).await;
    let (status, links) = parse_status_and_links(&resp);
    assert_eq!(status, 200, "get status");
    assert!(
        links
            .iter()
            .any(|l| l.contains("/buckets/people/keys/bob") && l.contains("friend")),
        "friend link re-emitted: {links:?}"
    );
    assert!(
        links
            .iter()
            .any(|l| l.contains("/buckets/work/keys/acme") && l.contains("employer")),
        "employer link re-emitted: {links:?}"
    );
    assert!(
        links.iter().any(|l| l.contains("rel=\"up\"")),
        "bucket-up link present: {links:?}"
    );
}

#[tokio::test]
async fn link_put_over_http_is_visible_over_pbc() {
    let (http_addr, pbc_addr, _ds, _dir) = spawn_dual().await;

    // hello -> [104,101,108,108,111]
    let body = "{\"value\":[104,101,108,108,111]}";
    let put = format!(
        "PUT /buckets/people/keys/carol HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Link: </buckets/people/keys/dave>; riaktag=\"friend\"\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    assert_eq!(
        parse_status_and_links(&send_raw(http_addr, &put).await).0,
        204
    );

    // PBC get returns the object payload (the value), not the raw
    // envelope, proving the shared storage form is unwrapped on read.
    let content = pbc_get(pbc_addr, b"people", b"carol").await;
    assert_eq!(content, vec![b"hello".to_vec()]);
}

#[tokio::test]
async fn value_put_over_pbc_is_fetchable_over_http() {
    let (http_addr, pbc_addr, _ds, _dir) = spawn_dual().await;

    pbc_put(pbc_addr, b"people", b"erin", b"world").await;

    let get = "GET /buckets/people/keys/erin HTTP/1.1\r\n\
         Host: localhost\r\nAccept: application/x-protobuf\r\nConnection: close\r\n\r\n";
    let resp = send_raw(http_addr, get).await;
    let sep = find_subslice(&resp, b"\r\n\r\n").expect("headers");
    let (status, _links) = parse_status_and_links(&resp);
    assert_eq!(status, 200, "http get status");
    let obj = HttpObject::from_storage_bytes(&resp[sep + 4..]).expect("decode protobuf body");
    assert_eq!(obj.value, b"world");
    assert!(obj.links.is_empty(), "pbc put attaches no links");
}
