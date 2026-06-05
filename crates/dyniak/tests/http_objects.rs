//! End-to-end HTTP round-trip tests for the Riak HTTP object
//! endpoints (`GET` / `PUT` / `DELETE /buckets/{b}/keys/{k}`).
//!
//! Spins up [`dyniak::serve_http`] over a real
//! `tokio::net::TcpListener` backed by a
//! [`dyniak::datastore::NoxuDatastore`] and drives the object
//! lifecycle across the three negotiated codecs. The central
//! property under test is cross-encoding: a value `PUT` as
//! `application/json` is fetchable as `application/cbor` and
//! `application/x-protobuf` and decodes to the same logical
//! [`dyniak::proto::http::object::HttpObject`], because the gateway
//! persists the decoded envelope rather than the raw request bytes.
//!
//! A non-noxu backend ([`dynomite::embed::MemoryDatastore`]) is
//! exercised too, to confirm the documented trampoline fallback
//! (PUT -> 204, GET -> 404, DELETE -> 204) still holds and never
//! panics.
//!
//! Gated on the `noxu` feature: the real object store is
//! `NoxuDatastore`, the only backend with a K/V object layer.
#![cfg(feature = "noxu")]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use dyn_encoding::{ErasedWireValue, WireCodec, WireValue};
use dyniak::datastore::NoxuDatastore;
use dyniak::proto::http::object::{object_codecs, HttpIndex, HttpObject};
use dyniak::serve_http;
use dynomite::embed::{Datastore, MemoryDatastore};

/// Send a raw HTTP request and read the full (possibly binary)
/// response back to EOF. Each request carries `Connection: close`,
/// so the read terminates when the server hangs up.
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

/// Split a raw HTTP/1.1 response into its status code, the
/// `Content-Type` header value (if any), and the raw body bytes.
fn parse_response(resp: &[u8]) -> (u16, Option<String>, Vec<u8>) {
    let sep = find_subslice(resp, b"\r\n\r\n").expect("end of headers");
    let head = std::str::from_utf8(&resp[..sep]).expect("ascii headers");
    let body = resp[sep + 4..].to_vec();

    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let status = status_line
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("status is numeric");

    let mut content_type = None;
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            if key.eq_ignore_ascii_case("content-type") {
                content_type = Some(value.trim().to_string());
            }
        }
    }
    (status, content_type, body)
}

/// Find the first occurrence of `needle` in `haystack`.
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Build a `PUT /buckets/u/keys/k` request with the given
/// content-type and (textual) body.
fn put_request(path: &str, content_type: &str, body: &str) -> String {
    format!(
        "PUT {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n{body}",
        body.len()
    )
}

/// Build a `PUT` request carrying one extra header line (used for
/// the `X-Riak-Index-*` case).
fn put_request_with_header(
    path: &str,
    content_type: &str,
    extra_header: &str,
    body: &str,
) -> String {
    format!(
        "PUT {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: {content_type}\r\n\
         {extra_header}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n{body}",
        body.len()
    )
}

/// Build a `GET` request that negotiates `accept`.
fn get_request(path: &str, accept: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Accept: {accept}\r\n\
         Connection: close\r\n\
         \r\n"
    )
}

/// Decode a response body that was framed with `content_type` back
/// into an [`HttpObject`].
fn decode_object(content_type: &str, body: &[u8]) -> HttpObject {
    let codec: &dyn WireCodec = object_codecs()
        .for_content_type(content_type)
        .expect("codec for negotiated content-type");
    let decoded: Box<dyn ErasedWireValue> = codec
        .decode(HttpObject::wire_type_id(), body)
        .expect("decode object body");
    decoded
        .as_any()
        .downcast_ref::<HttpObject>()
        .expect("downcast to HttpObject")
        .clone()
}

/// Spawn a server backed by a fresh transactional `NoxuDatastore`,
/// returning the bound address, the retained store handle, and the
/// join handle for the accept loop.
async fn spawn_noxu_server() -> (
    std::net::SocketAddr,
    Arc<NoxuDatastore>,
    tokio::task::JoinHandle<Result<(), dyniak::error::RiakError>>,
    tempfile::TempDir,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let dir = tempfile::TempDir::new().expect("tempdir");
    let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("open transactional"));
    let ds_for_server: Arc<dyn Datastore> = ds.clone();
    let server = tokio::spawn(async move { serve_http(listener, ds_for_server).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    (addr, ds, server, dir)
}

#[tokio::test]
async fn put_json_then_get_json_round_trips_value() {
    let (addr, _ds, server, _dir) = spawn_noxu_server().await;

    let obj = HttpObject {
        value: b"the quick brown fox".to_vec(),
        content_type: Some("text/plain".to_string()),
        indexes: Vec::new(),
    };
    let body = serde_json::to_string(&obj).expect("json body");

    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/k", "application/json", &body),
    )
    .await;
    let (status, _ct, _body) = parse_response(&resp);
    assert_eq!(status, 204, "put status");

    let resp = send_raw(addr, &get_request("/buckets/u/keys/k", "application/json")).await;
    let (status, ct, body) = parse_response(&resp);
    assert_eq!(status, 200, "get status");
    assert_eq!(ct.as_deref(), Some("application/json"));
    let back = decode_object("application/json", &body);
    assert_eq!(back, obj);
    assert_eq!(back.value, b"the quick brown fox");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn put_json_then_get_cbor_and_protobuf_cross_encode() {
    let (addr, ds, server, _dir) = spawn_noxu_server().await;

    let obj = HttpObject {
        value: b"cross-encoded payload".to_vec(),
        content_type: Some("application/octet-stream".to_string()),
        indexes: vec![HttpIndex {
            name: "age_int".to_string(),
            value: "42".to_string(),
        }],
    };
    let body = serde_json::to_string(&obj).expect("json body");

    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/k", "application/json", &body),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 204, "put status");

    // The 2i entry fanned out from the envelope indexes.
    assert_eq!(
        ds.index_eq(b"u", b"age_int", b"42").unwrap(),
        vec![b"k".to_vec()],
        "index entry must land in the 2i layer"
    );

    // GET as CBOR.
    let resp = send_raw(addr, &get_request("/buckets/u/keys/k", "application/cbor")).await;
    let (status, ct, cbor_body) = parse_response(&resp);
    assert_eq!(status, 200, "get cbor status");
    assert_eq!(ct.as_deref(), Some("application/cbor"));
    let from_cbor = decode_object("application/cbor", &cbor_body);
    assert_eq!(from_cbor, obj, "cbor decodes to the same logical object");

    // GET as protobuf.
    let resp = send_raw(
        addr,
        &get_request("/buckets/u/keys/k", "application/x-protobuf"),
    )
    .await;
    let (status, ct, pb_body) = parse_response(&resp);
    assert_eq!(status, 200, "get protobuf status");
    assert_eq!(ct.as_deref(), Some("application/x-protobuf"));
    let from_pb = decode_object("application/x-protobuf", &pb_body);
    assert_eq!(from_pb, obj, "protobuf decodes to the same logical object");

    // The protobuf GET body is exactly the canonical storage form.
    assert_eq!(pb_body, obj.to_storage_bytes());

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn get_missing_key_returns_404() {
    let (addr, _ds, server, _dir) = spawn_noxu_server().await;

    let resp = send_raw(
        addr,
        &get_request("/buckets/u/keys/absent", "application/json"),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 404, "missing key status");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn put_unsupported_content_type_returns_415() {
    let (addr, _ds, server, _dir) = spawn_noxu_server().await;

    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/k", "application/xml", "<doc/>"),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 415, "unsupported media type");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn put_empty_body_returns_400() {
    let (addr, _ds, server, _dir) = spawn_noxu_server().await;

    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/k", "application/json", ""),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 400, "empty body");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn put_undecodable_body_returns_400() {
    let (addr, _ds, server, _dir) = spawn_noxu_server().await;

    // A non-empty body that is not valid JSON for the envelope.
    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/k", "application/json", "{not json"),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 400, "undecodable body");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn delete_then_get_returns_404() {
    let (addr, _ds, server, _dir) = spawn_noxu_server().await;

    let obj = HttpObject {
        value: b"to be deleted".to_vec(),
        content_type: None,
        indexes: Vec::new(),
    };
    let body = serde_json::to_string(&obj).expect("json body");
    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/doomed", "application/json", &body),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 204, "put status");

    // The object is present before the delete.
    let resp = send_raw(
        addr,
        &get_request("/buckets/u/keys/doomed", "application/json"),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 200, "pre-delete get");

    let del = "DELETE /buckets/u/keys/doomed HTTP/1.1\r\n\
         Host: localhost\r\nConnection: close\r\n\r\n";
    let resp = send_raw(addr, del).await;
    assert_eq!(parse_response(&resp).0, 204, "delete status");

    // Deleting an absent key is still 204 (Riak semantics).
    let resp = send_raw(addr, del).await;
    assert_eq!(parse_response(&resp).0, 204, "delete-absent status");

    let resp = send_raw(
        addr,
        &get_request("/buckets/u/keys/doomed", "application/json"),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 404, "post-delete get");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn x_riak_index_header_fans_out_and_round_trips() {
    let (addr, ds, server, _dir) = spawn_noxu_server().await;

    // Body carries no indexes; the index arrives via the header.
    let obj = HttpObject {
        value: b"indexed by header".to_vec(),
        content_type: None,
        indexes: Vec::new(),
    };
    let body = serde_json::to_string(&obj).expect("json body");
    let resp = send_raw(
        addr,
        &put_request_with_header(
            "/buckets/u/keys/h",
            "application/json",
            "X-Riak-Index-tag_bin: alpha, beta",
            &body,
        ),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 204, "put status");

    // Both comma-separated values fanned into the 2i layer.
    assert_eq!(
        ds.index_eq(b"u", b"tag_bin", b"alpha").unwrap(),
        vec![b"h".to_vec()]
    );
    assert_eq!(
        ds.index_eq(b"u", b"tag_bin", b"beta").unwrap(),
        vec![b"h".to_vec()]
    );

    // And the header-sourced indexes are echoed in the envelope.
    let resp = send_raw(addr, &get_request("/buckets/u/keys/h", "application/json")).await;
    let (status, _ct, body) = parse_response(&resp);
    assert_eq!(status, 200);
    let back = decode_object("application/json", &body);
    assert!(back
        .indexes
        .iter()
        .any(|i| i.name == "tag_bin" && i.value == "alpha"));
    assert!(back
        .indexes
        .iter()
        .any(|i| i.name == "tag_bin" && i.value == "beta"));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn head_request_returns_headers_without_body() {
    let (addr, _ds, server, _dir) = spawn_noxu_server().await;

    let obj = HttpObject {
        value: b"head me".to_vec(),
        content_type: None,
        indexes: Vec::new(),
    };
    let body = serde_json::to_string(&obj).expect("json body");
    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/k", "application/json", &body),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 204);

    let head = "HEAD /buckets/u/keys/k HTTP/1.1\r\n\
         Host: localhost\r\nAccept: application/json\r\nConnection: close\r\n\r\n";
    let resp = send_raw(addr, head).await;
    let (status, ct, body) = parse_response(&resp);
    assert_eq!(status, 200, "head status");
    assert_eq!(ct.as_deref(), Some("application/json"));
    assert!(body.is_empty(), "HEAD body must be empty");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn memory_datastore_falls_back_without_panic() {
    // A non-noxu backend has no object layer; the gateway must fall
    // back to the documented trampoline shape: PUT -> 204, GET ->
    // 404, DELETE -> 204, and never panic.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(async move { serve_http(listener, ds).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let obj = HttpObject {
        value: b"value".to_vec(),
        content_type: None,
        indexes: Vec::new(),
    };
    let body = serde_json::to_string(&obj).expect("json body");
    let resp = send_raw(
        addr,
        &put_request("/buckets/u/keys/k", "application/json", &body),
    )
    .await;
    assert_eq!(parse_response(&resp).0, 204, "memory put status");

    let resp = send_raw(addr, &get_request("/buckets/u/keys/k", "application/json")).await;
    assert_eq!(parse_response(&resp).0, 404, "memory get status");

    let del = "DELETE /buckets/u/keys/k HTTP/1.1\r\n\
         Host: localhost\r\nConnection: close\r\n\r\n";
    let resp = send_raw(addr, del).await;
    assert_eq!(parse_response(&resp).0, 204, "memory delete status");

    server.abort();
    let _ = server.await;
}
