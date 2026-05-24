//! End-to-end HTTP round-trip integration test for the Riak HTTP
//! gateway.
//!
//! Spins up [`dyn_riak::serve_http`] over a real
//! `tokio::net::TcpListener` bound to localhost, then drives a
//! sequence of requests: a `GET /ping`, a `PUT
//! /buckets/u/keys/k` with a JSON body, a `GET` of the same key, a
//! `DELETE`, and a `GET /buckets/u/keys?keys=true` listing call.
//!
//! The test deliberately uses [`dynomite::embed::MemoryDatastore`]
//! so it runs without the lamdb checkout next door. The K/V
//! trampoline through [`dynomite::embed::Datastore::dispatch`]
//! returns no content, so the `GET` after the `PUT` yields `404
//! Not Found`; that mirrors what Riak returns for a missing key.
//! Listing streams chunked JSON arrays through
//! [`dynomite::embed::Datastore::list_keys_stream`] -- the
//! `MemoryDatastore` overrides the trait default so the streamed
//! body is the array of keys seeded into its in-memory listing
//! index by the test.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use dyn_riak::serve_http;
use dynomite::embed::{Datastore, MemoryDatastore};

/// Send a raw HTTP request and read the full response back, capped
/// at a reasonable buffer size for these small payloads.
async fn send_request(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    stream.flush().await.expect("flush");

    // Read until the peer closes (Connection: close on each request)
    // or the response is complete. We bound the read to keep the
    // test from hanging on a misbehaving server.
    let mut buf = Vec::with_capacity(4096);
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("response timeout");
    let _ = read.expect("read response");
    String::from_utf8(buf).expect("response is utf-8")
}

/// Pull the status code (e.g. 200, 204, 404) out of an HTTP/1.1
/// response message.
fn status_code(response: &str) -> u16 {
    let line = response.lines().next().expect("status line");
    let mut parts = line.split_whitespace();
    let _version = parts.next().expect("version");
    let code = parts.next().expect("status code");
    code.parse().expect("status is numeric")
}

/// Decode an HTTP/1.1 response body that uses
/// `Transfer-Encoding: chunked`.
///
/// The decoder reads `<hex-len>\r\n<bytes>\r\n` blocks until a
/// zero-length chunk terminates the body. Trailing headers (none
/// expected from this test) are ignored.
fn decode_chunked_body(response: &str) -> String {
    // Locate end of headers.
    let split = response.find("\r\n\r\n").expect("end of headers");
    let body = &response[split + 4..];
    let mut out = String::new();
    let mut rest = body;
    loop {
        let line_end = rest.find("\r\n").expect("chunk size line");
        let size_line = &rest[..line_end];
        // Some chunk-size lines carry extension parameters after
        // a `;`; drop them.
        let size_str = size_line.split(';').next().unwrap_or(size_line);
        let size = usize::from_str_radix(size_str.trim(), 16).expect("hex chunk size");
        rest = &rest[line_end + 2..];
        if size == 0 {
            break;
        }
        out.push_str(&rest[..size]);
        rest = &rest[size + 2..]; // +2 for the trailing CRLF.
    }
    out
}

#[tokio::test]
async fn http_ping_put_get_delete_listkeys_round_trip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let ds = Arc::new(MemoryDatastore::new());
    let ds_for_server: Arc<dyn Datastore> = ds.clone();
    let server = tokio::spawn(async move { serve_http(listener, ds_for_server).await });

    // Yield so the accept loop is reached before clients knock.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // --- Ping -----------------------------------------------------
    let resp = send_request(
        addr,
        "GET /ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert_eq!(status_code(&resp), 200, "ping status: {resp}");
    assert!(resp.contains("OK"), "ping body should contain OK: {resp}");

    // --- Put with JSON content-type ------------------------------
    let body = br#"{"hello":"world"}"#;
    let put = format!(
        "PUT /buckets/u/keys/k HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let put = format!("{put}{}", std::str::from_utf8(body).expect("ascii body"));
    let resp = send_request(addr, &put).await;
    assert_eq!(status_code(&resp), 204, "put status: {resp}");

    // --- Get with JSON Accept ------------------------------------
    let resp = send_request(
        addr,
        "GET /buckets/u/keys/k HTTP/1.1\r\n\
         Host: localhost\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n\
         \r\n",
    )
    .await;
    // MemoryDatastore stores nothing, so the trampoline produces a
    // 404. The structural round-trip (Accept negotiation, route
    // dispatch, datastore call) succeeded; the Riak K/V trait that
    // makes Get actually return the stored body lands later.
    assert_eq!(status_code(&resp), 404, "get status: {resp}");

    // --- Delete --------------------------------------------------
    let resp = send_request(
        addr,
        "DELETE /buckets/u/keys/k HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: close\r\n\
         \r\n",
    )
    .await;
    assert_eq!(status_code(&resp), 204, "delete status: {resp}");

    // --- List keys (chunked JSON array) -------------------------
    // Seed the listing index so the streamed body has content.
    for i in 0..3u8 {
        ds.insert(b"u", format!("k{i}").as_bytes());
    }
    let resp = send_request(
        addr,
        "GET /buckets/u/keys?keys=true HTTP/1.1\r\n\
         Host: localhost\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n\
         \r\n",
    )
    .await;
    assert_eq!(status_code(&resp), 200, "list-keys status: {resp}");
    assert!(
        resp.to_lowercase().contains("transfer-encoding: chunked"),
        "list-keys must use chunked transfer-encoding: {resp}"
    );
    assert!(
        resp.to_lowercase().contains("content-type: application/json"),
        "list-keys must declare JSON content-type: {resp}"
    );
    let body = decode_chunked_body(&resp);
    let parsed: serde_json::Value =
        serde_json::from_slice(body.as_bytes()).expect("list-keys body must be JSON");
    let arr = parsed.as_array().expect("list-keys body is an array");
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0], serde_json::Value::String("k0".to_string()));
    assert_eq!(arr[1], serde_json::Value::String("k1".to_string()));
    assert_eq!(arr[2], serde_json::Value::String("k2".to_string()));

    // The K/V ops (Put, Get, Delete) each trampoline through
    // dispatch. Ping and list-keys do not.
    assert_eq!(ds.dispatch_count(), 3);

    server.abort();
    let _ = server.await;
}
