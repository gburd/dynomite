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
//! so it runs without the lamdb checkout next door. The
//! `MemoryDatastore` does not implement key listing, so listing
//! returns `501 Not Implemented`. The K/V trampoline through
//! [`dynomite::embed::Datastore::dispatch`] returns no content, so
//! the `GET` after the `PUT` yields `404 Not Found` -- exactly what
//! Riak returns for a missing key. Both behaviours are intentional
//! for the v0.0.1 slice.

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

    // --- List keys (501 Not Implemented) -------------------------
    let resp = send_request(
        addr,
        "GET /buckets/u/keys?keys=true HTTP/1.1\r\n\
         Host: localhost\r\n\
         Connection: close\r\n\
         \r\n",
    )
    .await;
    assert_eq!(status_code(&resp), 501, "list-keys status: {resp}");

    // The K/V ops (Put, Get, Delete) each trampoline through
    // dispatch. Ping and list-keys do not.
    assert_eq!(ds.dispatch_count(), 3);

    server.abort();
    let _ = server.await;
}
