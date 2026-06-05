//! End-to-end HTTP round-trip test for the multi-key transaction
//! endpoint of the Riak HTTP gateway.
//!
//! Spins up [`dyniak::serve_http`] over a real
//! `tokio::net::TcpListener` backed by a transactional
//! [`dyniak::datastore::NoxuDatastore`], then drives:
//!
//! * `POST /transactions` with a three-put batch -> `200 OK` and
//!   `result: "committed"`; the keys are visible afterwards.
//! * `POST /transactions` with `abort: true` -> `409 Conflict` and
//!   `result: "aborted"`; none of its writes are visible.
//! * `POST /buckets/users/transactions` whose op targets a
//!   different bucket -> `400 Bad Request`.
//!
//! The test inspects the retained concrete `NoxuDatastore` handle to
//! confirm what did and did not land, because the v0.0.1 HTTP `GET`
//! handler is a trampoline that does not read the K/V store.
//!
//! Gated on the `noxu` feature: the transaction endpoint needs a
//! transactional backend, and `NoxuDatastore` is the only
//! implementation.
#![cfg(feature = "noxu")]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use dyniak::datastore::NoxuDatastore;
use dyniak::serve_http;
use dynomite::embed::Datastore;

/// Send a raw HTTP request and read the full response back.
async fn send_request(addr: std::net::SocketAddr, request: &str) -> String {
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
    String::from_utf8(buf).expect("response is utf-8")
}

/// Pull the numeric status code out of an HTTP/1.1 response.
fn status_code(response: &str) -> u16 {
    let line = response.lines().next().expect("status line");
    let mut parts = line.split_whitespace();
    let _version = parts.next().expect("version");
    parts
        .next()
        .expect("status code")
        .parse()
        .expect("status is numeric")
}

/// Return the response body (everything after the header break).
fn body_of(response: &str) -> &str {
    let split = response.find("\r\n\r\n").expect("end of headers");
    &response[split + 4..]
}

/// Build a `POST` request line + headers + body for `path`.
fn post(path: &str, body: &str) -> String {
    format!(
        "POST {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n{body}",
        body.len()
    )
}

#[tokio::test]
async fn http_transaction_commit_abort_and_bucket_scope() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let dir = tempfile::TempDir::new().expect("tempdir");
    let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("open transactional"));
    let ds_for_server: Arc<dyn Datastore> = ds.clone();
    let server = tokio::spawn(async move { serve_http(listener, ds_for_server).await });

    // Yield so the accept loop is reached before clients knock.
    tokio::time::sleep(Duration::from_millis(10)).await;

    // --- Commit a three-put batch --------------------------------
    let commit_body = r#"{"operations":[
        {"op":"put","bucket":"users","key":"alice","value":"a",
         "indexes":[{"name":"age_int","value":"30"}]},
        {"op":"put","bucket":"users","key":"bob","value":"b"},
        {"op":"put","bucket":"users","key":"carol","value":"c"}
    ]}"#;
    let resp = send_request(addr, &post("/transactions", commit_body)).await;
    assert_eq!(status_code(&resp), 200, "commit status: {resp}");
    let parsed: serde_json::Value =
        serde_json::from_str(body_of(&resp)).expect("commit body is JSON");
    assert_eq!(parsed["result"], "committed");
    assert_eq!(parsed["operations"], 3);

    // All three keys, and the 2i entry, are visible after commit.
    assert_eq!(
        ds.get_object(b"users", b"alice").unwrap().as_deref(),
        Some(&b"a"[..])
    );
    assert_eq!(
        ds.get_object(b"users", b"bob").unwrap().as_deref(),
        Some(&b"b"[..])
    );
    assert_eq!(
        ds.get_object(b"users", b"carol").unwrap().as_deref(),
        Some(&b"c"[..])
    );
    assert_eq!(
        ds.index_eq(b"users", b"age_int", b"30").unwrap(),
        vec![b"alice".to_vec()]
    );

    // --- Abort a batch -------------------------------------------
    let abort_body = r#"{"abort":true,"operations":[
        {"op":"put","bucket":"users","key":"dave","value":"d",
         "indexes":[{"name":"age_int","value":"40"}]}
    ]}"#;
    let resp = send_request(addr, &post("/transactions", abort_body)).await;
    assert_eq!(status_code(&resp), 409, "abort status: {resp}");
    let parsed: serde_json::Value =
        serde_json::from_str(body_of(&resp)).expect("abort body is JSON");
    assert_eq!(parsed["result"], "aborted");

    // Nothing from the aborted batch landed.
    assert!(ds.get_object(b"users", b"dave").unwrap().is_none());
    assert!(ds.index_eq(b"users", b"age_int", b"40").unwrap().is_empty());

    // --- Bucket-scoped route rejects a cross-bucket op -----------
    let mismatch_body = r#"{"operations":[
        {"op":"put","bucket":"other","key":"k","value":"v"}
    ]}"#;
    let resp = send_request(addr, &post("/buckets/users/transactions", mismatch_body)).await;
    assert_eq!(status_code(&resp), 400, "bucket-scope status: {resp}");
    assert!(ds.get_object(b"other", b"k").unwrap().is_none());

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn http_transaction_bucket_scoped_commit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let dir = tempfile::TempDir::new().expect("tempdir");
    let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("open transactional"));
    let ds_for_server: Arc<dyn Datastore> = ds.clone();
    let server = tokio::spawn(async move { serve_http(listener, ds_for_server).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let body = r#"{"operations":[
        {"op":"put","bucket":"users","key":"erin","value":"e"},
        {"op":"delete","bucket":"users","key":"ghost"}
    ]}"#;
    let resp = send_request(addr, &post("/buckets/users/transactions", body)).await;
    assert_eq!(status_code(&resp), 200, "status: {resp}");
    let parsed: serde_json::Value = serde_json::from_str(body_of(&resp)).expect("body is JSON");
    assert_eq!(parsed["result"], "committed");
    assert_eq!(parsed["operations"], 2);
    assert_eq!(
        ds.get_object(b"users", b"erin").unwrap().as_deref(),
        Some(&b"e"[..])
    );

    server.abort();
    let _ = server.await;
}
