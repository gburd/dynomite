//! End-to-end HTTP round-trip tests for the search surface of the
//! Riak HTTP gateway.
//!
//! Spins up [`dyniak::serve_http_with_search`] over a real
//! `tokio::net::TcpListener` backed by a transactional
//! [`dyniak::datastore::NoxuDatastore`] and a fresh
//! [`dynomite_search::VectorRegistry`], then drives:
//!
//! * a text index: declare a field, `PUT` three documents, and run a
//!   substring search that returns the matching keys;
//! * a regex search: exact (`k=0`) and approximate (`k=1`);
//! * a vector index: create it, `PUT` three vectors, and run a KNN
//!   query that returns the nearest key first;
//! * the unconfigured case: the same routes served by plain
//!   [`dyniak::serve_http`] (no registry) reply `501 Not Implemented`;
//! * content-type negotiation: a vector-search response round-trips
//!   as both JSON and CBOR.
//!
//! Gated on `noxu` (the object store that feeds the indexes on write)
//! and `search` (the registry and search routes).
#![cfg(all(feature = "noxu", feature = "search"))]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use dyn_encoding::{CborCodec, ErasedWireValue, WireCodec, WireValue};
use dyniak::datastore::NoxuDatastore;
use dyniak::proto::http::search::VectorSearchResponse;
use dyniak::{serve_http, serve_http_with_search};
use dynomite::embed::Datastore;
use dynomite_search::VectorRegistry;

/// Bring up an HTTP gateway with a search registry. Returns the bound
/// address, the server task handle, and the shared registry.
async fn spawn_with_search() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let dir = tempfile::TempDir::new().expect("tempdir");
    // Keep the temp dir alive for the life of the server task.
    let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("open"));
    let ds_dyn: Arc<dyn Datastore> = ds.clone();
    let registry = Arc::new(VectorRegistry::new());
    let server = tokio::spawn(async move {
        // `dir` is moved in so it outlives the listeners.
        let _keep = dir;
        let _ = serve_http_with_search(listener, ds_dyn, registry).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    (addr, server)
}

/// Send a raw request (head + body bytes) and read the full response.
async fn raw(addr: std::net::SocketAddr, request: Vec<u8>) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(&request).await.expect("write");
    stream.flush().await.expect("flush");
    let mut buf = Vec::with_capacity(4096);
    let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("response timeout");
    read.expect("read");
    buf
}

/// Locate a subslice within a slice.
fn find_sub(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Split a raw HTTP response into (status code, body bytes).
fn split_response(resp: &[u8]) -> (u16, Vec<u8>) {
    let sep = find_sub(resp, b"\r\n\r\n").expect("header break");
    let head = &resp[..sep];
    let body = resp[sep + 4..].to_vec();
    let first_line_end = find_sub(head, b"\r\n").unwrap_or(head.len());
    let line = std::str::from_utf8(&head[..first_line_end]).expect("ascii status line");
    let code = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status code");
    (code, body)
}

/// Build a request with an arbitrary method, path, optional headers,
/// and body bytes.
fn request(method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (k, v) in headers {
        let _ = write!(head, "{k}: {v}\r\n");
    }
    let _ = write!(head, "Content-Length: {}\r\n\r\n", body.len());
    let mut out = head.into_bytes();
    out.extend_from_slice(body);
    out
}

/// Build the JSON `HttpObject` envelope wrapping a document payload:
/// `{"value":[b0,b1,...]}`.
fn object_envelope(document: &str) -> Vec<u8> {
    let mut out = String::from("{\"value\":[");
    for (i, b) in document.as_bytes().iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&b.to_string());
    }
    out.push_str("]}");
    out.into_bytes()
}

/// PUT a document under `bucket`/`key`.
async fn put_document(addr: std::net::SocketAddr, bucket: &str, key: &str, document: &str) {
    let path = format!("/buckets/{bucket}/keys/{key}");
    let body = object_envelope(document);
    let req = request("PUT", &path, &[("Content-Type", "application/json")], &body);
    let resp = raw(addr, req).await;
    let (code, _) = split_response(&resp);
    assert_eq!(code, 204, "PUT {key} should be 204");
}

#[tokio::test]
async fn text_substring_search_returns_matching_keys() {
    let (addr, server) = spawn_with_search().await;

    // Declare a text index on the `title` field.
    let resp = raw(
        addr,
        request("PUT", "/buckets/docs/index/text/title", &[], &[]),
    )
    .await;
    assert_eq!(split_response(&resp).0, 200, "declare index");

    put_document(addr, "docs", "a", r#"{"title":"the quick brown fox"}"#).await;
    put_document(addr, "docs", "b", r#"{"title":"lazy dog sleeps"}"#).await;
    put_document(addr, "docs", "c", r#"{"title":"quick start guide"}"#).await;

    // Substring `quick` matches a and c.
    let resp = raw(
        addr,
        request(
            "GET",
            "/buckets/docs/search/text/title?q=quick",
            &[("Accept", "application/json")],
            &[],
        ),
    )
    .await;
    let (code, body) = split_response(&resp);
    assert_eq!(
        code,
        200,
        "search status: {}",
        String::from_utf8_lossy(&body)
    );
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let mut keys: Vec<String> = parsed["hits"]
        .as_array()
        .expect("hits array")
        .iter()
        .map(|h| h["key"].as_str().expect("key").to_string())
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["a".to_string(), "c".to_string()]);

    // Substring `lazy` matches b only.
    let resp = raw(
        addr,
        request(
            "GET",
            "/buckets/docs/search/text/title?q=lazy",
            &[("Accept", "application/json")],
            &[],
        ),
    )
    .await;
    let (_, body) = split_response(&resp);
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let keys: Vec<&str> = parsed["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["b"]);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn regex_search_exact_and_approx() {
    let (addr, server) = spawn_with_search().await;

    let resp = raw(
        addr,
        request("PUT", "/buckets/docs/index/text/title", &[], &[]),
    )
    .await;
    assert_eq!(split_response(&resp).0, 200);

    put_document(addr, "docs", "alpha", r#"{"title":"quick brown fox"}"#).await;
    put_document(addr, "docs", "beta", r#"{"title":"lazy dog"}"#).await;
    put_document(addr, "docs", "gamma", r#"{"title":"quack brown fox"}"#).await;

    // Exact regex (k=0): only `quick` matches alpha.
    let resp = raw(
        addr,
        request(
            "GET",
            "/buckets/docs/search/regex/title?pattern=quick&k=0",
            &[("Accept", "application/json")],
            &[],
        ),
    )
    .await;
    let (code, body) = split_response(&resp);
    assert_eq!(code, 200, "regex k=0: {}", String::from_utf8_lossy(&body));
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let keys: Vec<&str> = parsed["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, vec!["alpha"]);

    // Approximate regex (k=1): `quick` within one edit of "quack"
    // matches both alpha (quick) and gamma (quack).
    let resp = raw(
        addr,
        request(
            "GET",
            "/buckets/docs/search/regex/title?pattern=quack&k=1",
            &[("Accept", "application/json")],
            &[],
        ),
    )
    .await;
    let (code, body) = split_response(&resp);
    assert_eq!(code, 200, "regex k=1: {}", String::from_utf8_lossy(&body));
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let mut keys: Vec<String> = parsed["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["key"].as_str().unwrap().to_string())
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["alpha".to_string(), "gamma".to_string()]);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn vector_knn_returns_nearest_first() {
    let (addr, server) = spawn_with_search().await;

    // Create the vector index (dim 2, L2 distance).
    let resp = raw(
        addr,
        request(
            "POST",
            "/buckets/vecs/index/vector",
            &[("Content-Type", "application/json")],
            br#"{"dim":2,"metric":"l2","codec":"float32"}"#,
        ),
    )
    .await;
    assert_eq!(
        split_response(&resp).0,
        200,
        "create vector index: {}",
        String::from_utf8_lossy(&split_response(&resp).1)
    );

    put_document(addr, "vecs", "origin", r#"{"_vector":[0.0,0.0]}"#).await;
    put_document(addr, "vecs", "unit_x", r#"{"_vector":[1.0,0.0]}"#).await;
    put_document(addr, "vecs", "unit_y", r#"{"_vector":[0.0,1.0]}"#).await;

    let resp = raw(
        addr,
        request(
            "POST",
            "/buckets/vecs/search/vector",
            &[
                ("Content-Type", "application/json"),
                ("Accept", "application/json"),
            ],
            br#"{"query":[0.05,0.05],"k":3}"#,
        ),
    )
    .await;
    let (code, body) = split_response(&resp);
    assert_eq!(code, 200, "knn: {}", String::from_utf8_lossy(&body));
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let hits = parsed["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0]["key"].as_str().unwrap(), "origin");
    // Distances are non-decreasing (nearest first).
    let d0 = hits[0]["distance"].as_f64().unwrap();
    let d1 = hits[1]["distance"].as_f64().unwrap();
    let d2 = hits[2]["distance"].as_f64().unwrap();
    assert!(d0 <= d1 && d1 <= d2, "distances not sorted: {d0} {d1} {d2}");

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn vector_search_response_round_trips_json_and_cbor() {
    let (addr, server) = spawn_with_search().await;

    let resp = raw(
        addr,
        request(
            "POST",
            "/buckets/vecs/index/vector",
            &[("Content-Type", "application/json")],
            br#"{"dim":2,"metric":"l2"}"#,
        ),
    )
    .await;
    assert_eq!(split_response(&resp).0, 200);

    put_document(addr, "vecs", "origin", r#"{"_vector":[0.0,0.0]}"#).await;
    put_document(addr, "vecs", "far", r#"{"_vector":[9.0,9.0]}"#).await;

    let query = br#"{"query":[0.0,0.0],"k":2}"#;

    // JSON response.
    let resp = raw(
        addr,
        request(
            "POST",
            "/buckets/vecs/search/vector",
            &[
                ("Content-Type", "application/json"),
                ("Accept", "application/json"),
            ],
            query,
        ),
    )
    .await;
    let (code, body) = split_response(&resp);
    assert_eq!(code, 200);
    let from_json: VectorSearchResponse = serde_json::from_slice(&body).expect("json decode");
    assert_eq!(from_json.hits.len(), 2);
    assert_eq!(from_json.hits[0].key, "origin");

    // CBOR response decoded through the same wire codec the gateway
    // used to encode it.
    let resp = raw(
        addr,
        request(
            "POST",
            "/buckets/vecs/search/vector",
            &[
                ("Content-Type", "application/json"),
                ("Accept", "application/cbor"),
            ],
            query,
        ),
    )
    .await;
    let (code, body) = split_response(&resp);
    assert_eq!(code, 200);
    let mut cbor = CborCodec::new();
    cbor.register::<VectorSearchResponse>();
    let decoded: Box<dyn ErasedWireValue> = cbor
        .decode(VectorSearchResponse::wire_type_id(), &body)
        .expect("cbor decode");
    let from_cbor = decoded
        .as_any()
        .downcast_ref::<VectorSearchResponse>()
        .expect("downcast");
    assert_eq!(from_cbor.hits.len(), 2);
    assert_eq!(from_cbor.hits[0].key, "origin");
    // JSON and CBOR carry the same logical response.
    assert_eq!(from_cbor, &from_json);

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn search_routes_return_501_without_registry() {
    // A gateway built with plain `serve_http` has no registry wired
    // in; every search route must reply 501.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let dir = tempfile::TempDir::new().expect("tempdir");
    let ds = Arc::new(NoxuDatastore::open_transactional(dir.path()).expect("open"));
    let ds_dyn: Arc<dyn Datastore> = ds.clone();
    let server = tokio::spawn(async move {
        let _keep = dir;
        let _ = serve_http(listener, ds_dyn).await;
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    let cases: Vec<Vec<u8>> = vec![
        request("PUT", "/buckets/docs/index/text/title", &[], &[]),
        request(
            "POST",
            "/buckets/docs/index/vector",
            &[("Content-Type", "application/json")],
            br#"{"dim":2}"#,
        ),
        request("GET", "/buckets/docs/index", &[], &[]),
        request("GET", "/buckets/docs/search/text/title?q=x", &[], &[]),
        request(
            "GET",
            "/buckets/docs/search/regex/title?pattern=x&k=0",
            &[],
            &[],
        ),
        request(
            "POST",
            "/buckets/docs/search/vector",
            &[("Content-Type", "application/json")],
            br#"{"query":[1.0],"k":1}"#,
        ),
    ];
    for req in cases {
        let resp = raw(addr, req).await;
        let (code, body) = split_response(&resp);
        assert_eq!(
            code,
            501,
            "expected 501, got {code}: {}",
            String::from_utf8_lossy(&body)
        );
    }

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn list_indexes_reports_declared_indexes() {
    let (addr, server) = spawn_with_search().await;

    raw(
        addr,
        request("PUT", "/buckets/docs/index/text/title", &[], &[]),
    )
    .await;
    raw(
        addr,
        request("PUT", "/buckets/docs/index/text/body", &[], &[]),
    )
    .await;
    raw(
        addr,
        request(
            "POST",
            "/buckets/docs/index/vector",
            &[("Content-Type", "application/json")],
            br#"{"dim":3,"metric":"cosine"}"#,
        ),
    )
    .await;

    let resp = raw(
        addr,
        request(
            "GET",
            "/buckets/docs/index",
            &[("Accept", "application/json")],
            &[],
        ),
    )
    .await;
    let (code, body) = split_response(&resp);
    assert_eq!(code, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(parsed["bucket"], "docs");
    let mut text_fields: Vec<String> = parsed["text_fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    text_fields.sort();
    assert_eq!(text_fields, vec!["body".to_string(), "title".to_string()]);
    assert_eq!(parsed["vector"]["dim"], 3);
    assert_eq!(parsed["vector"]["metric"], "cosine");

    server.abort();
    let _ = server.await;
}
