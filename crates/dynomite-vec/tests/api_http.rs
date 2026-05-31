//! HTTP API integration test. Drives the dynvec HTTP surface
//! against an in-process server using `hyper`'s legacy client.

#![cfg(feature = "http")]

use std::sync::Arc;

use dynvec::api::serve;
use dynvec::storage::VectorStore;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::header::{HeaderValue, CONTENT_TYPE};
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::net::TcpListener;

async fn spawn_server() -> String {
    let store = Arc::new(VectorStore::in_memory());
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let _ = serve(listener, store).await;
    });
    format!("http://{addr}")
}

async fn http_send(method: Method, url: &str, body: Option<&str>) -> (hyper::StatusCode, Vec<u8>) {
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let mut req = Request::builder().method(method).uri(url);
    let body_bytes = body.map_or_else(Bytes::new, |s| Bytes::from(s.to_string()));
    if body.is_some() {
        req = req.header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    let req = req.body(Full::new(body_bytes)).expect("build req");
    let resp = client.request(req).await.expect("send");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes()
        .to_vec();
    (status, bytes)
}

#[tokio::test]
async fn full_round_trip() {
    let base = spawn_server().await;

    // Healthz.
    let (s, body) = http_send(Method::GET, &format!("{base}/healthz"), None).await;
    assert_eq!(s, 200);
    assert_eq!(body, b"ok");

    // Create table.
    let (s, _) = http_send(
        Method::POST,
        &format!("{base}/tables"),
        Some(r#"{"name":"t","dim":3,"codec":"int8_quantized","distance":"cosine"}"#),
    )
    .await;
    assert_eq!(s, 201);

    // List tables.
    let (s, body) = http_send(Method::GET, &format!("{base}/tables"), None).await;
    assert_eq!(s, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed.as_array().unwrap().len(), 1);

    // Upsert.
    let (s, _) = http_send(
        Method::POST,
        &format!("{base}/tables/t/vectors"),
        Some(r#"{"key":"a","vector":[1.0,0.0,0.0],"metadata":{"label":"x"}}"#),
    )
    .await;
    assert_eq!(s, 201);

    let (s, _) = http_send(
        Method::POST,
        &format!("{base}/tables/t/vectors"),
        Some(r#"{"key":"b","vector":[0.0,1.0,0.0],"metadata":{"label":"y"}}"#),
    )
    .await;
    assert_eq!(s, 201);

    // Get.
    let (s, body) = http_send(Method::GET, &format!("{base}/tables/t/vectors/a"), None).await;
    assert_eq!(s, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["key"], "a");
    assert_eq!(parsed["vector"].as_array().unwrap().len(), 3);
    assert_eq!(parsed["metadata"]["label"], "x");
    assert!(parsed["l2_norm"].as_f64().unwrap() > 0.0);

    // Search.
    let (s, body) = http_send(
        Method::POST,
        &format!("{base}/tables/t/search"),
        Some(r#"{"vector":[0.95,0.05,0.0],"k":2}"#),
    )
    .await;
    assert_eq!(s, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hits = parsed["hits"].as_array().unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0]["key"], "a");

    // Stats.
    let (s, body) = http_send(Method::GET, &format!("{base}/tables/t/stats"), None).await;
    assert_eq!(s, 200);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["live_rows"], 2);

    // Delete.
    let (s, _) = http_send(Method::DELETE, &format!("{base}/tables/t/vectors/a"), None).await;
    assert_eq!(s, 204);
    let (s, _) = http_send(Method::GET, &format!("{base}/tables/t/vectors/a"), None).await;
    assert_eq!(s, 404);
}

#[tokio::test]
async fn unknown_route_is_404() {
    let base = spawn_server().await;
    let (s, _) = http_send(Method::GET, &format!("{base}/nope"), None).await;
    assert_eq!(s, 404);
}

#[tokio::test]
async fn create_table_conflict_returns_409() {
    let base = spawn_server().await;
    let (s, _) = http_send(
        Method::POST,
        &format!("{base}/tables"),
        Some(r#"{"name":"x","dim":2,"codec":"fp16","distance":"euclidean"}"#),
    )
    .await;
    assert_eq!(s, 201);
    let (s, _) = http_send(
        Method::POST,
        &format!("{base}/tables"),
        Some(r#"{"name":"x","dim":2,"codec":"fp16","distance":"euclidean"}"#),
    )
    .await;
    assert_eq!(s, 409);
}

#[tokio::test]
async fn dim_mismatch_returns_400() {
    let base = spawn_server().await;
    http_send(
        Method::POST,
        &format!("{base}/tables"),
        Some(r#"{"name":"d","dim":2,"codec":"fp16","distance":"euclidean"}"#),
    )
    .await;
    let (s, _) = http_send(
        Method::POST,
        &format!("{base}/tables/d/vectors"),
        Some(r#"{"key":"a","vector":[1.0,2.0,3.0]}"#),
    )
    .await;
    assert_eq!(s, 400);
}
