//! End-to-end integration tests for the MapReduce slice.
//!
//! Two angles:
//!
//! * [`mapred_via_executor_api`] -- drives a MapReduce job through
//!   the public [`dyn_riak::mapreduce::run_job`] entry point.
//!   Asserts that the named-builtin pipeline (map + reduce) produces
//!   a deterministic JSON-shaped response.
//! * [`mapred_via_pbc`] -- spins up the PBC server, submits an
//!   `RpbMapRedReq` over a TCP socket, decodes the
//!   `RpbMapRedResp`, and confirms the captured outputs.
//! * [`mapred_via_http`] -- spins up the HTTP gateway, posts the
//!   job to `POST /mapred`, and decodes the JSON response.

use std::sync::Arc;
use std::time::Duration;

use prost::Message as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use dyn_riak::mapreduce::{
    builtins::default_registry, run_job, Inputs, KeyDatum, MapReduceJob, Phase,
};
use dyn_riak::proto::pb::{
    read_frame, write_frame, Frame, MessageCode, RpbMapRedReq, RpbMapRedResp,
};
use dyn_riak::{serve_http, serve_pbc};
use dynomite::embed::{Datastore, MemoryDatastore};

#[tokio::test]
async fn mapred_via_executor_api() {
    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![
            KeyDatum::with_value("b", "k1", serde_json::json!(10)),
            KeyDatum::with_value("b", "k2", serde_json::json!(20)),
            KeyDatum::with_value("b", "k3", serde_json::json!(30)),
        ]),
        phases: vec![
            Phase::Map {
                fn_name: "map_object_value".into(),
                arg: None,
                keep: false,
            },
            Phase::Reduce {
                fn_name: "reduce_sum".into(),
                arg: None,
                keep: true,
            },
        ],
        timeout_ms: None,
    };
    let out = run_job(job, Arc::new(default_registry()))
        .await
        .expect("ok");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].phase, 1);
    assert_eq!(out[0].value, serde_json::json!(60));
}

#[tokio::test]
async fn mapred_via_pbc() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(serve_pbc(listener, ds));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut r, mut w) = tokio::io::split(stream);

    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![
            KeyDatum::with_value("b", "k1", serde_json::json!(1)),
            KeyDatum::with_value("b", "k2", serde_json::json!(2)),
            KeyDatum::with_value("b", "k3", serde_json::json!(3)),
            KeyDatum::with_value("b", "k4", serde_json::json!(4)),
        ]),
        phases: vec![
            Phase::Map {
                fn_name: "map_object_value".into(),
                arg: None,
                keep: false,
            },
            Phase::Reduce {
                fn_name: "reduce_count".into(),
                arg: None,
                keep: true,
            },
        ],
        timeout_ms: None,
    };
    let body = serde_json::to_vec(&job).expect("encode");
    let req = RpbMapRedReq {
        request: body,
        content_type: b"application/json".to_vec(),
    };
    write_frame(
        &mut w,
        &Frame::new(MessageCode::MapRedReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("write");

    let frame = read_frame(&mut r).await.expect("read");
    assert_eq!(frame.code, MessageCode::MapRedResp.as_u8());
    let resp = RpbMapRedResp::decode(frame.body.as_slice()).expect("decode");
    assert_eq!(resp.done, Some(true));
    let body = resp.response.expect("response present");
    let parsed: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let arr = parsed.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["phase"], 1);
    assert_eq!(arr[0]["value"], serde_json::json!(4));

    drop(r);
    drop(w);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn mapred_via_pbc_unsupported_content_type() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(serve_pbc(listener, ds));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut r, mut w) = tokio::io::split(stream);

    let req = RpbMapRedReq {
        request: b"<doc/>".to_vec(),
        content_type: b"application/xml".to_vec(),
    };
    write_frame(
        &mut w,
        &Frame::new(MessageCode::MapRedReq.as_u8(), req.encode_to_vec()),
    )
    .await
    .expect("write");

    let frame = read_frame(&mut r).await.expect("read");
    assert_eq!(frame.code, MessageCode::ErrorResp.as_u8());

    drop(r);
    drop(w);
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn mapred_via_http() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(serve_http(listener, ds));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let body = br#"{
        "inputs": [
            {"bucket":"b","key":"k1","value":7},
            {"bucket":"b","key":"k2","value":8},
            {"bucket":"b","key":"k3","value":9}
        ],
        "query": [
            {"map":    {"name":"map_object_value"}},
            {"reduce": {"name":"reduce_sort", "keep": true}}
        ]
    }"#;
    let request = format!(
        "POST /mapred HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );

    let mut sock = TcpStream::connect(addr).await.expect("connect");
    sock.write_all(request.as_bytes())
        .await
        .expect("write head");
    sock.write_all(body).await.expect("write body");
    sock.flush().await.expect("flush");

    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.expect("read");

    // Naive HTTP parse: split on the first \r\n\r\n; the body is
    // the remainder.
    let needle = b"\r\n\r\n";
    let idx = buf
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("body separator");
    let head = std::str::from_utf8(&buf[..idx]).expect("ascii head");
    assert!(head.starts_with("HTTP/1.1 200"), "head was: {head}");
    let body = &buf[idx + needle.len()..];
    let parsed: serde_json::Value = serde_json::from_slice(body).expect("json");
    let arr = parsed.as_array().expect("array");
    // reduce_sort emits three sorted outputs, all from phase 1.
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["phase"], 1);
    assert_eq!(arr[0]["value"], serde_json::json!(7));
    assert_eq!(arr[2]["value"], serde_json::json!(9));

    server.abort();
    let _ = server.await;
}
