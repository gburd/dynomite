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

    // Naive HTTP parse: split on the first \r\n\r\n; the head
    // carries the status line and headers, the rest is the
    // chunked-encoded body.
    let needle = b"\r\n\r\n";
    let idx = buf
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("body separator");
    let head = std::str::from_utf8(&buf[..idx]).expect("ascii head");
    assert!(head.starts_with("HTTP/1.1 200"), "head was: {head}");
    let mut boundary: Option<String> = None;
    for line in head.split("\r\n") {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-type:") {
            let v = v.trim();
            if let Some(b) = v.strip_prefix("multipart/mixed; boundary=") {
                boundary = Some(b.to_string());
            } else if let Some(b) = v.strip_prefix("multipart/mixed;boundary=") {
                boundary = Some(b.to_string());
            }
        }
    }
    let boundary = boundary.unwrap_or_else(|| panic!("multipart boundary; head was: {head:?}"));

    // The body is chunked-transfer-encoded. Decode chunks before
    // parsing the multipart payload.
    let body_bytes = &buf[idx + needle.len()..];
    let decoded = decode_chunked(body_bytes);
    let decoded_str = std::str::from_utf8(&decoded).expect("ascii body");
    assert!(
        decoded_str.contains(&format!("--{boundary}--\r\n")),
        "body must end with closing delimiter; got: {decoded_str:?}"
    );
    // Parse out the one part body.
    let dash = format!("--{boundary}");
    let parts: Vec<&str> = decoded_str.split(&dash[..]).collect();
    // First chunk before the first boundary is empty; last
    // chunk is the closing `--\r\n`. Real parts are in between.
    let mid: Vec<&str> = parts
        .iter()
        .filter(|s| !s.trim().is_empty() && !s.starts_with("--"))
        .copied()
        .collect();
    assert_eq!(mid.len(), 1, "reduce_sort with keep=true emits one part");
    let raw = mid[0];
    // Skip leading CRLF and the headers up to the blank line.
    let raw = raw.trim_start_matches("\r\n");
    let sep = raw.find("\r\n\r\n").expect("hdr/body sep");
    let part_body = raw[sep + 4..].trim_end_matches("\r\n").as_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(part_body).expect("json part");
    let arr = parsed.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["phase"], 1);
    let data = arr[0]["data"].as_array().expect("data array");
    assert_eq!(data.len(), 3);
    assert_eq!(data[0], serde_json::json!(7));
    assert_eq!(data[2], serde_json::json!(9));

    server.abort();
    let _ = server.await;
}

/// Decode an HTTP/1.1 chunked-transfer-encoded body into the
/// concatenation of its data chunks. Stops at the first zero-size
/// chunk. Trailers (if any) are ignored.
fn decode_chunked(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor < input.len() {
        // Find the size line terminator.
        let Some(rel) = input[cursor..].windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size_line = &input[cursor..cursor + rel];
        cursor += rel + 2;
        // Strip optional chunk extensions after a `;`.
        let size_str = std::str::from_utf8(size_line).expect("ascii size");
        let size_str = size_str.split(';').next().unwrap_or(size_str).trim();
        let size = usize::from_str_radix(size_str, 16).expect("hex size");
        if size == 0 {
            break;
        }
        if cursor + size > input.len() {
            break;
        }
        out.extend_from_slice(&input[cursor..cursor + size]);
        cursor += size;
        // Trailing CRLF after data.
        if cursor + 2 <= input.len() {
            cursor += 2;
        }
    }
    out
}

#[tokio::test]
async fn mapred_via_http_phase_failure_emits_text_part() {
    // A failing phase: the JSON job names a function that does
    // not exist in the registry; the executor short-circuits and
    // the streaming body must end with a `text/plain` part
    // carrying the error message, plus the closing delimiter.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(serve_http(listener, ds));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let body = br#"{
        "inputs": [{"bucket":"b","key":"k","value":1}],
        "query": [{"map": {"name": "no_such_function", "keep": true}}]
    }"#;
    let request = format!(
        "POST /mapred HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let mut sock = TcpStream::connect(addr).await.expect("connect");
    sock.write_all(request.as_bytes()).await.expect("head");
    sock.write_all(body).await.expect("body");
    sock.flush().await.expect("flush");
    let mut buf = Vec::new();
    sock.read_to_end(&mut buf).await.expect("read");

    let needle = b"\r\n\r\n";
    let idx = buf
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("sep");
    let head = std::str::from_utf8(&buf[..idx]).expect("head");
    assert!(head.starts_with("HTTP/1.1 200"));
    let boundary = head
        .split("\r\n")
        .find_map(|l| {
            let lower = l.to_ascii_lowercase();
            lower
                .strip_prefix("content-type:")
                .map(|v| v.trim().to_string())
        })
        .and_then(|v| {
            v.strip_prefix("multipart/mixed; boundary=")
                .or_else(|| v.strip_prefix("multipart/mixed;boundary="))
                .map(str::to_string)
        })
        .unwrap_or_else(|| panic!("boundary; head was: {head:?}"));
    let body_bytes = &buf[idx + needle.len()..];
    let decoded = decode_chunked(body_bytes);
    let decoded_str = std::str::from_utf8(&decoded).expect("ascii");
    assert!(decoded_str.contains("Content-Type: text/plain"));
    assert!(
        decoded_str.contains("no_such_function"),
        "error message must mention the offending function: {decoded_str:?}"
    );
    assert!(decoded_str.contains(&format!("--{boundary}--\r\n")));

    server.abort();
    let _ = server.await;
}
