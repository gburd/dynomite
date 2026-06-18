//! End-to-end integration tests for the Wasm MapReduce phase
//! fitting. Gated behind the `wasm` cargo feature; without it, the
//! test file is empty and the binary contains zero tests.
//!
//! Two angles:
//!
//! * Executor-API tests run a 2-phase pipeline (Wasm map then the
//!   built-in `reduce_count`) directly through
//!   [`dyniak::mapreduce::run_job_with_wasm`].
//! * HTTP tests spin up the gateway via
//!   [`dyniak::serve_http_with_wasm`] and post a
//!   `Phase::WasmModule` job to `POST /mapred`, confirming the
//!   configured module is reached through the live HTTP handler.
//!   A companion test posts the same job to a plain
//!   [`dyniak::serve_http`] gateway (no Wasm store) and confirms
//!   the typed "wasm not implemented" error surfaces in the
//!   multipart error part rather than a panic or wrong result.

#![cfg(feature = "wasm")]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use dyniak::mapreduce::{
    builtins::default_registry, run_job_with_wasm, Inputs, KeyDatum, MapReduceJob, Phase, WasmHook,
    WasmModuleStore,
};
use dyniak::{serve_http, serve_http_with_wasm};
use dynomite::embed::{Datastore, MemoryDatastore};

/// Identity phase module: hand the inbound CBOR bytes straight
/// back through the output meta. Same WAT as the in-crate unit
/// test fixture; duplicated here so the integration binary is
/// self-contained.
const IDENTITY_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $heap_top (mut i32) (i32.const 1024))
      (func $alloc_inner (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap_top))
        (global.set $heap_top
          (i32.add (global.get $heap_top) (local.get $len)))
        (local.get $ptr))
      (func (export "phase_alloc") (param $len i32) (result i32)
        (call $alloc_inner (local.get $len)))
      (func (export "phase_apply")
        (param $in_ptr i32) (param $in_len i32)
        (param $out_ptr_ptr i32) (param $out_len_ptr i32)
        (result i32)
        (local $out_buf i32)
        (local.set $out_buf (call $alloc_inner (local.get $in_len)))
        (memory.copy
          (local.get $out_buf)
          (local.get $in_ptr)
          (local.get $in_len))
        (i32.store (local.get $out_ptr_ptr) (local.get $out_buf))
        (i32.store (local.get $out_len_ptr) (local.get $in_len))
        (i32.const 0)))
"#;

#[tokio::test]
async fn wasm_map_then_builtin_reduce_count() {
    let store = WasmModuleStore::new().expect("wasm store");
    store
        .register("identity", IDENTITY_WAT.as_bytes())
        .expect("register identity wat");
    let hook: Arc<dyn WasmHook> = Arc::new(store);

    let inputs: Vec<KeyDatum> = (0..7u32)
        .map(|i| KeyDatum::with_value("b", format!("k{i}"), serde_json::json!(i)))
        .collect();
    let expected_count = inputs.len();

    let job = MapReduceJob {
        inputs: Inputs::KeyData(inputs),
        phases: vec![
            Phase::WasmModule {
                module_id: "identity".into(),
                fn_name: "apply".into(),
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

    let registry = Arc::new(default_registry());
    let outs = run_job_with_wasm(job, registry, Some(hook))
        .await
        .expect("run ok");

    // Final phase output: a single integer matching input length.
    assert_eq!(outs.len(), 1);
    assert_eq!(outs[0].phase, 1);
    assert_eq!(outs[0].value, serde_json::json!(expected_count));
}

#[tokio::test]
async fn wasm_phase_without_hook_returns_typed_error() {
    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(1))]),
        phases: vec![Phase::WasmModule {
            module_id: "identity".into(),
            fn_name: "apply".into(),
            arg: None,
            keep: false,
        }],
        timeout_ms: None,
    };
    let registry = Arc::new(default_registry());
    let err = run_job_with_wasm(job, registry, None)
        .await
        .expect_err("should error without hook");
    assert!(matches!(
        err,
        dyniak::mapreduce::MrError::WasmNotImplemented
    ));
}

// ------------------------------------------------------------------
// HTTP end-to-end: POST /mapred reaching a configured Wasm module.
// ------------------------------------------------------------------

/// Post `body` to `POST /mapred` on `addr` and return the raw HTTP
/// response bytes (head + chunked body).
async fn post_mapred(addr: std::net::SocketAddr, body: &[u8]) -> Vec<u8> {
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
    buf
}

/// Split an HTTP response into the (head, decoded-body) pair,
/// asserting a `200 OK` status. The body is chunked-transfer
/// decoded into one contiguous string.
fn split_response(buf: &[u8]) -> (String, String) {
    let needle = b"\r\n\r\n";
    let idx = buf
        .windows(needle.len())
        .position(|w| w == needle)
        .expect("body separator");
    let head = std::str::from_utf8(&buf[..idx]).expect("ascii head");
    assert!(head.starts_with("HTTP/1.1 200"), "head was: {head}");
    let decoded = decode_chunked(&buf[idx + needle.len()..]);
    (
        head.to_string(),
        String::from_utf8(decoded).expect("ascii body"),
    )
}

/// Decode an HTTP/1.1 chunked-transfer-encoded body into a single
/// byte vector. Stops at the first zero-size chunk; trailers are
/// ignored.
fn decode_chunked(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut cursor = 0usize;
    while cursor < input.len() {
        let Some(rel) = input[cursor..].windows(2).position(|w| w == b"\r\n") else {
            break;
        };
        let size_line = &input[cursor..cursor + rel];
        cursor += rel + 2;
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
        if cursor + 2 <= input.len() {
            cursor += 2;
        }
    }
    out
}

/// Read the multipart boundary out of an HTTP response head.
fn boundary_of(head: &str) -> String {
    for line in head.split("\r\n") {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-type:") {
            let v = v.trim();
            if let Some(b) = v
                .strip_prefix("multipart/mixed; boundary=")
                .or_else(|| v.strip_prefix("multipart/mixed;boundary="))
            {
                return b.to_string();
            }
        }
    }
    panic!("multipart boundary; head was: {head:?}")
}

#[tokio::test]
async fn wasm_phase_via_http_reaches_configured_module() {
    // Build a store carrying the identity module, then start the
    // real HTTP gateway with the Wasm-aware entry point so the
    // whole RouteCtx -> dispatch -> mapred_response path runs.
    let store = WasmModuleStore::new().expect("wasm store");
    store
        .register("identity", IDENTITY_WAT.as_bytes())
        .expect("register identity wat");
    let store = Arc::new(store);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(serve_http_with_wasm(listener, ds, store));
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Wasm identity map (keep) followed by reduce_count (keep).
    let body = br#"{
        "inputs": [
            {"bucket":"b","key":"k1","value":1},
            {"bucket":"b","key":"k2","value":2},
            {"bucket":"b","key":"k3","value":3}
        ],
        "query": [
            {"wasmmodule": {"module_id":"identity","fn_name":"apply"}},
            {"reduce": {"name":"reduce_count", "keep": true}}
        ]
    }"#;
    let buf = post_mapred(addr, body).await;
    let (head, decoded) = split_response(&buf);
    let boundary = boundary_of(&head);
    assert!(
        decoded.contains(&format!("--{boundary}--\r\n")),
        "body must end with closing delimiter; got: {decoded:?}"
    );

    // One kept part for the final reduce: a count of 3.
    let dash = format!("--{boundary}");
    let mid: Vec<&str> = decoded
        .split(&dash[..])
        .filter(|s| !s.trim().is_empty() && !s.starts_with("--"))
        .collect();
    assert_eq!(mid.len(), 1, "reduce_count with keep=true emits one part");
    let raw = mid[0].trim_start_matches("\r\n");
    let sep = raw.find("\r\n\r\n").expect("hdr/body sep");
    let part_body = raw[sep + 4..].trim_end_matches("\r\n").as_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(part_body).expect("json part");
    let arr = parsed.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["phase"], 1);
    let data = arr[0]["data"].as_array().expect("data array");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0], serde_json::json!(3));

    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn wasm_phase_via_http_without_store_errors() {
    // No Wasm store: the plain gateway must surface the typed
    // WasmNotImplemented error in the trailing text part rather
    // than panic or return a wrong result.
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
    let server = tokio::spawn(serve_http(listener, ds));
    tokio::time::sleep(Duration::from_millis(10)).await;

    let body = br#"{
        "inputs": [ {"bucket":"b","key":"k1","value":1} ],
        "query": [
            {"wasmmodule": {"module_id":"identity","fn_name":"apply","keep":true}}
        ]
    }"#;
    let buf = post_mapred(addr, body).await;
    let (_head, decoded) = split_response(&buf);
    // The error is delivered as a text part carrying the
    // executor's message; the executor maps a hookless Wasm phase
    // to MrError::WasmNotImplemented, whose Display is wired into
    // the "MapReduce execution: ..." framing.
    assert!(
        decoded.to_ascii_lowercase().contains("wasm"),
        "error part must mention wasm; got: {decoded:?}"
    );
    assert!(
        decoded.contains("MapReduce execution"),
        "error part must use the execution framing; got: {decoded:?}"
    );

    server.abort();
    let _ = server.await;
}
