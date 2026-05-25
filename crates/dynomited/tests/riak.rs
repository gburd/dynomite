//! End-to-end integration tests for the optional Riak protocol surface.
//!
//! Compiled only when `dynomited` is built with the `riak` Cargo
//! feature. Each test:
//!
//! 1. Picks ephemeral ports for the client / dnode / stats listeners
//!    and for the Riak PBC and HTTP listeners.
//! 2. Builds a [`dynomited::server::Server`] from an in-memory YAML
//!    config that wires the Riak block.
//! 3. Drives a single request against the relevant listener and
//!    asserts the response shape.
//! 4. Calls [`dynomited::server::ShutdownHandle::shutdown`] and waits
//!    for the run loop to drain.
//!
//! The tests deliberately drive the server in-process (rather than
//! spawning the binary) so they need no external `redis-server` or
//! filesystem state. They run on every CI invocation that includes
//! `--features riak`.

#![cfg(feature = "riak")]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use dynomite::conf::Config;
use dynomited::server::Server;

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn pick_distinct_ports(n: usize) -> Vec<u16> {
    let mut out = Vec::new();
    while out.len() < n {
        let p = pick_port();
        if !out.contains(&p) {
            out.push(p);
        }
    }
    out
}

fn yaml(
    listen: u16,
    dyn_listen: u16,
    stats_listen: u16,
    riak_pbc: Option<u16>,
    riak_http: Option<u16>,
    aae: bool,
) -> String {
    let mut s = format!(
        "p:\n  listen: 127.0.0.1:{listen}\n  dyn_listen: 127.0.0.1:{dyn_listen}\n  stats_listen: 127.0.0.1:{stats_listen}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n",
    );
    if riak_pbc.is_some() || riak_http.is_some() || aae {
        s.push_str("  riak:\n");
        if let Some(p) = riak_pbc {
            use std::fmt::Write as _;
            writeln!(s, "    pbc_listen: 127.0.0.1:{p}").unwrap();
        }
        if let Some(p) = riak_http {
            use std::fmt::Write as _;
            writeln!(s, "    http_listen: 127.0.0.1:{p}").unwrap();
        }
        if aae {
            s.push_str("    aae_enabled: true\n");
            s.push_str("    aae_full_sweep_interval_seconds: 60\n");
            s.push_str("    aae_segment_interval_seconds: 5\n");
        }
    }
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn riak_pbc_ping_round_trip() {
    let ports = pick_distinct_ports(4);
    let cfg = Config::parse_str(&yaml(
        ports[0],
        ports[1],
        ports[2],
        Some(ports[3]),
        None,
        false,
    ))
    .unwrap();

    let server = Server::build(cfg).await.expect("build");
    let pbc_addr = server.riak_pbc_addr().expect("pbc bound");
    assert!(server.riak_http_addr().is_none());
    let handle = server.shutdown_handle();
    let supervisor = tokio::spawn(async move { server.run().await });

    // Wait for the listener to be ready. The TcpListener is
    // bound by Server::build, so a connect attempt should
    // succeed on the first poll; we still allow a few retries
    // for tokio's scheduler.
    let mut sock = None;
    for _ in 0..20 {
        if let Ok(s) = TcpStream::connect(pbc_addr).await {
            sock = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut sock = sock.expect("connect to riak pbc");
    sock.set_nodelay(true).ok();

    // RpbPingReq is message code 1 with an empty body. Frame is
    // [4-byte BE total-length][1-byte msg-code][body]. Total
    // length is `1 + body.len()` = 1.
    let frame = {
        let body: Vec<u8> = Vec::new();
        let total: u32 = u32::try_from(body.len() + 1).unwrap();
        let mut out = Vec::with_capacity(5 + body.len());
        out.extend_from_slice(&total.to_be_bytes());
        out.push(1u8);
        out.extend_from_slice(&body);
        out
    };
    sock.write_all(&frame).await.expect("write ping");

    // Read the reply: 4-byte length + 1-byte code + body.
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.expect("read len");
    let total = u32::from_be_bytes(len_buf) as usize;
    assert!((1..1024).contains(&total), "total={total}");
    let mut payload = vec![0u8; total];
    sock.read_exact(&mut payload).await.expect("read payload");
    // The first byte is the message code. RpbPingResp = 2.
    assert_eq!(payload[0], 2u8, "expected RpbPingResp code");
    // Body is empty for a ping response.
    let body = &payload[1..];
    assert!(body.is_empty(), "RpbPingResp has empty body; got {body:?}");

    drop(sock);
    handle.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("supervisor stuck")
        .expect("join");
    assert!(res.is_ok(), "run returned: {res:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn riak_http_ping_returns_200() {
    let ports = pick_distinct_ports(4);
    let cfg = Config::parse_str(&yaml(
        ports[0],
        ports[1],
        ports[2],
        None,
        Some(ports[3]),
        false,
    ))
    .unwrap();

    let server = Server::build(cfg).await.expect("build");
    let http_addr = server.riak_http_addr().expect("http bound");
    assert!(server.riak_pbc_addr().is_none());
    let handle = server.shutdown_handle();
    let supervisor = tokio::spawn(async move { server.run().await });

    // Drive a raw HTTP/1.1 GET /ping. The dyn-riak gateway
    // accepts both GET and HEAD on /ping with a 200 response.
    let mut sock = None;
    for _ in 0..20 {
        if let Ok(s) = TcpStream::connect(http_addr).await {
            sock = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut sock = sock.expect("connect to riak http");
    sock.set_nodelay(true).ok();

    let request = format!("GET /ping HTTP/1.1\r\nHost: {http_addr}\r\nConnection: close\r\n\r\n");
    sock.write_all(request.as_bytes())
        .await
        .expect("write request");

    // Read until EOF or until we see the status line plus a few
    // bytes of headers; we only assert on the status.
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut buf)).await;
    let head_text = String::from_utf8_lossy(&buf);
    assert!(
        head_text.starts_with("HTTP/1.1 200"),
        "expected 200 status; got: {head_text:?}"
    );

    handle.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("supervisor stuck")
        .expect("join");
    assert!(res.is_ok(), "run returned: {res:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn riak_aae_enabled_does_not_block_shutdown() {
    let ports = pick_distinct_ports(4);
    let cfg = Config::parse_str(&yaml(
        ports[0],
        ports[1],
        ports[2],
        Some(ports[3]),
        None,
        true,
    ))
    .unwrap();

    let server = Server::build(cfg).await.expect("build");
    assert!(server.riak_pbc_addr().is_some());
    let handle = server.shutdown_handle();
    let supervisor = tokio::spawn(async move { server.run().await });
    // Give the AAE task time to fire its first tick under the
    // configured 5s segment interval, but we do not actually
    // need to observe it. The point of the test is that
    // shutdown still completes promptly.
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("supervisor stuck after shutdown")
        .expect("join");
    assert!(res.is_ok(), "run returned: {res:?}");
}

/// Helper: write a Riak PBC frame onto `sock`.
#[cfg(test)]
async fn pbc_send(sock: &mut TcpStream, code: u8, body: &[u8]) {
    let total = u32::try_from(body.len() + 1).unwrap();
    sock.write_all(&total.to_be_bytes()).await.unwrap();
    sock.write_all(&[code]).await.unwrap();
    sock.write_all(body).await.unwrap();
}

/// Helper: read a Riak PBC frame from `sock`, returning
/// `(message_code, body_bytes)`.
#[cfg(test)]
async fn pbc_recv(sock: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut len = [0u8; 4];
    sock.read_exact(&mut len).await.unwrap();
    let total = u32::from_be_bytes(len) as usize;
    let mut payload = vec![0u8; total];
    sock.read_exact(&mut payload).await.unwrap();
    (payload[0], payload[1..].to_vec())
}

/// `data_store: noxu` happy-path: open a Noxu environment in a
/// tempdir, drive an `RpbPutReq` carrying two index entries via
/// the Riak PBC listener, and assert that an `RpbIndexReq`
/// equality query returns the stored key. The configured
/// backend supervisor is the in-process Noxu supervisor; the
/// PBC listener shares the same Noxu environment, so the put
/// and the query travel through the same backing store.
#[tokio::test(flavor = "multi_thread")]
async fn riak_pbc_2i_against_noxu_round_trip() {
    use prost::Message as _;

    // Tests run inside the `dynomited` lib's test harness, which
    // does not boot through `main()`; flip the noxu-supported
    // toggle by hand so the configuration validator accepts
    // `data_store: noxu`. The flag is process-wide; setting it
    // is idempotent.
    dynomite::conf::set_noxu_supported(true);

    let dir = tempfile::TempDir::new().expect("tempdir");
    let ports = pick_distinct_ports(4);
    let yaml_text = format!(
        "p:\n  listen: 127.0.0.1:{listen}\n  dyn_listen: 127.0.0.1:{dyn_listen}\n  stats_listen: 127.0.0.1:{stats}\n  tokens: '101134286'\n  data_store: noxu\n  noxu_path: {noxu}\n  servers:\n  - 127.0.0.1:6379:1\n  riak:\n    pbc_listen: 127.0.0.1:{pbc}\n",
        listen = ports[0],
        dyn_listen = ports[1],
        stats = ports[2],
        pbc = ports[3],
        noxu = dir.path().display(),
    );
    let cfg = Config::parse_str(&yaml_text).unwrap();
    let server = Server::build(cfg).await.expect("build");
    let pbc_addr = server.riak_pbc_addr().expect("pbc bound");
    let handle = server.shutdown_handle();
    let supervisor = tokio::spawn(async move { server.run().await });

    // Wait for the listener.
    let mut sock = None;
    for _ in 0..40 {
        if let Ok(s) = TcpStream::connect(pbc_addr).await {
            sock = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut sock = sock.expect("connect to riak pbc");
    sock.set_nodelay(true).ok();

    // Put alice with age_int=42 and city_bin=seattle.
    let put = dyn_riak::proto::pb::RpbPutReq {
        bucket: b"users".to_vec(),
        key: Some(b"alice".to_vec()),
        value: b"profile".to_vec(),
        indexes: vec![
            dyn_riak::proto::pb::RpbPair {
                key: b"age_int".to_vec(),
                value: Some(b"42".to_vec()),
            },
            dyn_riak::proto::pb::RpbPair {
                key: b"city_bin".to_vec(),
                value: Some(b"seattle".to_vec()),
            },
        ],
        ..dyn_riak::proto::pb::RpbPutReq::default()
    };
    pbc_send(&mut sock, 11, &put.encode_to_vec()).await;
    let (code, _) = pbc_recv(&mut sock).await;
    assert_eq!(code, 12, "expected RpbPutResp");

    // Equality query on age_int=42 returns alice.
    let idx_eq = dyn_riak::proto::pb::RpbIndexReq {
        bucket: b"users".to_vec(),
        index: b"age_int".to_vec(),
        qtype: dyn_riak::proto::pb::INDEX_QUERY_TYPE_EQ,
        key: Some(b"42".to_vec()),
        ..dyn_riak::proto::pb::RpbIndexReq::default()
    };
    pbc_send(&mut sock, 25, &idx_eq.encode_to_vec()).await;
    // Drain frames until we see done=true; with the streaming
    // RpbIndexResp shape (commit dyn-riak/streaming-mr-2i),
    // small result sets may arrive as one chunk frame plus a
    // terminator frame with the done flag set.
    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    loop {
        let (code, body) = pbc_recv(&mut sock).await;
        assert_eq!(code, 26, "expected RpbIndexResp");
        let resp = dyn_riak::proto::pb::RpbIndexResp::decode(body.as_slice()).expect("decode resp");
        all_keys.extend(resp.keys);
        if resp.done == Some(true) {
            break;
        }
    }
    assert_eq!(all_keys, vec![b"alice".to_vec()]);

    // Range query on age_int [10, 50] also returns alice.
    let idx_rg = dyn_riak::proto::pb::RpbIndexReq {
        bucket: b"users".to_vec(),
        index: b"age_int".to_vec(),
        qtype: dyn_riak::proto::pb::INDEX_QUERY_TYPE_RANGE,
        range_min: Some(b"10".to_vec()),
        range_max: Some(b"50".to_vec()),
        ..dyn_riak::proto::pb::RpbIndexReq::default()
    };
    pbc_send(&mut sock, 25, &idx_rg.encode_to_vec()).await;
    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    loop {
        let (code, body) = pbc_recv(&mut sock).await;
        assert_eq!(code, 26);
        let resp = dyn_riak::proto::pb::RpbIndexResp::decode(body.as_slice()).expect("decode resp");
        all_keys.extend(resp.keys);
        if resp.done == Some(true) {
            break;
        }
    }
    assert_eq!(all_keys, vec![b"alice".to_vec()]);

    drop(sock);
    handle.shutdown();
    let res = tokio::time::timeout(Duration::from_secs(5), supervisor)
        .await
        .expect("supervisor stuck")
        .expect("join");
    assert!(res.is_ok(), "run returned: {res:?}");
}

/// Integration test for the `riak.wasm_modules:` YAML block.
///
/// Drives the full happy path:
///   1. Write a tiny identity WAT module to disk under a
///      `tempfile::tempdir()`.
///   2. Build a `ConfRiak` referencing the file by `id` /
///      `path`. Validate it (catches the new uniqueness +
///      file-exists checks).
///   3. Call [`dynomited::riak::build_wasm_store_from_config`]
///      which is the same loader the server's startup wiring
///      goes through. Confirm the resulting store contains the
///      expected `id`.
///   4. Pass the store through to the MapReduce executor and
///      submit a `Phase::WasmModule { module_id }` job; assert
///      that the executor accepts the phase (no
///      `WasmModuleNotFound`, no `WasmNotImplemented`) and the
///      identity module round-trips its inputs.
#[cfg(feature = "wasm")]
#[tokio::test]
async fn riak_wasm_modules_yaml_loads_and_executor_accepts_phase() {
    use std::sync::Arc;

    use dyn_riak::mapreduce::{
        builtins::default_registry, run_job_with_wasm, Inputs, KeyDatum, MapReduceJob, Phase,
        WasmHook,
    };
    use dynomite::conf::{ConfRiak, ConfRiakWasmModule};

    /// Identity module: copies the inbound CBOR-encoded
    /// `Vec<Value>` straight back to the host.
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

    let tmp = tempfile::tempdir().expect("tmpdir");
    let path = tmp.path().join("identity.wat");
    std::fs::write(&path, IDENTITY_WAT).expect("write wat");

    let cfg = ConfRiak {
        wasm_modules: Some(vec![ConfRiakWasmModule {
            id: "identity".into(),
            path: path.clone(),
        }]),
        ..ConfRiak::default()
    };
    cfg.validate().expect("validate ok");

    let store = dynomited::riak::build_wasm_store_from_config(&cfg)
        .expect("build store")
        .expect("store is Some when wasm_modules has entries");
    assert!(store.contains("identity"));
    assert_eq!(store.count(), 1);

    let hook: Arc<dyn WasmHook> = store.clone();
    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![
            KeyDatum::with_value("b", "k1", serde_json::json!(10)),
            KeyDatum::with_value("b", "k2", serde_json::json!(20)),
        ]),
        phases: vec![Phase::WasmModule {
            module_id: "identity".into(),
            fn_name: "apply".into(),
            arg: None,
            keep: true,
        }],
        timeout_ms: None,
    };
    let registry = Arc::new(default_registry());
    let outputs = run_job_with_wasm(job, registry, Some(hook))
        .await
        .expect("executor accepts Phase::WasmModule");
    assert_eq!(outputs.len(), 2);
    assert_eq!(outputs[0].value["bucket"], "b");
    assert_eq!(outputs[0].value["value"], serde_json::json!(10));
}

/// Negative-path integration test: the validator must catch
/// non-existent paths before the loader runs, so the operator
/// gets a clear error at config time rather than a vague
/// I/O failure at startup.
#[cfg(feature = "wasm")]
#[tokio::test]
async fn riak_wasm_modules_validate_rejects_missing_path() {
    use dynomite::conf::{ConfRiak, ConfRiakWasmModule};
    let cfg = ConfRiak {
        wasm_modules: Some(vec![ConfRiakWasmModule {
            id: "missing".into(),
            path: std::path::PathBuf::from("/no/such/path/at/all.wasm"),
        }]),
        ..ConfRiak::default()
    };
    let err = cfg.validate().expect_err("missing path must reject");
    let msg = err.to_string();
    assert!(msg.contains("wasm_modules.path"), "unexpected error: {msg}");
}
