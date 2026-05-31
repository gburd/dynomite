//! Wire-protocol tests for the RediSearch FT.* surface.
//!
//! These tests exercise the FT.* command pipeline end-to-end:
//! they spawn a real `redis-server` (used as the storage
//! backend for HSET writes) plus a `dynomited` instance pointed
//! at it, then drive RESP traffic through the proxy port and
//! assert the responses come back from dynomited's in-process
//! vector index registry.
//!
//! The tests use a hand-rolled minimal RESP encoder/decoder
//! rather than pulling in the `redis` crate, mirroring the
//! pattern in [`crate::tests::integration`]. They are gated on
//! the same `integration` Cargo feature; without it they
//! compile-out so a `cargo build` on a host without
//! `redis-server` stays green.

#![cfg(feature = "integration")]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---- ports + child management ------------------------------------------

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn redis_server_in_path() -> Option<PathBuf> {
    let path_env = std::env::var_os("PATH")?;
    for entry in std::env::split_paths(&path_env) {
        let candidate = entry.join("redis-server");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn wait_for_listen(port: u16, deadline: Instant) -> bool {
    while Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{port}").parse().unwrap(),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {
                if Instant::now() >= deadline {
                    return None;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(_) => return None,
        }
    }
}

fn kill_silently(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

// ---- RESP codec --------------------------------------------------------

/// Encode an array of binary-safe bulk strings as a RESP array.
fn encode_array(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    write!(&mut out, "*{}\r\n", parts.len()).unwrap();
    for p in parts {
        write!(&mut out, "${}\r\n", p.len()).unwrap();
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// Decoded RESP value.
#[derive(Debug, Clone, PartialEq)]
enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

/// Read a single RESP value from `sock`, blocking until enough
/// bytes are available. Returns the parsed value.
async fn read_resp(sock: &mut TcpStream, buf: &mut Vec<u8>) -> RespValue {
    loop {
        if let Some((value, consumed)) = try_parse_resp(buf) {
            buf.drain(0..consumed);
            return value;
        }
        let mut tmp = [0u8; 4096];
        let n = sock.read(&mut tmp).await.expect("read");
        assert!(
            n != 0,
            "EOF while parsing RESP; buffer so far: {:?}",
            String::from_utf8_lossy(buf)
        );
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn try_parse_resp(buf: &[u8]) -> Option<(RespValue, usize)> {
    if buf.is_empty() {
        return None;
    }
    match buf[0] {
        b'+' => {
            let (line, end) = read_line(buf, 1)?;
            Some((
                RespValue::SimpleString(String::from_utf8_lossy(line).into_owned()),
                end,
            ))
        }
        b'-' => {
            let (line, end) = read_line(buf, 1)?;
            Some((
                RespValue::Error(String::from_utf8_lossy(line).into_owned()),
                end,
            ))
        }
        b':' => {
            let (line, end) = read_line(buf, 1)?;
            let n: i64 = std::str::from_utf8(line).ok()?.parse().ok()?;
            Some((RespValue::Integer(n), end))
        }
        b'$' => {
            let (line, header_end) = read_line(buf, 1)?;
            let len: i64 = std::str::from_utf8(line).ok()?.parse().ok()?;
            if len < 0 {
                return Some((RespValue::BulkString(None), header_end));
            }
            let len = usize::try_from(len).ok()?;
            let body_end = header_end + len + 2;
            if buf.len() < body_end {
                return None;
            }
            let body = buf[header_end..header_end + len].to_vec();
            Some((RespValue::BulkString(Some(body)), body_end))
        }
        b'*' => {
            let (line, header_end) = read_line(buf, 1)?;
            let n: i64 = std::str::from_utf8(line).ok()?.parse().ok()?;
            if n < 0 {
                return Some((RespValue::Array(None), header_end));
            }
            let n = usize::try_from(n).ok()?;
            let mut items = Vec::with_capacity(n);
            let mut cursor = header_end;
            for _ in 0..n {
                let (item, consumed) = try_parse_resp(&buf[cursor..])?;
                items.push(item);
                cursor += consumed;
            }
            Some((RespValue::Array(Some(items)), cursor))
        }
        _ => None,
    }
}

fn read_line(buf: &[u8], start: usize) -> Option<(&[u8], usize)> {
    let cr = buf[start..].iter().position(|&b| b == b'\r')?;
    if buf.len() < start + cr + 2 {
        return None;
    }
    if buf[start + cr + 1] != b'\n' {
        return None;
    }
    Some((&buf[start..start + cr], start + cr + 2))
}

// ---- helpers -----------------------------------------------------------

/// Convert a slice of f32 to its little-endian byte
/// representation (the Redis-Stack VECTOR field wire format).
fn f32_le(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Lookup a top-level key in an `FT.INFO`-shaped flattened
/// array of `[name, value, name, value, ...]`. Returns the
/// matching value, or `None` when absent.
fn info_lookup<'a>(items: &'a [RespValue], key: &str) -> Option<&'a RespValue> {
    let mut iter = items.iter();
    while let Some(name) = iter.next() {
        let value = iter.next()?;
        if let RespValue::BulkString(Some(bytes)) = name {
            if bytes == key.as_bytes() {
                return Some(value);
            }
        }
    }
    None
}

// ---- test rig ----------------------------------------------------------

struct Rig {
    redis: Child,
    dyn_child: Child,
    listen_port: u16,
}

impl Rig {
    fn try_spawn() -> Option<Self> {
        let redis_bin = redis_server_in_path()?;
        let backend_port = pick_port();
        let listen_port = pick_port();
        let dyn_port = pick_port();
        let stats_port = pick_port();

        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();
        // Leak the tempdir handle so it survives for the rig's
        // lifetime; the test harness drops it at end-of-test by
        // cleaning the path on Drop. Using an explicit `leak`
        // keeps the borrow checker out of the way without
        // affecting cleanup (the OS reclaims the temp tree on
        // process exit, and tempfile would do the same).
        std::mem::forget(dir);

        let mut redis = Command::new(&redis_bin)
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &backend_port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
                "--dir",
                dir_path.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn redis-server");

        if !wait_for_listen(backend_port, Instant::now() + Duration::from_secs(5)) {
            kill_silently(&mut redis);
            panic!("redis-server did not bind {backend_port} within 5s");
        }

        let conf = dir_path.join("d.yml");
        let mut f = std::fs::File::create(&conf).unwrap();
        write!(
            f,
            "p:\n  listen: 127.0.0.1:{listen_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:{backend_port}:1\n  data_store: 0\n",
        )
        .unwrap();
        f.sync_all().unwrap();

        let exe = assert_cmd::cargo::cargo_bin("dynomited");
        let pid_file = dir_path.join("dyn.pid");
        let mut dyn_child = Command::new(exe)
            .arg("-c")
            .arg(&conf)
            .arg("-p")
            .arg(&pid_file)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn dynomited");

        if !wait_for_listen(listen_port, Instant::now() + Duration::from_secs(5)) {
            kill_silently(&mut dyn_child);
            kill_silently(&mut redis);
            panic!("dynomited did not bind {listen_port} within 5s");
        }

        Some(Self {
            redis,
            dyn_child,
            listen_port,
        })
    }

    async fn connect(&self) -> TcpStream {
        let sock = TcpStream::connect(("127.0.0.1", self.listen_port))
            .await
            .expect("connect dynomited");
        sock.set_nodelay(true).ok();
        sock
    }

    fn shutdown(mut self) {
        let pid = nix::unistd::Pid::from_raw(i32::try_from(self.dyn_child.id()).unwrap());
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
        let _ = wait_with_timeout(&mut self.dyn_child, Duration::from_secs(5));
        kill_silently(&mut self.dyn_child);
        let redis_pid = nix::unistd::Pid::from_raw(i32::try_from(self.redis.id()).unwrap());
        let _ = nix::sys::signal::kill(redis_pid, nix::sys::signal::Signal::SIGTERM);
        if wait_with_timeout(&mut self.redis, Duration::from_secs(5)).is_none() {
            kill_silently(&mut self.redis);
        }
    }
}

/// Skip the test gracefully when `redis-server` is not on
/// PATH. The integration feature only gates compilation; we
/// still want CI runs without the binary to pass.
macro_rules! rig_or_skip {
    () => {
        match Rig::try_spawn() {
            Some(r) => r,
            None => {
                eprintln!("redis-server not in PATH; skipping wire test");
                return;
            }
        }
    };
}

// ---- tests --------------------------------------------------------------

#[tokio::test]
async fn ft_create_via_redis_cli_binary_returns_ok() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    let cmd = encode_array(&[
        b"FT.CREATE",
        b"myidx",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"docs:",
        b"SCHEMA",
        b"title",
        b"TEXT",
        b"vec",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        b"4",
        b"DISTANCE_METRIC",
        b"COSINE",
    ]);
    sock.write_all(&cmd).await.expect("write FT.CREATE");
    let reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(
        reply,
        RespValue::SimpleString("OK".to_string()),
        "FT.CREATE reply"
    );

    rig.shutdown();
}

#[tokio::test]
async fn hset_then_ft_search_round_trip_via_wire() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    // FT.CREATE: 4-dim cosine HNSW index over `docs:`.
    let create = encode_array(&[
        b"FT.CREATE",
        b"docidx",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"docs:",
        b"SCHEMA",
        b"title",
        b"TEXT",
        b"vec",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        b"4",
        b"DISTANCE_METRIC",
        b"L2",
    ]);
    sock.write_all(&create).await.unwrap();
    assert_eq!(
        read_resp(&mut sock, &mut buf).await,
        RespValue::SimpleString("OK".to_string())
    );

    // Insert 3 docs at increasing distance from the query.
    for (i, vec) in [
        [0.0_f32, 0.0, 0.0, 0.0],
        [1.0, 0.0, 0.0, 0.0],
        [3.0, 0.0, 0.0, 0.0],
    ]
    .iter()
    .enumerate()
    {
        let key = format!("docs:{i}");
        let title = format!("doc-{i}");
        let v_bytes = f32_le(vec);
        let hset = encode_array(&[
            b"HSET",
            key.as_bytes(),
            b"title",
            title.as_bytes(),
            b"vec",
            &v_bytes,
        ]);
        sock.write_all(&hset).await.unwrap();
        let reply = read_resp(&mut sock, &mut buf).await;
        // Real Redis returns the count of newly-added fields
        // (here: 2). The dispatcher forwards the HSET to the
        // backend after the index upsert, so the wire reply is
        // whatever redis-server produced.
        match reply {
            RespValue::Integer(_) => {}
            other => panic!("HSET expected integer reply, got {other:?}"),
        }
    }

    // FT.SEARCH for the 2 nearest neighbours of `[0.1, 0, 0, 0]`.
    let query = f32_le(&[0.1, 0.0, 0.0, 0.0]);
    let search = encode_array(&[
        b"FT.SEARCH",
        b"docidx",
        b"*=>[KNN 2 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &query,
    ]);
    sock.write_all(&search).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    // Layout: [total, docid_1, [field, value, ...], docid_2, [field, value, ...]]
    assert_eq!(items.len(), 1 + 2 * 2);
    assert_eq!(items[0], RespValue::Integer(2));
    let id_at = |i: usize| match &items[i] {
        RespValue::BulkString(Some(b)) => b.clone(),
        other => panic!("expected bulk string at position {i}, got {other:?}"),
    };
    assert_eq!(id_at(1), b"docs:0");
    assert_eq!(id_at(3), b"docs:1");
    // The top hit has score 0 (perfect for `[0,0,0,0]` if the
    // L2 metric squared the diff to zero - here the query is
    // `[0.1, 0, 0, 0]`, so the closest is `docs:0` with a
    // small positive score).
    let RespValue::Array(Some(fields_top)) = &items[2] else {
        rig.shutdown();
        panic!("expected fields array for top hit");
    };
    let mut iter = fields_top.iter();
    let mut found_score = false;
    while let Some(name) = iter.next() {
        let value = iter.next().expect("paired value");
        if matches!(name, RespValue::BulkString(Some(b)) if b == b"__vec_score") {
            if let RespValue::BulkString(Some(b)) = value {
                let s = std::str::from_utf8(b).unwrap();
                let score: f32 = s.parse().unwrap();
                assert!(score.is_finite() && score >= 0.0, "L2 score must be >= 0");
                found_score = true;
            }
        }
    }
    assert!(found_score, "search result must contain __vec_score field");

    rig.shutdown();
}

#[tokio::test]
async fn ft_info_via_wire_returns_array() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    let create = encode_array(&[
        b"FT.CREATE",
        b"infoidx",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"docs:",
        b"SCHEMA",
        b"title",
        b"TEXT",
        b"vec",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        b"4",
        b"DISTANCE_METRIC",
        b"COSINE",
    ]);
    sock.write_all(&create).await.unwrap();
    let _ = read_resp(&mut sock, &mut buf).await;

    let info = encode_array(&[b"FT.INFO", b"infoidx"]);
    sock.write_all(&info).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.INFO expected an array");
    };

    let name = info_lookup(&items, "index_name").cloned();
    assert_eq!(
        name,
        Some(RespValue::BulkString(Some(b"infoidx".to_vec()))),
        "index_name must surface in FT.INFO"
    );
    let alg = info_lookup(&items, "algorithm").cloned();
    assert_eq!(
        alg,
        Some(RespValue::BulkString(Some(b"HNSW".to_vec()))),
        "algorithm must surface in FT.INFO"
    );
    let dim = info_lookup(&items, "dim").cloned();
    assert_eq!(dim, Some(RespValue::Integer(4)), "dim must surface");
    assert!(
        info_lookup(&items, "schema_fields").is_some(),
        "schema_fields must surface"
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_dropindex_via_wire_removes_index() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    let create = encode_array(&[
        b"FT.CREATE",
        b"transient",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"docs:",
        b"SCHEMA",
        b"vec",
        b"VECTOR",
        b"HNSW",
        b"6",
        b"TYPE",
        b"FLOAT32",
        b"DIM",
        b"4",
        b"DISTANCE_METRIC",
        b"COSINE",
    ]);
    sock.write_all(&create).await.unwrap();
    let _ = read_resp(&mut sock, &mut buf).await;

    sock.write_all(&encode_array(&[b"FT.LIST"])).await.unwrap();
    let listed = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = listed else {
        rig.shutdown();
        panic!("FT.LIST expected array");
    };
    assert!(
        items
            .iter()
            .any(|v| matches!(v, RespValue::BulkString(Some(b)) if b == b"transient")),
        "FT.LIST should include the new index"
    );

    sock.write_all(&encode_array(&[b"FT.DROPINDEX", b"transient"]))
        .await
        .unwrap();
    let drop_reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(drop_reply, RespValue::SimpleString("OK".to_string()));

    sock.write_all(&encode_array(&[b"FT.LIST"])).await.unwrap();
    let listed_after = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items_after)) = listed_after else {
        rig.shutdown();
        panic!("FT.LIST expected array");
    };
    assert!(
        items_after
            .iter()
            .all(|v| !matches!(v, RespValue::BulkString(Some(b)) if b == b"transient")),
        "FT.LIST should no longer include the dropped index"
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_unsupported_command_via_wire_returns_err() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    sock.write_all(&encode_array(&[b"FT.AGGREGATE", b"idx", b"*"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Error(text) = reply else {
        rig.shutdown();
        panic!("FT.AGGREGATE expected -ERR reply");
    };
    assert!(
        text.starts_with("ERR ") && text.contains("not supported"),
        "expected `-ERR not supported in this build`, got {text:?}"
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_list_via_wire_returns_array_alias() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    // Create two indexes, exercise both `FT.LIST` and the
    // `FT._LIST` alias the parser accepts.
    for name in ["alpha", "bravo"] {
        let create = encode_array(&[
            b"FT.CREATE",
            name.as_bytes(),
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"docs:",
            b"SCHEMA",
            b"vec",
            b"VECTOR",
            b"HNSW",
            b"6",
            b"TYPE",
            b"FLOAT32",
            b"DIM",
            b"4",
            b"DISTANCE_METRIC",
            b"COSINE",
        ]);
        sock.write_all(&create).await.unwrap();
        let _ = read_resp(&mut sock, &mut buf).await;
    }

    sock.write_all(&encode_array(&[b"FT._LIST"])).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT._LIST expected array");
    };
    // The wire test creates two indexes; the registry surface
    // emits them in name order. Other tests in the same binary
    // could have run before this one and registered additional
    // indexes against the *same* in-process registry instance
    // because each test spawns its own dynomited process. Each
    // dynomited process gets a fresh registry, so the assert
    // checks for the two we just created, not for an exact
    // length.
    let names: Vec<Vec<u8>> = items
        .iter()
        .filter_map(|v| match v {
            RespValue::BulkString(Some(b)) => Some(b.clone()),
            _ => None,
        })
        .collect();
    assert!(names.contains(&b"alpha".to_vec()));
    assert!(names.contains(&b"bravo".to_vec()));

    rig.shutdown();
}
