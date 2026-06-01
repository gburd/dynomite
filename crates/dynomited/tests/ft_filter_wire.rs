//! Wire-protocol tests for FT.SEARCH filter expressions:
//! the LHS of the optional `=>[KNN ...]` operator now
//! accepts numeric ranges, tag sets, text substrings, and
//! the boolean combinators (AND / OR / NOT / grouping).
//!
//! The tests build on the same harness as `ft_wire.rs` and
//! `ft_extensions_wire.rs`: spawn a real `redis-server` (the
//! HSET storage backend), spawn a `dynomited` proxy in front
//! of it, then drive RESP traffic through the proxy port.
//! The tests are gated on the `integration` Cargo feature
//! and gracefully skip when `redis-server` is not on `PATH`,
//! so a `cargo build` on a host without it stays green.

#![cfg(feature = "integration")]

use std::collections::BTreeSet;
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

#[derive(Debug, Clone, PartialEq)]
enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

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

fn f32_le(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

/// Extract the ordered list of doc keys from an FT.SEARCH-shaped
/// reply with per-hit field arrays. Layout:
/// `[total, k1, [...], k2, [...], ...]`.
fn search_keys(items: &[RespValue]) -> Vec<Vec<u8>> {
    items
        .iter()
        .skip(1)
        .step_by(2)
        .filter_map(|v| match v {
            RespValue::BulkString(Some(b)) => Some(b.clone()),
            _ => None,
        })
        .collect()
}

/// Convenience: collect the doc-key set (order-insensitive)
/// for assertions where the order of results is not part of
/// the contract (filter-only paths don't have a defined
/// ordering until a SORTBY is supplied).
fn search_key_set(items: &[RespValue]) -> BTreeSet<Vec<u8>> {
    search_keys(items).into_iter().collect()
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

        if !wait_for_listen(backend_port, Instant::now() + Duration::from_secs(30)) {
            kill_silently(&mut redis);
            panic!("redis-server did not bind {backend_port} within 30s");
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

        if !wait_for_listen(listen_port, Instant::now() + Duration::from_secs(30)) {
            kill_silently(&mut dyn_child);
            kill_silently(&mut redis);
            panic!("dynomited did not bind {listen_port} within 30s");
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

// ---- shared FT.CREATE + HSET helpers -----------------------------------

/// Provision an index whose schema mixes every metadata
/// field type the filter parser knows: a `score` NUMERIC
/// field, a `body` TEXT field, a `status` TAG field with
/// the default `,` separator, and the canonical 4-dim L2
/// HNSW vector.
async fn create_filter_index(sock: &mut TcpStream, idx: &str) {
    let cmd = encode_array(&[
        b"FT.CREATE",
        idx.as_bytes(),
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"docs:",
        b"SCHEMA",
        b"score",
        b"NUMERIC",
        b"SORTABLE",
        b"body",
        b"TEXT",
        b"status",
        b"TAG",
        b"SEPARATOR",
        b",",
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
    sock.write_all(&cmd).await.expect("write FT.CREATE");
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(sock, &mut buf).await;
    assert_eq!(
        reply,
        RespValue::SimpleString("OK".to_string()),
        "FT.CREATE reply"
    );
}

/// Insert a doc with the four schema fields. `score` is the
/// numeric value (stored as text and parsed back at filter
/// time), `body` is the indexed text, `status` is a comma
/// separated tag list, and `dist` controls the vector's
/// distance from the origin.
async fn hset_doc(
    sock: &mut TcpStream,
    key: &str,
    score: i64,
    body: &str,
    status: &str,
    dist: f32,
) {
    let v_bytes = f32_le(&[dist, 0.0, 0.0, 0.0]);
    let score_str = score.to_string();
    let cmd = encode_array(&[
        b"HSET",
        key.as_bytes(),
        b"score",
        score_str.as_bytes(),
        b"body",
        body.as_bytes(),
        b"status",
        status.as_bytes(),
        b"vec",
        &v_bytes,
    ]);
    sock.write_all(&cmd).await.expect("write HSET");
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(sock, &mut buf).await;
    match reply {
        RespValue::Integer(_) => {}
        other => panic!("HSET expected integer reply, got {other:?}"),
    }
}

/// Run an FT.SEARCH against `idx` with the given query body
/// (no PARAMS / projection clauses) and return the parsed
/// response items.
async fn search(sock: &mut TcpStream, idx: &str, query: &[u8]) -> Vec<RespValue> {
    let cmd = encode_array(&[b"FT.SEARCH", idx.as_bytes(), query]);
    sock.write_all(&cmd).await.expect("write FT.SEARCH");
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(sock, &mut buf).await;
    match reply {
        RespValue::Array(Some(items)) => items,
        other => panic!("FT.SEARCH expected an array, got {other:?}"),
    }
}

async fn search_err(sock: &mut TcpStream, idx: &str, query: &[u8]) -> String {
    let cmd = encode_array(&[b"FT.SEARCH", idx.as_bytes(), query]);
    sock.write_all(&cmd).await.expect("write FT.SEARCH");
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(sock, &mut buf).await;
    match reply {
        RespValue::Error(text) => text,
        other => panic!("FT.SEARCH expected an error reply, got {other:?}"),
    }
}

// ---- numeric range tests ----------------------------------------------

#[tokio::test]
async fn numeric_range_filter_basic() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    // Five docs with scores 100, 150, 200, 300, 500.
    hset_doc(&mut sock, "docs:1", 100, "alpha", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 150, "alpha", "ok", 2.0).await;
    hset_doc(&mut sock, "docs:3", 200, "alpha", "ok", 3.0).await;
    hset_doc(&mut sock, "docs:4", 300, "alpha", "ok", 4.0).await;
    hset_doc(&mut sock, "docs:5", 500, "alpha", "ok", 5.0).await;

    let items = search(&mut sock, "idx", b"@score:[150 300]").await;
    assert_eq!(
        items[0],
        RespValue::Integer(3),
        "expected 3 docs in [150,300], got {items:?}"
    );
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:2".to_vec(), b"docs:3".to_vec(), b"docs:4".to_vec()])
    );

    rig.shutdown();
}

#[tokio::test]
async fn numeric_range_with_inclusive_and_exclusive_bounds() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:a", 100, "x", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:b", 150, "x", "ok", 2.0).await;
    hset_doc(&mut sock, "docs:c", 200, "x", "ok", 3.0).await;

    // Half-open [100, 200) excludes docs:c.
    let items = search(&mut sock, "idx", b"@score:[100 (200]").await;
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:a".to_vec(), b"docs:b".to_vec()])
    );

    // Open-open (100, 200) excludes both ends.
    let items = search(&mut sock, "idx", b"@score:[(100 (200]").await;
    assert_eq!(items[0], RespValue::Integer(1));
    let keys = search_key_set(&items);
    assert_eq!(keys, BTreeSet::from([b"docs:b".to_vec()]));

    rig.shutdown();
}

#[tokio::test]
async fn numeric_range_with_inf_bounds() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", -50, "x", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 0, "x", "ok", 2.0).await;
    hset_doc(&mut sock, "docs:3", 1000, "x", "ok", 3.0).await;

    let items = search(&mut sock, "idx", b"@score:[-inf +inf]").await;
    assert_eq!(items[0], RespValue::Integer(3));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:1".to_vec(), b"docs:2".to_vec(), b"docs:3".to_vec()])
    );

    rig.shutdown();
}

// ---- text substring tests ---------------------------------------------

#[tokio::test]
async fn text_substring_filter_basic() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 0, "errno=11 EAGAIN", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 0, "connection refused", "ok", 2.0).await;
    hset_doc(&mut sock, "docs:3", 0, "errno=2 ENOENT", "ok", 3.0).await;

    let items = search(&mut sock, "idx", b"@body:errno").await;
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:1".to_vec(), b"docs:3".to_vec()])
    );

    rig.shutdown();
}

#[tokio::test]
async fn text_filter_combined_with_knn() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    // docs:1 / docs:3 contain "errno"; docs:2 / docs:4 do not.
    // Distances are 1, 2, 3, 4 so the engine's L2 ranking is
    // doc1 < doc2 < doc3 < doc4.
    hset_doc(&mut sock, "docs:1", 0, "errno=11 EAGAIN", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 0, "connection refused", "ok", 2.0).await;
    hset_doc(&mut sock, "docs:3", 0, "errno=2 ENOENT", "ok", 3.0).await;
    hset_doc(&mut sock, "docs:4", 0, "no match here", "ok", 4.0).await;

    let q = f32_le(&[0.0, 0.0, 0.0, 0.0]);
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"idx",
        b"@body:errno =>[KNN 3 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    // Only the two errno docs survive the filter; KNN ranks
    // them by distance: docs:1 (dist 1) before docs:3 (dist 3).
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_keys(&items);
    assert_eq!(keys, vec![b"docs:1".to_vec(), b"docs:3".to_vec()]);

    rig.shutdown();
}

// ---- tag tests ---------------------------------------------------------

#[tokio::test]
async fn tag_filter_basic() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 0, "x", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 0, "y", "warn", 2.0).await;
    hset_doc(&mut sock, "docs:3", 0, "z", "ok,warn", 3.0).await;

    let items = search(&mut sock, "idx", b"@status:{ok}").await;
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:1".to_vec(), b"docs:3".to_vec()])
    );

    rig.shutdown();
}

#[tokio::test]
async fn tag_filter_multi_value() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 0, "x", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 0, "y", "warn", 2.0).await;
    hset_doc(&mut sock, "docs:3", 0, "z", "error", 3.0).await;

    let items = search(&mut sock, "idx", b"@status:{ok|warn}").await;
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:1".to_vec(), b"docs:2".to_vec()])
    );

    rig.shutdown();
}

// ---- boolean combinator tests -----------------------------------------

#[tokio::test]
async fn boolean_and_two_filters() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 100, "errno=11", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 100, "no match", "ok", 2.0).await;
    hset_doc(&mut sock, "docs:3", 500, "errno=2", "ok", 3.0).await;

    // AND: numeric range AND substring.
    let items = search(&mut sock, "idx", b"@score:[0 200] @body:errno").await;
    assert_eq!(items[0], RespValue::Integer(1));
    let keys = search_key_set(&items);
    assert_eq!(keys, BTreeSet::from([b"docs:1".to_vec()]));

    rig.shutdown();
}

#[tokio::test]
async fn boolean_or_two_filters() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 0, "errno", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 0, "warn", "warn", 2.0).await;
    hset_doc(&mut sock, "docs:3", 0, "neither", "info", 3.0).await;

    // OR: text substring OR tag.
    let items = search(&mut sock, "idx", b"@body:errno | @status:{warn}").await;
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:1".to_vec(), b"docs:2".to_vec()])
    );

    rig.shutdown();
}

#[tokio::test]
async fn boolean_negation_filter() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 0, "x", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 0, "y", "stale", 2.0).await;
    hset_doc(&mut sock, "docs:3", 0, "z", "warn", 3.0).await;

    // NOT: exclude `stale` status.
    let items = search(&mut sock, "idx", b"-@status:{stale}").await;
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:1".to_vec(), b"docs:3".to_vec()])
    );

    rig.shutdown();
}

#[tokio::test]
async fn boolean_grouping_with_parens() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    // (a OR b) AND c:
    //   a = body contains "alpha"
    //   b = body contains "beta"
    //   c = score in [100, 1000]
    //
    // Without parens, `a | b c` would parse as `a | (b c)`,
    // matching every alpha doc regardless of score. With the
    // parens the score filter binds to the OR result.
    hset_doc(&mut sock, "docs:1", 50, "alpha-low", "ok", 1.0).await;
    hset_doc(&mut sock, "docs:2", 200, "alpha-mid", "ok", 2.0).await;
    hset_doc(&mut sock, "docs:3", 500, "beta-mid", "ok", 3.0).await;
    hset_doc(&mut sock, "docs:4", 1500, "beta-high", "ok", 4.0).await;
    hset_doc(&mut sock, "docs:5", 200, "neither", "ok", 5.0).await;

    let items = search(
        &mut sock,
        "idx",
        b"(@body:alpha | @body:beta) @score:[100 1000]",
    )
    .await;
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_key_set(&items);
    assert_eq!(
        keys,
        BTreeSet::from([b"docs:2".to_vec(), b"docs:3".to_vec()])
    );

    rig.shutdown();
}

// ---- error paths -------------------------------------------------------

#[tokio::test]
async fn unsupported_geo_filter_returns_err() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 0, "x", "ok", 1.0).await;

    // RediSearch geo syntax `@loc:[lon lat radius unit]`. The
    // parser surfaces a `not supported` error for the extra
    // tokens inside the bracket.
    let text = search_err(&mut sock, "idx", b"@score:[10 20 5 km]").await;
    assert!(
        text.starts_with("ERR ") && text.contains("not supported"),
        "expected `-ERR not supported`, got {text:?}",
    );

    rig.shutdown();
}

#[tokio::test]
async fn filter_against_undeclared_field_returns_err() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_filter_index(&mut sock, "idx").await;
    hset_doc(&mut sock, "docs:1", 0, "x", "ok", 1.0).await;

    // `@nope` is not in the FT.CREATE schema; the executor
    // rejects it with a syntax error.
    let text = search_err(&mut sock, "idx", b"@nope:[0 100]").await;
    assert!(
        text.starts_with("ERR "),
        "expected `-ERR ...`, got {text:?}"
    );
    assert!(
        text.to_lowercase().contains("nope"),
        "error must reference the unknown field, got {text:?}",
    );

    // Same field declared as TEXT should reject a numeric
    // range over it.
    let text = search_err(&mut sock, "idx", b"@body:[0 100]").await;
    assert!(
        text.starts_with("ERR "),
        "expected `-ERR ...`, got {text:?}"
    );

    rig.shutdown();
}
