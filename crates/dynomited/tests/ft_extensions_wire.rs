//! Wire-protocol tests for the FT.* command surface
//! extensions: `FT.SEARCH` projection clauses (`RETURN`,
//! `LIMIT`, `SORTBY`, `NOCONTENT`), `FT.AGGREGATE`
//! `GROUPBY` / `REDUCE` pipelines, `FT.EXPLAIN`, and
//! `FT.ALTER ADD`.
//!
//! The tests build on the Phase D / Phase 4 patterns in
//! `ft_wire.rs` and `ft_text_wire.rs`: they spawn a real
//! `redis-server` (used as the storage backend for HSETs)
//! plus a `dynomited` instance, then drive RESP traffic
//! through the proxy port and assert the responses come
//! back from dynomited's in-process FT.* surface. The tests
//! are gated on the same `integration` Cargo feature and
//! gracefully skip when `redis-server` is not on PATH so a
//! `cargo build` on a host without it stays green.

#![cfg(feature = "integration")]

use std::collections::BTreeMap;
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

/// Extract the doc keys from an FT.SEARCH-shaped reply with
/// per-hit field arrays. Layout: `[total, k1, [...], k2, ...]`.
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

/// Extract the per-hit field arrays. Returns one
/// `Vec<(name, value)>` per hit in result order.
fn search_fields(items: &[RespValue]) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    for chunk in items.iter().skip(2).step_by(2) {
        let RespValue::Array(Some(fields)) = chunk else {
            continue;
        };
        let mut row = Vec::new();
        let mut iter = fields.iter();
        while let Some(name) = iter.next() {
            let value = iter.next().expect("paired value");
            let RespValue::BulkString(Some(name_b)) = name else {
                continue;
            };
            let RespValue::BulkString(Some(value_b)) = value else {
                continue;
            };
            row.push((name_b.clone(), value_b.clone()));
        }
        out.push(row);
    }
    out
}

/// Extract bare doc keys from a `NOCONTENT` reply:
/// `[total, k1, k2, ...]` (no per-hit field arrays).
fn search_nocontent_keys(items: &[RespValue]) -> Vec<Vec<u8>> {
    items
        .iter()
        .skip(1)
        .filter_map(|v| match v {
            RespValue::BulkString(Some(b)) => Some(b.clone()),
            _ => None,
        })
        .collect()
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

/// Provision a 4-dim L2 HNSW index with `category` (TAG-style
/// metadata stored as text), `score` (numeric metadata stored
/// as text), `body` (TEXT), and the canonical `vec` VECTOR
/// field. The metadata fields exercise SORTBY (alphabetic and
/// numeric) and the FT.AGGREGATE GROUPBY pipeline.
async fn create_extended_index(sock: &mut TcpStream, idx: &str) {
    let cmd = encode_array(&[
        b"FT.CREATE",
        idx.as_bytes(),
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"docs:",
        b"SCHEMA",
        b"category",
        b"TEXT",
        b"score",
        b"TEXT",
        b"body",
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
    sock.write_all(&cmd).await.expect("write FT.CREATE");
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(sock, &mut buf).await;
    assert_eq!(
        reply,
        RespValue::SimpleString("OK".to_string()),
        "FT.CREATE reply"
    );
}

/// Insert a doc with all four schema fields: `category`,
/// `score`, `body`, and a 4-dim float vector keyed off
/// `dist` so tests can rank hits by distance from the
/// origin.
async fn hset_doc(
    sock: &mut TcpStream,
    key: &str,
    category: &str,
    score: &str,
    body: &str,
    dist: f32,
) {
    let v_bytes = f32_le(&[dist, 0.0, 0.0, 0.0]);
    let cmd = encode_array(&[
        b"HSET",
        key.as_bytes(),
        b"category",
        category.as_bytes(),
        b"score",
        score.as_bytes(),
        b"body",
        body.as_bytes(),
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

// ---- tests --------------------------------------------------------------

#[tokio::test]
async fn ft_search_return_clause_filters_response_fields() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;
    hset_doc(&mut sock, "docs:1", "alpha", "10", "hello world", 1.0).await;
    hset_doc(&mut sock, "docs:2", "beta", "20", "hello there", 2.0).await;

    // KNN over the full index, but ask for only `category` in
    // the per-hit projection. The implicit `__vec_score` and
    // every other metadata field must be absent.
    let q = f32_le(&[0.0, 0.0, 0.0, 0.0]);
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"myidx",
        b"*=>[KNN 2 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
        b"RETURN",
        b"1",
        b"category",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(2));
    let fields = search_fields(&items);
    assert_eq!(fields.len(), 2);
    for row in &fields {
        // Exactly the requested field; nothing else (no
        // __vec_score, no body, no score).
        assert_eq!(row.len(), 1, "RETURN must filter to one field, got {row:?}");
        assert_eq!(row[0].0, b"category");
    }

    rig.shutdown();
}

#[tokio::test]
async fn ft_search_limit_clause_paginates() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;
    // Five docs at distance 1..5; KNN k=5 finds them all.
    for i in 1..=5_u32 {
        let key = format!("docs:{i}");
        let body = format!("body-{i}");
        // The loop bound (1..=5) is well within the f32
        // mantissa, so the cast is exact in practice.
        let dist = f32::from(u16::try_from(i).unwrap_or(0));
        hset_doc(&mut sock, &key, "any", "0", &body, dist).await;
    }
    let q = f32_le(&[0.0, 0.0, 0.0, 0.0]);

    // LIMIT 1 2 -> skip first hit, return 2.
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"myidx",
        b"*=>[KNN 5 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
        b"LIMIT",
        b"1",
        b"2",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_keys(&items);
    assert_eq!(keys, vec![b"docs:2".to_vec(), b"docs:3".to_vec()]);

    // LIMIT 0 1 -> just the closest.
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"myidx",
        b"*=>[KNN 5 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
        b"LIMIT",
        b"0",
        b"1",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(1));
    let keys = search_keys(&items);
    assert_eq!(keys, vec![b"docs:1".to_vec()]);

    // LIMIT past the end -> empty.
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"myidx",
        b"*=>[KNN 5 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
        b"LIMIT",
        b"100",
        b"10",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(0));
    assert_eq!(items.len(), 1, "LIMIT past end should produce no hits");

    rig.shutdown();
}

#[tokio::test]
async fn ft_search_nocontent_returns_only_keys() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;
    hset_doc(&mut sock, "docs:1", "a", "1", "hello", 1.0).await;
    hset_doc(&mut sock, "docs:2", "a", "2", "world", 2.0).await;

    let q = f32_le(&[0.0, 0.0, 0.0, 0.0]);
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"myidx",
        b"*=>[KNN 2 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
        b"NOCONTENT",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    // Layout: [total, k1, k2] (no per-hit field arrays).
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], RespValue::Integer(2));
    let keys = search_nocontent_keys(&items);
    assert_eq!(keys, vec![b"docs:1".to_vec(), b"docs:2".to_vec()]);
    // None of the trailing items should be field arrays.
    for v in items.iter().skip(1) {
        assert!(matches!(v, RespValue::BulkString(_)));
    }

    rig.shutdown();
}

#[tokio::test]
async fn ft_search_sortby_metadata_field_orders_results() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;
    // Distances are deliberately reversed against `score`
    // so the engine's distance order would put `docs:1`
    // first; SORTBY @score DESC must flip it.
    hset_doc(&mut sock, "docs:1", "a", "10", "hello", 1.0).await;
    hset_doc(&mut sock, "docs:2", "a", "30", "world", 2.0).await;
    hset_doc(&mut sock, "docs:3", "a", "20", "again", 3.0).await;

    // ASC by numeric `score` -> 10, 20, 30 -> docs:1, docs:3, docs:2.
    let q = f32_le(&[0.0, 0.0, 0.0, 0.0]);
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"myidx",
        b"*=>[KNN 3 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
        b"SORTBY",
        b"@score",
        b"ASC",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(3));
    let keys = search_keys(&items);
    assert_eq!(
        keys,
        vec![b"docs:1".to_vec(), b"docs:3".to_vec(), b"docs:2".to_vec()]
    );

    // DESC by numeric `score` -> 30, 20, 10.
    let cmd = encode_array(&[
        b"FT.SEARCH",
        b"myidx",
        b"*=>[KNN 3 @vec $blob]",
        b"PARAMS",
        b"2",
        b"blob",
        &q,
        b"SORTBY",
        b"@score",
        b"DESC",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(3));
    let keys = search_keys(&items);
    assert_eq!(
        keys,
        vec![b"docs:2".to_vec(), b"docs:3".to_vec(), b"docs:1".to_vec()]
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_aggregate_groupby_count_returns_per_group_counts() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;
    // 3 alpha, 2 beta, 1 gamma.
    hset_doc(&mut sock, "docs:1", "alpha", "0", "x", 1.0).await;
    hset_doc(&mut sock, "docs:2", "alpha", "0", "y", 2.0).await;
    hset_doc(&mut sock, "docs:3", "alpha", "0", "z", 3.0).await;
    hset_doc(&mut sock, "docs:4", "beta", "0", "p", 4.0).await;
    hset_doc(&mut sock, "docs:5", "beta", "0", "q", 5.0).await;
    hset_doc(&mut sock, "docs:6", "gamma", "0", "r", 6.0).await;

    let cmd = encode_array(&[
        b"FT.AGGREGATE",
        b"myidx",
        b"*",
        b"GROUPBY",
        b"1",
        b"@category",
        b"REDUCE",
        b"COUNT",
        b"0",
        b"AS",
        b"n",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.AGGREGATE expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(3), "three groups");

    // Walk every group row and accumulate (category -> n).
    let mut by_cat: BTreeMap<Vec<u8>, i64> = BTreeMap::new();
    for row_v in items.iter().skip(1) {
        let RespValue::Array(Some(row)) = row_v else {
            rig.shutdown();
            panic!("group row must be an array, got {row_v:?}");
        };
        let mut category: Option<Vec<u8>> = None;
        let mut count: Option<i64> = None;
        let mut iter = row.iter();
        while let Some(name) = iter.next() {
            let value = iter.next().expect("paired value");
            let RespValue::BulkString(Some(name_b)) = name else {
                continue;
            };
            let RespValue::BulkString(Some(value_b)) = value else {
                continue;
            };
            if name_b == b"category" {
                category = Some(value_b.clone());
            } else if name_b == b"n" {
                count = Some(
                    std::str::from_utf8(value_b)
                        .unwrap()
                        .parse::<i64>()
                        .unwrap(),
                );
            }
        }
        let cat = category.expect("category present");
        let n = count.expect("count present");
        by_cat.insert(cat, n);
    }
    assert_eq!(by_cat.get(b"alpha".as_slice()), Some(&3));
    assert_eq!(by_cat.get(b"beta".as_slice()), Some(&2));
    assert_eq!(by_cat.get(b"gamma".as_slice()), Some(&1));

    rig.shutdown();
}

#[tokio::test]
async fn ft_aggregate_groupby_sum_returns_per_group_sums() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;
    // alpha total: 10 + 20 + 30 = 60. beta total: 5 + 15 = 20.
    hset_doc(&mut sock, "docs:1", "alpha", "10", "x", 1.0).await;
    hset_doc(&mut sock, "docs:2", "alpha", "20", "y", 2.0).await;
    hset_doc(&mut sock, "docs:3", "alpha", "30", "z", 3.0).await;
    hset_doc(&mut sock, "docs:4", "beta", "5", "p", 4.0).await;
    hset_doc(&mut sock, "docs:5", "beta", "15", "q", 5.0).await;

    let cmd = encode_array(&[
        b"FT.AGGREGATE",
        b"myidx",
        b"*",
        b"GROUPBY",
        b"1",
        b"@category",
        b"REDUCE",
        b"SUM",
        b"1",
        b"@score",
        b"AS",
        b"total",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.AGGREGATE expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(2), "two groups");

    let mut by_cat: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
    for row_v in items.iter().skip(1) {
        let RespValue::Array(Some(row)) = row_v else {
            rig.shutdown();
            panic!("group row must be an array");
        };
        let mut category: Option<Vec<u8>> = None;
        let mut total: Option<f64> = None;
        let mut iter = row.iter();
        while let Some(name) = iter.next() {
            let value = iter.next().expect("paired value");
            let RespValue::BulkString(Some(name_b)) = name else {
                continue;
            };
            let RespValue::BulkString(Some(value_b)) = value else {
                continue;
            };
            if name_b == b"category" {
                category = Some(value_b.clone());
            } else if name_b == b"total" {
                total = Some(
                    std::str::from_utf8(value_b)
                        .unwrap()
                        .parse::<f64>()
                        .unwrap(),
                );
            }
        }
        by_cat.insert(category.unwrap(), total.unwrap());
    }
    let alpha = *by_cat.get(b"alpha".as_slice()).unwrap();
    let beta = *by_cat.get(b"beta".as_slice()).unwrap();
    assert!((alpha - 60.0).abs() < 1e-9, "alpha total {alpha}");
    assert!((beta - 20.0).abs() < 1e-9, "beta total {beta}");

    rig.shutdown();
}

#[tokio::test]
async fn ft_aggregate_unsupported_reducer_returns_err() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;
    hset_doc(&mut sock, "docs:1", "alpha", "10", "x", 1.0).await;

    // MAX is not in the brief's supported reducer set.
    let cmd = encode_array(&[
        b"FT.AGGREGATE",
        b"myidx",
        b"*",
        b"GROUPBY",
        b"1",
        b"@category",
        b"REDUCE",
        b"MAX",
        b"1",
        b"@score",
        b"AS",
        b"top",
    ]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Error(text) = reply else {
        rig.shutdown();
        panic!("FT.AGGREGATE MAX expected -ERR reply, got {reply:?}");
    };
    assert!(
        text.starts_with("ERR ") && text.contains("not supported"),
        "expected `-ERR not supported in this build`, got {text:?}"
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_explain_returns_query_plan() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;

    // KNN plan.
    let cmd = encode_array(&[b"FT.EXPLAIN", b"myidx", b"*=>[KNN 5 @vec $blob]"]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::BulkString(Some(plan)) = reply else {
        rig.shutdown();
        panic!("FT.EXPLAIN expected a bulk string, got {reply:?}");
    };
    let s = String::from_utf8(plan).unwrap();
    assert!(
        s.contains("VECTOR KNN"),
        "KNN plan must mention `VECTOR KNN`, got {s:?}"
    );
    assert!(s.contains("k: 5"), "KNN plan must include `k: 5`");
    assert!(s.contains("HNSW"), "KNN plan must mention HNSW algorithm");
    assert!(s.contains("L2"), "KNN plan must mention L2 metric");

    // Substring plan.
    let cmd = encode_array(&[b"FT.EXPLAIN", b"myidx", b"@body:hello"]);
    sock.write_all(&cmd).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::BulkString(Some(plan)) = reply else {
        rig.shutdown();
        panic!("FT.EXPLAIN expected a bulk string");
    };
    let s = String::from_utf8(plan).unwrap();
    assert!(
        s.contains("SUBSTRING"),
        "substring plan must mention SUBSTRING, got {s:?}"
    );
    assert!(s.contains("trigram"), "substring plan must mention trigram");

    rig.shutdown();
}

#[tokio::test]
async fn ft_alter_add_text_field_succeeds() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;

    // ALTER ADD a brand-new TEXT field that wasn't part of
    // the original schema. The response must be +OK.
    let cmd = encode_array(&[b"FT.ALTER", b"myidx", b"ADD", b"summary", b"TEXT"]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(
        reply,
        RespValue::SimpleString("OK".to_string()),
        "FT.ALTER ADD TEXT must return +OK"
    );

    // After ALTER, an HSET that writes the new field must
    // route through the trigram index, and FT.SEARCH on the
    // alter-added field must find the doc.
    let v = f32_le(&[1.0, 0.0, 0.0, 0.0]);
    let hset = encode_array(&[
        b"HSET",
        b"docs:99",
        b"category",
        b"x",
        b"score",
        b"0",
        b"body",
        b"x",
        b"summary",
        b"the quick brown fox",
        b"vec",
        &v,
    ]);
    sock.write_all(&hset).await.unwrap();
    let _ = read_resp(&mut sock, &mut buf).await;

    let q = encode_array(&[b"FT.SEARCH", b"myidx", b"@summary:fox"]);
    sock.write_all(&q).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH on alter-added field expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(1));
    let keys = search_keys(&items);
    assert_eq!(keys, vec![b"docs:99".to_vec()]);

    // Idempotency: a second ALTER ADD for the same field is
    // also +OK (no-op).
    let cmd = encode_array(&[b"FT.ALTER", b"myidx", b"ADD", b"summary", b"TEXT"]);
    sock.write_all(&cmd).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(reply, RespValue::SimpleString("OK".to_string()));

    rig.shutdown();
}

#[tokio::test]
async fn ft_alter_add_vector_field_returns_err() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_extended_index(&mut sock, "myidx").await;

    let cmd = encode_array(&[b"FT.ALTER", b"myidx", b"ADD", b"vec2", b"VECTOR"]);
    sock.write_all(&cmd).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Error(text) = reply else {
        rig.shutdown();
        panic!("FT.ALTER ADD VECTOR expected -ERR, got {reply:?}");
    };
    assert!(
        text.starts_with("ERR ") && text.contains("not supported"),
        "expected `-ERR not supported`, got {text:?}"
    );

    // DROP must also be rejected.
    let cmd = encode_array(&[b"FT.ALTER", b"myidx", b"DROP", b"category"]);
    sock.write_all(&cmd).await.unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Error(text) = reply else {
        rig.shutdown();
        panic!("FT.ALTER DROP expected -ERR");
    };
    assert!(text.starts_with("ERR "));

    rig.shutdown();
}
