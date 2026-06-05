//! Wire-protocol tests for the dyntext-backed text-search
//! surface (Phase D of the dynvec / dyntext fold).
//!
//! These tests exercise the trigram + bloom-filter index path
//! end to end: they spawn a real `valkey-server` (used as the
//! storage backend for HSET writes) plus a `dynomited`
//! instance pointed at it, then drive RESP traffic through
//! the proxy port and assert the responses come back from
//! dynomited's in-process vector + text index registry. The
//! pattern mirrors `crates/dynomited/tests/ft_wire.rs`; only
//! the assertions differ.
//!
//! The tests exercise:
//!
//! * `FT.CREATE ... SCHEMA <text-field> TEXT
//!     <vec-field> VECTOR HNSW ...` provisions a per-text-field
//!   trigram index alongside the vector engine.
//! * `HSET <key> <text-field> <text> <vec-field> <bytes>`
//!   pushes the bytes into both the dynvec engine and the
//!   trigram index.
//! * `FT.SEARCH <idx> "@<text-field>:<substring>"` returns
//!   the keys whose text field contains the substring.
//! * `FT.REGEX <idx> <text-field> <pattern> [K=<n>]` returns
//!   the keys whose text field matches the regex (exactly
//!   when `K=0` / omitted; approximately when `K>=1`).

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
        let candidate = entry.join("valkey-server");
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

/// Extract the key strings from an FT.SEARCH-shaped reply.
/// Layout: `[total, key_1, [field, value, ...], key_2, ...]`.
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

/// Extract the per-key field arrays from an FT.SEARCH-shaped
/// reply. Returns one [`Vec<(field, value)>`] per result.
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
            .expect("spawn valkey-server");

        if !wait_for_listen(backend_port, Instant::now() + Duration::from_secs(30)) {
            kill_silently(&mut redis);
            panic!("valkey-server did not bind {backend_port} within 30s");
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
                eprintln!("valkey-server not in PATH; skipping wire test");
                return;
            }
        }
    };
}

/// Build the `FT.CREATE myidx ON HASH PREFIX 1 docs: SCHEMA
/// body TEXT vec VECTOR HNSW ...` argument vector. The same
/// shape is used by every test in this file: the `body` TEXT
/// field is what the dyntext layer indexes; the `vec` VECTOR
/// field is the standard 4-dim COSINE HNSW field that the
/// dynvec engine requires today.
fn ft_create_args(idx: &[u8]) -> Vec<&'static [u8]> {
    let _ = idx;
    vec![
        b"FT.CREATE",
        b"PLACEHOLDER",
        b"ON",
        b"HASH",
        b"PREFIX",
        b"1",
        b"docs:",
        b"SCHEMA",
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
        b"COSINE",
    ]
}

/// Dispatch `FT.CREATE` for a fresh `myidx` with the
/// schema shared by every test in this file.
async fn create_index(sock: &mut TcpStream, idx: &str) {
    let mut parts = ft_create_args(idx.as_bytes());
    let idx_bytes = idx.as_bytes();
    parts[1] = idx_bytes;
    let cmd = encode_array(&parts);
    sock.write_all(&cmd).await.expect("write FT.CREATE");
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(sock, &mut buf).await;
    assert_eq!(
        reply,
        RespValue::SimpleString("OK".to_string()),
        "FT.CREATE reply"
    );
}

/// Issue an HSET that targets the `docs:` prefix with the
/// shared `body` + `vec` schema. The vector payload is a
/// throwaway `[1, 0, 0, 0]` for the read-path tests in this
/// file; the trigram index is keyed off the `body` text.
async fn hset(sock: &mut TcpStream, key: &str, body: &str) {
    let vec_bytes = f32_le(&[1.0, 0.0, 0.0, 0.0]);
    let cmd = encode_array(&[
        b"HSET",
        key.as_bytes(),
        b"body",
        body.as_bytes(),
        b"vec",
        &vec_bytes,
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
async fn ft_create_with_text_field_registers_text_index() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_index(&mut sock, "myidx").await;

    // Round-trip an FT.INFO that asserts the schema reflects
    // the TEXT field. The wire surface emits the per-field
    // schema under `schema_fields`; we walk it for `body`.
    let info = encode_array(&[b"FT.INFO", b"myidx"]);
    sock.write_all(&info).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.INFO expected an array");
    };
    let mut iter = items.iter();
    let mut found = false;
    while let Some(name) = iter.next() {
        let value = iter.next().expect("paired value");
        if matches!(name, RespValue::BulkString(Some(b)) if b == b"schema_fields") {
            let RespValue::Array(Some(fields)) = value else {
                rig.shutdown();
                panic!("schema_fields must be an array");
            };
            for field in fields {
                let RespValue::Array(Some(pair)) = field else {
                    continue;
                };
                if pair.len() == 2
                    && matches!(&pair[0], RespValue::BulkString(Some(b)) if b == b"body")
                    && matches!(&pair[1], RespValue::BulkString(Some(b)) if b == b"TEXT")
                {
                    found = true;
                }
            }
        }
    }
    assert!(found, "FT.INFO must list `body` as a TEXT-typed field");

    rig.shutdown();
}

#[tokio::test]
async fn hset_with_text_field_inserts_into_text_index() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_index(&mut sock, "myidx").await;

    hset(&mut sock, "docs:1", "errno: connection refused").await;
    hset(&mut sock, "docs:2", "errno: no route to host").await;
    hset(&mut sock, "docs:3", "all systems nominal").await;

    // The trigram index is invisible from the wire; we infer
    // its existence by issuing a substring query that only
    // the trigram path can answer for a corpus of three docs.
    let q = encode_array(&[b"FT.SEARCH", b"myidx", b"@body:errno"]);
    sock.write_all(&q).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(2), "two errno docs match");
    let keys = search_keys(&items);
    assert!(keys.contains(&b"docs:1".to_vec()));
    assert!(keys.contains(&b"docs:2".to_vec()));
    assert!(!keys.contains(&b"docs:3".to_vec()));

    rig.shutdown();
}

#[tokio::test]
async fn ft_search_at_field_substring_returns_matching_keys() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_index(&mut sock, "myidx").await;

    hset(&mut sock, "docs:1", "errno: connection refused").await;
    hset(&mut sock, "docs:2", "errno: no route to host").await;
    hset(&mut sock, "docs:3", "the quick brown fox").await;

    // Substring "refused" is unique to docs:1.
    let q = encode_array(&[b"FT.SEARCH", b"myidx", b"@body:refused"]);
    sock.write_all(&q).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(1));
    let keys = search_keys(&items);
    assert_eq!(keys, vec![b"docs:1".to_vec()]);

    rig.shutdown();
}

#[tokio::test]
async fn ft_search_returns_text_field_in_response() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_index(&mut sock, "myidx").await;

    hset(&mut sock, "docs:1", "errno: connection refused").await;
    hset(&mut sock, "docs:2", "errno: no route to host").await;

    // The brief shows the matched body text returned alongside
    // the doc key:
    //   docs:1 -> [body, "errno: connection refused"]
    //   docs:2 -> [body, "errno: no route to host"]
    let q = encode_array(&[b"FT.SEARCH", b"myidx", b"@body:errno"]);
    sock.write_all(&q).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SEARCH expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(2));

    let keys = search_keys(&items);
    let fields = search_fields(&items);
    assert_eq!(keys.len(), fields.len());
    assert_eq!(keys.len(), 2);

    let mut by_key: std::collections::BTreeMap<Vec<u8>, Vec<u8>> =
        std::collections::BTreeMap::new();
    for (key, row) in keys.iter().zip(fields.iter()) {
        let body = row
            .iter()
            .find(|(name, _)| name == b"body")
            .map(|(_, v)| v.clone())
            .expect("body field present");
        by_key.insert(key.clone(), body);
    }
    assert_eq!(
        by_key.get(b"docs:1".as_slice()).map(Vec::as_slice),
        Some(b"errno: connection refused".as_slice()),
    );
    assert_eq!(
        by_key.get(b"docs:2".as_slice()).map(Vec::as_slice),
        Some(b"errno: no route to host".as_slice()),
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_regex_k0_exact_match() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_index(&mut sock, "myidx").await;

    hset(&mut sock, "docs:1", "errno: connection refused").await;
    hset(&mut sock, "docs:2", "errno: no route to host").await;

    // Default K=0 -> exact regex.
    let q = encode_array(&[
        b"FT.REGEX",
        b"myidx",
        b"body",
        b"^errno: \\w+ refused$",
        b"K=0",
    ]);
    sock.write_all(&q).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.REGEX expected an array");
    };
    assert_eq!(items[0], RespValue::Integer(1));
    let keys = search_keys(&items);
    assert_eq!(keys, vec![b"docs:1".to_vec()]);
    let fields = search_fields(&items);
    let body = fields[0]
        .iter()
        .find(|(name, _)| name == b"body")
        .map(|(_, v)| v.clone())
        .expect("body field");
    assert_eq!(body, b"errno: connection refused");

    rig.shutdown();
}

#[tokio::test]
async fn ft_regex_k1_one_typo_tolerated() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_index(&mut sock, "myidx").await;

    hset(&mut sock, "docs:1", "errno: connection refused").await;
    hset(&mut sock, "docs:2", "errno: no route to host").await;

    // "refsed" is one transposition away from "refused".
    let q = encode_array(&[
        b"FT.REGEX",
        b"myidx",
        b"body",
        b"errno: connection refsed",
        b"K=1",
    ]);
    sock.write_all(&q).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.REGEX expected an array");
    };
    assert!(matches!(items[0], RespValue::Integer(n) if n >= 1));
    let keys = search_keys(&items);
    assert!(
        keys.contains(&b"docs:1".to_vec()),
        "K=1 must tolerate a single-edit typo and still match docs:1"
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_regex_k2_two_typos_tolerated() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    create_index(&mut sock, "myidx").await;

    hset(&mut sock, "docs:1", "errno: connection refused").await;
    hset(&mut sock, "docs:2", "errno: no route to host").await;

    // "rfusd" requires two edits to reach "refused".
    let q = encode_array(&[
        b"FT.REGEX",
        b"myidx",
        b"body",
        b"errno: connection rfusd",
        b"K=2",
    ]);
    sock.write_all(&q).await.unwrap();
    let mut buf: Vec<u8> = Vec::new();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.REGEX expected an array");
    };
    assert!(matches!(items[0], RespValue::Integer(n) if n >= 1));
    let keys = search_keys(&items);
    assert!(
        keys.contains(&b"docs:1".to_vec()),
        "K=2 must tolerate two edits and still match docs:1"
    );

    rig.shutdown();
}
