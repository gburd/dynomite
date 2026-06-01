//! Wire-protocol tests for the FT.SUG* autocomplete surface.
//!
//! These tests exercise the FT.SUGADD / FT.SUGGET / FT.SUGDEL
//! / FT.SUGLEN command pipeline end-to-end: they spawn a real
//! `redis-server` (used as the storage backend, even though
//! the FT.SUG* family does not touch it on the dispatch
//! path) plus a `dynomited` instance pointed at it, then
//! drive RESP traffic through the proxy port and assert the
//! responses come back from dynomited's in-process suggestion
//! registry.
//!
//! The tests use a hand-rolled minimal RESP encoder/decoder
//! mirroring [`crate::tests::ft_wire`]. Gated on the
//! `integration` Cargo feature; without it the file
//! compiles to nothing so a `cargo build` on a host without
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
async fn ft_sugadd_then_sugget_via_wire() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    // SUGADD: dictionary "ac1" gets three suggestions.
    for (s, score) in [("apple", "1.0"), ("apricot", "5.0"), ("banana", "3.0")] {
        let cmd = encode_array(&[b"FT.SUGADD", b"ac1", s.as_bytes(), score.as_bytes()]);
        sock.write_all(&cmd).await.unwrap();
        let reply = read_resp(&mut sock, &mut buf).await;
        match reply {
            RespValue::Integer(_) => {}
            other => {
                rig.shutdown();
                panic!("FT.SUGADD expected integer reply, got {other:?}");
            }
        }
    }

    // SUGGET prefix "a" should return apricot (5.0) before
    // apple (1.0); banana is excluded by the prefix.
    sock.write_all(&encode_array(&[b"FT.SUGGET", b"ac1", b"a"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SUGGET expected array");
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0], RespValue::BulkString(Some(b"apricot".to_vec())));
    assert_eq!(items[1], RespValue::BulkString(Some(b"apple".to_vec())));

    rig.shutdown();
}

#[tokio::test]
async fn ft_sugget_fuzzy_via_wire() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    sock.write_all(&encode_array(&[b"FT.SUGADD", b"ac2", b"hello", b"1.0"]))
        .await
        .unwrap();
    let _ = read_resp(&mut sock, &mut buf).await;

    // Strict prefix on the typo "helo" misses.
    sock.write_all(&encode_array(&[b"FT.SUGGET", b"ac2", b"helo"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SUGGET expected array");
    };
    assert!(items.is_empty(), "strict-prefix typo must miss");

    // FUZZY catches the single-edit typo.
    sock.write_all(&encode_array(&[b"FT.SUGGET", b"ac2", b"helo", b"FUZZY"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SUGGET expected array");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], RespValue::BulkString(Some(b"hello".to_vec())));

    rig.shutdown();
}

#[tokio::test]
async fn ft_sugget_withpayloads_via_wire() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    sock.write_all(&encode_array(&[
        b"FT.SUGADD",
        b"ac3",
        b"alpha",
        b"1.0",
        b"PAYLOAD",
        b"alpha-meta",
    ]))
    .await
    .unwrap();
    let _ = read_resp(&mut sock, &mut buf).await;

    sock.write_all(&encode_array(&[
        b"FT.SUGGET",
        b"ac3",
        b"a",
        b"WITHPAYLOADS",
    ]))
    .await
    .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    let RespValue::Array(Some(items)) = reply else {
        rig.shutdown();
        panic!("FT.SUGGET expected array");
    };
    // 1 hit * (value + payload) = 2 elements.
    assert_eq!(items.len(), 2);
    assert_eq!(items[0], RespValue::BulkString(Some(b"alpha".to_vec())));
    assert_eq!(
        items[1],
        RespValue::BulkString(Some(b"alpha-meta".to_vec()))
    );

    rig.shutdown();
}

#[tokio::test]
async fn ft_sugdel_then_suglen_via_wire() {
    let rig = rig_or_skip!();
    let mut sock = rig.connect().await;
    let mut buf: Vec<u8> = Vec::new();

    for s in ["alpha", "beta", "gamma"] {
        sock.write_all(&encode_array(&[b"FT.SUGADD", b"ac4", s.as_bytes(), b"1.0"]))
            .await
            .unwrap();
        let _ = read_resp(&mut sock, &mut buf).await;
    }

    sock.write_all(&encode_array(&[b"FT.SUGLEN", b"ac4"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(reply, RespValue::Integer(3));

    sock.write_all(&encode_array(&[b"FT.SUGDEL", b"ac4", b"beta"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(reply, RespValue::Integer(1), "delete present -> 1");

    sock.write_all(&encode_array(&[b"FT.SUGDEL", b"ac4", b"beta"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(reply, RespValue::Integer(0), "delete absent -> 0");

    sock.write_all(&encode_array(&[b"FT.SUGLEN", b"ac4"]))
        .await
        .unwrap();
    let reply = read_resp(&mut sock, &mut buf).await;
    assert_eq!(reply, RespValue::Integer(2));

    rig.shutdown();
}
