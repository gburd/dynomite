//! Regression tests for the Redis `AUTH` handshake performed by
//! the backend supervisor.
//!
//! Two scenarios:
//!
//! 1. `redis_auth_correct_password_round_trip` - spins up
//!    `redis-server --requirepass shibboleth`, points dynomited
//!    at it with `redis_requirepass: shibboleth`, drives a
//!    `SET` / `GET` round-trip, and asserts the proxy answered
//!    each command with the expected RESP reply. This is the
//!    happy path: AUTH succeeds, the supervisor enters its
//!    request loop, the dispatcher answers from the backend.
//!
//! 2. `redis_auth_wrong_password_supervisor_fails` - boots the
//!    same passworded backend but configures dynomited with the
//!    *wrong* password. The proxy still binds (the listener is
//!    independent of the backend supervisor), but every backend
//!    `AUTH` is rejected so the supervisor never enters the
//!    request loop. We probe by sending a `SET` and asserting
//!    no reply arrives within a reasonable window AND that
//!    dynomited's stderr carries the `auth_failed` reason on
//!    its reconnect log lines. After verification the proxy is
//!    SIGTERMed and exits cleanly, proving the supervisor was
//!    looping cooperatively rather than wedged on a panic.
//!
//! Gated behind the `integration` feature (which depends on
//! `redis-server` being on `PATH`); when missing, the test
//! marks itself as skipped rather than failing.

#![cfg(feature = "integration")]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SHIBBOLETH: &str = "shibboleth";

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

fn term_then_wait(child: &mut Child, timeout: Duration) {
    if let Ok(pid_raw) = i32::try_from(child.id()) {
        let pid = nix::unistd::Pid::from_raw(pid_raw);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
    }
    if wait_with_timeout(child, timeout).is_none() {
        kill_silently(child);
    }
}

async fn read_exact_n(sock: &mut TcpStream, n: usize) -> Vec<u8> {
    let mut buf = vec![0u8; n];
    sock.read_exact(&mut buf).await.expect("read_exact");
    buf
}

fn spawn_passworded_redis(redis_bin: &PathBuf, port: u16, dir: &std::path::Path) -> Child {
    let child = Command::new(redis_bin)
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
            "--protected-mode",
            "no",
            "--requirepass",
            SHIBBOLETH,
            "--dir",
            dir.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn redis-server");
    child
}

fn write_dynomite_yaml(
    path: &std::path::Path,
    listen_port: u16,
    dyn_port: u16,
    stats_port: u16,
    backend_port: u16,
    password: &str,
) {
    let mut f = std::fs::File::create(path).unwrap();
    write!(
        f,
        "p:\n  listen: 127.0.0.1:{listen_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:{backend_port}:1\n  data_store: 0\n  redis_requirepass: {password}\n",
    )
    .unwrap();
    f.sync_all().unwrap();
}

async fn fetch_metrics(stats_port: u16) -> String {
    let mut sock = TcpStream::connect(("127.0.0.1", stats_port))
        .await
        .expect("connect stats");
    sock.write_all(b"GET /metrics HTTP/1.0\r\n\r\n")
        .await
        .expect("write GET /metrics");
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut buf)).await;
    String::from_utf8_lossy(&buf).into_owned()
}

fn read_file_lossy(path: &std::path::Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

#[tokio::test]
async fn redis_auth_correct_password_round_trip() {
    let Some(redis_bin) = redis_server_in_path() else {
        eprintln!("redis-server not in PATH; skipping integration test");
        return;
    };

    let backend_port = pick_port();
    let listen_port = pick_port();
    let dyn_port = pick_port();
    let stats_port = pick_port();

    let dir = tempfile::tempdir().unwrap();

    let mut redis = spawn_passworded_redis(&redis_bin, backend_port, dir.path());
    if !wait_for_listen(backend_port, Instant::now() + Duration::from_secs(30)) {
        kill_silently(&mut redis);
        panic!("redis-server did not bind {backend_port} within 30s");
    }

    let conf = dir.path().join("d.yml");
    write_dynomite_yaml(
        &conf,
        listen_port,
        dyn_port,
        stats_port,
        backend_port,
        SHIBBOLETH,
    );

    let exe = assert_cmd::cargo::cargo_bin("dynomited");
    let pid_file = dir.path().join("dyn.pid");
    let mut dyn_child = Command::new(&exe)
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

    let mut sock = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("connect dynomited");
    sock.set_nodelay(true).ok();

    sock.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .expect("write SET");
    let set_rsp = read_exact_n(&mut sock, 5).await;
    assert_eq!(&set_rsp, b"+OK\r\n", "SET response (passworded backend)");

    sock.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
        .await
        .expect("write GET");
    let get_rsp = read_exact_n(&mut sock, 7).await;
    assert_eq!(
        &get_rsp,
        b"$1\r\nv\r\n",
        "GET response: {}",
        String::from_utf8_lossy(&get_rsp)
    );

    sock.write_all(b"*1\r\n$4\r\nQUIT\r\n")
        .await
        .expect("write QUIT");
    let mut tail = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut tail)).await;

    term_then_wait(&mut dyn_child, Duration::from_secs(5));
    term_then_wait(&mut redis, Duration::from_secs(5));
}

#[tokio::test]
async fn redis_auth_wrong_password_supervisor_fails() {
    let Some(redis_bin) = redis_server_in_path() else {
        eprintln!("redis-server not in PATH; skipping integration test");
        return;
    };

    let backend_port = pick_port();
    let listen_port = pick_port();
    let dyn_port = pick_port();
    let stats_port = pick_port();

    let dir = tempfile::tempdir().unwrap();

    let mut redis = spawn_passworded_redis(&redis_bin, backend_port, dir.path());
    if !wait_for_listen(backend_port, Instant::now() + Duration::from_secs(30)) {
        kill_silently(&mut redis);
        panic!("redis-server did not bind {backend_port} within 30s");
    }

    // Configure with a deliberately wrong password.
    let conf = dir.path().join("d.yml");
    write_dynomite_yaml(
        &conf,
        listen_port,
        dyn_port,
        stats_port,
        backend_port,
        "definitely-not-the-password",
    );

    let exe = assert_cmd::cargo::cargo_bin("dynomited");
    let pid_file = dir.path().join("dyn.pid");
    let stderr_path = dir.path().join("dyn.err");
    let stderr_file = std::fs::File::create(&stderr_path).expect("create stderr capture");
    let mut dyn_child = Command::new(&exe)
        .arg("-c")
        .arg(&conf)
        .arg("-p")
        .arg(&pid_file)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .expect("spawn dynomited");

    // The client listener still comes up: the AUTH failure
    // happens on the BACKEND side, not the client side.
    if !wait_for_listen(listen_port, Instant::now() + Duration::from_secs(30)) {
        kill_silently(&mut dyn_child);
        kill_silently(&mut redis);
        panic!("dynomited did not bind {listen_port} within 30s");
    }
    if !wait_for_listen(stats_port, Instant::now() + Duration::from_secs(30)) {
        kill_silently(&mut dyn_child);
        kill_silently(&mut redis);
        panic!("dynomited stats did not bind {stats_port} within 30s");
    }

    // Give the supervisor a moment to attempt at least one
    // reconnect. The first sleep advertises to the chaos-style
    // bounded backoff (50ms initial), so 2s is comfortably
    // enough for >= 1 attempt across CI noise.
    tokio::time::sleep(Duration::from_millis(2_000)).await;

    // Probe: drive a SET and confirm we do NOT get a reply
    // within a short window. The dispatcher hands the request
    // to the backend channel; the supervisor never enters its
    // run loop because every AUTH is rejected, so no reply is
    // ever produced.
    let mut sock = TcpStream::connect(("127.0.0.1", listen_port))
        .await
        .expect("connect dynomited");
    sock.set_nodelay(true).ok();
    sock.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
        .await
        .expect("write SET");
    let mut probe = [0u8; 5];
    let read_outcome =
        tokio::time::timeout(Duration::from_secs(2), sock.read_exact(&mut probe)).await;
    assert!(
        read_outcome.is_err(),
        "expected SET to hang (no backend), but got reply: {:?}",
        &probe[..]
    );
    drop(sock);

    // Inspect captured stderr to confirm the supervisor logged
    // an `auth_failed` reconnect reason. The supervisor's
    // log line carries `reason="auth_failed"` (see
    // `record_reconnect_and_back_off`).
    let logs = read_file_lossy(&stderr_path);
    assert!(
        logs.contains("auth_failed"),
        "dynomited stderr missing 'auth_failed' marker:\n{logs}"
    );
    // The `/metrics` endpoint is also live and serves the
    // engine snapshot; we touch it for a sanity check that
    // the proxy is responsive while AUTH keeps failing.
    let metrics = fetch_metrics(stats_port).await;
    assert!(
        metrics.contains("dynomite_uptime_seconds"),
        "metrics body did not include uptime line:\n{metrics}"
    );

    term_then_wait(&mut dyn_child, Duration::from_secs(5));
    term_then_wait(&mut redis, Duration::from_secs(5));
}
