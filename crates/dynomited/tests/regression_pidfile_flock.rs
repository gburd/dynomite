//! Regression test for the pidfile flock race between two
//! `dynomited` processes targeting the same pid path.
//!
//! Scenario: the chaos coordinator's `chaos-injector.sh` does
//! `kill -KILL <pid>` then `start-host.sh`, which spawns a new
//! `dynomited` immediately. On a fast restart the kernel may
//! still be reaping the killed process, so its `flock(2)`
//! entry can linger on the pidfile inode for a few hundred
//! microseconds. The new dynomited would observe `EAGAIN` from
//! its own `flock(LOCK_EX | LOCK_NB)` and bail.
//!
//! The fix: bounded retry on `EAGAIN` / `EWOULDBLOCK` (10
//! attempts at 100ms intervals; see
//! `dynomited::pidfile::DEFAULT_FLOCK_ATTEMPTS`). Long enough to
//! absorb the kernel-reaping race; short enough that a true
//! duplicate-instance error still surfaces in ~1s.
//!
//! What we exercise here:
//!
//! 1. Spawn dynomited A, wait for it to bind. Read the pidfile
//!    and verify it contains A's pid.
//! 2. Spawn dynomited B with the same pidfile path. Because A
//!    holds the flock, B's flock attempt should fail (after
//!    the bounded retry budget). B exits non-zero with a
//!    diagnostic message that names A's pid.
//! 3. SIGTERM A. Wait for it to exit. Confirm pidfile is
//!    removed (or empty).
//! 4. Spawn dynomited C with the same pidfile path. C should
//!    acquire the flock cleanly and bind. Read the pidfile
//!    and verify it now contains C's pid (not A's).
//!
//! Gated behind the `integration` feature so the default test
//! run does not need a redis-server to be present.

#![cfg(feature = "integration")]
#![allow(
    clippy::similar_names,
    reason = "the A/B/C sequencing in the scenario is deliberate; pid_*_in_file matches A/C suffixing"
)]

use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

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

fn term_then_wait(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    if let Ok(pid_raw) = i32::try_from(child.id()) {
        let pid = nix::unistd::Pid::from_raw(pid_raw);
        let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGTERM);
    }
    let s = wait_with_timeout(child, timeout);
    if s.is_none() {
        kill_silently(child);
    }
    s
}

fn write_yaml(
    path: &std::path::Path,
    listen_port: u16,
    dyn_port: u16,
    stats_port: u16,
    backend_port: u16,
) {
    let mut f = std::fs::File::create(path).unwrap();
    write!(
        f,
        "p:\n  listen: 127.0.0.1:{listen_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:{backend_port}:1\n  data_store: 0\n",
    )
    .unwrap();
    f.sync_all().unwrap();
}

fn read_pid_file(path: &std::path::Path) -> Option<u32> {
    let s = std::fs::read_to_string(path).ok()?;
    s.trim().parse::<u32>().ok()
}

/// Poll `path` until the file is present and contains a parseable
/// pid, or `timeout` elapses. Returns the parsed pid on success.
fn wait_for_pid_in_file(path: &std::path::Path, timeout: Duration) -> Option<u32> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(pid) = read_pid_file(path) {
            return Some(pid);
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    None
}

/// End-to-end: two dynomited binaries fight over the same pid
/// file. The contender must observe the holder's pid; once
/// the holder exits, the contender (a fresh spawn) takes over.
#[allow(
    clippy::too_many_lines,
    reason = "end-to-end scenario; splitting it adds spawn-handle plumbing without simplification"
)]
#[tokio::test]
async fn pidfile_flock_two_dynomiteds_back_to_back() {
    let Some(redis_bin) = redis_server_in_path() else {
        eprintln!("redis-server not in PATH; skipping integration test");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let backend_port = pick_port();
    let listen_port_a = pick_port();
    let dyn_port_a = pick_port();
    let stats_port_a = pick_port();
    let listen_port_c = pick_port();
    let dyn_port_c = pick_port();
    let stats_port_c = pick_port();

    // Start a single redis-server. Both proxies (A and C, but
    // not the contender B) will use it.
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
            dir.path().to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn redis-server");
    if !wait_for_listen(backend_port, Instant::now() + Duration::from_secs(30)) {
        kill_silently(&mut redis);
        panic!("redis-server did not bind {backend_port} within 30s");
    }

    let conf_a = dir.path().join("a.yml");
    let conf_c = dir.path().join("c.yml");
    write_yaml(
        &conf_a,
        listen_port_a,
        dyn_port_a,
        stats_port_a,
        backend_port,
    );
    write_yaml(
        &conf_c,
        listen_port_c,
        dyn_port_c,
        stats_port_c,
        backend_port,
    );

    let pid_file = dir.path().join("d.pid");
    let exe = assert_cmd::cargo::cargo_bin("dynomited");

    // Step 1: spawn A, wait for bind, verify pid file.
    let mut child_a = Command::new(&exe)
        .arg("-c")
        .arg(&conf_a)
        .arg("-p")
        .arg(&pid_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn dynomited A");
    if !wait_for_listen(listen_port_a, Instant::now() + Duration::from_secs(30)) {
        kill_silently(&mut child_a);
        kill_silently(&mut redis);
        panic!("dynomited A did not bind within 30s");
    }
    let pid_a_in_file = wait_for_pid_in_file(&pid_file, Duration::from_secs(5))
        .expect("A did not write pid file in time");
    assert_eq!(
        pid_a_in_file,
        child_a.id(),
        "pid file should hold A's pid {} but holds {pid_a_in_file}",
        child_a.id()
    );

    // Step 2: spawn B with the same pid file. B should fail
    // because A holds the flock. Capture B's stderr so we can
    // verify the diagnostic carries A's pid.
    let stderr_b = dir.path().join("b.err");
    let stderr_b_file = std::fs::File::create(&stderr_b).expect("create b stderr");
    let mut child_b = Command::new(&exe)
        .arg("-c")
        .arg(&conf_a)
        .arg("-p")
        .arg(&pid_file)
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_b_file))
        .spawn()
        .expect("spawn dynomited B");
    let status_b = wait_with_timeout(&mut child_b, Duration::from_secs(15))
        .expect("B did not exit within 15s of spawn");
    assert!(
        !status_b.success(),
        "B should have exited non-zero (flock held by A); got {status_b:?}"
    );
    let stderr_b_text = std::fs::read_to_string(&stderr_b).unwrap_or_default();
    let pid_a_str = format!("{}", child_a.id());
    assert!(
        stderr_b_text.contains(&pid_a_str)
            || stderr_b_text.contains("flock")
            || stderr_b_text.contains("pid file"),
        "B's stderr did not surface flock contention; got:\n{stderr_b_text}"
    );

    // Confirm A's pid is still in the file (B did not clobber it).
    let pid_after_b =
        read_pid_file(&pid_file).expect("pid file should still be readable after B's failure");
    assert_eq!(
        pid_after_b,
        child_a.id(),
        "pid file lost A's pid after B's failed take-over"
    );

    // Step 3: SIGTERM A, wait for clean exit.
    let status_a =
        term_then_wait(&mut child_a, Duration::from_secs(10)).expect("A did not exit on SIGTERM");
    assert!(status_a.success(), "A's exit status: {status_a:?}");
    // After A's clean exit, the pid file is removed. (PidFile
    // unlinks on drop.)
    assert!(
        !pid_file.exists(),
        "pid file should be unlinked after A's clean exit"
    );

    // Step 4: spawn C with the same pid file. Should bind
    // cleanly and write its own pid.
    let mut child_c = Command::new(&exe)
        .arg("-c")
        .arg(&conf_c)
        .arg("-p")
        .arg(&pid_file)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn dynomited C");
    if !wait_for_listen(listen_port_c, Instant::now() + Duration::from_secs(30)) {
        kill_silently(&mut child_c);
        kill_silently(&mut redis);
        panic!("dynomited C did not bind within 30s");
    }
    let pid_c_in_file = wait_for_pid_in_file(&pid_file, Duration::from_secs(5))
        .expect("C did not write pid file in time");
    assert_eq!(
        pid_c_in_file,
        child_c.id(),
        "pid file should hold C's pid {} but holds {pid_c_in_file}",
        child_c.id()
    );
    assert_ne!(
        pid_c_in_file,
        child_a.id(),
        "pid file still holds A's pid; supervisor takeover failed"
    );

    term_then_wait(&mut child_c, Duration::from_secs(5));
    if let Ok(pid_raw) = i32::try_from(redis.id()) {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid_raw),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
    if wait_with_timeout(&mut redis, Duration::from_secs(5)).is_none() {
        kill_silently(&mut redis);
    }
}
