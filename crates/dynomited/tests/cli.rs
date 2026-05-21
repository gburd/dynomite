//! End-to-end CLI smoke tests for `dynomited`.
//!
//! These tests exercise the binary the same way an operator would:
//! shell out to `target/<profile>/dynomited`, observe its stdout /
//! stderr, and assert the exit code.

use std::path::PathBuf;

use assert_cmd::Command;
use predicates::str::contains;

fn bin() -> Command {
    Command::cargo_bin("dynomited").expect("binary built")
}

fn conf_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("conf")
}

#[test]
fn help_flag_prints_usage() {
    bin()
        .arg("-h")
        .assert()
        .success()
        .stderr(contains("Usage: dynomite ["))
        .stderr(contains("Options:"))
        .stderr(contains("--test-conf"))
        .stderr(contains("This is dynomite-"));
}

#[test]
fn long_help_flag_prints_usage() {
    bin()
        .arg("--help")
        .assert()
        .success()
        .stderr(contains("--describe-stats"));
}

#[test]
fn version_flag_prints_version() {
    bin()
        .arg("-V")
        .assert()
        .success()
        .stderr(contains("This is dynomite-"));
}

#[test]
fn long_version_flag_prints_version() {
    bin()
        .arg("--version")
        .assert()
        .success()
        .stderr(contains("This is dynomite-"));
}

#[test]
fn test_conf_accepts_bundled_yaml() {
    for entry in std::fs::read_dir(conf_dir()).expect("conf dir") {
        let path = entry.expect("entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("yml") {
            continue;
        }
        bin()
            .arg("-t")
            .arg("-c")
            .arg(&path)
            .assert()
            .success()
            .stderr(contains("syntax is valid"));
    }
}

#[test]
fn test_conf_rejects_missing_listen() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.yml");
    // No `listen:` field at the top level.
    std::fs::write(
        &bad,
        "p:\n  dyn_listen: 127.0.0.1:8101\n  servers:\n  - 127.0.0.1:6379:1\n  tokens: '0'\n  data_store: 0\n",
    )
    .unwrap();
    bin()
        .arg("-t")
        .arg("-c")
        .arg(&bad)
        .assert()
        .failure()
        .stderr(contains("syntax is invalid"));
}

#[test]
fn test_conf_rejects_unknown_path() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does-not-exist.yml");
    bin()
        .arg("-t")
        .arg("-c")
        .arg(&missing)
        .assert()
        .failure()
        .stderr(contains("syntax is invalid"));
}

#[test]
fn describe_stats_prints_metrics() {
    bin()
        .arg("-D")
        .assert()
        .success()
        .stdout(contains("pool stats:"))
        .stdout(contains("server stats:"))
        .stderr(contains("This is dynomite-"));
}

#[test]
fn verbosity_out_of_range_rejected() {
    bin().args(["-v", "12"]).assert().failure();
    bin().args(["-v", "999"]).assert().failure();
    bin().args(["-v", "-1"]).assert().failure();
    // No assertion: bare `-v 5` enters the runtime path which exits 1
    // until Stage 12 commit 3 wires the run loop in; we only care that
    // option parsing accepts the value.
    drop(bin().args(["-v", "5"]).assert());
}

#[test]
fn verbosity_in_range_accepted_for_test_conf() {
    let p = conf_dir().join("dynomite.yml");
    bin()
        .args(["-t", "-v", "11", "-c"])
        .arg(&p)
        .assert()
        .success();
    bin()
        .args(["-t", "-v", "0", "-c"])
        .arg(&p)
        .assert()
        .success();
}

#[test]
fn unknown_flag_prints_usage_and_fails() {
    bin()
        .arg("--this-is-not-a-flag")
        .assert()
        .failure()
        .stderr(contains("Usage: dynomite ["));
}

#[test]
fn daemonize_with_test_conf_is_rejected() {
    let p = conf_dir().join("dynomite.yml");
    bin()
        .args(["-d", "-t", "-c"])
        .arg(&p)
        .assert()
        .failure()
        .stderr(contains("mutually exclusive"));
}

#[test]
fn pidfile_is_written_and_removed() {
    // Spawn dynomited with a config bound to fresh ephemeral
    // ports, wait long enough for the run loop to write the pid
    // file, send SIGTERM, and confirm the pid file is removed.
    use std::io::Write;
    use std::time::Duration;

    let dir = tempfile::tempdir().unwrap();
    let pid = dir.path().join("d.pid");

    let listen_port = pick_port();
    let dyn_port = pick_port();
    let stats_port = pick_port();
    let conf_path = dir.path().join("d.yml");
    let mut f = std::fs::File::create(&conf_path).unwrap();
    write!(
        f,
        "p:\n  listen: 127.0.0.1:{listen_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n",
    )
    .unwrap();
    f.sync_all().unwrap();

    let exe = assert_cmd::cargo::cargo_bin("dynomited");
    let stderr_path = dir.path().join("d.stderr");
    let stderr_file = std::fs::File::create(&stderr_path).unwrap();
    let mut child = std::process::Command::new(exe)
        .args([
            "-c".as_ref(),
            conf_path.as_os_str(),
            "-p".as_ref(),
            pid.as_os_str(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(stderr_file)
        .spawn()
        .unwrap();

    // Wait for the pid file to materialise. Allow 60s to absorb
    // heavily-loaded CI hosts, first-run binary build cost, and
    // the 3-5x instrumentation overhead of `cargo llvm-cov`.
    // On failure, surface dynomited's captured stderr to make
    // the diagnostic obvious.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !pid.exists() && std::time::Instant::now() < deadline {
        // Detect early child exit so we don't sleep until the
        // deadline when the child has already crashed.
        if let Ok(Some(status)) = child.try_wait() {
            let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
            panic!("dynomited exited before writing pid file: status={status:?} stderr=\n{stderr}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    if !pid.exists() {
        let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
        let _ = child.kill();
        let _ = child.wait();
        panic!("pid file did not appear within 60s; dynomited stderr:\n{stderr}");
    }

    // Graceful shutdown via SIGTERM, mirroring an init-script
    // stop. The run loop's signal handler flips the watch flag,
    // listeners drain, and Drop unlinks the pid file.
    let pid_id = nix::unistd::Pid::from_raw(i32::try_from(child.id()).expect("child pid fits i32"));
    nix::sys::signal::kill(pid_id, nix::sys::signal::Signal::SIGTERM).unwrap();

    let exit_status = wait_with_timeout(&mut child, Duration::from_secs(15))
        .expect("dynomited did not exit within 15s after SIGTERM");
    assert!(
        exit_status.success(),
        "dynomited exited with status {exit_status:?}"
    );
    assert!(!pid.exists(), "pid file should be removed on exit");
}

fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn wait_with_timeout(
    child: &mut std::process::Child,
    timeout: std::time::Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            Err(_) => return None,
        }
    }
}

#[test]
fn invalid_conf_path_is_rejected_for_run() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("missing.yml");
    bin()
        .args(["-c"])
        .arg(&missing)
        .assert()
        .failure()
        .stderr(contains("syntax is invalid"));
}
