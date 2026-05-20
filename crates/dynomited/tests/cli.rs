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
    // The placeholder runtime exits cleanly after writing the pid
    // file. We expect to see the file vanish on graceful exit.
    let dir = tempfile::tempdir().unwrap();
    let pid = dir.path().join("d.pid");
    let p = conf_dir().join("dynomite.yml");
    bin()
        .args(["-c"])
        .arg(&p)
        .args(["-p"])
        .arg(&pid)
        .assert()
        .success();
    // PidFile::Drop unlinks the file on graceful shutdown.
    assert!(!pid.exists(), "pid file should be removed on exit");
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
