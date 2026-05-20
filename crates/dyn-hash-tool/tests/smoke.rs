//! Smoke tests for the `dyn-hash-tool` binary.
//!
//! These exercise the CLI end-to-end by spawning the compiled binary
//! and comparing its stdout to a small set of fixed expectations.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn list_prints_all_algorithms() {
    let output = Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .arg("--list")
        .assert()
        .success();
    let out = String::from_utf8(output.get_output().stdout.clone()).unwrap();
    for name in [
        "one_at_a_time",
        "md5",
        "crc16",
        "crc32",
        "crc32a",
        "fnv1_64",
        "fnv1a_64",
        "fnv1_32",
        "fnv1a_32",
        "hsieh",
        "murmur",
        "jenkins",
        "murmur3",
    ] {
        assert!(out.contains(name), "output is missing {name}: {out}");
    }
}

#[test]
fn hashes_a_single_key() {
    Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["-H", "md5", "--key", "abc"])
        .assert()
        .success()
        .stdout(predicate::str::starts_with("md5:abc:"))
        .stdout(predicate::str::contains("98500190"));
}

#[test]
fn rejects_unknown_algorithm() {
    Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["-H", "no-such-algo", "--key", "abc"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown hash algorithm"));
}

#[test]
fn rejects_no_keys() {
    Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["-H", "md5"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("at least one"));
}

#[test]
fn hashes_multiple_keys_in_order() {
    let assertion = Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args([
            "-H", "crc32a", "--key", "abc", "--key", "xyz", "--key", "hello",
        ])
        .assert()
        .success();
    let out = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3);
    assert!(lines[0].starts_with("crc32a:abc:"));
    assert!(lines[1].starts_with("crc32a:xyz:"));
    assert!(lines[2].starts_with("crc32a:hello:"));
}

#[test]
fn murmur3_emits_four_words() {
    let assertion = Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["-H", "murmur3", "--key", "hello"])
        .assert()
        .success();
    let out = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let line = out.lines().next().unwrap();
    let token_hex = line.rsplit(':').next().unwrap();
    assert_eq!(token_hex.len(), 32, "murmur3 token must be 4*8 hex chars");
}
