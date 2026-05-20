//! C-compatibility tests for `dyn-hash-tool --c-compat`.
//!
//! These exercise the output format produced by the legacy C
//! `dyn_hash_tool`: one decimal token per line, optionally preceded by
//! a `KEY:<key>` line when `-k` is set. Input keys are read one per
//! line from stdin or from `-i <file>`.

use std::io::Write;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::NamedTempFile;

fn expected_token(key: &str) -> u64 {
    use dynomite::hashkit::{hash, HashType};
    u64::from(hash(HashType::Murmur, key.as_bytes()).get_int())
}

#[test]
fn c_compat_stdin_to_stdout_no_key_prefix() {
    let assertion = Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["--c-compat", "-H", "murmur"])
        .write_stdin("hello\nworld\n")
        .assert()
        .success();
    let out = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 2, "got: {out:?}");
    assert_eq!(lines[0], expected_token("hello").to_string());
    assert_eq!(lines[1], expected_token("world").to_string());
}

#[test]
fn c_compat_with_dash_k_prefixes_each_key() {
    let assertion = Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["--c-compat", "-k"])
        .write_stdin("alpha\nbeta\n")
        .assert()
        .success();
    let out = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 4, "got: {out:?}");
    assert_eq!(lines[0], "KEY:alpha");
    assert_eq!(lines[1], expected_token("alpha").to_string());
    assert_eq!(lines[2], "KEY:beta");
    assert_eq!(lines[3], expected_token("beta").to_string());
}

#[test]
fn c_compat_input_and_output_files() {
    let mut input = NamedTempFile::new().unwrap();
    writeln!(input, "foo").unwrap();
    writeln!(input, "bar").unwrap();
    writeln!(input, "baz").unwrap();
    let output = NamedTempFile::new().unwrap();

    Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["--c-compat", "-H", "murmur", "-i"])
        .arg(input.path())
        .arg("-o")
        .arg(output.path())
        .assert()
        .success();

    let body = std::fs::read_to_string(output.path()).unwrap();
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 3, "got: {body:?}");
    assert_eq!(lines[0], expected_token("foo").to_string());
    assert_eq!(lines[1], expected_token("bar").to_string());
    assert_eq!(lines[2], expected_token("baz").to_string());
}

#[test]
fn c_compat_rejects_non_murmur_algorithm() {
    Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["--c-compat", "-H", "md5"])
        .write_stdin("hello\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains("only supports the murmur"));
}

#[test]
fn c_compat_dash_dash_input_is_stdin() {
    let assertion = Command::cargo_bin("dyn-hash-tool")
        .unwrap()
        .args(["--c-compat", "-H", "murmur", "-i", "-"])
        .write_stdin("ping\n")
        .assert()
        .success();
    let out = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    assert_eq!(out.trim_end(), expected_token("ping").to_string());
}
