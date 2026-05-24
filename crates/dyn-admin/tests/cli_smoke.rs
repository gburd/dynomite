//! Smoke tests for the `dyn-admin` CLI surface that do not need a
//! live listener: `--help`, `--version`, missing subcommand, and
//! the `--json` flag's effect on output shape for offline-friendly
//! commands.

use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn version_flag_prints_crate_version() {
    Command::cargo_bin("dyn-admin")
        .unwrap()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn help_lists_every_v0_subcommand() {
    let assertion = Command::cargo_bin("dyn-admin")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
    let out = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    for sub in [
        "status",
        "ring-status",
        "stats",
        "metrics",
        "ping",
        "cluster-list",
    ] {
        assert!(out.contains(sub), "help is missing {sub}: {out}");
    }
}

#[test]
fn deferred_subcommands_are_absent_not_stubbed() {
    // riak-admin's mutating commands intentionally do not appear in
    // v0. The brief documents these as deferred follow-ups; this
    // test pins the "absent, not stubbed" guarantee.
    let assertion = Command::cargo_bin("dyn-admin")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
    let out = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    for deferred in [
        "cluster-join",
        "cluster-leave",
        "cluster-plan",
        "cluster-commit",
    ] {
        assert!(
            !out.contains(deferred),
            "deferred subcommand {deferred} appears in help: {out}"
        );
    }
    // Invoking a deferred name explicitly must error out (clap rejects
    // unknown subcommands) rather than landing in some stub branch.
    Command::cargo_bin("dyn-admin")
        .unwrap()
        .arg("cluster-join")
        .assert()
        .failure();
}

#[test]
fn missing_subcommand_prints_usage() {
    Command::cargo_bin("dyn-admin")
        .unwrap()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Usage"));
}
