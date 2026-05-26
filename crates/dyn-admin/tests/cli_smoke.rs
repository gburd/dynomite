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
        "cluster-join",
        "cluster-leave",
        "cluster-plan",
        "cluster-commit",
        "aae-status",
    ] {
        assert!(out.contains(sub), "help is missing {sub}: {out}");
    }
}

#[test]
fn cluster_mutating_subcommands_are_present() {
    // The cluster-mutation subcommands are wired in the v0.0.4 admin
    // slice; they live under the same `cluster-*` family and the
    // help text must announce every one. Invoking each without a
    // listening server fails because TCP connect is refused, but
    // the failure goes through clap's parser successfully (no
    // `unknown subcommand` error).
    for sub in [
        "cluster-join",
        "cluster-leave",
        "cluster-plan",
        "cluster-commit",
    ] {
        let mut cmd = Command::cargo_bin("dyn-admin").unwrap();
        let extra: &[&str] = match sub {
            "cluster-join" => &["127.0.0.1:1"],
            "cluster-leave" => &["99"],
            _ => &[],
        };
        cmd.arg(sub).args(extra).arg("--node").arg("127.0.0.1:1");
        cmd.assert()
            .failure()
            .stderr(predicate::str::contains("dyn-admin:"));
    }
}

#[test]
fn missing_subcommand_prints_usage() {
    Command::cargo_bin("dyn-admin")
        .unwrap()
        .assert()
        .failure()
        .stderr(predicate::str::contains("Usage"));
}
