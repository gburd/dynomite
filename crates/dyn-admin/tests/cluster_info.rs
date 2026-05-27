//! End-to-end smoke test for `dyn-admin cluster-info`.
//!
//! Stand up a [`dynomite::stats::StatsServer`] with a synthetic
//! [`ClusterInfoSnapshot`] provider, then drive a real
//! `dyn-admin cluster-info` invocation via [`assert_cmd`] against
//! the listener and assert that the binary exits with status 0
//! and emits every required section header.

use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use parking_lot::Mutex;
use predicates::prelude::*;

use dynomite::admin::cluster_info::ClusterInfoSnapshot;
use dynomite::stats::{Snapshot, StatsServer};

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio multi-thread runtime")
}

#[test]
fn cluster_info_round_trip_against_stats_server() {
    let runtime = make_runtime();

    let (addr, server_task) = runtime.block_on(async {
        let sink = Arc::new(Mutex::new(Snapshot::default()));
        let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)
            .await
            .expect("bind")
            .with_cluster_info_provider(Arc::new(ClusterInfoSnapshot::synthetic));
        let addr = server.local_addr().expect("local_addr");
        let task = tokio::spawn(async move {
            let _ = server.run().await;
        });
        // Yield so the listener is parked on accept() before the
        // CLI tries to connect from a separate process.
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
    });

    let node = format!("127.0.0.1:{}", addr.port());

    let assertion = Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["cluster-info", "--node", &node])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    for header in [
        "=== build ===",
        "=== config ===",
        "=== ring ===",
        "=== peers ===",
        "=== queues ===",
        "=== gossip ===",
        "=== recent_events ===",
        "=== memory ===",
        "=== fds ===",
    ] {
        assert!(stdout.contains(header), "missing {header} in:\n{stdout}");
    }
    assert!(stdout.is_ascii(), "response must be ASCII-only");
    server_task.abort();
}

#[test]
fn cluster_info_writes_to_output_path() {
    let runtime = make_runtime();

    let (addr, server_task) = runtime.block_on(async {
        let sink = Arc::new(Mutex::new(Snapshot::default()));
        let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)
            .await
            .expect("bind")
            .with_cluster_info_provider(Arc::new(ClusterInfoSnapshot::synthetic));
        let addr = server.local_addr().expect("local_addr");
        let task = tokio::spawn(async move {
            let _ = server.run().await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
    });

    let node = format!("127.0.0.1:{}", addr.port());
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    let path = tmp.path().to_path_buf();

    Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args([
            "cluster-info",
            "--node",
            &node,
            "--output",
            path.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("wrote "));
    let body = std::fs::read_to_string(&path).expect("read dump");
    assert!(body.contains("=== build ==="));
    assert!(body.contains("=== fds ==="));
    server_task.abort();
}

#[test]
fn cluster_info_returns_503_when_provider_unwired() {
    let runtime = make_runtime();

    let (addr, server_task) = runtime.block_on(async {
        let sink = Arc::new(Mutex::new(Snapshot::default()));
        // Note: no with_cluster_info_provider call; the route
        // must return 503 in that mode and the CLI must surface
        // the failure.
        let server = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)
            .await
            .expect("bind");
        let addr = server.local_addr().expect("local_addr");
        let task = tokio::spawn(async move {
            let _ = server.run().await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
    });

    let node = format!("127.0.0.1:{}", addr.port());

    Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["cluster-info", "--node", &node])
        .assert()
        .failure()
        .stderr(predicate::str::contains("HTTP 503"));
    server_task.abort();
}
