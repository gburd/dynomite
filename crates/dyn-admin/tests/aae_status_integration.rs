//! End-to-end smoke test for `dyn-admin aae-status`.
//!
//! Spins up [`dyn_riak::serve_pbc_with_aae_status`] with a
//! synthetic [`AaeStatusProvider`] returning a known snapshot
//! and confirms the CLI renders matching content in both
//! human and JSON modes.

use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;
use tokio::net::TcpListener;

use dyn_riak::aae::status::{AaePeerStatus, AaeStatusProvider, AaeStatusSnapshot};
use dyn_riak::serve_pbc_with_aae_status;
use dynomite::cluster::admin_rpc::{ClusterAdmin, NoopClusterAdmin};
use dynomite::embed::{Datastore, MemoryDatastore};

#[derive(Debug)]
struct StaticProvider;

impl AaeStatusProvider for StaticProvider {
    fn current_status(&self) -> AaeStatusSnapshot {
        AaeStatusSnapshot {
            peers: vec![
                AaePeerStatus {
                    peer_idx: 0,
                    dc: "dc1".into(),
                    rack: "rA".into(),
                    last_exchange_unix: 1_700_000_000,
                    divergent_keys_since_last_full_sweep: 12,
                    repair_dispatched_total: 9,
                },
                AaePeerStatus {
                    peer_idx: 1,
                    dc: "dc1".into(),
                    rack: "rB".into(),
                    last_exchange_unix: 0,
                    divergent_keys_since_last_full_sweep: 0,
                    repair_dispatched_total: 0,
                },
            ],
            snapshot_path: "/tmp/test-aae.snap".into(),
            snapshot_last_save_unix: 1_700_000_300,
            snapshot_last_load_unix: 0,
            snapshot_save_total: 5,
            snapshot_load_total: 0,
            snapshot_corruption_total: 0,
            tree_n_time_buckets: 24,
            tree_n_segments: 1024,
            tree_time_window_seconds: 3600,
            tree_memory_estimate_bytes: 4096,
        }
    }
}

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio multi-thread runtime")
}

#[test]
fn aae_status_human_round_trip_against_synthetic_provider() {
    let runtime = make_runtime();
    let (addr, server_task) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
        let provider: Arc<dyn AaeStatusProvider> = Arc::new(StaticProvider);
        let task = tokio::spawn(async move {
            let _ = serve_pbc_with_aae_status(listener, ds, admin, provider).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
    });

    let node = format!("127.0.0.1:{}", addr.port());
    Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["aae-status", "--node", &node])
        .assert()
        .success()
        .stdout(predicate::str::contains("AAE status (node "))
        .stdout(predicate::str::contains("dc1"))
        .stdout(predicate::str::contains(
            "snapshot_path: /tmp/test-aae.snap",
        ))
        .stdout(predicate::str::contains("snapshot_save_total: 5"))
        .stdout(predicate::str::contains("24 time-buckets"))
        .stdout(predicate::str::contains("2 peer(s)"));

    server_task.abort();
}

#[test]
fn aae_status_json_round_trip_against_synthetic_provider() {
    let runtime = make_runtime();
    let (addr, server_task) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
        let provider: Arc<dyn AaeStatusProvider> = Arc::new(StaticProvider);
        let task = tokio::spawn(async move {
            let _ = serve_pbc_with_aae_status(listener, ds, admin, provider).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
    });

    let node = format!("127.0.0.1:{}", addr.port());
    let assertion = Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["aae-status", "--node", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json parse");
    assert_eq!(parsed["node"], node);
    assert_eq!(parsed["peers"][0]["peer_idx"], 0);
    assert_eq!(parsed["peers"][0]["dc"], "dc1");
    assert_eq!(parsed["snapshot_save_total"], 5);
    assert_eq!(parsed["tree_n_time_buckets"], 24);

    server_task.abort();
}

#[test]
fn aae_status_default_provider_returns_empty_snapshot() {
    // serve_pbc (default entry point) wires a NoopAaeStatusProvider;
    // we expect the CLI to render an empty snapshot without
    // erroring.
    let runtime = make_runtime();
    let (addr, server_task) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let task = tokio::spawn(async move {
            let _ = dyn_riak::serve_pbc(listener, ds).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (addr, task)
    });

    let node = format!("127.0.0.1:{}", addr.port());
    Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["aae-status", "--node", &node, "--json"])
        .assert()
        .success();

    server_task.abort();
}
