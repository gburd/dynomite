//! End-to-end integration tests for the dyn-admin cluster-*
//! subcommands.
//!
//! Each test spins up a real `dyniak::serve_pbc_with_admin`
//! listener bound to a random localhost port with a
//! `PoolClusterAdmin` wired into the dispatch path, drives the
//! relevant `dyn-admin` subcommand via `assert_cmd`, and asserts
//! both the exit status and the post-call cluster state.

use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use predicates::prelude::*;
use tokio::net::TcpListener;

use dyniak::serve_pbc_with_admin;
use dynomite::cluster::admin_rpc::{ClusterAdmin, PoolClusterAdmin};
use dynomite::cluster::peer::{Peer, PeerEndpoint};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::embed::{Datastore, MemoryDatastore};
use dynomite::hashkit::DynToken;

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio multi-thread runtime")
}

fn small_pool() -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        ..PoolConfig::default()
    };
    let local = Peer::new(
        0,
        PeerEndpoint::tcp("127.0.0.1".into(), 8101),
        "r1".into(),
        "dc1".into(),
        vec![DynToken::from_u32(0)],
        true,
        true,
        false,
    );
    let remote = Peer::new(
        1,
        PeerEndpoint::tcp("127.0.0.1".into(), 8102),
        "r1".into(),
        "dc1".into(),
        vec![DynToken::from_u32(2_147_483_648)],
        false,
        true,
        false,
    );
    Arc::new(ServerPool::new(cfg, vec![local, remote]))
}

fn spawn_server(
    runtime: &tokio::runtime::Runtime,
    admin: Arc<dyn ClusterAdmin>,
) -> (String, tokio::task::JoinHandle<()>) {
    runtime.block_on(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let task = tokio::spawn(async move {
            let _ = serve_pbc_with_admin(listener, ds, admin).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (format!("127.0.0.1:{}", addr.port()), task)
    })
}

#[test]
fn cluster_list_reports_every_peer() {
    let runtime = make_runtime();
    let pool = small_pool();
    let admin: Arc<dyn ClusterAdmin> = Arc::new(PoolClusterAdmin::new(Arc::clone(&pool)));
    let (node, task) = spawn_server(&runtime, Arc::clone(&admin));

    let assertion = Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-list", "--seed", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    let peers = v["peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 2, "got {peers:?}");
    let by_idx: Vec<u64> = peers
        .iter()
        .map(|p| p["idx"].as_u64().expect("idx u64"))
        .collect();
    assert!(by_idx.contains(&0));
    assert!(by_idx.contains(&1));

    task.abort();
}

#[test]
fn cluster_join_then_plan_then_commit_then_list() {
    let runtime = make_runtime();
    let pool = small_pool();
    let admin: Arc<dyn ClusterAdmin> = Arc::new(PoolClusterAdmin::new(Arc::clone(&pool)));
    let (node, task) = spawn_server(&runtime, Arc::clone(&admin));

    // 1. cluster-join stages an Add change.
    Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-join", "10.0.0.5:8101", "--node", &node, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"kind\": \"add\""));

    // 2. cluster-plan reports the staged change.
    let plan_out = Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-plan", "--node", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(plan_out.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    let changes = v["changes"].as_array().expect("changes");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0]["kind"], "add");
    assert_eq!(changes[0]["peer"]["host"], "10.0.0.5");

    // 3. cluster-commit applies the staged change.
    Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-commit", "--node", &node, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"applied\": 1"));

    // 4. The peer list now includes the new peer.
    let list_out = Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-list", "--seed", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(list_out.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    let peers = v["peers"].as_array().expect("peers");
    assert_eq!(peers.len(), 3);
    assert!(peers
        .iter()
        .any(|p| p["host"] == "10.0.0.5" && p["port"] == 8101));

    // 5. plan is empty after commit.
    let plan_out2 = Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-plan", "--node", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(plan_out2.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["changes"].as_array().unwrap().len(), 0);

    task.abort();
}

#[test]
fn cluster_leave_then_commit_removes_peer() {
    let runtime = make_runtime();
    let pool = small_pool();
    let admin: Arc<dyn ClusterAdmin> = Arc::new(PoolClusterAdmin::new(Arc::clone(&pool)));
    let (node, task) = spawn_server(&runtime, Arc::clone(&admin));

    Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-leave", "1", "--node", &node, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"kind\": \"remove\""))
        .stdout(predicate::str::contains("\"peer_idx\": 1"));

    Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-commit", "--node", &node, "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"applied\": 1"));

    let list_out = Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-list", "--seed", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(list_out.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    let peers = v["peers"].as_array().expect("peers");
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0]["idx"], 0);

    task.abort();
}

#[test]
fn cluster_join_invalid_target_surfaces_error() {
    let runtime = make_runtime();
    let pool = small_pool();
    let admin: Arc<dyn ClusterAdmin> = Arc::new(PoolClusterAdmin::new(Arc::clone(&pool)));
    let (node, task) = spawn_server(&runtime, Arc::clone(&admin));

    Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-join", "this-is-not-a-socket-addr", "--node", &node])
        .assert()
        .failure()
        .stderr(predicate::str::contains("server error"));

    task.abort();
}

#[test]
fn cluster_leave_unknown_idx_surfaces_error() {
    let runtime = make_runtime();
    let pool = small_pool();
    let admin: Arc<dyn ClusterAdmin> = Arc::new(PoolClusterAdmin::new(Arc::clone(&pool)));
    let (node, task) = spawn_server(&runtime, Arc::clone(&admin));

    Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-leave", "99", "--node", &node])
        .assert()
        .failure()
        .stderr(predicate::str::contains("peer not found"));

    task.abort();
}

#[test]
fn cluster_list_against_noop_admin_returns_empty() {
    use dynomite::cluster::admin_rpc::NoopClusterAdmin;
    let runtime = make_runtime();
    let admin: Arc<dyn ClusterAdmin> = Arc::new(NoopClusterAdmin);
    let (node, task) = spawn_server(&runtime, admin);

    let out = Command::cargo_bin("dyn-admin")
        .expect("dyn-admin binary")
        .args(["cluster-list", "--seed", &node, "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let v: serde_json::Value = serde_json::from_str(&stdout).expect("json");
    assert_eq!(v["peers"].as_array().unwrap().len(), 0);

    task.abort();
}
