//! End-to-end smoke test for `dyn-admin ring-status`.
//!
//! Stand up a real PBC listener (for the queried node's server
//! info) plus a [`dynomite::stats::StatsServer`] serving a live
//! multi-peer `/ring` document, then drive a real
//! `dyn-admin ring-status` invocation via [`assert_cmd`] and
//! assert that every peer renders one row with real tokens and
//! state. A separate case proves the single-row fallback when the
//! stats endpoint is unreachable.

use std::sync::Arc;
use std::time::Duration;

use assert_cmd::Command;
use parking_lot::Mutex;
use predicates::prelude::*;
use tokio::net::TcpListener;

use dyniak::serve_pbc;
use dynomite::admin::cluster_info::gather_ring_from_pool;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::embed::{Datastore, MemoryDatastore};
use dynomite::hashkit::DynToken;
use dynomite::stats::{RingProvider, Snapshot, StatsServer};

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio multi-thread runtime")
}

fn three_peer_pool() -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        ..PoolConfig::default()
    };
    let mut local = Peer::new(
        0,
        PeerEndpoint::tcp("10.0.0.1".into(), 8101),
        "r1".into(),
        "dc1".into(),
        vec![DynToken::from_u32(0)],
        true,
        true,
        false,
    );
    local.set_state(PeerState::Normal, 0);
    let mut up = Peer::new(
        1,
        PeerEndpoint::tcp("10.0.0.2".into(), 8101),
        "r2".into(),
        "dc1".into(),
        vec![DynToken::from_u32(1_431_655_765)],
        false,
        true,
        false,
    );
    up.set_state(PeerState::Normal, 0);
    let mut down = Peer::new(
        2,
        PeerEndpoint::tcp("10.0.0.3".into(), 8101),
        "r1".into(),
        "dc2".into(),
        vec![DynToken::from_u32(2_863_311_530)],
        false,
        false,
        false,
    );
    down.set_state(PeerState::Down, 0);
    Arc::new(ServerPool::new(cfg, vec![local, up, down]))
}

#[test]
fn ring_status_renders_one_row_per_peer() {
    let runtime = make_runtime();
    let pool = three_peer_pool();

    let (pbc_addr, stats_addr, pbc_task, stats_task) = runtime.block_on(async {
        let pbc = TcpListener::bind("127.0.0.1:0").await.expect("bind pbc");
        let pbc_addr = pbc.local_addr().expect("pbc addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let pbc_task = tokio::spawn(async move {
            let _ = serve_pbc(pbc, ds).await;
        });

        let ring_pool = Arc::clone(&pool);
        let provider: RingProvider = Arc::new(move || gather_ring_from_pool(&ring_pool));
        let sink = Arc::new(Mutex::new(Snapshot::default()));
        let stats = StatsServer::bind("127.0.0.1:0".parse().unwrap(), sink)
            .await
            .expect("bind stats")
            .with_ring_provider(provider);
        let stats_addr = stats.local_addr().expect("stats addr");
        let stats_task = tokio::spawn(async move {
            let _ = stats.run().await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (pbc_addr, stats_addr, pbc_task, stats_task)
    });

    let node = format!("127.0.0.1:{}", pbc_addr.port());
    let stats_node = format!("127.0.0.1:{}", stats_addr.port());

    let assertion = Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args([
            "ring-status",
            "--node",
            &node,
            "--stats-node",
            &stats_node,
            "--json",
        ])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json parse");
    let entries = parsed["entries"].as_array().expect("entries array");
    assert_eq!(entries.len(), 3, "expected one row per peer:\n{stdout}");
    assert_eq!(entries[0]["node"], "10.0.0.1:8101");
    assert_eq!(entries[0]["token"], "0");
    assert_eq!(entries[0]["state"], "NORMAL");
    assert_eq!(entries[1]["token"], "1431655765");
    assert_eq!(entries[1]["rack"], "r2");
    assert_eq!(entries[2]["state"], "DOWN");
    assert_eq!(entries[2]["dc"], "dc2");
    assert!(stdout.is_ascii(), "output must be ASCII-only");

    pbc_task.abort();
    stats_task.abort();
}

#[test]
fn ring_status_falls_back_to_single_row_without_stats() {
    let runtime = make_runtime();

    let (pbc_addr, pbc_task) = runtime.block_on(async {
        let pbc = TcpListener::bind("127.0.0.1:0").await.expect("bind pbc");
        let pbc_addr = pbc.local_addr().expect("pbc addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let pbc_task = tokio::spawn(async move {
            let _ = serve_pbc(pbc, ds).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (pbc_addr, pbc_task)
    });

    let node = format!("127.0.0.1:{}", pbc_addr.port());

    // --no-stats disables the HTTP ring fetch; the command must
    // still succeed with a single best-effort row.
    let assertion = Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args(["ring-status", "--node", &node, "--no-stats", "--json"])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json parse");
    let entries = parsed["entries"].as_array().expect("entries array");
    assert_eq!(entries.len(), 1, "fallback must be a single row:\n{stdout}");
    assert_eq!(entries[0]["state"], "up");
    assert_eq!(entries[0]["token"], "<unset>");

    pbc_task.abort();
}

#[test]
fn ring_status_falls_back_when_stats_unreachable() {
    let runtime = make_runtime();

    let (pbc_addr, pbc_task) = runtime.block_on(async {
        let pbc = TcpListener::bind("127.0.0.1:0").await.expect("bind pbc");
        let pbc_addr = pbc.local_addr().expect("pbc addr");
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let pbc_task = tokio::spawn(async move {
            let _ = serve_pbc(pbc, ds).await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        (pbc_addr, pbc_task)
    });

    let node = format!("127.0.0.1:{}", pbc_addr.port());

    // Point --stats-node at a refused port; the command must not
    // hard-fail, it falls back to the single-row view with a note.
    let assertion = Command::cargo_bin("dyn-admin")
        .expect("locate dyn-admin binary")
        .args([
            "ring-status",
            "--node",
            &node,
            "--stats-node",
            "127.0.0.1:1",
            "--json",
        ])
        .assert()
        .success();
    let stdout = String::from_utf8(assertion.get_output().stdout.clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json parse");
    let entries = parsed["entries"].as_array().expect("entries array");
    assert_eq!(entries.len(), 1);
    assert!(predicate::str::contains("ring fetch failed").eval(&stdout));

    pbc_task.abort();
}
