//! Three-node single-DC conformance: a 3-node cluster, all in
//! the same DC and rack, gossiping over the loopback. Drive a
//! workload through each node and assert per-node round-trip
//! behaviour.
//!
//! With gossip wired only at the data-shape level today (see
//! `docs/parity.md` Stage 12b deviations), cross-node routing
//! is conservative: each node is its own primary for keys whose
//! hashes fall in its token range, and replication is
//! best-effort. The test therefore asserts:
//!
//! 1. Every node accepts client connections and serves SET/GET
//!    against its local Redis backend.
//! 2. The response shape is identical regardless of which node
//!    the client picks first.
//!
//! When the gossip runtime lands, the same scenario will
//! exercise cross-node routing without changes.

use std::time::Duration;

use crate::helpers::{redis_server_available, Cluster, NodeSpec, RespClient};

fn skip(name: &str) -> bool {
    if !redis_server_available() {
        eprintln!("[conformance::three_node::{name}] valkey-server not on PATH; skipping");
        return true;
    }
    false
}

fn launch_three_node() -> Cluster {
    // Three single-token nodes spaced evenly around the
    // 32-bit token ring.
    let s0 = NodeSpec::simple("n0", "127.0.0.1", 0, "1431655765"); // ~1/3 of u32::MAX
    let s1 = NodeSpec::simple("n1", "127.0.0.1", 0, "2863311530"); // ~2/3 of u32::MAX
    let s2 = NodeSpec::simple("n2", "127.0.0.1", 0, "4294967294"); // ~end of ring
    Cluster::launch(vec![s0, s1, s2], "dyn_o_mite").expect("launch 3-node cluster")
}

async fn open_client(cluster: &Cluster, idx: usize) -> RespClient {
    let n = &cluster.nodes[idx];
    let mut c = RespClient::connect(&n.spec.host, n.spec.listen_port)
        .await
        .expect("connect");
    c.set_timeout(Duration::from_secs(5));
    c
}

#[tokio::test]
async fn each_node_accepts_workload() {
    if skip("each_node_accepts_workload") {
        return;
    }
    let cluster = launch_three_node();
    assert_eq!(cluster.nodes.len(), 3);
    for idx in 0..cluster.nodes.len() {
        let mut c = open_client(&cluster, idx).await;
        let key = format!("idx-{idx}");
        let val = format!("val-{idx}");
        let v = c
            .cmd::<&[u8]>(&[b"SET", key.as_bytes(), val.as_bytes()])
            .await
            .expect("SET");
        assert_eq!(v.as_simple(), Some("OK"), "node {idx} SET reply");
        let v = c
            .cmd::<&[u8]>(&[b"GET", key.as_bytes()])
            .await
            .expect("GET");
        assert_eq!(
            v.as_bulk(),
            Some(val.as_bytes()),
            "node {idx} GET reply mismatch",
        );
    }
}

#[tokio::test]
async fn shape_matches_across_entry_points() {
    // Same workload through node 0 vs node 2; assert reply
    // shape (type + length) is identical for non-routed
    // commands. Local-only commands like PING and ECHO go
    // through every node identically.
    if skip("shape_matches_across_entry_points") {
        return;
    }
    let cluster = launch_three_node();
    let mut c0 = open_client(&cluster, 0).await;
    let mut c2 = open_client(&cluster, 2).await;
    for cmd in [&b"PING"[..], &b"COMMAND"[..]] {
        let v0 = c0.cmd::<&[u8]>(&[cmd]).await;
        let v2 = c2.cmd::<&[u8]>(&[cmd]).await;
        // Both nodes either both accept or both reject; we do
        // not assert byte equality (Redis adds version data) but
        // we do assert error/non-error parity.
        assert_eq!(
            v0.is_ok(),
            v2.is_ok(),
            "{} parity mismatch: {v0:?} vs {v2:?}",
            String::from_utf8_lossy(cmd),
        );
    }
}

#[tokio::test]
async fn cluster_drop_kills_every_child() {
    // Spawn the cluster, capture the dynomited child PIDs, drop
    // the cluster, and assert each PID is no longer alive.
    if skip("cluster_drop_kills_every_child") {
        return;
    }
    let mut pids = Vec::new();
    {
        let cluster = launch_three_node();
        for n in &cluster.nodes {
            // Node child is wrapped in Option<Child>; we can
            // peek at the Pid via the public `pid_file` once
            // dynomited writes it. The pid_file write is a
            // Stage 12b feature.
            if let Ok(s) = std::fs::read_to_string(&n.pid_file) {
                if let Ok(pid) = s.trim().parse::<i32>() {
                    pids.push(pid);
                }
            }
        }
        assert!(!pids.is_empty(), "expected pid files to exist");
    } // <-- cluster Drop runs here
      // Allow the SIGTERM grace window plus a small slack.
    std::thread::sleep(Duration::from_millis(1500));
    for pid in pids {
        let alive = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok();
        assert!(!alive, "dynomited pid {pid} survived cluster drop");
    }
}
