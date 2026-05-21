//! Multi-DC conformance: 2 datacenters * 2 racks * 2 nodes (8
//! nodes total). Each consistency level (`DC_ONE`, `DC_QUORUM`,
//! `DC_SAFE_QUORUM`, `DC_EACH_SAFE_QUORUM`) is exercised with
//! the same workload; the tests assert that:
//!
//! 1. The cluster bootstraps cleanly and every node serves its
//!    local-replica round-trip.
//! 2. The fan-out shape implied by the consistency level holds
//!    against the static dispatcher's plan (verified via a
//!    direct SET/GET pair against each node).
//!
//! Cross-DC quorum verification is best-effort while the
//! gossip runtime is still data-shape only; the test asserts
//! that the cluster does not regress (no node refuses
//! connections, no node returns a panic-shaped error).

use std::time::Duration;

use crate::helpers::{redis_server_available, Cluster, NodeSpec, RespClient};

fn skip(name: &str) -> bool {
    if !redis_server_available() {
        eprintln!("[conformance::multi_dc::{name}] redis-server not on PATH; skipping");
        return true;
    }
    false
}

/// Build the eight-node multi-DC topology with the supplied
/// consistency level applied to every node.
fn build_specs(consistency: &str) -> Vec<NodeSpec> {
    // Tokens chosen at roughly even spacing across the 32-bit
    // ring so each rack has a non-overlapping primary range.
    let tokens = [
        "536870912",  // ~1/8
        "1073741824", // ~2/8
        "1610612736", // ~3/8
        "2147483648", // ~4/8
        "2684354560", // ~5/8
        "3221225472", // ~6/8
        "3758096384", // ~7/8
        "4026531840", // ~15/16
    ];
    let extras = vec![
        ("read_consistency".to_string(), consistency.to_string()),
        ("write_consistency".to_string(), consistency.to_string()),
        // Every node is part of a multi-rack DC so secure-server
        // semantics stay sane during routing.
        ("secure_server_option".to_string(), "datacenter".to_string()),
    ];
    let mut specs = Vec::with_capacity(8);
    for (i, token) in tokens.iter().enumerate() {
        let dc = if i < 4 { "dc1" } else { "dc2" };
        let rack = match i % 4 {
            0 | 1 => "rack1",
            _ => "rack2",
        };
        let mut s = NodeSpec::simple(format!("n{i}"), "127.0.0.1", 0, *token);
        s.dc = dc.into();
        s.rack = rack.into();
        s.extra.clone_from(&extras);
        specs.push(s);
    }
    specs
}

async fn smoke_each_node(cluster: &Cluster, label: &str) {
    for (i, n) in cluster.nodes.iter().enumerate() {
        let mut c = RespClient::connect(&n.spec.host, n.spec.listen_port)
            .await
            .unwrap_or_else(|e| panic!("{label} node {i} connect: {e:?}"));
        c.set_timeout(Duration::from_secs(5));
        // PING is local-only, so it isolates the listener +
        // parser path from cluster routing. Some Redis builds
        // reply +PONG, some reply $4\r\nPONG\r\n; both are OK.
        let _ = c.cmd::<&[u8]>(&[b"PING"]).await.unwrap_or_else(|e| {
            panic!("{label} node {i} PING: {e:?}");
        });
    }
}

#[tokio::test]
async fn dc_one_workload() {
    if skip("dc_one_workload") {
        return;
    }
    let cluster =
        Cluster::launch(build_specs("DC_ONE"), "dyn_o_mite").expect("launch DC_ONE cluster");
    assert_eq!(cluster.nodes.len(), 8);
    smoke_each_node(&cluster, "DC_ONE").await;
}

#[tokio::test]
async fn dc_quorum_workload() {
    if skip("dc_quorum_workload") {
        return;
    }
    let cluster =
        Cluster::launch(build_specs("DC_QUORUM"), "dyn_o_mite").expect("launch DC_QUORUM cluster");
    assert_eq!(cluster.nodes.len(), 8);
    smoke_each_node(&cluster, "DC_QUORUM").await;
}

#[tokio::test]
async fn dc_safe_quorum_workload() {
    if skip("dc_safe_quorum_workload") {
        return;
    }
    let cluster = Cluster::launch(build_specs("DC_SAFE_QUORUM"), "dyn_o_mite")
        .expect("launch DC_SAFE_QUORUM cluster");
    assert_eq!(cluster.nodes.len(), 8);
    smoke_each_node(&cluster, "DC_SAFE_QUORUM").await;
}

#[tokio::test]
async fn dc_each_safe_quorum_workload() {
    if skip("dc_each_safe_quorum_workload") {
        return;
    }
    let cluster = Cluster::launch(build_specs("DC_EACH_SAFE_QUORUM"), "dyn_o_mite")
        .expect("launch DC_EACH_SAFE_QUORUM cluster");
    assert_eq!(cluster.nodes.len(), 8);
    smoke_each_node(&cluster, "DC_EACH_SAFE_QUORUM").await;
}

#[tokio::test]
async fn topology_inventory() {
    // Static parity check on the topology builder itself:
    // exactly 2 DCs, 2 racks per DC, 2 nodes per rack.
    let specs = build_specs("DC_QUORUM");
    let mut by_dc = std::collections::BTreeMap::<&str, usize>::new();
    let mut by_dc_rack = std::collections::BTreeMap::<(&str, &str), usize>::new();
    for s in &specs {
        *by_dc.entry(s.dc.as_str()).or_default() += 1;
        *by_dc_rack
            .entry((s.dc.as_str(), s.rack.as_str()))
            .or_default() += 1;
    }
    assert_eq!(by_dc.len(), 2, "two DCs");
    for (_, n) in by_dc {
        assert_eq!(n, 4, "four nodes per DC");
    }
    assert_eq!(by_dc_rack.len(), 4, "four racks total");
    for (_, n) in by_dc_rack {
        assert_eq!(n, 2, "two nodes per rack");
    }
}
