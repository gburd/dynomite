//! Integration tests for the cluster-info diagnostic dump.
//!
//! Build a snapshot from a synthetic [`ConfPool`] / synthetic
//! peer table, render it to text, and assert the section headers,
//! the no-secret-leakage invariant, and the recent-events ring.

use dynomite::admin::cluster_info::{
    build_section, config_section, fds_section, format_text, gossip_section, memory_section,
    peer_row, queues_section, recent_events_section, ring_section, ClusterInfoSnapshot,
    RecentEvent, RecentEvents, RowSection,
};
use dynomite::cluster::peer::PeerState;
use dynomite::conf::ConfPool;
use dynomite::stats::{PoolStats, ServerStats, ServiceInfo, Stats};

const SECRET_PASSWORD: &str = "this-must-not-leak";

/// Assemble a representative snapshot from synthetic inputs.
fn build_synthetic_snapshot() -> ClusterInfoSnapshot {
    let mut pool = ConfPool::default();
    pool.apply_defaults();
    pool.redis_requirepass = Some(SECRET_PASSWORD.to_string());
    pool.rack = Some("rack-floki".into());
    pool.datacenter = Some("dc-floki".into());

    let stats = Stats::new(
        ServiceInfo {
            source: "node-floki".into(),
            version: "0.0.1".into(),
            rack: "rack-floki".into(),
            dc: "dc-floki".into(),
        },
        PoolStats::new("dyn_o_mite"),
        ServerStats::new("redis"),
    );
    stats.set_resource_usage(1, 0, 4, 1, 16_384);
    let snap = stats.snapshot();

    let mut peers = RowSection::new("peers");
    peers.push(peer_row(
        "local",
        "dc-floki",
        "rack-floki",
        PeerState::Normal,
        0.42,
        120,
        4,
    ));
    peers.push(peer_row(
        "10.0.0.5:8101",
        "dc-arnold",
        "rack-1",
        PeerState::Down,
        9.10,
        1_500,
        4,
    ));

    let events = RecentEvents::new();
    events.push(RecentEvent::new(1_700_000_000, "restart", "boot"));
    events.push(RecentEvent::new(1_700_000_001, "peer_up", "10.0.0.5:8101"));
    events.push(RecentEvent::new(
        1_700_000_120,
        "peer_down",
        "10.0.0.5:8101 phi=9.10",
    ));

    ClusterInfoSnapshot {
        build: build_section(),
        config: config_section(&pool),
        ring: ring_section("vnode", 64, 4),
        peers,
        queues: queues_section(&snap),
        gossip: gossip_section(&snap),
        recent_events: recent_events_section(&events.snapshot()),
        memory: memory_section(snap.dyn_memory),
        fds: fds_section(),
    }
}

#[test]
fn rendered_dump_includes_all_required_sections() {
    let snap = build_synthetic_snapshot();
    let mut buf = Vec::new();
    format_text(&snap, &mut buf).expect("format_text");
    let text = String::from_utf8(buf).expect("ascii utf-8");
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
        assert!(text.contains(header), "missing section header: {header}");
    }
    assert!(text.is_ascii(), "snapshot must be ASCII-only");
}

#[test]
fn rendered_dump_never_leaks_secret_config() {
    let snap = build_synthetic_snapshot();
    let mut buf = Vec::new();
    format_text(&snap, &mut buf).expect("format_text");
    let text = String::from_utf8(buf).expect("ascii utf-8");
    assert!(
        !text.contains(SECRET_PASSWORD),
        "secret password leaked into dump:\n{text}"
    );
    assert!(text.contains("redis_requirepass=redacted"));
}

#[test]
fn rendered_dump_carries_synthetic_peer_rows() {
    let snap = build_synthetic_snapshot();
    let mut buf = Vec::new();
    format_text(&snap, &mut buf).expect("format_text");
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains("peer_id=local"));
    assert!(text.contains("status=NORMAL"));
    assert!(text.contains("status=DOWN"));
    assert!(text.contains("phi=9.10"));
}

#[test]
fn rendered_dump_carries_recent_events() {
    let snap = build_synthetic_snapshot();
    let mut buf = Vec::new();
    format_text(&snap, &mut buf).expect("format_text");
    let text = String::from_utf8(buf).unwrap();
    assert!(text.contains("restart boot"));
    assert!(text.contains("peer_up 10.0.0.5:8101"));
    assert!(text.contains("peer_down 10.0.0.5:8101 phi=9.10"));
    // Timestamps are iso-8601 UTC and lexicographically sorted.
    let first = text.find("=== recent_events ===").unwrap();
    let body = &text[first..];
    assert!(body.contains("2023-11-14T22:13:20Z"));
}

#[test]
fn ring_buffer_caps_at_max_recent_events() {
    let log = RecentEvents::new();
    for i in 0..200u64 {
        log.push(RecentEvent::new(i, "tick", ""));
    }
    let snap = log.snapshot();
    assert_eq!(snap.len(), 50);
    // The newest entry must be the highest ts, the oldest must
    // be at the wrap-around boundary.
    assert_eq!(snap.last().unwrap().ts_secs, 199);
    assert_eq!(snap.first().unwrap().ts_secs, 150);
}

#[test]
fn build_section_carries_version_and_profile() {
    let s = build_section();
    let pairs: Vec<(String, String)> = s.pairs().to_vec();
    let version = pairs.iter().find(|(k, _)| k == "version").expect("version");
    assert_eq!(version.1, env!("CARGO_PKG_VERSION"));
    let profile = pairs
        .iter()
        .find(|(k, _)| k == "build_profile")
        .expect("build_profile");
    assert!(profile.1 == "debug" || profile.1 == "release");
}
