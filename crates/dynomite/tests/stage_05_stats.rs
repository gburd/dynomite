//! Stage 5 integration tests for the stats subsystem.
//!
//! These tests cover the JSON snapshot fixture, the REST endpoint
//! smoke test, and property invariants on the histogram.

use std::sync::Arc;
use std::time::Duration;

use dynomite::admin::cluster_info::gather_ring_from_pool;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::hashkit::DynToken;
use dynomite::stats::{
    bucket_offsets, describe_stats, Histogram, Latency, PoolField, PoolStats, ServerField,
    ServerStats, ServiceInfo, Snapshot, Stats, StatsServer, BUCKET_COUNT, POOL_CODEC, SERVER_CODEC,
};
use hegel::generators as gs;
use hegel::TestCase;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn deterministic_snapshot() -> Snapshot {
    let mut pool = PoolStats::new("dyn_o_mite");
    pool.metrics[PoolField::ClientConnections.index()] = 3;
    pool.metrics[PoolField::ClientReadRequests.index()] = 100;
    pool.metrics[PoolField::PeerEjectedAt.index()] = 1_700_000_500;

    let mut server = ServerStats::new("redis_local");
    server.metrics[ServerField::ReadRequests.index()] = 100;
    server.metrics[ServerField::WriteRequests.index()] = 7;
    server.metrics[ServerField::ServerEjectedAt.index()] = 1_700_000_499;

    Snapshot {
        info: ServiceInfo {
            source: "node-a".to_string(),
            version: "0.0.1".to_string(),
            rack: "rack-1".to_string(),
            dc: "dc-1".to_string(),
        },
        uptime: 42,
        timestamp: 1_700_000_000,
        pool,
        server,
        ..Snapshot::default()
    }
}

#[test]
fn snapshot_matches_fixture() {
    // Structural equivalence: the writer is expected to emit a single
    // compact JSON object. The fixture captures that exact byte
    // sequence so any future regression in field set or ordering will
    // diverge here. See docs/parity.md "Stage 5: snapshot.json is
    // structural, not byte-equal" for the rationale.
    let snap = deterministic_snapshot();
    let actual = snap.to_json();
    let expected = include_str!("fixtures/stats/snapshot.json").trim_end_matches('\n');
    assert_eq!(actual, expected, "snapshot diverged from fixture");
}

#[test]
fn snapshot_contains_every_pool_metric_with_expected_value() {
    // Independent reconstruction of the expected JSON from POOL_CODEC
    // and SERVER_CODEC ensures the test does not just round-trip the
    // writer against a snapshot of itself: any change to the metric
    // set, naming, or ordering will fail this check.
    let snap = deterministic_snapshot();
    let body = snap.to_json();
    for (idx, spec) in POOL_CODEC.iter().enumerate() {
        let value = snap.pool.metrics[idx];
        let needle = format!("\"{}\":{value}", spec.name);
        assert!(
            body.contains(&needle),
            "pool metric {} missing or has wrong value in JSON",
            spec.name
        );
    }
    for (idx, spec) in SERVER_CODEC.iter().enumerate() {
        let value = snap.server.metrics[idx];
        let needle = format!("\"{}\":{value}", spec.name);
        assert!(
            body.contains(&needle),
            "server metric {} missing or has wrong value in JSON",
            spec.name
        );
    }
    // Engine identification fields.
    assert!(body.contains("\"service\":\"dynomite\""));
    assert!(body.contains("\"source\":\"node-a\""));
    assert!(body.contains("\"version\":\"0.0.1\""));
    assert!(body.contains("\"rack\":\"rack-1\""));
    assert!(body.contains("\"dc\":\"dc-1\""));
    // Pool and server names appear as object keys.
    assert!(body.contains("\"dyn_o_mite\":{"));
    assert!(body.contains("\"redis_local\":{"));
}

#[test]
fn snapshot_pool_object_appears_before_server_object() {
    // The reference output nests the per-server object inside the
    // per-pool object. We assert the structural ordering remains.
    let snap = deterministic_snapshot();
    let body = snap.to_json();
    let pool_idx = body.find("\"dyn_o_mite\":{").expect("pool object present");
    let server_idx = body
        .find("\"redis_local\":{")
        .expect("server object present");
    assert!(
        pool_idx < server_idx,
        "pool object must precede the nested server object"
    );
}

#[test]
fn describe_stats_lists_canonical_pool_metrics() {
    let text = describe_stats();
    assert!(text.starts_with("pool stats:\n"));
    assert!(text.contains("server stats:\n"));
    assert!(text.contains("client_eof"));
    assert!(text.contains("redis_req_other"));
}

#[tokio::test]
async fn rest_endpoint_serves_snapshot_json() {
    let snap = deterministic_snapshot();
    let sink = Arc::new(Mutex::new(snap));
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let server = StatsServer::bind(addr, Arc::clone(&sink)).expect("bind ephemeral port");
    let local = server.local_addr().expect("local address");
    let server = Arc::new(server);
    let bg = Arc::clone(&server);
    let handle = tokio::spawn(async move {
        // One-shot: serve a single request then return.
        bg.accept_one().await
    });

    let mut conn = TcpStream::connect(local)
        .await
        .expect("connect to stats server");
    conn.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("write request");
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(2), conn.read_to_end(&mut buf))
        .await
        .expect("response within timeout")
        .expect("read response");
    assert!(read > 0, "expected non-empty response");

    let response = String::from_utf8(buf).expect("response is ascii");
    let header_end = response
        .find("\r\n\r\n")
        .expect("header terminator present");
    let status_line = response.lines().next().expect("status line");
    assert!(
        status_line.starts_with("HTTP/1.1 200"),
        "unexpected status: {status_line}"
    );
    assert!(response[..header_end].contains("Content-Type: application/json"));
    let body = &response[header_end + 4..];
    assert!(body.starts_with('{'));
    assert!(body.ends_with('}'));
    assert!(body.contains("\"service\":\"dynomite\""));
    assert!(body.contains("\"source\":\"node-a\""));
    assert!(body.contains("\"dyn_o_mite\""));
    assert!(body.contains("\"redis_local\""));

    handle
        .await
        .expect("server task joined")
        .expect("server completed without error");
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus_text() {
    let snap = deterministic_snapshot();
    let sink = Arc::new(Mutex::new(snap));
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let server = StatsServer::bind(addr, Arc::clone(&sink)).expect("bind ephemeral port");
    let local = server.local_addr().expect("local address");
    let handle = tokio::spawn(async move { server.accept_one().await });

    let mut conn = TcpStream::connect(local)
        .await
        .expect("connect to stats server");
    conn.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("write request");
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(2), conn.read_to_end(&mut buf))
        .await
        .expect("response within timeout")
        .expect("read response");
    assert!(read > 0, "expected non-empty response");

    let response = String::from_utf8(buf).expect("response is utf-8");
    let header_end = response
        .find("\r\n\r\n")
        .expect("header terminator present");
    let status_line = response.lines().next().expect("status line");
    assert!(
        status_line.starts_with("HTTP/1.1 200"),
        "unexpected status: {status_line}"
    );
    let headers = &response[..header_end];
    let ct_line = headers
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-type:"))
        .expect("Content-Type header present");
    let ct_value = ct_line
        .split_once(':')
        .map(|(_, v)| v)
        .expect("Content-Type has value")
        .trim();
    assert!(
        ct_value.starts_with("text/plain"),
        "unexpected Content-Type: {ct_value}"
    );
    assert!(
        ct_value.contains("version=0.0.4"),
        "Content-Type missing prometheus version: {ct_value}"
    );

    let body = &response[header_end + 4..];
    assert!(
        body.contains("dynomite_build_info"),
        "prometheus body missing build_info:\n{body}"
    );
    assert!(
        body.contains("# HELP "),
        "prometheus body missing # HELP lines:\n{body}"
    );
    assert!(
        body.contains("# TYPE "),
        "prometheus body missing # TYPE lines:\n{body}"
    );

    handle
        .await
        .expect("server task joined")
        .expect("server completed without error");
}

#[tokio::test]
async fn ring_endpoint_serves_multi_peer_json() {
    // Build a real three-peer pool with distinct tokens, dcs, and
    // racks; mark the third peer Down. The /ring route must serialise
    // every peer's real tokens and lifecycle state -- not a synthetic
    // single-row view.
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
    let mut remote_up = Peer::new(
        1,
        PeerEndpoint::tcp("10.0.0.2".into(), 8101),
        "r2".into(),
        "dc1".into(),
        vec![DynToken::from_u32(1_431_655_765)],
        false,
        true,
        false,
    );
    remote_up.set_state(PeerState::Normal, 0);
    let mut remote_down = Peer::new(
        2,
        PeerEndpoint::tcp("10.0.0.3".into(), 8101),
        "r1".into(),
        "dc2".into(),
        vec![DynToken::from_u32(2_863_311_530)],
        false,
        false,
        false,
    );
    remote_down.set_state(PeerState::Down, 0);
    let pool = Arc::new(ServerPool::new(cfg, vec![local, remote_up, remote_down]));

    let ring_pool = Arc::clone(&pool);
    let provider: dynomite::stats::RingProvider =
        Arc::new(move || gather_ring_from_pool(&ring_pool));

    let sink = Arc::new(Mutex::new(Snapshot::default()));
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let server = StatsServer::bind(addr, Arc::clone(&sink))
        .expect("bind ephemeral port")
        .with_ring_provider(provider);
    let local_addr = server.local_addr().expect("local address");
    let handle = tokio::spawn(async move { server.accept_one().await });

    let mut conn = TcpStream::connect(local_addr)
        .await
        .expect("connect to stats server");
    conn.write_all(b"GET /ring HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("write request");
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), conn.read_to_end(&mut buf))
        .await
        .expect("response within timeout")
        .expect("read response");
    let response = String::from_utf8(buf).expect("response is ascii");
    let header_end = response
        .find("\r\n\r\n")
        .expect("header terminator present");
    assert!(response.lines().next().unwrap().starts_with("HTTP/1.1 200"));
    assert!(response[..header_end].contains("Content-Type: application/json"));
    let body = &response[header_end + 4..];
    let parsed: serde_json::Value = serde_json::from_str(body).expect("json body");
    let peers = parsed["peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 3);
    assert_eq!(peers[0]["node"], "10.0.0.1:8101");
    assert_eq!(peers[0]["tokens"][0], 0);
    assert_eq!(peers[0]["is_local"], true);
    assert_eq!(peers[0]["state"], "NORMAL");
    assert_eq!(peers[1]["tokens"][0], 1_431_655_765);
    assert_eq!(peers[1]["rack"], "r2");
    assert_eq!(peers[2]["dc"], "dc2");
    assert_eq!(peers[2]["tokens"][0], 2_863_311_530u64);
    assert_eq!(peers[2]["state"], "DOWN");

    handle
        .await
        .expect("server task joined")
        .expect("server completed without error");
}

#[tokio::test]
async fn ring_endpoint_returns_503_when_provider_unwired() {
    let sink = Arc::new(Mutex::new(Snapshot::default()));
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let server = StatsServer::bind(addr, Arc::clone(&sink)).expect("bind ephemeral port");
    let local_addr = server.local_addr().expect("local address");
    let handle = tokio::spawn(async move { server.accept_one().await });

    let mut conn = TcpStream::connect(local_addr)
        .await
        .expect("connect to stats server");
    conn.write_all(b"GET /ring HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("write request");
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), conn.read_to_end(&mut buf))
        .await
        .expect("response within timeout")
        .expect("read response");
    let response = String::from_utf8(buf).expect("response is ascii");
    assert!(response.lines().next().unwrap().starts_with("HTTP/1.1 503"));

    handle
        .await
        .expect("server task joined")
        .expect("server completed without error");
}

#[tokio::test]
async fn stats_endpoint_unchanged() {
    // Regression guard: hitting /stats must return the exact same JSON
    // body as the existing fixture used by `snapshot_matches_fixture`.
    // The /stats route is an alias for the legacy / and /info paths
    // and exists so monitoring tooling can use a stable URL.
    let snap = deterministic_snapshot();
    let sink = Arc::new(Mutex::new(snap));
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let server = StatsServer::bind(addr, Arc::clone(&sink)).expect("bind ephemeral port");
    let local = server.local_addr().expect("local address");
    let handle = tokio::spawn(async move { server.accept_one().await });

    let mut conn = TcpStream::connect(local)
        .await
        .expect("connect to stats server");
    conn.write_all(b"GET /stats HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("write request");
    let mut buf = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(2), conn.read_to_end(&mut buf))
        .await
        .expect("response within timeout")
        .expect("read response");
    assert!(read > 0, "expected non-empty response");

    let response = String::from_utf8(buf).expect("response is utf-8");
    let header_end = response
        .find("\r\n\r\n")
        .expect("header terminator present");
    assert!(response[..header_end].contains("Content-Type: application/json"));
    let body = &response[header_end + 4..];
    let expected = include_str!("fixtures/stats/snapshot.json").trim_end_matches('\n');
    assert_eq!(body, expected, "/stats body diverged from fixture");

    handle
        .await
        .expect("server task joined")
        .expect("server completed without error");
}

#[hegel::test(test_cases = 256)]
fn histogram_percentiles_are_ordered(tc: TestCase) {
    let values = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(0).max_value(1_000_000))
            .min_size(1)
            .max_size(255),
    );
    let mut h = Histogram::new();
    for v in &values {
        h.record(*v);
    }
    h.compute();
    assert!(h.val_95th() <= h.val_99th());
    assert!(h.val_99th() <= h.val_999th());
}

#[hegel::test(test_cases = 256)]
fn histogram_uniform_percentile_within_bucket_resolution(tc: TestCase) {
    let n = tc.draw(gs::integers::<u64>().min_value(100).max_value(1_999));
    let mut h = Histogram::new();
    for v in 0..n {
        h.record(v);
    }
    h.compute();
    for (num, q) in [
        (950, h.val_95th()),
        (990, h.val_99th()),
        (999, h.val_999th()),
    ] {
        // Uniform 0..n puts `offset[i + 1]` observations at or below
        // bucket i, so the published offset is the last one below the
        // threshold and the next offset (at most 1.2x) reaches it. The
        // slack of one covers the f64 rounding of `n * p`.
        let threshold = n * num / 1_000;
        assert!(
            q <= threshold && threshold <= (q * 6 / 5).max(q + 1) + 1,
            "p{num}: threshold {threshold}, got {q}"
        );
    }
}

#[hegel::test(test_cases = 256)]
fn histogram_merge_preserves_count_sum(tc: TestCase) {
    let xs = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(0).max_value(100_000))
            .min_size(0)
            .max_size(127),
    );
    let ys = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(0).max_value(100_000))
            .min_size(0)
            .max_size(127),
    );
    let mut a = Histogram::new();
    let mut b = Histogram::new();
    for v in &xs {
        a.record(*v);
    }
    for v in &ys {
        b.record(*v);
    }
    let total = a.count() + b.count();
    a.merge(&b);
    assert_eq!(a.count(), total);
}

/// Identical observations publish their bucket offset for every
/// percentile, including counts too small for any percentile threshold
/// to reach one observation (the first offset, 1, is published then).
#[hegel::test(test_cases = 256)]
fn identical_observations_publish_their_offset(tc: TestCase) {
    let count = tc.draw(gs::integers::<u64>().min_value(1).max_value(5_000));
    let v = tc.draw(gs::integers::<u64>().min_value(0).max_value(1));
    let mut h = Histogram::new();
    for _ in 0..count {
        h.record(v);
    }
    h.compute();
    assert_eq!((h.val_95th(), h.val_99th(), h.val_999th()), (1, 1, 1));
    assert_eq!(h.mean(), 1);
}

/// A value past the end of the bucket table is counted in the last
/// bucket the bisection can reach (never the final, overflow bucket),
/// so the snapshot keeps publishing: max is the raw value and the
/// percentiles that bucket reaches are its offset.
#[test]
fn latency_huge_value_lands_in_last_reachable_bucket() {
    let stats = Stats::new(
        ServiceInfo {
            source: "node".into(),
            version: "0.0.1".into(),
            rack: "r".into(),
            dc: "d".into(),
        },
        PoolStats::new("dyn_o_mite"),
        ServerStats::new("redis"),
    );

    for _ in 0..1_000 {
        stats.record_latency(Latency::Request, 50);
    }
    let pre = stats.snapshot();
    assert_eq!(pre.latency.max, 50);
    assert_eq!(pre.latency.p99, 44, "50 counts in the bucket at offset 44");

    assert_eq!(BUCKET_COUNT, Histogram::new().buckets().len());
    for _ in 0..20 {
        stats.record_latency(Latency::Request, u64::MAX);
    }

    let snap = stats.snapshot();
    let last_counted = bucket_offsets()[BUCKET_COUNT - 2];
    assert_eq!(snap.latency.max, u64::MAX);
    assert_eq!(snap.latency.p999, last_counted);
    assert_eq!(snap.latency.p99, last_counted);
    assert_eq!(snap.latency.p95, 44);
    assert!(snap.latency.mean > 44);

    // Other histograms are untouched.
    assert_eq!(snap.payload_size.max, 0);
    assert_eq!(snap.server_latency.max, 0);
}
