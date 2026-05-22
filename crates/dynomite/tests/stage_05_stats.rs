//! Stage 5 integration tests for the stats subsystem.
//!
//! These tests cover the JSON snapshot fixture, the REST endpoint
//! smoke test, and property invariants on the histogram.

use std::sync::Arc;
use std::time::Duration;

use dynomite::stats::{
    describe_stats, Histogram, Latency, PoolField, PoolStats, ServerField, ServerStats,
    ServiceInfo, Snapshot, Stats, StatsServer, BUCKET_COUNT, POOL_CODEC, SERVER_CODEC,
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
    let server = StatsServer::bind(addr, Arc::clone(&sink))
        .await
        .expect("bind ephemeral port");
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
    let server = StatsServer::bind(addr, Arc::clone(&sink))
        .await
        .expect("bind ephemeral port");
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
        .splitn(2, ':')
        .nth(1)
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
async fn stats_endpoint_unchanged() {
    // Regression guard: hitting /stats must return the exact same JSON
    // body as the existing fixture used by `snapshot_matches_fixture`.
    // The /stats route is an alias for the legacy / and /info paths
    // and exists so monitoring tooling can use a stable URL.
    let snap = deterministic_snapshot();
    let sink = Arc::new(Mutex::new(snap));
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().expect("valid loopback addr");
    let server = StatsServer::bind(addr, Arc::clone(&sink))
        .await
        .expect("bind ephemeral port");
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
fn histogram_percentile_is_monotone_non_decreasing(tc: TestCase) {
    let values = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(0).max_value(1_000_000))
            .min_size(1)
            .max_size(255),
    );
    let mut h = Histogram::new();
    for v in &values {
        h.record(*v);
    }
    let mut last = 0u64;
    for i in 0..=20 {
        let p = f64::from(i) / 20.0;
        let q = h.percentile(p);
        assert!(q >= last, "percentile decreased at p={p}: {q} < {last}");
        last = q;
    }
}

#[hegel::test(test_cases = 256)]
fn histogram_uniform_percentile_within_bucket_resolution(tc: TestCase) {
    let n = tc.draw(gs::integers::<u64>().min_value(100).max_value(1_999));
    let mut h = Histogram::new();
    for v in 0..n {
        h.record(v);
    }
    let percentiles: [(f64, u64); 9] = [
        (0.1, 1),
        (0.2, 2),
        (0.3, 3),
        (0.4, 4),
        (0.5, 5),
        (0.6, 6),
        (0.7, 7),
        (0.8, 8),
        (0.9, 9),
    ];
    for (p, num) in percentiles {
        let q = h.percentile(p);
        // For uniform [0..n] inputs the true percentile is num*n/10.
        // Bucket resolution at this scale is bounded; allow a
        // generous tolerance of 25% plus a small floor.
        let expected = (n * num) / 10;
        let tolerance = (expected / 4) + 2;
        let lo = expected.saturating_sub(tolerance);
        let hi = expected.saturating_add(tolerance);
        assert!(
            q >= lo && q <= hi,
            "p={p}: expected~={expected}, got {q}, tol={tolerance}"
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

/// Differential check: for any count of identical zero observations
/// the histogram's percentile threshold matches the reference
/// expression `(p * count as f64).floor() as u64`. The helper
/// `floor_p_times_u64` is internal to the engine so this test
/// pins the observable behavior at the public API.
#[hegel::test(test_cases = 256)]
fn percentile_threshold_matches_f64_floor(tc: TestCase) {
    let count = tc.draw(
        gs::integers::<u64>()
            .min_value(1)
            .max_value(u64::from(u16::MAX)),
    );
    let p_idx = tc.draw(gs::integers::<usize>().min_value(0).max_value(6));
    let ps = [0.0f64, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0];
    let p = ps[p_idx];
    let mut h = Histogram::new();
    for _ in 0..count {
        h.record(0);
    }
    // Bucket 0 has offset 1, so percentile is 1 when the floor
    // threshold is >= 1 and 0 otherwise. p=1.0 in the engine
    // returns 0 because the percentile gate excludes p > 1.0
    // wraps; here p=1.0 is in range and yields the offset.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let reference_floor = (p * (count as f64)).floor() as u64;
    let expected = u64::from(reference_floor >= 1);
    assert_eq!(
        h.percentile(p),
        expected,
        "percentile diverged from f64 floor at p={p} count={count}"
    );
}

/// Drives a `Stats` instance into histogram overflow via the public
/// `record_latency` entry point and asserts that the JSON snapshot
/// layer suppresses every percentile by zeroing them. The histogram
/// signals overflow with `Histogram::OVERFLOW_SENTINEL`; the
/// `HistogramSummary::from_histogram` mapping turns that into a
/// default-valued summary, so the publicly observed values are all
/// `0`. See `crates/dynomite/src/stats/snapshot.rs` and
/// `crates/dynomite/src/stats/histogram.rs`.
#[test]
fn latency_overflow_zeroes_snapshot_percentiles() {
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

    // First, a normal observation lands and shows up in the snapshot.
    stats.record_latency(Latency::Request, 50);
    let pre = stats.snapshot();
    assert!(pre.latency.max >= 1, "baseline observation lost");

    // Now overflow the request-latency histogram. BUCKET_COUNT covers
    // bucket positions 0..BUCKET_COUNT-1; recording a value strictly
    // larger than the largest geometric offset routes into the final
    // bucket and trips the overflow flag. u64::MAX is comfortably
    // beyond every offset.
    assert_eq!(BUCKET_COUNT, Histogram::new().buckets().len());
    stats.record_latency(Latency::Request, u64::MAX);

    let snap = stats.snapshot();
    assert_eq!(snap.latency.max, 0, "overflow must zero max");
    assert_eq!(snap.latency.p999, 0, "overflow must zero p999");
    assert_eq!(snap.latency.p99, 0, "overflow must zero p99");
    assert_eq!(snap.latency.p95, 0, "overflow must zero p95");
    assert_eq!(snap.latency.mean, 0, "overflow must zero mean");

    // Other histograms are untouched by an overflow on `latency`.
    assert_eq!(snap.payload_size.max, 0);
    assert_eq!(snap.server_latency.max, 0);
}
