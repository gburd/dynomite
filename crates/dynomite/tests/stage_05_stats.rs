//! Stage 5 integration tests for the stats subsystem.
//!
//! These tests cover the JSON snapshot fixture, the REST endpoint
//! smoke test, and property invariants on the histogram.

use std::sync::Arc;
use std::time::Duration;

use dynomite::stats::{
    describe_stats, Histogram, PoolField, PoolStats, ServerField, ServerStats, ServiceInfo,
    Snapshot, StatsServer,
};
use parking_lot::Mutex;
use proptest::prelude::*;
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
    let snap = deterministic_snapshot();
    let actual = snap.to_json();
    let expected = include_str!("fixtures/stats/snapshot.json").trim_end_matches('\n');
    assert_eq!(actual, expected, "snapshot diverged from fixture");
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

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 256,
        ..ProptestConfig::default()
    })]

    #[test]
    fn histogram_percentile_is_monotone_non_decreasing(values in proptest::collection::vec(0u64..=1_000_000u64, 1..256)) {
        let mut h = Histogram::new();
        for v in &values {
            h.record(*v);
        }
        let mut last = 0u64;
        for i in 0..=20 {
            let p = f64::from(i) / 20.0;
            let q = h.percentile(p);
            prop_assert!(q >= last, "percentile decreased at p={p}: {q} < {last}");
            last = q;
        }
    }

    #[test]
    fn histogram_uniform_percentile_within_bucket_resolution(n in 100u64..2_000u64) {
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
            prop_assert!(
                q >= lo && q <= hi,
                "p={p}: expected~={expected}, got {q}, tol={tolerance}"
            );
        }
    }

    #[test]
    fn histogram_merge_preserves_count_sum(
        xs in proptest::collection::vec(0u64..=100_000u64, 0..128),
        ys in proptest::collection::vec(0u64..=100_000u64, 0..128),
    ) {
        let mut a = Histogram::new();
        let mut b = Histogram::new();
        for v in &xs { a.record(*v); }
        for v in &ys { b.record(*v); }
        let total = a.count() + b.count();
        a.merge(&b);
        prop_assert_eq!(a.count(), total);
    }

    /// Differential check: for any count of identical zero observations
    /// the histogram's percentile threshold matches the reference
    /// expression `(p * count as f64).floor() as u64`. The helper
    /// `floor_p_times_u64` is internal to the engine so this test
    /// pins the observable behavior at the public API.
    #[test]
    fn percentile_threshold_matches_f64_floor(
        count in 1u64..=u64::from(u16::MAX),
        p_idx in 0usize..7,
    ) {
        let ps = [0.0f64, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0];
        let p = ps[p_idx];
        let mut h = Histogram::new();
        for _ in 0..count { h.record(0); }
        // Bucket 0 has offset 1, so percentile is 1 when the floor
        // threshold is >= 1 and 0 otherwise. p=1.0 in the engine
        // returns 0 because the percentile gate excludes p > 1.0
        // wraps; here p=1.0 is in range and yields the offset.
        #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let reference_floor = (p * (count as f64)).floor() as u64;
        let expected = if reference_floor >= 1 { 1 } else { 0 };
        prop_assert_eq!(
            h.percentile(p),
            expected,
            "percentile diverged from f64 floor at p={} count={}", p, count
        );
    }
}
