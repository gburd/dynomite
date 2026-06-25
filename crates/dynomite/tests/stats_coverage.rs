//! Coverage for the stats recorder and aggregator (`stats::mod`).
//!
//! Drives every Latency / QueueWait / QueueGauge match arm in the
//! record_* methods, the server_incr / server_decr helpers,
//! reset_histograms, the queue_p99 overflow short-circuit (via a
//! u64::MAX observation), and one tick of the async Aggregator loop
//! followed by clean cancellation.

use std::sync::Arc;
use std::time::Duration;

use dynomite::stats::{
    Aggregator, Latency, PoolField, PoolStats, QueueGauge, QueueWait, ServerField, ServerStats,
    ServiceInfo, Snapshot, Stats,
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

fn fresh() -> Stats {
    Stats::new(
        ServiceInfo::default(),
        PoolStats::new("dyn_o_mite"),
        ServerStats::new("redis"),
    )
}

#[test]
fn record_latency_all_channels() {
    // Each Latency variant routes to its own histogram.
    let s = fresh();
    s.record_latency(Latency::Request, 10);
    s.record_latency(Latency::CrossRegion, 20);
    s.record_latency(Latency::CrossZone, 30);
    s.record_latency(Latency::Server, 40);
    let snap = s.snapshot();
    assert_eq!(snap.latency.max, 10);
}

#[test]
fn record_queue_wait_all_channels() {
    // Each QueueWait variant routes to its own histogram.
    let s = fresh();
    s.record_queue_wait(QueueWait::CrossRegion, 1);
    s.record_queue_wait(QueueWait::CrossZone, 2);
    s.record_queue_wait(QueueWait::Server, 3);
    // No panic and the snapshot is produced.
    let _ = s.snapshot();
}

#[test]
fn record_queue_len_all_gauges() {
    // Each QueueGauge variant routes to its own histogram.
    let s = fresh();
    for g in [
        QueueGauge::ClientOut,
        QueueGauge::ServerIn,
        QueueGauge::ServerOut,
        QueueGauge::DnodeClientOut,
        QueueGauge::PeerIn,
        QueueGauge::PeerOut,
        QueueGauge::RemotePeerIn,
        QueueGauge::RemotePeerOut,
    ] {
        s.record_queue_len(g, 4);
    }
    let _ = s.snapshot();
}

#[test]
fn record_payload_size() {
    let s = fresh();
    s.record_payload_size(2048);
    assert!(s.snapshot().payload_size.max >= 2048);
}

#[test]
fn server_incr_and_decr_helpers() {
    // server_incr (+1) and server_decr (-1) wrap server_incr_by.
    let s = fresh();
    s.server_incr(ServerField::ReadRequests);
    s.server_incr(ServerField::ReadRequests);
    assert_eq!(s.server_get(ServerField::ReadRequests), 2);
    s.server_set(ServerField::InQueue, 3);
    s.server_decr(ServerField::InQueue);
    assert_eq!(s.server_get(ServerField::InQueue), 2);
}

#[test]
fn pool_incr_decr_set_get() {
    let s = fresh();
    s.pool_incr(PoolField::ClientEof);
    s.pool_incr(PoolField::ClientEof);
    assert_eq!(s.pool_get(PoolField::ClientEof), 2);
    s.pool_set(PoolField::ClientConnections, 5);
    s.pool_decr(PoolField::ClientConnections);
    assert_eq!(s.pool_get(PoolField::ClientConnections), 4);
}

#[test]
fn reset_histograms_clears_observations() {
    // reset_histograms zeroes every histogram channel.
    let s = fresh();
    s.record_latency(Latency::Request, 50);
    s.record_payload_size(100);
    s.record_queue_wait(QueueWait::Server, 7);
    s.record_queue_len(QueueGauge::ClientOut, 9);
    s.reset_histograms();
    assert_eq!(s.snapshot().latency.max, 0);
}

#[test]
fn queue_p99_overflow_is_suppressed() {
    // An overflowing queue histogram publishes p99 == 0 rather than
    // the OVERFLOW_SENTINEL (u64::MAX). A single u64::MAX observation
    // lands in the overflow bucket.
    let s = fresh();
    s.record_queue_len(QueueGauge::ClientOut, u64::MAX);
    let snap = s.snapshot();
    assert_eq!(snap.client_out_queue_p99, 0);
}

#[test]
fn queue_p99_non_overflow_publishes_value() {
    // A normal observation publishes a non-sentinel p99.
    let s = fresh();
    s.record_queue_len(QueueGauge::ServerIn, 5);
    let snap = s.snapshot();
    assert_ne!(snap.server_in_queue_p99, u64::MAX);
}

#[tokio::test]
async fn aggregator_publishes_one_snapshot_then_cancels() {
    // The aggregator ticks once, publishes the snapshot to the sink,
    // and returns cleanly when the cancellation token fires. A short
    // interval keeps the test well under the 100ms unit budget; the
    // histogram-reset cadence is short so the reset branch runs.
    let stats = Arc::new(fresh());
    stats.pool_incr(PoolField::StatsCount);
    let sink = Arc::new(Mutex::new(Snapshot::default()));
    let token = CancellationToken::new();
    let agg = Aggregator::new(
        Arc::clone(&stats),
        Arc::clone(&sink),
        Duration::from_millis(1),
        Duration::from_millis(1),
    );
    let cancel = token.clone();
    let handle = tokio::spawn(async move { agg.run(cancel).await });
    // Give the loop time to tick and publish at least once.
    tokio::time::sleep(Duration::from_millis(10)).await;
    token.cancel();
    handle.await.expect("aggregator task joins cleanly");
    // The sink received a snapshot reflecting the counter write.
    let published = sink.lock().clone();
    assert_eq!(
        published.pool.metrics[PoolField::StatsCount.index()],
        1,
        "aggregator should have published the recorded counter"
    );
}

#[tokio::test]
async fn aggregator_cancels_before_first_tick() {
    // Cancelling immediately exercises the biased select's
    // cancellation arm without a tick.
    let stats = Arc::new(fresh());
    let sink = Arc::new(Mutex::new(Snapshot::default()));
    let token = CancellationToken::new();
    token.cancel();
    let agg = Aggregator::new(stats, sink, Duration::from_mins(1), Duration::from_mins(5));
    // Already-cancelled token: run returns promptly.
    agg.run(token).await;
}
