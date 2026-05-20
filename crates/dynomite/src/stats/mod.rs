//! Pool, server, and peer metrics with histograms and a JSON snapshot.
//!
//! The stats subsystem is split into small modules:
//!
//! * [`Histogram`] - Cassandra-style estimated histogram.
//! * [`PoolField`] / [`ServerField`] - typed metric handles.
//! * [`Snapshot`] - aggregate value rendered to JSON.
//! * [`StatsServer`] - REST endpoint serving the latest snapshot.
//!
//! [`Stats`] glues the pieces together: a writer accumulates counters,
//! gauges, and histogram observations; a periodic aggregator publishes
//! a fresh [`Snapshot`] that the REST endpoint serves.

mod codec;
mod histogram;
mod numeric;
mod rest;
mod snapshot;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::time::{Duration, Instant};

pub use crate::stats::codec::{
    MetricSpec, PoolField, ServerField, StatsMetricType, POOL_CODEC, SERVER_CODEC,
};
pub use crate::stats::histogram::{Histogram, BUCKET_COUNT};
pub use crate::stats::rest::StatsServer;
pub use crate::stats::snapshot::{
    describe_stats, HistogramSummary, PeerStats, PoolStats, ServerStats, ServiceInfo, Snapshot,
};

/// Live, mutable counters and histograms for a single engine instance.
///
/// `Stats` is the writer side; readers consume frozen [`Snapshot`]
/// values produced by [`Stats::snapshot`].
#[derive(Debug)]
pub struct Stats {
    inner: Arc<Mutex<StatsInner>>,
    started: Instant,
}

#[derive(Debug)]
struct StatsInner {
    info: ServiceInfo,
    pool: PoolStats,
    server: ServerStats,
    latency: Histogram,
    payload_size: Histogram,
    cross_region_latency: Histogram,
    cross_zone_latency: Histogram,
    server_latency: Histogram,
    cross_region_queue_wait: Histogram,
    cross_zone_queue_wait: Histogram,
    server_queue_wait: Histogram,
    client_out_queue: Histogram,
    server_in_queue: Histogram,
    server_out_queue: Histogram,
    dnode_client_out_queue: Histogram,
    peer_in_queue: Histogram,
    peer_out_queue: Histogram,
    remote_peer_in_queue: Histogram,
    remote_peer_out_queue: Histogram,
    alloc_msgs: i64,
    free_msgs: i64,
    alloc_mbufs: i64,
    free_mbufs: i64,
    dyn_memory: i64,
}

impl StatsInner {
    fn new(info: ServiceInfo, pool: PoolStats, server: ServerStats) -> Self {
        Self {
            info,
            pool,
            server,
            latency: Histogram::new(),
            payload_size: Histogram::new(),
            cross_region_latency: Histogram::new(),
            cross_zone_latency: Histogram::new(),
            server_latency: Histogram::new(),
            cross_region_queue_wait: Histogram::new(),
            cross_zone_queue_wait: Histogram::new(),
            server_queue_wait: Histogram::new(),
            client_out_queue: Histogram::new(),
            server_in_queue: Histogram::new(),
            server_out_queue: Histogram::new(),
            dnode_client_out_queue: Histogram::new(),
            peer_in_queue: Histogram::new(),
            peer_out_queue: Histogram::new(),
            remote_peer_in_queue: Histogram::new(),
            remote_peer_out_queue: Histogram::new(),
            alloc_msgs: 0,
            free_msgs: 0,
            alloc_mbufs: 0,
            free_mbufs: 0,
            dyn_memory: 0,
        }
    }
}

/// Channels used to mutate histogram observations.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Latency {
    /// Top-level request latency.
    Request,
    /// Cross-region peer round-trip time.
    CrossRegion,
    /// Cross-zone peer latency.
    CrossZone,
    /// Backing-server response latency.
    Server,
}

/// Channels used for queue-wait-time observations.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum QueueWait {
    /// Cross-region queue wait time.
    CrossRegion,
    /// Cross-zone queue wait time.
    CrossZone,
    /// Backing-server queue wait time.
    Server,
}

/// Channels used for queue-length observations (observed at sample
/// time, not events).
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum QueueGauge {
    /// Client out-queue length.
    ClientOut,
    /// Server in-queue length.
    ServerIn,
    /// Server out-queue length.
    ServerOut,
    /// Dnode client out-queue length.
    DnodeClientOut,
    /// Local-DC peer in-queue length.
    PeerIn,
    /// Local-DC peer out-queue length.
    PeerOut,
    /// Remote-DC peer in-queue length.
    RemotePeerIn,
    /// Remote-DC peer out-queue length.
    RemotePeerOut,
}

impl Stats {
    /// Construct a new `Stats` with empty counters and histograms.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerStats, ServiceInfo, Stats};
    ///
    /// let info = ServiceInfo {
    ///     source: "node-a".into(),
    ///     version: "0.0.1".into(),
    ///     rack: "r1".into(),
    ///     dc: "dc1".into(),
    /// };
    /// let stats = Stats::new(
    ///     info,
    ///     PoolStats::new("dyn_o_mite"),
    ///     ServerStats::new("redis_local"),
    /// );
    /// let snap = stats.snapshot();
    /// assert_eq!(snap.pool.name, "dyn_o_mite");
    /// ```
    pub fn new(info: ServiceInfo, pool: PoolStats, server: ServerStats) -> Self {
        Self {
            inner: Arc::new(Mutex::new(StatsInner::new(info, pool, server))),
            started: Instant::now(),
        }
    }

    /// Record a latency observation in the matching histogram.
    pub fn record_latency(&self, channel: Latency, value: u64) {
        let mut inner = self.inner.lock();
        match channel {
            Latency::Request => inner.latency.record(value),
            Latency::CrossRegion => inner.cross_region_latency.record(value),
            Latency::CrossZone => inner.cross_zone_latency.record(value),
            Latency::Server => inner.server_latency.record(value),
        }
    }

    /// Record a payload-size observation.
    pub fn record_payload_size(&self, value: u64) {
        self.inner.lock().payload_size.record(value);
    }

    /// Record a queue wait time observation.
    pub fn record_queue_wait(&self, channel: QueueWait, value: u64) {
        let mut inner = self.inner.lock();
        match channel {
            QueueWait::CrossRegion => inner.cross_region_queue_wait.record(value),
            QueueWait::CrossZone => inner.cross_zone_queue_wait.record(value),
            QueueWait::Server => inner.server_queue_wait.record(value),
        }
    }

    /// Record a queue-length sample.
    pub fn record_queue_len(&self, channel: QueueGauge, value: u64) {
        let mut inner = self.inner.lock();
        match channel {
            QueueGauge::ClientOut => inner.client_out_queue.record(value),
            QueueGauge::ServerIn => inner.server_in_queue.record(value),
            QueueGauge::ServerOut => inner.server_out_queue.record(value),
            QueueGauge::DnodeClientOut => inner.dnode_client_out_queue.record(value),
            QueueGauge::PeerIn => inner.peer_in_queue.record(value),
            QueueGauge::PeerOut => inner.peer_out_queue.record(value),
            QueueGauge::RemotePeerIn => inner.remote_peer_in_queue.record(value),
            QueueGauge::RemotePeerOut => inner.remote_peer_out_queue.record(value),
        }
    }

    /// Increment a pool counter or gauge by one.
    pub fn pool_incr(&self, field: PoolField) {
        self.pool_incr_by(field, 1);
    }

    /// Decrement a pool gauge by one.
    pub fn pool_decr(&self, field: PoolField) {
        self.pool_incr_by(field, -1);
    }

    /// Add `delta` to a pool counter or gauge.
    pub fn pool_incr_by(&self, field: PoolField, delta: i64) {
        let mut inner = self.inner.lock();
        let slot = &mut inner.pool.metrics[field.index()];
        *slot = slot.saturating_add(delta);
    }

    /// Set a pool gauge or timestamp to an absolute value.
    pub fn pool_set(&self, field: PoolField, value: i64) {
        self.inner.lock().pool.metrics[field.index()] = value;
    }

    /// Read the current value of a pool metric.
    pub fn pool_get(&self, field: PoolField) -> i64 {
        self.inner.lock().pool.metrics[field.index()]
    }

    /// Increment a server counter or gauge by one.
    pub fn server_incr(&self, field: ServerField) {
        self.server_incr_by(field, 1);
    }

    /// Decrement a server gauge by one.
    pub fn server_decr(&self, field: ServerField) {
        self.server_incr_by(field, -1);
    }

    /// Add `delta` to a server counter or gauge.
    pub fn server_incr_by(&self, field: ServerField, delta: i64) {
        let mut inner = self.inner.lock();
        let slot = &mut inner.server.metrics[field.index()];
        *slot = slot.saturating_add(delta);
    }

    /// Set a server gauge or timestamp to an absolute value.
    pub fn server_set(&self, field: ServerField, value: i64) {
        self.inner.lock().server.metrics[field.index()] = value;
    }

    /// Read the current value of a server metric.
    pub fn server_get(&self, field: ServerField) -> i64 {
        self.inner.lock().server.metrics[field.index()]
    }

    /// Set the resource usage gauges that the C reference samples once
    /// per aggregation cycle.
    pub fn set_resource_usage(
        &self,
        alloc_msgs: i64,
        free_msgs: i64,
        alloc_mbufs: i64,
        free_mbufs: i64,
        dyn_memory: i64,
    ) {
        let mut inner = self.inner.lock();
        inner.alloc_msgs = alloc_msgs;
        inner.free_msgs = free_msgs;
        inner.alloc_mbufs = alloc_mbufs;
        inner.free_mbufs = free_mbufs;
        inner.dyn_memory = dyn_memory;
    }

    /// Build an immutable snapshot of every counter, gauge, and
    /// histogram quantile at the current instant.
    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock();
        let elapsed = self.started.elapsed();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Snapshot {
            info: inner.info.clone(),
            uptime: i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX),
            timestamp: i64::try_from(timestamp).unwrap_or(i64::MAX),
            latency: HistogramSummary::from_histogram(&inner.latency),
            payload_size: HistogramSummary::from_histogram(&inner.payload_size),
            cross_region_latency: HistogramSummary::from_histogram(&inner.cross_region_latency),
            cross_zone_latency: HistogramSummary::from_histogram(&inner.cross_zone_latency),
            server_latency: HistogramSummary::from_histogram(&inner.server_latency),
            cross_region_queue_wait: HistogramSummary::from_histogram(
                &inner.cross_region_queue_wait,
            ),
            cross_zone_queue_wait: HistogramSummary::from_histogram(&inner.cross_zone_queue_wait),
            server_queue_wait: HistogramSummary::from_histogram(&inner.server_queue_wait),
            client_out_queue_p99: queue_p99(&inner.client_out_queue),
            server_in_queue_p99: queue_p99(&inner.server_in_queue),
            server_out_queue_p99: queue_p99(&inner.server_out_queue),
            dnode_client_out_queue_p99: queue_p99(&inner.dnode_client_out_queue),
            peer_in_queue_p99: queue_p99(&inner.peer_in_queue),
            peer_out_queue_p99: queue_p99(&inner.peer_out_queue),
            remote_peer_in_queue_p99: queue_p99(&inner.remote_peer_in_queue),
            remote_peer_out_queue_p99: queue_p99(&inner.remote_peer_out_queue),
            alloc_msgs: inner.alloc_msgs,
            free_msgs: inner.free_msgs,
            alloc_mbufs: inner.alloc_mbufs,
            free_mbufs: inner.free_mbufs,
            dyn_memory: inner.dyn_memory,
            pool: inner.pool.clone(),
            server: inner.server.clone(),
        }
    }

    /// Reset every histogram. The C reference does this every five
    /// minutes from inside the aggregation loop.
    pub fn reset_histograms(&self) {
        let mut inner = self.inner.lock();
        inner.latency.reset();
        inner.payload_size.reset();
        inner.cross_region_latency.reset();
        inner.cross_zone_latency.reset();
        inner.server_latency.reset();
        inner.cross_region_queue_wait.reset();
        inner.cross_zone_queue_wait.reset();
        inner.server_queue_wait.reset();
        inner.client_out_queue.reset();
        inner.server_in_queue.reset();
        inner.server_out_queue.reset();
        inner.dnode_client_out_queue.reset();
        inner.peer_in_queue.reset();
        inner.peer_out_queue.reset();
        inner.remote_peer_in_queue.reset();
        inner.remote_peer_out_queue.reset();
    }
}

/// Returns the queue p99 from `h`, or `0` when the histogram is
/// overflowing. The reference implementation suppresses percentile
/// publishing in the overflow path; mirroring that keeps overflow
/// values from leaking into the JSON output as `u64::MAX`.
fn queue_p99(h: &Histogram) -> u64 {
    if h.is_overflowing() {
        0
    } else {
        h.percentile(0.99)
    }
}

/// Async aggregator handle: snapshots at a fixed interval into a
/// shared cell that the REST server reads from.
pub struct Aggregator {
    stats: Arc<Stats>,
    sink: Arc<Mutex<Snapshot>>,
    interval: Duration,
    histogram_reset: Duration,
}

impl Aggregator {
    /// Create a new aggregator. The aggregation loop reads from
    /// `stats` and publishes to `sink` once every `interval`.
    /// Histograms are reset every `histogram_reset` elapsed time, the
    /// same five-minute cadence the C reference uses by default.
    pub fn new(
        stats: Arc<Stats>,
        sink: Arc<Mutex<Snapshot>>,
        interval: Duration,
        histogram_reset: Duration,
    ) -> Self {
        Self {
            stats,
            sink,
            interval,
            histogram_reset,
        }
    }

    /// Run the aggregation loop. The future never returns until
    /// cancelled.
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_reset = Instant::now();
        loop {
            ticker.tick().await;
            let snap = self.stats.snapshot();
            *self.sink.lock() = snap;
            if last_reset.elapsed() >= self.histogram_reset {
                self.stats.reset_histograms();
                last_reset = Instant::now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> Stats {
        Stats::new(
            ServiceInfo {
                source: "node".into(),
                version: "0.0.1".into(),
                rack: "r".into(),
                dc: "d".into(),
            },
            PoolStats::new("dyn_o_mite"),
            ServerStats::new("redis"),
        )
    }

    #[test]
    fn counter_incr_and_get() {
        let s = fresh();
        s.pool_incr(PoolField::ClientEof);
        s.pool_incr(PoolField::ClientEof);
        assert_eq!(s.pool_get(PoolField::ClientEof), 2);
    }

    #[test]
    fn gauge_set_and_decrement() {
        let s = fresh();
        s.pool_set(PoolField::ClientConnections, 5);
        s.pool_decr(PoolField::ClientConnections);
        assert_eq!(s.pool_get(PoolField::ClientConnections), 4);
    }

    #[test]
    fn server_metric_round_trip() {
        let s = fresh();
        s.server_incr_by(ServerField::ReadRequests, 42);
        s.server_set(ServerField::ServerEjectedAt, 1_700_000_000);
        assert_eq!(s.server_get(ServerField::ReadRequests), 42);
        assert_eq!(s.server_get(ServerField::ServerEjectedAt), 1_700_000_000);
    }

    #[test]
    fn snapshot_reflects_writes() {
        let s = fresh();
        s.pool_incr(PoolField::StatsCount);
        s.record_latency(Latency::Request, 100);
        s.record_payload_size(2048);
        let snap = s.snapshot();
        assert_eq!(snap.pool.metrics[PoolField::StatsCount.index()], 1);
        assert_eq!(snap.latency.max, 100);
        assert!(snap.payload_size.max >= 2048);
    }

    #[test]
    fn metric_indexes_have_canonical_order() {
        for (i, variant) in PoolField::ALL.iter().enumerate() {
            assert_eq!(variant.index(), i);
        }
    }
}
