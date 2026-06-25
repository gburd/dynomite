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
mod failure;
mod histogram;
mod numeric;
mod prometheus;
mod rest;
mod snapshot;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub use crate::stats::codec::{
    MetricSpec, PoolField, ServerField, StatsMetricType, POOL_CODEC, SERVER_CODEC,
};
pub use crate::stats::failure::{
    FailureMetrics, FailureSnapshot, NoTargetsEntry, PeerEntry, PeerStateEntry, PhiEntry,
    TimeoutEntry, TransitionEntry,
};
pub use crate::stats::histogram::{Histogram, BUCKET_COUNT};
pub use crate::stats::prometheus::render_prometheus;
pub use crate::stats::rest::{
    ClusterInfoProvider, RingProvider, StatsServer, MAX_HEADERS, MAX_REQUEST_BYTES,
};
pub use crate::stats::snapshot::{
    describe_stats, HistogramSummary, PeerStats, PoolStats, ServerStats, ServiceInfo, Snapshot,
};

/// Live, mutable counters and histograms for a single engine instance.
///
/// `Stats` is the writer side; readers consume frozen [`Snapshot`]
/// values produced by [`Stats::snapshot`].
///
/// # Examples
///
/// ```
/// use dynomite::stats::{PoolStats, ServerStats, ServiceInfo, Stats};
/// let stats = Stats::new(
///     ServiceInfo::default(),
///     PoolStats::new("dyn_o_mite"),
///     ServerStats::new("redis"),
/// );
/// assert_eq!(stats.snapshot().pool.name, "dyn_o_mite");
/// ```
#[derive(Debug)]
pub struct Stats {
    inner: Arc<Mutex<StatsInner>>,
    failure: Arc<FailureMetrics>,
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
///
/// # Examples
///
/// ```
/// use dynomite::stats::Latency;
/// assert_ne!(Latency::Request, Latency::Server);
/// assert_eq!(Latency::Request, Latency::Request);
/// // The variant set is small and copy-able.
/// let copied = Latency::CrossRegion;
/// assert_eq!(copied, Latency::CrossRegion);
/// ```
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
///
/// # Examples
///
/// ```
/// use dynomite::stats::QueueWait;
/// assert_ne!(QueueWait::CrossRegion, QueueWait::CrossZone);
/// assert_ne!(QueueWait::CrossZone, QueueWait::Server);
/// // Variants implement Copy, so a binding survives a move.
/// let original = QueueWait::Server;
/// let copy = original;
/// assert_eq!(original, copy);
/// ```
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
///
/// # Examples
///
/// ```
/// use dynomite::stats::QueueGauge;
/// // Each variant is distinct so the dispatch in `record_queue_len`
/// // routes to a unique histogram.
/// let all = [
///     QueueGauge::ClientOut,
///     QueueGauge::ServerIn,
///     QueueGauge::ServerOut,
///     QueueGauge::DnodeClientOut,
///     QueueGauge::PeerIn,
///     QueueGauge::PeerOut,
///     QueueGauge::RemotePeerIn,
///     QueueGauge::RemotePeerOut,
/// ];
/// for (i, lhs) in all.iter().enumerate() {
///     for rhs in &all[i + 1..] {
///         assert_ne!(lhs, rhs);
///     }
/// }
/// ```
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
            failure: Arc::new(FailureMetrics::new()),
            started: Instant::now(),
        }
    }

    /// Borrow the failure-cause metrics handle.
    ///
    /// The dispatcher and the gossip handler clone this `Arc`
    /// so they can record per-cause errors and per-peer state
    /// transitions. The handle is created at construction time
    /// and lives for the lifetime of the [`Stats`] value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerStats, ServiceInfo, Stats};
    /// let s = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// let m = s.failure_metrics();
    /// assert!(m.snapshot().is_empty());
    /// ```
    #[must_use]
    pub fn failure_metrics(&self) -> Arc<FailureMetrics> {
        self.failure.clone()
    }

    /// Record a latency observation in the matching histogram.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{Latency, PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.record_latency(Latency::Request, 100);
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.record_payload_size(2048);
    /// ```
    pub fn record_payload_size(&self, value: u64) {
        self.inner.lock().payload_size.record(value);
    }

    /// Record a queue wait time observation.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, QueueWait, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.record_queue_wait(QueueWait::Server, 12);
    /// ```
    pub fn record_queue_wait(&self, channel: QueueWait, value: u64) {
        let mut inner = self.inner.lock();
        match channel {
            QueueWait::CrossRegion => inner.cross_region_queue_wait.record(value),
            QueueWait::CrossZone => inner.cross_zone_queue_wait.record(value),
            QueueWait::Server => inner.server_queue_wait.record(value),
        }
    }

    /// Record a queue-length sample.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, QueueGauge, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.record_queue_len(QueueGauge::ClientOut, 4);
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolField, PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.pool_incr(PoolField::ClientEof);
    /// assert_eq!(stats.pool_get(PoolField::ClientEof), 1);
    /// ```
    pub fn pool_incr(&self, field: PoolField) {
        self.pool_incr_by(field, 1);
    }

    /// Decrement a pool gauge by one.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolField, PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.pool_set(PoolField::ClientConnections, 5);
    /// stats.pool_decr(PoolField::ClientConnections);
    /// assert_eq!(stats.pool_get(PoolField::ClientConnections), 4);
    /// ```
    pub fn pool_decr(&self, field: PoolField) {
        self.pool_incr_by(field, -1);
    }

    /// Add `delta` to a pool counter or gauge.
    ///
    /// Wraps on overflow. Counters are 64-bit signed and never reach
    /// the wrap
    /// boundary under realistic workloads.
    pub fn pool_incr_by(&self, field: PoolField, delta: i64) {
        let mut inner = self.inner.lock();
        let slot = &mut inner.pool.metrics[field.index()];
        *slot = slot.wrapping_add(delta);
    }

    /// Set a pool gauge or timestamp to an absolute value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolField, PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.pool_set(PoolField::PeerEjectedAt, 1_700_000_000);
    /// assert_eq!(stats.pool_get(PoolField::PeerEjectedAt), 1_700_000_000);
    /// ```
    pub fn pool_set(&self, field: PoolField, value: i64) {
        self.inner.lock().pool.metrics[field.index()] = value;
    }

    /// Read the current value of a pool metric.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolField, PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// assert_eq!(stats.pool_get(PoolField::ClientEof), 0);
    /// ```
    pub fn pool_get(&self, field: PoolField) -> i64 {
        self.inner.lock().pool.metrics[field.index()]
    }

    /// Increment a server counter or gauge by one.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerField, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.server_incr(ServerField::ReadRequests);
    /// assert_eq!(stats.server_get(ServerField::ReadRequests), 1);
    /// ```
    pub fn server_incr(&self, field: ServerField) {
        self.server_incr_by(field, 1);
    }

    /// Decrement a server gauge by one.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerField, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.server_set(ServerField::InQueue, 3);
    /// stats.server_decr(ServerField::InQueue);
    /// assert_eq!(stats.server_get(ServerField::InQueue), 2);
    /// ```
    pub fn server_decr(&self, field: ServerField) {
        self.server_incr_by(field, -1);
    }

    /// Add `delta` to a server counter or gauge.
    ///
    /// Wraps on overflow. Counters are 64-bit signed and never reach
    /// the wrap
    /// boundary under realistic workloads.
    pub fn server_incr_by(&self, field: ServerField, delta: i64) {
        let mut inner = self.inner.lock();
        let slot = &mut inner.server.metrics[field.index()];
        *slot = slot.wrapping_add(delta);
    }

    /// Set a server gauge or timestamp to an absolute value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerField, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.server_set(ServerField::ServerEjectedAt, 1_700_000_000);
    /// assert_eq!(stats.server_get(ServerField::ServerEjectedAt), 1_700_000_000);
    /// ```
    pub fn server_set(&self, field: ServerField, value: i64) {
        self.inner.lock().server.metrics[field.index()] = value;
    }

    /// Read the current value of a server metric.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerField, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// assert_eq!(stats.server_get(ServerField::ReadRequests), 0);
    /// ```
    pub fn server_get(&self, field: ServerField) -> i64 {
        self.inner.lock().server.metrics[field.index()]
    }

    /// Set the resource usage gauges sampled
    /// once per aggregation cycle.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.set_resource_usage(0, 0, 0, 0, 0);
    /// assert_eq!(stats.snapshot().alloc_msgs, 0);
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// let snap = stats.snapshot();
    /// assert_eq!(snap.pool.name, "p");
    /// ```
    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock();
        let elapsed = self.started.elapsed();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
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
            failure: self.failure.snapshot(),
        }
    }

    /// Reset every histogram. This runs every
    /// five minutes from inside the aggregation loop.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{Latency, PoolStats, ServerStats, ServiceInfo, Stats};
    /// let stats = Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("p"),
    ///     ServerStats::new("s"),
    /// );
    /// stats.record_latency(Latency::Request, 50);
    /// stats.reset_histograms();
    /// assert_eq!(stats.snapshot().latency.max, 0);
    /// ```
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
/// overflowing. Percentile publishing is suppressed in the overflow
/// path so overflow values do not leak into the JSON output as
/// `u64::MAX`.
fn queue_p99(h: &Histogram) -> u64 {
    if h.is_overflowing() {
        0
    } else {
        h.percentile(0.99)
    }
}

/// Async aggregator handle: snapshots at a fixed interval into a
/// shared cell that the REST server reads from.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use std::time::Duration;
/// use dynomite::stats::{Aggregator, PoolStats, ServerStats, ServiceInfo, Snapshot, Stats};
/// use parking_lot::Mutex;
/// use tokio_util::sync::CancellationToken;
///
/// # async fn _example() {
/// let stats = Arc::new(Stats::new(
///     ServiceInfo::default(),
///     PoolStats::new("dyn_o_mite"),
///     ServerStats::new("redis"),
/// ));
/// let sink = Arc::new(Mutex::new(Snapshot::default()));
/// let token = CancellationToken::new();
/// let agg = Aggregator::new(stats, sink, Duration::from_secs(1), Duration::from_secs(300));
/// let _ = tokio::spawn({ let token = token.clone(); async move { agg.run(token).await } });
/// token.cancel();
/// # }
/// ```
pub struct Aggregator {
    stats: Arc<Stats>,
    sink: Arc<Mutex<Snapshot>>,
    interval: Duration,
    histogram_reset: Duration,
}

impl Aggregator {
    /// Create a new aggregator. The aggregation loop reads from
    /// `stats` and publishes to `sink` once every `interval`.
    /// Histograms are reset every `histogram_reset` elapsed time;
    /// the default cadence is five minutes.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use dynomite::stats::{Aggregator, PoolStats, ServerStats, ServiceInfo, Snapshot, Stats};
    /// use parking_lot::Mutex;
    ///
    /// let stats = Arc::new(Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("dyn_o_mite"),
    ///     ServerStats::new("redis"),
    /// ));
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let _agg = Aggregator::new(stats, sink, Duration::from_secs(1), Duration::from_secs(300));
    /// ```
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

    /// Run the aggregation loop until `cancel` is triggered. The future
    /// returns `()` after observing cancellation; callers that want a
    /// clean shutdown should clone the token and call
    /// [`CancellationToken::cancel`] on it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use dynomite::stats::{Aggregator, PoolStats, ServerStats, ServiceInfo, Snapshot, Stats};
    /// use parking_lot::Mutex;
    /// use tokio_util::sync::CancellationToken;
    ///
    /// # async fn _example() {
    /// let stats = Arc::new(Stats::new(
    ///     ServiceInfo::default(),
    ///     PoolStats::new("dyn_o_mite"),
    ///     ServerStats::new("redis"),
    /// ));
    /// let sink = Arc::new(Mutex::new(Snapshot::default()));
    /// let token = CancellationToken::new();
    /// let agg = Aggregator::new(stats, sink, Duration::from_secs(1), Duration::from_secs(300));
    /// let cancel = token.clone();
    /// let handle = tokio::spawn(async move { agg.run(cancel).await });
    /// token.cancel();
    /// let _ = handle.await;
    /// # }
    /// ```
    pub async fn run(self, cancel: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_reset = Instant::now();
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = ticker.tick() => {}
            }
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
