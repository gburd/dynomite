//! Snapshot type and JSON serialization for the stats subsystem.

use std::fmt::{self, Write};

use crate::stats::codec::{StatsMetricType, POOL_CODEC, SERVER_CODEC};
use crate::stats::failure::FailureSnapshot;
use crate::stats::histogram::Histogram;

/// Engine-wide identifying strings included in every snapshot.
///
/// # Examples
///
/// ```
/// use dynomite::stats::ServiceInfo;
/// let info = ServiceInfo {
///     source: "node-a".into(),
///     version: "0.0.1".into(),
///     rack: "r1".into(),
///     dc: "dc1".into(),
/// };
/// assert_eq!(info.source, "node-a");
/// ```
#[derive(Clone, Debug, Default)]
pub struct ServiceInfo {
    /// Hostname or address of the local node.
    pub source: String,
    /// Engine version reported as `version` in the JSON.
    pub version: String,
    /// Logical rack of the local node.
    pub rack: String,
    /// Logical datacenter of the local node.
    pub dc: String,
}

/// Pre-computed quantile summary derived from a [`Histogram`].
///
/// # Examples
///
/// ```
/// use dynomite::stats::HistogramSummary;
/// let s = HistogramSummary::default();
/// assert_eq!(s.max, 0);
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct HistogramSummary {
    /// Maximum observation in the window.
    pub max: u64,
    /// 99.9th percentile.
    pub p999: u64,
    /// 99th percentile.
    pub p99: u64,
    /// 95th percentile.
    pub p95: u64,
    /// Arithmetic mean of all observations.
    pub mean: u64,
}

impl HistogramSummary {
    /// Compute the standard quantile summary from a histogram.
    ///
    /// When the histogram is in overflow (a value larger than the
    /// largest bucket offset has been recorded), the summary is
    /// zeroed: percentiles are not published in that state.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{Histogram, HistogramSummary};
    /// let mut h = Histogram::new();
    /// for v in 0..100 { h.record(v); }
    /// let s = HistogramSummary::from_histogram(&h);
    /// assert!(s.p99 >= s.p95);
    /// ```
    pub fn from_histogram(h: &Histogram) -> Self {
        if h.is_overflowing() {
            return Self::default();
        }
        let mean_f = h.mean();
        let mean = if mean_f.is_finite() && mean_f > 0.0 {
            // Round mean up to the nearest integer.
            ceil_f64_to_u64(mean_f)
        } else {
            0
        };
        Self {
            max: h.max(),
            p999: h.percentile(0.999),
            p99: h.percentile(0.99),
            p95: h.percentile(0.95),
            mean,
        }
    }
}

/// Computes `ceil(x)` for non-negative finite `f64` values without an
/// `as` cast, returning `u64::MAX` on overflow.
fn ceil_f64_to_u64(x: f64) -> u64 {
    if !x.is_finite() || x <= 0.0 {
        return 0;
    }
    let ceil = x.ceil();
    let bits = ceil.to_bits();
    let exp = u32::try_from((bits >> 52) & 0x7FF).expect("11-bit field");
    let mant = bits & ((1u64 << 52) - 1);
    if exp < 1023 {
        // 0.0 < ceil < 1.0 cannot occur after ceil(); fall back safely.
        return 1;
    }
    let unbiased = exp - 1023;
    if unbiased >= 64 {
        return u64::MAX;
    }
    let m = (1u64 << 52) | mant;
    if unbiased >= 52 {
        let shift = unbiased - 52;
        m.checked_shl(shift).unwrap_or(u64::MAX)
    } else {
        m >> (52 - unbiased)
    }
}

/// Per-pool collected metrics.
///
/// # Examples
///
/// ```
/// use dynomite::stats::PoolStats;
/// let pool = PoolStats::new("dyn_o_mite");
/// assert_eq!(pool.name, "dyn_o_mite");
/// assert!(!pool.metrics.is_empty());
/// ```
#[derive(Clone, Debug)]
pub struct PoolStats {
    /// Pool name as declared in the YAML configuration.
    pub name: String,
    /// Counter/gauge values, indexed by `PoolField::index()`.
    pub metrics: Vec<i64>,
}

impl Default for PoolStats {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl PoolStats {
    /// Construct a fresh `PoolStats` with all metrics zeroed.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::PoolStats;
    /// let p = PoolStats::new("dyn_o_mite");
    /// assert!(p.metrics.iter().all(|&v| v == 0));
    /// ```
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            metrics: vec![0; POOL_CODEC.len()],
        }
    }
}

/// Per-datastore-server collected metrics.
///
/// # Examples
///
/// ```
/// use dynomite::stats::ServerStats;
/// let s = ServerStats::new("redis_local");
/// assert_eq!(s.name, "redis_local");
/// ```
#[derive(Clone, Debug)]
pub struct ServerStats {
    /// Server name (the host name from the YAML).
    pub name: String,
    /// Counter/gauge values, indexed by `ServerField::index()`.
    pub metrics: Vec<i64>,
}

impl Default for ServerStats {
    fn default() -> Self {
        Self::new(String::new())
    }
}

impl ServerStats {
    /// Construct a fresh `ServerStats` with all metrics zeroed.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::ServerStats;
    /// let s = ServerStats::new("redis_local");
    /// assert!(s.metrics.iter().all(|&v| v == 0));
    /// ```
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            metrics: vec![0; SERVER_CODEC.len()],
        }
    }
}

/// Per-peer collected metrics. Mirrors `ServerStats` for cluster peers.
///
/// # Examples
///
/// ```
/// use dynomite::stats::PeerStats;
/// let p = PeerStats::new("peer-a");
/// assert_eq!(p.name, "peer-a");
/// ```
#[derive(Clone, Debug)]
pub struct PeerStats {
    /// Peer name.
    pub name: String,
    /// Counter/gauge values indexed by `ServerField::index()`.
    pub metrics: Vec<i64>,
}

impl PeerStats {
    /// Construct a fresh `PeerStats` with all metrics zeroed.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::PeerStats;
    /// let p = PeerStats::new("peer-a");
    /// assert!(p.metrics.iter().all(|&v| v == 0));
    /// ```
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            metrics: vec![0; SERVER_CODEC.len()],
        }
    }
}

/// Aggregate snapshot of the stats subsystem at a point in time.
///
/// This is the value rendered by [`Snapshot::to_json`] and exposed
/// through the REST endpoint. It is `Send + Sync` and cheap to clone.
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    /// Static identification strings.
    pub info: ServiceInfo,
    /// Seconds since the engine started.
    pub uptime: i64,
    /// Wall-clock seconds since UNIX epoch.
    pub timestamp: i64,
    /// Latency histogram summary.
    pub latency: HistogramSummary,
    /// Payload size histogram summary.
    pub payload_size: HistogramSummary,
    /// Cross-region RTT histogram summary.
    pub cross_region_latency: HistogramSummary,
    /// Cross-zone latency histogram summary.
    pub cross_zone_latency: HistogramSummary,
    /// Per-server latency summary.
    pub server_latency: HistogramSummary,
    /// Cross-region queue wait time summary.
    pub cross_region_queue_wait: HistogramSummary,
    /// Cross-zone queue wait time summary.
    pub cross_zone_queue_wait: HistogramSummary,
    /// Server queue wait time summary.
    pub server_queue_wait: HistogramSummary,
    /// 99th percentile of the client outbound queue length.
    pub client_out_queue_p99: u64,
    /// 99th percentile of the server inbound queue length.
    pub server_in_queue_p99: u64,
    /// 99th percentile of the server outbound queue length.
    pub server_out_queue_p99: u64,
    /// 99th percentile of the dnode client outbound queue length.
    pub dnode_client_out_queue_p99: u64,
    /// 99th percentile of the local-DC peer inbound queue length.
    pub peer_in_queue_p99: u64,
    /// 99th percentile of the local-DC peer outbound queue length.
    pub peer_out_queue_p99: u64,
    /// 99th percentile of the remote-DC peer inbound queue length.
    pub remote_peer_in_queue_p99: u64,
    /// 99th percentile of the remote-DC peer outbound queue length.
    pub remote_peer_out_queue_p99: u64,
    /// Number of message structs allocated.
    pub alloc_msgs: i64,
    /// Number of message structs on the free list.
    pub free_msgs: i64,
    /// Number of mbuf chunks allocated.
    pub alloc_mbufs: i64,
    /// Number of mbuf chunks on the free list.
    pub free_mbufs: i64,
    /// Resident set size in bytes.
    pub dyn_memory: i64,
    /// Aggregated pool counters.
    pub pool: PoolStats,
    /// Aggregated server counters.
    pub server: ServerStats,
    /// Aggregated failure-cause metrics.
    pub failure: FailureSnapshot,
}

impl Snapshot {
    /// Serialize the snapshot to a JSON string.
    ///
    /// The layout is a single JSON object with flat top-level fields
    /// followed by a nested pool object containing the per-pool metric
    /// counters and a per-server sub-object.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{Snapshot, PoolStats, ServerStats};
    ///
    /// let mut snap = Snapshot::default();
    /// snap.pool = PoolStats::new("dyn_o_mite");
    /// snap.server = ServerStats::new("redis_local");
    /// let s = snap.to_json();
    /// assert!(s.starts_with('{'));
    /// assert!(s.contains("\"dyn_o_mite\""));
    /// ```
    pub fn to_json(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out)
            .expect("writing to a String never fails");
        out
    }

    /// Render the snapshot as JSON into any [`fmt::Write`] sink.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Snapshot;
    /// let snap = Snapshot::default();
    /// let mut s = String::new();
    /// snap.write_json(&mut s).expect("writing into String never fails");
    /// assert!(s.starts_with('{'));
    /// ```
    pub fn write_json<W: Write>(&self, w: &mut W) -> fmt::Result {
        w.write_char('{')?;
        self.write_header(w)?;
        self.write_pool(w)?;
        w.write_char('}')?;
        Ok(())
    }

    fn write_header<W: Write>(&self, w: &mut W) -> fmt::Result {
        write_string(w, "service", "dynomite")?;
        write_string(w, "source", &self.info.source)?;
        write_string(w, "version", &self.info.version)?;
        write_num(w, "uptime", self.uptime)?;
        write_num(w, "timestamp", self.timestamp)?;
        write_string(w, "rack", &self.info.rack)?;
        write_string(w, "dc", &self.info.dc)?;

        write_num_u64(w, "latency_max", self.latency.max)?;
        write_num_u64(w, "latency_999th", self.latency.p999)?;
        write_num_u64(w, "latency_99th", self.latency.p99)?;
        write_num_u64(w, "latency_95th", self.latency.p95)?;
        write_num_u64(w, "latency_mean", self.latency.mean)?;

        write_num_u64(w, "payload_size_max", self.payload_size.max)?;
        write_num_u64(w, "payload_size_999th", self.payload_size.p999)?;
        write_num_u64(w, "payload_size_99th", self.payload_size.p99)?;
        write_num_u64(w, "payload_size_95th", self.payload_size.p95)?;
        write_num_u64(w, "payload_size_mean", self.payload_size.mean)?;

        self.write_cross_region_latency(w)?;
        self.write_queue_wait(w)?;
        self.write_queue_p99s(w)?;
        self.write_resource_usage(w)?;
        Ok(())
    }

    fn write_cross_region_latency<W: Write>(&self, w: &mut W) -> fmt::Result {
        write_num_u64(
            w,
            "average_cross_region_rtt",
            self.cross_region_latency.mean,
        )?;
        write_num_u64(w, "99_cross_region_rtt", self.cross_region_latency.p99)?;
        write_num_u64(
            w,
            "average_cross_zone_latency",
            self.cross_zone_latency.mean,
        )?;
        write_num_u64(w, "99_cross_zone_latency", self.cross_zone_latency.p99)?;
        write_num_u64(w, "average_server_latency", self.server_latency.mean)?;
        write_num_u64(w, "99_server_latency", self.server_latency.p99)?;
        Ok(())
    }

    fn write_queue_wait<W: Write>(&self, w: &mut W) -> fmt::Result {
        write_num_u64(
            w,
            "average_cross_region_queue_wait",
            self.cross_region_queue_wait.mean,
        )?;
        write_num_u64(
            w,
            "99_cross_region_queue_wait",
            self.cross_region_queue_wait.p99,
        )?;
        write_num_u64(
            w,
            "average_cross_zone_queue_wait",
            self.cross_zone_queue_wait.mean,
        )?;
        write_num_u64(
            w,
            "99_cross_zone_queue_wait",
            self.cross_zone_queue_wait.p99,
        )?;
        write_num_u64(w, "average_server_queue_wait", self.server_queue_wait.mean)?;
        write_num_u64(w, "99_server_queue_wait", self.server_queue_wait.p99)?;
        Ok(())
    }

    fn write_queue_p99s<W: Write>(&self, w: &mut W) -> fmt::Result {
        write_num_u64(w, "client_out_queue_99", self.client_out_queue_p99)?;
        write_num_u64(w, "server_in_queue_99", self.server_in_queue_p99)?;
        write_num_u64(w, "server_out_queue_99", self.server_out_queue_p99)?;
        write_num_u64(
            w,
            "dnode_client_out_queue_99",
            self.dnode_client_out_queue_p99,
        )?;
        write_num_u64(w, "peer_in_queue_99", self.peer_in_queue_p99)?;
        write_num_u64(w, "peer_out_queue_99", self.peer_out_queue_p99)?;
        write_num_u64(
            w,
            "remote_peer_out_queue_99",
            self.remote_peer_out_queue_p99,
        )?;
        write_num_u64(w, "remote_peer_in_queue_99", self.remote_peer_in_queue_p99)?;
        Ok(())
    }

    fn write_resource_usage<W: Write>(&self, w: &mut W) -> fmt::Result {
        write_num(w, "alloc_msgs", self.alloc_msgs)?;
        write_num(w, "free_msgs", self.free_msgs)?;
        write_num(w, "alloc_mbufs", self.alloc_mbufs)?;
        write_num(w, "free_mbufs", self.free_mbufs)?;
        write_num(w, "dyn_memory", self.dyn_memory)?;
        Ok(())
    }

    fn write_pool<W: Write>(&self, w: &mut W) -> fmt::Result {
        write!(w, "\"{}\":{{", escape_str(&self.pool.name))?;
        for (i, spec) in POOL_CODEC.iter().enumerate() {
            if !is_visible_metric(spec.kind) {
                continue;
            }
            let value = self.pool.metrics.get(i).copied().unwrap_or(0);
            write_num(w, spec.name, value)?;
        }
        self.write_server(w)?;
        w.write_str("}")?;
        Ok(())
    }

    fn write_server<W: Write>(&self, w: &mut W) -> fmt::Result {
        write!(w, "\"{}\":{{", escape_str(&self.server.name))?;
        let server_visible: Vec<usize> = SERVER_CODEC
            .iter()
            .enumerate()
            .filter(|(_, s)| is_visible_metric(s.kind))
            .map(|(i, _)| i)
            .collect();
        for (j, idx) in server_visible.iter().copied().enumerate() {
            let spec = &SERVER_CODEC[idx];
            let value = self.server.metrics.get(idx).copied().unwrap_or(0);
            if j + 1 == server_visible.len() {
                write_num_no_comma(w, spec.name, value)?;
            } else {
                write_num(w, spec.name, value)?;
            }
        }
        w.write_str("}")?;
        Ok(())
    }
}

/// Whether a metric kind appears in the JSON output. Counters, gauges,
/// and timestamps are all rendered as numbers; invalid/string metric
/// kinds are omitted entirely.
fn is_visible_metric(kind: StatsMetricType) -> bool {
    matches!(
        kind,
        StatsMetricType::Counter | StatsMetricType::Gauge | StatsMetricType::Timestamp
    )
}

fn write_string<W: Write>(w: &mut W, key: &str, value: &str) -> fmt::Result {
    write!(w, "\"{}\":\"{}\",", escape_str(key), escape_str(value))
}

fn write_num<W: Write>(w: &mut W, key: &str, value: i64) -> fmt::Result {
    write!(w, "\"{}\":{value},", escape_str(key))
}

fn write_num_no_comma<W: Write>(w: &mut W, key: &str, value: i64) -> fmt::Result {
    write!(w, "\"{}\":{value}", escape_str(key))
}

fn write_num_u64<W: Write>(w: &mut W, key: &str, value: u64) -> fmt::Result {
    write!(w, "\"{}\":{value},", escape_str(key))
}

/// Minimal JSON string escaping. Backslashes, quotes, and control
/// characters below 0x20 are escaped; everything else passes through.
fn escape_str(s: &str) -> EscapedStr<'_> {
    EscapedStr(s)
}

struct EscapedStr<'a>(&'a str);

impl fmt::Display for EscapedStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for ch in self.0.chars() {
            match ch {
                '\\' => f.write_str("\\\\")?,
                '"' => f.write_str("\\\"")?,
                '\n' => f.write_str("\\n")?,
                '\r' => f.write_str("\\r")?,
                '\t' => f.write_str("\\t")?,
                c if (c as u32) < 0x20 => write!(f, "\\u{:04x}", c as u32)?,
                c => f.write_char(c)?,
            }
        }
        Ok(())
    }
}

/// Returns the human-readable description block printed by the `-D`
/// command-line flag.
///
/// # Examples
///
/// ```
/// let text = dynomite::stats::describe_stats();
/// assert!(text.contains("pool stats:"));
/// assert!(text.contains("server stats:"));
/// ```
pub fn describe_stats() -> String {
    let mut out = String::new();
    out.push_str("pool stats:\n");
    for spec in POOL_CODEC {
        let _ = writeln!(out, "  {:<20}\"{}\"", spec.name, spec.description);
    }
    out.push('\n');
    out.push_str("server stats:\n");
    for spec in SERVER_CODEC {
        let _ = writeln!(out, "  {:<20}\"{}\"", spec.name, spec.description);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ceil_helper_matches_known_values() {
        assert_eq!(ceil_f64_to_u64(0.0), 0);
        assert_eq!(ceil_f64_to_u64(1.0), 1);
        assert_eq!(ceil_f64_to_u64(1.5), 2);
        assert_eq!(ceil_f64_to_u64(2.0), 2);
        assert_eq!(ceil_f64_to_u64(99.99), 100);
        assert_eq!(ceil_f64_to_u64(f64::NAN), 0);
        assert_eq!(ceil_f64_to_u64(f64::INFINITY), 0);
        assert_eq!(ceil_f64_to_u64(-1.0), 0);
    }

    #[test]
    fn empty_snapshot_renders_to_valid_json_skeleton() {
        let snap = Snapshot {
            pool: PoolStats::new("dyn_o_mite"),
            server: ServerStats::new("redis"),
            ..Snapshot::default()
        };
        let s = snap.to_json();
        assert!(s.starts_with('{'));
        assert!(s.ends_with('}'));
        assert!(s.contains("\"service\":\"dynomite\""));
        assert!(s.contains("\"dyn_o_mite\":{"));
        assert!(s.contains("\"redis\":{"));
    }

    #[test]
    fn describe_lists_every_metric() {
        let text = describe_stats();
        for spec in POOL_CODEC {
            assert!(
                text.contains(spec.name),
                "pool metric {} missing",
                spec.name
            );
            assert!(text.contains(spec.description));
        }
        for spec in SERVER_CODEC {
            assert!(
                text.contains(spec.name),
                "server metric {} missing",
                spec.name
            );
        }
    }

    #[test]
    fn escape_handles_quotes_and_controls() {
        let s = EscapedStr("a\"b\nc").to_string();
        assert_eq!(s, r#"a\"b\nc"#);
    }
}
