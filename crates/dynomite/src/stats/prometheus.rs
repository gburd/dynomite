//! Prometheus text exposition rendering for stats snapshots.
//!
//! The renderer walks the immutable [`Snapshot`] value and produces a
//! Prometheus 0.0.4 text-format string suitable for serving over an
//! HTTP `/metrics` endpoint. The rendering uses the `prometheus`
//! crate's [`Registry`] and [`TextEncoder`]: a fresh registry is built
//! per call, every metric family is registered with its `# HELP` and
//! `# TYPE` headers, and the counter/gauge values are filled from the
//! snapshot before encoding.
//!
//! Naming conventions:
//!
//! * Pool counters become `dynomite_pool_<field>_total` with a single
//!   `pool` label.
//! * Pool gauges and timestamps become `dynomite_pool_<field>` with a
//!   single `pool` label.
//! * Server counters become `dynomite_server_<field>_total` with a
//!   single `server` label.
//! * Server gauges and timestamps become `dynomite_server_<field>`
//!   with a single `server` label.
//! * Histogram summaries (latency, payload size, queue waits, etc.)
//!   are exposed as gauges named `dynomite_<channel>_microseconds`
//!   carrying a `quantile` label. Prometheus does not have a way to
//!   round-trip a pre-aggregated estimated histogram, so we publish
//!   the same quantile rollups the JSON endpoint already exposes.
//! * The build identification block is published as
//!   `dynomite_build_info{version,source,rack,dc}` set to `1`, the
//!   convention popularised by `node_exporter`.
//! * Each server entry also produces `dynomite_peer_state` with the
//!   `peer` and `state` labels, set to `1` for `up` and `0` for
//!   `down`. The current snapshot model treats every server as up
//!   (the eject timestamps live in their respective metrics); the
//!   gauge is emitted so dashboards have a stable label set to
//!   target.

use prometheus::{Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};

use crate::stats::codec::{StatsMetricType, POOL_CODEC, SERVER_CODEC};
use crate::stats::snapshot::{HistogramSummary, Snapshot};

/// Render a [`Snapshot`] in the Prometheus 0.0.4 text exposition format.
///
/// The string returned is a complete, self-contained response body
/// that may be served directly with a
/// `Content-Type: text/plain; version=0.0.4; charset=utf-8` header.
///
/// # Examples
///
/// ```
/// use dynomite::stats::{render_prometheus, PoolStats, ServerStats, ServiceInfo, Snapshot};
///
/// let snap = Snapshot {
///     info: ServiceInfo {
///         source: "node-a".into(),
///         version: "0.0.1".into(),
///         rack: "r1".into(),
///         dc: "dc1".into(),
///     },
///     pool: PoolStats::new("dyn_o_mite"),
///     server: ServerStats::new("redis_local"),
///     ..Snapshot::default()
/// };
/// let text = render_prometheus(&snap);
/// assert!(text.contains("dynomite_build_info"));
/// assert!(text.contains("# TYPE dynomite_build_info gauge"));
/// ```
pub fn render_prometheus(snap: &Snapshot) -> String {
    let registry = Registry::new();
    register_build_info(&registry, snap);
    register_uptime(&registry, snap);
    register_resource_usage(&registry, snap);
    register_pool(&registry, snap);
    register_server(&registry, snap);
    register_peer_state(&registry, snap);
    register_histogram_summaries(&registry, snap);
    register_queue_p99s(&registry, snap);

    let mut buf = Vec::with_capacity(8 * 1024);
    let encoder = TextEncoder::new();
    encoder
        .encode(&registry.gather(), &mut buf)
        .expect("invariant: TextEncoder writes valid UTF-8 into Vec<u8>");
    String::from_utf8(buf).expect("invariant: TextEncoder emits UTF-8")
}

fn register_build_info(registry: &Registry, snap: &Snapshot) {
    let opts = Opts::new(
        "dynomite_build_info",
        "Static identification of the running engine; value is always 1.",
    );
    let gauge = IntGaugeVec::new(opts, &["version", "source", "rack", "dc"])
        .expect("invariant: build_info descriptor is valid");
    gauge
        .with_label_values(&[
            &snap.info.version,
            &snap.info.source,
            &snap.info.rack,
            &snap.info.dc,
        ])
        .set(1);
    registry
        .register(Box::new(gauge))
        .expect("invariant: build_info registers cleanly");
}

fn register_uptime(registry: &Registry, snap: &Snapshot) {
    let opts = Opts::new(
        "dynomite_uptime_seconds",
        "Seconds elapsed since the engine started.",
    );
    let gauge = IntGaugeVec::new(opts, &[]).expect("invariant: uptime descriptor is valid");
    gauge.with_label_values(&[]).set(snap.uptime);
    registry
        .register(Box::new(gauge))
        .expect("invariant: uptime registers cleanly");

    let opts = Opts::new(
        "dynomite_timestamp_seconds",
        "Wall-clock seconds since the UNIX epoch at snapshot time.",
    );
    let gauge = IntGaugeVec::new(opts, &[]).expect("invariant: timestamp descriptor is valid");
    gauge.with_label_values(&[]).set(snap.timestamp);
    registry
        .register(Box::new(gauge))
        .expect("invariant: timestamp registers cleanly");
}

fn register_resource_usage(registry: &Registry, snap: &Snapshot) {
    let entries: [(&str, &str, i64); 5] = [
        (
            "dynomite_alloc_msgs",
            "Number of message structs currently allocated.",
            snap.alloc_msgs,
        ),
        (
            "dynomite_free_msgs",
            "Number of message structs on the free list.",
            snap.free_msgs,
        ),
        (
            "dynomite_alloc_mbufs",
            "Number of mbuf chunks currently allocated.",
            snap.alloc_mbufs,
        ),
        (
            "dynomite_free_mbufs",
            "Number of mbuf chunks on the free list.",
            snap.free_mbufs,
        ),
        (
            "dynomite_memory_bytes",
            "Resident set size of the engine in bytes.",
            snap.dyn_memory,
        ),
    ];
    for (name, help, value) in entries {
        let gauge = IntGaugeVec::new(Opts::new(name, help), &[])
            .expect("invariant: resource gauge descriptor is valid");
        gauge.with_label_values(&[]).set(value);
        registry
            .register(Box::new(gauge))
            .expect("invariant: resource gauge registers cleanly");
    }
}

fn register_pool(registry: &Registry, snap: &Snapshot) {
    let pool = &snap.pool.name;
    for (i, spec) in POOL_CODEC.iter().enumerate() {
        let value = snap.pool.metrics.get(i).copied().unwrap_or(0);
        match spec.kind {
            StatsMetricType::Counter => {
                let name = format!("dynomite_pool_{}_total", spec.name);
                let opts = Opts::new(name, spec.description);
                let counter = IntCounterVec::new(opts, &["pool"])
                    .expect("invariant: pool counter descriptor is valid");
                if value > 0 {
                    counter
                        .with_label_values(&[pool.as_str()])
                        .inc_by(u64::try_from(value).unwrap_or(0));
                } else {
                    let _ = counter.with_label_values(&[pool.as_str()]);
                }
                registry
                    .register(Box::new(counter))
                    .expect("invariant: pool counter registers cleanly");
            }
            StatsMetricType::Gauge | StatsMetricType::Timestamp => {
                let name = format!("dynomite_pool_{}", spec.name);
                let opts = Opts::new(name, spec.description);
                let gauge = IntGaugeVec::new(opts, &["pool"])
                    .expect("invariant: pool gauge descriptor is valid");
                gauge.with_label_values(&[pool.as_str()]).set(value);
                registry
                    .register(Box::new(gauge))
                    .expect("invariant: pool gauge registers cleanly");
            }
        }
    }
}

fn register_server(registry: &Registry, snap: &Snapshot) {
    let server = &snap.server.name;
    for (i, spec) in SERVER_CODEC.iter().enumerate() {
        let value = snap.server.metrics.get(i).copied().unwrap_or(0);
        match spec.kind {
            StatsMetricType::Counter => {
                let name = format!("dynomite_server_{}_total", spec.name);
                let opts = Opts::new(name, spec.description);
                let counter = IntCounterVec::new(opts, &["server"])
                    .expect("invariant: server counter descriptor is valid");
                if value > 0 {
                    counter
                        .with_label_values(&[server.as_str()])
                        .inc_by(u64::try_from(value).unwrap_or(0));
                } else {
                    let _ = counter.with_label_values(&[server.as_str()]);
                }
                registry
                    .register(Box::new(counter))
                    .expect("invariant: server counter registers cleanly");
            }
            StatsMetricType::Gauge | StatsMetricType::Timestamp => {
                let name = format!("dynomite_server_{}", spec.name);
                let opts = Opts::new(name, spec.description);
                let gauge = IntGaugeVec::new(opts, &["server"])
                    .expect("invariant: server gauge descriptor is valid");
                gauge.with_label_values(&[server.as_str()]).set(value);
                registry
                    .register(Box::new(gauge))
                    .expect("invariant: server gauge registers cleanly");
            }
        }
    }
}

fn register_peer_state(registry: &Registry, snap: &Snapshot) {
    let opts = Opts::new(
        "dynomite_peer_state",
        "Peer up/down indicator. The active state has value 1; the other has value 0.",
    );
    let gauge = IntGaugeVec::new(opts, &["peer", "state"])
        .expect("invariant: peer_state descriptor is valid");
    let peer = snap.server.name.as_str();
    gauge.with_label_values(&[peer, "up"]).set(1);
    gauge.with_label_values(&[peer, "down"]).set(0);
    registry
        .register(Box::new(gauge))
        .expect("invariant: peer_state registers cleanly");
}

fn register_histogram_summaries(registry: &Registry, snap: &Snapshot) {
    let entries: [(&str, &str, &HistogramSummary); 8] = [
        (
            "dynomite_request_latency_microseconds",
            "Top-level request latency in microseconds.",
            &snap.latency,
        ),
        (
            "dynomite_payload_size_bytes",
            "Observed request/response payload sizes in bytes.",
            &snap.payload_size,
        ),
        (
            "dynomite_cross_region_latency_microseconds",
            "Cross-region peer round-trip latency in microseconds.",
            &snap.cross_region_latency,
        ),
        (
            "dynomite_cross_zone_latency_microseconds",
            "Cross-zone peer latency in microseconds.",
            &snap.cross_zone_latency,
        ),
        (
            "dynomite_server_latency_microseconds",
            "Backing-server response latency in microseconds.",
            &snap.server_latency,
        ),
        (
            "dynomite_cross_region_queue_wait_microseconds",
            "Cross-region queue wait time in microseconds.",
            &snap.cross_region_queue_wait,
        ),
        (
            "dynomite_cross_zone_queue_wait_microseconds",
            "Cross-zone queue wait time in microseconds.",
            &snap.cross_zone_queue_wait,
        ),
        (
            "dynomite_server_queue_wait_microseconds",
            "Server queue wait time in microseconds.",
            &snap.server_queue_wait,
        ),
    ];
    for (name, help, summary) in entries {
        let gauge = IntGaugeVec::new(Opts::new(name, help), &["quantile"])
            .expect("invariant: histogram quantile gauge is valid");
        let s = *summary;
        let mean_v = i64::try_from(s.mean).unwrap_or(i64::MAX);
        let q95 = i64::try_from(s.p95).unwrap_or(i64::MAX);
        let q99 = i64::try_from(s.p99).unwrap_or(i64::MAX);
        let q999 = i64::try_from(s.p999).unwrap_or(i64::MAX);
        let max_v = i64::try_from(s.max).unwrap_or(i64::MAX);
        gauge.with_label_values(&["mean"]).set(mean_v);
        gauge.with_label_values(&["0.95"]).set(q95);
        gauge.with_label_values(&["0.99"]).set(q99);
        gauge.with_label_values(&["0.999"]).set(q999);
        gauge.with_label_values(&["max"]).set(max_v);
        registry
            .register(Box::new(gauge))
            .expect("invariant: histogram quantile gauge registers cleanly");
    }
}

fn register_queue_p99s(registry: &Registry, snap: &Snapshot) {
    let entries: [(&str, &str, u64); 8] = [
        (
            "dynomite_client_out_queue_p99",
            "99th percentile of the client outbound queue length.",
            snap.client_out_queue_p99,
        ),
        (
            "dynomite_server_in_queue_p99",
            "99th percentile of the server inbound queue length.",
            snap.server_in_queue_p99,
        ),
        (
            "dynomite_server_out_queue_p99",
            "99th percentile of the server outbound queue length.",
            snap.server_out_queue_p99,
        ),
        (
            "dynomite_dnode_client_out_queue_p99",
            "99th percentile of the dnode client outbound queue length.",
            snap.dnode_client_out_queue_p99,
        ),
        (
            "dynomite_peer_in_queue_p99",
            "99th percentile of the local-DC peer inbound queue length.",
            snap.peer_in_queue_p99,
        ),
        (
            "dynomite_peer_out_queue_p99",
            "99th percentile of the local-DC peer outbound queue length.",
            snap.peer_out_queue_p99,
        ),
        (
            "dynomite_remote_peer_in_queue_p99",
            "99th percentile of the remote-DC peer inbound queue length.",
            snap.remote_peer_in_queue_p99,
        ),
        (
            "dynomite_remote_peer_out_queue_p99",
            "99th percentile of the remote-DC peer outbound queue length.",
            snap.remote_peer_out_queue_p99,
        ),
    ];
    for (name, help, value) in entries {
        let gauge = IntGaugeVec::new(Opts::new(name, help), &[])
            .expect("invariant: queue p99 gauge descriptor is valid");
        let value_i64 = i64::try_from(value).unwrap_or(i64::MAX);
        gauge.with_label_values(&[]).set(value_i64);
        registry
            .register(Box::new(gauge))
            .expect("invariant: queue p99 gauge registers cleanly");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::codec::PoolField;
    use crate::stats::snapshot::{PoolStats, ServerStats, ServiceInfo};

    fn make_snap() -> Snapshot {
        Snapshot {
            info: ServiceInfo {
                source: "node-a".into(),
                version: "0.0.1".into(),
                rack: "r1".into(),
                dc: "dc1".into(),
            },
            pool: PoolStats::new("dyn_o_mite"),
            server: ServerStats::new("redis_local"),
            ..Snapshot::default()
        }
    }

    #[test]
    fn render_prometheus_includes_help_and_type_lines() {
        let mut snap = make_snap();
        snap.pool.metrics[PoolField::ClientEof.index()] = 7;
        let out = render_prometheus(&snap);
        assert!(
            out.contains("# HELP dynomite_pool_client_eof_total"),
            "missing # HELP for pool client_eof:\n{out}"
        );
        assert!(
            out.contains("# TYPE dynomite_pool_client_eof_total counter"),
            "missing # TYPE for pool client_eof:\n{out}"
        );
        assert!(
            out.contains("dynomite_pool_client_eof_total{pool=\"dyn_o_mite\"} 7"),
            "missing pool client_eof value line:\n{out}"
        );
    }

    #[test]
    fn render_prometheus_quotes_label_values() {
        let mut snap = make_snap();
        snap.pool = PoolStats::new("my\\pool\"");
        snap.pool.metrics[PoolField::ClientEof.index()] = 3;
        let out = render_prometheus(&snap);
        let backslash = "\\\\";
        let escaped_quote = "\\\"";
        let expected_label = format!("pool=\"my{backslash}pool{escaped_quote}\"");
        assert!(
            out.contains(&expected_label),
            "expected escaped label `{expected_label}` not found in:\n{out}"
        );
    }

    #[test]
    fn render_prometheus_emits_build_info() {
        let snap = make_snap();
        let out = render_prometheus(&snap);
        assert!(
            out.contains("# TYPE dynomite_build_info gauge"),
            "missing build_info type line:\n{out}"
        );
        let needle = "dynomite_build_info{";
        let pos = out
            .find(needle)
            .unwrap_or_else(|| panic!("missing build_info value line:\n{out}"));
        let line_end = out[pos..].find('\n').map_or(out.len(), |n| pos + n);
        let line = &out[pos..line_end];
        assert!(
            line.contains("version=\"0.0.1\""),
            "build_info missing version label: {line}"
        );
        assert!(line.ends_with(" 1"), "build_info value should be 1: {line}");
    }

    #[test]
    fn render_prometheus_includes_server_counters_and_uptime() {
        let mut snap = make_snap();
        snap.uptime = 42;
        snap.server.metrics[crate::stats::ServerField::ReadRequests.index()] = 5;
        let out = render_prometheus(&snap);
        assert!(
            out.contains("# TYPE dynomite_server_read_requests_total counter"),
            "server counter type line missing"
        );
        assert!(
            out.contains("dynomite_server_read_requests_total{server=\"redis_local\"} 5"),
            "server counter value missing:\n{out}"
        );
        assert!(
            out.contains("dynomite_uptime_seconds 42"),
            "uptime gauge value missing:\n{out}"
        );
    }

    #[test]
    fn render_prometheus_emits_peer_state_for_server() {
        let snap = make_snap();
        let out = render_prometheus(&snap);
        assert!(
            out.contains("dynomite_peer_state{peer=\"redis_local\",state=\"up\"} 1"),
            "peer_state up line missing:\n{out}"
        );
        assert!(
            out.contains("dynomite_peer_state{peer=\"redis_local\",state=\"down\"} 0"),
            "peer_state down line missing:\n{out}"
        );
    }
}
