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

use crate::cluster::peer::PeerState;
use crate::stats::codec::{StatsMetricType, POOL_CODEC, SERVER_CODEC};
use crate::stats::failure::FailureSnapshot;
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
    register_failure_metrics(&registry, &snap.failure);
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
    gauge.with_label_values::<&str>(&[]).set(snap.uptime);
    registry
        .register(Box::new(gauge))
        .expect("invariant: uptime registers cleanly");

    let opts = Opts::new(
        "dynomite_timestamp_seconds",
        "Wall-clock seconds since the UNIX epoch at snapshot time.",
    );
    let gauge = IntGaugeVec::new(opts, &[]).expect("invariant: timestamp descriptor is valid");
    gauge.with_label_values::<&str>(&[]).set(snap.timestamp);
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
        gauge.with_label_values::<&str>(&[]).set(value);
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

fn register_failure_metrics(registry: &Registry, failure: &FailureSnapshot) {
    register_failure_no_targets(registry, failure);
    register_failure_peer_send(registry, failure);
    register_failure_backend_send(registry, failure);
    register_failure_response_timeout(registry, failure);
    register_failure_peer_state(registry, failure);
    register_failure_phi(registry, failure);
    register_failure_phi_threshold(registry, failure);
    register_failure_dwell(registry, failure);
}

fn register_failure_no_targets(registry: &Registry, failure: &FailureSnapshot) {
    let opts = Opts::new(
        "dispatch_no_targets_total",
        "Dispatch failures because the only routable peer for the hashed token was Down or absent.",
    );
    let counter = IntCounterVec::new(opts, &["dc", "rack", "consistency_level"])
        .expect("invariant: dispatch_no_targets descriptor is valid");
    for entry in &failure.no_targets {
        counter
            .with_label_values(&[
                entry.dc.as_str(),
                entry.rack.as_str(),
                entry.consistency.name(),
            ])
            .inc_by(entry.count);
    }
    registry
        .register(Box::new(counter))
        .expect("invariant: dispatch_no_targets registers cleanly");
}

fn register_failure_peer_send(registry: &Registry, failure: &FailureSnapshot) {
    let full = IntCounterVec::new(
        Opts::new(
            "dispatch_peer_send_full_total",
            "Dispatcher try_send to a peer's outbound channel returned Full.",
        ),
        &["peer_idx", "peer_dc"],
    )
    .expect("invariant: dispatch_peer_send_full descriptor is valid");
    for entry in &failure.peer_send_full {
        full.with_label_values(&[&entry.peer_idx.to_string(), &entry.peer_dc])
            .inc_by(entry.count);
    }
    registry
        .register(Box::new(full))
        .expect("invariant: dispatch_peer_send_full registers cleanly");

    let closed = IntCounterVec::new(
        Opts::new(
            "dispatch_peer_send_closed_total",
            "Dispatcher try_send to a peer's outbound channel returned Closed.",
        ),
        &["peer_idx", "peer_dc"],
    )
    .expect("invariant: dispatch_peer_send_closed descriptor is valid");
    for entry in &failure.peer_send_closed {
        closed
            .with_label_values(&[&entry.peer_idx.to_string(), &entry.peer_dc])
            .inc_by(entry.count);
    }
    registry
        .register(Box::new(closed))
        .expect("invariant: dispatch_peer_send_closed registers cleanly");
}

fn register_failure_backend_send(registry: &Registry, failure: &FailureSnapshot) {
    let full = IntCounterVec::new(
        Opts::new(
            "dispatch_backend_send_full_total",
            "Dispatcher try_send to the local datastore backend returned Full.",
        ),
        &[],
    )
    .expect("invariant: dispatch_backend_send_full descriptor is valid");
    if failure.backend_send_full > 0 {
        full.with_label_values::<&str>(&[])
            .inc_by(failure.backend_send_full);
    } else {
        let _ = full.with_label_values::<&str>(&[]);
    }
    registry
        .register(Box::new(full))
        .expect("invariant: dispatch_backend_send_full registers cleanly");

    let closed = IntCounterVec::new(
        Opts::new(
            "dispatch_backend_send_closed_total",
            "Dispatcher try_send to the local datastore backend returned Closed.",
        ),
        &[],
    )
    .expect("invariant: dispatch_backend_send_closed descriptor is valid");
    if failure.backend_send_closed > 0 {
        closed
            .with_label_values::<&str>(&[])
            .inc_by(failure.backend_send_closed);
    } else {
        let _ = closed.with_label_values::<&str>(&[]);
    }
    registry
        .register(Box::new(closed))
        .expect("invariant: dispatch_backend_send_closed registers cleanly");
}

fn register_failure_response_timeout(registry: &Registry, failure: &FailureSnapshot) {
    let counter = IntCounterVec::new(
        Opts::new(
            "dispatch_response_timeout_total",
            "Dispatcher's response coalescer gave up waiting for replies.",
        ),
        &["consistency_level"],
    )
    .expect("invariant: dispatch_response_timeout descriptor is valid");
    for entry in &failure.response_timeout {
        counter
            .with_label_values(&[entry.consistency.name()])
            .inc_by(entry.count);
    }
    registry
        .register(Box::new(counter))
        .expect("invariant: dispatch_response_timeout registers cleanly");
}

fn register_failure_peer_state(registry: &Registry, failure: &FailureSnapshot) {
    let trans = IntCounterVec::new(
        Opts::new(
            "peer_state_transitions_total",
            "Number of gossip-driven peer-state transitions, labelled by from/to state.",
        ),
        &["peer_idx", "from_state", "to_state"],
    )
    .expect("invariant: peer_state_transitions descriptor is valid");
    for entry in &failure.peer_state_transitions {
        let peer_idx = entry.peer_idx.to_string();
        trans
            .with_label_values(&[peer_idx.as_str(), entry.from.name(), entry.to.name()])
            .inc_by(entry.count);
    }
    registry
        .register(Box::new(trans))
        .expect("invariant: peer_state_transitions registers cleanly");

    let current = IntGaugeVec::new(
        Opts::new(
            "peer_state_current",
            "Current peer state. Numeric value matches PeerState's repr(u8): \
             0=Unknown, 1=Joining, 2=Normal, 3=Standby, 4=Down, 5=Reset, 6=Leaving.",
        ),
        &["peer_idx", "dc", "rack"],
    )
    .expect("invariant: peer_state_current descriptor is valid");
    for entry in &failure.peer_state_current {
        current
            .with_label_values(&[&entry.peer_idx.to_string(), &entry.dc, &entry.rack])
            .set(peer_state_value(entry.state));
    }
    registry
        .register(Box::new(current))
        .expect("invariant: peer_state_current registers cleanly");
}

fn register_failure_phi(registry: &Registry, failure: &FailureSnapshot) {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "gossip_phi_score_milli",
            "Phi-accrual failure detector score per peer, scaled by 1000 (gauge units = thousandths).",
        ),
        &["peer_idx", "dc", "rack"],
    )
    .expect("invariant: gossip_phi_score descriptor is valid");
    for entry in &failure.peer_phi {
        let value = phi_to_milli_clamped(entry.phi);
        gauge
            .with_label_values(&[&entry.peer_idx.to_string(), &entry.dc, &entry.rack])
            .set(value);
    }
    registry
        .register(Box::new(gauge))
        .expect("invariant: gossip_phi_score registers cleanly");
}

fn register_failure_phi_threshold(registry: &Registry, failure: &FailureSnapshot) {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "gossip_phi_threshold_observed_milli",
            "Phi-accrual threshold the failure detector last evaluated against the peer, \
             scaled by 1000 (gauge units = thousandths). Use to confirm operator-tuned \
             thresholds against the gossip handler's running config.",
        ),
        &["peer_idx", "dc", "rack"],
    )
    .expect("invariant: gossip_phi_threshold_observed descriptor is valid");
    for entry in &failure.peer_threshold {
        let value = phi_to_milli_clamped(entry.threshold);
        gauge
            .with_label_values(&[&entry.peer_idx.to_string(), &entry.dc, &entry.rack])
            .set(value);
    }
    registry
        .register(Box::new(gauge))
        .expect("invariant: gossip_phi_threshold_observed registers cleanly");
}

fn register_failure_dwell(registry: &Registry, failure: &FailureSnapshot) {
    use crate::stats::failure::DWELL_BUCKETS_SECONDS;
    if failure.peer_state_dwell.is_empty() {
        return;
    }
    // Cumulative bucket counts emitted as `peer_state_dwell_seconds_bucket{state, le}`.
    let bucket_gauge = IntGaugeVec::new(
        Opts::new(
            "peer_state_dwell_seconds_bucket",
            "Cumulative count of peer-state dwell observations whose duration is <= 'le', per state.",
        ),
        &["state", "le"],
    )
    .expect("invariant: peer_state_dwell_seconds_bucket descriptor is valid");
    let count_gauge = IntGaugeVec::new(
        Opts::new(
            "peer_state_dwell_seconds_count",
            "Total number of peer-state dwell observations recorded for the labelled state.",
        ),
        &["state"],
    )
    .expect("invariant: peer_state_dwell_seconds_count descriptor is valid");
    let sum_gauge = IntGaugeVec::new(
        Opts::new(
            "peer_state_dwell_seconds_sum_milli",
            "Sum of dwell observations in milliseconds per state. Divide by 1000 for seconds.",
        ),
        &["state"],
    )
    .expect("invariant: peer_state_dwell_seconds_sum descriptor is valid");
    for entry in &failure.peer_state_dwell {
        let state_label = entry.state.name();
        let count = i64::try_from(entry.count).unwrap_or(i64::MAX);
        count_gauge.with_label_values(&[state_label]).set(count);
        let sum_milli = phi_to_milli_clamped(entry.sum_seconds);
        sum_gauge.with_label_values(&[state_label]).set(sum_milli);
        for (i, upper) in DWELL_BUCKETS_SECONDS.iter().enumerate() {
            if let Some(c) = entry.bucket_counts.get(i) {
                let val = i64::try_from(*c).unwrap_or(i64::MAX);
                let le = format_le(*upper);
                bucket_gauge.with_label_values(&[state_label, &le]).set(val);
            }
        }
        if let Some(c) = entry.bucket_counts.last() {
            let val = i64::try_from(*c).unwrap_or(i64::MAX);
            bucket_gauge
                .with_label_values(&[state_label, "+Inf"])
                .set(val);
        }
    }
    registry
        .register(Box::new(bucket_gauge))
        .expect("invariant: peer_state_dwell_seconds_bucket registers cleanly");
    registry
        .register(Box::new(count_gauge))
        .expect("invariant: peer_state_dwell_seconds_count registers cleanly");
    registry
        .register(Box::new(sum_gauge))
        .expect("invariant: peer_state_dwell_seconds_sum registers cleanly");
}

/// Format a bucket upper-bound for the `le` label. Whole-second
/// boundaries are rendered without a fractional component so a
/// dashboard cleanly groups buckets like `1` instead of `1.0`.
fn format_le(upper: f64) -> String {
    if upper.fract() == 0.0 && (0.0..1e15).contains(&upper) {
        // Safe: the integer projection of a non-negative finite
        // value below 10^15 fits in u64 and we only emit it for
        // the label string.
        let as_u64 = if (0.0..1e15).contains(&upper) {
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "label rendering of a known finite, non-negative, sub-1e15 bucket boundary"
            )]
            {
                upper as u64
            }
        } else {
            0
        };
        format!("{as_u64}")
    } else {
        format!("{upper}")
    }
}

/// Map a [`PeerState`] to the integer value the Prometheus gauge
/// publishes. Mirrors the enum's `repr(u8)` discriminants but
/// goes via a match so the conversion is explicit and the
/// pedantic cast lints stay clean.
fn peer_state_value(state: PeerState) -> i64 {
    match state {
        PeerState::Unknown => 0,
        PeerState::Joining => 1,
        PeerState::Normal => 2,
        PeerState::Standby => 3,
        PeerState::Down => 4,
        PeerState::Reset => 5,
        PeerState::Leaving => 6,
    }
}

/// Render a finite phi value in thousandths as an `i64`. The
/// snapshot already clamps the upstream value; this helper
/// repeats the clamp for safety against future refactors.
fn phi_to_milli_clamped(phi: f64) -> i64 {
    if !phi.is_finite() || phi <= 0.0 {
        return 0;
    }
    let saturating = i64::MAX / 1000;
    let scaled = (phi * 1000.0).round();
    if !scaled.is_finite() || scaled <= 0.0 {
        return 0;
    }
    let bits = scaled.to_bits();
    let exp_field = u32::try_from((bits >> 52) & 0x7FF).unwrap_or(0);
    if exp_field < 1023 {
        return 0;
    }
    let unbiased = exp_field - 1023;
    if unbiased >= 63 {
        return saturating;
    }
    let mant = bits & ((1u64 << 52) - 1);
    let m = (1u64 << 52) | mant;
    let value = if unbiased >= 52 {
        m.checked_shl(unbiased - 52).unwrap_or(u64::MAX)
    } else {
        m >> (52 - unbiased)
    };
    i64::try_from(value).unwrap_or(saturating).min(saturating)
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
        gauge.with_label_values::<&str>(&[]).set(value_i64);
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

    /// The failure-cause counters are wired into the
    /// renderer; verify each family lands with the expected
    /// HELP and TYPE headers and that label values from the
    /// snapshot make it onto the wire.
    #[test]
    fn render_prometheus_emits_failure_cause_counters() {
        use crate::cluster::peer::PeerState;
        use crate::msg::ConsistencyLevel;
        use crate::stats::FailureMetrics;

        let metrics = FailureMetrics::new();
        metrics.record_no_targets("dc1", "rA", ConsistencyLevel::DcQuorum);
        metrics.record_peer_send_full(7, "dc2");
        metrics.record_peer_send_closed(7, "dc2");
        metrics.record_backend_send_full();
        metrics.record_backend_send_closed();
        metrics.record_response_timeout(ConsistencyLevel::DcOne);
        metrics.record_peer_state_transition(3, "dc1", "rA", PeerState::Normal, PeerState::Down);
        metrics.observe_phi(3, "dc1", "rA", 4.5);

        let mut snap = make_snap();
        snap.failure = metrics.snapshot();
        let out = render_prometheus(&snap);

        assert!(
            out.contains("# TYPE dispatch_no_targets_total counter"),
            "missing dispatch_no_targets type line:\n{out}"
        );
        assert!(
            out.contains(
                "dispatch_no_targets_total{consistency_level=\"DC_QUORUM\",dc=\"dc1\",rack=\"rA\"} 1"
            ),
            "missing dispatch_no_targets row:\n{out}"
        );
        assert!(
            out.contains("# TYPE dispatch_peer_send_full_total counter"),
            "missing dispatch_peer_send_full type line:\n{out}"
        );
        assert!(
            out.contains("dispatch_peer_send_full_total{peer_dc=\"dc2\",peer_idx=\"7\"} 1"),
            "missing dispatch_peer_send_full row:\n{out}"
        );
        assert!(
            out.contains("dispatch_peer_send_closed_total{peer_dc=\"dc2\",peer_idx=\"7\"} 1"),
            "missing dispatch_peer_send_closed row:\n{out}"
        );
        assert!(
            out.contains("dispatch_backend_send_full_total 1"),
            "missing dispatch_backend_send_full row:\n{out}"
        );
        assert!(
            out.contains("dispatch_backend_send_closed_total 1"),
            "missing dispatch_backend_send_closed row:\n{out}"
        );
        assert!(
            out.contains("dispatch_response_timeout_total{consistency_level=\"DC_ONE\"} 1"),
            "missing dispatch_response_timeout row:\n{out}"
        );
        assert!(
            out.contains(
                "peer_state_transitions_total{from_state=\"NORMAL\",peer_idx=\"3\",to_state=\"DOWN\"} 1"
            ),
            "missing peer_state_transitions row:\n{out}"
        );
        assert!(
            out.contains("peer_state_current{dc=\"dc1\",peer_idx=\"3\",rack=\"rA\"} 4"),
            "missing peer_state_current row (state=Down=4):\n{out}"
        );
        // phi=4.5 -> 4500 in the milli gauge.
        assert!(
            out.contains("gossip_phi_score_milli{dc=\"dc1\",peer_idx=\"3\",rack=\"rA\"} 4500"),
            "missing gossip_phi_score_milli row:\n{out}"
        );
    }

    /// The new threshold gauge and per-state dwell histogram
    /// must reach the wire when populated.
    #[test]
    fn render_prometheus_emits_threshold_and_dwell_rows() {
        use crate::cluster::peer::PeerState;
        use crate::stats::FailureMetrics;
        use std::time::{Duration, Instant};

        let metrics = FailureMetrics::new();
        metrics.observe_threshold(2, "dc1", "rA", 8.0);
        let t0 = Instant::now();
        metrics.record_peer_state_transition_at(
            2,
            "dc1",
            "rA",
            PeerState::Unknown,
            PeerState::Normal,
            t0,
        );
        // 1.25s in Normal -> Down.
        metrics.record_peer_state_transition_at(
            2,
            "dc1",
            "rA",
            PeerState::Normal,
            PeerState::Down,
            t0 + Duration::from_millis(1_250),
        );

        let mut snap = make_snap();
        snap.failure = metrics.snapshot();
        let out = render_prometheus(&snap);

        assert!(
            out.contains(
                "gossip_phi_threshold_observed_milli{dc=\"dc1\",peer_idx=\"2\",rack=\"rA\"} 8000"
            ),
            "missing gossip_phi_threshold_observed_milli row:\n{out}"
        );
        assert!(
            out.contains("peer_state_dwell_seconds_count{state=\"NORMAL\"} 1"),
            "missing peer_state_dwell_seconds_count row:\n{out}"
        );
        assert!(
            out.contains("peer_state_dwell_seconds_bucket{le=\"+Inf\",state=\"NORMAL\"} 1"),
            "missing peer_state_dwell_seconds_bucket +Inf row:\n{out}"
        );
        // 1.25s falls in 5s bucket but not in 1s bucket.
        assert!(
            out.contains("peer_state_dwell_seconds_bucket{le=\"5\",state=\"NORMAL\"} 1"),
            "missing peer_state_dwell_seconds_bucket le=5 row:\n{out}"
        );
        assert!(
            out.contains("peer_state_dwell_seconds_bucket{le=\"1\",state=\"NORMAL\"} 0"),
            "missing peer_state_dwell_seconds_bucket le=1 (should be 0):\n{out}"
        );
    }
}
