//! Failure-cause counters for the dispatch and gossip planes.
//!
//! The pre-existing pool / server metrics maintained by
//! [`crate::stats::Stats`] track aggregate request volume and a
//! single coarse `errors` bucket. When operators need to tell
//! "the cluster lost quorum because a peer was Down" apart from
//! "the cluster lost quorum because the per-peer outbound
//! channel was full", that single bucket is not enough.
//!
//! [`FailureMetrics`] supplements the existing counters with
//! label-rich counters and gauges:
//!
//! * Per-cause dispatch error counters (no-targets, channel
//!   full, channel closed, response timeout) keyed by the
//!   labels operators want to slice by.
//! * Per-peer state transition counts, for charting how often
//!   gossip flips a peer between [`crate::cluster::peer::PeerState`]
//!   variants.
//! * Per-peer current-state and phi-score gauges, so a
//!   dashboard can show the live failure-detector view.
//!
//! All counters initialise to zero. The accumulators take a
//! single `parking_lot::Mutex` over the inner state; every
//! observation is a hashmap insert and a tiny arithmetic
//! update, so the lock is held for at most a handful of
//! nanoseconds per call. The dispatch hot path is a direct
//! method invocation on an `Arc<FailureMetrics>`; when the
//! handle is `None` the dispatcher's call site short-circuits
//! to a single null-pointer test (see
//! [`crate::cluster::dispatch::ClusterDispatcher::with_failure_metrics`]).
//!
//! # Examples
//!
//! ```
//! use dynomite::msg::ConsistencyLevel;
//! use dynomite::stats::FailureMetrics;
//!
//! let m = FailureMetrics::new();
//! m.record_no_targets("dc1", "rA", ConsistencyLevel::DcQuorum);
//! let snap = m.snapshot();
//! assert_eq!(snap.no_targets.len(), 1);
//! assert_eq!(snap.no_targets[0].count, 1);
//! ```

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;

use crate::cluster::peer::PeerState;
use crate::msg::ConsistencyLevel;

/// Live, mutable accumulator for failure-cause counters.
///
/// Cheap to clone via `Arc`; every method takes `&self`.
#[derive(Debug, Default)]
pub struct FailureMetrics {
    inner: Mutex<FailureInner>,
}

#[derive(Debug, Default)]
struct FailureInner {
    no_targets: HashMap<NoTargetsKey, u64>,
    peer_send_full: HashMap<PeerKey, u64>,
    peer_send_closed: HashMap<PeerKey, u64>,
    backend_send_full: u64,
    backend_send_closed: u64,
    response_timeout: HashMap<ConsistencyLevel, u64>,
    peer_state_transitions: HashMap<TransitionKey, u64>,
    peer_state_current: HashMap<u32, PeerStateRecord>,
    peer_phi: HashMap<u32, PhiRecord>,
    /// Rolling threshold gauge per peer, populated by
    /// [`FailureMetrics::observe_threshold`]. Exposed as the
    /// `gossip_phi_threshold_observed{peer}` Prometheus gauge so
    /// operators can confirm the configured threshold per peer
    /// without reading the running config.
    peer_threshold: HashMap<u32, ThresholdRecord>,
    /// Per-peer instant of the last state change. Updated on
    /// every [`FailureMetrics::record_peer_state_transition`]
    /// call so the next transition can compute the dwell time
    /// the peer spent in the from-state.
    peer_last_change: HashMap<u32, Instant>,
    /// Per-state dwell histogram, keyed by the state being
    /// exited. The histogram uses the same exponential-style
    /// bucketing the engine's other observation surfaces use
    /// ([`DWELL_BUCKETS_SECONDS`]).
    dwell_per_state: HashMap<PeerState, DwellAccumulator>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct NoTargetsKey {
    dc: String,
    rack: String,
    consistency: ConsistencyLevel,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct PeerKey {
    peer_idx: u32,
    peer_dc: String,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct TransitionKey {
    peer_idx: u32,
    from: PeerState,
    to: PeerState,
}

#[derive(Debug, Clone)]
struct PeerStateRecord {
    dc: String,
    rack: String,
    state: PeerState,
}

#[derive(Debug, Clone)]
struct PhiRecord {
    dc: String,
    rack: String,
    /// Phi rounded to thousandths so the gauge survives the
    /// `i64` round-trip in the Prometheus encoder.
    phi_milli: i64,
}

#[derive(Debug, Clone)]
struct ThresholdRecord {
    dc: String,
    rack: String,
    /// Threshold rounded to thousandths so the gauge survives
    /// the `i64` round-trip in the Prometheus encoder.
    threshold_milli: i64,
}

/// Bucket boundaries for the `peer_state_dwell_seconds`
/// histogram, in seconds. Picked to match the operator-facing
/// SLO ranges every chaos run cares about: sub-second
/// (transient flap), single-second to one minute (settling
/// gossip), and minute-to-hour (long outage).
pub const DWELL_BUCKETS_SECONDS: &[f64] = &[
    0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1_800.0, 3_600.0,
];

/// Per-state dwell accumulator. Stores cumulative bucket
/// counts (one slot per upper bound in [`DWELL_BUCKETS_SECONDS`]
/// plus a final `+Inf` slot), the running sum of observations
/// in seconds, and the total observation count.
#[derive(Debug, Clone)]
struct DwellAccumulator {
    /// One slot per bucket boundary plus a final `+Inf` slot
    /// (`DWELL_BUCKETS_SECONDS.len() + 1` entries).
    bucket_counts: Vec<u64>,
    sum_seconds: f64,
    count: u64,
}

impl DwellAccumulator {
    fn new() -> Self {
        Self {
            bucket_counts: vec![0; DWELL_BUCKETS_SECONDS.len() + 1],
            sum_seconds: 0.0,
            count: 0,
        }
    }

    fn observe(&mut self, dwell_seconds: f64) {
        let v = if dwell_seconds.is_nan() || dwell_seconds < 0.0 {
            0.0
        } else {
            dwell_seconds
        };
        self.sum_seconds += v;
        self.count = self.count.saturating_add(1);
        // Cumulative buckets: increment every slot whose upper
        // bound is >= v, plus the trailing +Inf slot.
        let last = self.bucket_counts.len() - 1;
        for (i, upper) in DWELL_BUCKETS_SECONDS.iter().enumerate() {
            if v <= *upper {
                self.bucket_counts[i] = self.bucket_counts[i].saturating_add(1);
            }
        }
        self.bucket_counts[last] = self.bucket_counts[last].saturating_add(1);
    }
}

impl FailureMetrics {
    /// Construct a fresh accumulator with all counters and
    /// gauges zeroed.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::FailureMetrics;
    /// let m = FailureMetrics::new();
    /// assert_eq!(m.snapshot().backend_send_full, 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the `dispatch_no_targets_total` counter for the
    /// given local-DC labels.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::ConsistencyLevel;
    /// use dynomite::stats::FailureMetrics;
    /// let m = FailureMetrics::new();
    /// m.record_no_targets("dc1", "rA", ConsistencyLevel::DcOne);
    /// m.record_no_targets("dc1", "rA", ConsistencyLevel::DcOne);
    /// assert_eq!(m.snapshot().no_targets[0].count, 2);
    /// ```
    pub fn record_no_targets(&self, dc: &str, rack: &str, consistency: ConsistencyLevel) {
        let key = NoTargetsKey {
            dc: dc.to_owned(),
            rack: rack.to_owned(),
            consistency,
        };
        let mut inner = self.inner.lock();
        *inner.no_targets.entry(key).or_insert(0) += 1;
    }

    /// Increment the `dispatch_peer_send_full_total` counter.
    pub fn record_peer_send_full(&self, peer_idx: u32, peer_dc: &str) {
        let key = PeerKey {
            peer_idx,
            peer_dc: peer_dc.to_owned(),
        };
        let mut inner = self.inner.lock();
        *inner.peer_send_full.entry(key).or_insert(0) += 1;
    }

    /// Increment the `dispatch_peer_send_closed_total` counter.
    pub fn record_peer_send_closed(&self, peer_idx: u32, peer_dc: &str) {
        let key = PeerKey {
            peer_idx,
            peer_dc: peer_dc.to_owned(),
        };
        let mut inner = self.inner.lock();
        *inner.peer_send_closed.entry(key).or_insert(0) += 1;
    }

    /// Increment the `dispatch_backend_send_full_total` counter.
    pub fn record_backend_send_full(&self) {
        self.inner.lock().backend_send_full += 1;
    }

    /// Increment the `dispatch_backend_send_closed_total`
    /// counter.
    pub fn record_backend_send_closed(&self) {
        self.inner.lock().backend_send_closed += 1;
    }

    /// Increment the `dispatch_response_timeout_total` counter.
    /// Used by the response coalescer when every per-target
    /// sender drops without producing a reply (i.e. the request
    /// timed out at the dispatch layer).
    pub fn record_response_timeout(&self, consistency: ConsistencyLevel) {
        let mut inner = self.inner.lock();
        *inner.response_timeout.entry(consistency).or_insert(0) += 1;
    }

    /// Record a peer-state transition. Increments
    /// `peer_state_transitions_total` by one and updates the
    /// `peer_state_current` gauge to the new state. Also
    /// observes the dwell time the peer spent in `from` (when
    /// a previous transition timestamp is available) into the
    /// per-state `peer_state_dwell_seconds` histogram.
    pub fn record_peer_state_transition(
        &self,
        peer_idx: u32,
        dc: &str,
        rack: &str,
        from: PeerState,
        to: PeerState,
    ) {
        self.record_peer_state_transition_at(peer_idx, dc, rack, from, to, Instant::now());
    }

    /// Variant of [`Self::record_peer_state_transition`] that
    /// takes the wall-clock instant as a parameter. Lets tests
    /// drive the dwell histogram without sleeping.
    pub fn record_peer_state_transition_at(
        &self,
        peer_idx: u32,
        dc: &str,
        rack: &str,
        from: PeerState,
        to: PeerState,
        now: Instant,
    ) {
        let key = TransitionKey { peer_idx, from, to };
        let mut inner = self.inner.lock();
        *inner.peer_state_transitions.entry(key).or_insert(0) += 1;
        inner.peer_state_current.insert(
            peer_idx,
            PeerStateRecord {
                dc: dc.to_owned(),
                rack: rack.to_owned(),
                state: to,
            },
        );
        if let Some(prev) = inner.peer_last_change.insert(peer_idx, now) {
            let dwell = now.saturating_duration_since(prev).as_secs_f64();
            let acc = inner
                .dwell_per_state
                .entry(from)
                .or_insert_with(DwellAccumulator::new);
            acc.observe(dwell);
        }
    }

    /// Update the `peer_state_current` gauge without recording
    /// a transition. Useful for the initial publish at startup
    /// and when an evaluate tick observes a peer whose state
    /// has not changed.
    pub fn observe_peer_state(&self, peer_idx: u32, dc: &str, rack: &str, state: PeerState) {
        let mut inner = self.inner.lock();
        inner.peer_state_current.insert(
            peer_idx,
            PeerStateRecord {
                dc: dc.to_owned(),
                rack: rack.to_owned(),
                state,
            },
        );
    }

    /// Update the `gossip_phi_score` gauge for a peer. The phi
    /// value is rounded to thousandths and stored as an `i64`
    /// (millis); the snapshot exposes a floating-point view
    /// rebuilt from that integer.
    pub fn observe_phi(&self, peer_idx: u32, dc: &str, rack: &str, phi: f64) {
        let phi_milli = phi_to_milli(phi);
        let mut inner = self.inner.lock();
        inner.peer_phi.insert(
            peer_idx,
            PhiRecord {
                dc: dc.to_owned(),
                rack: rack.to_owned(),
                phi_milli,
            },
        );
    }

    /// Update the `gossip_phi_threshold_observed` gauge for a
    /// peer. Mirrors [`Self::observe_phi`] in storage so the
    /// operator can read the configured threshold next to the
    /// computed phi without reading the running YAML.
    pub fn observe_threshold(&self, peer_idx: u32, dc: &str, rack: &str, threshold: f64) {
        let threshold_milli = phi_to_milli(threshold);
        let mut inner = self.inner.lock();
        inner.peer_threshold.insert(
            peer_idx,
            ThresholdRecord {
                dc: dc.to_owned(),
                rack: rack.to_owned(),
                threshold_milli,
            },
        );
    }

    /// Build an immutable snapshot of every counter and gauge.
    ///
    /// The returned `FailureSnapshot` is `Clone` and `Send`, so
    /// the stats aggregator can stash it in the snapshot cell
    /// without holding the underlying lock.
    #[must_use]
    pub fn snapshot(&self) -> FailureSnapshot {
        let inner = self.inner.lock();
        let mut no_targets: Vec<NoTargetsEntry> = inner
            .no_targets
            .iter()
            .map(|(k, v)| NoTargetsEntry {
                dc: k.dc.clone(),
                rack: k.rack.clone(),
                consistency: k.consistency,
                count: *v,
            })
            .collect();
        no_targets.sort_by(|a, b| {
            a.dc.cmp(&b.dc)
                .then(a.rack.cmp(&b.rack))
                .then((a.consistency as u8).cmp(&(b.consistency as u8)))
        });
        let peer_send_full = collect_peer_entries(&inner.peer_send_full);
        let peer_send_closed = collect_peer_entries(&inner.peer_send_closed);
        let mut response_timeout: Vec<TimeoutEntry> = inner
            .response_timeout
            .iter()
            .map(|(c, v)| TimeoutEntry {
                consistency: *c,
                count: *v,
            })
            .collect();
        response_timeout.sort_by_key(|e| e.consistency as u8);
        let mut peer_state_transitions: Vec<TransitionEntry> = inner
            .peer_state_transitions
            .iter()
            .map(|(k, v)| TransitionEntry {
                peer_idx: k.peer_idx,
                from: k.from,
                to: k.to,
                count: *v,
            })
            .collect();
        peer_state_transitions.sort_by(|a, b| {
            a.peer_idx
                .cmp(&b.peer_idx)
                .then((a.from as u8).cmp(&(b.from as u8)))
                .then((a.to as u8).cmp(&(b.to as u8)))
        });
        let mut peer_state_current: Vec<PeerStateEntry> = inner
            .peer_state_current
            .iter()
            .map(|(idx, rec)| PeerStateEntry {
                peer_idx: *idx,
                dc: rec.dc.clone(),
                rack: rec.rack.clone(),
                state: rec.state,
            })
            .collect();
        peer_state_current.sort_by_key(|e| e.peer_idx);
        let mut peer_phi: Vec<PhiEntry> = inner
            .peer_phi
            .iter()
            .map(|(idx, rec)| PhiEntry {
                peer_idx: *idx,
                dc: rec.dc.clone(),
                rack: rec.rack.clone(),
                phi: milli_to_phi(rec.phi_milli),
            })
            .collect();
        peer_phi.sort_by_key(|e| e.peer_idx);
        let mut peer_threshold: Vec<ThresholdEntry> = inner
            .peer_threshold
            .iter()
            .map(|(idx, rec)| ThresholdEntry {
                peer_idx: *idx,
                dc: rec.dc.clone(),
                rack: rec.rack.clone(),
                threshold: milli_to_phi(rec.threshold_milli),
            })
            .collect();
        peer_threshold.sort_by_key(|e| e.peer_idx);
        let mut peer_state_dwell: Vec<DwellEntry> = inner
            .dwell_per_state
            .iter()
            .map(|(state, acc)| DwellEntry {
                state: *state,
                count: acc.count,
                sum_seconds: acc.sum_seconds,
                bucket_counts: acc.bucket_counts.clone(),
            })
            .collect();
        peer_state_dwell.sort_by_key(|e| e.state as u8);
        FailureSnapshot {
            no_targets,
            peer_send_full,
            peer_send_closed,
            backend_send_full: inner.backend_send_full,
            backend_send_closed: inner.backend_send_closed,
            response_timeout,
            peer_state_transitions,
            peer_state_current,
            peer_phi,
            peer_threshold,
            peer_state_dwell,
        }
    }
}

/// Convert a phi value to a thousandths-rounded `i64`. Floats
/// outside `[0, i64::MAX/1000]`, NaN, and infinity all clamp to
/// the saturating ceiling. The function is implemented without
/// `as`-casts so the pedantic precision-loss lint stays clean.
fn phi_to_milli(phi: f64) -> i64 {
    let saturating = i64::MAX / 1000;
    if phi.is_nan() {
        return saturating;
    }
    if !phi.is_finite() {
        // Both +inf and -inf are unexpected; treat positive
        // infinity as a saturating high and negative infinity
        // as zero (phi cannot be negative in practice).
        if phi > 0.0 {
            return saturating;
        }
        return 0;
    }
    if phi <= 0.0 {
        return 0;
    }
    let scaled = (phi * 1000.0).round();
    f64_to_i64_clamped(scaled).min(saturating)
}

/// Render the stored thousandths-precision integer back as a
/// floating-point phi value.
fn milli_to_phi(milli: i64) -> f64 {
    i64_to_f64(milli) / 1000.0
}

/// Lossless `i64 -> f64` for the small magnitudes we hold in
/// the gauge. Implemented without an `as`-cast.
fn i64_to_f64(value: i64) -> f64 {
    let neg = value < 0;
    let abs = value.unsigned_abs();
    let hi = u32::try_from(abs >> 32).unwrap_or(u32::MAX);
    let lo = u32::try_from(abs & 0xFFFF_FFFF).unwrap_or(u32::MAX);
    let f = f64::from(hi) * 4_294_967_296.0_f64 + f64::from(lo);
    if neg {
        -f
    } else {
        f
    }
}

/// Convert a non-negative finite f64 (assumed less than
/// `i64::MAX`) to an `i64` without using a raw `as` cast.
fn f64_to_i64_clamped(x: f64) -> i64 {
    if !x.is_finite() || x <= 0.0 {
        return 0;
    }
    let bits = x.to_bits();
    let exp_field = u32::try_from((bits >> 52) & 0x7FF).unwrap_or(0);
    let mant = bits & ((1u64 << 52) - 1);
    if exp_field < 1023 {
        return 0;
    }
    let unbiased = exp_field - 1023;
    if unbiased >= 63 {
        return i64::MAX;
    }
    let m = (1u64 << 52) | mant;
    let value = if unbiased >= 52 {
        let shift = unbiased - 52;
        m.checked_shl(shift).unwrap_or(u64::MAX)
    } else {
        m >> (52 - unbiased)
    };
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn collect_peer_entries(map: &HashMap<PeerKey, u64>) -> Vec<PeerEntry> {
    let mut out: Vec<PeerEntry> = map
        .iter()
        .map(|(k, v)| PeerEntry {
            peer_idx: k.peer_idx,
            peer_dc: k.peer_dc.clone(),
            count: *v,
        })
        .collect();
    out.sort_by(|a, b| a.peer_idx.cmp(&b.peer_idx).then(a.peer_dc.cmp(&b.peer_dc)));
    out
}

/// A single labeled `dispatch_no_targets_total` row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NoTargetsEntry {
    /// Local datacenter the request originated from.
    pub dc: String,
    /// Local rack.
    pub rack: String,
    /// Effective consistency level of the request that produced
    /// the `NoTargets` plan.
    pub consistency: ConsistencyLevel,
    /// Cumulative occurrences.
    pub count: u64,
}

/// A single labeled per-peer dispatch error row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerEntry {
    /// Index of the target peer in the pool's peer array.
    pub peer_idx: u32,
    /// Datacenter of the target peer.
    pub peer_dc: String,
    /// Cumulative occurrences.
    pub count: u64,
}

/// A single labeled `dispatch_response_timeout_total` row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TimeoutEntry {
    /// Consistency level of the request that timed out.
    pub consistency: ConsistencyLevel,
    /// Cumulative occurrences.
    pub count: u64,
}

/// A single labeled `peer_state_transitions_total` row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransitionEntry {
    /// Peer that transitioned.
    pub peer_idx: u32,
    /// State the peer was in before the transition.
    pub from: PeerState,
    /// State the peer is in after the transition.
    pub to: PeerState,
    /// Cumulative occurrences.
    pub count: u64,
}

/// A single labeled `peer_state_current` gauge row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerStateEntry {
    /// Peer index.
    pub peer_idx: u32,
    /// Datacenter of the peer.
    pub dc: String,
    /// Rack of the peer.
    pub rack: String,
    /// Current state.
    pub state: PeerState,
}

/// A single labeled `gossip_phi_score` gauge row.
#[derive(Clone, Debug)]
pub struct PhiEntry {
    /// Peer index.
    pub peer_idx: u32,
    /// Datacenter of the peer.
    pub dc: String,
    /// Rack of the peer.
    pub rack: String,
    /// Current phi value as observed at the last evaluate tick.
    pub phi: f64,
}

/// A single labeled `gossip_phi_threshold_observed` gauge row.
#[derive(Clone, Debug)]
pub struct ThresholdEntry {
    /// Peer index.
    pub peer_idx: u32,
    /// Datacenter of the peer.
    pub dc: String,
    /// Rack of the peer.
    pub rack: String,
    /// Threshold the failure detector last evaluated against
    /// this peer (typically the cluster-wide configured
    /// threshold, surfaced per peer so an operator viewing one
    /// peer's panel sees its own threshold).
    pub threshold: f64,
}

/// A single per-state row of the `peer_state_dwell_seconds`
/// histogram. Holds the cumulative bucket counts (one slot per
/// boundary in [`DWELL_BUCKETS_SECONDS`] plus a trailing `+Inf`
/// slot), the running sum of observations in seconds, and the
/// total observation count.
#[derive(Clone, Debug)]
pub struct DwellEntry {
    /// State the peer was in when the dwell was observed.
    pub state: PeerState,
    /// Total number of dwell observations for this state.
    pub count: u64,
    /// Sum of all observed dwell durations in seconds.
    pub sum_seconds: f64,
    /// Cumulative bucket counts. Length is
    /// `DWELL_BUCKETS_SECONDS.len() + 1`; the last entry is the
    /// `+Inf` bucket (always equal to [`Self::count`]).
    pub bucket_counts: Vec<u64>,
}

/// Immutable snapshot of every failure-cause metric.
#[derive(Clone, Debug, Default)]
pub struct FailureSnapshot {
    /// `dispatch_no_targets_total` rows.
    pub no_targets: Vec<NoTargetsEntry>,
    /// `dispatch_peer_send_full_total` rows.
    pub peer_send_full: Vec<PeerEntry>,
    /// `dispatch_peer_send_closed_total` rows.
    pub peer_send_closed: Vec<PeerEntry>,
    /// `dispatch_backend_send_full_total` value.
    pub backend_send_full: u64,
    /// `dispatch_backend_send_closed_total` value.
    pub backend_send_closed: u64,
    /// `dispatch_response_timeout_total` rows.
    pub response_timeout: Vec<TimeoutEntry>,
    /// `peer_state_transitions_total` rows.
    pub peer_state_transitions: Vec<TransitionEntry>,
    /// `peer_state_current` gauge rows.
    pub peer_state_current: Vec<PeerStateEntry>,
    /// `gossip_phi_score` gauge rows.
    pub peer_phi: Vec<PhiEntry>,
    /// `gossip_phi_threshold_observed` gauge rows.
    pub peer_threshold: Vec<ThresholdEntry>,
    /// `peer_state_dwell_seconds` histogram rows, one per
    /// observed [`PeerState`].
    pub peer_state_dwell: Vec<DwellEntry>,
}

impl FailureSnapshot {
    /// True when every counter and gauge is empty. Used by the
    /// stats subsystem to skip rendering the failure block when
    /// the operator has not wired the metrics in (and so every
    /// observation has been a no-op).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.no_targets.is_empty()
            && self.peer_send_full.is_empty()
            && self.peer_send_closed.is_empty()
            && self.backend_send_full == 0
            && self.backend_send_closed == 0
            && self.response_timeout.is_empty()
            && self.peer_state_transitions.is_empty()
            && self.peer_state_current.is_empty()
            && self.peer_phi.is_empty()
            && self.peer_threshold.is_empty()
            && self.peer_state_dwell.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_targets_increments_per_label_set() {
        let m = FailureMetrics::new();
        m.record_no_targets("dc1", "rA", ConsistencyLevel::DcQuorum);
        m.record_no_targets("dc1", "rA", ConsistencyLevel::DcQuorum);
        m.record_no_targets("dc2", "rA", ConsistencyLevel::DcQuorum);
        let s = m.snapshot();
        assert_eq!(s.no_targets.len(), 2);
        let dc1 = s.no_targets.iter().find(|e| e.dc == "dc1").unwrap();
        let dc2 = s.no_targets.iter().find(|e| e.dc == "dc2").unwrap();
        assert_eq!(dc1.count, 2);
        assert_eq!(dc2.count, 1);
    }

    #[test]
    fn peer_send_full_and_closed_are_distinct_buckets() {
        let m = FailureMetrics::new();
        m.record_peer_send_full(7, "dc2");
        m.record_peer_send_closed(7, "dc2");
        m.record_peer_send_closed(7, "dc2");
        let s = m.snapshot();
        assert_eq!(s.peer_send_full.len(), 1);
        assert_eq!(s.peer_send_full[0].count, 1);
        assert_eq!(s.peer_send_closed.len(), 1);
        assert_eq!(s.peer_send_closed[0].count, 2);
    }

    #[test]
    fn backend_counters_track_independently() {
        let m = FailureMetrics::new();
        m.record_backend_send_full();
        m.record_backend_send_closed();
        m.record_backend_send_closed();
        let s = m.snapshot();
        assert_eq!(s.backend_send_full, 1);
        assert_eq!(s.backend_send_closed, 2);
    }

    #[test]
    fn response_timeout_rolls_up_by_consistency() {
        let m = FailureMetrics::new();
        m.record_response_timeout(ConsistencyLevel::DcOne);
        m.record_response_timeout(ConsistencyLevel::DcQuorum);
        m.record_response_timeout(ConsistencyLevel::DcQuorum);
        let s = m.snapshot();
        assert_eq!(s.response_timeout.len(), 2);
        let q = s
            .response_timeout
            .iter()
            .find(|e| e.consistency == ConsistencyLevel::DcQuorum)
            .unwrap();
        assert_eq!(q.count, 2);
    }

    #[test]
    fn peer_state_transition_records_count_and_current() {
        let m = FailureMetrics::new();
        m.record_peer_state_transition(3, "dc1", "rA", PeerState::Normal, PeerState::Down);
        m.record_peer_state_transition(3, "dc1", "rA", PeerState::Down, PeerState::Normal);
        m.record_peer_state_transition(3, "dc1", "rA", PeerState::Normal, PeerState::Down);
        let s = m.snapshot();
        let to_down = s
            .peer_state_transitions
            .iter()
            .find(|t| t.from == PeerState::Normal && t.to == PeerState::Down)
            .unwrap();
        assert_eq!(to_down.count, 2);
        assert_eq!(s.peer_state_current.len(), 1);
        assert_eq!(s.peer_state_current[0].state, PeerState::Down);
    }

    #[test]
    fn observe_phi_rounds_to_thousandths() {
        let m = FailureMetrics::new();
        m.observe_phi(1, "dc1", "rA", 1.234_567);
        let s = m.snapshot();
        assert_eq!(s.peer_phi.len(), 1);
        // 1.234_567 rounds to 1.235 at three decimal places.
        let diff = (s.peer_phi[0].phi - 1.235).abs();
        assert!(diff < 1e-9, "phi={}", s.peer_phi[0].phi);
    }

    #[test]
    fn snapshot_empty_predicate_is_correct() {
        let m = FailureMetrics::new();
        assert!(m.snapshot().is_empty());
        m.record_backend_send_full();
        assert!(!m.snapshot().is_empty());
    }

    #[test]
    fn observe_threshold_records_value_per_peer() {
        let m = FailureMetrics::new();
        m.observe_threshold(7, "dc1", "rA", 8.0);
        m.observe_threshold(8, "dc2", "rB", 6.5);
        let s = m.snapshot();
        assert_eq!(s.peer_threshold.len(), 2);
        assert!((s.peer_threshold[0].threshold - 8.0).abs() < 1e-9);
        assert_eq!(s.peer_threshold[0].peer_idx, 7);
        assert!((s.peer_threshold[1].threshold - 6.5).abs() < 1e-9);
    }

    #[test]
    fn dwell_histogram_records_observation_on_transition() {
        // Drive a Normal->Down->Normal flap with controlled
        // instants so the histogram observes deterministic
        // dwell durations.
        let m = FailureMetrics::new();
        let t0 = Instant::now();
        // First transition: no prior change time, so the
        // accumulator stays empty but the timestamp is set.
        m.record_peer_state_transition_at(
            1,
            "dc1",
            "rA",
            PeerState::Unknown,
            PeerState::Normal,
            t0,
        );
        // Spent 2.5 seconds in Normal -> 0.5..5 second bucket.
        m.record_peer_state_transition_at(
            1,
            "dc1",
            "rA",
            PeerState::Normal,
            PeerState::Down,
            t0 + std::time::Duration::from_millis(2_500),
        );
        // Spent 45 seconds in Down -> 30..60 second bucket.
        m.record_peer_state_transition_at(
            1,
            "dc1",
            "rA",
            PeerState::Down,
            PeerState::Normal,
            t0 + std::time::Duration::from_millis(2_500 + 45_000),
        );
        let s = m.snapshot();
        let normal = s
            .peer_state_dwell
            .iter()
            .find(|e| e.state == PeerState::Normal)
            .expect("Normal dwell entry present");
        assert_eq!(normal.count, 1);
        assert!((normal.sum_seconds - 2.5).abs() < 1e-6);
        let down = s
            .peer_state_dwell
            .iter()
            .find(|e| e.state == PeerState::Down)
            .expect("Down dwell entry present");
        assert_eq!(down.count, 1);
        assert!((down.sum_seconds - 45.0).abs() < 1e-6);
        // The +Inf bucket equals the count.
        assert_eq!(*normal.bucket_counts.last().unwrap(), normal.count);
        assert_eq!(*down.bucket_counts.last().unwrap(), down.count);
    }

    #[test]
    fn dwell_buckets_are_cumulative() {
        let m = FailureMetrics::new();
        let t0 = Instant::now();
        m.record_peer_state_transition_at(
            5,
            "dc1",
            "rA",
            PeerState::Unknown,
            PeerState::Normal,
            t0,
        );
        // 0.05 second dwell falls in the smallest 0.1 bucket.
        m.record_peer_state_transition_at(
            5,
            "dc1",
            "rA",
            PeerState::Normal,
            PeerState::Down,
            t0 + std::time::Duration::from_millis(50),
        );
        let s = m.snapshot();
        let normal = s
            .peer_state_dwell
            .iter()
            .find(|e| e.state == PeerState::Normal)
            .expect("Normal dwell entry");
        // Every bucket boundary >= 0.05 should hold count 1.
        for bc in &normal.bucket_counts {
            assert_eq!(*bc, 1, "every cumulative bucket sees the 0.05s observation");
        }
    }
}
