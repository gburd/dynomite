//! Operator metrics for the Tictac AAE worker.
//!
//! AAE runs in the background; without dedicated counters and
//! gauges an operator has no signal whether it is working,
//! whether divergences are being found, or whether convergence
//! is happening. This module ships a small accumulator that
//! the scheduler, exchange, repair, and persist sub-modules
//! call at sensible points; the snapshot it produces is what
//! the embedding crate copies into its own Prometheus exposer
//! (the `dynomited` HTTP `/metrics` endpoint, the OpenTelemetry
//! exporter, etc.).
//!
//! The accumulator is deliberately self-contained: it does not
//! reach into `dynomite::stats::FailureMetrics` because the
//! AAE worker lives in a different crate than the engine's
//! failure-cause counters and a cross-crate handle would
//! couple `crates/dyniak` to internal engine details. The
//! shapes intentionally mirror `FailureMetrics` (a single
//! `parking_lot::Mutex` over the inner state, snapshot
//! returns immutable rows) so a future merge into one
//! handle is mechanical.
//!
//! # Counters and gauges
//!
//! * `aae_exchange_attempts_total{peer_idx, dc, rack}`
//! * `aae_exchange_success_total{peer_idx, dc, rack}`
//! * `aae_exchange_divergent_keys_total{peer_idx, dc, rack}`
//! * `aae_repair_dispatched_total{peer_idx, dc, rack}`
//! * `aae_tree_segments_dirty_gauge{peer_idx}`
//! * `aae_full_sweep_last_completed_seconds_gauge{peer_idx}`
//! * `aae_snapshot_save_total`
//! * `aae_snapshot_load_total`
//! * `aae_snapshot_corruption_total`
//!
//! All counters initialise to zero. Gauges return zero (and
//! `0` for "never completed" timestamps) until an observation
//! lands.
//!
//! # Examples
//!
//! ```
//! use dyniak::aae::metrics::AaeMetrics;
//!
//! let m = AaeMetrics::new();
//! m.record_exchange_attempt(7, "dc1", "r1");
//! m.record_exchange_success(7, "dc1", "r1");
//! m.record_divergent_keys(7, "dc1", "r1", 12);
//! let snap = m.snapshot();
//! assert_eq!(snap.exchange_attempts[0].count, 1);
//! assert_eq!(snap.divergent_keys[0].count, 12);
//! ```

use std::collections::HashMap;
use std::path::Path;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use parking_lot::Mutex;

use crate::aae::persist::PersistError;
use crate::aae::tictac::Tree;

/// Live, mutable accumulator for AAE-specific counters and
/// gauges. Cheap to clone via `Arc`; every method takes
/// `&self` and grabs a `parking_lot::Mutex` for the duration
/// of one hashmap insert.
#[derive(Debug, Default)]
pub struct AaeMetrics {
    inner: Mutex<AaeInner>,
}

#[derive(Debug, Default)]
struct AaeInner {
    exchange_attempts: HashMap<PeerKey, u64>,
    exchange_success: HashMap<PeerKey, u64>,
    divergent_keys: HashMap<PeerKey, u64>,
    repair_dispatched: HashMap<PeerKey, u64>,
    segments_dirty: HashMap<u32, u64>,
    full_sweep_last_completed_unix: HashMap<u32, u64>,
    snapshot_save_total: u64,
    snapshot_load_total: u64,
    snapshot_corruption_total: u64,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct PeerKey {
    peer_idx: u32,
    dc: String,
    rack: String,
}

impl AaeMetrics {
    /// Construct a fresh accumulator with all counters and
    /// gauges zeroed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment `aae_exchange_attempts_total` by one for the
    /// given peer/dc/rack triple. Called by the AAE driver on
    /// every sweep tick that selects this peer (whether or
    /// not the exchange ultimately succeeds).
    pub fn record_exchange_attempt(&self, peer_idx: u32, dc: &str, rack: &str) {
        let key = peer_key(peer_idx, dc, rack);
        let mut inner = self.inner.lock();
        *inner.exchange_attempts.entry(key).or_insert(0) += 1;
    }

    /// Increment `aae_exchange_success_total` by one. Called
    /// by the AAE driver when an exchange completes without a
    /// transport error, regardless of whether divergences
    /// were found.
    pub fn record_exchange_success(&self, peer_idx: u32, dc: &str, rack: &str) {
        let key = peer_key(peer_idx, dc, rack);
        let mut inner = self.inner.lock();
        *inner.exchange_success.entry(key).or_insert(0) += 1;
    }

    /// Add `count` to `aae_exchange_divergent_keys_total`.
    /// Called once per exchange after the symmetric
    /// difference is computed; `count` is the total number
    /// of divergent keys observed across every diverging
    /// segment.
    pub fn record_divergent_keys(&self, peer_idx: u32, dc: &str, rack: &str, count: u64) {
        if count == 0 {
            return;
        }
        let key = peer_key(peer_idx, dc, rack);
        let mut inner = self.inner.lock();
        *inner.divergent_keys.entry(key).or_insert(0) += count;
    }

    /// Add `count` to `aae_repair_dispatched_total`. Called
    /// once per exchange after the repair scheduler emits
    /// outcomes; `count` is the number of `Repaired`
    /// outcomes (winners + siblings). Outcomes that surface
    /// `AmbiguousVClock` or `PeerUnavailable` do NOT add to
    /// this count; those are tracked elsewhere.
    pub fn record_repair_dispatched(&self, peer_idx: u32, dc: &str, rack: &str, count: u64) {
        if count == 0 {
            return;
        }
        let key = peer_key(peer_idx, dc, rack);
        let mut inner = self.inner.lock();
        *inner.repair_dispatched.entry(key).or_insert(0) += count;
    }

    /// Set `aae_tree_segments_dirty_gauge` for one peer to
    /// `count`. The driver calls this whenever a segment is
    /// marked dirty (a write landed in it since the last
    /// rebuild) or cleared (a rebuild visited it).
    pub fn set_segments_dirty(&self, peer_idx: u32, count: u64) {
        let mut inner = self.inner.lock();
        inner.segments_dirty.insert(peer_idx, count);
    }

    /// Record that a full sweep over every time bucket
    /// completed at the given wall-clock time, expressed as
    /// "seconds since UNIX epoch". Operators read the
    /// rendered gauge as "seconds since this peer's last
    /// full sweep finished".
    pub fn mark_full_sweep_completed(&self, peer_idx: u32, unix_seconds: u64) {
        let mut inner = self.inner.lock();
        inner
            .full_sweep_last_completed_unix
            .insert(peer_idx, unix_seconds);
    }

    /// Convenience wrapper: capture `SystemTime::now()` and
    /// hand it off to [`Self::mark_full_sweep_completed`].
    /// Clamps a clock skew that puts the system before the
    /// epoch to zero.
    pub fn mark_full_sweep_completed_now(&self, peer_idx: u32) {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.mark_full_sweep_completed(peer_idx, secs);
    }

    /// Increment `aae_snapshot_save_total` by one.
    pub fn record_snapshot_save(&self) {
        self.inner.lock().snapshot_save_total += 1;
    }

    /// Increment `aae_snapshot_load_total` by one.
    pub fn record_snapshot_load(&self) {
        self.inner.lock().snapshot_load_total += 1;
    }

    /// Increment `aae_snapshot_corruption_total` by one.
    /// Called when the snapshot loader rejects a file as
    /// corrupted, version-skewed, or shape-inconsistent;
    /// each of those causes the next sweep to rebuild from
    /// scratch.
    pub fn record_snapshot_corruption(&self) {
        self.inner.lock().snapshot_corruption_total += 1;
    }

    /// Build an immutable snapshot of every counter and
    /// gauge.
    #[must_use]
    pub fn snapshot(&self) -> AaeMetricsSnapshot {
        let inner = self.inner.lock();
        AaeMetricsSnapshot {
            exchange_attempts: collect_peer_entries(&inner.exchange_attempts),
            exchange_success: collect_peer_entries(&inner.exchange_success),
            divergent_keys: collect_peer_entries(&inner.divergent_keys),
            repair_dispatched: collect_peer_entries(&inner.repair_dispatched),
            segments_dirty: collect_simple_peer_entries(&inner.segments_dirty),
            full_sweep_last_completed_unix: collect_simple_peer_entries(
                &inner.full_sweep_last_completed_unix,
            ),
            snapshot_save_total: inner.snapshot_save_total,
            snapshot_load_total: inner.snapshot_load_total,
            snapshot_corruption_total: inner.snapshot_corruption_total,
        }
    }
}

/// Wrap [`Tree::save_snapshot`] and bump the relevant counters
/// on the supplied `metrics` handle. Existing snapshot calls
/// continue to work unchanged; operators who want metric
/// integration call this function from their AAE driver.
///
/// # Errors
///
/// Forwards [`PersistError`] from the underlying call. Failed
/// saves do NOT bump `aae_snapshot_save_total` (the counter
/// names successful writes, matching the convention used for
/// the existing `dispatch_*_total` families).
pub fn save_snapshot_with_metrics(
    tree: &Tree,
    path: &Path,
    metrics: &AaeMetrics,
) -> Result<(), PersistError> {
    tree.save_snapshot(path)?;
    metrics.record_snapshot_save();
    Ok(())
}

/// Wrap [`Tree::load_snapshot`] and bump the relevant counters.
/// Successful loads bump `aae_snapshot_load_total`; the
/// `Corrupted`, `VersionSkew`, and `BadShape` variants of
/// [`PersistError`] each bump `aae_snapshot_corruption_total`
/// before propagating.
///
/// `PersistError::Io` is NOT counted as corruption (the
/// `NotFound` case is the common "no prior snapshot, rebuild"
/// path).
///
/// # Errors
///
/// Forwards [`PersistError`] from the underlying call after
/// bumping the corruption counter when applicable.
pub fn load_snapshot_with_metrics(path: &Path, metrics: &AaeMetrics) -> Result<Tree, PersistError> {
    match Tree::load_snapshot(path) {
        Ok(t) => {
            metrics.record_snapshot_load();
            Ok(t)
        }
        Err(e) => {
            match &e {
                PersistError::Corrupted(_)
                | PersistError::VersionSkew { .. }
                | PersistError::BadShape(_) => metrics.record_snapshot_corruption(),
                PersistError::Io(_) => {}
            }
            Err(e)
        }
    }
}

fn peer_key(peer_idx: u32, dc: &str, rack: &str) -> PeerKey {
    PeerKey {
        peer_idx,
        dc: dc.to_owned(),
        rack: rack.to_owned(),
    }
}

fn collect_peer_entries(map: &HashMap<PeerKey, u64>) -> Vec<PeerEntry> {
    let mut out: Vec<PeerEntry> = map
        .iter()
        .map(|(k, v)| PeerEntry {
            peer_idx: k.peer_idx,
            dc: k.dc.clone(),
            rack: k.rack.clone(),
            count: *v,
        })
        .collect();
    out.sort_by(|a, b| {
        a.peer_idx
            .cmp(&b.peer_idx)
            .then(a.dc.cmp(&b.dc))
            .then(a.rack.cmp(&b.rack))
    });
    out
}

fn collect_simple_peer_entries(map: &HashMap<u32, u64>) -> Vec<SimplePeerEntry> {
    let mut out: Vec<SimplePeerEntry> = map
        .iter()
        .map(|(k, v)| SimplePeerEntry {
            peer_idx: *k,
            value: *v,
        })
        .collect();
    out.sort_by_key(|e| e.peer_idx);
    out
}

/// One labeled `aae_*_total` counter row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerEntry {
    /// Peer index.
    pub peer_idx: u32,
    /// Datacenter of the peer.
    pub dc: String,
    /// Rack of the peer.
    pub rack: String,
    /// Cumulative observation count.
    pub count: u64,
}

/// One labeled gauge row that only carries `peer_idx`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SimplePeerEntry {
    /// Peer index.
    pub peer_idx: u32,
    /// Gauge value.
    pub value: u64,
}

/// Immutable snapshot of every AAE counter and gauge.
#[derive(Clone, Debug, Default)]
pub struct AaeMetricsSnapshot {
    /// `aae_exchange_attempts_total` rows.
    pub exchange_attempts: Vec<PeerEntry>,
    /// `aae_exchange_success_total` rows.
    pub exchange_success: Vec<PeerEntry>,
    /// `aae_exchange_divergent_keys_total` rows.
    pub divergent_keys: Vec<PeerEntry>,
    /// `aae_repair_dispatched_total` rows.
    pub repair_dispatched: Vec<PeerEntry>,
    /// `aae_tree_segments_dirty_gauge` rows.
    pub segments_dirty: Vec<SimplePeerEntry>,
    /// `aae_full_sweep_last_completed_seconds_gauge` rows.
    /// Each value is wall-clock seconds since UNIX epoch
    /// when the corresponding peer's most recent full sweep
    /// finished. Zero means "never completed".
    pub full_sweep_last_completed_unix: Vec<SimplePeerEntry>,
    /// `aae_snapshot_save_total` value.
    pub snapshot_save_total: u64,
    /// `aae_snapshot_load_total` value.
    pub snapshot_load_total: u64,
    /// `aae_snapshot_corruption_total` value.
    pub snapshot_corruption_total: u64,
}

impl AaeMetricsSnapshot {
    /// True when every counter and gauge is at its initial
    /// zero state. Used by the embedding to skip rendering an
    /// AAE block that has seen no activity.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.exchange_attempts.is_empty()
            && self.exchange_success.is_empty()
            && self.divergent_keys.is_empty()
            && self.repair_dispatched.is_empty()
            && self.segments_dirty.is_empty()
            && self.full_sweep_last_completed_unix.is_empty()
            && self.snapshot_save_total == 0
            && self.snapshot_load_total == 0
            && self.snapshot_corruption_total == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aae::tictac::TreeShape;
    use tempfile::TempDir;

    fn shape() -> TreeShape {
        TreeShape {
            n_time_buckets: 2,
            n_segments: 16,
            time_window_seconds: 60,
        }
    }

    #[test]
    fn fresh_metrics_snapshot_is_empty() {
        let m = AaeMetrics::new();
        assert!(m.snapshot().is_empty());
    }

    #[test]
    fn exchange_attempts_keyed_by_peer_dc_rack() {
        let m = AaeMetrics::new();
        m.record_exchange_attempt(1, "dc1", "rA");
        m.record_exchange_attempt(1, "dc1", "rA");
        m.record_exchange_attempt(2, "dc1", "rA");
        let s = m.snapshot();
        assert_eq!(s.exchange_attempts.len(), 2);
        assert_eq!(s.exchange_attempts[0].peer_idx, 1);
        assert_eq!(s.exchange_attempts[0].count, 2);
        assert_eq!(s.exchange_attempts[1].peer_idx, 2);
        assert_eq!(s.exchange_attempts[1].count, 1);
    }

    #[test]
    fn divergent_and_repair_zero_values_are_dropped() {
        let m = AaeMetrics::new();
        m.record_divergent_keys(1, "dc1", "rA", 0);
        m.record_repair_dispatched(1, "dc1", "rA", 0);
        let s = m.snapshot();
        assert!(s.divergent_keys.is_empty());
        assert!(s.repair_dispatched.is_empty());
    }

    #[test]
    fn segments_dirty_gauge_is_set_by_peer() {
        let m = AaeMetrics::new();
        m.set_segments_dirty(1, 17);
        m.set_segments_dirty(2, 4);
        m.set_segments_dirty(1, 5); // overwrite
        let s = m.snapshot();
        assert_eq!(s.segments_dirty.len(), 2);
        let one = s.segments_dirty.iter().find(|e| e.peer_idx == 1).unwrap();
        assert_eq!(one.value, 5);
    }

    #[test]
    fn full_sweep_completed_records_unix_seconds() {
        let m = AaeMetrics::new();
        m.mark_full_sweep_completed(7, 1_700_000_000);
        let s = m.snapshot();
        assert_eq!(s.full_sweep_last_completed_unix.len(), 1);
        assert_eq!(s.full_sweep_last_completed_unix[0].peer_idx, 7);
        assert_eq!(s.full_sweep_last_completed_unix[0].value, 1_700_000_000);
    }

    #[test]
    fn snapshot_counters_increment() {
        let m = AaeMetrics::new();
        m.record_snapshot_save();
        m.record_snapshot_save();
        m.record_snapshot_load();
        m.record_snapshot_corruption();
        let s = m.snapshot();
        assert_eq!(s.snapshot_save_total, 2);
        assert_eq!(s.snapshot_load_total, 1);
        assert_eq!(s.snapshot_corruption_total, 1);
    }

    #[test]
    fn save_with_metrics_bumps_save_total() {
        let m = AaeMetrics::new();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aae.snap");
        let tree = Tree::new(shape());
        save_snapshot_with_metrics(&tree, &path, &m).unwrap();
        assert_eq!(m.snapshot().snapshot_save_total, 1);
    }

    #[test]
    fn load_with_metrics_bumps_load_total() {
        let m = AaeMetrics::new();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aae.snap");
        let tree = Tree::new(shape());
        tree.save_snapshot(&path).unwrap();
        let _ = load_snapshot_with_metrics(&path, &m).unwrap();
        assert_eq!(m.snapshot().snapshot_load_total, 1);
        assert_eq!(m.snapshot().snapshot_corruption_total, 0);
    }

    #[test]
    fn load_with_metrics_corrupted_bumps_corruption_total() {
        use std::fs;
        let m = AaeMetrics::new();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("aae.snap");
        // Write a deliberately bad file.
        fs::write(&path, b"not a snapshot").unwrap();
        let err = load_snapshot_with_metrics(&path, &m).unwrap_err();
        assert!(matches!(err, PersistError::Corrupted(_)));
        assert_eq!(m.snapshot().snapshot_corruption_total, 1);
        assert_eq!(m.snapshot().snapshot_load_total, 0);
    }

    #[test]
    fn load_with_metrics_missing_file_does_not_count_as_corruption() {
        let m = AaeMetrics::new();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nope.snap");
        let err = load_snapshot_with_metrics(&path, &m).unwrap_err();
        assert!(matches!(err, PersistError::Io(_)));
        assert_eq!(m.snapshot().snapshot_corruption_total, 0);
        assert_eq!(m.snapshot().snapshot_load_total, 0);
    }
}
