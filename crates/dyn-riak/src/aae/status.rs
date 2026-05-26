//! Server-side AAE-status snapshot.
//!
//! [`AaeStatusSnapshot`] is the structured view the
//! `dyn-admin aae-status` PBC operation returns. The shape is a
//! straight roll-up of [`crate::aae::metrics::AaeMetricsSnapshot`]
//! into a per-peer table plus the snapshot-file metadata and
//! the local tree shape, suitable for both the human and JSON
//! renderings the CLI offers.
//!
//! Operators wire a concrete provider via the
//! [`AaeStatusProvider`] trait; the default
//! [`NoopAaeStatusProvider`] returns an empty (but valid)
//! snapshot so the PBC op never fails for "AAE not yet
//! enabled" deployments.
//!
//! The struct here is deliberately serialisable (`serde::Serialize`
//! pulled in via `dyn-admin`'s rendering path), and the wire
//! format is the dedicated
//! [`crate::proto::pb::messages::DynRpbAaeStatusResp`]
//! message.
//!
//! # Examples
//!
//! ```
//! use dyn_riak::aae::status::{AaeStatusSnapshot, AaeStatusProvider, NoopAaeStatusProvider};
//!
//! let p = NoopAaeStatusProvider;
//! let snap: AaeStatusSnapshot = p.current_status();
//! assert!(snap.peers.is_empty());
//! assert_eq!(snap.snapshot_save_total, 0);
//! ```
//!
//! # Wire mapping
//!
//! * `peers[*]` -> repeated `DynRpbAaePeerStatus`
//! * `snapshot_path` -> `bytes` (UTF-8)
//! * `snapshot_last_save_unix` -> `uint64` (zero means
//!   "never saved")
//! * `snapshot_last_load_unix` -> `uint64` (zero means
//!   "never loaded")
//! * `snapshot_save_total` -> `uint64`
//! * `snapshot_load_total` -> `uint64`
//! * `snapshot_corruption_total` -> `uint64`
//! * `tree_n_time_buckets` / `tree_n_segments` /
//!   `tree_time_window_seconds` -> tree shape
//! * `tree_memory_estimate_bytes` -> server-side rough
//!   estimate

/// Per-peer slice of an AAE status snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AaePeerStatus {
    /// Peer index, matching the gossip table.
    pub peer_idx: u32,
    /// Datacenter of the peer.
    pub dc: String,
    /// Rack of the peer.
    pub rack: String,
    /// Wall-clock seconds (UNIX epoch) when this peer's
    /// most recent exchange completed. Zero means "no
    /// exchange has ever completed".
    pub last_exchange_unix: u64,
    /// Cumulative count of divergent keys observed since
    /// the last full sweep finished. Zero means either
    /// "the last sweep saw no divergence" or "no full sweep
    /// has completed yet". The provider implementation
    /// decides which interpretation applies.
    pub divergent_keys_since_last_full_sweep: u64,
    /// Cumulative count of repair tasks dispatched against
    /// this peer.
    pub repair_dispatched_total: u64,
}

/// The full AAE status snapshot returned to operators.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AaeStatusSnapshot {
    /// One row per peer the AAE worker is exchanging with.
    pub peers: Vec<AaePeerStatus>,
    /// Path of the local snapshot file, if any.
    pub snapshot_path: String,
    /// Wall-clock seconds (UNIX epoch) when the most recent
    /// snapshot save succeeded. Zero means "never".
    pub snapshot_last_save_unix: u64,
    /// Wall-clock seconds (UNIX epoch) when the most recent
    /// snapshot load succeeded. Zero means "never".
    pub snapshot_last_load_unix: u64,
    /// Cumulative count of snapshot writes.
    pub snapshot_save_total: u64,
    /// Cumulative count of snapshot loads.
    pub snapshot_load_total: u64,
    /// Cumulative count of corrupted-snapshot rejections.
    pub snapshot_corruption_total: u64,
    /// Number of top-level time buckets in the local tree.
    pub tree_n_time_buckets: u32,
    /// Number of bottom-level segments per time bucket in
    /// the local tree.
    pub tree_n_segments: u32,
    /// Wall-clock width of one time bucket, in seconds.
    pub tree_time_window_seconds: u64,
    /// Rough estimate of the local tree's resident memory,
    /// in bytes.
    pub tree_memory_estimate_bytes: u64,
}

/// Pluggable provider trait. Operators implement this on
/// their AAE driver's state object so the PBC server can
/// route `DynRpbAaeStatusReq` requests through to the live
/// snapshot.
pub trait AaeStatusProvider: Send + Sync {
    /// Return the current AAE status snapshot. Callers
    /// expect this to be cheap and non-blocking; the
    /// provider should walk a small in-memory cache rather
    /// than locking out the AAE driver.
    fn current_status(&self) -> AaeStatusSnapshot;
}

/// Default provider that returns an empty snapshot. Wired
/// into [`crate::serve_pbc`] via the existing default-arg
/// path so the PBC op always has a handler even before the
/// operator enables AAE.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopAaeStatusProvider;

impl AaeStatusProvider for NoopAaeStatusProvider {
    fn current_status(&self) -> AaeStatusSnapshot {
        AaeStatusSnapshot::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_provider_returns_empty_snapshot() {
        let p = NoopAaeStatusProvider;
        let s = p.current_status();
        assert!(s.peers.is_empty());
        assert_eq!(s.snapshot_save_total, 0);
        assert_eq!(s.tree_n_time_buckets, 0);
    }

    #[test]
    fn snapshot_default_is_empty() {
        let s = AaeStatusSnapshot::default();
        assert_eq!(s.peers.len(), 0);
        assert_eq!(s.snapshot_path, "");
        assert_eq!(s.tree_memory_estimate_bytes, 0);
    }

    #[test]
    fn populated_snapshot_carries_per_peer_rows() {
        let s = AaeStatusSnapshot {
            peers: vec![AaePeerStatus {
                peer_idx: 7,
                dc: "dc1".into(),
                rack: "rA".into(),
                last_exchange_unix: 100,
                divergent_keys_since_last_full_sweep: 3,
                repair_dispatched_total: 2,
            }],
            snapshot_path: "/var/lib/dynomite/aae/tree.snapshot".into(),
            snapshot_last_save_unix: 200,
            snapshot_save_total: 5,
            ..Default::default()
        };
        assert_eq!(s.peers[0].peer_idx, 7);
        assert_eq!(s.snapshot_save_total, 5);
    }
}
