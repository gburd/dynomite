//! Operator-facing configuration for the Tictac active anti-entropy
//! worker.
//!
//! The dynomite engine's `ConfPool` is intentionally not extended
//! from this crate (per AGENTS.md "do not touch
//! `crates/dynomite/src/conf/`"); this struct is therefore stand-
//! alone and the operator (or, in a follow-up slice, the
//! `dynomited` Riak feature wiring) plumbs an instance into
//! [`crate::aae::scheduler::Scheduler`] explicitly.
//!
//! # Examples
//!
//! ```
//! use dyn_riak::aae::config::ConfAae;
//! let cfg = ConfAae::default();
//! assert!(cfg.validate().is_ok());
//! ```

use std::path::PathBuf;
use std::time::Duration;

/// Default cadence for a full sweep over every peer pair (24h).
pub const DEFAULT_FULL_SWEEP_SECONDS: u64 = 24 * 60 * 60;

/// Default cadence for the per-segment exchange tick (60s).
pub const DEFAULT_SEGMENT_SECONDS: u64 = 60;

/// Default count of time-buckets in a tree (24, one per hour of a
/// rolling-day window when paired with the default 24h full sweep).
pub const DEFAULT_TIME_BUCKETS: u32 = 24;

/// Default count of segment leaves per time bucket (1024).
///
/// 1024 is the same fan-out the reference Erlang implementation
/// uses; it strikes a balance between merkle-walk depth and per-
/// segment XOR collision probability for million-key vnodes.
pub const DEFAULT_SEGMENTS: u32 = 1024;

/// Default time-window per top-level bucket, in seconds (1h).
pub const DEFAULT_TIME_WINDOW_SECONDS: u64 = 60 * 60;

/// Hard ceiling on the number of segment leaves per time bucket.
pub const MAX_SEGMENTS: u32 = 1 << 20;

/// Hard ceiling on the number of time-buckets per tree.
pub const MAX_TIME_BUCKETS: u32 = 1 << 12;

/// Default cadence at which the AAE driver writes a fresh
/// snapshot of its tree to disk (5 minutes).
pub const DEFAULT_SNAPSHOT_INTERVAL_SECONDS: u64 = 5 * 60;

/// Default directory under which the AAE snapshot file lives.
pub const DEFAULT_AAE_STATE_DIR: &str = "/var/lib/dynomite/aae";

/// Filename of the AAE snapshot under [`ConfAae::aae_state_dir`].
pub const SNAPSHOT_FILE_NAME: &str = "tree.snapshot";

/// Operator-facing AAE configuration.
///
/// # Examples
///
/// ```
/// use dyn_riak::aae::config::ConfAae;
/// let cfg = ConfAae {
///     enabled: true,
///     full_sweep_interval_seconds: 12 * 60 * 60,
///     segment_interval_seconds: 30,
///     n_time_buckets: 12,
///     n_segments: 512,
///     time_window_seconds: 60 * 60,
///     ..ConfAae::default()
/// };
/// assert!(cfg.validate().is_ok());
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ConfAae {
    /// Whether the AAE worker is started at all.
    pub enabled: bool,
    /// How often a full sweep across every peer pair completes.
    pub full_sweep_interval_seconds: u64,
    /// How often a single (peer, time-bucket) exchange tick fires.
    pub segment_interval_seconds: u64,
    /// Top-level tree fan-out: number of time buckets.
    pub n_time_buckets: u32,
    /// Bottom-level tree fan-out: segments per time bucket.
    pub n_segments: u32,
    /// Width of one time bucket, in seconds.
    pub time_window_seconds: u64,
    /// Cadence at which the AAE driver persists its tree to
    /// disk. The snapshot is consumed on the next process
    /// start to avoid a full datastore rescan; see
    /// [`crate::aae::persist`].
    pub snapshot_interval_seconds: u64,
    /// Directory holding the AAE state files (currently the
    /// snapshot file). Created on first save with mode
    /// `0700` on Unix.
    pub aae_state_dir: PathBuf,
    /// When `true`, the scheduler emits one sweep tick per
    /// `(peer, token, time_bucket)` triple instead of one per
    /// `(peer, time_bucket)` pair. Operators with peers that
    /// hold multiple tokens (vnode-style) get a finer sweep
    /// cadence and a smaller per-tick working set.
    ///
    /// Default `false` (per-peer cadence) so existing
    /// deployments are unaffected.
    pub per_token_exchange: bool,
}

impl Default for ConfAae {
    fn default() -> Self {
        Self {
            enabled: false,
            full_sweep_interval_seconds: DEFAULT_FULL_SWEEP_SECONDS,
            segment_interval_seconds: DEFAULT_SEGMENT_SECONDS,
            n_time_buckets: DEFAULT_TIME_BUCKETS,
            n_segments: DEFAULT_SEGMENTS,
            time_window_seconds: DEFAULT_TIME_WINDOW_SECONDS,
            snapshot_interval_seconds: DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
            aae_state_dir: PathBuf::from(DEFAULT_AAE_STATE_DIR),
            per_token_exchange: false,
        }
    }
}

impl ConfAae {
    /// Validate the cross-field invariants.
    ///
    /// # Errors
    /// Returns a string describing the first violated invariant.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyn_riak::aae::config::ConfAae;
    /// let mut cfg = ConfAae::default();
    /// assert!(cfg.validate().is_ok());
    /// cfg.n_segments = 0;
    /// assert!(cfg.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        if self.n_time_buckets == 0 || self.n_time_buckets > MAX_TIME_BUCKETS {
            return Err(format!(
                "n_time_buckets {} out of range (1..={MAX_TIME_BUCKETS})",
                self.n_time_buckets
            ));
        }
        if self.n_segments == 0 || self.n_segments > MAX_SEGMENTS {
            return Err(format!(
                "n_segments {} out of range (1..={MAX_SEGMENTS})",
                self.n_segments
            ));
        }
        if self.time_window_seconds == 0 {
            return Err("time_window_seconds must be > 0".to_string());
        }
        if self.segment_interval_seconds == 0 {
            return Err("segment_interval_seconds must be > 0".to_string());
        }
        if self.full_sweep_interval_seconds == 0 {
            return Err("full_sweep_interval_seconds must be > 0".to_string());
        }
        if self.segment_interval_seconds > self.full_sweep_interval_seconds {
            return Err(format!(
                "segment_interval_seconds {} must be <= full_sweep_interval_seconds {}",
                self.segment_interval_seconds, self.full_sweep_interval_seconds
            ));
        }
        if self.snapshot_interval_seconds == 0 {
            return Err("snapshot_interval_seconds must be > 0".to_string());
        }
        Ok(())
    }

    /// The configured segment-tick cadence as a [`Duration`].
    #[must_use]
    pub fn segment_interval(&self) -> Duration {
        Duration::from_secs(self.segment_interval_seconds)
    }

    /// The configured full-sweep cadence as a [`Duration`].
    #[must_use]
    pub fn full_sweep_interval(&self) -> Duration {
        Duration::from_secs(self.full_sweep_interval_seconds)
    }

    /// The configured snapshot cadence as a [`Duration`].
    #[must_use]
    pub fn snapshot_interval(&self) -> Duration {
        Duration::from_secs(self.snapshot_interval_seconds)
    }

    /// Path where the snapshot file is written and read from.
    #[must_use]
    pub fn snapshot_path(&self) -> PathBuf {
        self.aae_state_dir.join(SNAPSHOT_FILE_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_validates() {
        assert!(ConfAae::default().validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_segments() {
        let cfg = ConfAae {
            n_segments: 0,
            ..ConfAae::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_segment_interval_above_sweep() {
        let cfg = ConfAae {
            segment_interval_seconds: 10,
            full_sweep_interval_seconds: 5,
            ..ConfAae::default()
        };
        assert!(cfg.validate().is_err());
    }
}
