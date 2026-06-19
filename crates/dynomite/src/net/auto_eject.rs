//! Consecutive-failure auto-eject decision state.
//!
//! When a backend datastore (or peer) accumulates more than
//! `failure_limit` consecutive connection or operation failures,
//! the engine ejects it: subsequent calls into the pool return
//! [`AutoEjectState::Ejected`] until the eject window configured
//! by `retry_after` has elapsed. After the window passes,
//! [`AutoEject::record_attempt`] resumes returning
//! [`AutoEjectState::Reachable`] (the next outbound connect attempt
//! will then run, and a successful connect resets the failure
//! counter through [`AutoEject::record_success`]).
//!
//! The same shared policy is reused by [`crate::net::pool::ConnPool`]
//! and by the [`crate::cluster`] layer; lifting the policy out of any
//! one caller keeps the implementation single-sourced.
//!
//! # Examples
//!
//! ```
//! use dynomite::net::auto_eject::{AutoEject, AutoEjectState};
//! use std::time::{Duration, Instant};
//!
//! let mut ae = AutoEject::new(true, 2, Duration::from_millis(50));
//! let now = Instant::now();
//! assert_eq!(ae.record_attempt(now), AutoEjectState::Reachable);
//!
//! ae.record_failure(now);
//! ae.record_failure(now);
//! // After two consecutive failures the host is ejected for 50ms.
//! assert_eq!(ae.record_attempt(now), AutoEjectState::Ejected);
//!
//! let later = now + Duration::from_millis(60);
//! assert_eq!(ae.record_attempt(later), AutoEjectState::Reachable);
//!
//! ae.record_success(later);
//! assert_eq!(ae.failure_count(), 0);
//! ```

use std::time::{Duration, Instant};

/// Result of asking [`AutoEject`] whether a target is reachable.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum AutoEjectState {
    /// Target is reachable. The caller may proceed.
    Reachable,
    /// Target was auto-ejected and the eject window has not yet
    /// elapsed. The caller must skip this target.
    Ejected,
}

/// Failure tracker that decides whether a target is currently
/// auto-ejected.
///
/// The struct is purely synchronous: it never schedules timers and
/// never holds locks. The tokio-driven dispatch layer queries it
/// before issuing every outbound request and feeds back the result
/// through [`AutoEject::record_success`] or
/// [`AutoEject::record_failure`].
#[derive(Debug, Clone)]
pub struct AutoEject {
    enabled: bool,
    failure_limit: u32,
    retry_after: Duration,
    failures: u32,
    next_retry: Option<Instant>,
}

impl AutoEject {
    /// Construct a fresh tracker. `enabled` mirrors the
    /// `auto_eject_hosts` knob from the YAML config.
    /// `failure_limit` mirrors `server_failure_limit`. `retry_after`
    /// mirrors `server_retry_timeout_ms` rendered as a
    /// [`Duration`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::auto_eject::AutoEject;
    /// use std::time::Duration;
    ///
    /// let ae = AutoEject::new(true, 3, Duration::from_secs(1));
    /// assert!(ae.is_enabled());
    /// assert_eq!(ae.failure_limit(), 3);
    /// ```
    #[must_use]
    pub fn new(enabled: bool, failure_limit: u32, retry_after: Duration) -> Self {
        Self {
            enabled,
            failure_limit,
            retry_after,
            failures: 0,
            next_retry: None,
        }
    }

    /// True when auto-eject is enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::auto_eject::AutoEject;
    /// use std::time::Duration;
    /// assert!(!AutoEject::new(false, 1, Duration::from_secs(1)).is_enabled());
    /// ```
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Configured failure limit before ejecting.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::auto_eject::AutoEject;
    /// use std::time::Duration;
    /// assert_eq!(AutoEject::new(true, 5, Duration::from_secs(1)).failure_limit(), 5);
    /// ```
    #[must_use]
    pub fn failure_limit(&self) -> u32 {
        self.failure_limit
    }

    /// Eject window length.
    #[must_use]
    pub fn retry_after(&self) -> Duration {
        self.retry_after
    }

    /// Current consecutive-failure count.
    #[must_use]
    pub fn failure_count(&self) -> u32 {
        self.failures
    }

    /// Instant after which the target should be retried, when an
    /// eject is currently active.
    #[must_use]
    pub fn next_retry(&self) -> Option<Instant> {
        self.next_retry
    }

    /// Test whether the caller should proceed (`Reachable`) or skip
    /// (`Ejected`) at the given instant.
    ///
    /// The caller passes `now` so the function stays deterministic
    /// in tests.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::auto_eject::{AutoEject, AutoEjectState};
    /// use std::time::{Duration, Instant};
    /// let mut ae = AutoEject::new(true, 1, Duration::from_millis(10));
    /// let now = Instant::now();
    /// ae.record_failure(now);
    /// assert_eq!(ae.record_attempt(now), AutoEjectState::Ejected);
    /// ```
    pub fn record_attempt(&mut self, now: Instant) -> AutoEjectState {
        if !self.enabled {
            return AutoEjectState::Reachable;
        }
        match self.next_retry {
            Some(eta) if now < eta => AutoEjectState::Ejected,
            Some(_) => {
                // Eject window has elapsed; clear the marker so the
                // caller can retry. The failure counter stays at
                // `failure_limit` so a single follow-up failure
                // re-ejects immediately.
                self.next_retry = None;
                AutoEjectState::Reachable
            }
            None => AutoEjectState::Reachable,
        }
    }

    /// Record a successful operation.
    ///
    /// Resets the consecutive-failure counter and clears any active
    /// eject window. After a success, the next failure starts a
    /// fresh streak from one (so the host has to fail
    /// `failure_limit` more times before being re-ejected).
    ///
    /// `_now` is currently unused but accepted for parity with
    /// [`record_attempt`](Self::record_attempt) so callers can
    /// supply a deterministic clock in tests; future revisions may
    /// use it to record time-to-recovery metrics.
    pub fn record_success(&mut self, _now: Instant) {
        self.failures = 0;
        self.next_retry = None;
    }

    /// Record a failed operation. Returns the new state of the
    /// tracker.
    ///
    /// When the consecutive-failure count reaches
    /// `failure_limit`, the function arms the eject window starting
    /// at `now + retry_after`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::auto_eject::{AutoEject, AutoEjectState};
    /// use std::time::{Duration, Instant};
    /// let mut ae = AutoEject::new(true, 2, Duration::from_secs(1));
    /// let now = Instant::now();
    /// assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
    /// assert_eq!(ae.record_failure(now), AutoEjectState::Ejected);
    /// ```
    pub fn record_failure(&mut self, now: Instant) -> AutoEjectState {
        self.failures = self.failures.saturating_add(1);
        if self.enabled && self.failures >= self.failure_limit {
            self.next_retry = Some(now + self.retry_after);
            AutoEjectState::Ejected
        } else {
            AutoEjectState::Reachable
        }
    }

    /// Reset the tracker to its post-construction state.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::net::auto_eject::AutoEject;
    /// use std::time::{Duration, Instant};
    /// let mut ae = AutoEject::new(true, 1, Duration::from_millis(10));
    /// ae.record_failure(Instant::now());
    /// ae.reset();
    /// assert_eq!(ae.failure_count(), 0);
    /// ```
    pub fn reset(&mut self) {
        self.failures = 0;
        self.next_retry = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_never_ejects() {
        let mut ae = AutoEject::new(false, 1, Duration::from_secs(1));
        let now = Instant::now();
        for _ in 0..5 {
            assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
        }
        assert_eq!(ae.record_attempt(now), AutoEjectState::Reachable);
    }

    #[test]
    fn ejects_after_threshold_and_recovers_after_window() {
        let mut ae = AutoEject::new(true, 3, Duration::from_millis(50));
        let now = Instant::now();
        assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
        assert_eq!(ae.record_failure(now), AutoEjectState::Reachable);
        assert_eq!(ae.record_failure(now), AutoEjectState::Ejected);
        assert_eq!(ae.record_attempt(now), AutoEjectState::Ejected);
        let after = now + Duration::from_millis(51);
        assert_eq!(ae.record_attempt(after), AutoEjectState::Reachable);
    }

    #[test]
    fn record_success_clears_state() {
        let mut ae = AutoEject::new(true, 2, Duration::from_secs(1));
        let now = Instant::now();
        ae.record_failure(now);
        ae.record_failure(now);
        assert_eq!(ae.record_attempt(now), AutoEjectState::Ejected);
        ae.record_success(now);
        assert_eq!(ae.record_attempt(now), AutoEjectState::Reachable);
        assert_eq!(ae.failure_count(), 0);
    }

    #[test]
    fn accessors_echo_configuration() {
        // The config accessors report the values passed to new().
        let ae = AutoEject::new(true, 5, Duration::from_millis(250));
        assert!(ae.is_enabled());
        assert_eq!(ae.failure_limit(), 5);
        assert_eq!(ae.retry_after(), Duration::from_millis(250));
        assert!(ae.next_retry().is_none());
        assert!(!AutoEject::new(false, 1, Duration::from_secs(1)).is_enabled());
    }

    #[test]
    fn next_retry_is_set_once_ejected() {
        // next_retry exposes the armed eject deadline.
        let mut ae = AutoEject::new(true, 1, Duration::from_millis(10));
        let now = Instant::now();
        assert!(ae.next_retry().is_none());
        ae.record_failure(now);
        assert_eq!(ae.next_retry(), Some(now + Duration::from_millis(10)));
    }

    #[test]
    fn reset_restores_post_construction_state() {
        // reset clears both the failure count and the eject window.
        let mut ae = AutoEject::new(true, 1, Duration::from_millis(10));
        let now = Instant::now();
        ae.record_failure(now);
        assert!(ae.next_retry().is_some());
        ae.reset();
        assert_eq!(ae.failure_count(), 0);
        assert!(ae.next_retry().is_none());
        assert_eq!(ae.record_attempt(now), AutoEjectState::Reachable);
    }
}
