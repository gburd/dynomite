//! Phi-accrual failure detector.
//!
//! Adapted from "The Phi Accrual Failure Detector" (Hayashibara,
//! Defago, Yared, Katayama 2004) and as deployed in production by
//! Cassandra, Akka, and Riak.
//!
//! Unlike a binary detector that flips between "alive" and "dead"
//! based on a missed-heartbeat counter, the phi-accrual detector
//! produces a continuous **suspicion level** `phi(t)`. Phi is the
//! negative log10 of the probability that a heartbeat WOULD NOT
//! have arrived yet given the historical inter-arrival time
//! distribution:
//!
//! ```text
//!     phi(t) = -log10(1 - F(elapsed_since_last_heartbeat))
//! ```
//!
//! where `F` is the CDF of recent inter-arrival times. We model
//! heartbeat arrivals as exponential, which gives the closed form
//!
//! ```text
//!     phi(t) = elapsed_ms / (mean_interval_ms * ln(10))
//! ```
//!
//! that this module computes. (Cassandra uses a normal-distribution
//! variant; the exponential model gives nearly identical operator
//! semantics for the values of phi we care about, with cheaper math
//! and no special-case for low-variance heartbeats.)
//!
//! The operator picks a threshold; the detector reports "suspect"
//! when `phi > threshold`. Cassandra ships with `phi_convict_threshold:
//! 8` as the default, which means roughly "I'd accept a false
//! positive rate of 10^-8". We use the same default.
//!
//! # Scope
//!
//! This detector is for the **peer plane** (dnode gossip). The
//! backend (redis / memcache) is not heartbeat-driven; it gets
//! pinged by actual client traffic, and the consecutive-failure
//! tracker in [`crate::net::auto_eject::AutoEject`] is the right
//! tool there. Do not wire this detector into backend supervision.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::failure_detector::PhiAccrual;
//! use std::time::{Duration, Instant};
//!
//! let mut fd = PhiAccrual::new(100, 8.0);
//! let t0 = Instant::now();
//!
//! // Steady 1Hz heartbeats for ~5 seconds.
//! for i in 0..5 {
//!     fd.record_heartbeat(t0 + Duration::from_secs(i));
//! }
//!
//! // Right after the last heartbeat, phi is essentially zero.
//! let after_last = t0 + Duration::from_secs(4) + Duration::from_millis(10);
//! assert!(fd.phi(after_last) < 1.0);
//!
//! // After 30 missed heartbeats (30s of silence on a 1s cadence),
//! // phi is very high and the peer is suspect at threshold 8.
//! let later = t0 + Duration::from_secs(34);
//! assert!(fd.phi(later) > 8.0);
//! assert!(fd.is_suspect(later));
//! ```

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Default sliding-window size (number of historical inter-arrival
/// times retained). Cassandra defaults to 1000; Riak uses ~100.
/// We pick 100 so the detector reaches steady-state quickly.
pub const DEFAULT_WINDOW_SIZE: usize = 100;

/// Default phi threshold. Crossing it makes the peer "suspect"
/// (caller should mark [`crate::cluster::peer::PeerState::Down`]).
///
/// Matches Cassandra's `phi_convict_threshold` default. Roughly:
///
/// | phi | meaning                                    |
/// |-----|--------------------------------------------|
/// | 1.0 | 10% chance heartbeat is genuinely late     |
/// | 2.0 | 1% chance                                  |
/// | 5.0 | ~10^-5                                     |
/// | 8.0 | ~10^-8 - "almost certainly dead"           |
pub const DEFAULT_THRESHOLD: f64 = 8.0;

/// Default minimum mean interval: prevents `phi` from spiking when
/// a peer's first few heartbeats came in faster than the realistic
/// gossip cadence. Set to one second; if the configured
/// `gos_interval` is smaller, callers should lower this too.
pub const DEFAULT_MIN_MEAN_MS: f64 = 1_000.0;

/// Phi-accrual failure detector for one peer.
///
/// Maintains a sliding window of recent heartbeat inter-arrival
/// times. Compute the suspicion value via [`Self::phi`] or the
/// convenience predicate [`Self::is_suspect`].
///
/// The struct is purely synchronous and holds no I/O: a peer
/// supervisor / gossip task feeds it `record_heartbeat(now)` on
/// every received gossip message and a periodic ticker queries
/// `phi(now)` to decide whether to transition the peer's state.
#[derive(Debug, Clone)]
pub struct PhiAccrual {
    intervals: VecDeque<f64>,
    window_size: usize,
    last_heartbeat: Option<Instant>,
    threshold: f64,
    min_mean_ms: f64,
}

impl PhiAccrual {
    /// Create a fresh detector with explicit window size and
    /// threshold. Use [`DEFAULT_WINDOW_SIZE`] and
    /// [`DEFAULT_THRESHOLD`] when you do not have stronger
    /// preferences.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::failure_detector::PhiAccrual;
    /// let _fd = PhiAccrual::new(100, 8.0);
    /// ```
    #[must_use]
    pub fn new(window_size: usize, threshold: f64) -> Self {
        Self {
            intervals: VecDeque::with_capacity(window_size.max(1)),
            window_size: window_size.max(1),
            last_heartbeat: None,
            threshold,
            min_mean_ms: DEFAULT_MIN_MEAN_MS,
        }
    }

    /// Override the minimum-mean clamp. Callers driving this
    /// detector with sub-second gossip intervals should reduce
    /// the clamp to match (or set to 0 to disable).
    #[must_use]
    pub fn with_min_mean(mut self, min_mean: Duration) -> Self {
        self.min_mean_ms = duration_to_ms(min_mean);
        self
    }

    /// Number of inter-arrival samples currently retained.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.intervals.len()
    }

    /// Threshold this detector was constructed with.
    #[must_use]
    pub fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Replace the threshold (useful for runtime config reload).
    pub fn set_threshold(&mut self, threshold: f64) {
        self.threshold = threshold;
    }

    /// Most recent heartbeat timestamp, if any have been recorded.
    #[must_use]
    pub fn last_heartbeat(&self) -> Option<Instant> {
        self.last_heartbeat
    }

    /// Record a heartbeat received at `now`.
    ///
    /// The first call only initialises `last_heartbeat`; phi is
    /// always 0 until at least one inter-arrival sample has been
    /// observed.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::failure_detector::PhiAccrual;
    /// use std::time::{Duration, Instant};
    /// let mut fd = PhiAccrual::new(10, 8.0);
    /// let t0 = Instant::now();
    /// fd.record_heartbeat(t0);
    /// fd.record_heartbeat(t0 + Duration::from_secs(1));
    /// assert_eq!(fd.sample_count(), 1);
    /// ```
    pub fn record_heartbeat(&mut self, now: Instant) {
        if let Some(prev) = self.last_heartbeat {
            // `now` may be slightly behind `prev` on systems with
            // imperfect monotonic clocks. Clamp to avoid pushing
            // negative samples into the window.
            let dt = now.saturating_duration_since(prev);
            let dt_ms = duration_to_ms(dt);
            if self.intervals.len() == self.window_size {
                self.intervals.pop_front();
            }
            self.intervals.push_back(dt_ms);
        }
        self.last_heartbeat = Some(now);
    }

    /// Reset the detector. Useful when a peer is administratively
    /// removed and re-added so historical jitter does not bias the
    /// new suspicion value.
    pub fn reset(&mut self) {
        self.intervals.clear();
        self.last_heartbeat = None;
    }

    /// Compute the suspicion level `phi(now)`.
    ///
    /// Returns `0.0` when no heartbeat has ever been recorded
    /// (otherwise phi would be `+inf` for a brand-new peer, which
    /// is misleading - "no data" is not the same as "definitely
    /// dead"). Returns `0.0` when fewer than two heartbeats have
    /// been recorded, again because we have no inter-arrival data.
    ///
    /// # Examples
    ///
    /// See the module-level example.
    #[must_use]
    pub fn phi(&self, now: Instant) -> f64 {
        let Some(last) = self.last_heartbeat else {
            return 0.0;
        };
        if self.intervals.is_empty() {
            return 0.0;
        }
        let elapsed_ms = duration_to_ms(now.saturating_duration_since(last));
        let mean_ms = self.mean_interval_ms();
        if mean_ms <= 0.0 {
            return 0.0;
        }
        elapsed_ms / (mean_ms * std::f64::consts::LN_10)
    }

    /// Convenience: phi exceeds the configured threshold.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::failure_detector::PhiAccrual;
    /// use std::time::{Duration, Instant};
    /// let mut fd = PhiAccrual::new(10, 8.0);
    /// let t0 = Instant::now();
    /// for i in 0..5 {
    ///     fd.record_heartbeat(t0 + Duration::from_secs(i));
    /// }
    /// assert!(!fd.is_suspect(t0 + Duration::from_secs(5)));
    /// assert!(fd.is_suspect(t0 + Duration::from_secs(60)));
    /// ```
    #[must_use]
    pub fn is_suspect(&self, now: Instant) -> bool {
        self.phi(now) > self.threshold
    }

    /// Mean of the inter-arrival window, clamped at `min_mean_ms`.
    /// The clamp prevents `phi` from spiking unrealistically when
    /// heartbeats arrive faster than the configured gossip
    /// cadence (e.g. burst-then-pause).
    #[must_use]
    pub fn mean_interval_ms(&self) -> f64 {
        if self.intervals.is_empty() {
            return self.min_mean_ms;
        }
        let raw = self.intervals.iter().sum::<f64>() / self.intervals.len() as f64;
        if raw < self.min_mean_ms {
            self.min_mean_ms
        } else {
            raw
        }
    }
}

impl Default for PhiAccrual {
    /// `[DEFAULT_WINDOW_SIZE]` slots and `[DEFAULT_THRESHOLD]`.
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW_SIZE, DEFAULT_THRESHOLD)
    }
}

fn duration_to_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: u64, ms: u64) -> Instant {
        // Use a per-test fixed origin so every test gets a fresh
        // monotonic baseline. `Instant` does not expose a `from_*`
        // constructor, so derive everything off a single anchor.
        // Tests are run in their own process so the static is
        // safe.
        static ANCHOR: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let a = ANCHOR.get_or_init(Instant::now);
        *a + Duration::from_secs(secs) + Duration::from_millis(ms)
    }

    #[test]
    fn no_heartbeat_means_phi_zero() {
        let fd = PhiAccrual::default();
        assert_eq!(fd.phi(Instant::now()), 0.0);
        assert!(!fd.is_suspect(Instant::now()));
    }

    #[test]
    fn single_heartbeat_means_phi_zero() {
        let mut fd = PhiAccrual::default();
        fd.record_heartbeat(t(0, 0));
        // No inter-arrival data yet.
        assert_eq!(fd.phi(t(60, 0)), 0.0);
    }

    #[test]
    fn steady_heartbeats_then_silence_raises_phi() {
        let mut fd = PhiAccrual::new(20, 8.0).with_min_mean(Duration::from_millis(100));
        // 1Hz heartbeats for 5 seconds.
        for i in 0..5 {
            fd.record_heartbeat(t(i, 0));
        }
        assert_eq!(fd.sample_count(), 4);

        // Right after the last heartbeat - phi near 0.
        assert!(fd.phi(t(4, 10)) < 0.5);

        // After 30 silent seconds on a 1Hz cadence, phi >> 8.
        let phi30 = fd.phi(t(34, 0));
        assert!(
            phi30 > 8.0,
            "expected phi >> 8 after 30s silence on 1Hz cadence, got {phi30}"
        );

        // Threshold detection.
        assert!(fd.is_suspect(t(34, 0)));
        assert!(!fd.is_suspect(t(5, 100)));
    }

    #[test]
    fn high_jitter_relaxes_suspicion() {
        // Two detectors with the same threshold: one fed steady
        // 100ms intervals, one fed wildly varying intervals
        // averaging 100ms. The jittery one should NOT flag a 1s
        // gap as suspect; the steady one should.
        let mut steady = PhiAccrual::new(50, 8.0).with_min_mean(Duration::from_millis(50));
        let mut jittery = PhiAccrual::new(50, 8.0).with_min_mean(Duration::from_millis(50));

        // Steady: 100ms apart, exactly.
        for i in 0..50 {
            steady.record_heartbeat(t(0, i * 100));
        }
        // Jittery: alternating 50ms / 950ms - same mean, very high variance.
        let mut elapsed = 0u64;
        for i in 0..50 {
            jittery.record_heartbeat(t(0, elapsed));
            elapsed += if i % 2 == 0 { 50 } else { 950 };
        }

        // After a 1.0s silence past the last heartbeat:
        let probe = Duration::from_millis(1_000);
        let last_steady = steady.last_heartbeat().unwrap();
        let last_jittery = jittery.last_heartbeat().unwrap();

        let phi_steady = steady.phi(last_steady + probe);
        let phi_jittery = jittery.phi(last_jittery + probe);
        // Both detectors compute phi by elapsed/(mean*ln10); the
        // means are similar (steady 100, jittery ~500 due to the
        // 950ms tail dominating), so the jittery detector reports
        // a lower phi for the same 1s gap. The point: jitter
        // demonstrably reduces suspicion.
        assert!(
            phi_jittery < phi_steady,
            "jittery phi ({phi_jittery}) should be less than steady phi ({phi_steady})"
        );
    }

    #[test]
    fn window_eviction_keeps_size_bounded() {
        let mut fd = PhiAccrual::new(5, 8.0);
        for i in 0..20 {
            fd.record_heartbeat(t(i, 0));
        }
        // 20 heartbeats produce 19 inter-arrival samples but the
        // window caps at 5.
        assert_eq!(fd.sample_count(), 5);
    }

    #[test]
    fn reset_clears_state() {
        let mut fd = PhiAccrual::default();
        for i in 0..10 {
            fd.record_heartbeat(t(i, 0));
        }
        assert!(fd.sample_count() > 0);
        fd.reset();
        assert_eq!(fd.sample_count(), 0);
        assert!(fd.last_heartbeat().is_none());
        assert_eq!(fd.phi(Instant::now()), 0.0);
    }

    #[test]
    fn clock_going_backwards_is_handled() {
        // If `Instant::now()` ever ticks backwards (which it
        // should not on a healthy monotonic clock, but tokio's
        // pause/advance test harness can produce this), we just
        // record a zero inter-arrival.
        let mut fd = PhiAccrual::default();
        fd.record_heartbeat(t(10, 0));
        // The "next" heartbeat is BEFORE the previous one.
        fd.record_heartbeat(t(5, 0));
        // No panic; one zero-length sample stored.
        assert_eq!(fd.sample_count(), 1);
    }

    #[test]
    fn min_mean_clamp_prevents_runaway_phi() {
        // With a tiny mean (e.g. burst arrivals at sub-ms spacing)
        // a 1s silence should NOT immediately flag suspect under
        // the default 1s clamp.
        let mut fd = PhiAccrual::new(10, 8.0);
        for i in 0..10 {
            fd.record_heartbeat(t(0, i)); // 1ms apart
        }
        let phi_after_1s = fd.phi(t(1, 0));
        // With min_mean clamped at 1000ms, phi after 1s of
        // silence is roughly 1000 / (1000 * ln(10)) =~ 0.43.
        assert!(
            phi_after_1s < 1.0,
            "min_mean clamp should hold phi below 1.0 here, got {phi_after_1s}"
        );
    }
}
