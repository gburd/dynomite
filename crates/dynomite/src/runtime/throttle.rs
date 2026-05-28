//! Token-bucket admission control gate.
//!
//! [`Throttle`] is a thin tokio-aware wrapper around the
//! algorithm in the `throttle-core` crate. The core type owns
//! the atomic bucket state and the [`SystemClock`]-driven refill
//! arithmetic; this module adds:
//!
//! * a tokio-async [`Throttle::acquire`] that uses
//!   [`tokio::time::sleep`] in the wait loop, and
//! * a `throttle_wait_seconds{queue="..."}` Prometheus histogram
//!   that records the time spent inside [`Throttle::acquire`].
//!
//! The split lets the algorithm be model-checked under loom
//! (where tokio cannot link) while the production embed keeps
//! the same async-friendly surface.
//!
//! # Examples
//!
//! ```
//! use dynomite::runtime::Throttle;
//! let t = Throttle::new(8, 4); // burst 8, sustain 4 tokens/sec
//! assert!(t.try_acquire(8));   // burst the whole bucket
//! assert!(!t.try_acquire(8));  // empty now, fast-fail
//! ```

use std::time::{Duration, Instant};

use throttle_core::{SystemClock, Throttle as Inner};

pub use throttle_core::ThrottleError;

use crate::runtime::metrics;

/// Token-bucket admission control gate.
///
/// The bucket is initialised full, so the first burst of up to
/// `capacity` tokens is granted immediately.
pub struct Throttle {
    /// Static name used as the `queue` label on
    /// `throttle_wait_seconds`.
    name: &'static str,
    inner: Inner<SystemClock>,
}

impl Throttle {
    /// Build a new throttle with the given burst capacity and
    /// sustained refill rate. The bucket starts full.
    ///
    /// The `queue` metric label defaults to `"default"`. Use
    /// [`Throttle::with_name`] to pick a meaningful label.
    pub fn new(capacity: u64, refill_per_sec: u64) -> Self {
        Self::with_name("default", capacity, refill_per_sec)
    }

    /// Build a new throttle with an explicit metric label.
    pub fn with_name(name: &'static str, capacity: u64, refill_per_sec: u64) -> Self {
        // Eagerly touch the histogram so the first acquire does
        // not pay the registry-lock cost.
        let _ = metrics::throttle_wait().with_label_values(&[name]);
        Self {
            name,
            inner: Inner::new(capacity, refill_per_sec),
        }
    }

    /// Burst capacity (the maximum number of tokens that may be
    /// acquired in one go).
    pub fn capacity(&self) -> u64 {
        self.inner.capacity()
    }

    /// Sustained refill rate in tokens per second.
    pub fn refill_per_sec(&self) -> u64 {
        self.inner.refill_per_sec()
    }

    /// Best-effort snapshot of the currently available tokens.
    /// Useful for tests and diagnostics; do not branch on this in
    /// admission code (use [`Throttle::try_acquire`] instead).
    pub fn available(&self) -> u64 {
        self.inner.available()
    }

    /// Try to take `n` tokens. Returns `true` on success and
    /// `false` if the bucket does not currently hold `n` tokens.
    /// Refills the bucket from the elapsed wall-clock interval
    /// before checking.
    ///
    /// Requesting `n > capacity` always returns `false`: the
    /// bucket can never hold that many tokens, so blocking is
    /// pointless.
    pub fn try_acquire(&self, n: u64) -> bool {
        self.inner.try_acquire(n)
    }

    /// Acquire `n` tokens, waiting if necessary for the bucket to
    /// refill. The time spent waiting is recorded on the
    /// `throttle_wait_seconds` histogram.
    ///
    /// # Panics
    ///
    /// Panics if `n > capacity`. The bucket can never hold that
    /// many tokens, so an unconditional wait would be a deadlock.
    /// Panics if `refill_per_sec` is zero and the initial bucket
    /// cannot satisfy the request: the throttle has no way to
    /// ever recover, so a deadlock is the alternative.
    pub async fn acquire(&self, n: u64) {
        let capacity = self.inner.capacity();
        assert!(
            n <= capacity,
            "throttle: requested {n} tokens > capacity {capacity}",
        );
        if n == 0 {
            return;
        }
        let start = Instant::now();
        // Fast path: enough tokens already, no histogram cost.
        if self.inner.try_acquire(n) {
            return;
        }
        let refill = self.inner.refill_per_sec();
        assert!(
            refill > 0,
            "throttle: zero refill rate cannot satisfy acquire({n})",
        );
        loop {
            // Sleep just long enough that the missing fraction of
            // tokens is expected to have been refilled. The
            // `try_acquire` after the sleep folds in real elapsed
            // time, so we never grant more than the contract.
            let needed = n.saturating_sub(self.inner.available());
            let needed = needed.max(1);
            // Compute the wait in integer nanoseconds. Multiplying
            // by a billion in u128 avoids float precision loss
            // and tracks the same time domain as the core refill.
            let want_nanos = u128::from(needed).saturating_mul(1_000_000_000) / u128::from(refill);
            // Floor at 1ms so a fractional refill never spins
            // tightly, and ceiling at 1s so a misconfigured
            // throttle still polls regularly.
            let want_nanos = want_nanos.clamp(1_000_000, 1_000_000_000);
            let dur = Duration::from_nanos(u64::try_from(want_nanos).unwrap_or(u64::MAX));
            tokio::time::sleep(dur).await;
            if self.inner.try_acquire(n) {
                let waited = start.elapsed().as_secs_f64();
                metrics::throttle_wait()
                    .with_label_values(&[self.name])
                    .observe(waited);
                return;
            }
        }
    }
}

impl std::fmt::Debug for Throttle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Throttle")
            .field("name", &self.name)
            .field("capacity", &self.capacity())
            .field("refill_per_sec", &self.refill_per_sec())
            .field("available", &self.available())
            .finish_non_exhaustive()
    }
}
