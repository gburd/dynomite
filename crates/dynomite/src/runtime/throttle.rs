//! Token-bucket admission control gate.
//!
//! [`Throttle`] is a fixed-capacity bucket that refills at a
//! configured rate. Callers ask for `n` tokens with
//! [`Throttle::try_acquire`] (non-blocking) or
//! [`Throttle::acquire`] (await-until-available). The bucket caps
//! the long-run rate at `refill_per_sec` tokens per second while
//! still allowing short bursts up to `capacity` tokens.
//!
//! The throttle records the time spent inside [`Throttle::acquire`]
//! against the `throttle_wait_seconds{queue="..."}` histogram so
//! operators can see how often internal queues stall.
//!
//! # Examples
//!
//! ```
//! use dynomite::runtime::Throttle;
//! let t = Throttle::new(8, 4); // burst 8, sustain 4 tokens/sec
//! assert!(t.try_acquire(8));   // burst the whole bucket
//! assert!(!t.try_acquire(8));  // empty now, fast-fail
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::runtime::metrics;

/// Token-bucket admission control gate.
///
/// The bucket is initialised full, so the first burst of up to
/// `capacity` tokens is granted immediately.
pub struct Throttle {
    /// Static name used as the `queue` label on
    /// `throttle_wait_seconds`.
    name: &'static str,
    /// Maximum tokens the bucket holds at any instant.
    capacity: u64,
    /// Sustained refill rate, in tokens per second.
    refill_per_sec: u64,
    /// Currently available tokens. Bounded above by `capacity`.
    available: AtomicU64,
    /// Wall-clock instant of the last refill computation. The
    /// mutex serialises refill so floating-point fractional
    /// tokens are accumulated against a single observer.
    last_refill: Mutex<Instant>,
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
            capacity,
            refill_per_sec,
            available: AtomicU64::new(capacity),
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Burst capacity (the maximum number of tokens that may be
    /// acquired in one go).
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Sustained refill rate in tokens per second.
    pub fn refill_per_sec(&self) -> u64 {
        self.refill_per_sec
    }

    /// Best-effort snapshot of the currently available tokens.
    /// Useful for tests and diagnostics; do not branch on this in
    /// admission code (use [`Throttle::try_acquire`] instead).
    pub fn available(&self) -> u64 {
        self.available.load(Ordering::Acquire)
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
        if n > self.capacity {
            return false;
        }
        self.refill();
        if n == 0 {
            return true;
        }
        self.consume(n)
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
        assert!(
            n <= self.capacity,
            "throttle: requested {n} tokens > capacity {}",
            self.capacity,
        );
        if n == 0 {
            return;
        }
        let start = Instant::now();
        // Fast path: enough tokens already, no histogram cost.
        if self.try_acquire(n) {
            return;
        }
        assert!(
            self.refill_per_sec > 0,
            "throttle: zero refill rate cannot satisfy acquire({n})",
        );
        loop {
            // Sleep just long enough that the missing fraction of
            // tokens is expected to have been refilled. The
            // `try_acquire` after the sleep folds in real elapsed
            // time, so we never grant more than the contract.
            let needed = n.saturating_sub(self.available.load(Ordering::Acquire));
            let needed = needed.max(1);
            // Compute the wait in integer nanoseconds. Multiplying
            // by a billion in u128 avoids float precision loss
            // and tracks the same time domain as `refill`.
            let want_nanos =
                u128::from(needed).saturating_mul(1_000_000_000) / u128::from(self.refill_per_sec);
            // Floor at 1ms so a fractional refill never spins
            // tightly, and ceiling at 1s so a misconfigured
            // throttle still polls regularly.
            let want_nanos = want_nanos.clamp(1_000_000, 1_000_000_000);
            let dur = Duration::from_nanos(u64::try_from(want_nanos).unwrap_or(u64::MAX));
            tokio::time::sleep(dur).await;
            if self.try_acquire(n) {
                let waited = start.elapsed().as_secs_f64();
                metrics::throttle_wait()
                    .with_label_values(&[self.name])
                    .observe(waited);
                return;
            }
        }
    }

    /// Add tokens earned since the last refill instant. Does not
    /// exceed `capacity`. Holds the `last_refill` mutex while
    /// computing the increment so concurrent refillers add up to
    /// the same total.
    fn refill(&self) {
        if self.refill_per_sec == 0 {
            return;
        }
        let now = Instant::now();
        let mut last = self.last_refill.lock();
        let elapsed = now.duration_since(*last);
        // Convert elapsed time to whole tokens. Using nanoseconds
        // and integer math avoids the precision drift floats would
        // cause across many short refills.
        let elapsed_nanos: u128 = elapsed.as_nanos();
        let rate = u128::from(self.refill_per_sec);
        let new_tokens_u128 = elapsed_nanos.saturating_mul(rate) / 1_000_000_000u128;
        if new_tokens_u128 == 0 {
            return;
        }
        // Advance `last_refill` only by the integer-token slice
        // we are crediting; the fractional tail rolls into the
        // next refill.
        let new_tokens = u64::try_from(new_tokens_u128).unwrap_or(u64::MAX);
        let credited_nanos = (u128::from(new_tokens) * 1_000_000_000u128) / rate;
        *last += Duration::from_nanos(u64::try_from(credited_nanos).unwrap_or(u64::MAX));
        // Saturating add into `available`, capped at `capacity`.
        let mut cur = self.available.load(Ordering::Acquire);
        loop {
            let target = cur.saturating_add(new_tokens).min(self.capacity);
            if target == cur {
                break;
            }
            match self.available.compare_exchange_weak(
                cur,
                target,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Atomically subtract `n` tokens from `available` if at
    /// least `n` are currently held. Returns whether the
    /// subtraction succeeded.
    fn consume(&self, n: u64) -> bool {
        let mut cur = self.available.load(Ordering::Acquire);
        loop {
            if cur < n {
                return false;
            }
            match self.available.compare_exchange_weak(
                cur,
                cur - n,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

impl std::fmt::Debug for Throttle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Throttle")
            .field("name", &self.name)
            .field("capacity", &self.capacity)
            .field("refill_per_sec", &self.refill_per_sec)
            .field("available", &self.available())
            .finish_non_exhaustive()
    }
}
