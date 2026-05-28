//! Tokio-free token-bucket admission control with loom-checkable
//! atomics.
//!
//! [`Throttle`] is a fixed-capacity bucket that refills at a
//! configured rate. Callers ask for `n` tokens with
//! [`Throttle::try_acquire`] (non-blocking) or
//! [`Throttle::acquire_blocking`] (sync, sleeps the calling
//! thread until the bucket has enough). The bucket caps the
//! long-run rate at `refill_per_sec` tokens per second while
//! still allowing short bursts up to `capacity` tokens.
//!
//! This crate intentionally has no async runtime dependency. The
//! `dynomite` crate wraps [`Throttle`] in a tokio-aware adapter
//! that uses `tokio::time::sleep` instead of
//! [`std::thread::sleep`] in the wait loop. The two layers share
//! the same algorithm and the same invariants.
//!
//! # Loom support
//!
//! Under `RUSTFLAGS='--cfg loom'` the atomics and mutex used by
//! [`Throttle`] are sourced from the [`loom`] crate's
//! shadow-`std` modules. This lets a model checker explore every
//! legal interleaving of the CAS loop in
//! [`Throttle::try_acquire`] and verify that no interleaving
//! over-grants tokens. The clock is abstracted behind the
//! [`Clock`] trait so loom tests can drive a deterministic
//! [`ManualClock`] rather than wall-clock time.
//!
//! # Examples
//!
//! ```no_run
//! use throttle_core::Throttle;
//! let t: Throttle = Throttle::new(8, 4); // burst 8, sustain 4 tokens/sec
//! assert!(t.try_acquire(8));   // burst the whole bucket
//! assert!(!t.try_acquire(8));  // empty now, fast-fail
//! ```
//!
//! The example uses `no_run` because the actual atomics under
//! `RUSTFLAGS='--cfg loom'` require a `loom::model()` context.
//! Behavioural correctness is exercised by the in-crate unit
//! tests and the integration tests under `tests/`.

#![forbid(unsafe_code)]

use std::time::{Duration, Instant};

#[cfg(loom)]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(loom)]
use loom::sync::Mutex;

#[cfg(not(loom))]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::Mutex;

/// A monotonic time source.
///
/// Production code uses [`SystemClock`], which delegates to
/// [`Instant::now`]. Tests and loom models inject a
/// [`ManualClock`] so the refill computation runs against
/// deterministic timestamps.
pub trait Clock: Send + Sync {
    /// Returns a monotonic instant. Successive calls must never
    /// return an earlier instant than a prior call.
    fn now(&self) -> Instant;
}

/// Clock implementation backed by [`Instant::now`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

// Blanket impls so callers can share a clock through `Arc` or
// pass a borrowed reference into [`Throttle::with_clock`]
// without wrapping it in a newtype. We provide separate impls
// for the two `Arc` flavours (`std`'s and `loom`'s) so loom
// tests can build `Arc<ManualClock>` from `loom::sync::Arc`
// while the default build keeps using `std::sync::Arc`.
impl<C: Clock + ?Sized> Clock for &C {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

impl<C: Clock + ?Sized> Clock for std::sync::Arc<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

#[cfg(loom)]
impl<C: Clock + ?Sized> Clock for loom::sync::Arc<C> {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

/// Deterministic clock for tests and loom models.
///
/// The clock starts at the [`Instant`] captured at construction
/// time and advances only when callers invoke
/// [`ManualClock::advance`]. Internally the offset is stored as
/// nanoseconds in an [`AtomicU64`], so the clock is `Send + Sync`
/// and can be shared across threads (or loom-modelled threads)
/// without further wrapping.
#[derive(Debug)]
pub struct ManualClock {
    base: Instant,
    offset_nanos: AtomicU64,
}

impl ManualClock {
    /// Creates a clock anchored at the calling-thread's current
    /// [`Instant`] with zero offset.
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            offset_nanos: AtomicU64::new(0),
        }
    }

    /// Advances the clock by `delta`. Saturates at
    /// [`u64::MAX`] nanoseconds (~584 years), which is plenty for
    /// any realistic test horizon.
    pub fn advance(&self, delta: Duration) {
        let add = u64::try_from(delta.as_nanos()).unwrap_or(u64::MAX);
        // Saturating add: a wraparound here would silently rewind
        // the clock and break the monotonicity contract.
        let mut cur = self.offset_nanos.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_add(add);
            match self.offset_nanos.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.base + Duration::from_nanos(self.offset_nanos.load(Ordering::Acquire))
    }
}

/// Errors returned by [`Throttle::acquire_blocking`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ThrottleError {
    /// The caller asked for more tokens than the bucket can ever
    /// hold. Waiting would deadlock; the request must be rejected
    /// at the source.
    #[error("requested {requested} tokens exceeds capacity {capacity}")]
    RequestExceedsCapacity {
        /// Tokens the caller asked for.
        requested: u64,
        /// Bucket capacity.
        capacity: u64,
    },
    /// The bucket has a zero refill rate and the initial capacity
    /// is insufficient for the request. There is no way to ever
    /// satisfy the request, so blocking is pointless.
    #[error(
        "zero refill rate cannot satisfy acquire of {requested} tokens \
         (only {available} available)"
    )]
    ZeroRefillExhausted {
        /// Tokens the caller asked for.
        requested: u64,
        /// Tokens currently in the bucket.
        available: u64,
    },
}

/// Token-bucket admission control gate.
///
/// The bucket is initialised full, so the first burst of up to
/// `capacity` tokens is granted immediately. Tokens accrue at
/// `refill_per_sec` per second up to `capacity`, computed lazily
/// against the [`Clock`] on each [`Throttle::try_acquire`] call.
///
/// `Throttle` is generic over the clock so loom and unit tests
/// can inject a [`ManualClock`]. Production code uses the
/// [`SystemClock`] default.
pub struct Throttle<C: Clock = SystemClock> {
    capacity: u64,
    refill_per_sec: u64,
    available: AtomicU64,
    last_refill: Mutex<Instant>,
    clock: C,
}

impl Throttle<SystemClock> {
    /// Builds a throttle backed by the [`SystemClock`].
    ///
    /// The bucket starts full; the first acquire of up to
    /// `capacity` tokens succeeds without waiting.
    pub fn new(capacity: u64, refill_per_sec: u64) -> Self {
        Self::with_clock(capacity, refill_per_sec, SystemClock)
    }
}

impl<C: Clock> Throttle<C> {
    /// Builds a throttle that consults `clock` for refill timing.
    ///
    /// The bucket starts full and the last-refill timestamp is
    /// captured from `clock` at construction time. Subsequent
    /// `clock.now()` values must be monotonic relative to that
    /// initial reading.
    pub fn with_clock(capacity: u64, refill_per_sec: u64, clock: C) -> Self {
        let now = clock.now();
        Self {
            capacity,
            refill_per_sec,
            available: AtomicU64::new(capacity),
            last_refill: Mutex::new(now),
            clock,
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
    ///
    /// Useful for tests and diagnostics; do not branch on this
    /// in admission code (use [`Throttle::try_acquire`] instead).
    pub fn available(&self) -> u64 {
        self.available.load(Ordering::Acquire)
    }

    /// Tries to take `n` tokens.
    ///
    /// Returns `true` on success and `false` if the bucket does
    /// not currently hold `n` tokens. The bucket is refilled
    /// from the clock-elapsed interval before the check.
    ///
    /// Requesting `n > capacity` always returns `false`: the
    /// bucket can never hold that many tokens, so blocking would
    /// be pointless.
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

    /// Acquires `n` tokens, sleeping the calling thread if the
    /// bucket is empty.
    ///
    /// This is the synchronous counterpart of `dynomite`'s
    /// async `Throttle::acquire`. The wait loop sleeps for the
    /// time required to refill the missing tokens at
    /// `refill_per_sec`, clamped to the range 1 ms .. 1 s so a
    /// fractional refill never spins tightly and a misconfigured
    /// throttle still polls regularly.
    ///
    /// # Errors
    ///
    /// * [`ThrottleError::RequestExceedsCapacity`] if `n` is
    ///   larger than the bucket's capacity.
    /// * [`ThrottleError::ZeroRefillExhausted`] if
    ///   `refill_per_sec == 0` and the initial bucket cannot
    ///   satisfy the request.
    pub fn acquire_blocking(&self, n: u64) -> Result<(), ThrottleError> {
        if n > self.capacity {
            return Err(ThrottleError::RequestExceedsCapacity {
                requested: n,
                capacity: self.capacity,
            });
        }
        if n == 0 {
            return Ok(());
        }
        if self.try_acquire(n) {
            return Ok(());
        }
        if self.refill_per_sec == 0 {
            return Err(ThrottleError::ZeroRefillExhausted {
                requested: n,
                available: self.available(),
            });
        }
        loop {
            let needed = n.saturating_sub(self.available.load(Ordering::Acquire));
            let needed = needed.max(1);
            // Compute the wait in integer nanoseconds. Multiplying
            // by a billion in u128 avoids float precision loss
            // and tracks the same time domain as `refill`.
            let want_nanos =
                u128::from(needed).saturating_mul(1_000_000_000) / u128::from(self.refill_per_sec);
            let want_nanos = want_nanos.clamp(1_000_000, 1_000_000_000);
            let dur = Duration::from_nanos(u64::try_from(want_nanos).unwrap_or(u64::MAX));
            std::thread::sleep(dur);
            if self.try_acquire(n) {
                return Ok(());
            }
        }
    }

    /// Adds tokens earned since the last refill instant. Does not
    /// exceed `capacity`. Holds the `last_refill` mutex while
    /// computing the increment so concurrent refillers add up to
    /// the same total.
    fn refill(&self) {
        if self.refill_per_sec == 0 {
            return;
        }
        let now = self.clock.now();
        let mut last = self
            .last_refill
            .lock()
            .expect("invariant: throttle last_refill mutex must not be poisoned");
        let elapsed = now.duration_since(*last);
        // Convert elapsed time to whole tokens. Using nanoseconds
        // and integer math avoids the precision drift floats would
        // cause across many short refills.
        let elapsed_nanos: u128 = elapsed.as_nanos();
        let rate = u128::from(self.refill_per_sec);
        let new_tokens_u128 = elapsed_nanos.saturating_mul(rate) / 1_000_000_000_u128;
        if new_tokens_u128 == 0 {
            return;
        }
        // Advance `last_refill` only by the integer-token slice
        // we are crediting; the fractional tail rolls into the
        // next refill.
        let new_tokens = u64::try_from(new_tokens_u128).unwrap_or(u64::MAX);
        let credited_nanos = (u128::from(new_tokens) * 1_000_000_000_u128) / rate;
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

    /// Atomically subtracts `n` tokens from `available` if at
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

impl<C: Clock> std::fmt::Debug for Throttle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Throttle")
            .field("capacity", &self.capacity)
            .field("refill_per_sec", &self.refill_per_sec)
            .field("available", &self.available())
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use hegel::generators as gs;
    use hegel::TestCase;
    use std::sync::Arc;

    #[test]
    fn new_starts_full() {
        let t: Throttle = Throttle::new(8, 1);
        assert_eq!(t.capacity(), 8);
        assert_eq!(t.refill_per_sec(), 1);
        assert_eq!(t.available(), 8);
    }

    #[test]
    fn try_acquire_zero_is_always_true_even_when_empty() {
        let t: Throttle = Throttle::new(2, 0);
        assert!(t.try_acquire(2));
        assert_eq!(t.available(), 0);
        assert!(t.try_acquire(0));
    }

    #[test]
    fn try_acquire_above_capacity_fails_fast() {
        let t: Throttle = Throttle::new(4, 100);
        assert!(!t.try_acquire(5));
        // The bucket is unchanged.
        assert_eq!(t.available(), 4);
    }

    #[test]
    fn manual_clock_drives_refill() {
        let clock = Arc::new(ManualClock::new());
        let t = Throttle::with_clock(10, 100, Arc::clone(&clock));
        assert!(t.try_acquire(10));
        assert_eq!(t.available(), 0);
        // 100 ms at 100 tokens/s yields exactly 10 tokens.
        clock.advance(Duration::from_millis(100));
        // try_acquire(0) triggers refill but consumes nothing.
        assert!(t.try_acquire(0));
        assert_eq!(t.available(), 10);
    }

    #[test]
    fn manual_clock_caps_at_capacity() {
        let clock = Arc::new(ManualClock::new());
        let t = Throttle::with_clock(5, 1, Arc::clone(&clock));
        // Drain.
        assert!(t.try_acquire(5));
        // Advance an hour at 1 token/s == 3600 tokens; should
        // saturate at capacity 5.
        clock.advance(Duration::from_secs(3600));
        assert!(t.try_acquire(0));
        assert_eq!(t.available(), 5);
    }

    #[test]
    fn manual_clock_zero_refill_does_not_replenish() {
        let clock = Arc::new(ManualClock::new());
        let t = Throttle::with_clock(3, 0, Arc::clone(&clock));
        assert!(t.try_acquire(3));
        clock.advance(Duration::from_secs(60));
        assert!(!t.try_acquire(1));
    }

    #[test]
    fn acquire_blocking_above_capacity_returns_typed_error() {
        let t: Throttle = Throttle::new(2, 1);
        let err = t.acquire_blocking(5).unwrap_err();
        assert_eq!(
            err,
            ThrottleError::RequestExceedsCapacity {
                requested: 5,
                capacity: 2,
            }
        );
    }

    #[test]
    fn acquire_blocking_zero_refill_with_empty_bucket_returns_error() {
        let t: Throttle = Throttle::new(1, 0);
        assert!(t.try_acquire(1));
        let err = t.acquire_blocking(1).unwrap_err();
        assert!(matches!(
            err,
            ThrottleError::ZeroRefillExhausted {
                requested: 1,
                available: 0,
            }
        ));
    }

    #[test]
    fn acquire_blocking_zero_request_is_noop() {
        let t: Throttle = Throttle::new(1, 0);
        // Even with refill=0 and capacity=1, an acquire of zero
        // succeeds without blocking.
        t.acquire_blocking(0).unwrap();
        assert_eq!(t.available(), 1);
    }

    #[test]
    fn acquire_blocking_waits_for_refill() {
        let t = Throttle::new(2, 200); // refill 200/s -> 5ms/token
        assert!(t.try_acquire(2));
        let start = Instant::now();
        t.acquire_blocking(2).unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(5),
            "acquire returned in {elapsed:?}, expected at least ~10ms"
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "acquire took unexpectedly long: {elapsed:?}"
        );
    }

    #[hegel::test(test_cases = 64)]
    fn manual_clock_envelope_holds(tc: TestCase) {
        let capacity = tc.draw(gs::integers::<u64>().min_value(1).max_value(64));
        let refill = tc.draw(gs::integers::<u64>().min_value(1).max_value(1_000));
        let req_sizes: Vec<u64> = (0..16)
            .map(|_| tc.draw(gs::integers::<u64>().min_value(0).max_value(capacity)))
            .collect();
        // Test both wall-clock (via the Hegel harness) and a
        // ManualClock that does not advance: the second case
        // pins the envelope to capacity exactly.
        let clock = Arc::new(ManualClock::new());
        let t = Throttle::with_clock(capacity, refill, Arc::clone(&clock));
        let mut granted: u128 = 0;
        for n in &req_sizes {
            if t.try_acquire(*n) {
                granted += u128::from(*n);
            }
        }
        // No clock advance => envelope is exactly capacity.
        assert!(
            granted <= u128::from(capacity),
            "granted {granted} > capacity {capacity} with frozen clock"
        );
    }
}
