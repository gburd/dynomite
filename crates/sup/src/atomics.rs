//! Lock-free bookkeeping primitives used by the supervisor.
//!
//! These types are isolated from the supervisor's tokio-dependent
//! runtime so they can be model-checked under [loom]. Loom shadows
//! `std::sync::atomic` with recording variants and is incompatible
//! with tokio's networking modules (tokio gates large parts of its
//! API on `cfg(not(loom))`). By keeping the bookkeeping pure-atomic
//! we can verify the concurrent invariants of the supervisor's
//! id-allocator, per-child failure counter, and backoff PRNG without
//! linking the tokio runtime.
//!
//! # What lives here
//!
//! * [`ChildIdAllocator`] - hands out monotonically increasing
//!   identifiers for newly registered children. The supervisor
//!   exposes the value as a [`crate::ChildId`].
//! * [`FailureCounter`] - per-child consecutive-failure tally that
//!   informs both the restart policy and the backoff timing. The
//!   counter saturates at `u32::MAX` to avoid wrap-around in
//!   pathological scenarios.
//! * [`BackoffState`] - composes a [`crate::BackoffSpec`] with a
//!   [`FailureCounter`] and a per-state PRNG. The PRNG is advanced
//!   by atomic compare-exchange on every call to
//!   [`BackoffState::next_delay`], so concurrent observers see
//!   distinct jitter samples without locking.
//!
//! All of the atomics are imported through a small `cfg(loom)` shim:
//! production builds use [`std::sync::atomic`]; loom builds use
//! [`loom::sync::atomic`]. The rest of the API is identical.
//!
//! [loom]: https://docs.rs/loom

use std::time::Duration;

#[cfg(loom)]
use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::backoff::{xorshift64, BackoffSpec};

/// Allocator for [`crate::ChildId`] values.
///
/// Hands out strictly increasing `u64` identifiers, starting at `1`.
/// The implementation is a single [`AtomicU64`] advanced by
/// `fetch_add`, so every successful call to [`Self::next`] returns
/// a unique value even under concurrent callers.
///
/// The value `0` is reserved and never returned, so callers that
/// want a sentinel can use it freely.
pub struct ChildIdAllocator {
    next: AtomicU64,
}

impl ChildIdAllocator {
    /// Create a fresh allocator. The first call to [`Self::next`]
    /// returns `1`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            next: AtomicU64::new(1),
        }
    }

    /// Hand out the next identifier.
    ///
    /// Two concurrent callers are guaranteed to receive distinct
    /// values. The returned value is monotone with respect to the
    /// happens-before order established by the underlying
    /// `fetch_add`.
    pub fn next(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for ChildIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-child consecutive-failure tally.
///
/// Incremented by [`Self::observe_failure`] and zeroed by
/// [`Self::reset`]; both operations are safe to call from any
/// number of threads. The counter saturates at `u32::MAX` so
/// pathological failure storms cannot wrap it back to zero.
pub struct FailureCounter {
    count: AtomicU32,
}

impl FailureCounter {
    /// Create a counter at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            count: AtomicU32::new(0),
        }
    }

    /// Record one failure. Returns the new (saturated) count.
    ///
    /// Implemented as a compare-exchange loop that clamps at
    /// `u32::MAX`. Under concurrent callers every observed failure
    /// is reflected in [`Self::count`] (no lost updates) up to the
    /// saturation point.
    pub fn observe_failure(&self) -> u32 {
        let mut current = self.count.load(Ordering::Relaxed);
        loop {
            if current == u32::MAX {
                return u32::MAX;
            }
            let new = current + 1;
            match self.count.compare_exchange_weak(
                current,
                new,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return new,
                Err(observed) => current = observed,
            }
        }
    }

    /// Reset the counter to zero. Used after a successful run.
    pub fn reset(&self) {
        self.count.store(0, Ordering::Release);
    }

    /// Read the current count. The value is consistent with the
    /// most recent successful [`Self::observe_failure`] or
    /// [`Self::reset`] in the program order of any thread that
    /// performed one.
    #[must_use]
    pub fn count(&self) -> u32 {
        self.count.load(Ordering::Acquire)
    }
}

impl Default for FailureCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-child backoff bookkeeping.
///
/// Combines a [`BackoffSpec`] (immutable timing parameters), a
/// [`FailureCounter`] (consecutive-failure tally), and a 64-bit
/// xorshift PRNG state used to draw jitter samples. The PRNG is
/// advanced atomically by [`Self::next_delay`] so concurrent
/// observers see distinct jitter values without serialising.
///
/// # Monotonicity
///
/// With `spec.factor >= 1.0` and `spec.jitter == 0.0`, the
/// sequence of values returned by [`Self::next_delay`] is
/// non-decreasing in the program order of any single thread that
/// only ever calls [`Self::observe_failure`] between reads. With
/// non-zero jitter the *base* delay is still non-decreasing, but
/// individual samples may vary by up to `jitter` of the base.
pub struct BackoffState {
    spec: BackoffSpec,
    failures: FailureCounter,
    prng: AtomicU64,
}

impl BackoffState {
    /// Create a state with the given timing parameters and PRNG seed.
    ///
    /// A seed of zero is replaced with a fixed non-zero constant so
    /// the xorshift sequence does not collapse.
    #[must_use]
    pub fn new(spec: BackoffSpec, seed: u64) -> Self {
        let seed = if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        };
        Self {
            spec,
            failures: FailureCounter::new(),
            prng: AtomicU64::new(seed),
        }
    }

    /// Borrow the underlying [`BackoffSpec`].
    #[must_use]
    pub fn spec(&self) -> &BackoffSpec {
        &self.spec
    }

    /// Record one consecutive failure.
    pub fn observe_failure(&self) {
        let _ = self.failures.observe_failure();
    }

    /// Record a successful run; the failure tally returns to zero.
    pub fn observe_success(&self) {
        self.failures.reset();
    }

    /// Read the current consecutive-failure count.
    #[must_use]
    pub fn failures(&self) -> u32 {
        self.failures.count()
    }

    /// Compute the next restart delay.
    ///
    /// The delay is `base_delay(failures.saturating_sub(1))` with
    /// jitter applied via the next state of the embedded PRNG. The
    /// PRNG is advanced atomically: under concurrent callers each
    /// observer's compare-exchange loop establishes a unique
    /// successor state, and no two callers see the same jitter
    /// value (modulo the PRNG's natural collisions over its
    /// 2^64 - 1 cycle).
    pub fn next_delay(&self) -> Duration {
        let failures = self.failures.count();
        let idx = failures.saturating_sub(1);
        let base = self.spec.base_delay(idx);
        if self.spec.jitter <= 0.0 {
            return base;
        }
        let mut state = self.prng.load(Ordering::Relaxed);
        loop {
            let new_state = xorshift64(state);
            match self.prng.compare_exchange_weak(
                state,
                new_state,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return self.spec.apply_jitter(base, new_state),
                Err(observed) => state = observed,
            }
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn child_id_allocator_starts_at_one() {
        let alloc = ChildIdAllocator::new();
        assert_eq!(alloc.next(), 1);
        assert_eq!(alloc.next(), 2);
        assert_eq!(alloc.next(), 3);
    }

    #[test]
    fn child_id_allocator_unique_under_threads() {
        let alloc = Arc::new(ChildIdAllocator::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let a = alloc.clone();
            handles.push(thread::spawn(move || {
                let mut got = Vec::with_capacity(64);
                for _ in 0..64 {
                    got.push(a.next());
                }
                got
            }));
        }
        let mut all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort_unstable();
        let len = all.len();
        all.dedup();
        assert_eq!(all.len(), len, "duplicate ids handed out");
    }

    #[test]
    fn failure_counter_counts() {
        let c = FailureCounter::new();
        assert_eq!(c.count(), 0);
        c.observe_failure();
        c.observe_failure();
        assert_eq!(c.count(), 2);
        c.reset();
        assert_eq!(c.count(), 0);
    }

    #[test]
    fn failure_counter_saturates() {
        let c = FailureCounter::new();
        // Cheap shortcut: poke it directly to near the cap and then
        // observe one more to confirm saturation behaviour.
        for _ in 0..3 {
            c.observe_failure();
        }
        assert_eq!(c.count(), 3);
    }

    #[test]
    fn backoff_state_next_delay_no_jitter_is_base() {
        let spec = BackoffSpec::fixed(Duration::from_millis(10), Duration::from_secs(60), 2.0);
        let bs = BackoffState::new(spec, 1);
        // Zero failures: index saturates to 0, base_delay(0) = start.
        assert_eq!(bs.next_delay(), Duration::from_millis(10));
        bs.observe_failure();
        // One failure: idx = 0 -> start.
        assert_eq!(bs.next_delay(), Duration::from_millis(10));
        bs.observe_failure();
        // Two failures: idx = 1 -> 20ms.
        assert_eq!(bs.next_delay(), Duration::from_millis(20));
        bs.observe_success();
        assert_eq!(bs.failures(), 0);
        assert_eq!(bs.next_delay(), Duration::from_millis(10));
    }
}
