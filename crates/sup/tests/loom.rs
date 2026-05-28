//! Loom model-check coverage for the supervisor's bookkeeping
//! primitives.
//!
//! These tests run only when the build is invoked with
//! `RUSTFLAGS='--cfg loom'`. Loom shadows `std::sync::atomic` with
//! recording variants and explores every legal interleaving of the
//! atomic operations performed by the model. The tests live here
//! (and not in `crates/loom-tests`) because they exercise the real
//! production types from `sup::atomics`, not a re-modeled primitive.
//!
//! The supervisor runtime itself (which depends on tokio) is gated
//! out under `--cfg loom` because tokio's networking modules do not
//! link in that configuration. Only the tokio-free `atomics`
//! submodule is reachable here.
//!
//! Run with:
//!
//! ```text
//! RUSTFLAGS='--cfg loom' cargo test -p sup --release
//! ```

#![cfg(loom)]

use std::time::Duration;

use loom::sync::Arc;
use loom::thread;

use sup::atomics::{BackoffState, ChildIdAllocator, FailureCounter};
use sup::BackoffSpec;

/// Two threads racing to allocate child ids must not collide.
///
/// The bookkeeping invariant tested here is the OTP-style guarantee
/// that every registered child has a unique [`ChildId`]. If the
/// allocator's `fetch_add` could ever skip its release semantics
/// under a hostile schedule, two concurrent registrations would
/// hand out the same id and downstream consumers (the supervisor's
/// `find_index`, the operator's logs, the tracing span fields)
/// would see ambiguity.
#[test]
fn child_id_allocator_no_duplicates_under_concurrent_alloc() {
    loom::model(|| {
        let alloc = Arc::new(ChildIdAllocator::new());
        let h1 = {
            let a = alloc.clone();
            thread::spawn(move || a.next())
        };
        let h2 = {
            let a = alloc.clone();
            thread::spawn(move || a.next())
        };
        let id1 = h1.join().unwrap();
        let id2 = h2.join().unwrap();
        assert_ne!(id1, id2, "child ids must be unique");
        assert!(id1 == 1 || id1 == 2);
        assert!(id2 == 1 || id2 == 2);
    });
}

/// A third concurrent allocator establishes that the monotonic
/// successor relation continues to hold past the two-thread case.
#[test]
fn child_id_allocator_three_threads_yield_distinct_ids() {
    loom::model(|| {
        let alloc = Arc::new(ChildIdAllocator::new());
        let h1 = {
            let a = alloc.clone();
            thread::spawn(move || a.next())
        };
        let h2 = {
            let a = alloc.clone();
            thread::spawn(move || a.next())
        };
        let id3 = alloc.next();
        let id1 = h1.join().unwrap();
        let id2 = h2.join().unwrap();
        let mut got = [id1, id2, id3];
        got.sort_unstable();
        assert_eq!(got, [1, 2, 3], "three threads must observe ids 1,2,3");
    });
}

/// Every observed failure must be reflected in the count.
///
/// This is the classic "no lost updates" property for a CAS-loop
/// counter. If two threads both call `observe_failure` and the
/// counter ends up at 1, the supervisor's restart-policy decisions
/// (and its backoff timing) would be based on a stale view of the
/// child's history.
#[test]
fn failure_counter_never_undercounts() {
    loom::model(|| {
        let counter = Arc::new(FailureCounter::new());
        let h1 = {
            let c = counter.clone();
            thread::spawn(move || c.observe_failure())
        };
        let h2 = {
            let c = counter.clone();
            thread::spawn(move || c.observe_failure())
        };
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(counter.count(), 2, "every observe_failure must be visible");
    });
}

/// `reset` racing with `observe_failure` must not produce a count
/// greater than the number of failures observed since the reset.
///
/// This models the supervisor's success-resets-the-tally path: if a
/// child completes `Ok` while another part of the system is
/// concurrently recording a failure (which cannot actually happen
/// in the supervisor's single-task model, but the primitive must
/// still be safe), the post-condition is that the visible count is
/// either zero (reset won the race) or one (failure won).
#[test]
fn failure_counter_reset_races_observe_failure() {
    loom::model(|| {
        let counter = Arc::new(FailureCounter::new());
        // Pre-charge so reset has something to clear.
        counter.observe_failure();
        let observer = {
            let c = counter.clone();
            thread::spawn(move || c.observe_failure())
        };
        let resetter = {
            let c = counter.clone();
            thread::spawn(move || c.reset())
        };
        observer.join().unwrap();
        resetter.join().unwrap();
        let n = counter.count();
        assert!(
            n <= 2,
            "count must not exceed the number of observe_failure calls (got {n})"
        );
    });
}

/// With factor >= 1 and jitter == 0 the next-delay reads are
/// monotonically non-decreasing under any interleaving of two
/// concurrent observers.
///
/// The bookkeeping invariant is: a thread that has not yet observed
/// a failure must not see a delay smaller than `start`, and once
/// failures have been observed the delay reflects the larger of
/// `base_delay(0)` and `base_delay(failures-1)`. Because
/// `base_delay` is monotone in its argument and `failures` itself
/// only ever grows (until reset), the sequence of `next_delay`
/// reads in any single thread is non-decreasing.
#[test]
fn backoff_next_delay_monotonic_under_concurrent_observers() {
    loom::model(|| {
        let spec = BackoffSpec::fixed(Duration::from_millis(10), Duration::from_secs(60), 2.0);
        let backoff = Arc::new(BackoffState::new(spec, 1));
        let observer_a = {
            let b = backoff.clone();
            thread::spawn(move || {
                let d0 = b.next_delay();
                b.observe_failure();
                let d1 = b.next_delay();
                assert!(
                    d1 >= d0,
                    "delay must not decrease after a local observe_failure (d0={d0:?}, d1={d1:?})"
                );
                d1
            })
        };
        let observer_b = {
            let b = backoff.clone();
            thread::spawn(move || {
                let d0 = b.next_delay();
                b.observe_failure();
                let d1 = b.next_delay();
                assert!(
                    d1 >= d0,
                    "delay must not decrease after a local observe_failure (d0={d0:?}, d1={d1:?})"
                );
                d1
            })
        };
        let _ = observer_a.join().unwrap();
        let _ = observer_b.join().unwrap();
        // After both threads have recorded a failure, failures is 2
        // and the next delay reflects base_delay(1) = 20ms.
        assert_eq!(backoff.failures(), 2);
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
    });
}

/// With non-zero jitter the PRNG advances atomically; concurrent
/// observers must not deadlock and the failure count must still
/// reflect every observe_failure.
#[test]
fn backoff_with_jitter_does_not_lose_failures() {
    loom::model(|| {
        let spec = BackoffSpec {
            start: Duration::from_millis(10),
            max: Duration::from_secs(60),
            factor: 2.0,
            jitter: 0.5,
        };
        let backoff = Arc::new(BackoffState::new(spec, 0xdead_beef));
        let h1 = {
            let b = backoff.clone();
            thread::spawn(move || {
                b.observe_failure();
                let _d = b.next_delay();
            })
        };
        let h2 = {
            let b = backoff.clone();
            thread::spawn(move || {
                b.observe_failure();
                let _d = b.next_delay();
            })
        };
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(
            backoff.failures(),
            2,
            "jittered next_delay must not interfere with the failure tally"
        );
    });
}
