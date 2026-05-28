//! Loom model-checks for the [`Throttle`] CAS loop.
//!
//! Loom shadows the atomics and mutex used by the production
//! [`Throttle`] type with model-recording variants, then runs
//! every legal interleaving of the CAS loop. The invariant we
//! check is the same one production cares about: the bucket
//! never grants more tokens than the envelope
//! `capacity + refill_per_sec * elapsed` allows.
//!
//! Run via:
//!
//! ```text
//! RUSTFLAGS='--cfg loom' cargo test -p throttle-core --release
//! ```
//!
//! Without `--cfg loom` the file compiles to nothing.

#![cfg(loom)]

use std::time::Duration;

use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

use throttle_core::{ManualClock, Throttle};

/// Two threads each call `try_acquire(1)` twice on a bucket with
/// capacity 2 and zero refill. Across all interleavings the total
/// number of grants must be exactly 2: never more (overgrant) and
/// never less (the four calls collectively cover every token).
#[test]
fn two_threads_never_overgrant() {
    loom::model(|| {
        let clock = Arc::new(ManualClock::new());
        let throttle = Arc::new(Throttle::with_clock(2, 0, clock));
        let granted = Arc::new(AtomicU64::new(0));

        let t1 = {
            let t = throttle.clone();
            let g = granted.clone();
            thread::spawn(move || {
                if t.try_acquire(1) {
                    g.fetch_add(1, Ordering::SeqCst);
                }
                if t.try_acquire(1) {
                    g.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        let t2 = {
            let t = throttle.clone();
            let g = granted.clone();
            thread::spawn(move || {
                if t.try_acquire(1) {
                    g.fetch_add(1, Ordering::SeqCst);
                }
                if t.try_acquire(1) {
                    g.fetch_add(1, Ordering::SeqCst);
                }
            })
        };

        t1.join().unwrap();
        t2.join().unwrap();

        // Capacity 2; 4 try_acquire calls; refill is 0 and the
        // clock did not advance, so the envelope is exactly 2.
        let g = granted.load(Ordering::SeqCst);
        assert_eq!(g, 2, "expected exactly 2 grants under no-refill, got {g}");
        assert_eq!(throttle.available(), 0);
    });
}

/// After the bucket is drained, advancing the clock past the
/// refill quantum must let the next `try_acquire` succeed. We
/// drain on the main thread, advance the clock on a background
/// thread, and join: the post-join `try_acquire(1)` must always
/// observe the refill regardless of the interleaving order
/// between the advance and the load.
#[test]
fn refill_grants_resume_after_clock_advance() {
    loom::model(|| {
        let clock = Arc::new(ManualClock::new());
        // 1000 tokens/sec: 1 ms advance == 1 token.
        let throttle = Arc::new(Throttle::with_clock(1, 1_000, clock.clone()));

        // Drain.
        assert!(throttle.try_acquire(1));
        assert!(!throttle.try_acquire(1));

        let advancer = {
            let c = clock.clone();
            thread::spawn(move || {
                c.advance(Duration::from_millis(10));
            })
        };
        advancer.join().unwrap();

        // After the advance is visible, the bucket must hold at
        // least 1 token. The advance happened-before the join,
        // and the join happens-before this load.
        assert!(
            throttle.try_acquire(1),
            "refill did not become visible after clock advance"
        );
    });
}

/// `try_acquire(0)` is a no-op admission check. It must always
/// return true regardless of the bucket's current state, even
/// across racing acquisitions.
#[test]
fn try_acquire_zero_is_always_true() {
    loom::model(|| {
        let clock = Arc::new(ManualClock::new());
        let throttle = Arc::new(Throttle::with_clock(1, 0, clock));

        let racer = {
            let t = throttle.clone();
            thread::spawn(move || {
                let _ = t.try_acquire(1);
            })
        };

        // Whatever the racer does, our zero-token request must
        // succeed.
        assert!(throttle.try_acquire(0));

        racer.join().unwrap();
    });
}

/// A capacity-1 bucket with zero refill must serialise grants:
/// across all interleavings of two contending threads, exactly
/// one thread observes a successful `try_acquire`.
#[test]
fn capacity_one_serializes_grants() {
    loom::model(|| {
        let clock = Arc::new(ManualClock::new());
        let throttle = Arc::new(Throttle::with_clock(1, 0, clock));
        let winners = Arc::new(AtomicU64::new(0));

        let t1 = {
            let t = throttle.clone();
            let w = winners.clone();
            thread::spawn(move || {
                if t.try_acquire(1) {
                    w.fetch_add(1, Ordering::SeqCst);
                }
            })
        };
        let t2 = {
            let t = throttle.clone();
            let w = winners.clone();
            thread::spawn(move || {
                if t.try_acquire(1) {
                    w.fetch_add(1, Ordering::SeqCst);
                }
            })
        };

        t1.join().unwrap();
        t2.join().unwrap();

        let w = winners.load(Ordering::SeqCst);
        assert_eq!(
            w, 1,
            "capacity-1 bucket must hand out exactly 1 grant, got {w}"
        );
        assert_eq!(throttle.available(), 0);
    });
}
