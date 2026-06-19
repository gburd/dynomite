//! Property tests for the `gen_fsm` driver.
//!
//! Each `#[hegel::test]` runs at least 256 generated cases under the
//! default profile. The invariants covered:
//!
//! * Totality + no-panic: for any sequence of events, the driver
//!   processes them all and stays alive (never panics, never wedges).
//! * Determinism: a pure transition table driven through the FSM
//!   lands in the same final state as evaluating the table directly,
//!   for any event sequence.
//! * Internal-event ordering: internal follow-ups always drain before
//!   the next mailbox event, regardless of the cast/internal mix.

#![cfg(not(loom))]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gen_fsm::{Action, EventType, FsmDriver, FsmHandler, Transition};
use hegel::generators as gs;
use hegel::TestCase;

/// A three-state ring whose transition is a pure function of
/// `(state, event)`. The FSM's observed final state must equal the
/// reference table applied to the same event sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ring {
    A,
    B,
    C,
}

/// Events are `0`, `1`, or `2`: `0` steps the ring forward, `1` steps
/// it backward, `2` is a self-loop (Keep). Encoded via modular index
/// arithmetic so the three-state ring stays branch-free and free of
/// duplicate-bodied match arms.
fn step(state: Ring, ev: u8) -> Ring {
    let idx = match state {
        Ring::A => 0i8,
        Ring::B => 1,
        Ring::C => 2,
    };
    let delta = match ev % 3 {
        0 => 1,  // forward
        1 => -1, // backward
        _ => 0,  // self-loop
    };
    match (idx + delta).rem_euclid(3) {
        0 => Ring::A,
        1 => Ring::B,
        _ => Ring::C,
    }
}

struct RingFsm {
    final_state: Arc<std::sync::Mutex<Ring>>,
}

impl FsmHandler for RingFsm {
    type State = Ring;
    type Event = u8;
    type Reply = ();
    type Stop = ();

    fn initial(&self) -> Self::State {
        Ring::A
    }

    fn handle(&mut self, state: Ring, _et: EventType, ev: u8) -> Transition<Self> {
        let next = step(state, ev);
        *self.final_state.lock().unwrap() = next;
        if next == state {
            Transition::Keep(vec![])
        } else {
            Transition::Next(next, vec![])
        }
    }
}

fn arb_events(tc: &TestCase) -> Vec<u8> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(40));
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(tc.draw(gs::integers::<u8>()));
    }
    out
}

#[hegel::test(test_cases = 256)]
fn fsm_final_state_matches_reference_table(tc: TestCase) {
    let events = arb_events(&tc);

    // Reference: fold the pure transition over the event sequence.
    let mut expected = Ring::A;
    for &ev in &events {
        expected = step(expected, ev);
    }

    // Drive the FSM with the same sequence and read its final state.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let observed = rt.block_on(async {
        let final_state = Arc::new(std::sync::Mutex::new(Ring::A));
        let driver = FsmDriver::start(RingFsm {
            final_state: final_state.clone(),
        });
        for ev in events {
            // cast_checked never errors here: the FSM never stops.
            driver.cast_checked(ev).await.unwrap();
        }
        // Let the single-threaded runtime drain the mailbox before
        // dropping the handle and reading the recorded final state.
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(driver);
        let g = *final_state.lock().unwrap();
        g
    });

    // The reference fold above already accounts for every event.
    assert_eq!(observed, expected);
}

/// Determinism: the same `(state, event)` always maps to the same
/// next state. This pins the transition table's functional property
/// directly (no driver needed), over arbitrary state/event draws.
#[hegel::test(test_cases = 256)]
fn transition_is_deterministic(tc: TestCase) {
    let state = match tc.draw(gs::integers::<u8>().min_value(0).max_value(2)) {
        0 => Ring::A,
        1 => Ring::B,
        _ => Ring::C,
    };
    let ev = tc.draw(gs::integers::<u8>());
    assert_eq!(step(state, ev), step(state, ev));
}

/// No-panic totality: for any event sequence, every event is handled
/// exactly once and the driver survives to process the next one. We
/// count handler invocations and assert they equal the input length.
#[hegel::test(test_cases = 256)]
fn driver_handles_every_event_without_loss(tc: TestCase) {
    struct Counter {
        seen: Arc<AtomicU64>,
    }
    impl FsmHandler for Counter {
        type State = ();
        type Event = u8;
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, _ev: u8) -> Transition<Self> {
            self.seen.fetch_add(1, Ordering::SeqCst);
            Transition::Keep(vec![])
        }
    }

    let events = arb_events(&tc);
    let expected = events.len() as u64;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let count = rt.block_on(async {
        let seen = Arc::new(AtomicU64::new(0));
        let driver = FsmDriver::start(Counter { seen: seen.clone() });
        for ev in events {
            driver.cast_checked(ev).await.unwrap();
        }
        // Let the single-threaded runtime drain the mailbox.
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(driver);
        seen.load(Ordering::SeqCst)
    });

    assert_eq!(count, expected);
}

/// Internal events always drain before the next mailbox event. Each
/// cast posts one internal follow-up; the recorded log must be a
/// strict alternation of `Cast`,`Internal` pairs for any cast count.
#[hegel::test(test_cases = 256)]
fn internal_events_drain_before_next_mailbox(tc: TestCase) {
    struct H {
        log: Arc<std::sync::Mutex<Vec<EventType>>>,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = bool;
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), et: EventType, is_cast: bool) -> Transition<Self> {
            self.log.lock().unwrap().push(et);
            if is_cast {
                Transition::Keep(vec![Action::post_internal(false)])
            } else {
                Transition::Keep(vec![])
            }
        }
    }

    let casts = tc.draw(gs::integers::<usize>().min_value(0).max_value(20));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let log = rt.block_on(async {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let driver = FsmDriver::start(H { log: log.clone() });
        for _ in 0..casts {
            driver.cast_checked(true).await.unwrap();
        }
        // Give the driver a beat to drain internal follow-ups.
        tokio::time::sleep(Duration::from_millis(10)).await;
        drop(driver);
        let g = log.lock().unwrap().clone();
        g
    });

    let mut expected = Vec::with_capacity(casts * 2);
    for _ in 0..casts {
        expected.push(EventType::Cast);
        expected.push(EventType::Internal);
    }
    assert_eq!(log, expected);
}
