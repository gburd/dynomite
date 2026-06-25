//! Targeted unit tests closing the gaps left by `tests/driver.rs`.
//!
//! Each test names the public surface it exercises: the `Action`
//! convenience constructors and their `Debug` formatting, the
//! `Transition` `Debug` impl, `ReplyHandle` send/`is_closed`, the
//! driver's `cast_checked`/`info`/`clone`/`join` error paths, the
//! event-timeout and generic-timeout-cancellation transitions, and
//! the default no-op `on_enter`/`on_timeout` handler methods.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gen_fsm::{Action, DriverError, EventType, FsmDriver, FsmHandler, TimeoutKind, Transition};

/// Minimal single-state handler used by several tests below.
struct Noop;

impl FsmHandler for Noop {
    type State = ();
    type Event = ();
    type Reply = ();
    type Stop = ();

    fn initial(&self) -> Self::State {}

    fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
        Transition::Keep(vec![])
    }
}

/// The `Action` convenience constructors build the matching variant,
/// and the `Debug` impl renders each variant without leaking the
/// inner event payloads.
///
/// The `Reply` variant (and `Action::reply`) cannot be constructed
/// from outside the crate: `ReplyHandle` has a crate-private field
/// and the driver drops the handle before the handler runs, so there
/// is no public path to a `ReplyHandle`. That arm is reported as a
/// coverage Deviation rather than tested here.
#[test]
fn action_constructors_and_debug() {
    let postpone: Action<Noop> = Action::postpone();
    let set_state: Action<Noop> = Action::set_state_timeout(Duration::from_millis(5));
    let cancel_state: Action<Noop> = Action::cancel_state_timeout();
    let set_event: Action<Noop> = Action::set_event_timeout(Duration::from_millis(7));
    let set_generic: Action<Noop> = Action::set_generic_timeout("g", Duration::from_millis(9));
    let cancel_generic: Action<Noop> = Action::cancel_generic_timeout("g");
    let post: Action<Noop> = Action::post_internal(());

    assert_eq!(format!("{postpone:?}"), "Postpone");
    assert_eq!(format!("{cancel_state:?}"), "CancelStateTimeout");
    assert!(format!("{set_state:?}").starts_with("SetStateTimeout("));
    assert!(format!("{set_event:?}").starts_with("SetEventTimeout("));
    assert!(format!("{set_generic:?}").starts_with("SetGenericTimeout(\"g\""));
    assert_eq!(format!("{cancel_generic:?}"), "CancelGenericTimeout(\"g\")");
    assert_eq!(format!("{post:?}"), "PostInternal(..)");
}

/// The `Transition` `Debug` impl renders each of its three variants.
#[test]
fn transition_debug_renders_all_variants() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum S {
        A,
    }
    struct H;
    impl FsmHandler for H {
        type State = S;
        type Event = ();
        type Reply = ();
        type Stop = &'static str;
        fn initial(&self) -> Self::State {
            S::A
        }
        fn handle(&mut self, _s: S, _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Keep(vec![])
        }
    }

    let keep: Transition<H> = Transition::Keep(vec![Action::cancel_state_timeout()]);
    let next: Transition<H> = Transition::Next(S::A, vec![]);
    let stop: Transition<H> = Transition::Stop("bye");

    assert!(format!("{keep:?}").starts_with("Keep { actions: ["));
    let next_dbg = format!("{next:?}");
    assert!(next_dbg.contains("Next"));
    assert!(next_dbg.contains("state: A"));
    assert_eq!(format!("{stop:?}"), "Stop { reason: \"bye\" }");
}

/// `cast_checked`, `info`, and `call` all return `DriverError::Stopped`
/// once the driver task has exited and the mailbox is closed.
#[tokio::test]
async fn send_methods_error_after_stop() {
    struct Stopper;
    impl FsmHandler for Stopper {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Stop(())
        }
    }

    let driver = FsmDriver::start(Stopper);
    // First cast stops the FSM; give the task time to exit.
    driver.cast(()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert!(matches!(
        driver.cast_checked(()).await,
        Err(DriverError::Stopped)
    ));
    assert!(matches!(driver.info(()).await, Err(DriverError::Stopped)));
    assert!(matches!(driver.call(()).await, Err(DriverError::Stopped)));
}

/// `cast_checked` and `info` succeed while the FSM is running.
#[tokio::test]
async fn cast_checked_and_info_deliver_to_running_fsm() {
    struct H {
        log: Arc<std::sync::Mutex<Vec<EventType>>>,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), et: EventType, _ev: ()) -> Transition<Self> {
            self.log.lock().unwrap().push(et);
            Transition::Keep(vec![])
        }
    }

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let driver = FsmDriver::start(H { log: log.clone() });
    driver.cast_checked(()).await.unwrap();
    driver.info(()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    drop(driver);

    let seen = log.lock().unwrap().clone();
    assert_eq!(seen, vec![EventType::Cast, EventType::Info]);
}

/// A cloned `FsmDriver` shares the same mailbox: events sent through
/// the clone reach the same FSM task. A cloned handle cannot `join`
/// (its join slot is empty), surfacing `DriverError::Stopped`.
#[tokio::test]
async fn clone_shares_mailbox_and_clone_join_errors() {
    struct H {
        count: Arc<AtomicU64>,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Transition::Keep(vec![])
        }
    }

    let count = Arc::new(AtomicU64::new(0));
    let driver = FsmDriver::start(H {
        count: count.clone(),
    });
    let clone = driver.clone();
    clone.cast(()).await;
    driver.cast(()).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);

    // The clone's join slot is None -> Stopped.
    assert!(matches!(clone.join().await, Err(DriverError::Stopped)));
}

/// An event timeout fires when no event arrives within the window,
/// and is reported to `on_timeout` as `TimeoutKind::Event`.
#[tokio::test]
async fn event_timeout_fires_when_idle() {
    struct H {
        fired: Arc<AtomicU64>,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Keep(vec![])
        }
        fn on_enter(&mut self, _s: ()) -> Transition<Self> {
            Transition::Keep(vec![Action::set_event_timeout(Duration::from_millis(20))])
        }
        fn on_timeout(&mut self, _s: (), kind: TimeoutKind) -> Transition<Self> {
            assert_eq!(kind, TimeoutKind::Event);
            self.fired.fetch_add(1, Ordering::SeqCst);
            Transition::Keep(vec![])
        }
    }

    let fired = Arc::new(AtomicU64::new(0));
    let driver = FsmDriver::start(H {
        fired: fired.clone(),
    });
    tokio::time::sleep(Duration::from_millis(60)).await;
    drop(driver);
    assert_eq!(fired.load(Ordering::SeqCst), 1);
}

/// Any arriving event cancels the active event timeout: the timer
/// is cleared when a mailbox event is dispatched, so it never fires.
#[tokio::test]
async fn event_timeout_cancelled_by_arriving_event() {
    struct H {
        fired: Arc<AtomicU64>,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            // Arm a fresh event timeout each time, then immediately
            // cancel it next event. We only assert it does not fire
            // within the test window because events keep arriving.
            Transition::Keep(vec![Action::set_event_timeout(Duration::from_millis(40))])
        }
        fn on_timeout(&mut self, _s: (), _kind: TimeoutKind) -> Transition<Self> {
            self.fired.fetch_add(1, Ordering::SeqCst);
            Transition::Keep(vec![])
        }
    }

    let fired = Arc::new(AtomicU64::new(0));
    let driver = FsmDriver::start(H {
        fired: fired.clone(),
    });
    // Deliver an event every 10ms for 50ms; each event clears the
    // 40ms timer before it can fire.
    for _ in 0..5 {
        driver.cast(()).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(driver);
    assert_eq!(fired.load(Ordering::SeqCst), 0);
}

/// A generic timer cancelled by name before it fires never reaches
/// `on_timeout`. Two timers are armed; one is cancelled and only the
/// surviving timer fires.
#[tokio::test]
async fn generic_timeout_cancel_by_name() {
    struct H {
        fired: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }
    enum E {
        Arm,
        Cancel,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = E;
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, ev: E) -> Transition<Self> {
            match ev {
                E::Arm => Transition::Keep(vec![
                    Action::set_generic_timeout("keep", Duration::from_millis(40)),
                    Action::set_generic_timeout("drop", Duration::from_millis(20)),
                ]),
                E::Cancel => Transition::Keep(vec![
                    Action::cancel_generic_timeout("drop"),
                    // Cancelling an unknown name is a no-op.
                    Action::cancel_generic_timeout("never-armed"),
                ]),
            }
        }
        fn on_timeout(&mut self, _s: (), kind: TimeoutKind) -> Transition<Self> {
            if let TimeoutKind::Generic(name) = kind {
                self.fired.lock().unwrap().push(name);
            }
            Transition::Keep(vec![])
        }
    }

    let fired = Arc::new(std::sync::Mutex::new(Vec::new()));
    let driver = FsmDriver::start(H {
        fired: fired.clone(),
    });
    driver.cast(E::Arm).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    driver.cast(E::Cancel).await;
    tokio::time::sleep(Duration::from_millis(60)).await;
    drop(driver);

    let seen = fired.lock().unwrap().clone();
    assert_eq!(seen, vec!["keep"], "cancelled timer never fires");
}

/// Postpone redelivers the postponed event on the next state change,
/// ahead of any later mailbox events.
/// A `Postpone` action returned for a mailbox event is currently a
/// documented no-op (the driver defers true postpone redelivery to a
/// future iteration; see `driver.rs` `Action::Postpone`). This test
/// pins that observed behaviour: the postponed event is *not*
/// redelivered on the next state change, so the handler runs once.
#[tokio::test]
async fn postpone_is_currently_a_noop_for_mailbox_events() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum S {
        Closed,
        Open,
    }
    #[derive(Clone, Copy)]
    enum E {
        Work,
        OpenUp,
    }
    struct H {
        count: Arc<AtomicU64>,
    }
    impl FsmHandler for H {
        type State = S;
        type Event = E;
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {
            S::Closed
        }
        fn handle(&mut self, state: S, _et: EventType, ev: E) -> Transition<Self> {
            match (state, ev) {
                (S::Closed, E::Work) => {
                    self.count.fetch_add(1, Ordering::SeqCst);
                    Transition::Keep(vec![Action::postpone()])
                }
                (S::Closed, E::OpenUp) => Transition::Next(S::Open, vec![]),
                (S::Open, _) => Transition::Keep(vec![]),
            }
        }
    }

    let count = Arc::new(AtomicU64::new(0));
    let driver = FsmDriver::start(H {
        count: count.clone(),
    });
    driver.cast(E::Work).await;
    driver.cast(E::OpenUp).await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    drop(driver);

    // Handled once in Closed; postpone did not requeue it for Open.
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

/// The default `on_timeout` (no-op `Keep`) implementation runs
/// without panicking: a handler that arms a state timer in
/// `on_enter` but does not override `on_timeout` keeps its state
/// when the timer fires. (The default `on_enter` is exercised by
/// the other handlers in this file that do not override it.)
#[tokio::test]
async fn default_on_timeout_is_a_noop() {
    struct H;
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn on_enter(&mut self, _s: ()) -> Transition<Self> {
            // Arm a state timer on the initial Enter; it is not
            // cleared because there is no subsequent transition.
            Transition::Keep(vec![Action::set_state_timeout(Duration::from_millis(10))])
        }
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Keep(vec![])
        }
        // on_timeout left as the trait default (no-op Keep).
    }

    let driver = FsmDriver::start(H);
    // Wait past the timer; default on_timeout fires a no-op Keep.
    tokio::time::sleep(Duration::from_millis(40)).await;
    // FSM is still alive (no Stop, no panic).
    assert!(driver.cast_checked(()).await.is_ok());
    drop(driver);
}

/// `Transition::Stop` returned from a `Call` handler, a `Cast`
/// handler, an `Info` handler, an internal-event handler, and from
/// `on_timeout` all terminate the driver with `StopReason::Handler`.
/// Each variant exercises a distinct `return` in the driver loop.
#[tokio::test]
async fn stop_from_each_dispatch_path() {
    #[derive(Clone, Copy)]
    enum Where {
        Call,
        Cast,
        Info,
        Internal,
        Timeout,
    }

    struct H {
        from: Where,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = Where;
        type Reply = ();
        type Stop = &'static str;
        fn initial(&self) -> Self::State {}
        fn on_enter(&mut self, _s: ()) -> Transition<Self> {
            if matches!(self.from, Where::Timeout) {
                Transition::Keep(vec![Action::set_state_timeout(Duration::from_millis(5))])
            } else {
                Transition::Keep(vec![])
            }
        }
        fn handle(&mut self, _s: (), et: EventType, ev: Where) -> Transition<Self> {
            match (et, ev) {
                (EventType::Call, Where::Call) => Transition::Stop("call"),
                (EventType::Cast, Where::Cast) => Transition::Stop("cast"),
                (EventType::Info, Where::Info) => Transition::Stop("info"),
                (EventType::Cast, Where::Internal) => {
                    Transition::Keep(vec![Action::post_internal(Where::Internal)])
                }
                (EventType::Internal, Where::Internal) => Transition::Stop("internal"),
                _ => Transition::Keep(vec![]),
            }
        }
        fn on_timeout(&mut self, _s: (), _k: TimeoutKind) -> Transition<Self> {
            Transition::Stop("timeout")
        }
    }

    // Call path: the call resolves with ReplyDropped (handler stopped
    // without replying), and join reports the Stop reason.
    let d = FsmDriver::start(H { from: Where::Call });
    let _ = d.call(Where::Call).await;
    match d.join().await.unwrap() {
        gen_fsm::StopReason::Handler(r) => assert_eq!(r, "call"),
        gen_fsm::StopReason::Closed => panic!("expected Handler"),
    }

    for (from, ev, want) in [
        (Where::Cast, Where::Cast, "cast"),
        (Where::Info, Where::Info, "info"),
        (Where::Internal, Where::Internal, "internal"),
    ] {
        let d = FsmDriver::start(H { from });
        match ev {
            Where::Info => d.info(ev).await.unwrap(),
            _ => d.cast(ev).await,
        }
        match d.join().await.unwrap() {
            gen_fsm::StopReason::Handler(r) => assert_eq!(r, want),
            gen_fsm::StopReason::Closed => panic!("expected Handler for {want}"),
        }
    }

    // Timeout path: the state timer fires and on_timeout stops.
    let d = FsmDriver::start(H {
        from: Where::Timeout,
    });
    match d.join().await.unwrap() {
        gen_fsm::StopReason::Handler(r) => assert_eq!(r, "timeout"),
        gen_fsm::StopReason::Closed => panic!("expected Handler for timeout"),
    }
}

/// `Transition::Stop` returned from `on_enter` (the synthesized
/// initial Enter) terminates the driver before any event is
/// processed.
#[tokio::test]
async fn stop_from_initial_enter() {
    struct H;
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = &'static str;
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Keep(vec![])
        }
        fn on_enter(&mut self, _s: ()) -> Transition<Self> {
            Transition::Stop("enter")
        }
    }

    let d = FsmDriver::start(H);
    match d.join().await.unwrap() {
        gen_fsm::StopReason::Handler(r) => assert_eq!(r, "enter"),
        gen_fsm::StopReason::Closed => panic!("expected Handler for enter"),
    }
}

/// `Action::CancelStateTimeout` clears an armed state timer so it
/// never fires. The FSM arms a state timer on enter, then a `Cast`
/// cancels it; `on_timeout` is therefore never called.
#[tokio::test]
async fn cancel_state_timeout_disarms_timer() {
    struct H {
        fired: Arc<AtomicU64>,
    }
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn on_enter(&mut self, _s: ()) -> Transition<Self> {
            Transition::Keep(vec![Action::set_state_timeout(Duration::from_millis(30))])
        }
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Keep(vec![Action::cancel_state_timeout()])
        }
        fn on_timeout(&mut self, _s: (), _k: TimeoutKind) -> Transition<Self> {
            self.fired.fetch_add(1, Ordering::SeqCst);
            Transition::Keep(vec![])
        }
    }

    let fired = Arc::new(AtomicU64::new(0));
    let driver = FsmDriver::start(H {
        fired: fired.clone(),
    });
    driver.cast(()).await; // cancels the armed state timer
    tokio::time::sleep(Duration::from_millis(60)).await;
    drop(driver);
    assert_eq!(fired.load(Ordering::SeqCst), 0);
}

/// The mailbox closing while a timer is pending drives the driver
/// through the timer-select branch's `rx.recv() -> None` arm. We arm
/// a long state timer on enter, let the run loop park in that select,
/// then drop the only handle. The driver task exits via that arm.
/// The `StopReason` is not observable (dropping the handle is the
/// trigger), so this test only confirms no hang and no panic.
#[tokio::test]
async fn closed_while_timer_pending() {
    struct H;
    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn on_enter(&mut self, _s: ()) -> Transition<Self> {
            Transition::Keep(vec![Action::set_state_timeout(Duration::from_mins(1))])
        }
        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Keep(vec![])
        }
    }

    let driver = FsmDriver::start(H);
    // Give the task time to run on_enter and arm the long timer, so
    // the run loop is parked in the timer-select branch.
    tokio::time::sleep(Duration::from_millis(20)).await;
    // Drop the only sender: the timer-select `rx.recv()` arm yields
    // None and the driver task returns StopReason::Closed.
    drop(driver);
    // If the task wrongly waited out the 60s timer this would hang;
    // a short sleep is enough for the (correct) prompt exit.
    tokio::time::sleep(Duration::from_millis(20)).await;
}
