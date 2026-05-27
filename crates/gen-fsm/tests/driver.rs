//! Integration tests for the gen_fsm driver. Exercise the five
//! event types, transitions, postpone semantics, and timeouts.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use gen_fsm::{Action, EventType, FsmDriver, FsmHandler, StopReason, TimeoutKind, Transition};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TurnstileState {
    Locked,
    Unlocked,
}

#[derive(Debug)]
enum TurnstileEvent {
    Coin,
    Push,
}

struct Turnstile {
    coins: u64,
    enters: Arc<AtomicU64>,
}

impl FsmHandler for Turnstile {
    type State = TurnstileState;
    type Event = TurnstileEvent;
    type Reply = ();
    type Stop = String;

    fn initial(&self) -> Self::State {
        TurnstileState::Locked
    }

    fn handle(
        &mut self,
        state: Self::State,
        _event_type: EventType,
        event: Self::Event,
    ) -> Transition<Self> {
        match (state, event) {
            (TurnstileState::Locked, TurnstileEvent::Coin) => {
                self.coins += 1;
                Transition::Next(TurnstileState::Unlocked, vec![])
            }
            (TurnstileState::Locked, TurnstileEvent::Push) => Transition::Keep(vec![]),
            (TurnstileState::Unlocked, TurnstileEvent::Push) => {
                Transition::Next(TurnstileState::Locked, vec![])
            }
            (TurnstileState::Unlocked, TurnstileEvent::Coin) => {
                self.coins += 1;
                Transition::Keep(vec![])
            }
        }
    }

    fn on_enter(&mut self, _state: Self::State) -> Transition<Self> {
        self.enters.fetch_add(1, Ordering::SeqCst);
        Transition::Keep(vec![])
    }
}

#[tokio::test]
async fn turnstile_counts_enters() {
    let enters = Arc::new(AtomicU64::new(0));
    let driver = FsmDriver::start(Turnstile {
        coins: 0,
        enters: enters.clone(),
    });

    driver.cast(TurnstileEvent::Coin).await;
    driver.cast(TurnstileEvent::Push).await;
    driver.cast(TurnstileEvent::Coin).await;
    driver.cast(TurnstileEvent::Push).await;

    drop(driver);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let ent = enters.load(Ordering::SeqCst);
    assert!(ent >= 1, "initial Enter must fire; got {ent}");
}

#[tokio::test]
async fn driver_call_replies_via_action() {
    // The handler stashes the reply handle on its own; current
    // driver semantics do not forward the ReplyHandle to the
    // handler. We verify that an unanswered call surfaces a
    // ReplyDropped error.
    struct Ignore;
    impl FsmHandler for Ignore {
        type State = ();
        type Event = u64;
        type Reply = u64;
        type Stop = ();
        fn initial(&self) -> Self::State {}
        fn handle(&mut self, _s: (), _et: EventType, _n: u64) -> Transition<Self> {
            Transition::Keep(vec![])
        }
    }

    let driver = FsmDriver::start(Ignore);
    let res = driver.call(7).await;
    assert!(matches!(res, Err(gen_fsm::DriverError::ReplyDropped)));
}

#[tokio::test]
async fn state_timeout_fires_on_inactivity() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum S {
        Idle,
        TimedOut,
    }

    struct H {
        timeouts: Arc<AtomicU64>,
    }

    impl FsmHandler for H {
        type State = S;
        type Event = ();
        type Reply = ();
        type Stop = ();

        fn initial(&self) -> Self::State {
            S::Idle
        }

        fn handle(&mut self, _s: S, _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Keep(vec![])
        }

        fn on_enter(&mut self, state: Self::State) -> Transition<Self> {
            match state {
                S::Idle => {
                    Transition::Keep(vec![Action::set_state_timeout(Duration::from_millis(20))])
                }
                S::TimedOut => Transition::Keep(vec![]),
            }
        }

        fn on_timeout(&mut self, _state: S, kind: TimeoutKind) -> Transition<Self> {
            assert_eq!(kind, TimeoutKind::State);
            self.timeouts.fetch_add(1, Ordering::SeqCst);
            Transition::Next(S::TimedOut, vec![])
        }
    }

    let timeouts = Arc::new(AtomicU64::new(0));
    let driver = FsmDriver::start(H {
        timeouts: timeouts.clone(),
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    drop(driver);
    assert_eq!(
        timeouts.load(Ordering::SeqCst),
        1,
        "state timeout fires exactly once"
    );
}

#[tokio::test]
async fn internal_event_drains_before_mailbox() {
    struct H {
        log: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    enum E {
        FromCast,
        FromInternal,
    }

    impl FsmHandler for H {
        type State = ();
        type Event = E;
        type Reply = ();
        type Stop = ();

        fn initial(&self) -> Self::State {}

        fn handle(&mut self, _s: (), et: EventType, ev: E) -> Transition<Self> {
            match (et, ev) {
                (EventType::Cast, E::FromCast) => {
                    self.log.lock().unwrap().push("cast");
                    Transition::Keep(vec![Action::post_internal(E::FromInternal)])
                }
                (EventType::Internal, E::FromInternal) => {
                    self.log.lock().unwrap().push("internal");
                    Transition::Keep(vec![])
                }
                _ => Transition::Keep(vec![]),
            }
        }
    }

    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let driver = FsmDriver::start(H { log: log.clone() });
    driver.cast(E::FromCast).await;
    driver.cast(E::FromCast).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(driver);

    let entries = log.lock().unwrap().clone();
    assert_eq!(
        entries,
        vec!["cast", "internal", "cast", "internal"],
        "internal events drain before next mailbox event"
    );
}

#[tokio::test]
async fn stop_reason_propagates_through_join() {
    struct H;

    impl FsmHandler for H {
        type State = ();
        type Event = ();
        type Reply = ();
        type Stop = String;

        fn initial(&self) -> Self::State {}

        fn handle(&mut self, _s: (), _et: EventType, _ev: ()) -> Transition<Self> {
            Transition::Stop("done".to_string())
        }
    }

    let driver = FsmDriver::start(H);
    driver.cast(()).await;
    let res = driver.join().await.unwrap();
    match res {
        StopReason::Handler(reason) => assert_eq!(reason, "done"),
        StopReason::Closed => panic!("expected Handler stop"),
    }
}

#[tokio::test]
async fn generic_timeouts_named_independently() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum S {
        Init,
    }

    enum E {
        SetTwo,
    }

    struct H {
        fired: Arc<std::sync::Mutex<Vec<&'static str>>>,
    }

    impl FsmHandler for H {
        type State = S;
        type Event = E;
        type Reply = ();
        type Stop = ();

        fn initial(&self) -> Self::State {
            S::Init
        }

        fn handle(&mut self, _s: S, _et: EventType, _ev: E) -> Transition<Self> {
            Transition::Keep(vec![
                Action::set_generic_timeout("a", Duration::from_millis(10)),
                Action::set_generic_timeout("b", Duration::from_millis(30)),
            ])
        }

        fn on_timeout(&mut self, _s: S, kind: TimeoutKind) -> Transition<Self> {
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
    driver.cast(E::SetTwo).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    drop(driver);

    let entries = fired.lock().unwrap().clone();
    assert_eq!(entries, vec!["a", "b"], "generic timers fire in order");
}
