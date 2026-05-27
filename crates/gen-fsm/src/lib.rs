//! State-functions-style finite state machine driver.
//!
//! This crate implements an FSM runtime modeled after OTP's
//! [`gen_statem`](https://www.erlang.org/doc/man/gen_statem.html) running
//! in `state_functions` callback mode. It is named `gen_fsm` because that
//! is the more recognizable name to readers without an Erlang background;
//! Erlang's original `gen_fsm` behaviour was deprecated in OTP 19 in favor
//! of `gen_statem`, but the *style* (one function per state, transitions
//! returned as values) is preserved here.
//!
//! # When to reach for this
//!
//! Use `gen_fsm` when you have a multi-step protocol or workflow whose
//! correctness depends on which state you are in:
//!
//! * Quorum-based read/write coordination (waiting for R or W replies,
//!   handling partial failure, scheduling repair).
//! * Anti-entropy exchange (snapshot, compare, repair, finalize).
//! * Hinted handoff (queue, send, ack, retry).
//! * Connection lifecycle (connecting, authenticating, ready, draining).
//!
//! Reach for a plain `async fn` when the workflow is purely linear
//! (no branching on partial failure, no per-state timeouts, no postponed
//! events). The point of `gen_fsm` is to make the state graph explicit
//! when there is one; do not impose it where there is not.
//!
//! # The five concepts
//!
//! 1. **State**: an enum variant identifying which state the FSM is in.
//!    States are typed; the compiler stops you from confusing them.
//! 2. **Data**: the state-independent context the FSM carries between
//!    state transitions. Lives inside `self`.
//! 3. **Event**: an input the FSM reacts to. Events come in five flavors:
//!    [`EventType::Call`] (synchronous request expecting a reply),
//!    [`EventType::Cast`] (asynchronous notification, no reply),
//!    [`EventType::Info`] (typed background message),
//!    [`EventType::Timeout`] (one of three kinds: state, event, or
//!    generic), [`EventType::Internal`] (a follow-up event the FSM
//!    posts to itself).
//! 4. **Action**: a side effect the FSM requests after handling an event.
//!    Examples: send a reply, postpone the current event until the next
//!    state, set a state timeout, post an internal event.
//! 5. **Transition**: the return value from a state function. Either
//!    `Keep(actions)` (stay in the current state, run the actions),
//!    `Next(new_state, actions)` (transition to `new_state`, run actions
//!    on the way), or `Stop(reason)` (terminate the FSM).
//!
//! # Postpone semantics
//!
//! A handler that returns [`Action::Postpone`] keeps the current event
//! in the FSM's mailbox to be redelivered on the next state change.
//! This eliminates the manual queue-myself dance that gen_fsm's
//! original API required.
//!
//! # Example
//!
//! ```no_run
//! use gen_fsm::{Action, EventType, FsmDriver, FsmHandler, Transition};
//!
//! #[derive(Clone, Copy, Debug, PartialEq, Eq)]
//! enum State {
//!     Locked,
//!     Unlocked,
//! }
//!
//! struct Turnstile {
//!     coins: u64,
//! }
//!
//! enum Event {
//!     Coin,
//!     Push,
//! }
//!
//! impl FsmHandler for Turnstile {
//!     type State = State;
//!     type Event = Event;
//!     type Reply = ();
//!     type Stop = &'static str;
//!
//!     fn initial(&self) -> Self::State {
//!         State::Locked
//!     }
//!
//!     fn handle(
//!         &mut self,
//!         state: Self::State,
//!         _event_type: EventType,
//!         event: Self::Event,
//!     ) -> Transition<Self> {
//!         match (state, event) {
//!             (State::Locked, Event::Coin) => {
//!                 self.coins += 1;
//!                 Transition::Next(State::Unlocked, vec![])
//!             }
//!             (State::Locked, Event::Push) => Transition::Keep(vec![]),
//!             (State::Unlocked, Event::Push) => {
//!                 Transition::Next(State::Locked, vec![])
//!             }
//!             (State::Unlocked, Event::Coin) => {
//!                 self.coins += 1;
//!                 Transition::Keep(vec![])
//!             }
//!         }
//!     }
//! }
//!
//! # async fn run() {
//! let driver = FsmDriver::start(Turnstile { coins: 0 });
//! driver.cast(Event::Coin).await;
//! driver.cast(Event::Push).await;
//! # }
//! ```
//!
//! # Tracing
//!
//! The driver emits structured logs around each transition. Subscribe
//! to the `gen_fsm` target to see the protocol's state graph in your
//! logs.

#![doc(html_root_url = "https://docs.rs/gen-fsm/0.0.1")]

mod action;
mod driver;
mod event;
mod handler;
mod transition;

pub use action::{Action, ReplyHandle};
pub use driver::{DriverError, FsmDriver, StopReason};
pub use event::{EventType, TimeoutKind};
pub use handler::FsmHandler;
pub use transition::Transition;
