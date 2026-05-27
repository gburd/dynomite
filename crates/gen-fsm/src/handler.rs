//! The FSM handler trait. Implementors define the state graph and
//! event-handling logic.

use std::fmt::Debug;

use crate::event::{EventType, TimeoutKind};
use crate::transition::Transition;

/// User-implemented logic for a finite state machine.
///
/// The trait is intentionally small: pick a state type, an event type,
/// a reply type, a stop reason, and write three methods (only
/// [`Self::initial`] and [`Self::handle`] are required; [`Self::on_enter`]
/// and [`Self::on_timeout`] have no-op defaults).
pub trait FsmHandler: Sized + Send + 'static {
    /// The set of states this FSM can be in. Typically a `Copy` enum
    /// with no payload; the per-state context lives in `self`.
    type State: Copy + Debug + PartialEq + Eq + Send + 'static;

    /// Events the FSM accepts. Includes both external events and any
    /// internal follow-ups posted via [`crate::Action::post_internal`].
    type Event: Send + 'static;

    /// Reply type for `Call` events.
    type Reply: Send + 'static;

    /// Reason this FSM stopped. Returned to the caller through
    /// [`crate::FsmDriver::join`].
    type Stop: Debug + Send + 'static;

    /// The state the FSM starts in. Called once during driver
    /// startup, before any events.
    fn initial(&self) -> Self::State;

    /// Handle one event in the given state. Return a [`Transition`]
    /// describing where to go next and which side effects to apply.
    ///
    /// Called only for `Call`, `Cast`, `Info`, and `Internal`
    /// events; `Enter` and `Timeout` events route to
    /// [`Self::on_enter`] and [`Self::on_timeout`] respectively.
    fn handle(
        &mut self,
        state: Self::State,
        event_type: EventType,
        event: Self::Event,
    ) -> Transition<Self>;

    /// Called once per transition into a state, before any mailbox
    /// events for the new state. Default impl returns
    /// `Transition::Keep(vec![])` (no-op).
    ///
    /// Override to do per-state setup (open file, start timer, log
    /// entry, request resources).
    fn on_enter(&mut self, state: Self::State) -> Transition<Self> {
        let _ = state;
        Transition::Keep(vec![])
    }

    /// Called when a timer fires. Default impl returns
    /// `Transition::Keep(vec![])` (no-op).
    ///
    /// Override to react to state, event, or generic timeouts.
    fn on_timeout(&mut self, state: Self::State, kind: TimeoutKind) -> Transition<Self> {
        let _ = (state, kind);
        Transition::Keep(vec![])
    }
}
