//! Transition: the return value from a state function.

use crate::action::Action;
use crate::handler::FsmHandler;

/// What the state function decided.
pub enum Transition<H: FsmHandler> {
    /// Stay in the current state. Run the listed actions.
    Keep(Vec<Action<H>>),
    /// Transition to a new state. Run the listed actions on the way;
    /// then synthesize an [`crate::EventType::Enter`] event for the
    /// new state's handler.
    Next(H::State, Vec<Action<H>>),
    /// Stop the FSM. The driver's [`crate::FsmDriver`] resolves with
    /// the given reason.
    Stop(H::Stop),
}

impl<H: FsmHandler> std::fmt::Debug for Transition<H>
where
    H::State: std::fmt::Debug,
    H::Stop: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Keep(actions) => f.debug_struct("Keep").field("actions", actions).finish(),
            Self::Next(state, actions) => f
                .debug_struct("Next")
                .field("state", state)
                .field("actions", actions)
                .finish(),
            Self::Stop(reason) => f.debug_struct("Stop").field("reason", reason).finish(),
        }
    }
}
