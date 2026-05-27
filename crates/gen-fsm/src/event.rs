//! Event taxonomy. The five `EventType` variants mirror gen_statem's:
//! `call`, `cast`, `info`, `timeout`, and `internal`. State entry is
//! routed through [`crate::FsmHandler::on_enter`] separately.

/// The kind of event being delivered to a state function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    /// A synchronous request that the caller expects a reply to. The
    /// state function must, eventually, run [`crate::Action::reply`]
    /// against the matching [`crate::ReplyHandle`].
    Call,
    /// An asynchronous notification. No reply is expected.
    Cast,
    /// A typed background message (analogous to gen_server's `info`).
    Info,
    /// A timeout fired. The [`TimeoutKind`] argument tells you which.
    /// Routed to [`crate::FsmHandler::on_timeout`] instead of `handle`.
    Timeout(TimeoutKind),
    /// A follow-up event the FSM posted to itself via
    /// [`crate::Action::post_internal`].
    Internal,
}

/// Which timer fired.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeoutKind {
    /// Per-state timer. Cancelled on state change. Useful for
    /// "give up if I am in this state for more than X".
    State,
    /// Per-event timer. Cancelled when *any* event arrives. Useful
    /// for "I expected a reply within X".
    Event,
    /// Generic named timer. Identified by a string. Cancelled
    /// individually via [`crate::Action::cancel_generic_timeout`].
    Generic(&'static str),
}
