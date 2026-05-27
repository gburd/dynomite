//! Actions a state function can request after handling an event.

use std::time::Duration;

use tokio::sync::oneshot;

use crate::handler::FsmHandler;

/// Side effects requested by a state function. Returned in a `Vec`
/// from a [`crate::Transition`].
pub enum Action<H: FsmHandler> {
    /// Send a reply to a `Call` event.
    Reply(ReplyHandle<H::Reply>, H::Reply),
    /// Defer the current event until the next state change. The event
    /// is reinserted at the head of the mailbox on entry to the new
    /// state, before any pending events.
    Postpone,
    /// Set a state timeout. Cancelled automatically on state change.
    SetStateTimeout(Duration),
    /// Cancel the active state timeout, if any.
    CancelStateTimeout,
    /// Set a per-event timeout. Cancelled automatically when *any*
    /// event arrives.
    SetEventTimeout(Duration),
    /// Set a named generic timeout. Independent of state and event
    /// timeouts; cancelled only by name.
    SetGenericTimeout(&'static str, Duration),
    /// Cancel a generic timeout by name. No-op if not active.
    CancelGenericTimeout(&'static str),
    /// Post a synthetic event to ourselves with type `Internal`. The
    /// event is delivered ahead of mailbox events but after the
    /// current handler returns.
    PostInternal(H::Event),
}

/// Convenience constructors. Equivalent to building the variants by
/// hand; preferred for readability.
impl<H: FsmHandler> Action<H> {
    /// Reply to a `Call` event.
    #[must_use]
    pub fn reply(handle: ReplyHandle<H::Reply>, reply: H::Reply) -> Self {
        Self::Reply(handle, reply)
    }

    /// Postpone the current event to the next state change.
    #[must_use]
    pub const fn postpone() -> Self {
        Self::Postpone
    }

    /// Set the state timer.
    #[must_use]
    pub const fn set_state_timeout(after: Duration) -> Self {
        Self::SetStateTimeout(after)
    }

    /// Cancel the state timer.
    #[must_use]
    pub const fn cancel_state_timeout() -> Self {
        Self::CancelStateTimeout
    }

    /// Set a per-event timer.
    #[must_use]
    pub const fn set_event_timeout(after: Duration) -> Self {
        Self::SetEventTimeout(after)
    }

    /// Set a named generic timer.
    #[must_use]
    pub const fn set_generic_timeout(name: &'static str, after: Duration) -> Self {
        Self::SetGenericTimeout(name, after)
    }

    /// Cancel a generic timer by name.
    #[must_use]
    pub const fn cancel_generic_timeout(name: &'static str) -> Self {
        Self::CancelGenericTimeout(name)
    }

    /// Post an internal event to ourselves.
    #[must_use]
    pub fn post_internal(event: H::Event) -> Self {
        Self::PostInternal(event)
    }
}

impl<H: FsmHandler> std::fmt::Debug for Action<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Reply(_, _) => f.write_str("Reply(..)"),
            Self::Postpone => f.write_str("Postpone"),
            Self::SetStateTimeout(d) => write!(f, "SetStateTimeout({d:?})"),
            Self::CancelStateTimeout => f.write_str("CancelStateTimeout"),
            Self::SetEventTimeout(d) => write!(f, "SetEventTimeout({d:?})"),
            Self::SetGenericTimeout(n, d) => write!(f, "SetGenericTimeout({n:?}, {d:?})"),
            Self::CancelGenericTimeout(n) => write!(f, "CancelGenericTimeout({n:?})"),
            Self::PostInternal(_) => f.write_str("PostInternal(..)"),
        }
    }
}

/// Handle for replying to a `Call` event. The driver hands this to
/// the state function alongside the event payload; the state function
/// must use it (in this state or a later state, after a `Postpone`)
/// to deliver a reply, or the caller blocks until the FSM stops.
///
/// Internally a [`oneshot::Sender`].
pub struct ReplyHandle<R>(pub(crate) oneshot::Sender<R>);

impl<R> ReplyHandle<R> {
    /// Send the reply. The caller's [`oneshot::Receiver`] resolves.
    /// If the receiver was dropped (caller cancelled), the reply is
    /// discarded silently.
    pub fn send(self, reply: R) {
        let _ = self.0.send(reply);
    }

    /// Returns true if the caller is no longer waiting (receiver
    /// dropped). Useful to skip expensive reply computation.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

impl<R> std::fmt::Debug for ReplyHandle<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplyHandle")
    }
}

/// Internal: which kind of mailbox event just arrived. The driver
/// uses this to dispatch the matching [`crate::EventType`] when
/// handing the event to the state function.
#[derive(Debug)]
pub(crate) enum MailboxEvent<H: FsmHandler> {
    Call(H::Event, ReplyHandle<H::Reply>),
    Cast(H::Event),
    Info(H::Event),
}
