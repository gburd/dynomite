//! Async driver. Owns the handler, the mailbox, and the timers.

use std::collections::VecDeque;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Instant};

use crate::action::{Action, MailboxEvent, ReplyHandle};
use crate::event::{EventType, TimeoutKind};
use crate::handler::FsmHandler;
use crate::transition::Transition;

/// Driver errors.
#[derive(Debug, Error)]
pub enum DriverError {
    /// The FSM was already dropped.
    #[error("FSM is no longer running")]
    Stopped,
    /// The reply channel was dropped before a reply arrived.
    #[error("reply channel dropped before reply")]
    ReplyDropped,
}

/// Reason the driver stopped.
#[derive(Debug)]
pub enum StopReason<H: FsmHandler> {
    /// Handler returned [`Transition::Stop`].
    Handler(H::Stop),
    /// Mailbox closed (all `FsmDriver` handles dropped).
    Closed,
}

/// External handle for a running FSM. Drop the handle to stop the
/// FSM (the driver task observes mailbox closure and exits).
///
/// Cloneable; multiple handles can drive the same FSM.
pub struct FsmDriver<H: FsmHandler> {
    tx: mpsc::Sender<MailboxEvent<H>>,
    join: Option<JoinHandle<StopReason<H>>>,
}

impl<H: FsmHandler> FsmDriver<H> {
    /// Spawn the FSM on the current Tokio runtime.
    pub fn start(handler: H) -> Self {
        let (tx, rx) = mpsc::channel(64);
        let join = tokio::spawn(run(handler, rx));
        Self {
            tx,
            join: Some(join),
        }
    }

    /// Send a synchronous request and wait for the reply.
    ///
    /// # Errors
    /// Returns [`DriverError::Stopped`] if the FSM is gone, or
    /// [`DriverError::ReplyDropped`] if the FSM stopped before
    /// dispatching a reply.
    pub async fn call(&self, event: H::Event) -> Result<H::Reply, DriverError> {
        let (rtx, rrx) = oneshot::channel();
        self.tx
            .send(MailboxEvent::Call(event, ReplyHandle(rtx)))
            .await
            .map_err(|_| DriverError::Stopped)?;
        rrx.await.map_err(|_| DriverError::ReplyDropped)
    }

    /// Send an asynchronous notification.
    ///
    /// Silently no-ops if the FSM is gone. Use [`Self::cast_checked`]
    /// to detect that.
    pub async fn cast(&self, event: H::Event) {
        let _ = self.tx.send(MailboxEvent::Cast(event)).await;
    }

    /// Send an asynchronous notification, returning an error if the
    /// FSM is gone.
    ///
    /// # Errors
    /// Returns [`DriverError::Stopped`] if the mailbox is closed.
    pub async fn cast_checked(&self, event: H::Event) -> Result<(), DriverError> {
        self.tx
            .send(MailboxEvent::Cast(event))
            .await
            .map_err(|_| DriverError::Stopped)
    }

    /// Send a typed background message (`info`).
    ///
    /// # Errors
    /// Returns [`DriverError::Stopped`] if the mailbox is closed.
    pub async fn info(&self, event: H::Event) -> Result<(), DriverError> {
        self.tx
            .send(MailboxEvent::Info(event))
            .await
            .map_err(|_| DriverError::Stopped)
    }

    /// Wait for the driver task to exit.
    ///
    /// # Errors
    /// Returns [`DriverError::Stopped`] if the join handle was
    /// already taken or the task panicked.
    pub async fn join(mut self) -> Result<StopReason<H>, DriverError> {
        let join = self.join.take().ok_or(DriverError::Stopped)?;
        join.await.map_err(|_| DriverError::Stopped)
    }
}

impl<H: FsmHandler> Clone for FsmDriver<H> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            join: None,
        }
    }
}

struct DriverState<H: FsmHandler> {
    state: H::State,
    state_deadline: Option<Instant>,
    event_deadline: Option<Instant>,
    generic_timers: Vec<(&'static str, Instant)>,
    internal_queue: VecDeque<H::Event>,
    postponed: VecDeque<MailboxEvent<H>>,
}

async fn run<H: FsmHandler>(
    mut handler: H,
    mut rx: mpsc::Receiver<MailboxEvent<H>>,
) -> StopReason<H> {
    let initial = handler.initial();
    let mut ds: DriverState<H> = DriverState {
        state: initial,
        state_deadline: None,
        event_deadline: None,
        generic_timers: Vec::new(),
        internal_queue: VecDeque::new(),
        postponed: VecDeque::new(),
    };

    // Synthesize the initial Enter.
    if let Err(reason) = run_enter(&mut handler, &mut ds) {
        return StopReason::Handler(reason);
    }

    loop {
        // Drain internal events first.
        if let Some(payload) = ds.internal_queue.pop_front() {
            let transition = handler.handle(ds.state, EventType::Internal, payload);
            if let Err(reason) = apply_transition(&mut handler, &mut ds, transition) {
                return StopReason::Handler(reason);
            }
            continue;
        }

        let next_timer = soonest_timer(
            ds.state_deadline.as_ref(),
            ds.event_deadline.as_ref(),
            &ds.generic_timers,
        );

        let event = match next_timer {
            Some((kind, deadline)) => tokio::select! {
                biased;
                () = sleep_until(deadline) => TimerOrMailbox::Timer(kind),
                msg = rx.recv() => match msg {
                    Some(m) => TimerOrMailbox::Mailbox(m),
                    None => return StopReason::Closed,
                }
            },
            None => match rx.recv().await {
                Some(m) => TimerOrMailbox::Mailbox(m),
                None => return StopReason::Closed,
            },
        };

        if matches!(event, TimerOrMailbox::Mailbox(_)) {
            ds.event_deadline = None;
        }

        match event {
            TimerOrMailbox::Timer(kind) => {
                match kind {
                    TimeoutKind::State => ds.state_deadline = None,
                    TimeoutKind::Event => ds.event_deadline = None,
                    TimeoutKind::Generic(name) => {
                        ds.generic_timers.retain(|(n, _)| *n != name);
                    }
                }
                let transition = handler.on_timeout(ds.state, kind);
                if let Err(reason) = apply_transition(&mut handler, &mut ds, transition) {
                    return StopReason::Handler(reason);
                }
            }
            TimerOrMailbox::Mailbox(mb) => match mb {
                MailboxEvent::Call(ev, reply) => {
                    let transition = handler.handle(ds.state, EventType::Call, ev);
                    drop(reply);
                    if let Err(reason) = apply_transition(&mut handler, &mut ds, transition) {
                        return StopReason::Handler(reason);
                    }
                }
                MailboxEvent::Cast(ev) => {
                    let transition = handler.handle(ds.state, EventType::Cast, ev);
                    if let Err(reason) = apply_transition(&mut handler, &mut ds, transition) {
                        return StopReason::Handler(reason);
                    }
                }
                MailboxEvent::Info(ev) => {
                    let transition = handler.handle(ds.state, EventType::Info, ev);
                    if let Err(reason) = apply_transition(&mut handler, &mut ds, transition) {
                        return StopReason::Handler(reason);
                    }
                }
            },
        }
    }
}

fn run_enter<H: FsmHandler>(handler: &mut H, ds: &mut DriverState<H>) -> Result<(), H::Stop> {
    let transition = handler.on_enter(ds.state);
    apply_transition(handler, ds, transition)
}

enum TimerOrMailbox<H: FsmHandler> {
    Timer(TimeoutKind),
    Mailbox(MailboxEvent<H>),
}

fn apply_transition<H: FsmHandler>(
    handler: &mut H,
    ds: &mut DriverState<H>,
    transition: Transition<H>,
) -> Result<(), H::Stop> {
    let (next_state, actions, stop) = match transition {
        Transition::Keep(actions) => (None, actions, None),
        Transition::Next(s, actions) => (Some(s), actions, None),
        Transition::Stop(reason) => (None, vec![], Some(reason)),
    };
    if let Some(reason) = stop {
        return Err(reason);
    }
    for action in actions {
        match action {
            Action::Reply(handle, reply) => handle.send(reply),
            Action::Postpone => {
                // Postpone has no effect outside the mailbox path
                // for synthetic Enter/Timeout/Internal events; it is
                // simply ignored. A handler that postpones a Call
                // loses the reply handle: deferred-reply is not
                // supported for these synthetic events.
            }
            Action::SetStateTimeout(d) => ds.state_deadline = Some(Instant::now() + d),
            Action::CancelStateTimeout => ds.state_deadline = None,
            Action::SetEventTimeout(d) => ds.event_deadline = Some(Instant::now() + d),
            Action::SetGenericTimeout(name, d) => {
                ds.generic_timers.retain(|(n, _)| *n != name);
                ds.generic_timers.push((name, Instant::now() + d));
            }
            Action::CancelGenericTimeout(name) => {
                ds.generic_timers.retain(|(n, _)| *n != name);
            }
            Action::PostInternal(ev) => ds.internal_queue.push_back(ev),
        }
    }
    if let Some(s) = next_state {
        ds.state = s;
        ds.state_deadline = None;
        // Re-deliver postponed events ahead of mailbox events.
        while let Some(ev) = ds.postponed.pop_back() {
            match ev {
                MailboxEvent::Call(e, _) | MailboxEvent::Cast(e) | MailboxEvent::Info(e) => {
                    ds.internal_queue.push_front(e);
                }
            }
        }
        // Run the Enter for the new state.
        let transition = handler.on_enter(ds.state);
        return apply_transition(handler, ds, transition);
    }
    Ok(())
}

fn soonest_timer(
    state_deadline: Option<&Instant>,
    event_deadline: Option<&Instant>,
    generic_timers: &[(&'static str, Instant)],
) -> Option<(TimeoutKind, Instant)> {
    let mut soonest: Option<(TimeoutKind, Instant)> = None;
    if let Some(d) = state_deadline {
        soonest = Some((TimeoutKind::State, *d));
    }
    if let Some(d) = event_deadline {
        if soonest.is_none_or(|(_, s)| *d < s) {
            soonest = Some((TimeoutKind::Event, *d));
        }
    }
    for (name, d) in generic_timers {
        if soonest.is_none_or(|(_, s)| *d < s) {
            soonest = Some((TimeoutKind::Generic(name), *d));
        }
    }
    soonest
}
