//! [`Subscriber`] - the receive side of the cluster events
//! channel.
//!
//! A [`Subscriber`] wraps a [`tokio::sync::broadcast::Receiver`]
//! and translates the broadcast error shapes into typed
//! `Result<ClusterEvent, _>` returns.

use tokio::sync::broadcast::{self, error};

use super::ClusterEvent;

/// Error produced by [`Subscriber::recv`].
#[derive(Debug, thiserror::Error)]
pub enum SubscriberError {
    /// The [`super::EventManager`] was dropped; no further
    /// events will arrive on this subscriber.
    #[error("event manager closed")]
    Closed,
    /// The receiver fell behind the channel tail and missed
    /// `n` events. The next `recv` resumes from the freshest
    /// event in the buffer.
    #[error("subscriber lagged by {0} events")]
    Lagged(u64),
}

/// Error produced by [`Subscriber::try_recv`].
#[derive(Debug, thiserror::Error)]
pub enum TryRecvError {
    /// No event is currently buffered.
    #[error("no event available")]
    Empty,
    /// The [`super::EventManager`] was dropped; no further
    /// events will arrive on this subscriber.
    #[error("event manager closed")]
    Closed,
    /// The receiver fell behind the channel tail and missed
    /// `n` events. The next call resumes from the freshest
    /// event in the buffer.
    #[error("subscriber lagged by {0} events")]
    Lagged(u64),
}

/// Receive side of the cluster events channel.
///
/// Construct via [`super::EventManager::subscribe`]. Multiple
/// subscribers are independent: each receives its own copy of
/// every event published after the subscribe call.
#[derive(Debug)]
pub struct Subscriber {
    rx: broadcast::Receiver<ClusterEvent>,
}

impl Subscriber {
    pub(super) fn new(rx: broadcast::Receiver<ClusterEvent>) -> Self {
        Self { rx }
    }

    /// Await the next event.
    ///
    /// Returns [`SubscriberError::Closed`] when the upstream
    /// [`super::EventManager`] has been dropped, and
    /// [`SubscriberError::Lagged`] if the receiver fell behind
    /// the channel tail. After [`SubscriberError::Lagged`], the
    /// subscriber is still usable; the next call resumes from
    /// the freshest event in the buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::SystemTime;
    /// use dynomite::events::{ClusterEvent, EventManager};
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let mgr = EventManager::new(4);
    /// let mut sub = mgr.subscribe();
    /// mgr.publish(ClusterEvent::RingChanged {
    ///     tag: "x".into(),
    ///     ts: SystemTime::now(),
    /// });
    /// let evt = sub.recv().await.unwrap();
    /// assert!(matches!(evt, ClusterEvent::RingChanged { .. }));
    /// # });
    /// ```
    pub async fn recv(&mut self) -> Result<ClusterEvent, SubscriberError> {
        match self.rx.recv().await {
            Ok(evt) => Ok(evt),
            Err(error::RecvError::Closed) => Err(SubscriberError::Closed),
            Err(error::RecvError::Lagged(n)) => Err(SubscriberError::Lagged(n)),
        }
    }

    /// Non-blocking poll for the next event.
    ///
    /// Returns [`TryRecvError::Empty`] when no event is yet
    /// available, [`TryRecvError::Closed`] when the upstream
    /// manager has been dropped and the buffer drained, and
    /// [`TryRecvError::Lagged`] if the receiver fell behind the
    /// channel tail.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::events::{EventManager, TryRecvError};
    /// let mgr = EventManager::new(4);
    /// let mut sub = mgr.subscribe();
    /// assert!(matches!(sub.try_recv(), Err(TryRecvError::Empty)));
    /// ```
    pub fn try_recv(&mut self) -> Result<ClusterEvent, TryRecvError> {
        match self.rx.try_recv() {
            Ok(evt) => Ok(evt),
            Err(error::TryRecvError::Empty) => Err(TryRecvError::Empty),
            Err(error::TryRecvError::Closed) => Err(TryRecvError::Closed),
            Err(error::TryRecvError::Lagged(n)) => Err(TryRecvError::Lagged(n)),
        }
    }
}
