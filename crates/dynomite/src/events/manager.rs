//! [`EventManager`] - the publish side of the cluster events
//! channel.
//!
//! Backed by [`tokio::sync::broadcast`]. The manager owns the
//! sender; subscribers each hold a fresh receiver. Publishing
//! is lock-free and never blocks: a slow subscriber will lag
//! and surface [`super::SubscriberError::Lagged`] on its next
//! [`Subscriber::recv`](super::Subscriber::recv) call.

use tokio::sync::broadcast;

use super::{ClusterEvent, Subscriber};

/// Publisher side of the cluster events channel.
///
/// `EventManager` is cheap to clone (the inner
/// [`broadcast::Sender`] is reference-counted). Hand a clone or
/// an [`std::sync::Arc`] handle to every component that needs
/// to publish.
///
/// # Examples
///
/// ```
/// use dynomite::events::EventManager;
/// let mgr = EventManager::new(8);
/// let _sub = mgr.subscribe();
/// assert_eq!(mgr.subscriber_count(), 1);
/// ```
#[derive(Clone, Debug)]
pub struct EventManager {
    tx: broadcast::Sender<ClusterEvent>,
}

impl EventManager {
    /// Build a fresh manager backed by a broadcast channel of
    /// the supplied capacity.
    ///
    /// A capacity of zero is rejected by tokio; this constructor
    /// silently clamps it to one so embedding code does not have
    /// to special-case the empty value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::events::EventManager;
    /// let mgr = EventManager::new(64);
    /// assert_eq!(mgr.subscriber_count(), 0);
    /// ```
    #[must_use]
    pub fn new(buffer: usize) -> Self {
        let cap = buffer.max(1);
        let (tx, _) = broadcast::channel(cap);
        Self { tx }
    }

    /// Publish an event to every live subscriber.
    ///
    /// Returns silently when there are no subscribers attached;
    /// `tokio::sync::broadcast::Sender::send` reports that case
    /// as `Err(SendError(_))`, which we explicitly drop because
    /// "no observers" is normal during quiet periods and is
    /// never the publisher's problem.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::SystemTime;
    /// use dynomite::events::{ClusterEvent, EventManager};
    /// let mgr = EventManager::new(4);
    /// // No subscribers attached; publish is still infallible.
    /// mgr.publish(ClusterEvent::RingChanged {
    ///     tag: "init".into(),
    ///     ts: SystemTime::now(),
    /// });
    /// ```
    pub fn publish(&self, event: ClusterEvent) {
        // The Err arm carries the event back; we deliberately
        // discard it because "no subscribers" is not an error.
        let _ = self.tx.send(event);
    }

    /// Subscribe a fresh receiver.
    ///
    /// Each call returns an independent [`Subscriber`] whose
    /// queue starts empty; events published before the call are
    /// not delivered to the new subscriber.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::events::EventManager;
    /// let mgr = EventManager::new(4);
    /// let _a = mgr.subscribe();
    /// let _b = mgr.subscribe();
    /// assert_eq!(mgr.subscriber_count(), 2);
    /// ```
    #[must_use]
    pub fn subscribe(&self) -> Subscriber {
        Subscriber::new(self.tx.subscribe())
    }

    /// Number of attached subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for EventManager {
    /// Build a manager with a 64-event buffer.
    ///
    /// 64 matches the default capacity of [`crate::embed::events::EventBus`]
    /// so embedders that swap one for the other do not see a
    /// surprise change in lag behaviour.
    fn default() -> Self {
        Self::new(64)
    }
}
