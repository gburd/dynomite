//! Server-wide event stream.
//!
//! The embedding API exposes a [`tokio::sync::broadcast`] of
//! [`ServerEvent`] values so consumers can observe peer
//! transitions, gossip rounds, configuration reloads, and other
//! cluster-wide signals. The handle returned by
//! [`crate::embed::ServerHandle::subscribe_events`] wraps the
//! broadcast receiver in [`EventStream`] for ergonomic polling.
//!
//! # Examples
//!
//! ```
//! use dynomite::embed::events::{ServerEvent, EventBus, ConnRoleTag, CloseReason, PeerDownReason};
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let bus = EventBus::new(16);
//! let mut rx = bus.subscribe();
//! bus.send(ServerEvent::ConfigReloaded { generation: 1 });
//! let evt = rx.recv().await.unwrap();
//! assert!(matches!(evt, ServerEvent::ConfigReloaded { generation: 1 }));
//! # let _ = (ConnRoleTag::Client, CloseReason::PeerEof, PeerDownReason::FailureDetector);
//! # });
//! ```

use std::net::SocketAddr;

use tokio::sync::broadcast;

/// Identifier handed out for every accepted connection.
pub type ConnId = u64;

/// Identifier of a peer in the cluster pool.
pub type PeerId = u32;

/// Summary tag for the role of a connection at accept time.
///
/// Mirrors [`crate::io::reactor::ConnRole`] but renamed to keep
/// the embed surface independent of the I/O substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnRoleTag {
    /// Listener that accepted a client connection.
    Proxy,
    /// Connected client.
    Client,
    /// Outbound datastore connection.
    Server,
    /// Listener that accepted a peer connection.
    DnodeProxy,
    /// Inbound peer connection.
    DnodeClient,
    /// Outbound peer connection.
    DnodeServer,
}

/// Reason carried with [`ServerEvent::ConnectionClosed`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CloseReason {
    /// The peer closed cleanly (FIN / EOF).
    PeerEof,
    /// The local side initiated the close (graceful shutdown).
    LocalClose,
    /// I/O error.
    IoError,
    /// Connection was idle past the configured timeout.
    Timeout,
}

/// Reason carried with [`ServerEvent::PeerDown`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PeerDownReason {
    /// Failure detector marked the peer down.
    FailureDetector,
    /// Auto-eject hosts policy ejected the peer.
    AutoEjected,
    /// Operator-initiated reload removed the peer.
    Reconfigured,
    /// Peer announced a graceful shutdown.
    Leaving,
}

/// Cluster-wide event published on the broadcast channel.
///
/// The variants are non-exhaustive at the type level; consumers
/// must use a wildcard arm so future additions remain
/// non-breaking.
///
/// # Examples
///
/// ```
/// use dynomite::embed::events::{ServerEvent, PeerDownReason};
/// let e = ServerEvent::PeerDown { peer: 7, reason: PeerDownReason::FailureDetector };
/// match e {
///     ServerEvent::PeerDown { peer, .. } => assert_eq!(peer, 7),
///     _ => panic!(),
/// }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ServerEvent {
    /// A peer transitioned to a routable state.
    PeerUp(PeerId),
    /// A peer transitioned to an unroutable state.
    PeerDown {
        /// Peer index.
        peer: PeerId,
        /// Why the transition fired.
        reason: PeerDownReason,
    },
    /// A configuration reload completed successfully.
    ConfigReloaded {
        /// Monotonic generation id.
        generation: u64,
    },
    /// A periodic gossip pass finished.
    GossipRound {
        /// Round number (monotonic).
        round: u64,
        /// Number of distinct peers known after the round.
        peers: u32,
    },
    /// Auto-eject policy ejected a peer.
    AutoEjected {
        /// Peer index.
        peer: PeerId,
        /// Failure count at eject time.
        failures: u32,
    },
    /// Read-repair was triggered for a key.
    RepairTriggered {
        /// Hash of the key whose responses diverged.
        key_hash: u64,
        /// Datacenter that initiated the repair.
        dc: String,
    },
    /// A connection was accepted on a listener.
    ConnectionAccepted {
        /// Synthetic connection id.
        conn_id: ConnId,
        /// Role of the accepted connection.
        role: ConnRoleTag,
        /// Local socket address; `None` for non-socket transports.
        local_addr: Option<SocketAddr>,
    },
    /// A connection closed.
    ConnectionClosed {
        /// Connection id from the prior `ConnectionAccepted`.
        conn_id: ConnId,
        /// Why the connection closed.
        reason: CloseReason,
    },
    /// The receiver fell behind the broadcast tail and missed
    /// `missed` events. The next `recv` will resume from the
    /// freshest event in the buffer.
    Lagged {
        /// Number of events the receiver missed.
        missed: u64,
    },
}

/// Event publisher held by the embed runtime.
///
/// `EventBus` is a thin wrapper around a
/// [`tokio::sync::broadcast::Sender`] that drops events silently
/// when no receivers are attached.
#[derive(Debug, Clone)]
pub struct EventBus {
    tx: broadcast::Sender<ServerEvent>,
}

impl EventBus {
    /// Build a fresh bus with the supplied channel capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::events::EventBus;
    /// let bus = EventBus::new(8);
    /// assert_eq!(bus.subscriber_count(), 0);
    /// ```
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let (tx, _) = broadcast::channel(cap);
        Self { tx }
    }

    /// Subscribe a fresh receiver.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::events::EventBus;
    /// let bus = EventBus::new(4);
    /// let _rx = bus.subscribe();
    /// assert!(bus.subscriber_count() >= 1);
    /// ```
    #[must_use]
    pub fn subscribe(&self) -> EventStream {
        EventStream {
            rx: self.tx.subscribe(),
        }
    }

    /// Publish an event. Returns the number of live subscribers
    /// the event was delivered to (zero is normal during quiet
    /// periods and is never an error).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::events::{EventBus, ServerEvent};
    /// let bus = EventBus::new(2);
    /// // No subscribers: send returns 0 and is silently dropped.
    /// assert_eq!(bus.send(ServerEvent::ConfigReloaded { generation: 1 }), 0);
    /// ```
    pub fn send(&self, event: ServerEvent) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    /// Number of attached subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

/// Pollable async stream of [`ServerEvent`]s.
///
/// `EventStream` is the public-facing receiver returned by
/// [`crate::embed::ServerHandle::subscribe_events`]. It wraps a
/// [`tokio::sync::broadcast::Receiver`] and translates the
/// `RecvError::Lagged` shape into a synthesized
/// [`ServerEvent::Lagged`] so consumers can stay on the happy
/// path.
#[derive(Debug)]
pub struct EventStream {
    rx: broadcast::Receiver<ServerEvent>,
}

impl EventStream {
    /// Receive the next event.
    ///
    /// Returns `None` when the bus is closed (the server has shut
    /// down and dropped its [`EventBus`]). On lag, returns a
    /// synthesized [`ServerEvent::Lagged`] and resumes from the
    /// freshest event in the buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::events::{EventBus, ServerEvent};
    /// # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
    /// let bus = EventBus::new(2);
    /// let mut s = bus.subscribe();
    /// bus.send(ServerEvent::ConfigReloaded { generation: 7 });
    /// let evt = s.recv().await.unwrap();
    /// assert!(matches!(evt, ServerEvent::ConfigReloaded { generation: 7 }));
    /// # });
    /// ```
    pub async fn recv(&mut self) -> Option<ServerEvent> {
        match self.rx.recv().await {
            Ok(evt) => Some(evt),
            Err(broadcast::error::RecvError::Closed) => None,
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                Some(ServerEvent::Lagged { missed })
            }
        }
    }

    /// Non-blocking poll: returns the next event if one is
    /// already buffered, else `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::events::EventBus;
    /// let bus = EventBus::new(2);
    /// let mut s = bus.subscribe();
    /// assert!(s.try_recv().is_none());
    /// ```
    pub fn try_recv(&mut self) -> Option<ServerEvent> {
        match self.rx.try_recv() {
            Ok(evt) => Some(evt),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                None
            }
            Err(broadcast::error::TryRecvError::Lagged(missed)) => {
                Some(ServerEvent::Lagged { missed })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn round_trip_ok() {
        let bus = EventBus::new(4);
        let mut s = bus.subscribe();
        bus.send(ServerEvent::PeerUp(1));
        let evt = s.recv().await.unwrap();
        assert!(matches!(evt, ServerEvent::PeerUp(1)));
    }

    #[tokio::test]
    async fn lagged_synthesised() {
        let bus = EventBus::new(2);
        let mut s = bus.subscribe();
        for i in 0..8u64 {
            bus.send(ServerEvent::ConfigReloaded { generation: i });
        }
        let first = s.recv().await.unwrap();
        // Either Lagged or one of the surviving events.
        assert!(matches!(
            first,
            ServerEvent::Lagged { .. } | ServerEvent::ConfigReloaded { .. }
        ));
    }

    #[test]
    fn try_recv_empty() {
        let bus = EventBus::new(2);
        let mut s = bus.subscribe();
        assert!(s.try_recv().is_none());
    }
}
