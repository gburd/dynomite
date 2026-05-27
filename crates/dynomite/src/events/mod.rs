//! Cluster events: a structured fan-out channel for state
//! transitions that test harnesses, admin tools, and OTLP
//! appenders can consume programmatically.
//!
//! The engine already publishes cluster signals to two
//! observability surfaces - tracing spans and Prometheus
//! counters - which are excellent for humans and dashboards but
//! awkward to consume from code. This module adds a third
//! surface: a strongly-typed, OTP `gen_event`-style broadcast of
//! [`ClusterEvent`] values backed by a
//! [`tokio::sync::broadcast`] channel. Consumers subscribe via
//! [`EventManager::subscribe`] and read events through
//! [`Subscriber::recv`] / [`Subscriber::try_recv`].
//!
//! Slow subscribers do not block publishers. When a subscriber
//! falls behind the channel tail, [`Subscriber::recv`] returns
//! [`SubscriberError::Lagged`] reporting the count of dropped
//! events, then resumes from the freshest event in the buffer.
//!
//! # Examples
//!
//! ```
//! use std::time::SystemTime;
//! use dynomite::events::{ClusterEvent, EventManager};
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! let mgr = EventManager::new(16);
//! let mut sub = mgr.subscribe();
//! mgr.publish(ClusterEvent::PeerUp {
//!     peer_id: 7,
//!     dc: "dc1".to_string(),
//!     ts: SystemTime::now(),
//! });
//! match sub.recv().await.unwrap() {
//!     ClusterEvent::PeerUp { peer_id, .. } => assert_eq!(peer_id, 7),
//!     _ => panic!("unexpected event"),
//! }
//! # });
//! ```

mod manager;
mod subscriber;

use std::time::{Duration, SystemTime};

pub use self::manager::EventManager;
pub use self::subscriber::{Subscriber, SubscriberError, TryRecvError};

use crate::hashkit::DynToken;

/// Identifier of a peer in the cluster pool.
///
/// Mirrors [`crate::cluster::peer::Peer::idx`] and the existing
/// [`crate::embed::events::PeerId`] alias so the two event
/// surfaces remain index-compatible.
pub type PeerId = u32;

/// Half-open token range `[start, end)` covering one slice of
/// the consistent-hashing ring.
///
/// Used by AAE exchanges to identify which partition is being
/// reconciled. The range is interpreted as wrap-around when
/// `start >= end`, matching the ring traversal semantics in
/// [`crate::cluster::pool`].
///
/// # Examples
///
/// ```
/// use dynomite::events::TokenRange;
/// use dynomite::hashkit::DynToken;
/// let r = TokenRange::new(DynToken::from_u32(0), DynToken::from_u32(1024));
/// assert_eq!(r.start(), &DynToken::from_u32(0));
/// assert_eq!(r.end(), &DynToken::from_u32(1024));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TokenRange {
    start: DynToken,
    end: DynToken,
}

impl TokenRange {
    /// Build a fresh token range covering `[start, end)`.
    #[must_use]
    pub fn new(start: DynToken, end: DynToken) -> Self {
        Self { start, end }
    }

    /// Inclusive lower bound.
    #[must_use]
    pub fn start(&self) -> &DynToken {
        &self.start
    }

    /// Exclusive upper bound.
    #[must_use]
    pub fn end(&self) -> &DynToken {
        &self.end
    }
}

/// Structured cluster-event payload published on the
/// [`EventManager`] broadcast.
///
/// Variants are non-exhaustive at the type level; consumers must
/// always include a wildcard arm so future additions remain
/// non-breaking.
///
/// # Examples
///
/// ```
/// use std::time::SystemTime;
/// use dynomite::events::ClusterEvent;
/// let e = ClusterEvent::PeerUp { peer_id: 1, dc: "dc1".into(), ts: SystemTime::now() };
/// match e {
///     ClusterEvent::PeerUp { peer_id, .. } => assert_eq!(peer_id, 1),
///     _ => panic!("unexpected"),
/// }
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ClusterEvent {
    /// A peer transitioned to a routable state.
    PeerUp {
        /// Peer index.
        peer_id: PeerId,
        /// Datacenter the peer belongs to.
        dc: String,
        /// Wall-clock timestamp at which the engine observed
        /// the transition.
        ts: SystemTime,
    },
    /// A peer transitioned to an unroutable state because the
    /// failure detector crossed its phi threshold.
    PeerDown {
        /// Peer index.
        peer_id: PeerId,
        /// Datacenter the peer belongs to.
        dc: String,
        /// Phi value observed at the moment of the transition.
        phi: f64,
        /// Wall-clock timestamp at which the engine observed
        /// the transition.
        ts: SystemTime,
    },
    /// One periodic gossip pass finished.
    GossipRoundComplete {
        /// Wall-clock duration spent in the round.
        duration: Duration,
        /// Number of distinct peers visited during the round.
        peers_seen: usize,
        /// Wall-clock timestamp at which the round finished.
        ts: SystemTime,
    },
    /// An anti-entropy exchange started against a peer.
    AaeExchangeStarted {
        /// Peer the exchange is running against.
        with_peer: PeerId,
        /// Token range the exchange covers.
        partition: TokenRange,
        /// Wall-clock timestamp at which the exchange started.
        ts: SystemTime,
    },
    /// An anti-entropy exchange finished against a peer.
    AaeExchangeCompleted {
        /// Peer the exchange ran against.
        with_peer: PeerId,
        /// Token range the exchange covered.
        partition: TokenRange,
        /// Number of keys repaired during the exchange.
        repaired: u64,
        /// Wall-clock timestamp at which the exchange finished.
        ts: SystemTime,
    },
    /// A peer was observed restarting (its incarnation changed
    /// or its peering session was re-established after a clean
    /// shutdown).
    RestartObserved {
        /// Peer index.
        peer_id: PeerId,
        /// Wall-clock timestamp at which the restart was
        /// observed.
        ts: SystemTime,
    },
    /// The token ring topology changed: a peer was added,
    /// removed, or its tokens were reassigned.
    RingChanged {
        /// Free-form tag describing the trigger (e.g.
        /// `"seed-discovery"`, `"reconfigure"`, `"shutdown"`).
        tag: String,
        /// Wall-clock timestamp at which the change was
        /// applied.
        ts: SystemTime,
    },
}
