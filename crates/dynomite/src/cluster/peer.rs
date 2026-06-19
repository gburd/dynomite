//! Cluster peer state.
//!
//! A [`Peer`] is the in-memory record for one dynomite node in the
//! ring. Each peer carries its endpoint, rack, datacenter, token list,
//! liveness state, and a handle to the outbound connection pool used
//! when the dispatcher routes a request to it.
//!
//! The data shape carries the per-peer node record
//! (rack, dc, secure flag, same-DC flag, token list, state). Peers are
//! held by [`crate::cluster::ServerPool`] in an `Arc<RwLock<_>>` so
//! gossip and dispatch can both observe the table without taking out
//! exclusive locks for read.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
//! use dynomite::hashkit::DynToken;
//!
//! let p = Peer::new(
//!     0,
//!     PeerEndpoint::tcp("127.0.0.1".into(), 8101),
//!     "rack1".into(),
//!     "dc1".into(),
//!     vec![DynToken::from_u32(101_134_286)],
//!     true,
//!     true,
//!     false,
//! );
//! assert_eq!(p.rack(), "rack1");
//! assert_eq!(p.state(), PeerState::Joining);
//! ```

use crate::hashkit::DynToken;

/// Lifecycle state of a peer in the gossip view.
///
/// Numeric values are the stable per-node state
/// constants (`UNKNOWN`, `JOINING`, `NORMAL`, `STANDBY`, `DOWN`,
/// `RESET`, `LEAVING`).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Default)]
#[repr(u8)]
pub enum PeerState {
    /// Initial state before the first observation.
    #[default]
    Unknown = 0,
    /// Peer is bootstrapping into the ring.
    Joining = 1,
    /// Peer is healthy and serving traffic.
    Normal = 2,
    /// Peer is on warm standby; requests are not routed there.
    Standby = 3,
    /// Failure detector marked the peer down.
    Down = 4,
    /// Peer is being re-established (connection pool reset).
    Reset = 5,
    /// Peer is preparing to leave the ring.
    Leaving = 6,
}

impl PeerState {
    /// Stable string label.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::peer::PeerState;
    /// assert_eq!(PeerState::Normal.name(), "NORMAL");
    /// ```
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            PeerState::Unknown => "UNKNOWN",
            PeerState::Joining => "JOINING",
            PeerState::Normal => "NORMAL",
            PeerState::Standby => "STANDBY",
            PeerState::Down => "DOWN",
            PeerState::Reset => "RESET",
            PeerState::Leaving => "LEAVING",
        }
    }

    /// True when the dispatcher should consider this peer for
    /// routing decisions. `Joining` peers are accepted because they
    /// remain in the continuum until they
    /// transition to `Down` or `Leaving`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::peer::PeerState;
    /// assert!(PeerState::Normal.is_routable());
    /// assert!(!PeerState::Down.is_routable());
    /// ```
    #[must_use]
    pub fn is_routable(self) -> bool {
        matches!(self, PeerState::Normal | PeerState::Joining)
    }
}

/// Network endpoint of a peer.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PeerEndpoint {
    host: String,
    port: u16,
}

impl PeerEndpoint {
    /// Construct a TCP endpoint.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::peer::PeerEndpoint;
    /// let ep = PeerEndpoint::tcp("10.0.0.1".into(), 8101);
    /// assert_eq!(ep.host(), "10.0.0.1");
    /// assert_eq!(ep.port(), 8101);
    /// assert_eq!(ep.pname(), "10.0.0.1:8101");
    /// ```
    #[must_use]
    pub fn tcp(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// Hostname or numeric IP.
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// TCP port.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Colon-joined `host:port` string.
    #[must_use]
    pub fn pname(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// One peer in the cluster ring.
#[derive(Clone, Debug)]
pub struct Peer {
    idx: u32,
    endpoint: PeerEndpoint,
    rack: String,
    dc: String,
    tokens: Vec<DynToken>,
    is_local: bool,
    is_same_dc: bool,
    is_secure: bool,
    state: PeerState,
    failure_count: u32,
    last_state_ts_secs: u64,
    /// Phi-accrual failure detector for this peer. Fed by
    /// gossip heartbeats; queried by the gossip task to decide
    /// when to transition `state` to [`PeerState::Down`].
    /// Initialised lazily on the first `record_heartbeat` call.
    fd: crate::cluster::failure_detector::PhiAccrual,
}

impl Peer {
    /// Build a new peer record.
    ///
    /// `idx` is the peer's index in the pool's peer array (0 is
    /// always the local node). The initial state is
    /// [`PeerState::Joining`] for the local node and
    /// [`PeerState::Down`] for a remote one (a remote peer is
    /// promoted only after the first gossip ack).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
    /// use dynomite::hashkit::DynToken;
    /// let p = Peer::new(
    ///     1,
    ///     PeerEndpoint::tcp("h".into(), 1),
    ///     "r".into(),
    ///     "d".into(),
    ///     vec![DynToken::from_u32(0)],
    ///     false,
    ///     true,
    ///     false,
    /// );
    /// assert_eq!(p.idx(), 1);
    /// assert_eq!(p.state(), PeerState::Down);
    /// ```
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        idx: u32,
        endpoint: PeerEndpoint,
        rack: String,
        dc: String,
        tokens: Vec<DynToken>,
        is_local: bool,
        is_same_dc: bool,
        is_secure: bool,
    ) -> Self {
        let state = if is_local {
            PeerState::Joining
        } else {
            PeerState::Down
        };
        Self {
            idx,
            endpoint,
            rack,
            dc,
            tokens,
            is_local,
            is_same_dc,
            is_secure,
            state,
            failure_count: 0,
            last_state_ts_secs: 0,
            fd: crate::cluster::failure_detector::PhiAccrual::default(),
        }
    }

    /// Index of the peer in the pool's array.
    #[must_use]
    pub fn idx(&self) -> u32 {
        self.idx
    }

    /// Endpoint reference.
    #[must_use]
    pub fn endpoint(&self) -> &PeerEndpoint {
        &self.endpoint
    }

    /// Rack name.
    #[must_use]
    pub fn rack(&self) -> &str {
        &self.rack
    }

    /// Datacenter name.
    #[must_use]
    pub fn dc(&self) -> &str {
        &self.dc
    }

    /// Token list.
    #[must_use]
    pub fn tokens(&self) -> &[DynToken] {
        &self.tokens
    }

    /// True for the local node.
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.is_local
    }

    /// True when the peer shares the local datacenter.
    #[must_use]
    pub fn is_same_dc(&self) -> bool {
        self.is_same_dc
    }

    /// True when the peer expects an encrypted dnode link.
    #[must_use]
    pub fn is_secure(&self) -> bool {
        self.is_secure
    }

    /// Current lifecycle state.
    #[must_use]
    pub fn state(&self) -> PeerState {
        self.state
    }

    /// Update the lifecycle state. The supplied `ts_secs` is the
    /// last-observed gossip timestamp, used by the
    /// failure detector to age the peer out.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
    /// use dynomite::hashkit::DynToken;
    /// let mut p = Peer::new(
    ///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    ///     vec![DynToken::from_u32(0)], true, true, false,
    /// );
    /// p.set_state(PeerState::Normal, 42);
    /// assert_eq!(p.state(), PeerState::Normal);
    /// assert_eq!(p.last_state_ts_secs(), 42);
    /// ```
    pub fn set_state(&mut self, state: PeerState, ts_secs: u64) {
        self.state = state;
        self.last_state_ts_secs = ts_secs;
    }

    /// Last observed gossip timestamp (epoch seconds).
    #[must_use]
    pub fn last_state_ts_secs(&self) -> u64 {
        self.last_state_ts_secs
    }

    /// Increment the consecutive-failure counter.
    pub fn record_failure(&mut self) {
        self.failure_count = self.failure_count.saturating_add(1);
    }

    /// Reset the consecutive-failure counter.
    pub fn record_success(&mut self) {
        self.failure_count = 0;
    }

    /// Current consecutive-failure count.
    #[must_use]
    pub fn failure_count(&self) -> u32 {
        self.failure_count
    }

    /// Borrow the phi-accrual failure detector for this peer.
    /// Used by the gossip task to record heartbeat arrivals and
    /// to query the suspicion level on every tick.
    #[must_use]
    pub fn failure_detector(&self) -> &crate::cluster::failure_detector::PhiAccrual {
        &self.fd
    }

    /// Mutably borrow the phi-accrual failure detector. Use
    /// from the gossip / heartbeat task.
    pub fn failure_detector_mut(&mut self) -> &mut crate::cluster::failure_detector::PhiAccrual {
        &mut self.fd
    }

    /// First (primary) token of this peer, if any.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// use dynomite::hashkit::DynToken;
    /// let p = Peer::new(
    ///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    ///     vec![DynToken::from_u32(7)], true, true, false,
    /// );
    /// assert_eq!(p.primary_token().unwrap().get_int(), 7);
    /// ```
    #[must_use]
    pub fn primary_token(&self) -> Option<&DynToken> {
        self.tokens.first()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(rack: &str, dc: &str, is_local: bool, is_same_dc: bool) -> Peer {
        Peer::new(
            0,
            PeerEndpoint::tcp("127.0.0.1".into(), 8101),
            rack.into(),
            dc.into(),
            vec![DynToken::from_u32(1)],
            is_local,
            is_same_dc,
            false,
        )
    }

    #[test]
    fn local_peer_starts_joining() {
        let p = mk("r", "d", true, true);
        assert_eq!(p.state(), PeerState::Joining);
    }

    #[test]
    fn remote_peer_starts_down() {
        let p = mk("r", "d", false, true);
        assert_eq!(p.state(), PeerState::Down);
    }

    #[test]
    fn state_names_round_trip() {
        for s in [
            PeerState::Unknown,
            PeerState::Joining,
            PeerState::Normal,
            PeerState::Standby,
            PeerState::Down,
            PeerState::Reset,
            PeerState::Leaving,
        ] {
            assert!(!s.name().is_empty());
        }
    }

    #[test]
    fn failure_counter_works() {
        let mut p = mk("r", "d", false, true);
        p.record_failure();
        p.record_failure();
        assert_eq!(p.failure_count(), 2);
        p.record_success();
        assert_eq!(p.failure_count(), 0);
    }

    #[test]
    fn routable_states() {
        assert!(PeerState::Normal.is_routable());
        assert!(PeerState::Joining.is_routable());
        assert!(!PeerState::Down.is_routable());
        assert!(!PeerState::Leaving.is_routable());
    }
}
