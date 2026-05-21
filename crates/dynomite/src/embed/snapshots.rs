//! Owned point-in-time topology snapshots.
//!
//! Each type is `Clone` and free of internal locks so it can be
//! held across an `await` without blocking the runtime.

use crate::cluster::peer::{Peer, PeerEndpoint, PeerState};
use crate::cluster::Datacenter;
use crate::hashkit::DynToken;

/// Owned snapshot of one peer in the cluster ring.
///
/// # Examples
///
/// ```
/// use dynomite::embed::PeerSnapshot;
/// use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
/// use dynomite::hashkit::DynToken;
/// let p = Peer::new(
///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
///     vec![DynToken::from_u32(0)], true, true, false,
/// );
/// let snap = PeerSnapshot::from(&p);
/// assert_eq!(snap.idx, 0);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSnapshot {
    /// Index of the peer in the pool's peer array.
    pub idx: u32,
    /// Hostname or IP.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// Lifecycle state.
    pub state: PeerState,
    /// Token list at snapshot time.
    pub tokens: Vec<DynToken>,
    /// True for the local node.
    pub is_local: bool,
}

impl From<&Peer> for PeerSnapshot {
    fn from(p: &Peer) -> Self {
        Self {
            idx: p.idx(),
            host: p.endpoint().host().to_string(),
            port: p.endpoint().port(),
            dc: p.dc().to_string(),
            rack: p.rack().to_string(),
            state: p.state(),
            tokens: p.tokens().to_vec(),
            is_local: p.is_local(),
        }
    }
}

impl PeerSnapshot {
    /// Convert back to a runtime [`Peer`] (used by tests and
    /// by the embed-internal forwarder).
    #[must_use]
    pub fn to_peer(&self) -> Peer {
        Peer::new(
            self.idx,
            PeerEndpoint::tcp(self.host.clone(), self.port),
            self.rack.clone(),
            self.dc.clone(),
            self.tokens.clone(),
            self.is_local,
            true,
            false,
        )
    }
}

/// Owned snapshot of one rack in a datacenter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RackSnapshot {
    /// Rack name.
    pub name: String,
    /// Continuum size.
    pub continuum_len: usize,
}

/// Owned snapshot of one datacenter and its racks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatacenterSnapshot {
    /// DC name.
    pub name: String,
    /// Racks in this DC.
    pub racks: Vec<RackSnapshot>,
}

impl From<&Datacenter> for DatacenterSnapshot {
    fn from(dc: &Datacenter) -> Self {
        Self {
            name: dc.name().to_string(),
            racks: dc
                .racks()
                .iter()
                .map(|r| RackSnapshot {
                    name: r.name().to_string(),
                    continuum_len: r.continuums().len(),
                })
                .collect(),
        }
    }
}

/// Owned snapshot of the token ring.
///
/// Each entry is a `(token, owner_peer_idx)` pair.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RingSnapshot {
    /// Token-to-owner pairs in token order.
    pub entries: Vec<(DynToken, u32)>,
    /// Monotonic generation counter.
    pub generation: u64,
}
