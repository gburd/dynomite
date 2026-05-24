//! Cluster layer: pools, peers, datacenters, racks, gossip,
//! snitch, vnode dispatch, and the cluster-aware
//! [`Dispatcher`](crate::net::Dispatcher) implementation.
//!
//! This module owns the cluster-wide data structures the C
//! reference engine threaded through `struct server_pool` and the
//! peer/dc/rack arrays. It is the seam between the per-connection
//! state machines from [`crate::net`] (which only know about a
//! single peer) and the routing logic that decides which peers
//! receive a given request.
//!
//! Public surface:
//!
//! * [`peer::Peer`] / [`peer::PeerState`] - per-peer record.
//! * [`datacenter::Datacenter`] / [`datacenter::Rack`] - topology.
//! * [`vnode::dispatch`] - token ring lookup.
//! * [`snitch`] - rack-distance helpers.
//! * [`pool::ServerPool`] - cluster-wide owner.
//! * [`gossip::GossipState`] / [`gossip::GossipConfig`] - gossip
//!   bookkeeping.
//! * [`dispatch::ClusterDispatcher`] - the
//!   [`crate::net::Dispatcher`] implementation that replaces
//!   [`crate::net::NoopDispatcher`] for production wiring.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::peer::{Peer, PeerEndpoint};
//! use dynomite::hashkit::DynToken;
//! let p = Peer::new(
//!     0,
//!     PeerEndpoint::tcp("127.0.0.1".into(), 8101),
//!     "rack1".into(),
//!     "dc1".into(),
//!     vec![DynToken::from_u32(1)],
//!     true,
//!     true,
//!     false,
//! );
//! assert_eq!(p.dc(), "dc1");
//! ```

pub mod datacenter;
pub mod dispatch;
pub mod failure_detector;
pub mod gossip;
pub mod hints;
pub mod peer;
pub mod pool;
pub mod snitch;
pub mod vnode;

pub use self::datacenter::{Continuum, Datacenter, Rack};
pub use self::dispatch::{ClusterDispatcher, DispatchPlan, ReplicaTarget};
pub use self::gossip::{
    parse_seed_node, GossipConfig, GossipHandler, GossipNode, GossipState, GossipStep, SeedRecord,
};
pub use self::hints::{Hint, HintStore, HintStoreError, HintStoreStats};
pub use self::peer::{Peer, PeerEndpoint, PeerState};
pub use self::pool::{PoolConfig, ServerPool};
pub use self::snitch::{rack_distance, RackDistance};
pub use self::vnode::{dispatch as vnode_dispatch, rebuild_continuums, PeerTokens};
