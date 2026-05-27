//! Cluster-wide capability negotiation.
//!
//! Every node advertises a set of typed capabilities at the
//! gossip handshake. For each capability name the cluster picks
//! the lowest-common-denominator value: the highest local value
//! that the peer also supports, falling back to a "floor" value
//! when there is no overlap. The mechanism mirrors the design of
//! `riak_core_capability` and is intended to let us land
//! wire-format changes (e.g. dnode framing v2, AAE tree format
//! v2) behind feature flags that flip on automatically once every
//! peer in a mixed-version cluster reports support.
//!
//! The public surface is small:
//!
//! * [`Capability`] - the trait each capability implements; it
//!   carries the typed value, the local supported set, and the
//!   merge rule.
//! * [`CapabilityRegistry`] - the per-node registry that owns
//!   capability instances, generates the local advertisement, and
//!   resolves negotiated values.
//! * [`CapabilityAd`] - the wire-bound advertisement (a list of
//!   `(name, supported_values)`).
//! * [`NegotiatedCapabilities`] - the result of a single
//!   negotiation, keyed by capability name.
//!
//! # Wire encoding
//!
//! [`CapabilityAd`] travels on the wire inside the dnode
//! handshake (see [`crate::proto::dnode::Handshake`]). The
//! encoding is a simple length-prefixed binary layout that uses
//! only the standard library; no external codec is dragged in.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::capability::{Capability, CapabilityRegistry};
//!
//! struct Framing;
//! impl Capability for Framing {
//!     type Value = u32;
//!     fn name(&self) -> &'static str { "framing" }
//!     fn supported_values(&self) -> Vec<u32> { vec![1, 2] }
//!     fn merge(&self, peer: &[u32]) -> Option<u32> {
//!         self.supported_values()
//!             .into_iter()
//!             .filter(|v| peer.contains(v))
//!             .max()
//!     }
//!     fn encode_value(&self, v: &u32) -> Vec<u8> { v.to_le_bytes().to_vec() }
//!     fn decode_value(&self, b: &[u8]) -> Option<u32> {
//!         <[u8; 4]>::try_from(b).ok().map(u32::from_le_bytes)
//!     }
//! }
//!
//! let mut reg = CapabilityRegistry::new();
//! reg.register(Framing);
//! let ad = reg.local_advertise();
//! assert_eq!(ad.entries().len(), 1);
//! ```

mod negotiator;
mod registry;

pub use self::negotiator::NegotiatedCapabilities;
pub use self::registry::{
    Capability, CapabilityAd, CapabilityAdEntry, CapabilityCodecError, CapabilityRegistry,
};

/// Capability name shipped in the v0.0.1 registry.
///
/// The first real consumer of this name will be the dnode
/// framing v2 work; today only v1 is implemented.
pub const CAP_DNODE_FRAMING_VERSION: &str = "dnode_framing_version";

/// Capability name for the active append-anti-entropy tree
/// format. Reserved for future format upgrades; today only v1
/// is implemented.
pub const CAP_AAE_TREE_FORMAT: &str = "aae_tree_format";

/// Capability name for the on-the-wire CRDT object format used
/// by the entropy reconciliation path.
pub const CAP_CRDT_OBJECT_FORMAT: &str = "crdt_object_format";

/// Capability name for whether the peer accepts a dynamic phi
/// threshold negotiated at runtime.
pub const CAP_GOSSIP_PHI_NEGOTIABLE: &str = "gossip_phi_threshold_negotiable";

/// Stock capability advertising the supported dnode framing
/// versions on this build. Today the local set is `[1, 2]`; v1
/// is the implemented framing and v2 is reserved for the next
/// stage's wire upgrade.
pub struct DnodeFramingVersion;

impl Capability for DnodeFramingVersion {
    type Value = u32;
    fn name(&self) -> &'static str {
        CAP_DNODE_FRAMING_VERSION
    }
    fn supported_values(&self) -> Vec<u32> {
        vec![1, 2]
    }
    fn merge(&self, peer: &[u32]) -> Option<u32> {
        self.supported_values()
            .into_iter()
            .filter(|v| peer.contains(v))
            .max()
    }
    fn encode_value(&self, v: &u32) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }
    fn decode_value(&self, b: &[u8]) -> Option<u32> {
        <[u8; 4]>::try_from(b).ok().map(u32::from_le_bytes)
    }
}

/// Stock capability advertising the supported AAE tree formats.
pub struct AaeTreeFormat;

impl Capability for AaeTreeFormat {
    type Value = u32;
    fn name(&self) -> &'static str {
        CAP_AAE_TREE_FORMAT
    }
    fn supported_values(&self) -> Vec<u32> {
        vec![1]
    }
    fn merge(&self, peer: &[u32]) -> Option<u32> {
        self.supported_values()
            .into_iter()
            .filter(|v| peer.contains(v))
            .max()
    }
    fn encode_value(&self, v: &u32) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }
    fn decode_value(&self, b: &[u8]) -> Option<u32> {
        <[u8; 4]>::try_from(b).ok().map(u32::from_le_bytes)
    }
}

/// Stock capability advertising the supported CRDT object
/// wire formats.
pub struct CrdtObjectFormat;

impl Capability for CrdtObjectFormat {
    type Value = u32;
    fn name(&self) -> &'static str {
        CAP_CRDT_OBJECT_FORMAT
    }
    fn supported_values(&self) -> Vec<u32> {
        vec![1]
    }
    fn merge(&self, peer: &[u32]) -> Option<u32> {
        self.supported_values()
            .into_iter()
            .filter(|v| peer.contains(v))
            .max()
    }
    fn encode_value(&self, v: &u32) -> Vec<u8> {
        v.to_le_bytes().to_vec()
    }
    fn decode_value(&self, b: &[u8]) -> Option<u32> {
        <[u8; 4]>::try_from(b).ok().map(u32::from_le_bytes)
    }
}

/// Stock capability advertising whether this node will accept a
/// dynamic phi threshold pushed by the cluster.
pub struct GossipPhiNegotiable;

impl Capability for GossipPhiNegotiable {
    type Value = bool;
    fn name(&self) -> &'static str {
        CAP_GOSSIP_PHI_NEGOTIABLE
    }
    fn supported_values(&self) -> Vec<bool> {
        // Lowest preference first: a peer that does not accept
        // dynamic phi at all is the safe floor; `true` is the
        // forward-looking value we prefer.
        vec![false, true]
    }
    fn merge(&self, peer: &[bool]) -> Option<bool> {
        // "Highest" common value: prefer `true` when both sides
        // declare it; otherwise the only common entry is `false`.
        if self.supported_values().contains(&true) && peer.contains(&true) {
            Some(true)
        } else if self.supported_values().contains(&false) && peer.contains(&false) {
            Some(false)
        } else {
            None
        }
    }
    fn encode_value(&self, v: &bool) -> Vec<u8> {
        vec![u8::from(*v)]
    }
    fn decode_value(&self, b: &[u8]) -> Option<bool> {
        match b {
            [0] => Some(false),
            [1] => Some(true),
            _ => None,
        }
    }
}

/// Construct a registry pre-populated with the v0.0.1 stock
/// capabilities: dnode framing version, AAE tree format, CRDT
/// object format, and the dynamic-phi-threshold flag.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::capability::{default_registry, CAP_DNODE_FRAMING_VERSION};
/// let reg = default_registry();
/// let ad = reg.local_advertise();
/// assert!(ad.entries().iter().any(|e| e.name() == CAP_DNODE_FRAMING_VERSION));
/// ```
#[must_use]
pub fn default_registry() -> CapabilityRegistry {
    let mut reg = CapabilityRegistry::new();
    reg.register(DnodeFramingVersion);
    reg.register(AaeTreeFormat);
    reg.register(CrdtObjectFormat);
    reg.register(GossipPhiNegotiable);
    reg
}
