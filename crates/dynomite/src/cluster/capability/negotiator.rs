//! Negotiation logic shared between [`CapabilityRegistry`] and
//! its tests.
//!
//! The flow is:
//!
//! 1. For every capability the local node has registered, look
//!    up the matching entry in the peer ad (by capability name).
//! 2. If the peer advertises the capability, ask the local cap
//!    to merge: this returns the highest local value also
//!    supported by the peer, or `None` if there is no overlap.
//! 3. If the peer does not advertise the capability, or the
//!    merge returned `None`, fall back to the local "floor"
//!    value (the lowest-preference local value).
//!
//! Capabilities that the peer advertises but the local node has
//! never registered are silently ignored: there is nothing the
//! local node can pick because it does not know how to decode
//! the values.
//!
//! [`CapabilityRegistry`]: super::CapabilityRegistry

use std::collections::HashMap;

use crate::cluster::capability::registry::{CapabilityAd, CapabilityRegistry};

/// Outcome of a single round of negotiation.
///
/// Each entry is `(capability name, encoded chosen value)`. The
/// caller decodes the value via the registered
/// [`crate::cluster::capability::Capability::decode_value`] of
/// the matching cap.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NegotiatedCapabilities {
    chosen: HashMap<String, Vec<u8>>,
}

impl NegotiatedCapabilities {
    /// Construct an empty result.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or overwrite the chosen value for `name`.
    pub fn insert(&mut self, name: String, value: Vec<u8>) {
        self.chosen.insert(name, value);
    }

    /// Look up the encoded value picked for `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.chosen.get(name).map(Vec::as_slice)
    }

    /// True when the result holds no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chosen.is_empty()
    }

    /// Number of negotiated entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chosen.len()
    }

    /// Iterate over `(name, value-bytes)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Vec<u8>)> {
        self.chosen.iter()
    }
}

/// Compute the negotiated value for every locally registered
/// capability, falling back to the floor when the peer does not
/// advertise overlap.
///
/// The function is exported as `pub` at the module level so
/// alternative storage layouts (or future test harnesses that
/// wrap a registry) can reuse it without duplicating the
/// floor-fallback logic.
pub(crate) fn negotiate_with_floor(
    registry: &CapabilityRegistry,
    peer_ad: &CapabilityAd,
) -> NegotiatedCapabilities {
    let slots = registry.slots_for_negotiation();
    // Build a peer lookup once: name -> peer-supplied byte
    // blobs.
    let mut peer_by_name: HashMap<&str, &[Vec<u8>]> =
        HashMap::with_capacity(peer_ad.entries().len());
    for entry in peer_ad.entries() {
        peer_by_name.insert(entry.name(), entry.supported());
    }
    let mut out = NegotiatedCapabilities::new();
    for (name, slot) in slots {
        let chosen = if let Some(peer_supported) = peer_by_name.get(name) {
            match slot.merge_bytes(peer_supported) {
                Some(v) => v,
                None => slot.floor_bytes().to_vec(),
            }
        } else {
            // Peer never declared this capability - the safest
            // assumption is "peer only supports the floor". The
            // local node treats the floor as authoritative.
            slot.floor_bytes().to_vec()
        };
        out.insert((*name).to_string(), chosen);
    }
    // Capabilities the peer ships but the local node does not
    // know about are ignored: there is no way to pick a value
    // for an unknown cap because the registry cannot decode it.
    out
}
