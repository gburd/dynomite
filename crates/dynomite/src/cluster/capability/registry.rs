//! Type-erased capability registry plus the [`Capability`] trait.
//!
//! Capabilities advertise typed values, but the registry must
//! treat them uniformly so it can encode an advertisement,
//! receive a peer ad, and look up negotiated values by name. The
//! erasure is handled internally; the public surface stays
//! generic over the user's [`Capability::Value`] type.

use std::any::Any;
use std::collections::HashMap;

use parking_lot::RwLock;

use crate::cluster::capability::negotiator::{negotiate_with_floor, NegotiatedCapabilities};

/// Magic prefix marking an encoded [`CapabilityAd`] blob.
///
/// The four bytes are stable on the wire; bumping the format
/// requires bumping the trailing version byte.
const CAP_AD_MAGIC: [u8; 3] = *b"CAP";

/// Format version embedded in encoded [`CapabilityAd`] blobs.
const CAP_AD_VERSION: u8 = 1;

/// Trait every capability implements.
///
/// `Value` is the typed representation of a single supported
/// value. The registry serialises values via
/// [`Capability::encode_value`] / [`Capability::decode_value`] so
/// no third-party serialisation dependency is required.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::capability::Capability;
///
/// struct Bool;
/// impl Capability for Bool {
///     type Value = bool;
///     fn name(&self) -> &'static str { "feature" }
///     fn supported_values(&self) -> Vec<bool> { vec![false, true] }
///     fn merge(&self, peer: &[bool]) -> Option<bool> {
///         if peer.contains(&true) { Some(true) }
///         else if peer.contains(&false) { Some(false) }
///         else { None }
///     }
///     fn encode_value(&self, v: &bool) -> Vec<u8> { vec![u8::from(*v)] }
///     fn decode_value(&self, b: &[u8]) -> Option<bool> {
///         match b { [0] => Some(false), [1] => Some(true), _ => None }
///     }
/// }
/// ```
pub trait Capability: Any + Send + Sync + 'static {
    /// The typed value this capability negotiates.
    type Value: Clone + Eq + Send + Sync + 'static;

    /// Stable on-the-wire name. Must be ASCII.
    fn name(&self) -> &'static str;

    /// Locally supported values, ordered from lowest preference
    /// to highest preference. The first element is also used as
    /// the "floor" when negotiation finds no overlap.
    fn supported_values(&self) -> Vec<Self::Value>;

    /// Returns the highest local value also supported by `peer`,
    /// or `None` when there is no overlap. The notion of
    /// "highest" is owned by the implementation.
    fn merge(&self, peer_supports: &[Self::Value]) -> Option<Self::Value>;

    /// Serialise a value to a stable byte sequence. Used to
    /// build the on-the-wire advertisement.
    fn encode_value(&self, value: &Self::Value) -> Vec<u8>;

    /// Inverse of [`Capability::encode_value`]. Returning `None`
    /// causes the registry to drop the malformed value when
    /// merging a peer ad.
    fn decode_value(&self, bytes: &[u8]) -> Option<Self::Value>;
}

/// Errors produced while decoding a [`CapabilityAd`] blob.
#[derive(Debug, thiserror::Error)]
pub enum CapabilityCodecError {
    /// Buffer ended mid-record.
    #[error("capability advertisement truncated")]
    Truncated,
    /// The leading magic / version did not match.
    #[error("capability advertisement: invalid magic or version")]
    BadMagic,
    /// A capability name contained a non-ASCII byte.
    #[error("capability advertisement: non-ASCII capability name")]
    NonAsciiName,
    /// The encoded entry count exceeded the safety bound.
    #[error("capability advertisement: too many entries ({0})")]
    TooManyEntries(usize),
}

/// One entry in a [`CapabilityAd`]: a capability name and the
/// list of opaque, capability-defined value blobs the advertising
/// peer supports.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityAdEntry {
    name: String,
    supported: Vec<Vec<u8>>,
}

impl CapabilityAdEntry {
    /// Build an entry from already-encoded value blobs.
    #[must_use]
    pub fn new(name: String, supported: Vec<Vec<u8>>) -> Self {
        Self { name, supported }
    }

    /// Capability name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Supported value blobs in the advertiser's preference
    /// order.
    #[must_use]
    pub fn supported(&self) -> &[Vec<u8>] {
        &self.supported
    }
}

/// On-the-wire advertisement built by [`CapabilityRegistry::local_advertise`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapabilityAd {
    entries: Vec<CapabilityAdEntry>,
}

/// Maximum number of entries we will encode into or decode from
/// a single advertisement. Intentionally far above what we ever
/// expect to use; the bound exists to reject garbage payloads.
const CAP_AD_MAX_ENTRIES: usize = 1024;

/// Maximum byte length of a single value blob inside an entry.
const CAP_AD_MAX_VALUE_LEN: usize = 16 * 1024;

/// Maximum byte length of a single capability name.
const CAP_AD_MAX_NAME_LEN: usize = 256;

impl CapabilityAd {
    /// Construct an empty advertisement.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build an advertisement from pre-shaped entries.
    #[must_use]
    pub fn from_entries(entries: Vec<CapabilityAdEntry>) -> Self {
        Self { entries }
    }

    /// Read-only view of the advertised entries.
    #[must_use]
    pub fn entries(&self) -> &[CapabilityAdEntry] {
        &self.entries
    }

    /// Serialise the advertisement to a length-prefixed byte
    /// stream. The encoding is stable, ASCII-clean for capability
    /// names, and uses only the standard library.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::capability::{CapabilityAd, CapabilityAdEntry};
    /// let ad = CapabilityAd::from_entries(vec![
    ///     CapabilityAdEntry::new("framing".into(), vec![vec![1, 0, 0, 0]]),
    /// ]);
    /// let bytes = ad.encode();
    /// let back = CapabilityAd::decode(&bytes).unwrap();
    /// assert_eq!(back, ad);
    /// ```
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.entries.len() * 32);
        out.extend_from_slice(&CAP_AD_MAGIC);
        out.push(CAP_AD_VERSION);
        let count = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_le_bytes());
        for entry in &self.entries {
            let name_bytes = entry.name.as_bytes();
            let name_len = u16::try_from(name_bytes.len()).unwrap_or(u16::MAX);
            out.extend_from_slice(&name_len.to_le_bytes());
            out.extend_from_slice(name_bytes);
            let val_count = u16::try_from(entry.supported.len()).unwrap_or(u16::MAX);
            out.extend_from_slice(&val_count.to_le_bytes());
            for value in &entry.supported {
                let vlen = u32::try_from(value.len()).unwrap_or(u32::MAX);
                out.extend_from_slice(&vlen.to_le_bytes());
                out.extend_from_slice(value);
            }
        }
        out
    }

    /// Inverse of [`CapabilityAd::encode`]. Rejects malformed or
    /// truncated input with a typed error.
    pub fn decode(mut bytes: &[u8]) -> Result<Self, CapabilityCodecError> {
        if bytes.len() < CAP_AD_MAGIC.len() + 1 + 4 {
            return Err(CapabilityCodecError::Truncated);
        }
        if bytes[..CAP_AD_MAGIC.len()] != CAP_AD_MAGIC {
            return Err(CapabilityCodecError::BadMagic);
        }
        bytes = &bytes[CAP_AD_MAGIC.len()..];
        if bytes[0] != CAP_AD_VERSION {
            return Err(CapabilityCodecError::BadMagic);
        }
        bytes = &bytes[1..];
        let count = read_u32(&mut bytes)?;
        let count_us = usize::try_from(count).unwrap_or(usize::MAX);
        if count_us > CAP_AD_MAX_ENTRIES {
            return Err(CapabilityCodecError::TooManyEntries(count_us));
        }
        let mut entries = Vec::with_capacity(count_us);
        for _ in 0..count_us {
            let name_len = read_u16(&mut bytes)? as usize;
            if name_len > CAP_AD_MAX_NAME_LEN {
                return Err(CapabilityCodecError::TooManyEntries(name_len));
            }
            let name_bytes = read_slice(&mut bytes, name_len)?;
            if !name_bytes.is_ascii() {
                return Err(CapabilityCodecError::NonAsciiName);
            }
            // Safe because we just checked is_ascii: ASCII bytes
            // are valid UTF-8 by construction.
            let name = std::str::from_utf8(name_bytes)
                .map_err(|_| CapabilityCodecError::NonAsciiName)?
                .to_string();
            let val_count = read_u16(&mut bytes)? as usize;
            let mut supported = Vec::with_capacity(val_count);
            for _ in 0..val_count {
                let vlen = read_u32(&mut bytes)? as usize;
                if vlen > CAP_AD_MAX_VALUE_LEN {
                    return Err(CapabilityCodecError::TooManyEntries(vlen));
                }
                let vbytes = read_slice(&mut bytes, vlen)?;
                supported.push(vbytes.to_vec());
            }
            entries.push(CapabilityAdEntry::new(name, supported));
        }
        Ok(Self { entries })
    }
}

fn read_slice<'a>(cur: &mut &'a [u8], len: usize) -> Result<&'a [u8], CapabilityCodecError> {
    if cur.len() < len {
        return Err(CapabilityCodecError::Truncated);
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}

fn read_u16(cur: &mut &[u8]) -> Result<u16, CapabilityCodecError> {
    let bytes = read_slice(cur, 2)?;
    let arr: [u8; 2] = bytes.try_into().expect("invariant: read_slice(2)");
    Ok(u16::from_le_bytes(arr))
}

fn read_u32(cur: &mut &[u8]) -> Result<u32, CapabilityCodecError> {
    let bytes = read_slice(cur, 4)?;
    let arr: [u8; 4] = bytes.try_into().expect("invariant: read_slice(4)");
    Ok(u32::from_le_bytes(arr))
}

/// Type-erased merge closure stored alongside each registered
/// capability. Defined as a type alias so the `Slot` definition
/// stays readable.
type MergeFn = Box<dyn Fn(&[Vec<u8>]) -> Option<Vec<u8>> + Send + Sync>;

/// Slot stored in the registry for one registered capability.
pub(crate) struct Slot {
    /// The original boxed cap, kept type-erased so callers can
    /// downcast back via [`Capability::decode_value`] inside
    /// [`CapabilityRegistry::current`].
    cap: Box<dyn Any + Send + Sync>,
    /// Locally supported values pre-encoded for fast ad
    /// generation.
    supported_bytes: Vec<Vec<u8>>,
    /// Floor value, pre-encoded. Used when negotiation finds no
    /// overlap.
    floor_bytes: Vec<u8>,
    /// Type-erased merge: takes peer-supplied byte blobs, picks
    /// the highest common value, returns it pre-encoded.
    merge: MergeFn,
}

/// Per-node registry that owns capability instances, generates
/// the local advertisement, and stores the most recently
/// negotiated value for each capability.
pub struct CapabilityRegistry {
    slots: HashMap<&'static str, Slot>,
    negotiated: RwLock<HashMap<String, Vec<u8>>>,
}

impl Default for CapabilityRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for CapabilityRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilityRegistry")
            .field("registered", &self.slots.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl CapabilityRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            slots: HashMap::new(),
            negotiated: RwLock::new(HashMap::new()),
        }
    }

    /// Register a capability. Re-registering a capability with
    /// the same name replaces the previous entry; the cluster
    /// layer never registers two caps with the same name, so
    /// this only matters for tests.
    pub fn register<C: Capability>(&mut self, cap: C) {
        let name = cap.name();
        assert!(name.is_ascii(), "capability name must be ASCII: {name:?}");
        let supported_bytes: Vec<Vec<u8>> = cap
            .supported_values()
            .iter()
            .map(|v| cap.encode_value(v))
            .collect();
        // Floor: the first element of supported_values, the
        // lowest-preferred local value. Documented in the trait
        // contract.
        let floor_bytes = supported_bytes
            .first()
            .cloned()
            .expect("capability must declare at least one supported value");
        // Build a type-erased merge closure that decodes peer
        // blobs, calls the typed merge, and re-encodes.
        let cap_arc = std::sync::Arc::new(cap);
        let cap_for_merge = cap_arc.clone();
        let merge: MergeFn = Box::new(move |peer_blobs: &[Vec<u8>]| {
            let peer: Vec<C::Value> = peer_blobs
                .iter()
                .filter_map(|b| cap_for_merge.decode_value(b))
                .collect();
            cap_for_merge
                .merge(&peer)
                .map(|v| cap_for_merge.encode_value(&v))
        });
        let cap_any: Box<dyn Any + Send + Sync> = Box::new(cap_arc);
        self.slots.insert(
            name,
            Slot {
                cap: cap_any,
                supported_bytes,
                floor_bytes,
                merge,
            },
        );
        // Drop any stale negotiated entry for this name so
        // re-registration starts from the floor.
        self.negotiated.write().remove(name);
    }

    /// Build the advertisement to ship to peers.
    #[must_use]
    pub fn local_advertise(&self) -> CapabilityAd {
        let mut entries: Vec<CapabilityAdEntry> = self
            .slots
            .iter()
            .map(|(name, slot)| {
                CapabilityAdEntry::new((*name).to_string(), slot.supported_bytes.clone())
            })
            .collect();
        // Stable order: alphabetical by name. The HashMap
        // iteration order is otherwise nondeterministic, which
        // would make test assertions and gossip diffs flaky.
        entries.sort_by(|a, b| a.name().cmp(b.name()));
        CapabilityAd::from_entries(entries)
    }

    /// Resolve `peer_ad` against the locally registered caps.
    ///
    /// Returns a [`NegotiatedCapabilities`] keyed by capability
    /// name. The registry also caches each negotiated value so
    /// later calls to [`CapabilityRegistry::current`] reflect the
    /// most recent negotiation.
    ///
    /// Capabilities present in `peer_ad` but not registered
    /// locally fall through silently: the negotiator can only
    /// pick a value for capabilities both sides know about.
    pub fn negotiate(&self, peer_ad: &CapabilityAd) -> NegotiatedCapabilities {
        let result = negotiate_with_floor(self, peer_ad);
        // Cache the negotiated bytes so `current()` sees them.
        let mut neg = self.negotiated.write();
        for (name, value) in result.iter() {
            neg.insert(name.clone(), value.clone());
        }
        result
    }

    /// Return the currently active value for the named
    /// capability, decoded with the registered cap's
    /// [`Capability::decode_value`].
    ///
    /// Returns `None` when no capability with that name is
    /// registered, when the type parameter `C` does not match
    /// the registered cap, or when the stored bytes fail to
    /// decode (which would be a registry bug).
    ///
    /// Before any negotiation has happened the floor value
    /// (lowest-preference local value) is returned.
    pub fn current<C: Capability>(&self, name: &str) -> Option<C::Value> {
        let slot = self.slots.get(name)?;
        let cap_arc = slot.cap.downcast_ref::<std::sync::Arc<C>>()?;
        let neg = self.negotiated.read();
        let bytes: &[u8] = neg
            .get(name)
            .map_or(slot.floor_bytes.as_slice(), Vec::as_slice);
        cap_arc.decode_value(bytes)
    }

    /// Number of registered capabilities. Useful in tests and
    /// diagnostics.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// True when no capabilities have been registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Internal slot accessor used by the negotiator.
    pub(crate) fn slots_for_negotiation(&self) -> &HashMap<&'static str, Slot> {
        &self.slots
    }
}

impl Slot {
    pub(crate) fn floor_bytes(&self) -> &[u8] {
        &self.floor_bytes
    }

    pub(crate) fn merge_bytes(&self, peer: &[Vec<u8>]) -> Option<Vec<u8>> {
        (self.merge)(peer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct U32Cap {
        name: &'static str,
        supported: Vec<u32>,
    }
    impl Capability for U32Cap {
        type Value = u32;
        fn name(&self) -> &'static str {
            self.name
        }
        fn supported_values(&self) -> Vec<u32> {
            self.supported.clone()
        }
        fn merge(&self, peer: &[u32]) -> Option<u32> {
            self.supported
                .iter()
                .filter(|v| peer.contains(v))
                .max()
                .copied()
        }
        fn encode_value(&self, v: &u32) -> Vec<u8> {
            v.to_le_bytes().to_vec()
        }
        fn decode_value(&self, b: &[u8]) -> Option<u32> {
            <[u8; 4]>::try_from(b).ok().map(u32::from_le_bytes)
        }
    }

    #[test]
    fn ad_round_trips() {
        let mut reg = CapabilityRegistry::new();
        reg.register(U32Cap {
            name: "framing",
            supported: vec![1, 2],
        });
        reg.register(U32Cap {
            name: "aae",
            supported: vec![1],
        });
        let ad = reg.local_advertise();
        let bytes = ad.encode();
        let back = CapabilityAd::decode(&bytes).expect("decode");
        assert_eq!(back, ad);
    }

    #[test]
    fn ad_decode_rejects_bad_magic() {
        let err = CapabilityAd::decode(&[0; 16]).unwrap_err();
        assert!(matches!(err, CapabilityCodecError::BadMagic));
    }

    #[test]
    fn ad_decode_rejects_truncated() {
        let err = CapabilityAd::decode(&[]).unwrap_err();
        assert!(matches!(err, CapabilityCodecError::Truncated));
    }
}
