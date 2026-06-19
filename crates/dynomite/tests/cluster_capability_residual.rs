//! Residual coverage for `cluster::capability` decode error arms,
//! the floor/empty accessors, and the stock `GossipPhiNegotiable`
//! merge/decode arms not reached by the happy-path suite.

use dynomite::cluster::capability::{
    default_registry, Capability, CapabilityAd, CapabilityAdEntry, CapabilityCodecError,
    CapabilityRegistry, GossipPhiNegotiable, CAP_GOSSIP_PHI_NEGOTIABLE,
};

/// Local cap that supports a configurable subset of u32 values.
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

/// A second cap type registered under the same name as `U32Cap`
/// is impossible (names are static), so the downcast-failure arm
/// of `current` is reached by asking for the wrong `C`.
struct ByteCap;
impl Capability for ByteCap {
    type Value = u8;
    fn name(&self) -> &'static str {
        "framing"
    }
    fn supported_values(&self) -> Vec<u8> {
        vec![0]
    }
    fn merge(&self, _peer: &[u8]) -> Option<u8> {
        Some(0)
    }
    fn encode_value(&self, v: &u8) -> Vec<u8> {
        vec![*v]
    }
    fn decode_value(&self, b: &[u8]) -> Option<u8> {
        b.first().copied()
    }
}

#[test]
fn empty_ad_and_registry_accessors() {
    let ad = CapabilityAd::new();
    assert!(ad.entries().is_empty());
    let reg = CapabilityRegistry::new();
    assert!(reg.is_empty());
    assert_eq!(reg.len(), 0);
    // Debug rendering touches the field-printing arm.
    assert!(format!("{reg:?}").contains("CapabilityRegistry"));
}

#[test]
fn current_returns_floor_before_negotiation() {
    let mut reg = CapabilityRegistry::new();
    reg.register(U32Cap {
        name: "framing",
        supported: vec![5, 9],
    });
    // No negotiation yet: floor (first supported value) wins.
    assert_eq!(reg.current::<U32Cap>("framing"), Some(5));
    assert_eq!(reg.len(), 1);
    assert!(!reg.is_empty());
}

#[test]
fn current_for_missing_and_wrong_type_is_none() {
    let mut reg = CapabilityRegistry::new();
    reg.register(U32Cap {
        name: "framing",
        supported: vec![1],
    });
    // Unknown name.
    assert!(reg.current::<U32Cap>("nope").is_none());
    // Registered under a different concrete type: the downcast in
    // `current` fails and yields None rather than panicking.
    assert!(reg.current::<ByteCap>("framing").is_none());
}

#[test]
fn decode_rejects_too_many_entries() {
    // A hand-built blob declaring more entries than the cap will
    // accept: CAP + version + count=u32::MAX.
    let mut blob = b"CAP".to_vec();
    blob.push(1); // version
    blob.extend_from_slice(&u32::MAX.to_le_bytes());
    let err = CapabilityAd::decode(&blob).unwrap_err();
    assert!(matches!(err, CapabilityCodecError::TooManyEntries(_)));
}

#[test]
fn decode_rejects_oversized_name() {
    // One entry with a name length above CAP_AD_MAX_NAME_LEN
    // (256). We do not need the name bytes to follow; the length
    // check fires first.
    let mut blob = b"CAP".to_vec();
    blob.push(1);
    blob.extend_from_slice(&1u32.to_le_bytes()); // 1 entry
    blob.extend_from_slice(&300u16.to_le_bytes()); // name_len = 300
    let err = CapabilityAd::decode(&blob).unwrap_err();
    assert!(matches!(err, CapabilityCodecError::TooManyEntries(300)));
}

#[test]
fn decode_rejects_non_ascii_name() {
    let mut blob = b"CAP".to_vec();
    blob.push(1);
    blob.extend_from_slice(&1u32.to_le_bytes()); // 1 entry
    blob.extend_from_slice(&2u16.to_le_bytes()); // name_len = 2
    blob.extend_from_slice(&[0xff, 0xfe]); // non-ASCII name bytes
    let err = CapabilityAd::decode(&blob).unwrap_err();
    assert!(matches!(err, CapabilityCodecError::NonAsciiName));
}

#[test]
fn decode_rejects_oversized_value() {
    let mut blob = b"CAP".to_vec();
    blob.push(1);
    blob.extend_from_slice(&1u32.to_le_bytes()); // 1 entry
    blob.extend_from_slice(&3u16.to_le_bytes()); // name_len = 3
    blob.extend_from_slice(b"cap"); // name
    blob.extend_from_slice(&1u16.to_le_bytes()); // 1 value
    blob.extend_from_slice(&(64u32 * 1024).to_le_bytes()); // vlen > max
    let err = CapabilityAd::decode(&blob).unwrap_err();
    assert!(matches!(err, CapabilityCodecError::TooManyEntries(_)));
}

#[test]
fn decode_rejects_truncated_mid_value() {
    // Valid header + one entry header, but the declared value
    // bytes are missing.
    let mut blob = b"CAP".to_vec();
    blob.push(1);
    blob.extend_from_slice(&1u32.to_le_bytes()); // 1 entry
    blob.extend_from_slice(&3u16.to_le_bytes()); // name_len = 3
    blob.extend_from_slice(b"cap");
    blob.extend_from_slice(&1u16.to_le_bytes()); // 1 value
    blob.extend_from_slice(&4u32.to_le_bytes()); // vlen = 4 but none follow
    let err = CapabilityAd::decode(&blob).unwrap_err();
    assert!(matches!(err, CapabilityCodecError::Truncated));
}

#[test]
fn gossip_phi_merge_arms() {
    let cap = GossipPhiNegotiable;
    // Both declare true -> Some(true).
    assert_eq!(cap.merge(&[true]), Some(true));
    // Only false common -> Some(false).
    assert_eq!(cap.merge(&[false]), Some(false));
    // Peer declares neither known value -> None.
    assert_eq!(cap.merge(&[]), None);
    // decode_value covers all three arms.
    assert_eq!(cap.decode_value(&[0]), Some(false));
    assert_eq!(cap.decode_value(&[1]), Some(true));
    assert_eq!(cap.decode_value(&[2]), None);
    assert_eq!(cap.encode_value(&true), vec![1]);
}

#[test]
fn negotiated_capabilities_is_empty_reports_state() {
    // A registry with a registered cap that the peer never
    // advertises still produces a non-empty negotiated set (the
    // floor fallback), and the peer's unknown cap is dropped.
    let reg = default_registry();
    let empty_peer = CapabilityAd::new();
    let result = reg.negotiate(&empty_peer);
    assert!(!result.is_empty());
    assert_eq!(result.len(), reg.len());
    // The stock phi cap falls back to its floor (false).
    let phi = reg.current::<GossipPhiNegotiable>(CAP_GOSSIP_PHI_NEGOTIABLE);
    assert_eq!(phi, Some(false));
    // Build an explicit empty NegotiatedCapabilities via no-cap
    // registry to exercise is_empty()==true.
    let bare = CapabilityRegistry::new();
    let none = bare.negotiate(&empty_peer);
    assert!(none.is_empty());
    assert_eq!(none.len(), 0);
}

#[test]
fn round_trip_entry_accessor_supported_blobs() {
    let entry = CapabilityAdEntry::new("cap".into(), vec![vec![1, 2, 3]]);
    assert_eq!(entry.name(), "cap");
    assert_eq!(entry.supported(), &[vec![1u8, 2, 3]]);
}
