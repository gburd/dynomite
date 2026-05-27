//! Integration tests for the `cluster::capability` module.
//!
//! Five named tests:
//!
//! * `register_two_caps_advertises_both`
//! * `negotiate_picks_max_common_value`
//! * `negotiate_returns_floor_when_no_overlap`
//! * `negotiate_unknown_capability_falls_through`
//! * `negotiate_round_trip_via_dnode_handshake_bytes`
//!
//! Each test wires the public surface only: no internals are
//! poked, so the suite doubles as a contract of the API the
//! cluster layer (and future wire-format upgrades) depend on.

use dynomite::cluster::capability::{
    default_registry, AaeTreeFormat, Capability, CapabilityAd, CapabilityAdEntry,
    CapabilityRegistry, CrdtObjectFormat, DnodeFramingVersion, GossipPhiNegotiable,
    CAP_AAE_TREE_FORMAT, CAP_CRDT_OBJECT_FORMAT, CAP_DNODE_FRAMING_VERSION,
    CAP_GOSSIP_PHI_NEGOTIABLE,
};
use dynomite::proto::dnode::Handshake;

/// Local cap that supports a configurable subset of u32 values.
/// "Highest" is `Ord::max`, mirroring the stock framing-version
/// cap.
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
fn register_two_caps_advertises_both() {
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
    let names: Vec<&str> = ad.entries().iter().map(CapabilityAdEntry::name).collect();
    assert_eq!(names, vec!["aae", "framing"]); // sorted alphabetically
    let framing = ad
        .entries()
        .iter()
        .find(|e| e.name() == "framing")
        .expect("framing entry");
    assert_eq!(framing.supported().len(), 2);
    let aae = ad
        .entries()
        .iter()
        .find(|e| e.name() == "aae")
        .expect("aae entry");
    assert_eq!(aae.supported().len(), 1);
}

#[test]
fn negotiate_picks_max_common_value() {
    let mut reg = CapabilityRegistry::new();
    reg.register(U32Cap {
        name: "framing",
        supported: vec![1, 2, 3],
    });
    // Peer supports {2, 3, 4}: highest common with local
    // {1, 2, 3} is 3.
    let peer = CapabilityAd::from_entries(vec![CapabilityAdEntry::new(
        "framing".into(),
        vec![
            2u32.to_le_bytes().to_vec(),
            3u32.to_le_bytes().to_vec(),
            4u32.to_le_bytes().to_vec(),
        ],
    )]);
    let result = reg.negotiate(&peer);
    let chosen = result.get("framing").expect("framing chosen");
    assert_eq!(chosen, &3u32.to_le_bytes());
    // current<C> reflects the negotiated value.
    let live = reg
        .current::<U32Cap>("framing")
        .expect("current framing decode");
    assert_eq!(live, 3);
}

#[test]
fn negotiate_returns_floor_when_no_overlap() {
    let mut reg = CapabilityRegistry::new();
    reg.register(U32Cap {
        name: "framing",
        supported: vec![2, 3], // floor = 2 (first entry)
    });
    // Peer ships {7, 9}: no overlap.
    let peer = CapabilityAd::from_entries(vec![CapabilityAdEntry::new(
        "framing".into(),
        vec![7u32.to_le_bytes().to_vec(), 9u32.to_le_bytes().to_vec()],
    )]);
    let result = reg.negotiate(&peer);
    let chosen = result.get("framing").expect("framing chosen");
    assert_eq!(
        chosen,
        &2u32.to_le_bytes(),
        "no-overlap negotiation must fall back to floor"
    );
    let live = reg.current::<U32Cap>("framing").expect("current");
    assert_eq!(live, 2);
}

#[test]
fn negotiate_unknown_capability_falls_through() {
    let mut reg = CapabilityRegistry::new();
    reg.register(U32Cap {
        name: "framing",
        supported: vec![1, 2],
    });
    // Peer ships an extra cap we have never registered, plus
    // the one we do know about.
    let peer = CapabilityAd::from_entries(vec![
        CapabilityAdEntry::new(
            "framing".into(),
            vec![1u32.to_le_bytes().to_vec(), 2u32.to_le_bytes().to_vec()],
        ),
        CapabilityAdEntry::new("from_the_future".into(), vec![vec![0xde, 0xad, 0xbe, 0xef]]),
    ]);
    let result = reg.negotiate(&peer);
    // Only the locally known cap appears in the result.
    assert_eq!(result.len(), 1);
    assert_eq!(result.get("framing").expect("framing"), &2u32.to_le_bytes());
    assert!(result.get("from_the_future").is_none());
    // current<C> for an unknown cap is None, never a panic.
    assert!(reg.current::<U32Cap>("from_the_future").is_none());
}

#[test]
fn negotiate_round_trip_via_dnode_handshake_bytes() {
    // Two registries: one local, one acting as the "peer".
    let local = default_registry();
    let mut peer = CapabilityRegistry::new();
    peer.register(DnodeFramingVersion);
    peer.register(AaeTreeFormat);
    peer.register(CrdtObjectFormat);
    peer.register(GossipPhiNegotiable);

    // Peer encodes its advertisement into a Handshake and ships
    // the bytes over the (here, in-process) wire.
    let peer_hs = Handshake::new(peer.local_advertise());
    let on_wire = peer_hs.encode();
    assert!(on_wire.starts_with(&Handshake::MAGIC));

    // Local decodes and negotiates.
    let received = Handshake::decode(&on_wire).expect("decode handshake");
    let result = local.negotiate(received.capabilities());

    // Each stock capability gets a value.
    assert_eq!(result.len(), 4);

    // Stock framing cap: local supports {1,2}, peer supports
    // {1,2}: highest common is 2.
    let v_framing = local
        .current::<DnodeFramingVersion>(CAP_DNODE_FRAMING_VERSION)
        .expect("framing");
    assert_eq!(v_framing, 2);

    // AAE tree: only v1 on either side.
    let v_aae = local
        .current::<AaeTreeFormat>(CAP_AAE_TREE_FORMAT)
        .expect("aae");
    assert_eq!(v_aae, 1);

    // CRDT object format: only v1.
    let v_crdt = local
        .current::<CrdtObjectFormat>(CAP_CRDT_OBJECT_FORMAT)
        .expect("crdt");
    assert_eq!(v_crdt, 1);

    // Phi negotiable: both sides advertise [false, true]; the
    // negotiated value is true.
    let v_phi = local
        .current::<GossipPhiNegotiable>(CAP_GOSSIP_PHI_NEGOTIABLE)
        .expect("phi");
    assert!(v_phi);

    // The handshake delta is small: the fixed prefix is six
    // bytes (4-byte magic + 2-byte flags) plus the encoded ad.
    assert_eq!(Handshake::header_len(), 6);
    let blank = Handshake::default().encode();
    // A handshake with an empty ad is exactly the header plus the
    // empty CapabilityAd encoding (3-byte CAP magic, 1-byte
    // version, 4-byte zero count).
    assert_eq!(blank.len(), Handshake::header_len() + 3 + 1 + 4);
}
