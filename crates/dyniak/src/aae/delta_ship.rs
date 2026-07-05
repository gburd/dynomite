//! Delta-shipping hook for CRDT anti-entropy.
//!
//! The tictac tree ([`crate::aae::tictac`]) localises *which* keys
//! diverge between two peers. Once a divergent CRDT key is found, the
//! reconciler must move state from one replica to the other. The
//! state-based path ships the whole CRDT value; this module ships a
//! *delta-interval* instead -- the join of every delta the source has
//! produced since the destination's last acknowledged point (see
//! Almeida, Baquero, Shapiro, "Delta State Replicated Data Types",
//! JPDC 2018).
//!
//! The hook is deliberately narrow: it owns the *shipping decision*
//! and the *delta-interval computation* for a
//! [`crate::datatypes::DeltaOrSet`] backed by a
//! [`crate::datatypes::DeltaBuffer`]. It does not touch the tree
//! structure or the three-phase exchange codec -- those decide *when*
//! and *for which key* a reconciliation runs. This separation is why
//! the delta upgrade is near-drop-in: the exchange still finds the
//! divergent key; only the payload it carries changes.
//!
//! # Graceful degradation
//!
//! A delta-CRDT degrades to a state-CRDT on first contact. When the
//! source has no acknowledged point for the destination (it has never
//! reconciled with it, so it cannot know which deltas the destination
//! is missing), the hook ships full state. Full-state shipping is
//! always correct -- it is the same join -- just larger on the wire.
//!
//! # Example
//!
//! ```
//! use dyniak::aae::delta_ship::{plan_shipment, Shipment};
//! use dyniak::datatypes::{ActorId, DeltaBuffer, DeltaOrSet};
//!
//! let a = ActorId::new("dc1", "a");
//! let mut src = DeltaOrSet::new();
//! let mut buf = DeltaBuffer::new();
//! buf.record(src.add(&a, b"x".to_vec()));
//!
//! // First contact with peer-b: no ack point, so full state ships.
//! assert!(matches!(plan_shipment(&src, &buf, "peer-b"), Shipment::FullState(_)));
//! ```

use crate::datatypes::{Crdt, DeltaBuffer, DeltaOrSet, OrSetDelta};

/// What a source replica decides to send a destination peer for one
/// CRDT key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Shipment {
    /// A delta-interval: the join of the source's buffered deltas the
    /// destination has not yet acknowledged. Carries the sequence
    /// number the destination should acknowledge once it has applied
    /// the interval.
    DeltaInterval {
        /// The joined delta fragment.
        delta: OrSetDelta,
        /// Highest sequence covered by this interval; the peer
        /// acknowledges this once applied.
        ack_seq: u64,
    },
    /// Full state, shipped when the destination has no known ack
    /// point (first contact) so no interval can be computed.
    FullState(DeltaOrSet),
    /// Nothing to ship: the destination is already current.
    UpToDate,
}

impl Shipment {
    /// Byte length this shipment would put on the AAE wire. The
    /// bandwidth bench compares the delta path against the full-state
    /// path with this.
    #[must_use]
    pub fn wire_len(&self) -> usize {
        match self {
            Shipment::DeltaInterval { delta, .. } => delta.wire_len(),
            Shipment::FullState(state) => state.wire_len(),
            Shipment::UpToDate => 0,
        }
    }
}

/// Decide what to ship `peer` for one `DeltaOrSet` key.
///
/// * First contact (no ack point for `peer`): [`Shipment::FullState`].
/// * Otherwise the delta-interval since `peer`'s ack point, or
///   [`Shipment::UpToDate`] when there is nothing newer.
#[must_use]
pub fn plan_shipment(state: &DeltaOrSet, buf: &DeltaBuffer, peer: &str) -> Shipment {
    let Some(since) = buf.next_needed(peer) else {
        // First contact: degrade to full-state shipping.
        return Shipment::FullState(state.clone());
    };
    match (buf.interval_since(since), buf.high_water()) {
        (Some(delta), Some(hw)) => Shipment::DeltaInterval { delta, ack_seq: hw },
        _ => Shipment::UpToDate,
    }
}

/// Apply a shipment received from a peer to a local replica, and
/// return the sequence number to acknowledge (if any). Applying is
/// the same lattice join whether the shipment is a delta or full
/// state, so the destination converges either way.
pub fn apply_shipment(local: &mut DeltaOrSet, shipment: &Shipment) -> Option<u64> {
    match shipment {
        Shipment::DeltaInterval { delta, ack_seq } => {
            local.merge_delta(delta);
            Some(*ack_seq)
        }
        Shipment::FullState(state) => {
            local.merge(state);
            None
        }
        Shipment::UpToDate => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datatypes::ActorId;

    fn aid(name: &str) -> ActorId {
        ActorId::new("dc1", name)
    }

    #[test]
    fn first_contact_ships_full_state() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let mut buf = DeltaBuffer::new();
        buf.record(src.add(&a, b"x".to_vec()));
        let ship = plan_shipment(&src, &buf, "peer-b");
        assert!(matches!(ship, Shipment::FullState(_)));

        // Applying full state converges a fresh replica.
        let mut dst = DeltaOrSet::new();
        apply_shipment(&mut dst, &ship);
        assert_eq!(dst.value(), src.value());
    }

    #[test]
    fn known_peer_ships_delta_interval() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let mut buf = DeltaBuffer::new();
        buf.record(src.add(&a, b"x".to_vec())); // seq 0
        buf.ack("peer-b", 0); // peer-b has seq 0

        // New mutation after the ack.
        buf.record(src.add(&a, b"y".to_vec())); // seq 1

        let ship = plan_shipment(&src, &buf, "peer-b");
        let Shipment::DeltaInterval { ack_seq, .. } = &ship else {
            panic!("expected delta interval, got {ship:?}");
        };
        assert_eq!(*ack_seq, 1);

        // Destination already has x; apply the interval -> gains y.
        let mut dst = DeltaOrSet::new();
        dst.add(&a, b"x".to_vec());
        let ack = apply_shipment(&mut dst, &ship);
        assert_eq!(ack, Some(1));
        assert!(dst.contains(b"y"));
    }

    #[test]
    fn current_peer_ships_nothing() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let mut buf = DeltaBuffer::new();
        buf.record(src.add(&a, b"x".to_vec())); // seq 0
        buf.ack("peer-b", 0);
        let ship = plan_shipment(&src, &buf, "peer-b");
        assert_eq!(ship, Shipment::UpToDate);
    }

    #[test]
    fn delta_interval_is_smaller_than_full_state() {
        // The whole point: after a long history, the interval for a
        // caught-up peer is far smaller than the full state.
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let mut buf = DeltaBuffer::new();
        for i in 0..100u32 {
            let k = format!("key-{i:04}");
            buf.record(src.add(&a, k.into_bytes()));
        }
        // Peer has caught up through seq 98; only one new element.
        buf.ack("peer-b", 98);
        let ship = plan_shipment(&src, &buf, "peer-b");
        let full = src.wire_len();
        assert!(
            ship.wire_len() * 10 < full,
            "delta {} not much smaller than full {}",
            ship.wire_len(),
            full
        );
    }
}
