//! Delta-state observed-remove set (delta-CRDT).
//!
//! This is a delta-mutation upgrade of [`crate::datatypes::OrSet`].
//! It carries the same add-wins observed-remove semantics and the
//! same `(actor, counter)` dot tags, but each mutation additionally
//! returns a small [`OrSetDelta`] -- a join-irreducible fragment of
//! the full state -- so replication and anti-entropy can ship the
//! delta instead of the whole set.
//!
//! # Why this converges
//!
//! The design follows Almeida, Baquero and Shapiro, "Delta State
//! Replicated Data Types" (JPDC 2018) and "Efficient State-based
//! CRDTs by Delta-Mutation" (2016). The three facts that make it
//! safe:
//!
//! 1. A delta is a value in *the same join-semilattice* as a full
//!    state. Here [`OrSetDelta`] is literally a partial
//!    [`DeltaOrSet`] state (a subset of the element tag maps), so
//!    "join a delta" and "join a full state" are the identical
//!    operation ([`OrSetDelta::join`]). No separate lattice, no
//!    separate merge rule to keep in sync.
//! 2. A delta-mutator returns a *join-irreducible* fragment: the
//!    smallest state whose join with the pre-state yields the
//!    post-state. For an add that is the single new dot plus the
//!    element key; for a remove it is the observed dots moved into
//!    the tombstone side. Crucially the fragment carries the causal
//!    context it needs (the dots) so an out-of-order or duplicated
//!    delta still joins to the correct least-upper-bound.
//! 3. Because join is associative, commutative and idempotent, any
//!    two replicas that have joined the *same set* of deltas (in any
//!    order, with duplicates) reach the identical state -- strong
//!    eventual consistency.
//!
//! # Delta buffer and delta-interval
//!
//! Each replica stamps every delta it produces with a
//! monotonically-increasing local sequence number and retains it in
//! a [`DeltaBuffer`]. When a peer reconciles, the replica ships the
//! *delta-interval*: the join of every buffered delta with a
//! sequence number at or after the peer's last acknowledged point
//! ([`DeltaBuffer::interval_since`]). On first contact (no ack point
//! known) the replica falls back to shipping full state -- a
//! delta-CRDT degrades gracefully to a state-CRDT.
//!
//! # Example
//!
//! ```
//! use dyniak::datatypes::{ActorId, DeltaOrSet};
//!
//! let a = ActorId::new("dc1", "a");
//! let mut left = DeltaOrSet::new();
//! let d = left.add(&a, b"x".to_vec());
//!
//! // The delta alone reproduces the mutation on a fresh replica.
//! let mut right = DeltaOrSet::new();
//! right.merge_delta(&d);
//! assert!(right.contains(b"x"));
//! ```

use std::collections::{BTreeMap, BTreeSet};

use crate::datatypes::set::Tag;
use crate::datatypes::{ActorId, Crdt};

/// Per-element tag state: the add-dot set and the tombstone dot set.
///
/// Identical shape to the state-based OR-Set's internal element
/// state. Present iff some add dot is not shadowed by a tombstone.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ElementState {
    adds: BTreeSet<Tag>,
    removes: BTreeSet<Tag>,
}

impl ElementState {
    fn is_present(&self) -> bool {
        self.adds.iter().any(|t| !self.removes.contains(t))
    }

    /// Join another element state into this one: union of both dot
    /// sets. This is the least-upper-bound in the per-element
    /// lattice.
    fn join(&mut self, other: &Self) {
        self.adds.extend(other.adds.iter().cloned());
        self.removes.extend(other.removes.iter().cloned());
    }

    fn is_empty(&self) -> bool {
        self.adds.is_empty() && self.removes.is_empty()
    }
}

/// A join-irreducible fragment of a [`DeltaOrSet`] state.
///
/// A delta is *not* a distinct type in a distinct lattice: it is a
/// (sparse) `DeltaOrSet` state carrying only the elements and dots a
/// single mutation touched. Shipping it and joining it is exactly the
/// same operation as shipping and joining a full state. The wrapper
/// exists only to name the intent at the API boundary and to let the
/// AAE path length-count "bytes on the wire".
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrSetDelta {
    /// Element key -> touched tag fragment. Sparse: only the
    /// elements this delta touched appear.
    fragment: BTreeMap<Vec<u8>, ElementState>,
    /// Actor-counter frontier advanced by this delta. Lets a
    /// receiving replica keep its own tag generator monotone even
    /// when it adopts dots minted elsewhere.
    actor_counters: BTreeMap<ActorId, u64>,
}

impl OrSetDelta {
    /// Whether the delta carries no mutation (an empty join). Empty
    /// deltas are the identity element of the lattice and can be
    /// dropped on the wire.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fragment.values().all(ElementState::is_empty) && self.actor_counters.is_empty()
    }

    /// Join another delta into this one, producing a single delta
    /// equivalent to applying both. This is what
    /// [`DeltaBuffer::interval_since`] uses to collapse a run of
    /// buffered deltas into one delta-interval.
    pub fn join(&mut self, other: &Self) {
        for (actor, &count) in &other.actor_counters {
            let entry = self.actor_counters.entry(actor.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
        for (element, state) in &other.fragment {
            self.fragment
                .entry(element.clone())
                .or_default()
                .join(state);
        }
    }

    /// Serialised byte length as it would appear on the AAE wire.
    ///
    /// Counts the same bytes the length-prefixed exchange framing
    /// would emit: per actor-counter entry the actor id plus the
    /// eight-byte counter, and per element the key plus twelve bytes
    /// per dot (an eight-byte counter and a length-prefixed actor
    /// id). Used by the bandwidth bench to compare delta-shipping
    /// against state-shipping without standing up a socket.
    #[must_use]
    pub fn wire_len(&self) -> usize {
        let mut n = 0;
        for actor in self.actor_counters.keys() {
            n += actor.dc.len() + actor.peer.len() + 8;
        }
        for (element, state) in &self.fragment {
            n += element.len();
            n += (state.adds.len() + state.removes.len()) * tag_wire_len();
        }
        n
    }
}

/// On-wire byte cost of a single dot: eight-byte counter plus the
/// dot's actor id. Actor ids are small and bounded; we charge a
/// fixed conservative sixteen bytes for the id so the bench compares
/// like with like against full-state shipping.
fn tag_wire_len() -> usize {
    8 + 16
}

/// Delta-state observed-remove set.
///
/// The visible value and the state-based [`Crdt::merge`] behave
/// exactly like [`OrSet`](crate::datatypes::OrSet); the addition is [`DeltaOrSet::add`] /
/// [`DeltaOrSet::remove`] returning deltas and
/// [`DeltaOrSet::merge_delta`] joining them.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeltaOrSet {
    elements: BTreeMap<Vec<u8>, ElementState>,
    actor_counters: BTreeMap<ActorId, u64>,
}

impl DeltaOrSet {
    /// Construct an empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `element` on behalf of `actor`. Returns the
    /// join-irreducible delta: the single fresh dot on this element.
    pub fn add(&mut self, actor: &ActorId, element: impl Into<Vec<u8>>) -> OrSetDelta {
        let element = element.into();
        let counter = self.actor_counters.entry(actor.clone()).or_insert(0);
        *counter = counter
            .checked_add(1)
            .expect("delta-or-set counter overflow");
        let tag = Tag {
            actor: actor.clone(),
            counter: *counter,
        };
        let entry = self.elements.entry(element.clone()).or_default();
        entry.adds.insert(tag.clone());

        let mut frag = ElementState::default();
        frag.adds.insert(tag);
        let mut delta = OrSetDelta::default();
        delta.fragment.insert(element, frag);
        delta.actor_counters.insert(actor.clone(), *counter);
        delta
    }

    /// Remove `element`. Tombstones every currently-observed add dot.
    /// Returns the delta: those dots moved into the tombstone side.
    /// An empty delta is returned when the element is absent (the
    /// mutation is a no-op and the identity of the lattice).
    pub fn remove(&mut self, element: &[u8]) -> OrSetDelta {
        let mut delta = OrSetDelta::default();
        if let Some(state) = self.elements.get_mut(element) {
            let observed: Vec<Tag> = state.adds.iter().cloned().collect();
            if observed.is_empty() {
                return delta;
            }
            let mut frag = ElementState::default();
            for tag in observed {
                state.removes.insert(tag.clone());
                frag.removes.insert(tag);
            }
            delta.fragment.insert(element.to_vec(), frag);
        }
        delta
    }

    /// Join a delta (or a delta-interval) into the set. Identical
    /// least-upper-bound operation as [`Crdt::merge`], restricted to
    /// the delta's fragment. Idempotent: re-applying a delta is a
    /// no-op.
    pub fn merge_delta(&mut self, delta: &OrSetDelta) {
        for (actor, &count) in &delta.actor_counters {
            let entry = self.actor_counters.entry(actor.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
        for (element, frag) in &delta.fragment {
            self.elements.entry(element.clone()).or_default().join(frag);
        }
    }

    /// Whether `element` is present in the value projection.
    #[must_use]
    pub fn contains(&self, element: &[u8]) -> bool {
        self.elements
            .get(element)
            .is_some_and(ElementState::is_present)
    }

    /// Full-state serialised byte length as it would appear on the
    /// AAE wire, using the same accounting as [`OrSetDelta::wire_len`].
    /// This is what state-shipping would put on the wire; the bench
    /// compares it against the delta-interval length.
    #[must_use]
    pub fn wire_len(&self) -> usize {
        let mut n = 0;
        for actor in self.actor_counters.keys() {
            n += actor.dc.len() + actor.peer.len() + 8;
        }
        for (element, state) in &self.elements {
            n += element.len();
            n += (state.adds.len() + state.removes.len()) * tag_wire_len();
        }
        n
    }
}

impl Crdt for DeltaOrSet {
    type Value = BTreeSet<Vec<u8>>;

    fn merge(&mut self, other: &Self) {
        for (actor, &count) in &other.actor_counters {
            let entry = self.actor_counters.entry(actor.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
        for (element, state) in &other.elements {
            self.elements
                .entry(element.clone())
                .or_default()
                .join(state);
        }
    }

    fn value(&self) -> BTreeSet<Vec<u8>> {
        self.elements
            .iter()
            .filter(|(_e, s)| s.is_present())
            .map(|(e, _s)| e.clone())
            .collect()
    }
}

/// A buffered delta stamped with the producing replica's local
/// sequence number.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferedDelta {
    /// Monotone local sequence number, unique and increasing per
    /// buffer. A peer acknowledges "I have seen up to sequence N".
    pub seq: u64,
    /// The delta fragment.
    pub delta: OrSetDelta,
}

/// A per-replica buffer of produced deltas plus the per-peer
/// acknowledged sequence numbers.
///
/// The buffer is what turns per-mutation deltas into a single
/// delta-interval per reconciliation: [`DeltaBuffer::interval_since`]
/// joins every delta strictly after a peer's ack point.
#[derive(Clone, Debug, Default)]
pub struct DeltaBuffer {
    next_seq: u64,
    deltas: Vec<BufferedDelta>,
    /// Peer name -> highest sequence number that peer has
    /// acknowledged receiving.
    acked: BTreeMap<String, u64>,
}

impl DeltaBuffer {
    /// Construct an empty buffer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a freshly-produced delta, returning its assigned
    /// sequence number. Empty deltas are dropped (identity element).
    pub fn record(&mut self, delta: OrSetDelta) -> Option<u64> {
        if delta.is_empty() {
            return None;
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.deltas.push(BufferedDelta { seq, delta });
        Some(seq)
    }

    /// The highest sequence number this buffer has assigned, or
    /// `None` if it has produced no deltas. A peer acknowledging this
    /// value has seen everything the buffer has produced.
    #[must_use]
    pub fn high_water(&self) -> Option<u64> {
        self.next_seq.checked_sub(1)
    }

    /// Compute the delta-interval to ship a peer: the join of every
    /// buffered delta with `seq >= since`. `since` is the first
    /// sequence the peer has *not* yet acknowledged.
    ///
    /// Returns `None` when there is nothing to ship (the peer is
    /// already current).
    #[must_use]
    pub fn interval_since(&self, since: u64) -> Option<OrSetDelta> {
        let mut acc = OrSetDelta::default();
        let mut any = false;
        for buffered in &self.deltas {
            if buffered.seq >= since {
                acc.join(&buffered.delta);
                any = true;
            }
        }
        any.then_some(acc)
    }

    /// Whether we know a peer's ack point. When we do not (first
    /// contact) the AAE path must fall back to full-state shipping.
    #[must_use]
    pub fn knows_peer(&self, peer: &str) -> bool {
        self.acked.contains_key(peer)
    }

    /// The next sequence a peer needs (one past its ack point), or
    /// `None` if the peer is unknown (first contact).
    #[must_use]
    pub fn next_needed(&self, peer: &str) -> Option<u64> {
        self.acked.get(peer).map(|acked| acked + 1)
    }

    /// Record that `peer` has acknowledged receiving up to and
    /// including sequence `seq`.
    pub fn ack(&mut self, peer: &str, seq: u64) {
        let entry = self.acked.entry(peer.to_string()).or_insert(0);
        *entry = (*entry).max(seq);
    }

    /// Drop every buffered delta that all known peers have
    /// acknowledged. Bounds buffer growth; a production rollout would
    /// call this after each reconciliation round.
    pub fn compact(&mut self) {
        let Some(min_acked) = self.acked.values().min().copied() else {
            return;
        };
        self.deltas.retain(|b| b.seq > min_acked);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(name: &str) -> ActorId {
        ActorId::new("dc1", name)
    }

    #[test]
    fn add_then_contains() {
        let a = aid("a");
        let mut s = DeltaOrSet::new();
        s.add(&a, b"x".to_vec());
        assert!(s.contains(b"x"));
    }

    #[test]
    fn delta_reproduces_add_on_fresh_replica() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let d = src.add(&a, b"x".to_vec());
        let mut dst = DeltaOrSet::new();
        dst.merge_delta(&d);
        assert!(dst.contains(b"x"));
        assert_eq!(src.value(), dst.value());
    }

    #[test]
    fn delta_reproduces_remove_on_fresh_replica() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let add = src.add(&a, b"x".to_vec());
        let rem = src.remove(b"x");
        let mut dst = DeltaOrSet::new();
        dst.merge_delta(&add);
        dst.merge_delta(&rem);
        assert!(!dst.contains(b"x"));
        assert_eq!(src.value(), dst.value());
    }

    #[test]
    fn out_of_order_delivery_still_converges() {
        // Apply remove-delta before its add-delta. The add dot and
        // its tombstone both land; the join is order-independent.
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let add = src.add(&a, b"x".to_vec());
        let rem = src.remove(b"x");
        let mut dst = DeltaOrSet::new();
        dst.merge_delta(&rem);
        dst.merge_delta(&add);
        assert_eq!(src.value(), dst.value());
        assert!(!dst.contains(b"x"));
    }

    #[test]
    fn duplicate_delta_is_idempotent() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let d = src.add(&a, b"x".to_vec());
        let mut dst = DeltaOrSet::new();
        dst.merge_delta(&d);
        let once = dst.value();
        dst.merge_delta(&d);
        dst.merge_delta(&d);
        assert_eq!(dst.value(), once);
    }

    #[test]
    fn concurrent_remove_loses_to_concurrent_add() {
        let a = aid("a");
        let b = aid("b");
        let mut shared = DeltaOrSet::new();
        let seed = shared.add(&a, b"x".to_vec());

        let mut left = DeltaOrSet::new();
        left.merge_delta(&seed);
        let left_rem = left.remove(b"x");

        let mut right = DeltaOrSet::new();
        right.merge_delta(&seed);
        let right_add = right.add(&b, b"x".to_vec());

        // Cross-ship the concurrent deltas.
        left.merge_delta(&right_add);
        right.merge_delta(&left_rem);
        assert!(left.contains(b"x"));
        assert!(right.contains(b"x"));
        assert_eq!(left.value(), right.value());
    }

    #[test]
    fn interval_joins_buffered_deltas() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let mut buf = DeltaBuffer::new();
        buf.record(src.add(&a, b"x".to_vec()));
        buf.record(src.add(&a, b"y".to_vec()));
        buf.record(src.add(&a, b"z".to_vec()));

        // Peer has seen up to seq 0; ship the interval since 1.
        let interval = buf.interval_since(1).expect("interval");
        let mut dst = DeltaOrSet::new();
        dst.merge_delta(&interval);
        // Only y and z; x was already delivered.
        assert!(!dst.contains(b"x"));
        assert!(dst.contains(b"y"));
        assert!(dst.contains(b"z"));
    }

    #[test]
    fn empty_delta_not_buffered() {
        let mut buf = DeltaBuffer::new();
        // Removing an absent element yields an empty delta.
        let mut s = DeltaOrSet::new();
        let empty = s.remove(b"absent");
        assert!(empty.is_empty());
        assert_eq!(buf.record(empty), None);
        assert_eq!(buf.high_water(), None);
    }

    #[test]
    fn compact_drops_fully_acked_deltas() {
        let a = aid("a");
        let mut src = DeltaOrSet::new();
        let mut buf = DeltaBuffer::new();
        buf.record(src.add(&a, b"x".to_vec()));
        buf.record(src.add(&a, b"y".to_vec()));
        buf.ack("peer-b", 0);
        buf.compact();
        // seq 0 dropped, seq 1 retained.
        assert!(buf.interval_since(0).is_some());
        let interval = buf.interval_since(0).unwrap();
        let mut dst = DeltaOrSet::new();
        dst.merge_delta(&interval);
        assert!(!dst.contains(b"x"));
        assert!(dst.contains(b"y"));
    }

    #[test]
    fn delta_merge_matches_full_state_merge() {
        // The core delta-CRDT theorem: joining the sequence of
        // deltas equals merging the full states.
        let a = aid("a");
        let b = aid("b");

        // Replica producing a mutation log; capture deltas.
        let mut producer = DeltaOrSet::new();
        let d1 = producer.add(&a, b"x".to_vec());
        let d2 = producer.add(&b, b"y".to_vec());
        let d3 = producer.remove(b"x");

        // Path 1: join the deltas onto a fresh replica.
        let mut via_deltas = DeltaOrSet::new();
        via_deltas.merge_delta(&d1);
        via_deltas.merge_delta(&d2);
        via_deltas.merge_delta(&d3);

        // Path 2: full-state merge of the producer's final state.
        let mut via_state = DeltaOrSet::new();
        via_state.merge(&producer);

        assert_eq!(via_deltas.value(), via_state.value());
        assert_eq!(via_deltas, via_state);
    }

    #[test]
    fn first_contact_has_no_ack_point() {
        let buf = DeltaBuffer::new();
        assert!(!buf.knows_peer("peer-b"));
        assert_eq!(buf.next_needed("peer-b"), None);
    }
}
