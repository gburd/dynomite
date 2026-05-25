//! Dotted Version Vector Set causality clock.
//!
//! The [`DvvSet`] type tracks per-key causality for the Riak
//! datastore as described by Almeida, Baquero, Goncalves,
//! Preguica and Fonte in "Dotted Version Vectors: Logical Clocks
//! for Optimistic Replication" (2010) and the follow-up
//! "Scalable and Accurate Causality Tracking for Eventually
//! Consistent Stores" (2014). The data structure has two pieces:
//!
//! * a contiguous vector clock [`DvvSet::vc`] mapping each
//!   [`ActorId`] to the highest event sequence number whose
//!   prefix is fully observed, and
//! * a sorted list of [`DvvSet::dots`] representing events whose
//!   sequence number is strictly above `vc[actor]` (a "gap" in
//!   the per-actor history).
//!
//! Together they describe the same causal history a classic
//! [`crate::datatypes::Vclock`] does, but they additionally
//! distinguish "I observed events 1..=3 and 5" (a real gap)
//! from "I observed events 1..=5" (no gap). Classic vector
//! clocks lose that distinction and can mark sequential writes
//! by the same actor as concurrent in the presence of a
//! sync-then-update interleaving where another actor has not yet
//! caught up. The unit tests in
//! [`crate::aae::repair`] and the property tests under
//! `crates/dyn-riak/tests/dvv_properties.rs` exercise the fix.
//!
//! # Wire format
//!
//! Riak treats the per-key context as opaque to clients, so this
//! module ships a length-prefixed encoding ([`DvvSet::encode`] /
//! [`DvvSet::decode`]) that round-trips a [`DvvSet`] through the
//! `bytes` field on the protobuf messages without exposing its
//! shape to clients. The format is stable across this crate's
//! versions and is not part of Riak's wire format.
//!
//! # Examples
//!
//! ```
//! use dyn_riak::datatypes::{ActorId, DvvOrder, DvvSet};
//! let actor = ActorId::new("dc1", "peer-a");
//! let mut clock = DvvSet::new();
//! clock.update(&actor);
//! clock.update(&actor);
//! let mut later = clock.clone();
//! later.update(&actor);
//! assert_eq!(clock.compare(&later), DvvOrder::Less);
//! ```

use std::collections::{BTreeMap, BTreeSet};

use crate::datatypes::ActorId;

/// Result of comparing two [`DvvSet`] clocks under the
/// happens-before relation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DvvOrder {
    /// `self` strictly precedes `other`: every event recorded
    /// in `self` is also in `other`, and `other` carries at
    /// least one event `self` does not.
    Less,
    /// Both clocks describe the same set of events.
    Equal,
    /// `self` strictly succeeds `other`.
    Greater,
    /// Neither clock dominates: each side has at least one
    /// event the other does not.
    Concurrent,
}

/// Dotted version vector set.
///
/// See the [module docs](crate::datatypes::dvv) for the
/// algorithm summary and the citations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DvvSet {
    vc: BTreeMap<ActorId, u64>,
    dots: Vec<(ActorId, u64)>,
}

impl DvvSet {
    /// Construct an empty clock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct actors the clock has observed (in the
    /// vc OR in the dot list).
    #[must_use]
    pub fn len(&self) -> usize {
        let mut keys: BTreeSet<&ActorId> = self.vc.keys().collect();
        for (a, _) in &self.dots {
            keys.insert(a);
        }
        keys.len()
    }

    /// Whether the clock has observed no events at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.vc.is_empty() && self.dots.is_empty()
    }

    /// Iterate over `(actor, highest-contiguous-count)` pairs in
    /// actor-id order. Dots are not visited; use
    /// [`Self::dots`] for the gap list.
    pub fn vc_iter(&self) -> impl Iterator<Item = (&ActorId, u64)> {
        self.vc.iter().map(|(a, &n)| (a, n))
    }

    /// Borrow the sorted dot list. A `(a, n)` entry means actor
    /// `a` performed event `n` and the run `vc[a]+1 .. n-1`
    /// has not yet been observed.
    #[must_use]
    pub fn dots(&self) -> &[(ActorId, u64)] {
        &self.dots
    }

    /// Greatest sequence number `actor` has ever appeared with
    /// in this clock (zero if the actor is unknown).
    #[must_use]
    pub fn max_seq(&self, actor: &ActorId) -> u64 {
        let vc = self.vc.get(actor).copied().unwrap_or(0);
        let dot = self
            .dots
            .iter()
            .filter(|(a, _)| a == actor)
            .map(|(_, n)| *n)
            .max()
            .unwrap_or(0);
        std::cmp::max(vc, dot)
    }

    /// Whether the event `(actor, n)` is recorded in this clock.
    #[must_use]
    pub fn contains_event(&self, actor: &ActorId, n: u64) -> bool {
        if n == 0 {
            return false;
        }
        if n <= self.vc.get(actor).copied().unwrap_or(0) {
            return true;
        }
        self.dots
            .binary_search_by(|probe| probe.0.cmp(actor).then(probe.1.cmp(&n)))
            .is_ok()
    }

    /// Record one new local event for `actor`.
    ///
    /// The event is assigned sequence number
    /// `max(vc[actor], max_dot[actor]) + 1`. When the new number
    /// is contiguous with `vc[actor]` it is folded directly into
    /// the vector clock; otherwise it is recorded as a dot. The
    /// canonicalisation pass then absorbs any dot that became
    /// contiguous as a side effect.
    pub fn update(&mut self, actor: &ActorId) {
        let cur_vc = self.vc.get(actor).copied().unwrap_or(0);
        let cur_dot = self
            .dots
            .iter()
            .filter(|(a, _)| a == actor)
            .map(|(_, n)| *n)
            .max()
            .unwrap_or(0);
        let next = std::cmp::max(cur_vc, cur_dot)
            .checked_add(1)
            .expect("dvv overflow");
        if next == cur_vc + 1 {
            self.vc.insert(actor.clone(), next);
        } else {
            self.dots.push((actor.clone(), next));
        }
        self.canonicalize();
    }

    /// Pure (non-mutating) version of [`Self::sync`].
    ///
    /// Returns a new clock whose recorded events are exactly the
    /// union of the events recorded by `self` and `other`.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        let mut out = self.clone();
        out.sync(other);
        out
    }

    /// Combine `other` into `self` in place.
    ///
    /// The vc takes the pointwise maximum of the two inputs.
    /// Dots from either side are kept iff they are not already
    /// covered by the post-merge vc. The canonicalisation pass
    /// then folds contiguous dots back into the vc.
    pub fn sync(&mut self, other: &Self) {
        // Pointwise max on vc.
        for (actor, &n) in &other.vc {
            let entry = self.vc.entry(actor.clone()).or_insert(0);
            if *entry < n {
                *entry = n;
            }
        }
        // Union dots from both sides, drop any covered by vc.
        let mut combined: BTreeSet<(ActorId, u64)> = BTreeSet::new();
        for (a, n) in self.dots.drain(..) {
            if n > self.vc.get(&a).copied().unwrap_or(0) {
                combined.insert((a, n));
            }
        }
        for (a, n) in &other.dots {
            if *n > self.vc.get(a).copied().unwrap_or(0) {
                combined.insert((a.clone(), *n));
            }
        }
        self.dots = combined.into_iter().collect();
        self.canonicalize();
    }

    /// Compare two clocks under the happens-before relation.
    /// See [`DvvOrder`] for the semantics.
    #[must_use]
    pub fn compare(&self, other: &Self) -> DvvOrder {
        let self_in_other = self.subset_of(other);
        let other_in_self = other.subset_of(self);
        match (self_in_other, other_in_self) {
            (true, true) => DvvOrder::Equal,
            (true, false) => DvvOrder::Less,
            (false, true) => DvvOrder::Greater,
            (false, false) => DvvOrder::Concurrent,
        }
    }

    /// Encode the clock into the opaque byte string used by
    /// `RpbDtFetchResp.context`.
    ///
    /// The format is two length-prefixed sections (vc then
    /// dots). Each section starts with a `u32` big-endian count;
    /// each entry is `(dc-len: u32, dc bytes, peer-len: u32,
    /// peer bytes, value: u64)`. Both length prefixes and the
    /// value are big-endian. The format is stable across this
    /// crate's versions and is not part of Riak's wire format.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyn_riak::datatypes::{ActorId, DvvSet};
    /// let mut c = DvvSet::new();
    /// c.update(&ActorId::new("dc1", "alpha"));
    /// let bytes = c.encode();
    /// let back = DvvSet::decode(&bytes).expect("round-trip");
    /// assert_eq!(c, back);
    /// ```
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.vc.len() * 32 + self.dots.len() * 32);
        out.extend_from_slice(
            &u32::try_from(self.vc.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        for (actor, count) in &self.vc {
            encode_actor_pair(&mut out, actor, *count);
        }
        out.extend_from_slice(
            &u32::try_from(self.dots.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        for (actor, n) in &self.dots {
            encode_actor_pair(&mut out, actor, *n);
        }
        out
    }

    /// Decode a clock previously produced by [`Self::encode`].
    /// Returns `None` if the byte sequence is malformed (length
    /// mismatch, non-UTF-8 actor strings, dot list out of order
    /// or duplicate, or a dot that is already covered by the vc
    /// half of the same blob).
    #[must_use]
    pub fn decode(mut bytes: &[u8]) -> Option<Self> {
        let vc_count = read_u32(&mut bytes)? as usize;
        let mut vc = BTreeMap::new();
        for _ in 0..vc_count {
            let actor = read_actor(&mut bytes)?;
            let n = read_u64(&mut bytes)?;
            if vc.insert(actor, n).is_some() {
                return None;
            }
        }
        let dot_count = read_u32(&mut bytes)? as usize;
        let mut dots = Vec::with_capacity(dot_count);
        for _ in 0..dot_count {
            let actor = read_actor(&mut bytes)?;
            let n = read_u64(&mut bytes)?;
            dots.push((actor, n));
        }
        if !bytes.is_empty() {
            return None;
        }
        // Reject malformed dot lists: must be sorted ascending
        // with no duplicates and no entry covered by vc.
        for w in dots.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            if a >= b {
                return None;
            }
        }
        for (a, n) in &dots {
            if *n <= vc.get(a).copied().unwrap_or(0) {
                return None;
            }
        }
        Some(Self { vc, dots })
    }

    /// Whether every event recorded in `self` is also recorded
    /// in `other`.
    fn subset_of(&self, other: &Self) -> bool {
        // 1. Every contiguous prefix in self.vc must be present
        // in other.
        for (actor, &v) in &self.vc {
            let ov = other.vc.get(actor).copied().unwrap_or(0);
            if v > ov {
                // Each k in (ov, v] must be a dot in other.
                for k in (ov + 1)..=v {
                    if !other.contains_event(actor, k) {
                        return false;
                    }
                }
            }
        }
        // 2. Every dot in self must be present in other.
        for (actor, n) in &self.dots {
            if !other.contains_event(actor, *n) {
                return false;
            }
        }
        true
    }

    /// Drop dots covered by the vc and absorb the contiguous
    /// prefix of remaining dots into the vc.
    fn canonicalize(&mut self) {
        // Drop dots already covered by vc.
        self.dots
            .retain(|(a, n)| *n > self.vc.get(a).copied().unwrap_or(0));
        if self.dots.is_empty() {
            return;
        }
        // Group by actor for contiguous-run absorption.
        let mut by_actor: BTreeMap<ActorId, BTreeSet<u64>> = BTreeMap::new();
        for (a, n) in self.dots.drain(..) {
            by_actor.entry(a).or_default().insert(n);
        }
        let mut leftover: Vec<(ActorId, u64)> = Vec::new();
        for (actor, mut ns) in by_actor {
            let entry = self.vc.entry(actor.clone()).or_insert(0);
            loop {
                let next = entry.checked_add(1).expect("dvv overflow");
                if ns.remove(&next) {
                    *entry = next;
                } else {
                    break;
                }
            }
            for n in ns {
                leftover.push((actor.clone(), n));
            }
        }
        leftover.sort();
        self.dots = leftover;
    }
}

fn encode_actor_pair(out: &mut Vec<u8>, actor: &ActorId, n: u64) {
    let dc = actor.dc.as_bytes();
    let peer = actor.peer.as_bytes();
    out.extend_from_slice(&u32::try_from(dc.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(dc);
    out.extend_from_slice(&u32::try_from(peer.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(peer);
    out.extend_from_slice(&n.to_be_bytes());
}

fn read_u32(bytes: &mut &[u8]) -> Option<u32> {
    if bytes.len() < 4 {
        return None;
    }
    let v = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    *bytes = &bytes[4..];
    Some(v)
}

fn read_u64(bytes: &mut &[u8]) -> Option<u64> {
    if bytes.len() < 8 {
        return None;
    }
    let v = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    *bytes = &bytes[8..];
    Some(v)
}

fn read_str(bytes: &mut &[u8]) -> Option<String> {
    let len = read_u32(bytes)? as usize;
    if bytes.len() < len {
        return None;
    }
    let s = std::str::from_utf8(&bytes[..len]).ok()?.to_owned();
    *bytes = &bytes[len..];
    Some(s)
}

fn read_actor(bytes: &mut &[u8]) -> Option<ActorId> {
    let dc = read_str(bytes)?;
    let peer = read_str(bytes)?;
    Some(ActorId { dc, peer })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(peer: &str) -> ActorId {
        ActorId::new("dc1", peer)
    }

    #[test]
    fn empty_clocks_are_equal() {
        let a = DvvSet::new();
        let b = DvvSet::new();
        assert_eq!(a.compare(&b), DvvOrder::Equal);
        assert!(a.is_empty());
        assert_eq!(a.len(), 0);
    }

    #[test]
    fn single_actor_update_grows_vc_only() {
        let mut c = DvvSet::new();
        let a = aid("p1");
        c.update(&a);
        c.update(&a);
        c.update(&a);
        assert_eq!(c.max_seq(&a), 3);
        assert_eq!(c.vc.get(&a).copied().unwrap_or(0), 3);
        assert!(c.dots.is_empty());
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn sequential_updates_dominate() {
        let mut prev = DvvSet::new();
        let a = aid("p1");
        let mut clocks: Vec<DvvSet> = Vec::new();
        for _ in 0..5 {
            prev.update(&a);
            clocks.push(prev.clone());
        }
        for w in clocks.windows(2) {
            assert_eq!(w[0].compare(&w[1]), DvvOrder::Less);
            assert_eq!(w[1].compare(&w[0]), DvvOrder::Greater);
        }
    }

    #[test]
    fn cross_actor_writes_are_concurrent() {
        let mut prev = DvvSet::new();
        prev.update(&aid("p1"));
        let mut a = prev.clone();
        a.update(&aid("p1"));
        let mut b = prev.clone();
        b.update(&aid("p2"));
        assert_eq!(a.compare(&b), DvvOrder::Concurrent);
        assert_eq!(b.compare(&a), DvvOrder::Concurrent);
    }

    #[test]
    fn merge_takes_pointwise_max() {
        let mut a = DvvSet::new();
        let mut b = DvvSet::new();
        let p1 = aid("p1");
        let p2 = aid("p2");
        a.update(&p1);
        a.update(&p1);
        b.update(&p1);
        b.update(&p2);
        let m = a.merge(&b);
        assert_eq!(m.max_seq(&p1), 2);
        assert_eq!(m.max_seq(&p2), 1);
        // a < m: m has p2:1 a doesn't, but b is missing p1:2 so
        // it can't be Less of m by itself.
        let order_a = a.compare(&m);
        assert!(matches!(order_a, DvvOrder::Less));
        let order_b = b.compare(&m);
        assert!(matches!(order_b, DvvOrder::Less));
    }

    #[test]
    fn sync_is_idempotent() {
        let mut a = DvvSet::new();
        a.update(&aid("p1"));
        a.update(&aid("p2"));
        let original = a.clone();
        a.sync(&original);
        assert_eq!(a, original);
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = DvvSet::new();
        let mut b = DvvSet::new();
        a.update(&aid("p1"));
        a.update(&aid("p2"));
        b.update(&aid("p3"));
        b.update(&aid("p1"));
        let m1 = a.merge(&b);
        let m2 = b.merge(&a);
        assert_eq!(m1, m2);
    }

    #[test]
    fn dot_for_actor_with_gap_lands_in_dots() {
        // Construct a state where node X observed event A:5
        // (e.g. via sync from a peer) without observing the
        // intervening A events, then performs its own update.
        // The locally-known max is 5 so update bumps to 6,
        // which must land in dots because vc[A]=0 and 6 != 1.
        let a = aid("p1");
        let mut c = DvvSet::new();
        // Manually inject the dot to simulate a prior sync.
        c.dots.push((a.clone(), 5));
        c.canonicalize();
        assert_eq!(c.dots, vec![(a.clone(), 5)]);
        assert_eq!(c.vc.get(&a).copied().unwrap_or(0), 0);

        c.update(&a);
        assert_eq!(c.max_seq(&a), 6);
        // dots still hold (5, 6) since vc still 0.
        assert_eq!(c.vc.get(&a).copied().unwrap_or(0), 0);
        assert_eq!(c.dots.len(), 2);
    }

    #[test]
    fn dot_absorbed_when_contiguous_run_completes() {
        // Inject (A, 2) as a dot, then add events 1 and 3.
        // After all updates, vc[A]=3 and dots is empty.
        let a = aid("p1");
        let mut c = DvvSet::new();
        c.dots.push((a.clone(), 2));
        c.canonicalize();
        assert_eq!(c.vc.get(&a).copied().unwrap_or(0), 0);
        assert_eq!(c.dots, vec![(a.clone(), 2)]);

        // Now event 1 by A: max=2, next=3, not contiguous with
        // vc[A]=0+1, lands in dots => dots=[(A,2),(A,3)].
        // Bug: that's wrong. update increments by 1 from max
        // (2) so next is 3, which is > vc[A]+1, dot.
        c.update(&a);
        assert_eq!(c.max_seq(&a), 3);

        // Inject the missing event 1 directly via a sync from a
        // peer that observed A:1 contiguously.
        let mut peer = DvvSet::new();
        peer.update(&a); // vc[A]=1
        c.sync(&peer);
        // After sync: vc[A]=1, dots=[(A,2),(A,3)] -> absorb 2
        // -> vc[A]=2, dots=[(A,3)] -> absorb 3 -> vc[A]=3.
        assert_eq!(c.vc.get(&a).copied().unwrap_or(0), 3);
        assert!(c.dots.is_empty());
    }

    #[test]
    fn compare_equal_with_dots() {
        let a = aid("p1");
        let mut c1 = DvvSet::new();
        c1.dots.push((a.clone(), 3));
        c1.canonicalize();
        let c2 = c1.clone();
        assert_eq!(c1.compare(&c2), DvvOrder::Equal);
    }

    #[test]
    fn compare_less_when_other_has_extra_dot() {
        let a = aid("p1");
        let mut c1 = DvvSet::new();
        c1.update(&a);
        let mut c2 = c1.clone();
        c2.dots.push((aid("p2"), 2));
        c2.canonicalize();
        assert_eq!(c1.compare(&c2), DvvOrder::Less);
        assert_eq!(c2.compare(&c1), DvvOrder::Greater);
    }

    #[test]
    fn compare_concurrent_when_each_has_unique_event() {
        let a = aid("p1");
        let mut c1 = DvvSet::new();
        c1.update(&a);
        let mut c2 = DvvSet::new();
        c2.update(&aid("p2"));
        assert_eq!(c1.compare(&c2), DvvOrder::Concurrent);
        assert_eq!(c2.compare(&c1), DvvOrder::Concurrent);
    }

    #[test]
    fn encode_decode_round_trip_empty() {
        let c = DvvSet::new();
        let bytes = c.encode();
        let back = DvvSet::decode(&bytes).expect("decode");
        assert_eq!(back, c);
    }

    #[test]
    fn encode_decode_round_trip_with_vc_and_dots() {
        let mut c = DvvSet::new();
        c.update(&aid("alpha"));
        c.update(&aid("beta"));
        c.update(&aid("alpha"));
        c.dots.push((aid("gamma"), 7));
        c.canonicalize();
        let bytes = c.encode();
        let back = DvvSet::decode(&bytes).expect("decode");
        assert_eq!(back, c);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let mut c = DvvSet::new();
        c.update(&aid("p1"));
        let bytes = c.encode();
        assert!(DvvSet::decode(&bytes[..bytes.len() - 1]).is_none());
        assert!(DvvSet::decode(&[0u8; 3]).is_none());
    }

    #[test]
    fn decode_rejects_unsorted_dots() {
        // Hand-construct a blob with two dots out of order.
        let mut bad = Vec::new();
        bad.extend_from_slice(&0u32.to_be_bytes()); // vc count = 0
        bad.extend_from_slice(&2u32.to_be_bytes()); // dot count = 2
                                                    // dot 1: (dc1, p2, 5)
        encode_actor_pair(&mut bad, &aid("p2"), 5);
        // dot 2: (dc1, p1, 5) <- out of order
        encode_actor_pair(&mut bad, &aid("p1"), 5);
        assert!(DvvSet::decode(&bad).is_none());
    }

    #[test]
    fn decode_rejects_dot_covered_by_vc() {
        // vc has p1:5; dots has (p1, 3), which is covered.
        let mut bad = Vec::new();
        bad.extend_from_slice(&1u32.to_be_bytes());
        encode_actor_pair(&mut bad, &aid("p1"), 5);
        bad.extend_from_slice(&1u32.to_be_bytes());
        encode_actor_pair(&mut bad, &aid("p1"), 3);
        assert!(DvvSet::decode(&bad).is_none());
    }

    #[test]
    fn merge_associative_basic() {
        let mut a = DvvSet::new();
        let mut b = DvvSet::new();
        let mut c = DvvSet::new();
        a.update(&aid("p1"));
        b.update(&aid("p2"));
        c.update(&aid("p3"));
        let left = a.merge(&b).merge(&c);
        let right = a.merge(&b.merge(&c));
        assert_eq!(left, right);
    }

    #[test]
    fn contains_event_handles_zero_seq() {
        let mut c = DvvSet::new();
        c.update(&aid("p1"));
        assert!(!c.contains_event(&aid("p1"), 0));
        assert!(c.contains_event(&aid("p1"), 1));
        assert!(!c.contains_event(&aid("p1"), 2));
    }

    #[test]
    fn dots_accessor_mirrors_internal_state() {
        let a = aid("p1");
        let mut c = DvvSet::new();
        c.dots.push((a.clone(), 3));
        c.canonicalize();
        assert_eq!(c.dots(), &[(a, 3)]);
    }

    #[test]
    fn vc_iter_visits_actors_in_order() {
        let mut c = DvvSet::new();
        c.update(&aid("z"));
        c.update(&aid("a"));
        c.update(&aid("m"));
        let names: Vec<&str> = c.vc_iter().map(|(a, _)| a.peer.as_str()).collect();
        assert_eq!(names, vec!["a", "m", "z"]);
    }

    #[test]
    fn sync_drops_dots_subsumed_by_other_vc() {
        // self has dot (p1, 3); other has vc[p1]=5.
        // After sync, the dot is absorbed (covered by vc).
        let p1 = aid("p1");
        let mut a = DvvSet::new();
        a.dots.push((p1.clone(), 3));
        a.canonicalize();
        let mut b = DvvSet::new();
        for _ in 0..5 {
            b.update(&p1);
        }
        a.sync(&b);
        assert_eq!(a.vc.get(&p1).copied().unwrap_or(0), 5);
        assert!(a.dots.is_empty());
    }
}
