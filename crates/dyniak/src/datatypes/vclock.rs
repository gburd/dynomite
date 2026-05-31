//! Classic vector clock (deprecated; superseded by
//! [`crate::datatypes::DvvSet`]).
//!
//! Retained for archaeology and direct in-test comparisons.
//! New callers should use [`crate::datatypes::DvvSet`], which
//! tracks per-actor non-contiguous events through dots and
//! avoids the false-concurrency artefact classic VVs can
//! exhibit when one actor performs sync-then-update against a
//! peer that has not yet caught up. See
//! `docs/journal/2026-05-25-dvv-default.md` for the migration
//! notes.
//!
//! A vector clock tracks how many events each [`crate::datatypes::ActorId`]
//! has emitted. Two clocks compare under the standard
//! happens-before relation:
//!
//! * `Less`: every entry of `a` is less than or equal to the same
//!   entry in `b`, and at least one entry is strictly less.
//! * `Equal`: identical entry sets and counts.
//! * `Greater`: the reverse of `Less`.
//! * `Concurrent`: neither dominates the other -- some entries
//!   favour `a` and others favour `b`.
//!
//! The wire encoding used by `RpbDtFetchResp.context` is opaque to
//! Riak clients, so this module also exposes
//! [`Vclock::encode`] / [`Vclock::decode`] for round-tripping a
//! clock through the `bytes` field on the protobuf messages.

// Internal unit tests in this module reference `Vclock` and
// `VclockOrder` directly. Allowing the deprecation lint at
// module scope keeps the kept-for-archaeology behaviour live
// without forcing every test to repeat the attribute. See
// `docs/journal/allowances.md` (2026-05-25 entry).
#![allow(deprecated)]

use std::collections::BTreeMap;

use crate::datatypes::ActorId;

/// Result of comparing two vector clocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[deprecated(
    since = "0.0.2",
    note = "use `dyniak::datatypes::DvvOrder` (DVVSet) instead; see docs/journal/2026-05-25-dvv-default.md"
)]
pub enum VclockOrder {
    /// `self` strictly precedes `other`.
    Less,
    /// Both clocks have identical entry sets and counts.
    Equal,
    /// `self` strictly succeeds `other`.
    Greater,
    /// Neither clock dominates: there exist actors where each
    /// side leads.
    Concurrent,
}

/// Vector clock keyed by [`ActorId`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[deprecated(
    since = "0.0.2",
    note = "use `dyniak::datatypes::DvvSet` instead; see docs/journal/2026-05-25-dvv-default.md"
)]
pub struct Vclock {
    entries: BTreeMap<ActorId, u64>,
}

impl Vclock {
    /// Construct an empty clock.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment `actor`'s entry by one and return the new count.
    pub fn increment(&mut self, actor: &ActorId) -> u64 {
        let entry = self.entries.entry(actor.clone()).or_insert(0);
        *entry = entry.checked_add(1).expect("vclock overflow");
        *entry
    }

    /// Return the current count for `actor` (zero if absent).
    #[must_use]
    pub fn get(&self, actor: &ActorId) -> u64 {
        self.entries.get(actor).copied().unwrap_or(0)
    }

    /// Return the number of actors recorded in this clock.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the clock has no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over `(actor, count)` pairs in actor-id order.
    pub fn iter(&self) -> impl Iterator<Item = (&ActorId, u64)> {
        self.entries.iter().map(|(a, c)| (a, *c))
    }

    /// Take the entry-wise maximum of `self` and `other` and
    /// return the result. Pure; does not mutate either input.
    #[must_use]
    pub fn merge(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for (actor, &count) in &other.entries {
            let entry = out.entries.entry(actor.clone()).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
        out
    }

    /// Compare two clocks under the happens-before relation.
    #[must_use]
    pub fn compare(&self, other: &Self) -> VclockOrder {
        let mut self_strictly_less = false;
        let mut other_strictly_less = false;
        let mut keys: std::collections::BTreeSet<&ActorId> = std::collections::BTreeSet::new();
        for k in self.entries.keys() {
            keys.insert(k);
        }
        for k in other.entries.keys() {
            keys.insert(k);
        }
        for actor in &keys {
            let a = self.get(actor);
            let b = other.get(actor);
            if a < b {
                self_strictly_less = true;
            } else if a > b {
                other_strictly_less = true;
            }
            if self_strictly_less && other_strictly_less {
                return VclockOrder::Concurrent;
            }
        }
        match (self_strictly_less, other_strictly_less) {
            (true, false) => VclockOrder::Less,
            (false, true) => VclockOrder::Greater,
            (false, false) => VclockOrder::Equal,
            (true, true) => VclockOrder::Concurrent,
        }
    }

    /// Encode the clock into the opaque byte string used by
    /// `RpbDtFetchResp.context`.
    ///
    /// The format is a length-prefixed sequence of records
    /// `(dc-len, dc, peer-len, peer, count)` where each length
    /// prefix is a 32-bit big-endian unsigned integer and `count`
    /// is a 64-bit big-endian unsigned integer. The format is
    /// stable across this crate's versions and is not part of
    /// Riak's wire format -- Riak treats the context as opaque.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.entries.len() * 32);
        out.extend_from_slice(
            &u32::try_from(self.entries.len())
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        for (actor, count) in &self.entries {
            let dc = actor.dc.as_bytes();
            let peer = actor.peer.as_bytes();
            out.extend_from_slice(&u32::try_from(dc.len()).unwrap_or(u32::MAX).to_be_bytes());
            out.extend_from_slice(dc);
            out.extend_from_slice(&u32::try_from(peer.len()).unwrap_or(u32::MAX).to_be_bytes());
            out.extend_from_slice(peer);
            out.extend_from_slice(&count.to_be_bytes());
        }
        out
    }

    /// Decode a clock previously produced by [`Self::encode`].
    /// Returns `None` if the byte sequence is malformed.
    #[must_use]
    pub fn decode(mut bytes: &[u8]) -> Option<Self> {
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
        let count = read_u32(&mut bytes)? as usize;
        let mut entries = BTreeMap::new();
        for _ in 0..count {
            let dc = read_str(&mut bytes)?;
            let peer = read_str(&mut bytes)?;
            let n = read_u64(&mut bytes)?;
            entries.insert(ActorId { dc, peer }, n);
        }
        if !bytes.is_empty() {
            return None;
        }
        Some(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(dc: &str, peer: &str) -> ActorId {
        ActorId::new(dc, peer)
    }

    #[test]
    fn empty_clocks_are_equal() {
        let a = Vclock::new();
        let b = Vclock::new();
        assert_eq!(a.compare(&b), VclockOrder::Equal);
    }

    #[test]
    fn increment_increases_entry() {
        let mut c = Vclock::new();
        let a = aid("dc1", "p1");
        assert_eq!(c.increment(&a), 1);
        assert_eq!(c.increment(&a), 2);
        assert_eq!(c.get(&a), 2);
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn order_less_when_strictly_dominated() {
        let mut a = Vclock::new();
        let mut b = Vclock::new();
        let p = aid("dc1", "p");
        a.increment(&p);
        b.increment(&p);
        b.increment(&p);
        assert_eq!(a.compare(&b), VclockOrder::Less);
        assert_eq!(b.compare(&a), VclockOrder::Greater);
    }

    #[test]
    fn order_concurrent_when_neither_dominates() {
        let mut a = Vclock::new();
        let mut b = Vclock::new();
        a.increment(&aid("dc1", "p1"));
        b.increment(&aid("dc1", "p2"));
        assert_eq!(a.compare(&b), VclockOrder::Concurrent);
        assert_eq!(b.compare(&a), VclockOrder::Concurrent);
    }

    #[test]
    fn merge_takes_pointwise_max() {
        let mut a = Vclock::new();
        let mut b = Vclock::new();
        let p1 = aid("dc1", "p1");
        let p2 = aid("dc1", "p2");
        a.increment(&p1);
        a.increment(&p1);
        b.increment(&p1);
        b.increment(&p2);
        let m = a.merge(&b);
        assert_eq!(m.get(&p1), 2);
        assert_eq!(m.get(&p2), 1);
    }

    #[test]
    fn encode_decode_round_trip() {
        let mut c = Vclock::new();
        c.increment(&aid("dc1", "alpha"));
        c.increment(&aid("dc1", "alpha"));
        c.increment(&aid("dc2", "beta-the-second"));
        let bytes = c.encode();
        let back = Vclock::decode(&bytes).expect("decode");
        assert_eq!(back, c);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let mut c = Vclock::new();
        c.increment(&aid("dc1", "p1"));
        let bytes = c.encode();
        assert!(Vclock::decode(&bytes[..bytes.len() - 1]).is_none());
        assert!(Vclock::decode(&[0u8; 3]).is_none());
    }
}
