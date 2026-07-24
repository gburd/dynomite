//! Per-object causal context as a dotted version vector.
//!
//! An opaque Riak object carries a causal context so a write either
//! descends from what the client read (and supersedes it) or is
//! concurrent (a conflict / sibling). Riak models this with a dotted
//! version vector keyed by the coordinating actor; this module is that
//! model. Two writes coordinated by different actors from the same read
//! context are concurrent; two writes coordinated by the same actor are
//! ordered (the coordinator serialises them).
//!
//! The wire form is a length-prefixed list of `(actor, counter)`
//! entries sorted by actor, so encoding is canonical and comparison is
//! order-independent.

use std::cmp::Ordering;
use std::collections::BTreeMap;

/// A version vector: actor identifier -> monotonically increasing
/// counter. An empty vector is the bottom element, dominated by every
/// non-empty vector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VClock {
    entries: BTreeMap<Vec<u8>, u64>,
}

impl VClock {
    /// The empty (bottom) version vector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment `actor`'s counter, recording a write coordinated by it.
    pub fn advance(&mut self, actor: &[u8]) {
        let c = self.entries.entry(actor.to_vec()).or_insert(0);
        *c = c.saturating_add(1);
    }

    /// Merge `other` into `self` (element-wise maximum). The result
    /// dominates both inputs.
    pub fn merge(&mut self, other: &VClock) {
        for (actor, &n) in &other.entries {
            let e = self.entries.entry(actor.clone()).or_insert(0);
            if *e < n {
                *e = n;
            }
        }
    }

    /// Causal comparison. `Some(Less/Greater/Equal)` when the two are
    /// ordered; `None` when concurrent (neither dominates).
    #[must_use]
    pub fn partial_cmp(&self, other: &VClock) -> Option<Ordering> {
        let mut self_gt = false;
        let mut other_gt = false;
        // Every actor in either vector; a missing actor counts as 0.
        for actor in self.entries.keys().chain(other.entries.keys()) {
            let a = self.entries.get(actor).copied().unwrap_or(0);
            let b = other.entries.get(actor).copied().unwrap_or(0);
            match a.cmp(&b) {
                Ordering::Greater => self_gt = true,
                Ordering::Less => other_gt = true,
                Ordering::Equal => {}
            }
        }
        match (self_gt, other_gt) {
            (false, false) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (true, true) => None,
        }
    }

    /// Encode to the canonical wire form: a `u32` entry count, then each
    /// `(u32 actor-len, actor bytes, u64 counter)` in actor order.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        if self.entries.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(4 + self.entries.len() * 16);
        let count = u32::try_from(self.entries.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_be_bytes());
        for (actor, &counter) in &self.entries {
            let alen = u32::try_from(actor.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&alen.to_be_bytes());
            out.extend_from_slice(actor);
            out.extend_from_slice(&counter.to_be_bytes());
        }
        out
    }

    /// Decode from the wire form. An empty input is the empty vector; a
    /// truncated or malformed input yields the empty vector (a bad
    /// context is treated as no context rather than an error, so a
    /// corrupt blob never aborts a read or write).
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            return Self::new();
        }
        let mut vc = Self::new();
        let mut pos = 0usize;
        let read_u32 = |b: &[u8], pos: &mut usize| -> Option<u32> {
            let end = pos.checked_add(4)?;
            if end > b.len() {
                return None;
            }
            let v = u32::from_be_bytes(b[*pos..end].try_into().ok()?);
            *pos = end;
            Some(v)
        };
        let read_u64 = |b: &[u8], pos: &mut usize| -> Option<u64> {
            let end = pos.checked_add(8)?;
            if end > b.len() {
                return None;
            }
            let v = u64::from_be_bytes(b[*pos..end].try_into().ok()?);
            *pos = end;
            Some(v)
        };
        let Some(count) = read_u32(bytes, &mut pos) else {
            return Self::new();
        };
        for _ in 0..count {
            let Some(alen) = read_u32(bytes, &mut pos) else {
                return vc;
            };
            let alen = alen as usize;
            let Some(end) = pos.checked_add(alen) else {
                return vc;
            };
            if end > bytes.len() {
                return vc;
            }
            let actor = bytes[pos..end].to_vec();
            pos = end;
            let Some(counter) = read_u64(bytes, &mut pos) else {
                return vc;
            };
            vc.entries.insert(actor, counter);
        }
        vc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_dominated_by_any_write() {
        let empty = VClock::new();
        let mut a = VClock::new();
        a.advance(b"n1");
        assert_eq!(empty.partial_cmp(&a), Some(Ordering::Less));
        assert_eq!(a.partial_cmp(&empty), Some(Ordering::Greater));
    }

    #[test]
    fn same_actor_writes_are_ordered() {
        let mut a = VClock::new();
        a.advance(b"n1");
        let mut b = a.clone();
        b.advance(b"n1");
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
    }

    #[test]
    fn different_actors_from_same_base_are_concurrent() {
        let base = VClock::new();
        let mut a = base.clone();
        a.advance(b"n1");
        let mut b = base;
        b.advance(b"n2");
        assert_eq!(a.partial_cmp(&b), None, "distinct-actor writes concurrent");
    }

    #[test]
    fn merge_dominates_both() {
        let mut a = VClock::new();
        a.advance(b"n1");
        let mut b = VClock::new();
        b.advance(b"n2");
        let mut m = a.clone();
        m.merge(&b);
        assert_eq!(m.partial_cmp(&a), Some(Ordering::Greater));
        assert_eq!(m.partial_cmp(&b), Some(Ordering::Greater));
    }

    #[test]
    fn encode_decode_round_trips() {
        let mut a = VClock::new();
        a.advance(b"north");
        a.advance(b"north");
        a.advance(b"south");
        let bytes = a.encode();
        assert_eq!(VClock::decode(&bytes), a);
        // Empty round-trips to empty.
        assert_eq!(VClock::decode(&VClock::new().encode()), VClock::new());
    }

    #[test]
    fn corrupt_blob_decodes_to_empty_or_partial_without_panic() {
        // Truncated garbage must not panic; it decodes to whatever
        // prefix was well-formed (here: empty).
        let _ = VClock::decode(&[0, 0, 0, 5, 9, 9]);
        let _ = VClock::decode(&[0xff, 0xff, 0xff, 0xff]);
    }
}
