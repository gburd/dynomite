//! Riak PN-Counter (positive-negative).
//!
//! State is two `BTreeMap<ActorId, u64>` columns: `pos` accumulates
//! every positive contribution per actor, `neg` accumulates every
//! negative contribution per actor. The user-visible value is
//! `sum(pos) - sum(neg)`. Merge is element-wise max on each column,
//! which is the canonical CRDT join for grow-only counters and
//! extends to PN by treating the PN-counter as a pair of G-Counters.

use std::collections::BTreeMap;

use crate::datatypes::{ActorId, Crdt};

/// Positive-negative counter CRDT.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PnCounter {
    pos: BTreeMap<ActorId, u64>,
    neg: BTreeMap<ActorId, u64>,
}

impl PnCounter {
    /// Construct an empty counter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply `delta` from `actor`.
    ///
    /// Positive `delta` is added to the actor's `pos` slot;
    /// negative `delta` is added (after `abs`) to the actor's
    /// `neg` slot. The two slots are independent, which is what
    /// keeps the merge an element-wise max on a join-semilattice.
    ///
    /// Saturates at `u64::MAX` per slot. A pathological client
    /// emitting more than `u64::MAX` total increments through a
    /// single replica is treated as if the counter saturated; the
    /// alternative -- panicking -- would let an unauthenticated
    /// client crash the node.
    pub fn apply(&mut self, actor: &ActorId, delta: i64) {
        if delta >= 0 {
            let entry = self.pos.entry(actor.clone()).or_insert(0);
            *entry = entry.saturating_add(delta.unsigned_abs());
        } else {
            let entry = self.neg.entry(actor.clone()).or_insert(0);
            *entry = entry.saturating_add(delta.unsigned_abs());
        }
    }

    /// Increment the counter through `actor`. Convenience wrapper
    /// over [`Self::apply`].
    pub fn increment(&mut self, actor: &ActorId, by: u64) {
        let entry = self.pos.entry(actor.clone()).or_insert(0);
        *entry = entry.saturating_add(by);
    }

    /// Decrement the counter through `actor`.
    pub fn decrement(&mut self, actor: &ActorId, by: u64) {
        let entry = self.neg.entry(actor.clone()).or_insert(0);
        *entry = entry.saturating_add(by);
    }
}

impl Crdt for PnCounter {
    type Value = i64;

    fn merge(&mut self, other: &Self) {
        for (actor, &count) in &other.pos {
            let entry = self.pos.entry(actor.clone()).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
        for (actor, &count) in &other.neg {
            let entry = self.neg.entry(actor.clone()).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
    }

    fn value(&self) -> i64 {
        let pos: u64 = self
            .pos
            .values()
            .fold(0u64, |acc, &x| acc.saturating_add(x));
        let neg: u64 = self
            .neg
            .values()
            .fold(0u64, |acc, &x| acc.saturating_add(x));
        let pos = i64::try_from(pos).unwrap_or(i64::MAX);
        let neg = i64::try_from(neg).unwrap_or(i64::MAX);
        pos.saturating_sub(neg)
    }
}

impl PnCounter {
    /// Borrow the raw `(pos, neg)` per-actor columns for
    /// serialization. The user-visible value is
    /// `sum(pos) - sum(neg)`; the columns are what merge joins.
    #[must_use]
    pub fn columns(
        &self,
    ) -> (
        &std::collections::BTreeMap<ActorId, u64>,
        &std::collections::BTreeMap<ActorId, u64>,
    ) {
        (&self.pos, &self.neg)
    }

    /// Reconstruct a counter from its `(pos, neg)` columns, the
    /// inverse of [`PnCounter::columns`]. Used by deserialization.
    #[must_use]
    pub fn from_columns(
        pos: std::collections::BTreeMap<ActorId, u64>,
        neg: std::collections::BTreeMap<ActorId, u64>,
    ) -> Self {
        Self { pos, neg }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(name: &str) -> ActorId {
        ActorId::new("dc1", name)
    }

    #[test]
    fn fresh_counter_is_zero() {
        let c = PnCounter::new();
        assert_eq!(c.value(), 0);
    }

    #[test]
    fn increment_then_decrement_yields_net() {
        let a = aid("a");
        let mut c = PnCounter::new();
        c.increment(&a, 5);
        c.decrement(&a, 2);
        assert_eq!(c.value(), 3);
    }

    #[test]
    fn apply_routes_signed_delta() {
        let a = aid("a");
        let mut c = PnCounter::new();
        c.apply(&a, 10);
        c.apply(&a, -3);
        assert_eq!(c.value(), 7);
    }

    #[test]
    fn merge_two_actors_sums_their_contributions() {
        let a = aid("a");
        let b = aid("b");
        let mut x = PnCounter::new();
        x.increment(&a, 5);
        let mut y = PnCounter::new();
        y.increment(&b, 7);
        x.merge(&y);
        assert_eq!(x.value(), 12);
    }

    #[test]
    fn merge_takes_pointwise_max_per_actor() {
        // A and B both incremented through actor "a" without
        // observing each other; the canonical PN-counter rule is
        // element-wise max, not sum, so the higher count wins.
        let a = aid("a");
        let mut x = PnCounter::new();
        x.increment(&a, 3);
        let mut y = PnCounter::new();
        y.increment(&a, 5);
        x.merge(&y);
        assert_eq!(x.value(), 5);
    }

    #[test]
    fn merge_is_idempotent() {
        let a = aid("a");
        let mut x = PnCounter::new();
        x.increment(&a, 4);
        let snapshot = x.clone();
        x.merge(&snapshot);
        assert_eq!(x.value(), 4);
        assert_eq!(x, snapshot);
    }

    #[test]
    fn merge_is_commutative() {
        let a = aid("a");
        let b = aid("b");
        let mut x = PnCounter::new();
        x.increment(&a, 3);
        x.decrement(&b, 1);
        let mut y = PnCounter::new();
        y.increment(&b, 4);
        y.decrement(&a, 2);

        let mut left = x.clone();
        left.merge(&y);
        let mut right = y.clone();
        right.merge(&x);
        assert_eq!(left, right);
    }

    #[test]
    fn concurrent_increments_then_merge_sums_distinct_actors() {
        let a = aid("a");
        let b = aid("b");
        let mut x = PnCounter::new();
        x.increment(&a, 1);
        let mut y = PnCounter::new();
        y.increment(&b, 1);
        x.merge(&y);
        assert_eq!(x.value(), 2);
    }
}
