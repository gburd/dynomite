//! Riak OR-Set (observed-remove set).
//!
//! Each element carries two tag sets: `adds` records every
//! generation tag the element has ever been added under, and
//! `removes` records every tag that has been observed-and-removed.
//! An element is present iff `adds.difference(removes)` is
//! non-empty. Merge unions both sides element-wise.
//!
//! # Tag generation
//!
//! Tags are `(actor, counter)` pairs. Each replica keeps a
//! per-actor monotonically increasing counter; an `add` increments
//! the counter and stores the tag. Tags are unique per replica:
//! two adds from the same replica produce two distinct tags.
//!
//! A `remove` copies the element's currently-observed tags into
//! the `removes` set, which acts as a tombstone -- a concurrent
//! add from another replica produces a fresh tag that is not in
//! `removes`, so the element survives the merge.

use std::collections::{BTreeMap, BTreeSet};

use crate::datatypes::{ActorId, Crdt};

/// OR-Set element-tag tuple.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Tag {
    /// Replica that produced the tag.
    pub actor: ActorId,
    /// Per-actor monotonically increasing counter at the time of
    /// the add.
    pub counter: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ElementState {
    adds: BTreeSet<Tag>,
    removes: BTreeSet<Tag>,
}

impl ElementState {
    fn is_present(&self) -> bool {
        self.adds.iter().any(|t| !self.removes.contains(t))
    }

    fn merge(&mut self, other: &Self) {
        for tag in &other.adds {
            self.adds.insert(tag.clone());
        }
        for tag in &other.removes {
            self.removes.insert(tag.clone());
        }
    }
}

/// Observed-remove set CRDT keyed by arbitrary `Vec<u8>` elements.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrSet {
    elements: BTreeMap<Vec<u8>, ElementState>,
    actor_counters: BTreeMap<ActorId, u64>,
}

impl OrSet {
    /// Construct an empty set.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `element` to the set on behalf of `actor`. Returns the
    /// freshly minted tag.
    pub fn add(&mut self, actor: &ActorId, element: impl Into<Vec<u8>>) -> Tag {
        let counter = self.actor_counters.entry(actor.clone()).or_insert(0);
        *counter = counter.checked_add(1).expect("or-set counter overflow");
        let tag = Tag {
            actor: actor.clone(),
            counter: *counter,
        };
        let entry = self.elements.entry(element.into()).or_default();
        entry.adds.insert(tag.clone());
        tag
    }

    /// Remove `element` from the set. Tombstones every tag
    /// currently observed for the element. Concurrent adds with
    /// fresh tags survive, which is the OR-Set "add wins on tie"
    /// rule Riak uses.
    pub fn remove(&mut self, element: &[u8]) {
        if let Some(state) = self.elements.get_mut(element) {
            for tag in state.adds.clone() {
                state.removes.insert(tag);
            }
        }
    }

    /// Whether `element` is present in the set's value projection.
    #[must_use]
    pub fn contains(&self, element: &[u8]) -> bool {
        self.elements
            .get(element)
            .is_some_and(ElementState::is_present)
    }
}

impl Crdt for OrSet {
    type Value = BTreeSet<Vec<u8>>;

    fn merge(&mut self, other: &Self) {
        for (actor, &count) in &other.actor_counters {
            let entry = self.actor_counters.entry(actor.clone()).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
        for (element, state) in &other.elements {
            let entry = self.elements.entry(element.clone()).or_default();
            entry.merge(state);
        }
    }

    fn value(&self) -> BTreeSet<Vec<u8>> {
        self.elements
            .iter()
            .filter_map(|(e, s)| {
                if s.is_present() {
                    Some(e.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(name: &str) -> ActorId {
        ActorId::new("dc1", name)
    }

    #[test]
    fn add_then_value_contains_element() {
        let a = aid("a");
        let mut s = OrSet::new();
        s.add(&a, b"x".to_vec());
        let v = s.value();
        assert_eq!(v.len(), 1);
        assert!(v.contains(b"x".as_slice()));
    }

    #[test]
    fn remove_after_add_clears_element() {
        let a = aid("a");
        let mut s = OrSet::new();
        s.add(&a, b"x".to_vec());
        s.remove(b"x");
        assert!(!s.contains(b"x"));
    }

    #[test]
    fn concurrent_adds_merge_to_union() {
        let actor_a = aid("a");
        let actor_b = aid("b");
        let mut left = OrSet::new();
        left.add(&actor_a, b"foo".to_vec());
        let mut right = OrSet::new();
        right.add(&actor_b, b"foo".to_vec());
        right.add(&actor_b, b"bar".to_vec());
        left.merge(&right);
        let merged = left.value();
        assert!(merged.contains(b"foo".as_slice()));
        assert!(merged.contains(b"bar".as_slice()));
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn concurrent_remove_loses_to_concurrent_add() {
        // Both replicas start with `x` already added by replica A.
        // Replica A removes x; replica B (concurrent, no knowledge
        // of A's remove) adds x again with a fresh tag. The OR-Set
        // rule says the new add survives the merge.
        let a = aid("a");
        let b = aid("b");
        let mut shared = OrSet::new();
        shared.add(&a, b"x".to_vec());

        let mut left = shared.clone();
        left.remove(b"x");

        let mut right = shared.clone();
        right.add(&b, b"x".to_vec());

        let mut merged = left.clone();
        merged.merge(&right);

        assert!(merged.contains(b"x"));
    }

    #[test]
    fn merge_is_commutative() {
        let a = aid("a");
        let b = aid("b");
        let mut x = OrSet::new();
        x.add(&a, b"alpha".to_vec());
        x.add(&a, b"beta".to_vec());
        let mut y = OrSet::new();
        y.add(&b, b"beta".to_vec());
        y.remove(b"beta");
        y.add(&b, b"gamma".to_vec());

        let mut left = x.clone();
        left.merge(&y);
        let mut right = y.clone();
        right.merge(&x);
        assert_eq!(left.value(), right.value());
    }

    #[test]
    fn merge_is_idempotent() {
        let a = aid("a");
        let mut x = OrSet::new();
        x.add(&a, b"x".to_vec());
        let snapshot = x.clone();
        x.merge(&snapshot);
        assert_eq!(x.value(), snapshot.value());
    }

    #[test]
    fn add_after_remove_resurrects_with_fresh_tag() {
        let a = aid("a");
        let mut s = OrSet::new();
        s.add(&a, b"x".to_vec());
        s.remove(b"x");
        assert!(!s.contains(b"x"));
        s.add(&a, b"x".to_vec());
        assert!(s.contains(b"x"));
    }
}
