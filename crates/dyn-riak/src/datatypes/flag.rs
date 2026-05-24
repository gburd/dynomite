//! Riak enable-wins boolean flag.
//!
//! An enable-wins flag is structurally an OR-Set restricted to the
//! singleton "this flag is enabled" element. `enable` produces a
//! fresh tag and inserts it into the live set; `disable` copies
//! every currently-observed live tag into the tombstone set.
//! Concurrent enable + disable resolves to enabled because the
//! enable produces a tag the concurrent disable has not observed.

use std::collections::BTreeSet;

use crate::datatypes::set::Tag;
use crate::datatypes::{ActorId, Crdt};

/// Enable-wins boolean flag CRDT.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EwFlag {
    adds: BTreeSet<Tag>,
    removes: BTreeSet<Tag>,
    actor_counters: std::collections::BTreeMap<ActorId, u64>,
}

impl EwFlag {
    /// Construct a fresh, disabled flag.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable the flag through `actor`. Mints a new tag.
    pub fn enable(&mut self, actor: &ActorId) -> Tag {
        let counter = self.actor_counters.entry(actor.clone()).or_insert(0);
        *counter = counter.checked_add(1).expect("ew-flag counter overflow");
        let tag = Tag {
            actor: actor.clone(),
            counter: *counter,
        };
        self.adds.insert(tag.clone());
        tag
    }

    /// Disable the flag. Tombstones every currently-observed live
    /// tag. Concurrent enables that produced fresh tags survive.
    pub fn disable(&mut self) {
        for tag in &self.adds {
            self.removes.insert(tag.clone());
        }
    }
}

impl Crdt for EwFlag {
    type Value = bool;

    fn merge(&mut self, other: &Self) {
        for (actor, &count) in &other.actor_counters {
            let entry = self.actor_counters.entry(actor.clone()).or_insert(0);
            if *entry < count {
                *entry = count;
            }
        }
        for tag in &other.adds {
            self.adds.insert(tag.clone());
        }
        for tag in &other.removes {
            self.removes.insert(tag.clone());
        }
    }

    fn value(&self) -> bool {
        self.adds.iter().any(|t| !self.removes.contains(t))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(name: &str) -> ActorId {
        ActorId::new("dc1", name)
    }

    #[test]
    fn fresh_flag_is_disabled() {
        let f = EwFlag::new();
        assert!(!f.value());
    }

    #[test]
    fn enable_makes_flag_true() {
        let a = aid("a");
        let mut f = EwFlag::new();
        f.enable(&a);
        assert!(f.value());
    }

    #[test]
    fn disable_after_enable_clears_flag() {
        let a = aid("a");
        let mut f = EwFlag::new();
        f.enable(&a);
        f.disable();
        assert!(!f.value());
    }

    #[test]
    fn concurrent_enable_wins_over_disable() {
        let a = aid("a");
        let b = aid("b");
        let mut shared = EwFlag::new();
        shared.enable(&a);

        let mut left = shared.clone();
        left.disable();
        assert!(!left.value());

        let mut right = shared.clone();
        right.enable(&b);

        let mut merged = left.clone();
        merged.merge(&right);
        assert!(
            merged.value(),
            "enable-wins: concurrent enable must beat disable"
        );
    }

    #[test]
    fn merge_is_commutative() {
        let a = aid("a");
        let b = aid("b");
        let mut x = EwFlag::new();
        x.enable(&a);
        x.disable();
        let mut y = EwFlag::new();
        y.enable(&b);

        let mut left = x.clone();
        left.merge(&y);
        let mut right = y.clone();
        right.merge(&x);
        assert_eq!(left, right);
    }

    #[test]
    fn merge_is_idempotent() {
        let a = aid("a");
        let mut x = EwFlag::new();
        x.enable(&a);
        let snap = x.clone();
        x.merge(&snap);
        assert_eq!(x, snap);
    }

    #[test]
    fn enable_disable_enable_is_enabled() {
        let a = aid("a");
        let mut f = EwFlag::new();
        f.enable(&a);
        f.disable();
        assert!(!f.value());
        f.enable(&a);
        assert!(f.value());
    }
}
