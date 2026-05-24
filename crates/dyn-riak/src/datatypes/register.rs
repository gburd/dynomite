//! Riak LWW-Register (last-write-wins).
//!
//! State is `(value, timestamp, actor)`. An assignment replaces
//! the register's contents iff the new `(timestamp, actor)` pair
//! is strictly greater than the current one under the lexicographic
//! order `lww_order`. Merge does the same comparison.
//!
//! # Timestamp policy
//!
//! Riak's reference implementation uses microseconds since the
//! Unix epoch. This module exposes [`LwwRegister::assign_now`]
//! that reads the system clock and matches Riak's units; tests
//! that need determinism use [`LwwRegister::assign`] with an
//! explicit timestamp.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::datatypes::{lww_order, ActorId, Crdt};

/// Last-write-wins register.
///
/// An empty register has `value` empty and timestamp zero. Any
/// assignment with a non-zero timestamp dominates the empty
/// register.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LwwRegister {
    value: Vec<u8>,
    ts_micros: u64,
    actor: Option<ActorId>,
}

impl LwwRegister {
    /// Construct an empty register.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign `value` from `actor` at the given timestamp.
    ///
    /// The assignment is dropped if the supplied timestamp/actor
    /// pair is not strictly greater than the current one.
    pub fn assign(&mut self, actor: &ActorId, ts_micros: u64, value: impl Into<Vec<u8>>) {
        let dominates = match &self.actor {
            None => true,
            Some(current) => lww_order(ts_micros, actor, self.ts_micros, current).is_gt(),
        };
        if dominates {
            self.value = value.into();
            self.ts_micros = ts_micros;
            self.actor = Some(actor.clone());
        }
    }

    /// Assign `value` from `actor` using the current wall-clock
    /// time in microseconds since the Unix epoch.
    ///
    /// The system clock is consulted via [`SystemTime::now`].
    /// Test code that needs determinism should call
    /// [`Self::assign`] with an explicit timestamp.
    pub fn assign_now(&mut self, actor: &ActorId, value: impl Into<Vec<u8>>) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        self.assign(actor, ts, value);
    }

    /// Return the timestamp in microseconds since the Unix epoch.
    /// Zero when the register has never been assigned.
    #[must_use]
    pub fn timestamp(&self) -> u64 {
        self.ts_micros
    }

    /// Return the actor that produced the current value, if any.
    #[must_use]
    pub fn actor(&self) -> Option<&ActorId> {
        self.actor.as_ref()
    }
}

impl Crdt for LwwRegister {
    type Value = Vec<u8>;

    fn merge(&mut self, other: &Self) {
        let take_other = match (&self.actor, &other.actor) {
            (None, Some(_)) => true,
            (None | Some(_), None) => false,
            (Some(self_actor), Some(other_actor)) => {
                lww_order(other.ts_micros, other_actor, self.ts_micros, self_actor).is_gt()
            }
        };
        if take_other {
            self.value.clone_from(&other.value);
            self.ts_micros = other.ts_micros;
            self.actor.clone_from(&other.actor);
        }
    }

    fn value(&self) -> Vec<u8> {
        self.value.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aid(name: &str) -> ActorId {
        ActorId::new("dc1", name)
    }

    #[test]
    fn empty_register_has_empty_value() {
        let r = LwwRegister::new();
        assert!(r.value().is_empty());
        assert_eq!(r.timestamp(), 0);
        assert!(r.actor().is_none());
    }

    #[test]
    fn assign_with_higher_timestamp_wins() {
        let a = aid("a");
        let b = aid("b");
        let mut r = LwwRegister::new();
        r.assign(&a, 1, b"first".to_vec());
        r.assign(&b, 2, b"second".to_vec());
        assert_eq!(r.value(), b"second".to_vec());
    }

    #[test]
    fn earlier_timestamp_does_not_overwrite() {
        let a = aid("a");
        let b = aid("b");
        let mut r = LwwRegister::new();
        r.assign(&a, 5, b"newer".to_vec());
        r.assign(&b, 3, b"older".to_vec());
        assert_eq!(r.value(), b"newer".to_vec());
    }

    #[test]
    fn tie_breaks_by_actor_id() {
        let a = aid("alpha");
        let b = aid("beta");
        let mut r = LwwRegister::new();
        r.assign(&a, 5, b"alpha".to_vec());
        r.assign(&b, 5, b"beta".to_vec());
        assert_eq!(r.value(), b"beta".to_vec());

        let mut r2 = LwwRegister::new();
        r2.assign(&b, 5, b"beta".to_vec());
        r2.assign(&a, 5, b"alpha".to_vec());
        assert_eq!(r2.value(), b"beta".to_vec());
    }

    #[test]
    fn merge_picks_higher_timestamp() {
        let a = aid("a");
        let b = aid("b");
        let mut x = LwwRegister::new();
        x.assign(&a, 1, b"early".to_vec());
        let mut y = LwwRegister::new();
        y.assign(&b, 2, b"late".to_vec());
        x.merge(&y);
        assert_eq!(x.value(), b"late".to_vec());
    }

    #[test]
    fn merge_is_commutative() {
        let a = aid("a");
        let b = aid("b");
        let mut x = LwwRegister::new();
        x.assign(&a, 7, b"x".to_vec());
        let mut y = LwwRegister::new();
        y.assign(&b, 9, b"y".to_vec());

        let mut left = x.clone();
        left.merge(&y);
        let mut right = y.clone();
        right.merge(&x);
        assert_eq!(left, right);
    }

    #[test]
    fn merge_is_idempotent() {
        let a = aid("a");
        let mut r = LwwRegister::new();
        r.assign(&a, 1, b"x".to_vec());
        let snap = r.clone();
        r.merge(&snap);
        assert_eq!(r, snap);
    }

    #[test]
    fn assign_now_advances_timestamp() {
        let a = aid("a");
        let mut r = LwwRegister::new();
        r.assign_now(&a, b"v".to_vec());
        assert!(r.timestamp() > 0);
        assert_eq!(r.value(), b"v".to_vec());
    }
}
