//! Riak CRDT data types.
//!
//! This module ships the four primitive Riak CRDTs in their
//! state-based form:
//!
//! * [`PnCounter`] -- positive/negative counter. Per-actor pos and
//!   neg counts; merge is element-wise max; value is sum(pos) -
//!   sum(neg).
//! * [`OrSet`] -- observed-remove set. Per-element add and remove
//!   tag sets; an element is present iff at least one add tag is
//!   not shadowed by a remove tag; merge is set-union of tag sets.
//! * [`LwwRegister`] -- last-write-wins register. State is
//!   (value, timestamp, actor); merge picks the higher timestamp
//!   with ties broken by actor id.
//! * [`EwFlag`] -- enable-wins boolean. Same shape as [`OrSet`]
//!   restricted to a singleton domain. Concurrent enable + disable
//!   resolves to enabled.
//!
//! Vector clocks for tag generation and conflict-resolution
//! metadata live in [`vclock`].
//!
//! # Actor-id mapping
//!
//! Riak's CRDTs key per-replica metadata by an Erlang `vnode_id`
//! tuple. This crate models actors as a value-typed
//! [`ActorId`] carrying the (datacenter name, peer name) pair the
//! Dynomite substrate already exposes through
//! [`dynomite::embed::TopologySnapshot`]. The pair is stable across
//! gossip rounds and totally ordered, which is exactly what an
//! OR-Set tag generator and an LWW-register tiebreaker need.
//!
//! # CRDT laws
//!
//! Every type in this module satisfies the standard CRDT laws:
//!
//! * Associativity: `merge(merge(a, b), c) == merge(a, merge(b, c))`.
//! * Commutativity: `merge(a, b) == merge(b, a)`.
//! * Idempotence: `merge(a, a) == a`.
//!
//! Property tests under
//! `crates/dyn-riak/tests/datatypes_properties.rs` exercise all
//! three on randomly generated states.

pub mod counter;
pub mod flag;
pub mod register;
pub mod set;
pub mod vclock;

// Map and HyperLogLog land in the second CRDT slice; appended
// below the original four-type block so parallel branches do
// not conflict.
pub mod hll;
pub mod map;

use std::cmp::Ordering;

pub use crate::datatypes::counter::PnCounter;
pub use crate::datatypes::flag::EwFlag;
pub use crate::datatypes::register::LwwRegister;
pub use crate::datatypes::set::OrSet;
pub use crate::datatypes::vclock::{Vclock, VclockOrder};

pub use crate::datatypes::hll::HyperLogLog;
pub use crate::datatypes::map::{FieldKey, FieldType, FieldValue, Map, MapOp, NestedOp};

/// Identifier for a replica that produces CRDT operations.
///
/// Riak uses an Erlang `vnode_id` tuple. This crate uses the
/// (datacenter name, peer name) pair the Dynomite substrate
/// publishes through gossip. Both names are arbitrary ASCII byte
/// strings; ordering is lexicographic on the pair so OR-Set tag
/// comparisons and LWW tiebreakers are deterministic.
///
/// # Examples
///
/// ```
/// use dyn_riak::datatypes::ActorId;
/// let a = ActorId::new("dc1", "peer-a");
/// let b = ActorId::new("dc1", "peer-b");
/// assert!(a < b);
/// ```
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ActorId {
    /// Datacenter name (matches `dynomite::cluster::Datacenter::name`).
    pub dc: String,
    /// Peer name (matches `dynomite::cluster::Peer::name`).
    pub peer: String,
}

impl ActorId {
    /// Construct an actor id from (datacenter, peer).
    pub fn new(dc: impl Into<String>, peer: impl Into<String>) -> Self {
        Self {
            dc: dc.into(),
            peer: peer.into(),
        }
    }
}

/// Helper: compare two `(timestamp, actor)` pairs as Riak's LWW
/// rule does: the higher timestamp wins; on tie, the higher actor
/// id wins.
#[must_use]
pub fn lww_order(a_ts: u64, a_actor: &ActorId, b_ts: u64, b_actor: &ActorId) -> Ordering {
    a_ts.cmp(&b_ts).then_with(|| a_actor.cmp(b_actor))
}

/// State-based CRDT contract.
///
/// Every CRDT in this module merges by an idempotent, commutative,
/// associative join. The `value` projection extracts the user-
/// visible state for the Riak `DtValue` response.
pub trait Crdt {
    /// User-visible value type produced by [`Self::value`].
    type Value;

    /// Merge `other` into `self` in place. Equivalent to
    /// state-based join: the result is the least upper bound of
    /// the two inputs in the CRDT's lattice.
    fn merge(&mut self, other: &Self);

    /// Return the user-visible value. For maps this is a snapshot;
    /// for counters and flags it is a primitive; for sets it is a
    /// `BTreeSet`.
    fn value(&self) -> Self::Value;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_id_is_lex_ordered() {
        let a = ActorId::new("dc1", "alpha");
        let b = ActorId::new("dc1", "beta");
        let c = ActorId::new("dc2", "alpha");
        assert!(a < b);
        assert!(b < c);
        assert_eq!(a, ActorId::new("dc1", "alpha"));
    }

    #[test]
    fn lww_order_breaks_ties_by_actor() {
        let a = ActorId::new("dc1", "alpha");
        let b = ActorId::new("dc1", "beta");
        assert_eq!(lww_order(5, &a, 5, &b), Ordering::Less);
        assert_eq!(lww_order(6, &a, 5, &b), Ordering::Greater);
        assert_eq!(lww_order(5, &a, 5, &a), Ordering::Equal);
    }
}
