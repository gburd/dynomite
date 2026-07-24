//! Model of CRDT read-coordination over a replica set.
//!
//! The prior `crdt_convergence` model assumed a write is delivered to
//! every node and asserted per-replica convergence -- a premise the
//! production code does not meet, which is why the owner-centric gap
//! (docs/journal/2026-07-24-chaos-crdt-owner-centric-finding.md) reached
//! a scale test instead of a unit test. This model corrects the premise
//! to match the intended fix and proves the fix converges a read from
//! ANY node while a local-only read (the pre-fix behaviour) does not.
//!
//! Abstract state machine:
//!
//! * A fixed key has a REPLICA SET: a subset of the nodes (size R < N).
//!   Only replicas ever hold the key's CRDT state.
//! * A write coordinated at any node is applied to each replica's local
//!   PN-counter state (attributed to the writer's actor) -- the
//!   write-to-the-replica-set fan-out. Non-replica nodes never hold the
//!   key. Delivery may be partial (a replica may miss a write -- models
//!   a fan-out drop or a lagging replica).
//! * A READ is issued at an arbitrary node (which may not be a
//!   replica). Two read strategies:
//!   - CORRECT (coordinated): the read fans to the key's replica set,
//!     merges the states it collects, and returns the merged value.
//!     Even a read at a non-replica, or at a replica that missed a
//!     write, converges to the value implied by the union of replica
//!     states.
//!   - BROKEN (local-only, the negative control): the read returns the
//!     issuing node's local state -- null at a non-replica, partial at a
//!     lagging replica.
//!
//! # Invariants
//!
//! * **Coordinated read is correct** (`always`, correct model): a
//!   coordinated read at any node returns exactly the merge of all
//!   replica states, i.e. the value that reflects every write any
//!   replica has seen -- never a partial or missing value relative to
//!   the replica-set union.
//! * **Reachability** (`sometimes`): the fully-fanned-out state where
//!   every replica has every write, and a read returns the total, is
//!   reachable (not vacuous).
//!
//! # Negative control
//!
//! [`CrdtRouting::broken`] uses the local-only read. A read at a
//! non-replica returns null, and a read at a replica that missed a
//! write returns a value below the replica-set union -- so the
//! "coordinated read is correct" property has a counterexample and the
//! checker reports it, proving the model catches exactly the
//! owner-centric gap the chaos test found.

use std::collections::BTreeMap;

use stateright::{Model, Property};

/// Total nodes in the model.
const NODES: u8 = 3;
/// Replica-set size (R < NODES so a non-replica read is exercised).
const R: u8 = 2;
/// One write per node (each attributed to a distinct actor).
const WRITES: u8 = NODES;

/// Per-actor G-counter column. Value is the sum.
type Column = BTreeMap<u8, u8>;

/// A write in flight to a specific replica: "raise replica's column for
/// `actor` to `value`" (state-shipped, so re-delivery is idempotent).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Fan {
    /// Replica node the fan targets.
    replica: u8,
    /// Originating actor.
    actor: u8,
    /// The shipped per-actor value (post-increment). Merge is max.
    value: u8,
}

/// Model state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    /// `cols[n]` is node `n`'s local column for the key (empty when the
    /// node is not a replica or has not yet received any write).
    cols: Vec<Column>,
    /// Fans queued for delivery to replicas (may be delivered more than
    /// once or dropped).
    fans: Vec<Fan>,
    /// Whether node `n` has issued its single write.
    issued: Vec<bool>,
}

/// Actions the checker explores.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Action {
    /// Node `n` coordinates a write: enqueue a fan to every replica.
    Write(u8),
    /// Deliver the fan at index `i` (merged into its target replica).
    Deliver(usize),
    /// Drop the fan at index `i` (a lost fan-out / lagging replica).
    Drop(usize),
}

/// The read-coordination model. `broken` selects the local-only read.
pub struct CrdtRouting {
    /// Local-only read (negative control) when true.
    pub broken: bool,
}

impl CrdtRouting {
    /// Correct, coordinated-read model.
    #[must_use]
    pub fn correct() -> Self {
        Self { broken: false }
    }

    /// Broken, local-only-read model (negative control).
    #[must_use]
    pub fn broken() -> Self {
        Self { broken: true }
    }

    /// The replica set for the key: the first `R` nodes (0..R). Nodes
    /// numbered R or higher are non-replicas that must still return a
    /// correct value on a coordinated read.
    fn is_replica(n: u8) -> bool {
        n < R
    }

    fn value(col: &Column) -> u8 {
        col.values().copied().sum()
    }

    /// The value a read at `node` returns.
    fn read(&self, state: &State, node: u8) -> u8 {
        if self.broken {
            // Local-only: whatever this node happens to hold.
            Self::value(&state.cols[node as usize])
        } else {
            // Coordinated: merge every replica's column (element-wise
            // max per actor) and sum. A read at any node -- replica or
            // not -- sees the replica-set union.
            let mut merged: Column = Column::new();
            for r in 0..NODES {
                if Self::is_replica(r) {
                    for (&a, &v) in &state.cols[r as usize] {
                        let e = merged.entry(a).or_insert(0);
                        if *e < v {
                            *e = v;
                        }
                    }
                }
            }
            Self::value(&merged)
        }
    }

    /// The correct value once all writes are delivered: the number of
    /// writes issued (each contributes 1 through a distinct actor).
    fn expected(state: &State) -> u8 {
        u8::try_from(state.issued.iter().filter(|&&b| b).count())
            .expect("invariant: issued writes bounded by WRITES < 256")
    }
}

impl Model for CrdtRouting {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            cols: vec![Column::new(); usize::from(NODES)],
            fans: Vec::new(),
            issued: vec![false; usize::from(NODES)],
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for n in 0..NODES {
            if !state.issued[n as usize] {
                actions.push(Action::Write(n));
            }
        }
        for i in 0..state.fans.len() {
            actions.push(Action::Deliver(i));
            actions.push(Action::Drop(i));
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        match action {
            Action::Write(n) => {
                if state.issued[n as usize] {
                    return None;
                }
                next.issued[n as usize] = true;
                // Enqueue one fan per replica: actor n contributes value
                // 1. (A single write per actor keeps the domain small;
                // the shipped value is the actor's post-increment count.)
                for r in 0..NODES {
                    if Self::is_replica(r) {
                        next.fans.push(Fan {
                            replica: r,
                            actor: n,
                            value: 1,
                        });
                    }
                }
            }
            Action::Deliver(i) => {
                let fan = state.fans.get(i)?.clone();
                let col = &mut next.cols[fan.replica as usize];
                let e = col.entry(fan.actor).or_insert(0);
                if *e < fan.value {
                    *e = fan.value;
                }
                // Remove on delivery: at-most-once per queued fan keeps
                // the reachable state finite and small. Idempotent merge
                // means a re-delivery would be a no-op anyway; partial
                // delivery is still exercised via Drop.
                next.fans.remove(i);
            }
            Action::Drop(i) => {
                if i >= state.fans.len() {
                    return None;
                }
                next.fans.remove(i);
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // A coordinated read at ANY node never returns a value above
            // the replica-set union, and once every issued write has
            // been delivered to at least one replica, a coordinated read
            // at every node returns the exact total. Expressed as an
            // always-property: a coordinated read is never WRONG (too
            // high), and when the replica set is complete it is exactly
            // right at every node including non-replicas.
            Property::<Self>::always(
                "coordinated read is correct at every node",
                |model, state| {
                    let expected_if_complete = Self::expected(state);
                    // Is every issued write present on at least one replica?
                    let complete = (0..NODES).all(|n| {
                        !state.issued[n as usize]
                            || (0..NODES)
                                .filter(|&r| Self::is_replica(r))
                                .any(|r| state.cols[r as usize].get(&n).copied().unwrap_or(0) >= 1)
                    });
                    (0..NODES).all(|node| {
                        let v = model.read(state, node);
                        if v > expected_if_complete {
                            return false; // never over-count
                        }
                        !complete || v == expected_if_complete
                    })
                },
            ),
            Property::<Self>::sometimes("all writes fanned, read returns total", |model, state| {
                let total = Self::expected(state);
                total == WRITES && (0..NODES).all(|n| model.read(state, n) == total)
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::{Checker, Model};

    #[test]
    fn coordinated_read_converges_at_every_node() {
        let checker = CrdtRouting::correct().checker().spawn_bfs().join();
        checker.assert_properties();
    }

    /// Direct witness: with every write delivered to the replica set, a
    /// coordinated read at a NON-REPLICA node (node R, outside the
    /// replica set) still returns the total, while a local-only read
    /// there returns 0 -- the exact owner-centric gap.
    #[test]
    fn nonreplica_read_coordinated_vs_local() {
        let build = |m: &CrdtRouting| {
            let mut st = m.init_states().into_iter().next().unwrap();
            for n in 0..NODES {
                st = m.next_state(&st, Action::Write(n)).unwrap();
            }
            // Deliver every fan.
            while !st.fans.is_empty() {
                st = m.next_state(&st, Action::Deliver(0)).unwrap();
            }
            st
        };
        let correct = CrdtRouting::correct();
        let st = build(&correct);
        let nonreplica = NODES - 1; // node index R.. is a non-replica
        assert!(!CrdtRouting::is_replica(nonreplica));
        assert_eq!(
            correct.read(&st, nonreplica),
            WRITES,
            "coordinated read at a non-replica must return the total"
        );
        let broken = CrdtRouting::broken();
        assert_eq!(
            broken.read(&st, nonreplica),
            0,
            "local-only read at a non-replica returns nothing (the gap)"
        );
    }

    /// Full-model teeth: the local-only (broken) model violates the
    /// coordinated-read-correct property.
    #[test]
    fn local_only_read_violates_correctness() {
        let checker = CrdtRouting::broken().checker().spawn_bfs().join();
        let name = &checker.model().properties()[0].name;
        assert!(
            checker.discovery(name).is_some(),
            "expected the local-only-read model to violate read correctness"
        );
    }
}
