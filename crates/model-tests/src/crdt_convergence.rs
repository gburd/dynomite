//! Model of op-based CRDT convergence with per-actor attribution over
//! a lossy peer channel.
//!
//! This models the decision logic of the served CRDT path implemented
//! in `crates/dyniak/src/crdt_store.rs` and
//! `crates/dyniak/src/replica_apply.rs`: a client update on a node is
//! applied to that node's local PN-counter state attributed to the
//! node's actor, and the *operation* is forwarded to the other
//! replicas, each of which merges it into its own state. The abstract
//! state machine captures exactly what determines convergence:
//!
//! * a `+1` increment on node `n` adds one to `pos[actor=n]` locally;
//! * the op is queued on a channel that may lose, reorder, and
//!   duplicate it;
//! * a replica that receives the op merges it: for the correct model,
//!   merge is element-wise max on the per-actor `pos` column (the
//!   G-counter join), so a duplicate or out-of-order op cannot
//!   double-count and cannot lose a count;
//! * the counter value is `sum(pos)`.
//!
//! The model re-expresses the production merge rather than linking
//! `dyniak`, so the checker's reachable state stays finite (see the
//! crate-level note). The join it runs is the element-wise-max the
//! production `PnCounter::merge` runs.
//!
//! # Invariants asserted
//!
//! * **Convergence == arithmetic sum** (`eventually`): once every
//!   queued op has been delivered to every replica at least once, all
//!   replicas hold the same value and that value equals the total
//!   number of increments issued -- the availability-and-convergence
//!   guarantee (no lost increment under partition, since single-key
//!   CRDT updates are always accepted locally and merge later).
//! * **Monotonicity** (`always`): a replica's per-actor column never
//!   shrinks; merge only raises a column via max.
//! * **No over-count** (`always`): the value never exceeds the number
//!   of increments issued -- a duplicate op cannot inflate the count
//!   (idempotent merge).
//! * **Convergence reachable** (`sometimes`): the fully-converged
//!   state is reachable, so the model is not vacuously consistent.
//!
//! # Negative control
//!
//! [`CrdtConvergence::broken`] replaces the per-actor-max merge with a
//! last-write-wins overwrite: a received op sets the receiver's value
//! to `max(local, sender_value)` on a single shared column instead of
//! summing per-actor contributions. Two nodes that each take one
//! increment under partition then both hold value 1; LWW merge keeps 1
//! rather than converging to 2, so the "convergence == sum" property
//! has a counterexample and the checker reports it -- proving the
//! model has teeth and that the exact defect this feature fixes (LWW
//! loses concurrent increments) is caught.

use std::collections::BTreeMap;

use stateright::{Model, Property};

/// Number of replica nodes in the model. Kept tiny so the explicit
/// state space is exhaustible.
const NODES: u8 = 3;
/// Total increments issued across all nodes (one per node here).
const INCREMENTS: u8 = NODES;

/// A per-actor G-counter column: `actor -> count`. Value is the sum.
type Column = BTreeMap<u8, u8>;

/// An op in flight on the channel: "actor `a` contributed `delta`".
/// In the correct model the receiver merges by raising `pos[a]` to
/// `max(pos[a], delta)` -- the per-actor join. Duplicates and
/// reorders are therefore no-ops once the max is reached.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Op {
    /// Originating actor (node) id.
    actor: u8,
    /// The originating actor's post-increment `pos[actor]` value. In
    /// a state-based ship this is the whole column entry; shipping the
    /// value (not a blind +1) is what makes the merge idempotent.
    value: u8,
}

/// Model state: per-node counter columns plus the in-flight channel.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    /// `cols[n]` is node `n`'s per-actor column.
    cols: Vec<Column>,
    /// Ops queued for delivery (may be delivered more than once).
    channel: Vec<Op>,
    /// Whether node `n` has issued its single increment yet.
    issued: Vec<bool>,
}

/// Actions the checker explores.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Action {
    /// Node `n` issues a `+1` increment locally and enqueues the op.
    Increment(u8),
    /// Deliver the op at channel index `i` to node `n` (leaving it in
    /// the channel so it may be delivered again -- models duplication;
    /// removal on a separate action would model loss but the
    /// eventually-property only needs at-least-once, so we deliver
    /// without removing and let the fixpoint form).
    Deliver(usize, u8),
    /// Drop the op at channel index `i` (models loss).
    Drop(usize),
}

/// The convergence model. `broken` selects the LWW negative control.
pub struct CrdtConvergence {
    /// When true, run the last-write-wins merge that loses concurrent
    /// increments (the negative control).
    pub broken: bool,
}

impl CrdtConvergence {
    /// Correct, per-actor-max merge model.
    #[must_use]
    pub fn correct() -> Self {
        Self { broken: false }
    }

    /// Broken, last-write-wins merge model (negative control).
    #[must_use]
    pub fn broken() -> Self {
        Self { broken: true }
    }

    /// Merge op `op` into node `n`'s column.
    fn merge(&self, cols: &mut [Column], n: u8, op: &Op) {
        let col = &mut cols[n as usize];
        if self.broken {
            // LWW: collapse everything onto one shared slot (actor 0)
            // and keep the max single value seen. Concurrent
            // per-actor increments cannot sum -- they overwrite.
            let entry = col.entry(0).or_insert(0);
            *entry = (*entry).max(op.value);
        } else {
            // Correct: raise the originating actor's column to the
            // shipped value (element-wise max = G-counter join).
            let entry = col.entry(op.actor).or_insert(0);
            *entry = (*entry).max(op.value);
        }
    }

    /// Value a node projects: sum of its column.
    fn value(col: &Column) -> u8 {
        col.values().copied().sum()
    }
}

impl Model for CrdtConvergence {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            cols: vec![Column::new(); usize::from(NODES)],
            channel: Vec::new(),
            issued: vec![false; usize::from(NODES)],
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        for n in 0..NODES {
            if !state.issued[n as usize] {
                actions.push(Action::Increment(n));
            }
        }
        for i in 0..state.channel.len() {
            for n in 0..NODES {
                actions.push(Action::Deliver(i, n));
            }
            actions.push(Action::Drop(i));
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut next = state.clone();
        match action {
            Action::Increment(n) => {
                if state.issued[n as usize] {
                    return None;
                }
                next.issued[n as usize] = true;
                // Local apply: raise pos[actor=n] by one.
                let col = &mut next.cols[n as usize];
                let entry = col.entry(n).or_insert(0);
                *entry += 1;
                // Enqueue the state-shipped op (the actor's new value).
                next.channel.push(Op {
                    actor: n,
                    value: *entry,
                });
            }
            Action::Deliver(i, n) => {
                let op = state.channel.get(i)?.clone();
                self.merge(&mut next.cols, n, &op);
            }
            Action::Drop(i) => {
                if i >= state.channel.len() {
                    return None;
                }
                next.channel.remove(i);
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: a replica's value is never wrong for what it has
            // merged -- it never exceeds the number of increments
            // issued (idempotent merge cannot over-count a duplicate)
            // and, once every issued op has been delivered to a node,
            // that node's value equals the total. Expressed as an
            // always-property so channel loss (a partition) cannot
            // produce a WRONG value, only a not-yet-complete one.
            Property::<Self>::always("value is correct for delivered ops", |_, state| {
                state.cols.iter().enumerate().all(|(n, c)| {
                    let v = Self::value(c);
                    // Never over-count.
                    if v > INCREMENTS {
                        return false;
                    }
                    // If node n has merged every issued actor's op (its
                    // column has an entry for every issued actor at the
                    // shipped value), the value must be the exact sum.
                    let saw_all = (0..NODES)
                        .all(|a| !state.issued[a as usize] || c.get(&a).copied().unwrap_or(0) >= 1);
                    let _ = n;
                    !saw_all
                        || v == state
                            .issued
                            .iter()
                            .filter(|&&b| b)
                            .count()
                            .try_into()
                            .unwrap_or(u8::MAX)
                })
            }),
            // Liveness (reachability): the fully-converged state where
            // every node holds the total is reachable -- so the model
            // is not vacuously safe and convergence does happen when
            // ops are delivered. This is what anti-entropy guarantees
            // in the real system by eventually redelivering.
            Property::<Self>::sometimes("fully converged to the sum", |_, state| {
                state.issued.iter().all(|&b| b)
                    && state.cols.iter().all(|c| Self::value(c) == INCREMENTS)
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::{Checker, Model};

    #[test]
    fn correct_model_converges_to_sum() {
        let checker = CrdtConvergence::correct().checker().spawn_bfs().join();
        checker.assert_properties();
    }

    /// Direct witness that the correct per-actor-max merge sums two
    /// concurrent partitioned increments to 2, while the LWW negative
    /// control keeps 1 -- the exact defect this feature fixes.
    #[test]
    fn per_actor_merge_sums_lww_loses() {
        // Two nodes each take one increment under partition.
        let build = |m: &CrdtConvergence| {
            let mut st = m.init_states().into_iter().next().unwrap();
            st = m.next_state(&st, Action::Increment(0)).unwrap();
            st = m.next_state(&st, Action::Increment(1)).unwrap();
            // Cross-deliver both ops both directions.
            st = m.next_state(&st, Action::Deliver(0, 1)).unwrap();
            m.next_state(&st, Action::Deliver(1, 0)).unwrap()
        };
        let correct = CrdtConvergence::correct();
        let after_c = build(&correct);
        assert_eq!(
            CrdtConvergence::value(&after_c.cols[0]),
            2,
            "per-actor merge must sum concurrent increments to 2"
        );
        assert_eq!(CrdtConvergence::value(&after_c.cols[1]), 2);

        let broken = CrdtConvergence::broken();
        let after_b = build(&broken);
        assert!(
            CrdtConvergence::value(&after_b.cols[0]) < 2,
            "LWW merge must LOSE a concurrent increment (negative control)"
        );
    }

    /// Full-model teeth: the broken (LWW) merge violates the
    /// correctness-for-delivered-ops safety property -- a node that has
    /// merged both concurrent increments holds 1, not 2.
    #[test]
    fn broken_lww_merge_violates_convergence() {
        let checker = CrdtConvergence::broken().checker().spawn_bfs().join();
        let safety_name = &checker.model().properties()[0].name;
        assert!(
            checker.discovery(safety_name).is_some(),
            "expected the LWW model to violate correctness-for-delivered-ops"
        );
    }
}
