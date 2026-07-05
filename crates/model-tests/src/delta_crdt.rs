//! Model of delta-state CRDT convergence over a lossy channel.
//!
//! This models the delta-CRDT upgrade of the observed-remove set
//! implemented in `crates/dyniak/src/datatypes/delta_set.rs`
//! (`DeltaOrSet`) and the delta-shipping AAE hook in
//! `crates/dyniak/src/aae/delta_ship.rs`. The abstract state machine
//! captures the decision logic that matters for convergence:
//!
//! * a delta-mutator produces a join-irreducible fragment (here, a
//!   single dot on an element);
//! * deltas travel over a channel that may lose, reorder, and
//!   duplicate them;
//! * a replica joins whatever deltas it receives, in any order;
//! * join is set-union of dots (least-upper-bound in the lattice).
//!
//! The model deliberately re-expresses the production join rather
//! than linking `dyniak`, so the checker's reachable state space
//! stays small (see the crate-level note). The join it runs is the
//! same set-union-of-dots the production `merge_delta` runs.
//!
//! # Invariants asserted
//!
//! * **Strong eventual consistency** (`eventually`): once the channel
//!   has drained (every buffered delta delivered at least once), any
//!   two replicas that have delivered the same *set* of deltas hold
//!   equal state -- regardless of delivery order or duplication.
//! * **Monotonicity** (`always`): a replica's dot set never shrinks;
//!   join only adds dots. This is the lattice-law consequence that
//!   makes the system have a stable fixpoint.
//! * **Delivered-implies-present** (`always`): every dot a replica
//!   has delivered is in its state (idempotent, order-independent
//!   join -- an out-of-order or duplicate delta cannot drop a dot).
//! * **Convergence reachable** (`sometimes`): the fully-converged
//!   state is reachable, so the model is not vacuously consistent.
//!
//! # Negative control
//!
//! [`DeltaCrdt::broken`] flips one behaviour: a "remove" delta ships
//! *only the tombstone marker without the dot it tombstones* -- i.e.
//! it drops the causal context, so it is NOT join-irreducible. A
//! replica that receives the tombstone before the corresponding add
//! then later receives the add re-adds the element, diverging from a
//! replica that received them in the other order. The SEC property
//! then has a counterexample and the checker reports it, proving the
//! model has teeth.

use std::collections::BTreeSet;

use stateright::{Model, Property};

/// A dot: `(replica, sequence)`. The join-irreducible unit an add
/// mutator produces. Small domain keeps the search finite.
type Dot = (u8, u8);

/// A delta in flight on the channel. In the correct model an add
/// delta carries its dot in `adds`; a remove delta carries the
/// tombstoned dots in `removes` (this IS the causal context that
/// makes the fragment join-irreducible: joining it is a pure
/// set-union, independent of the receiver's current state or the
/// delivery order).
///
/// The broken model instead ships a remove as an element-keyed
/// instruction `broken_remove` that, at join time, deletes whatever
/// add dots with that key the receiver happens to hold *right now*.
/// That makes the join depend on the receiver's current state -- it
/// is not a pure lattice value, so applying `[add, broken_remove]`
/// differs from `[broken_remove, add]`. This is precisely a
/// non-join-irreducible mutator that drops the causal context.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Delta {
    /// Add dots carried by this delta.
    adds: BTreeSet<Dot>,
    /// Tombstoned dots carried by this delta (the causal context).
    removes: BTreeSet<Dot>,
    /// Broken-model-only: element keys whose currently-present add
    /// dots are deleted at join time. Always empty in the correct
    /// model.
    broken_remove: BTreeSet<u8>,
}

/// One replica's observed-remove set state: add dots and tombstoned
/// dots.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Default)]
pub struct Replica {
    adds: BTreeSet<Dot>,
    removes: BTreeSet<Dot>,
    /// The set of deltas this replica has delivered (by identity),
    /// used to state the SEC precondition "delivered the same set".
    delivered: BTreeSet<Delta>,
}

impl Replica {
    /// Join a delta into this replica. For the correct model this is
    /// a set-union of both dot sets -- the least-upper-bound of the
    /// lattice, independent of order. For the broken model, a
    /// `broken_remove` instruction destructively deletes the add dots
    /// with that key that are present *at this moment*, which is
    /// state-dependent and therefore order-dependent.
    fn join(&mut self, d: &Delta) {
        self.adds.extend(d.adds.iter().copied());
        self.removes.extend(d.removes.iter().copied());
        for &key in &d.broken_remove {
            let doomed: Vec<Dot> = self
                .adds
                .iter()
                .filter(|&&(_seq, k)| k == key)
                .copied()
                .collect();
            for dot in doomed {
                self.adds.remove(&dot);
            }
        }
        self.delivered.insert(d.clone());
    }

    /// The presence set: which element keys are visible. An element
    /// is present iff some add dot on that key is not tombstoned.
    fn present(&self) -> BTreeSet<u8> {
        let mut out = BTreeSet::new();
        for &dot in &self.adds {
            let (_seq, key) = dot;
            if !self.removes.contains(&dot) {
                out.insert(key);
            }
        }
        out
    }
}

/// Aggregated model state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DeltaState {
    /// Per-replica state.
    replicas: Vec<Replica>,
    /// Deltas the source has produced (the full history). The channel
    /// delivers from here; a replica converges once it has delivered
    /// them all.
    produced: Vec<Delta>,
    /// Remaining mutation budget (bounds the search).
    mutations_left: u8,
}

/// Delta-CRDT convergence model.
#[derive(Clone)]
pub struct DeltaCrdt {
    /// Replica count.
    pub replicas: usize,
    /// Number of local mutations allowed before the channel drains.
    pub mutations: u8,
    /// When true, the remove mutator drops causal context (negative
    /// control). The checker should find an SEC counterexample.
    pub broken: bool,
}

/// Actions: mutate at the source, or deliver a produced delta to a
/// replica (possibly out of order, possibly a duplicate).
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Replica 0 adds element `key` (mints a fresh dot, buffers the
    /// add delta into `produced`).
    Add(u8),
    /// Replica 0 removes element `key` (buffers a remove delta).
    Remove(u8),
    /// Deliver `produced[idx]` to replica `r`. Reordering and
    /// duplication are modelled by allowing any produced index to be
    /// delivered to any replica at any time.
    Deliver(usize, usize),
}

impl DeltaCrdt {
    /// The correct-model tombstone dots for removing `key` from the
    /// source (replica 0): every add dot on that key the source has.
    fn observed_dots(source: &Replica, key: u8) -> BTreeSet<Dot> {
        source
            .adds
            .iter()
            .filter(|&&(_seq, k)| k == key)
            .copied()
            .collect()
    }
}

impl Model for DeltaCrdt {
    type State = DeltaState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![DeltaState {
            replicas: vec![Replica::default(); self.replicas],
            produced: Vec::new(),
            mutations_left: self.mutations,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Mutations happen at the source (replica 0) while budget
        // allows. Two element keys keep the space finite while still
        // exercising add/remove interleavings.
        if state.mutations_left > 0 {
            for key in 0..2u8 {
                actions.push(Action::Add(key));
                actions.push(Action::Remove(key));
            }
        }
        // Deliver any produced delta to any replica (reorder +
        // duplicate). Delivering to the source is a no-op join, so
        // skip replica 0 as a delivery target.
        for idx in 0..state.produced.len() {
            for r in 1..self.replicas {
                actions.push(Action::Deliver(idx, r));
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::Add(key) => {
                if s.mutations_left == 0 {
                    return None;
                }
                // Mint a fresh, unique dot. The sequence is the
                // produced-history length, so every add gets a
                // distinct dot even for the same element key. The
                // element key is carried in the dot's second field.
                let seq = u8::try_from(s.produced.len()).ok()?;
                let dot: Dot = (seq, key);
                let delta = Delta {
                    adds: BTreeSet::from([dot]),
                    removes: BTreeSet::new(),
                    broken_remove: BTreeSet::new(),
                };
                s.replicas[0].join(&delta);
                s.produced.push(delta);
                s.mutations_left -= 1;
                Some(s)
            }
            Action::Remove(key) => {
                if s.mutations_left == 0 {
                    return None;
                }
                let observed = Self::observed_dots(&s.replicas[0], key);
                if observed.is_empty() {
                    // No-op remove: prune the transition.
                    return None;
                }
                let delta = if self.broken {
                    // NEGATIVE CONTROL: ship an element-keyed removal
                    // that drops the causal context (the observed
                    // dots). At join time it deletes whatever add dots
                    // with this key the receiver currently holds, so
                    // the delta is not a pure lattice value: applying
                    // [add, remove] differs from [remove, add]. Two
                    // replicas that deliver the SAME set of deltas in
                    // different orders then diverge.
                    Delta {
                        adds: BTreeSet::new(),
                        removes: BTreeSet::new(),
                        broken_remove: BTreeSet::from([key]),
                    }
                } else {
                    // Correct: tombstone exactly the observed dots.
                    // Joining this is a pure set-union, order- and
                    // duplicate-independent.
                    Delta {
                        adds: BTreeSet::new(),
                        removes: observed,
                        broken_remove: BTreeSet::new(),
                    }
                };
                s.replicas[0].join(&delta);
                s.produced.push(delta);
                s.mutations_left -= 1;
                Some(s)
            }
            Action::Deliver(idx, r) => {
                let delta = s.produced.get(idx)?.clone();
                if s.replicas[r].delivered.contains(&delta) {
                    // Duplicate delivery: idempotent join. Prune only
                    // if it changes nothing (it never does), so the
                    // checker still explores duplicate paths once.
                    let before = s.replicas[r].clone();
                    s.replicas[r].join(&delta);
                    if s.replicas[r] == before {
                        return None;
                    }
                    return Some(s);
                }
                s.replicas[r].join(&delta);
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Strong eventual consistency: any two replicas that have
            // delivered the same set of deltas hold equal presence
            // sets. This is the core delta-CRDT guarantee.
            Property::<Self>::always("strong eventual consistency", |_, s| {
                for i in 0..s.replicas.len() {
                    for j in (i + 1)..s.replicas.len() {
                        if s.replicas[i].delivered == s.replicas[j].delivered
                            && s.replicas[i].present() != s.replicas[j].present()
                        {
                            return false;
                        }
                    }
                }
                true
            }),
            // Monotonicity: a replica's dot set never shrinks (join
            // only unions). Stated as: every delivered add dot stays
            // present in the adds set.
            Property::<Self>::always("dot set is monotone", |_, s| {
                s.replicas.iter().all(|r| {
                    r.delivered
                        .iter()
                        .flat_map(|d| d.adds.iter())
                        .all(|dot| r.adds.contains(dot))
                })
            }),
            // Convergence reachable: some state has a replica other
            // than the source holding the source's presence set (not
            // vacuously converged).
            Property::<Self>::sometimes("convergence reachable", |_, s| {
                s.replicas.len() > 1
                    && s.produced.len() >= 2
                    && s.replicas[1].present() == s.replicas[0].present()
                    && !s.replicas[1].delivered.is_empty()
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    /// Correct model: SEC and the lattice invariants hold across all
    /// reachable states of a small lossy/reordering/duplicating
    /// channel.
    #[test]
    fn correct_model_is_strongly_eventually_consistent() {
        let checker = DeltaCrdt {
            replicas: 3,
            mutations: 3,
            broken: false,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    /// Negative control: the broken (non-join-irreducible) remove
    /// mutator makes replicas diverge; the checker must find an SEC
    /// counterexample. If it does not, the model is toothless and this
    /// test fails.
    #[test]
    fn broken_mutator_violates_sec() {
        let checker = DeltaCrdt {
            replicas: 3,
            mutations: 3,
            broken: true,
        }
        .checker()
        .spawn_bfs()
        .join();
        // A discovery for an `always` property is a counterexample:
        // the broken mutator diverges, proving the model has teeth.
        assert!(
            checker.discovery("strong eventual consistency").is_some(),
            "expected an SEC counterexample from the broken mutator"
        );
    }
}
