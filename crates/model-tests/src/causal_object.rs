//! Model of causal-context conflict resolution for opaque objects.
//!
//! Dyniak objects carry a per-object causal context so a write either
//! descends from the context the client read (and supersedes it) or is
//! concurrent with the stored value (a conflict). This model proves the
//! resolution rule the write path implements:
//!
//! * a write that causally dominates the stored value(s) replaces them;
//! * a write concurrent with a stored value is retained as a SIBLING
//!   when `allow_mult` is set (so no write is lost), or collapsed to one
//!   value by a deterministic tie-break otherwise;
//! * a causally-dominated write (an old, superseded write arriving late)
//!   never overwrites a newer value.
//!
//! Causality is modelled abstractly: each write carries a context that
//! is a set of the write ids it has observed (a dotted-version-vector
//! frontier). Write `a` dominates `b` iff `b`'s id is in `a`'s observed
//! set. Two writes are concurrent iff neither observed the other. This
//! is the same partial order ITC's `partial_cmp_event` realises; the
//! model checks the resolution logic, not the clock encoding.
//!
//! # Invariants
//!
//! * **No lost causal write** (`always`): after any interleaving of
//!   writes, the stored value set contains every write that is not
//!   causally dominated by some other stored write. Equivalently: a
//!   write is absent only if a strictly newer write superseded it.
//! * **Siblings retained under allow_mult** (`always`, allow_mult
//!   model): two concurrent writes are both present in the stored set.
//! * **Reachability** (`sometimes`): a state with two concurrent
//!   siblings stored is reachable (not vacuous).
//!
//! # Negative control
//!
//! [`CausalObject::lww_always`] resolves every conflict by keeping only
//! the highest-id write, even when two writes are concurrent. That
//! drops a concurrent write, so "no lost causal write" (in the
//! allow_mult sense) has a counterexample and the checker reports it,
//! proving the model catches a resolution rule that silently loses a
//! concurrent write.

use std::collections::BTreeSet;

use stateright::{Model, Property};

/// Number of client writes the model issues.
const WRITES: u8 = 3;

/// A stored value: its write id plus the set of write ids its author
/// had observed when it wrote (its causal context frontier).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Stored {
    /// This write's unique id.
    id: u8,
    /// Write ids this write causally descends from (observed-at-write).
    observed: BTreeSet<u8>,
}

impl Stored {
    /// `self` causally dominates `other` iff `other`'s id is in
    /// `self`'s observed frontier (self saw other, so self is newer).
    fn dominates(&self, other: &Stored) -> bool {
        self.observed.contains(&other.id)
    }

    /// Concurrent iff neither dominates the other.
    fn concurrent_with(&self, other: &Stored) -> bool {
        !self.dominates(other) && !other.dominates(self)
    }
}

/// The abstract state: the set of stored values for one key (a sibling
/// set), the full set of writes ever issued (ground truth for the
/// no-lost-write check), and how many writes have been issued so far.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    /// The stored sibling set (what the resolution rule kept).
    stored: BTreeSet<Stored>,
    /// Every write ever issued, retained by the model (NOT by the
    /// production code) as ground truth so the no-lost-write property
    /// can be checked against reality rather than against the rule.
    issued: BTreeSet<Stored>,
    /// Next write id to hand out.
    next_id: u8,
}

/// A modelled action: one client write that observed a chosen subset of
/// the ids currently stored (its read context), then wrote a fresh id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// A write observing `observed` (a subset of currently-stored ids),
    /// producing a new write with the next id.
    Write {
        /// Write ids the client had read (observed) before writing.
        observed: BTreeSet<u8>,
    },
}

/// Conflict-resolution strategy under test.
#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Correct: causally dominated values are dropped, concurrent
    /// values are retained as siblings.
    AllowMult,
    /// Negative control: always collapse to the highest id, dropping
    /// concurrent writes.
    LwwAlways,
}

/// The causal-object model.
#[derive(Clone, Debug)]
pub struct CausalObject {
    mode: Mode,
}

impl CausalObject {
    /// Correct model: sibling-retaining causal resolution.
    #[must_use]
    pub fn allow_mult() -> Self {
        Self {
            mode: Mode::AllowMult,
        }
    }

    /// Negative control: last-id-wins, drops concurrent writes.
    #[must_use]
    pub fn lww_always() -> Self {
        Self {
            mode: Mode::LwwAlways,
        }
    }

    /// Apply `w` to `stored` under the active resolution rule.
    fn resolve(&self, stored: &BTreeSet<Stored>, w: &Stored) -> BTreeSet<Stored> {
        match self.mode {
            Mode::AllowMult => {
                // Keep every stored value the new write does NOT
                // dominate; add the new write. A value the new write
                // dominates is causally superseded and dropped.
                let mut out: BTreeSet<Stored> =
                    stored.iter().filter(|s| !w.dominates(s)).cloned().collect();
                // The new write is retained unless something already
                // stored dominates it (a stale, late-arriving write).
                if !stored.iter().any(|s| s.dominates(w)) {
                    out.insert(w.clone());
                }
                out
            }
            Mode::LwwAlways => {
                // Collapse to a single highest-id value, ignoring
                // concurrency. This is the broken control.
                let mut all: BTreeSet<Stored> = stored.clone();
                all.insert(w.clone());
                let winner = all
                    .into_iter()
                    .max_by_key(|s| s.id)
                    .expect("at least the new write is present");
                let mut out = BTreeSet::new();
                out.insert(winner);
                out
            }
        }
    }
}

impl Model for CausalObject {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            stored: BTreeSet::new(),
            issued: BTreeSet::new(),
            next_id: 1,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.next_id > WRITES {
            return;
        }
        // A write may observe any subset of the currently-stored ids
        // (modelling a client that read some frontier before writing).
        // Enumerate all subsets of the stored id set.
        let ids: Vec<u8> = state.stored.iter().map(|s| s.id).collect();
        let subset_count = 1usize << ids.len();
        for mask in 0..subset_count {
            let mut observed = BTreeSet::new();
            for (bit, id) in ids.iter().enumerate() {
                if mask & (1 << bit) != 0 {
                    observed.insert(*id);
                }
            }
            actions.push(Action::Write { observed });
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let Action::Write { observed } = action;
        let w = Stored {
            id: state.next_id,
            observed,
        };
        let stored = self.resolve(&state.stored, &w);
        let mut issued = state.issued.clone();
        issued.insert(w.clone());
        Some(State {
            stored,
            issued,
            next_id: state.next_id + 1,
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: no stored value is causally dominated by another
            // stored value (the stored set is an antichain -- the causal
            // frontier). Under AllowMult this holds; the negative
            // control can store a lone highest-id value that a concurrent
            // write should have joined as a sibling, so the sibling-
            // retention property below fails for it.
            Property::<Self>::always("stored set is a causal antichain", |_, state| {
                for a in &state.stored {
                    for b in &state.stored {
                        if a != b && a.dominates(b) {
                            return false;
                        }
                    }
                }
                true
            }),
            // Safety: no lost concurrent write. Every write id that was
            // issued and is NOT dominated by some other issued write
            // must still be present. We reconstruct the issued set from
            // next_id and check the frontier is complete. This is the
            // property the LwwAlways control violates.
            Property::<Self>::always("no concurrent write is silently dropped", |_, state| {
                // Ground truth: a write is on the true causal frontier
                // iff no OTHER issued write dominates it. Every such
                // frontier write must be present in the stored set.
                // The AllowMult rule keeps exactly the frontier; the
                // LwwAlways control drops concurrent frontier writes, so
                // it violates this.
                for w in &state.issued {
                    let dominated = state.issued.iter().any(|o| o != w && o.dominates(w));
                    if !dominated && !state.stored.contains(w) {
                        return false;
                    }
                }
                true
            }),
            // Liveness/reachability: a state with two concurrent
            // siblings stored is reachable (proves the model is not
            // vacuously antichain-of-size-<=1).
            Property::<Self>::sometimes("two concurrent siblings coexist", |_, state| {
                let v: Vec<&Stored> = state.stored.iter().collect();
                v.iter()
                    .enumerate()
                    .any(|(i, a)| v.iter().skip(i + 1).any(|b| a.concurrent_with(b)))
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn allow_mult_retains_siblings_and_loses_nothing() {
        let checker = CausalObject::allow_mult().checker().spawn_bfs().join();
        checker.assert_properties();
    }

    #[test]
    fn lww_always_drops_a_concurrent_write() {
        // The negative control must FAIL "no concurrent write is
        // silently dropped": the checker finds a counterexample where
        // two concurrent writes collapse to one.
        let checker = CausalObject::lww_always().checker().spawn_bfs().join();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            checker.assert_properties();
        }));
        assert!(
            result.is_err(),
            "the LwwAlways control must violate a safety property (it drops \
             a concurrent write); if this passes the model has no teeth"
        );
    }

    /// Keep only the causal frontier of `set` (drop any element another
    /// element dominates) -- the operation `resolve_write` /
    /// `merge_sibling_sets` performs when folding replica states.
    fn frontier(set: &BTreeSet<Stored>) -> BTreeSet<Stored> {
        set.iter()
            .filter(|w| !set.iter().any(|o| o != *w && o.dominates(w)))
            .cloned()
            .collect()
    }

    #[test]
    fn read_coordination_merges_partial_replicas_to_the_full_frontier() {
        // Model read coordination: a set of writes is distributed across
        // R replicas, each replica holding an ARBITRARY subset (a
        // dropped fan-out, a lagging replica). A coordinated read merges
        // every replica's frontier; the result must equal the frontier
        // of ALL writes -- no concurrent write is missed regardless of
        // how the writes were scattered. This is the invariant the KV
        // read-coordination path (merge_sibling_sets across the replica
        // set) provides.
        //
        // Build a fixed set of concurrent + causally-ordered writes.
        let w = |id: u8, obs: &[u8]| Stored {
            id,
            observed: obs.iter().copied().collect(),
        };
        // 1 and 2 concurrent (neither observed the other); 3 descends
        // from 1 (observed 1); 4 concurrent with everything.
        let all: BTreeSet<Stored> = [w(1, &[]), w(2, &[]), w(3, &[1]), w(4, &[])]
            .into_iter()
            .collect();
        let full_frontier = frontier(&all);
        // The full frontier drops 1 (dominated by 3): {2, 3, 4}.
        assert_eq!(full_frontier.len(), 3);

        // Enumerate every way to split `all` across 3 replicas (each
        // write independently assigned to one of the 3), and confirm
        // merging the replicas' frontiers always reconstructs the full
        // frontier.
        let writes: Vec<Stored> = all.iter().cloned().collect();
        let r = 3usize;
        let assignments = r.pow(u32::try_from(writes.len()).expect("few writes"));
        for mask in 0..assignments {
            let mut replicas: Vec<BTreeSet<Stored>> = vec![BTreeSet::new(); r];
            let mut m = mask;
            for wr in &writes {
                let target = m % r;
                m /= r;
                replicas[target].insert(wr.clone());
            }
            // Coordinated read: union the replica frontiers, then take
            // the frontier of the union (what merge_sibling_sets does).
            let mut union: BTreeSet<Stored> = BTreeSet::new();
            for rep in &replicas {
                for s in frontier(rep) {
                    union.insert(s);
                }
            }
            let merged = frontier(&union);
            assert_eq!(
                merged, full_frontier,
                "coordinated read of a partial split (mask {mask}) must equal the full frontier"
            );
        }
    }
}
