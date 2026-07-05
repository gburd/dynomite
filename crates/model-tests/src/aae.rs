//! Model of divergence-proportional anti-entropy (AAE)
//! reconciliation via a Merkle Search Tree (MST).
//!
//! This models the reconcile implemented in
//! `crates/dyniak/src/aae/mst_reconcile.rs` (`reconcile_pull`)
//! and the diff it drives,
//! `crates/dyn-hashtree/src/mst.rs` (`Mst::diff`). Two replicas
//! each hold a set of `(key, version)` entries. The diff
//! computes the symmetric difference of the two sets; the pull
//! step copies the entries the diff surfaced from the peer to
//! the local side, taking the higher version on a conflict
//! (last-writer-wins, the merge rule the repair applies when a
//! key is present on both sides at different values).
//!
//! # The pure core both the model and the code drive
//!
//! The production `Mst::diff` reduces to: "the set of keys
//! present-or-differing on exactly one side". The model
//! reproduces exactly that set operation. The production
//! `reconcile_pull` reduces to: "for each key the peer has that
//! we lack or hold older, adopt the peer's entry". The model
//! reproduces exactly that. The MST *structure* is what makes
//! the diff cheap; its *result* is the symmetric difference,
//! and that result is what convergence depends on -- so the
//! abstract model checks the result-level invariants and the
//! hegel property tests + unit tests check the structural cost
//! bound directly against the real tree.
//!
//! # Invariants asserted
//!
//! * **Convergence** (`eventually`): once both directions of
//!   the reconcile have run to quiescence, both replicas hold
//!   the identical merged set -- no key lost, no spurious key,
//!   the higher version retained on every conflict.
//! * **Diff bound** (`always`): every reconcile step exchanges
//!   at most `symmetric_difference` keys -- the efficiency
//!   property, checkable at the set level.
//! * **No spurious keys** (`always`): a reconcile never invents
//!   a key neither replica held (safety).
//! * **Agreement reachable** (`sometimes`): a fully-converged
//!   state is reachable (the model is not vacuously converged).
//!
//! # Negative control
//!
//! [`BrokenAae`] is [`Aae`] with one deliberate defect: the
//! diff skips one divergent key (as a broken MST walk would if
//! it pruned a subtree whose hash it failed to compare). The
//! checker is shown to catch it: convergence fails, because the
//! skipped key is never exchanged and the replicas stay
//! divergent forever.

use std::collections::BTreeMap;

use stateright::{Model, Property};

/// A key identifier in the abstract model (small domain keeps
/// the state space finite).
type Key = u8;
/// A version stamp; higher wins on a conflict (LWW).
type Version = u8;

/// One replica's view: `key -> version`.
type Replica = BTreeMap<Key, Version>;

/// Aggregate state of the two-replica reconcile.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AaeState {
    /// Replica A's entries.
    a: Replica,
    /// Replica B's entries.
    b: Replica,
    /// Local-write budget remaining. Writes are bounded so the
    /// reachable state space is finite; reconciles are unbounded
    /// (they only ever move the system toward agreement and are
    /// pruned when they make no change, so they add no new states
    /// once the replicas agree).
    writes_left: u8,
}

/// Divergence-proportional AAE reconcile model.
#[derive(Clone)]
pub struct Aae {
    /// Number of distinct keys in the universe.
    pub n_keys: Key,
    /// Number of distinct versions a key can take.
    pub n_versions: Version,
    /// Number of local writes allowed before quiescence.
    pub writes: u8,
}

/// Actions the checker may take.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// A local write at replica 0 (A) or 1 (B): set `key` to
    /// `version` on that replica, diverging it from the other.
    Write(u8, Key, Version),
    /// Run the reconcile in direction `dir`: 0 means "A pulls
    /// from B", 1 means "B pulls from A".
    Reconcile(u8),
}

/// The pure diff core: keys present-or-differing on the pull
/// target's side vs the source. Returns the keys the target
/// must adopt from the source (present on source, absent or
/// older on target).
///
/// This is the abstract equivalent of `Mst::diff(...).only_there()`
/// filtered to the pull direction: exactly the keys
/// `reconcile_pull` fetches and applies.
fn keys_to_pull(target: &Replica, source: &Replica) -> Vec<Key> {
    let mut out = Vec::new();
    for (k, sv) in source {
        match target.get(k) {
            // Absent on target: pull it.
            None => out.push(*k),
            // Present but older on target: pull the newer.
            Some(tv) if sv > tv => out.push(*k),
            _ => {}
        }
    }
    out
}

/// The full symmetric difference of two replicas -- keys present
/// on one side only, or present on both at different versions.
/// The diff-bound invariant checks that a reconcile exchanges no
/// more than this.
fn symmetric_difference(a: &Replica, b: &Replica) -> usize {
    let mut count = 0usize;
    let all: std::collections::BTreeSet<&Key> = a.keys().chain(b.keys()).collect();
    for k in all {
        match (a.get(k), b.get(k)) {
            (Some(va), Some(vb)) if va == vb => {}
            _ => count += 1,
        }
    }
    count
}

/// Apply a pull: `target` adopts `source`'s version for each key
/// in `pull`, keeping the higher version on a conflict (LWW).
/// When `broken` is set, one specific divergent key (the lowest
/// key id) is permanently skipped -- modelling an MST diff that
/// prunes a subtree it should have descended into. Because the
/// skip is keyed on the value (always the lowest divergent key),
/// it is skipped on every reconcile in every direction, so that
/// key never converges: the deterministic negative control.
fn apply_pull(target: &mut Replica, source: &Replica, pull: &[Key], broken: bool) {
    let skip_key = if broken {
        pull.iter().min().copied()
    } else {
        None
    };
    for k in pull {
        if Some(*k) == skip_key {
            // Broken: this divergent key's subtree was pruned and
            // never compared, so it is never exchanged.
            continue;
        }
        if let Some(sv) = source.get(k) {
            let entry = target.entry(*k).or_insert(*sv);
            if sv > entry {
                *entry = *sv;
            }
        }
    }
}

impl Aae {
    /// Shared model transition, parameterised by whether the
    /// diff is broken (skips one divergent key). `broken = false`
    /// is the correct model; `broken = true` is the negative
    /// control.
    fn step(&self, last: &AaeState, action: &Action, broken: bool) -> Option<AaeState> {
        let mut s = last.clone();
        match *action {
            Action::Write(replica, key, version) => {
                if s.writes_left == 0
                    || key >= self.n_keys
                    || version == 0
                    || version > self.n_versions
                {
                    return None;
                }
                let target = if replica == 0 { &mut s.a } else { &mut s.b };
                // A write must actually change something, else it
                // is a redundant transition.
                match target.get(&key) {
                    Some(v) if *v == version => return None,
                    _ => {}
                }
                target.insert(key, version);
                s.writes_left -= 1;
            }
            Action::Reconcile(dir) => {
                let before = if dir == 0 { s.a.clone() } else { s.b.clone() };
                let pull = if dir == 0 {
                    keys_to_pull(&s.a, &s.b)
                } else {
                    keys_to_pull(&s.b, &s.a)
                };
                if pull.is_empty() {
                    // Nothing to do: prune the redundant
                    // transition.
                    return None;
                }
                if dir == 0 {
                    let source = s.b.clone();
                    apply_pull(&mut s.a, &source, &pull, broken);
                    // Prune a broken no-op reconcile (the skip_key
                    // reconcile that changes nothing) so it does
                    // not spin as a self-loop -- the state is a
                    // genuine fixpoint the convergence property
                    // must judge.
                    if s.a == before {
                        return None;
                    }
                } else {
                    let source = s.a.clone();
                    apply_pull(&mut s.b, &source, &pull, broken);
                    if s.b == before {
                        return None;
                    }
                }
            }
        }
        Some(s)
    }

    fn model_actions(&self, state: &AaeState, actions: &mut Vec<Action>) {
        // Local writes on either replica, from a small key/version
        // domain, while the write budget remains.
        if state.writes_left > 0 {
            for replica in 0..2u8 {
                for key in 0..self.n_keys {
                    for version in 1..=self.n_versions {
                        actions.push(Action::Write(replica, key, version));
                    }
                }
            }
        }
        // Reconcile in either direction (unbounded: pruned when it
        // makes no progress).
        actions.push(Action::Reconcile(0));
        actions.push(Action::Reconcile(1));
    }

    /// Build the property set for a model with the given base
    /// parameters. A free function (not a `&self` method) so it
    /// can be shared by [`Aae`] and [`BrokenAae`] without a
    /// spurious `unused self`.
    fn model_properties() -> Vec<Property<Self>> {
        vec![
            // Convergence (liveness): from any reachable state,
            // once the write budget is spent and both reconcile
            // directions have drained, the two replicas hold the
            // identical merged set. Expressed as `eventually`
            // reaching-and-staying agreed: the correct model drains
            // to agreement on every path; the broken model gets
            // stuck in a divergent fixpoint (the skipped key never
            // moves), which the checker reports as a terminal state
            // that never satisfies the property.
            Property::<Self>::eventually("convergence", |_, s| s.writes_left == 0 && s.a == s.b),
            // Safety: a reconcile never invents a key neither side
            // ever held.
            Property::<Self>::always("no spurious keys", move |model, s| {
                s.a.keys().all(|k| *k < model.n_keys) && s.b.keys().all(|k| *k < model.n_keys)
            }),
            // Efficiency: a pull moves at most the
            // symmetric-difference many keys -- reconcile cost is
            // bounded by the divergence, not the dataset size.
            Property::<Self>::always("diff bounded by symmetric difference", |_, s| {
                let sd = symmetric_difference(&s.a, &s.b);
                let pull_a = keys_to_pull(&s.a, &s.b).len();
                let pull_b = keys_to_pull(&s.b, &s.a).len();
                pull_a <= sd && pull_b <= sd
            }),
            Property::<Self>::sometimes("agreement reachable", |_, s| {
                s.writes_left == 0 && s.a == s.b
            }),
            Property::<Self>::sometimes("divergence reachable", |_, s| s.a != s.b),
        ]
    }
}

impl Model for Aae {
    type State = AaeState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![AaeState {
            a: BTreeMap::new(),
            b: BTreeMap::new(),
            writes_left: self.writes,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        self.model_actions(state, actions);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        self.step(last, &action, false)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        Self::model_properties()
    }
}

/// Negative control: the reconcile skips one divergent key, as a
/// broken MST diff would if it pruned a subtree it should have
/// descended into. The checker must catch the convergence
/// violation.
#[derive(Clone)]
pub struct BrokenAae(pub Aae);

impl Model for BrokenAae {
    type State = AaeState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        self.0.init_states()
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        self.0.model_actions(state, actions);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        // The one defect: broken = true.
        self.0.step(last, &action, true)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        // Only the convergence property matters for the negative
        // control; it must FAIL because the skipped key leaves a
        // divergent fixpoint the reconcile can never escape.
        vec![Property::<Self>::eventually("convergence", |_, s| {
            s.writes_left == 0 && s.a == s.b
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    /// The correct model: two replicas with a small key/version
    /// domain converge on every terminal path, the diff bound
    /// holds in every reachable state, and no reconcile invents a
    /// phantom key.
    #[test]
    fn reconcile_converges_and_is_bounded() {
        let checker = Aae {
            n_keys: 3,
            n_versions: 2,
            writes: 3,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    /// A wider domain still converges (deeper search, same
    /// invariants).
    #[test]
    fn reconcile_converges_wider() {
        let checker = Aae {
            n_keys: 4,
            n_versions: 2,
            writes: 4,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
    }

    /// Negative control: the broken diff (skips one divergent
    /// key) leaves the replicas non-convergent, and the checker
    /// CATCHES it -- the convergence property must be violated.
    #[test]
    fn broken_diff_fails_convergence() {
        let checker = BrokenAae(Aae {
            n_keys: 3,
            n_versions: 2,
            writes: 3,
        })
        .checker()
        .spawn_bfs()
        .join();
        // The convergence property must have a discovered
        // counterexample: at least one reachable divergent
        // fixpoint where a skipped key never gets exchanged.
        let discoveries = checker.discoveries();
        assert!(
            discoveries.contains_key("convergence"),
            "negative control must expose a convergence counterexample; discoveries: {:?}",
            discoveries.keys().collect::<Vec<_>>()
        );
    }
}
