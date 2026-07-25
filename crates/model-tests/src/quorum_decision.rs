//! Model of quorum enforcement for reads and writes.
//!
//! A read must gather `R` responses and a write `W` acks from the
//! key's `N` replicas before it succeeds; below that it must fail
//! (unavailable) rather than return a possibly-stale or unreplicated
//! result. This model enumerates every reachable-subset of the replica
//! set and checks the decision the production quorum path makes:
//!
//! * an operation SUCCEEDS iff the number of reachable replicas is at
//!   least the required quorum;
//! * it FAILS otherwise.
//!
//! # Invariants
//!
//! * **Sound success** (`always`): whenever the model reports success,
//!   at least `quorum` replicas were reachable -- a success never
//!   happens below quorum.
//! * **Available above quorum** (`always`): whenever at least `quorum`
//!   replicas are reachable, the operation succeeds -- it never fails
//!   spuriously above quorum.
//! * **Reachability** (`sometimes`): both a success state and a
//!   below-quorum failure state are reachable (not vacuous).
//!
//! # Negative control
//!
//! [`QuorumModel::one_wins`] uses a broken rule that succeeds as soon
//! as ONE replica is reachable, ignoring the quorum. It violates
//! "sound success" (it reports success with a single reachable replica
//! when `quorum > 1`), so the checker catches a rule that does not
//! actually enforce the quorum.

use stateright::{Model, Property};

/// Replica count.
const N: u8 = 5;
/// Required quorum (a strict majority of N = 3).
const QUORUM: u8 = N / 2 + 1;

/// Decision rule under test.
#[derive(Clone, Copy, Debug)]
enum Rule {
    /// Correct: succeed iff `reachable >= quorum`.
    Quorum,
    /// Negative control: succeed as soon as one replica is reachable.
    OneWins,
}

/// The quorum model.
#[derive(Clone, Debug)]
pub struct QuorumModel {
    rule: Rule,
}

impl QuorumModel {
    /// Correct quorum rule.
    #[must_use]
    pub fn quorum() -> Self {
        Self { rule: Rule::Quorum }
    }

    /// Negative control: one reachable replica is enough.
    #[must_use]
    pub fn one_wins() -> Self {
        Self {
            rule: Rule::OneWins,
        }
    }

    /// Does the rule report success for `reachable` reachable replicas?
    fn succeeds(&self, reachable: u8) -> bool {
        match self.rule {
            Rule::Quorum => reachable >= QUORUM,
            Rule::OneWins => reachable >= 1,
        }
    }
}

/// State: how many replicas the model has marked reachable so far, and
/// how many it has considered. The model steps through each replica
/// deciding reachable/unreachable, then the terminal state exposes the
/// decision.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    /// Replicas considered so far.
    considered: u8,
    /// Reachable replicas so far.
    reachable: u8,
}

/// Action: mark the next replica reachable or not.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// The next replica is reachable.
    Reachable,
    /// The next replica is unreachable (partitioned / down).
    Unreachable,
}

impl Model for QuorumModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            considered: 0,
            reachable: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.considered < N {
            actions.push(Action::Reachable);
            actions.push(Action::Unreachable);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        if state.considered >= N {
            return None;
        }
        let reachable = match action {
            Action::Reachable => state.reachable + 1,
            Action::Unreachable => state.reachable,
        };
        Some(State {
            considered: state.considered + 1,
            reachable,
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Sound success: a reported success never happens below the
            // quorum. Checked on terminal states (all replicas
            // considered). The OneWins control violates this.
            Property::<Self>::always("success implies quorum reachable", |model, state| {
                if state.considered < N {
                    return true;
                }
                if model.succeeds(state.reachable) {
                    state.reachable >= QUORUM
                } else {
                    true
                }
            }),
            // Available above quorum: at or above quorum, the correct
            // rule always succeeds. (Only asserted for the correct rule;
            // the control may over-succeed, which the property above
            // catches.)
            Property::<Self>::always("quorum reachable implies success", |model, state| {
                if state.considered < N {
                    return true;
                }
                if matches!(model.rule, Rule::Quorum) && state.reachable >= QUORUM {
                    model.succeeds(state.reachable)
                } else {
                    true
                }
            }),
            // Reachability: a below-quorum terminal state exists.
            Property::<Self>::sometimes("a below-quorum state is reachable", |_, state| {
                state.considered == N && state.reachable < QUORUM
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn quorum_rule_is_sound_and_available() {
        let checker = QuorumModel::quorum().checker().spawn_bfs().join();
        checker.assert_properties();
    }

    #[test]
    fn one_wins_control_violates_soundness() {
        let checker = QuorumModel::one_wins().checker().spawn_bfs().join();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            checker.assert_properties();
        }));
        assert!(
            result.is_err(),
            "the one-wins control must violate sound-success (it reports \
             success below quorum); if this passes the model has no teeth"
        );
    }
}
