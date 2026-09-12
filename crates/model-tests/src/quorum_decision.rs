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

/// Model of PR / PW / DW: a quorum over a distinguished SUBSET of the
/// replica responses, layered on top of the plain total-response
/// quorum (R / W).
///
/// Riak's primary-read (PR) and primary-write (PW) quorums require
/// that at least `subset_quorum` of the responses come from a
/// PRIMARY-owner replica, not merely that `quorum` total responses
/// arrived (which may include sloppy-quorum fallback stand-ins). The
/// durable-write quorum (DW) has the identical shape with "durable
/// ack" standing in for "primary ack". This model abstracts the
/// shared decision the production code makes in
/// `dyniak::server::check_read_quorum` (the `pr_floor` branch) and
/// `dyniak::server::check_write_quorum` (the `pw` and `dw` branches):
/// each of those is exactly "count total responses against `quorum`;
/// separately count SUBSET responses against `subset_quorum`; both
/// must clear" -- this model is that shared shape once, parameterized
/// by which counting rule is under test.
///
/// # Invariants
///
/// * **Sound success** (`always`): a reported success never happens
///   with fewer than `quorum` total responses OR fewer than
///   `subset_quorum` SUBSET (primary / durable) responses.
/// * **Available at or above both quorums** (`always`, correct rule
///   only): whenever both thresholds are met, the correct rule
///   succeeds.
/// * **Reachability** (`sometimes`): a state exists where `quorum` is
///   met by total responses but `subset_quorum` is NOT met by subset
///   responses alone (a fallback/non-durable response propped up the
///   total) -- the scenario PR / PW / DW exists to reject.
///
/// # Negative control
///
/// [`SubsetQuorumModel::fallback_counts_rule`] uses the broken rule
/// the brief calls out by name: it counts EVERY response (subset or
/// not) toward the subset quorum, exactly the bug of "a fallback
/// response counts toward PR". It violates sound-success whenever a
/// non-subset response can single-handedly satisfy `subset_quorum`.
use stateright::{Model as SModel, Property as SProperty};

/// Replica count for the subset-quorum model.
const SN: u8 = 5;
/// Total quorum (majority of 5).
const S_QUORUM: u8 = SN / 2 + 1;
/// Subset quorum (PR/PW/DW threshold): 2 primaries/durables required.
const S_SUBSET_QUORUM: u8 = 2;

/// Counting rule under test for the subset-quorum model.
#[derive(Clone, Copy, Debug)]
enum SubsetRule {
    /// Correct: total acks gate `quorum`; SUBSET acks (and only
    /// subset acks) gate `subset_quorum` -- mirrors
    /// `check_read_quorum`'s `pr_floor` branch and
    /// `check_write_quorum`'s `pw` / `dw` branches exactly.
    Correct,
    /// Negative control: every ack (subset or not) counts toward
    /// `subset_quorum`, so a fallback / non-durable response can
    /// satisfy PR / PW / DW on its own.
    FallbackCounts,
}

/// Model of the PR / PW / DW subset-quorum decision.
#[derive(Clone, Debug)]
pub struct SubsetQuorumModel {
    rule: SubsetRule,
}

impl SubsetQuorumModel {
    /// The correct counting rule.
    #[must_use]
    pub fn correct() -> Self {
        Self {
            rule: SubsetRule::Correct,
        }
    }

    /// Negative control: a fallback (non-subset) response counts
    /// toward the subset quorum.
    #[must_use]
    pub fn fallback_counts_rule() -> Self {
        Self {
            rule: SubsetRule::FallbackCounts,
        }
    }

    fn succeeds(&self, total_acked: u8, subset_acked: u8, other_acked: u8) -> bool {
        if total_acked < S_QUORUM {
            return false;
        }
        match self.rule {
            SubsetRule::Correct => subset_acked >= S_SUBSET_QUORUM,
            SubsetRule::FallbackCounts => subset_acked + other_acked >= S_SUBSET_QUORUM,
        }
    }
}

/// State: replicas considered so far, split into subset (primary /
/// durable) and other (fallback / non-durable) responses.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubsetState {
    considered: u8,
    subset_acked: u8,
    other_acked: u8,
    total_acked: u8,
}

/// Action: the next replica's response.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SubsetAction {
    /// A subset (primary / durable) replica acks.
    SubsetAck,
    /// A non-subset (fallback / non-durable) replica acks.
    OtherAck,
    /// The replica does not respond.
    Silent,
}

impl SModel for SubsetQuorumModel {
    type State = SubsetState;
    type Action = SubsetAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![SubsetState {
            considered: 0,
            subset_acked: 0,
            other_acked: 0,
            total_acked: 0,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.considered < SN {
            actions.push(SubsetAction::SubsetAck);
            actions.push(SubsetAction::OtherAck);
            actions.push(SubsetAction::Silent);
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        if state.considered >= SN {
            return None;
        }
        let (subset_acked, other_acked) = match action {
            SubsetAction::SubsetAck => (state.subset_acked + 1, state.other_acked),
            SubsetAction::OtherAck => (state.subset_acked, state.other_acked + 1),
            SubsetAction::Silent => (state.subset_acked, state.other_acked),
        };
        Some(SubsetState {
            considered: state.considered + 1,
            subset_acked,
            other_acked,
            total_acked: subset_acked + other_acked,
        })
    }

    fn properties(&self) -> Vec<SProperty<Self>> {
        vec![
            SProperty::<Self>::always("success implies both quorums met", |model, state| {
                if state.considered < SN {
                    return true;
                }
                if model.succeeds(state.total_acked, state.subset_acked, state.other_acked) {
                    state.total_acked >= S_QUORUM && state.subset_acked >= S_SUBSET_QUORUM
                } else {
                    true
                }
            }),
            SProperty::<Self>::always(
                "both quorums met implies success (correct rule)",
                |model, state| {
                    if state.considered < SN {
                        return true;
                    }
                    if matches!(model.rule, SubsetRule::Correct)
                        && state.total_acked >= S_QUORUM
                        && state.subset_acked >= S_SUBSET_QUORUM
                    {
                        model.succeeds(state.total_acked, state.subset_acked, state.other_acked)
                    } else {
                        true
                    }
                },
            ),
            SProperty::<Self>::sometimes(
                "total quorum met but subset quorum not met is reachable",
                |_, state| {
                    state.considered == SN
                        && state.total_acked >= S_QUORUM
                        && state.subset_acked < S_SUBSET_QUORUM
                },
            ),
        ]
    }
}

#[cfg(test)]
mod subset_tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn correct_rule_is_sound_and_available() {
        let checker = SubsetQuorumModel::correct().checker().spawn_bfs().join();
        checker.assert_properties();
    }

    #[test]
    fn fallback_counts_control_violates_soundness() {
        let checker = SubsetQuorumModel::fallback_counts_rule()
            .checker()
            .spawn_bfs()
            .join();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            checker.assert_properties();
        }));
        assert!(
            result.is_err(),
            "a rule that counts a fallback / non-durable response toward \
             PR / PW / DW must violate sound-success; if this passes the \
             model has no teeth"
        );
    }

    #[test]
    fn a_below_subset_quorum_success_is_reachable_under_the_control() {
        // Demonstrate the exact failure mode: two OTHER (fallback)
        // acks alone satisfy the broken rule's subset quorum, with
        // zero subset (primary) acks -- precisely "a fallback response
        // counts toward PR".
        let model = SubsetQuorumModel::fallback_counts_rule();
        let state = SubsetState {
            considered: SN,
            subset_acked: 0,
            other_acked: S_SUBSET_QUORUM,
            total_acked: S_SUBSET_QUORUM.max(S_QUORUM),
        };
        assert!(model.succeeds(state.total_acked, state.subset_acked, state.other_acked));
        assert_eq!(
            state.subset_acked, 0,
            "zero primaries acked, yet the control succeeds"
        );
    }
}
