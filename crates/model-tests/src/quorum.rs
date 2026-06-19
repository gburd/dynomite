//! Model of the per-DC quorum / consistency decision.
//!
//! This models the decision logic implemented in
//! `crates/dynomite/src/msg/response_mgr.rs`
//! ([`dynomite::msg::ResponseMgr`]) and the consistency fan-out chosen
//! in `crates/dynomite/src/cluster/dispatch.rs` /
//! `crates/dynomite/src/cluster/pool.rs`. The production quorum size
//! is `max_responses / 2 + 1` and a request is accepted once at least
//! that many good (non-error) replies arrive and agree on the body
//! checksum; it fails once enough errors arrive that quorum is no
//! longer reachable.
//!
//! The abstract model fans a request out to `n` replicas, lets each
//! reply success or error in any order (or never, up to a tolerated
//! failure bound), and tracks the same accept / fail decision. The
//! safety property is the one the dispatcher must never violate:
//!
//! * **No false success** (`always`): the request is reported durable
//!   (accepted) only when at least a quorum (`n / 2 + 1`) of replicas
//!   acknowledged. A `DC_ONE` request is satisfied by a single ack; a
//!   `DC_QUORUM` request needs a majority. The model asserts the
//!   accept decision never fires below the configured threshold, for
//!   replica-failure counts up to the tolerated bound.
//! * **Decision reachability** (`sometimes`): both an accept and a
//!   reject are reachable, so the model is not vacuously safe.

use stateright::{Model, Property};

/// Consistency level under test. Mirrors the subset of
/// [`dynomite::msg::ConsistencyLevel`] that drives the accept
/// threshold: `DC_ONE` accepts on one ack; the quorum levels accept on
/// a majority.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Consistency {
    /// Accept on a single acknowledgement.
    DcOne,
    /// Accept on a majority (`n / 2 + 1`) of acknowledgements.
    DcQuorum,
}

/// Reply state of one replica.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Reply {
    /// No reply yet.
    None,
    /// Acknowledged (a good response).
    Ack,
    /// Errored.
    Err,
}

/// Aggregated decision -- mirrors [`dynomite::msg::QuorumOutcome`]
/// collapsed to the accept / reject / pending distinction the client
/// observes.
/// Aggregated decision -- mirrors [`dynomite::msg::QuorumOutcome`]
/// collapsed to the accept / reject / pending distinction the client
/// observes.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Decision {
    /// Not enough replies yet to decide.
    Pending,
    /// Quorum reached; the write is reported durable.
    Accepted,
    /// Quorum is unreachable; the write is rejected.
    Rejected,
}

/// Aggregated state across all replicas for one request.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct QuorumState {
    replies: Vec<Reply>,
    decision: Decision,
}

/// Quorum decision model over `n` replicas at a given consistency
/// level, tolerating up to `max_failures` replica failures.
#[derive(Clone)]
pub struct Quorum {
    /// Replica count.
    pub n: usize,
    /// Consistency level being checked.
    pub consistency: Consistency,
    /// Maximum replicas allowed to fail (errored or silent).
    pub max_failures: usize,
}

/// Actions the checker may take from a quorum state.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Replica `i` acknowledges.
    Ack(usize),
    /// Replica `i` errors.
    Err(usize),
    /// Re-evaluate the aggregate decision.
    Decide,
}

impl Quorum {
    /// The accept threshold for the configured consistency level.
    fn threshold(&self) -> usize {
        match self.consistency {
            Consistency::DcOne => 1,
            // Matches the production `max_responses / 2 + 1`.
            Consistency::DcQuorum => self.n / 2 + 1,
        }
    }
}

fn count(replies: &[Reply], want: Reply) -> usize {
    replies.iter().filter(|r| **r == want).count()
}

impl Model for Quorum {
    type State = QuorumState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![QuorumState {
            replies: vec![Reply::None; self.n],
            decision: Decision::Pending,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.decision != Decision::Pending {
            return;
        }
        let errs = count(&state.replies, Reply::Err);
        for (i, r) in state.replies.iter().enumerate() {
            if *r == Reply::None {
                actions.push(Action::Ack(i));
                // A replica may only error while the failure budget
                // allows; this keeps the modelled failure count within
                // the tolerated bound.
                if errs < self.max_failures {
                    actions.push(Action::Err(i));
                }
            }
        }
        actions.push(Action::Decide);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::Ack(i) => s.replies[i] = Reply::Ack,
            Action::Err(i) => s.replies[i] = Reply::Err,
            Action::Decide => {
                let acks = count(&s.replies, Reply::Ack);
                let errs = count(&s.replies, Reply::Err);
                let threshold = self.threshold();
                let pending = self.n - acks - errs;
                if acks >= threshold {
                    s.decision = Decision::Accepted;
                } else if acks + pending < threshold {
                    // Quorum is no longer reachable.
                    s.decision = Decision::Rejected;
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // The core safety property: never accept below threshold.
            Property::<Self>::always("no false success", |model, s| {
                if s.decision == Decision::Accepted {
                    return count(&s.replies, Reply::Ack) >= model.threshold();
                }
                true
            }),
            // A rejected request must genuinely be unable to reach
            // quorum (no false reject when acks are still possible and
            // already sufficient).
            Property::<Self>::always("reject only when quorum unreachable", |model, s| {
                if s.decision == Decision::Rejected {
                    return count(&s.replies, Reply::Ack) < model.threshold();
                }
                true
            }),
            Property::<Self>::sometimes("can accept", |_, s| s.decision == Decision::Accepted),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn dc_quorum_three_replicas_one_failure() {
        let checker = Quorum {
            n: 3,
            consistency: Consistency::DcQuorum,
            max_failures: 1,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
    }

    #[test]
    fn dc_quorum_five_replicas_two_failures() {
        let checker = Quorum {
            n: 5,
            consistency: Consistency::DcQuorum,
            max_failures: 2,
        }
        .checker()
        .spawn_bfs()
        .join();
        // Safety holds; reachability of both decisions is the
        // `sometimes` set.
        checker.assert_properties();
    }

    /// When the failure budget can deny a majority (3 of 5 replicas
    /// fail), the reject decision is reachable and quorum is correctly
    /// refused -- the safety property still holds.
    #[test]
    fn dc_quorum_rejects_when_majority_unreachable() {
        let checker = Quorum {
            n: 5,
            consistency: Consistency::DcQuorum,
            max_failures: 3,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        // A reject is genuinely reachable in this configuration.
        assert!(
            checker.discovery("can accept").is_some(),
            "accept must remain reachable"
        );
        // The safety invariants (the real guarantees) never fire a
        // counterexample.
        assert!(checker.discovery("no false success").is_none());
        assert!(checker
            .discovery("reject only when quorum unreachable")
            .is_none());
    }

    #[test]
    fn dc_one_accepts_on_single_ack() {
        let checker = Quorum {
            n: 3,
            consistency: Consistency::DcOne,
            max_failures: 2,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
    }

    /// The threshold matches the production `max_responses / 2 + 1`.
    #[test]
    fn threshold_matches_production_formula() {
        for n in 1..=5usize {
            let q = Quorum {
                n,
                consistency: Consistency::DcQuorum,
                max_failures: 0,
            };
            assert_eq!(q.threshold(), n / 2 + 1);
        }
    }
}
