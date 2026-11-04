//! Model of reply classification before a quorum vote.
//!
//! The coalescer (`crates/dynomite/src/proto/redis/coalesce.rs`) groups
//! replica replies by an equivalence key of `(is_error, payload bytes)`
//! and, under `DC_SAFE_QUORUM`, answers only when every local reply falls
//! in one group. A reply reaches the coalescer either already parsed (the
//! local datastore) or as the raw payload of a peer's dnode frame. The
//! dispatcher (`classify_reply` in `crates/dynomite/src/cluster/dispatch.rs`)
//! runs raw replies through the datastore's response parser first, so the
//! key depends only on the payload, never on which path delivered it.
//!
//! The model lets each of three local replicas answer with one of a small
//! set of payloads (two values, one error) over either path, in any order.
//!
//! # Invariants
//!
//! * **Agreement is accepted** (`always`): once every replica has answered
//!   and all payloads are byte-identical, the decision is Ready.
//! * **Disagreement is rejected** (`always`): once every replica has
//!   answered and the payloads differ, the decision is Divergent.
//! * **Reachability** (`sometimes`): Ready on identical error replies over
//!   mixed paths, and Divergent, are both reachable.
//!
//! # Negative control
//!
//! [`ReplyClassModel::unclassified`] keys raw peer replies with
//! `is_error = false` (the defect `classify_reply` fixed: three identical
//! `-NOSCRIPT` replies, one local and two from peers, voted as two groups
//! and a `DC_SAFE_QUORUM` `EVALSHA` failed). It violates "agreement is
//! accepted".

use stateright::{Model, Property};

/// Local replicas voting.
const REPLICAS: usize = 3;

/// A reply payload.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Payload {
    /// A bulk value.
    ValueA,
    /// A different bulk value.
    ValueB,
    /// An error reply (`-NOSCRIPT ...`).
    Error,
}

impl Payload {
    const ALL: [Payload; 3] = [Payload::ValueA, Payload::ValueB, Payload::Error];

    fn is_error(self) -> bool {
        matches!(self, Payload::Error)
    }
}

/// How a reply reached the coalescer.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Path {
    /// The local datastore: parsed, `is_error` set by the parser.
    Local,
    /// A peer's dnode frame: raw bytes, type unknown until classified.
    Peer,
}

/// The equivalence key the vote groups by.
type Key = (bool, Payload);

/// One answered replica.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Answer {
    payload: Payload,
    path: Path,
}

/// Coalescer decision.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Decision {
    /// Waiting for more replies.
    Pending,
    /// All local replies agreed; the reply is sent.
    Ready,
    /// Local replies disagreed; an error is sent.
    Divergent,
}

/// State: the answers so far and the decision.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    answers: Vec<Answer>,
    decision: Decision,
}

/// The reply-classification model.
#[derive(Clone, Debug)]
pub struct ReplyClassModel {
    classify: bool,
}

impl ReplyClassModel {
    /// Raw replies are classified before they vote (the production rule).
    #[must_use]
    pub fn classified() -> Self {
        Self { classify: true }
    }

    /// Negative control: raw peer replies vote with `is_error` unset.
    #[must_use]
    pub fn unclassified() -> Self {
        Self { classify: false }
    }

    fn key(&self, a: Answer) -> Key {
        let is_error = match a.path {
            Path::Local => a.payload.is_error(),
            Path::Peer if self.classify => a.payload.is_error(),
            Path::Peer => false,
        };
        (is_error, a.payload)
    }

    fn decide(&self, answers: &[Answer]) -> Decision {
        if answers.len() < REPLICAS {
            return Decision::Pending;
        }
        let first = self.key(answers[0]);
        if answers.iter().all(|a| self.key(*a) == first) {
            Decision::Ready
        } else {
            Decision::Divergent
        }
    }
}

fn all_same_payload(answers: &[Answer]) -> bool {
    answers.windows(2).all(|w| w[0].payload == w[1].payload)
}

impl Model for ReplyClassModel {
    type State = State;
    type Action = Answer;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            answers: Vec::new(),
            decision: Decision::Pending,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Answer>) {
        if state.decision != Decision::Pending {
            return;
        }
        // Exactly one replica is the local node; the rest are peers.
        let local_used = state.answers.iter().any(|a| a.path == Path::Local);
        for payload in Payload::ALL {
            for path in [Path::Local, Path::Peer] {
                if path == Path::Local && local_used {
                    continue;
                }
                actions.push(Answer { payload, path });
            }
        }
    }

    fn next_state(&self, state: &State, action: Answer) -> Option<State> {
        let mut answers = state.answers.clone();
        answers.push(action);
        let decision = self.decide(&answers);
        Some(State { answers, decision })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("agreement is accepted", |_, s: &State| {
                s.answers.len() < REPLICAS
                    || !all_same_payload(&s.answers)
                    || s.decision == Decision::Ready
            }),
            Property::always("disagreement is rejected", |_, s: &State| {
                s.answers.len() < REPLICAS
                    || all_same_payload(&s.answers)
                    || s.decision == Decision::Divergent
            }),
            Property::sometimes(
                "identical errors over mixed paths are accepted",
                |_, s: &State| {
                    s.decision == Decision::Ready
                        && s.answers.iter().all(|a| a.payload == Payload::Error)
                        && s.answers.iter().any(|a| a.path == Path::Local)
                        && s.answers.iter().any(|a| a.path == Path::Peer)
                },
            ),
            Property::sometimes("divergence is reachable", |_, s: &State| {
                s.decision == Decision::Divergent
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use stateright::Checker;

    use super::*;

    #[test]
    fn classified_replies_vote_by_payload() {
        let c = ReplyClassModel::classified().checker().spawn_bfs().join();
        c.assert_properties();
    }

    #[test]
    fn negative_control_unclassified_peer_errors_look_divergent() {
        let c = ReplyClassModel::unclassified().checker().spawn_bfs().join();
        assert!(
            c.discovery("agreement is accepted").is_some(),
            "the checker must catch raw peer replies voting with is_error unset"
        );
    }
}
