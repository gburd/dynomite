//! Model of which replica answers a `DC_ONE` request.
//!
//! A `DC_ONE` write is sent to the key's owner in every rack of the local
//! datacenter so each rack holds a replica, but only the replica in the
//! coordinating node's own rack answers the client; the other racks'
//! replies are swallowed. The dispatcher (`coalesce_actor` in
//! `crates/dynomite/src/cluster/dispatch.rs`, the `answering` replica)
//! applies that rule before the vote.
//!
//! The model lets each of three racks' replicas answer OK or an error
//! (a remote rack's peer link may be down: "Peer Node is not connected"),
//! in any order. Rack 0 is the coordinator's own rack.
//!
//! # Invariants
//!
//! * **Own-rack answer** (`always`): once the client is answered, the
//!   answer is the own rack's reply. A write the own rack applied is never
//!   reported failed because a remote rack's link was down, and a write the
//!   own rack refused is never reported done.
//! * **Liveness** (`eventually`): every run answers the client.
//! * **Reachability** (`sometimes`): an answer while a remote rack failed.
//!
//! # Negative control
//!
//! [`DcOneModel::first_reply`] answers with whichever reply arrives first
//! (the defect: a fast "Peer Node is not connected" from a remote rack
//! failed a write the local rack had applied). It violates "own-rack
//! answer".

use stateright::{Model, Property};

/// Racks in the local datacenter; rack 0 is the coordinator's.
const RACKS: usize = 3;

/// One rack replica's reply.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Reply {
    /// The write was applied.
    Ok,
    /// An error (datastore down, peer not connected).
    Err,
}

/// Model state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    /// What each rack will reply (fixed at the start).
    outcome: [Reply; RACKS],
    /// Racks whose reply has reached the coordinator.
    arrived: [bool; RACKS],
    /// The client's answer.
    answer: Option<Reply>,
}

/// Actions.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Action {
    /// Fix every rack's reply.
    Start([Reply; RACKS]),
    /// Rack `n`'s reply arrives.
    Arrive(usize),
}

/// The `DC_ONE` answer model.
#[derive(Clone, Debug)]
pub struct DcOneModel {
    own_rack_only: bool,
}

impl DcOneModel {
    /// The production rule: only the own rack answers.
    #[must_use]
    pub fn own_rack() -> Self {
        Self {
            own_rack_only: true,
        }
    }

    /// Negative control: the first reply to arrive answers.
    #[must_use]
    pub fn first_reply() -> Self {
        Self {
            own_rack_only: false,
        }
    }
}

impl Model for DcOneModel {
    type State = Option<State>;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![None]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        match state {
            None => {
                for a in [Reply::Ok, Reply::Err] {
                    for b in [Reply::Ok, Reply::Err] {
                        for c in [Reply::Ok, Reply::Err] {
                            actions.push(Action::Start([a, b, c]));
                        }
                    }
                }
            }
            Some(s) => {
                for n in 0..RACKS {
                    if !s.arrived[n] {
                        actions.push(Action::Arrive(n));
                    }
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        match (state, action) {
            (None, Action::Start(outcome)) => Some(Some(State {
                outcome,
                arrived: [false; RACKS],
                answer: None,
            })),
            (Some(s), Action::Arrive(n)) => {
                let mut s = s.clone();
                s.arrived[n] = true;
                let answers = if self.own_rack_only { n == 0 } else { true };
                if s.answer.is_none() && answers {
                    s.answer = Some(s.outcome[n]);
                }
                Some(Some(s))
            }
            _ => None,
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("own-rack answer", |_, s: &Option<State>| {
                s.as_ref()
                    .and_then(|s| s.answer.map(|a| a == s.outcome[0]))
                    .unwrap_or(true)
            }),
            Property::eventually("client answered", |_, s: &Option<State>| {
                s.as_ref().is_some_and(|s| s.answer.is_some())
            }),
            Property::sometimes(
                "answered while a remote rack failed",
                |_, s: &Option<State>| {
                    s.as_ref().is_some_and(|s| {
                        s.answer == Some(Reply::Ok) && s.outcome[1..].contains(&Reply::Err)
                    })
                },
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn dc_one_answers_with_the_own_rack_reply() {
        DcOneModel::own_rack()
            .checker()
            .spawn_bfs()
            .join()
            .assert_properties();
    }

    #[test]
    fn negative_control_first_reply_is_caught() {
        let checker = DcOneModel::first_reply().checker().spawn_bfs().join();
        assert!(
            checker.discovery("own-rack answer").is_some(),
            "the first-reply rule must violate own-rack answer"
        );
    }
}
