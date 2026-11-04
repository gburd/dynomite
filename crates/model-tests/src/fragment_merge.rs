//! Model of multi-key request fragmentation and reply merging.
//!
//! The dispatcher (`dispatch_fragmented` in
//! `crates/dynomite/src/cluster/dispatch.rs`) splits a multi-key `MGET` by
//! the owning node of each key, one sub-request per distinct owner holding
//! that owner's keys in client order, and merges the sub-replies with
//! `redis_coalesce_fragments` (`crates/dynomite/src/proto/redis/coalesce.rs`):
//! for each key in client order it takes the next value from that key's
//! fragment reply.
//!
//! The model fixes a request of four keys over three owners (`OWNER_OF`)
//! and lets the fragment replies arrive in every order, each either a
//! value reply or an error.
//!
//! # Invariants
//!
//! * **Merged reply is the per-key reply in client order** (`always`):
//!   once every fragment answered with values, the merged reply equals
//!   `[value(k) for k in keys]`, whatever the arrival order.
//! * **Any error fails the whole reply** (`always`).
//! * **Reachability** (`sometimes`): a merged value reply after an
//!   out-of-order arrival, and an error reply, are both reachable.
//!
//! # Negative control
//!
//! [`FragmentMergeModel::arrival_order`] concatenates fragment values in
//! the order the replies arrived instead of walking keys in client order.
//! It violates the first invariant.

use stateright::{Model, Property};

/// The owner of each key of the request, in client order.
const OWNER_OF: [usize; 4] = [1, 0, 1, 2];
/// Distinct owners, i.e. fragments.
const FRAGMENTS: usize = 3;

/// The value a datastore returns for key `k`.
fn value(k: usize) -> u8 {
    u8::try_from(k).unwrap_or(u8::MAX) + 10
}

/// Fragment `f`'s keys in client order.
fn keys_of(f: usize) -> Vec<usize> {
    (0..OWNER_OF.len()).filter(|k| OWNER_OF[*k] == f).collect()
}

/// One fragment reply.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Arrival {
    fragment: usize,
    error: bool,
}

/// Model state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct State {
    arrivals: Vec<Arrival>,
    merged: Merged,
}

/// The client reply.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Merged {
    /// Not every fragment has answered.
    Pending,
    /// The merged values.
    Values(Vec<u8>),
    /// The error reply.
    Error,
}

/// The fragment-merge model.
#[derive(Clone, Debug)]
pub struct FragmentMergeModel {
    client_order: bool,
}

impl FragmentMergeModel {
    /// The production merge: walk keys in client order.
    #[must_use]
    pub fn client_order() -> Self {
        Self { client_order: true }
    }

    /// Negative control: concatenate replies in arrival order.
    #[must_use]
    pub fn arrival_order() -> Self {
        Self {
            client_order: false,
        }
    }

    fn merge(&self, arrivals: &[Arrival]) -> Option<Vec<u8>> {
        if arrivals.iter().any(|a| a.error) {
            return None;
        }
        // Each fragment reply carries its keys' values in client order.
        let replies: Vec<Vec<u8>> = (0..FRAGMENTS)
            .map(|f| keys_of(f).into_iter().map(value).collect())
            .collect();
        if self.client_order {
            let mut cursor = [0usize; FRAGMENTS];
            Some(
                OWNER_OF
                    .iter()
                    .map(|&f| {
                        let v = replies[f][cursor[f]];
                        cursor[f] += 1;
                        v
                    })
                    .collect(),
            )
        } else {
            Some(
                arrivals
                    .iter()
                    .flat_map(|a| replies[a.fragment].clone())
                    .collect(),
            )
        }
    }
}

fn expected() -> Vec<u8> {
    (0..OWNER_OF.len()).map(value).collect()
}

impl Model for FragmentMergeModel {
    type State = State;
    type Action = Arrival;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            arrivals: Vec::new(),
            merged: Merged::Pending,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Arrival>) {
        for fragment in 0..FRAGMENTS {
            if state.arrivals.iter().any(|a| a.fragment == fragment) {
                continue;
            }
            for error in [false, true] {
                actions.push(Arrival { fragment, error });
            }
        }
    }

    fn next_state(&self, state: &State, action: Arrival) -> Option<State> {
        let mut arrivals = state.arrivals.clone();
        arrivals.push(action);
        let merged = if arrivals.len() < FRAGMENTS {
            Merged::Pending
        } else {
            self.merge(&arrivals).map_or(Merged::Error, Merged::Values)
        };
        Some(State { arrivals, merged })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "merged reply is per-key in client order",
                |_, s: &State| match &s.merged {
                    Merged::Values(v) => *v == expected(),
                    _ => true,
                },
            ),
            Property::always(
                "any fragment error fails the reply",
                |_, s: &State| match &s.merged {
                    Merged::Error => s.arrivals.iter().any(|a| a.error),
                    Merged::Values(_) => !s.arrivals.iter().any(|a| a.error),
                    Merged::Pending => true,
                },
            ),
            Property::sometimes("out-of-order arrival merges", |_, s: &State| {
                matches!(s.merged, Merged::Values(_))
                    && s.arrivals.first().is_some_and(|a| a.fragment != 1)
            }),
            Property::sometimes("an error reply is reachable", |_, s: &State| {
                matches!(s.merged, Merged::Error)
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use stateright::Checker;

    use super::*;

    #[test]
    fn client_order_merge_holds_for_every_arrival_order() {
        FragmentMergeModel::client_order()
            .checker()
            .spawn_bfs()
            .join()
            .assert_properties();
    }

    #[test]
    fn negative_control_arrival_order_merge_is_caught() {
        let checker = FragmentMergeModel::arrival_order()
            .checker()
            .spawn_bfs()
            .join();
        assert!(checker
            .discovery("merged reply is per-key in client order")
            .is_some());
    }
}
