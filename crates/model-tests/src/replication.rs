//! Model of the dyniak cross-node replica fan-out.
//!
//! This models the replication routing wired into `dynomited` by the
//! sending side in `crates/dyniak/src/router.rs`
//! (`RoutingHooks` / `PeerOutbound::dispatch`) and the receive side in
//! `crates/dyniak/src/replica_apply.rs` (`ReplicaApplier`). A client
//! write to a key is handled by the node that received it (the
//! coordinator): it persists locally and forwards a `PeerOp::Put` to
//! each of the other replicas on the key's preference list. Each
//! replica applies the op to its LOCAL store and, crucially, does NOT
//! re-forward it -- a replica write fans out exactly once.
//!
//! The abstract state machine drives the actual decision the code
//! runs: given a preference list of `n_val` replicas, the coordinator
//! emits exactly one forward per non-coordinator replica, and an
//! applied forward is terminal. Messages may be delivered in any
//! order and (in a lossy variant) dropped; the model asserts:
//!
//! * **Bounded fan-out** (`always`, safety): the total number of
//!   applies triggered by a single client write never exceeds
//!   `n_val` -- the coordinator's local apply plus one apply per
//!   forwarded replica. A replica never re-forwards, so there is no
//!   fan-out storm and no infinite loop. This is the invariant the
//!   "terminal, local-only" contract guarantees.
//! * **Convergence under reliable delivery** (`eventually`,
//!   liveness): when every forward is delivered, every replica on the
//!   preference list holds the written value.
//!
//! The negative control replaces the terminal applier with one that
//! re-forwards each applied op to the other replicas (the bug the
//! "does NOT re-forward" contract forbids). Under that rule the apply
//! count grows past `n_val` and the checker reports the bounded-fan-out
//! violation, proving the model has teeth.

use std::collections::BTreeMap;

use stateright::{Model, Property};

/// A replica (peer) id. Small domain so the state space is finite.
type Peer = u8;

/// One forwarded replica op in flight from the coordinator (or, in the
/// broken variant, from a re-forwarding replica) to a destination peer.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Forward {
    /// Destination replica.
    dst: Peer,
    /// The value being replicated (a single opaque version here).
    value: u8,
}

/// Model state: which replicas have applied the value, the forwards
/// still in flight, and the running count of applies for the single
/// client write under test.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FanoutState {
    /// Per-replica applied value, if any.
    applied: BTreeMap<Peer, u8>,
    /// Forwards awaiting delivery.
    in_flight: Vec<Forward>,
    /// Total applies performed so far (coordinator + replicas).
    apply_count: u32,
    /// Whether the coordinator has run its local apply + fan-out.
    coordinated: bool,
}

/// A step the checker may take.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// The coordinator applies locally and fans out to the replicas.
    Coordinate,
    /// Deliver the in-flight forward at `idx` (the destination applies
    /// it; a correct applier stops, a broken one re-forwards).
    Deliver(usize),
    /// Drop the in-flight forward at `idx` (lossy delivery).
    Drop(usize),
}

/// Replica fan-out model.
#[derive(Clone)]
pub struct Fanout {
    /// The coordinator (the node that received the client write).
    pub coordinator: Peer,
    /// The preference list for the key (includes the coordinator).
    pub preference: Vec<Peer>,
    /// The value the client writes.
    pub value: u8,
    /// When true, an applied forward is re-forwarded to every other
    /// replica (the negative control -- the forbidden behaviour).
    pub broken: bool,
    /// When true, forwards may be dropped (models fire-and-forget best
    /// effort). Convergence is only asserted in the reliable variant.
    pub lossy: bool,
}

impl Fanout {
    /// `n_val`: the number of replicas the write must reach.
    fn n_val(&self) -> u32 {
        u32::try_from(self.preference.len()).unwrap_or(u32::MAX)
    }

    /// The replicas other than `from` on the preference list.
    fn others(&self, from: Peer) -> Vec<Peer> {
        self.preference
            .iter()
            .copied()
            .filter(|&p| p != from)
            .collect()
    }
}

/// A hard cap on the broken variant's re-forwarding so the state space
/// stays finite. Once the apply count is a small multiple past `n_val`,
/// the safety violation has already been reached and further
/// re-forwarding adds nothing but unbounded states.
fn model_cap(model: &Fanout) -> u32 {
    model.n_val() * 2
}

impl Model for Fanout {
    type State = FanoutState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![FanoutState {
            applied: BTreeMap::new(),
            in_flight: Vec::new(),
            apply_count: 0,
            coordinated: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if !state.coordinated {
            actions.push(Action::Coordinate);
        }
        for idx in 0..state.in_flight.len() {
            actions.push(Action::Deliver(idx));
            if self.lossy {
                actions.push(Action::Drop(idx));
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::Coordinate => {
                if s.coordinated {
                    return None;
                }
                s.coordinated = true;
                // The coordinator applies locally...
                s.applied.insert(self.coordinator, self.value);
                s.apply_count += 1;
                // ...and forwards to every other replica exactly once.
                for dst in self.others(self.coordinator) {
                    s.in_flight.push(Forward {
                        dst,
                        value: self.value,
                    });
                }
                Some(s)
            }
            Action::Deliver(idx) => {
                let fwd = s.in_flight.get(idx)?.clone();
                s.in_flight.remove(idx);
                // The destination applies to its local store.
                s.applied.insert(fwd.dst, fwd.value);
                s.apply_count += 1;
                if self.broken {
                    // The forbidden behaviour: re-forward to the other
                    // replicas. A correct applier is terminal and does
                    // nothing more here. We stop generating successors
                    // once the apply count has already exceeded n_val:
                    // the safety violation is reached, and capping the
                    // re-forward keeps the state space finite (an
                    // unbounded re-forward loop would never terminate).
                    if s.apply_count <= model_cap(self) {
                        for dst in self.others(fwd.dst) {
                            s.in_flight.push(Forward {
                                dst,
                                value: fwd.value,
                            });
                        }
                    }
                }
                Some(s)
            }
            Action::Drop(idx) => {
                if idx >= s.in_flight.len() {
                    return None;
                }
                s.in_flight.remove(idx);
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Safety: the total apply count never exceeds n_val. A
            // correct fan-out applies once at the coordinator and once
            // per forwarded replica; a replica never re-forwards, so
            // the count is bounded by the preference-list size. The
            // broken (re-forwarding) applier blows past this bound.
            Property::<Self>::always("bounded fan-out", |model, s| s.apply_count <= model.n_val()),
            // Liveness (reliable delivery only): once all forwards are
            // delivered, every replica on the preference list holds the
            // value.
            Property::<Self>::eventually("convergence", |model, s| {
                if model.lossy {
                    // In the lossy variant convergence is best-effort
                    // (anti-entropy reconciles later); the reliable
                    // variant is where we assert full convergence, so
                    // trivially satisfy the eventually here.
                    return true;
                }
                s.coordinated
                    && s.in_flight.is_empty()
                    && model
                        .preference
                        .iter()
                        .all(|p| s.applied.get(p) == Some(&model.value))
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    fn preference() -> Vec<Peer> {
        vec![0, 1, 2]
    }

    /// The correct fan-out: bounded apply count and convergence under
    /// reliable delivery, across all interleavings.
    #[test]
    fn correct_fan_out_is_bounded_and_converges() {
        let checker = Fanout {
            coordinator: 0,
            preference: preference(),
            value: 42,
            broken: false,
            lossy: false,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    /// Fire-and-forget best effort: forwards may be dropped; the
    /// bounded-fan-out safety invariant still holds (no re-forward
    /// storm even under loss).
    #[test]
    fn lossy_fan_out_stays_bounded() {
        let checker = Fanout {
            coordinator: 0,
            preference: preference(),
            value: 7,
            broken: false,
            lossy: true,
        }
        .checker()
        .spawn_bfs()
        .join();
        // Safety holds under loss; we assert the bounded-fan-out
        // property explicitly rather than the whole set (convergence is
        // best-effort in the lossy variant).
        assert!(checker.discovery("bounded fan-out").is_none());
    }

    /// Negative control: a re-forwarding applier violates the
    /// "terminal, local-only" contract. The apply count grows past
    /// n_val (a fan-out storm / would-be infinite loop) and the checker
    /// finds the bounded-fan-out counterexample. If it does not, the
    /// model is toothless and this test fails.
    #[test]
    fn re_forwarding_applier_violates_bounded_fan_out() {
        let checker = Fanout {
            coordinator: 0,
            preference: preference(),
            value: 1,
            broken: true,
            lossy: false,
        }
        .checker()
        .spawn_bfs()
        .join();
        assert!(
            checker.discovery("bounded fan-out").is_some(),
            "expected a bounded-fan-out counterexample from the re-forwarding applier"
        );
    }
}
