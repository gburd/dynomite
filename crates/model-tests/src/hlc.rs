//! Model of Hybrid Logical Clock monotonicity, causality capture, and
//! bounded drift over a deterministic physical-clock schedule.
//!
//! This models the HLC primitive implemented in
//! `crates/dyniak/src/datatypes/hlc.rs` (the `dyniak::datatypes::Hlc`
//! type).
//! The abstract state machine captures the decision logic that
//! matters for the clock's guarantees:
//!
//! * each node advances its own HLC on a local event, reading its own
//!   (possibly skewed, possibly stalled) physical clock;
//! * a node may send its current HLC to another node, which merges it
//!   on receive;
//! * physical time advances on a scripted, bounded schedule so the
//!   search space stays finite while still exercising skew and
//!   non-advancing time.
//!
//! The model deliberately re-expresses the `(l, c)` update rules
//! rather than linking `dyniak`, so the checker's reachable state
//! space stays small (see the crate-level note). The update rules it
//! runs are the same "Send or local event" and "Receive event" rules
//! the production `Hlc::tick` and `Hlc::update` run.
//!
//! # Invariants asserted
//!
//! * **Monotonicity** (`always`): a node's HLC never goes backwards
//!   across its own successive events, regardless of physical-clock
//!   jitter or stalls. Stated as: every recorded event stamp is `>=`
//!   the node's previous event stamp.
//! * **Causality capture** (`always`): for every happens-before edge
//!   -- a node's program order (event `i` before event `i+1` on the
//!   same node) and every send -> receive edge -- the cause's HLC is
//!   strictly less than the effect's HLC.
//! * **Bounded drift** (`always`): every node's `l` stays within the
//!   maximum inter-node physical skew of the *global* physical time
//!   (the paper's bound: `|l - pt| <= max_skew`). A node cannot run
//!   its logical time arbitrarily far ahead of real time.
//! * **Progress reachable** (`sometimes`): a receive event that
//!   strictly advances a node past a remote stamp is reachable, so the
//!   model is not vacuously monotone.
//!
//! # Negative control
//!
//! [`Hlc::broken`] flips the receive rule: on receive it takes
//! `l' = max(l, l_m, pt)` but *forgets to advance the counter* --
//! specifically it copies the received counter verbatim (or resets to
//! zero) instead of adding one on the tie branches. When the received
//! `l_m` equals the receiver's own `l`, the merged stamp can then
//! equal the sending stamp instead of exceeding it, so a send ->
//! receive edge has `hlc(cause) >= hlc(effect)`: a causality
//! inversion. The causality-capture property then has a counterexample
//! and the checker reports it, proving the model has teeth.

use stateright::{Model, Property};

/// An HLC stamp `(l, c)` as the model represents it. Small integers
/// keep the search finite. Ordered `l` then `c`, matching the
/// production type's derived `Ord`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Stamp {
    l: u32,
    c: u32,
}

impl Stamp {
    const ZERO: Stamp = Stamp { l: 0, c: 0 };
}

/// A message in flight: a stamp sent from one node, awaiting delivery
/// to another. Carries the sender's event index so the model can
/// record the send -> receive happens-before edge on delivery.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InFlight {
    stamp: Stamp,
    /// Global index of the send event (the cause).
    src_event: usize,
    /// Destination node.
    dst: usize,
}

/// A recorded event: which node produced it and the stamp it produced.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Event {
    node: usize,
    stamp: Stamp,
}

/// Aggregated model state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct HlcState {
    /// Each node's current HLC.
    clocks: Vec<Stamp>,
    /// Each node's current physical-clock reading (bounded schedule).
    phys: Vec<u32>,
    /// Global event log, in production order. Index into this is the
    /// event id used by the happens-before edges.
    events: Vec<Event>,
    /// Program-order: the last event index each node produced.
    last_event: Vec<Option<usize>>,
    /// Happens-before edges (cause_event, effect_event).
    edges: Vec<(usize, usize)>,
    /// Messages awaiting delivery.
    channel: Vec<InFlight>,
    /// Remaining event budget (bounds the search).
    budget: u8,
}

/// HLC model.
#[derive(Clone)]
pub struct Hlc {
    /// Node count.
    pub nodes: usize,
    /// Per-node fixed physical-clock skew (the offset each node's
    /// clock carries relative to node 0). `max_skew` is the max of
    /// these; the bounded-drift bound is stated against it.
    pub skew: Vec<u32>,
    /// Total local/send/receive events allowed.
    pub budget: u8,
    /// When true, the receive rule drops the `+1` counter advance
    /// (negative control). The checker should find a causality
    /// inversion.
    pub broken: bool,
}

impl Hlc {
    fn max_skew(&self) -> u32 {
        self.skew.iter().copied().max().unwrap_or(0)
    }

    /// Global physical time: the maximum of all nodes' physical
    /// readings, used as the reference for the bounded-drift bound.
    fn global_phys(state: &HlcState) -> u32 {
        state.phys.iter().copied().max().unwrap_or(0)
    }

    /// The correct "Send or local event" rule.
    fn tick(clock: Stamp, pt: u32) -> Stamp {
        let new_l = clock.l.max(pt);
        let new_c = if new_l == clock.l { clock.c + 1 } else { 0 };
        Stamp { l: new_l, c: new_c }
    }

    /// The "Receive event" rule. `broken` drops the `+1` counter
    /// advance so a tie between the receiver's `l` and the message `l`
    /// no longer yields a strictly greater stamp.
    fn recv(clock: Stamp, msg: Stamp, pt: u32, broken: bool) -> Stamp {
        let new_l = clock.l.max(msg.l).max(pt);
        let new_c = if broken {
            // NEGATIVE CONTROL: take the max counter but forget to add
            // one. On the full-tie branch (new_l == l == l_m) this
            // yields max(c, c_m), which for c_m >= c equals the
            // message's own stamp -- no strict advance, so the send ->
            // receive edge is not strictly increasing.
            if new_l == clock.l && new_l == msg.l {
                clock.c.max(msg.c)
            } else if new_l == clock.l {
                clock.c
            } else if new_l == msg.l {
                msg.c
            } else {
                0
            }
        } else if new_l == clock.l && new_l == msg.l {
            clock.c.max(msg.c) + 1
        } else if new_l == clock.l {
            clock.c + 1
        } else if new_l == msg.l {
            msg.c + 1
        } else {
            0
        };
        Stamp { l: new_l, c: new_c }
    }
}

/// Actions: a local event, a send (local event + enqueue message), a
/// physical-clock advance, or a delivery of an in-flight message.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Node `n` performs a local event at its current physical time.
    Local(usize),
    /// Node `src` sends its (advanced) clock to node `dst`.
    Send(usize, usize),
    /// Advance node `n`'s physical clock by one tick (bounded).
    AdvancePhys(usize),
    /// Deliver `channel[idx]`.
    Deliver(usize),
}

impl Hlc {
    fn record_event(state: &mut HlcState, node: usize, stamp: Stamp) -> usize {
        let idx = state.events.len();
        state.events.push(Event { node, stamp });
        // Program-order edge from this node's previous event.
        if let Some(prev) = state.last_event[node] {
            state.edges.push((prev, idx));
        }
        state.last_event[node] = Some(idx);
        idx
    }
}

impl Model for Hlc {
    type State = HlcState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![HlcState {
            clocks: vec![Stamp::ZERO; self.nodes],
            // Each node starts at its own skew offset, modelling
            // clocks that are already spread apart at t=0.
            phys: self.skew.clone(),
            events: Vec::new(),
            last_event: vec![None; self.nodes],
            edges: Vec::new(),
            channel: Vec::new(),
            budget: self.budget,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.budget > 0 {
            for n in 0..self.nodes {
                actions.push(Action::Local(n));
                // A node may advance its physical clock (bounded so the
                // search terminates and drift stays within skew).
                if state.phys[n] < self.max_skew() + u32::from(self.budget) {
                    actions.push(Action::AdvancePhys(n));
                }
                for dst in 0..self.nodes {
                    if dst != n {
                        actions.push(Action::Send(n, dst));
                    }
                }
            }
        }
        for idx in 0..state.channel.len() {
            actions.push(Action::Deliver(idx));
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::Local(n) => {
                if s.budget == 0 {
                    return None;
                }
                let stamp = Self::tick(s.clocks[n], s.phys[n]);
                s.clocks[n] = stamp;
                Self::record_event(&mut s, n, stamp);
                s.budget -= 1;
                Some(s)
            }
            Action::Send(src, dst) => {
                if s.budget == 0 {
                    return None;
                }
                // A send is a local event that also emits a message.
                let stamp = Self::tick(s.clocks[src], s.phys[src]);
                s.clocks[src] = stamp;
                let idx = Self::record_event(&mut s, src, stamp);
                s.channel.push(InFlight {
                    stamp,
                    src_event: idx,
                    dst,
                });
                s.budget -= 1;
                Some(s)
            }
            Action::AdvancePhys(n) => {
                if s.budget == 0 {
                    return None;
                }
                let cap = self.max_skew() + u32::from(self.budget);
                if s.phys[n] >= cap {
                    return None;
                }
                s.phys[n] += 1;
                // Advancing the clock does not consume the event budget
                // (it is not a logical event), but we bound it via the
                // cap above so the search terminates.
                Some(s)
            }
            Action::Deliver(idx) => {
                let msg = *s.channel.get(idx)?;
                let n = msg.dst;
                let stamp = Self::recv(s.clocks[n], msg.stamp, s.phys[n], self.broken);
                s.clocks[n] = stamp;
                let eidx = Self::record_event(&mut s, n, stamp);
                // send -> receive happens-before edge.
                s.edges.push((msg.src_event, eidx));
                s.channel.remove(idx);
                Some(s)
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Monotonicity: every node's successive event stamps are
            // non-decreasing. Program-order edges (same node, adjacent
            // events) must be strictly increasing; here we assert the
            // weaker >= over ALL of a node's events to state the "never
            // goes backward" guarantee directly, and the strict part is
            // covered by causality capture below.
            Property::<Self>::always("monotonicity", |_, s| {
                for node in 0..s.clocks.len() {
                    let mut prev = Stamp::ZERO;
                    for e in &s.events {
                        if e.node == node {
                            if e.stamp < prev {
                                return false;
                            }
                            prev = e.stamp;
                        }
                    }
                }
                true
            }),
            // Causality capture: every happens-before edge is strictly
            // increasing in HLC. This is the property the negative
            // control violates.
            Property::<Self>::always("causality capture", |_, s| {
                s.edges
                    .iter()
                    .all(|&(cause, effect)| s.events[cause].stamp < s.events[effect].stamp)
            }),
            // Bounded drift: each node's logical l stays within the max
            // inter-node skew of the global physical time. HLC never
            // runs arbitrarily ahead of real time.
            Property::<Self>::always("bounded drift", |model, s| {
                let gp = Hlc::global_phys(s);
                let max_skew = model.max_skew();
                s.clocks.iter().all(|c| c.l <= gp + max_skew)
            }),
            // Progress reachable: a receive that strictly advances a
            // node past the remote stamp is reachable (not vacuous).
            Property::<Self>::sometimes("receive advances past remote", |_, s| {
                s.edges.iter().any(|&(cause, effect)| {
                    s.events[cause].node != s.events[effect].node
                        && s.events[effect].stamp > s.events[cause].stamp
                })
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    // The model state carries an append-only event/edge history for
    // the happens-before checks, so two paths that reach the same
    // logical clock configuration via different interleavings are
    // distinct states and are not deduplicated by the BFS checker.
    // The reachable-state count is therefore the number of distinct
    // schedules, which grows super-exponentially in the event budget.
    // `nodes = 2` with `budget = 3` keeps the search exhaustive, fast
    // (~2s), and under a few MB of RSS while still exercising a
    // send -> receive causal edge under clock skew, a stalled clock,
    // and the broken-receive-rule inversion. Do not raise these bounds
    // for the CI gate; a deeper schedule belongs in a soak run, not
    // `scripts/model.sh`.

    /// Correct model: monotonicity, causality capture, and bounded
    /// drift hold across all reachable states of a small multi-node
    /// schedule with clock skew and stalls.
    #[test]
    fn correct_model_captures_causality_and_stays_monotone() {
        let checker = Hlc {
            nodes: 2,
            skew: vec![0, 1],
            budget: 3,
            broken: false,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    /// Negative control: the broken receive rule (no counter advance)
    /// makes a send -> receive edge non-strict when the logical times
    /// tie; the checker must find a causality-capture counterexample.
    /// If it does not, the model is toothless and this test fails.
    #[test]
    fn broken_receive_rule_inverts_causality() {
        let checker = Hlc {
            nodes: 2,
            skew: vec![0, 1],
            budget: 3,
            broken: true,
        }
        .checker()
        .spawn_bfs()
        .join();
        assert!(
            checker.discovery("causality capture").is_some(),
            "expected a causality-capture counterexample from the broken receive rule"
        );
    }
}
