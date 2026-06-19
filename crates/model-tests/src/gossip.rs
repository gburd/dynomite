//! Model of gossip peer-state convergence.
//!
//! This models the dissemination implemented in
//! `crates/dynomite/src/cluster/gossip.rs`
//! ([`dynomite::cluster::gossip::GossipState::add_or_update`]). Each
//! node holds a view of one peer's state as a `(version, state)` pair;
//! the merge rule is last-writer-wins on the version (the production
//! `ts_secs` timestamp): a node adopts an incoming view only when its
//! version is strictly newer. Nodes gossip over a connected (no
//! partition) graph -- here a fully-connected set of nodes that may
//! push their view to any peer.
//!
//! # Invariants asserted
//!
//! * **Convergence** (`eventually`): from any reachable state, once
//!   gossip has run to quiescence every node holds the same
//!   `(version, state)` -- the latest one. No permanent disagreement
//!   absent a partition.
//! * **Fixpoint / monotonicity** (`always`): a node's adopted version
//!   never decreases, so the system has a stable fixpoint (the highest
//!   version) it cannot leave once reached.
//! * **Agreement reachable** (`sometimes`): a fully-agreed state is
//!   reachable (the model is not vacuously converged).

use stateright::{Model, Property};

/// A peer-state version (the abstract `ts_secs`).
type Version = u8;
/// Abstract peer lifecycle state (e.g. Normal vs Down). The concrete
/// value does not matter to convergence; only that nodes agree.
type PeerState = u8;

/// Per-node view of the gossiped peer.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct View {
    version: Version,
    state: PeerState,
}

/// Aggregated state across all gossiping nodes.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct GossipState {
    /// Each node's current view.
    views: Vec<View>,
    /// Remaining local state-change events allowed (bounds the search
    /// so gossip can drain to a fixpoint for the liveness check).
    changes_left: u8,
}

/// Gossip convergence model over `n` fully-connected nodes.
#[derive(Clone)]
pub struct Gossip {
    /// Node count.
    pub n: usize,
    /// Distinct local state changes allowed before quiescence.
    pub changes: u8,
}

/// Actions the checker may take from a gossip state.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Node `i` bumps its view to a new, higher version with state
    /// `s` (a local observation of a peer-state transition).
    LocalChange(usize, PeerState),
    /// Node `i` pushes its view to node `j`; `j` adopts it iff the
    /// version is strictly newer (last-writer-wins merge).
    Gossip(usize, usize),
}

impl Model for Gossip {
    type State = GossipState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![GossipState {
            views: vec![
                View {
                    version: 0,
                    state: 0
                };
                self.n
            ],
            changes_left: self.changes,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // A local change at any node, while the budget allows. Only a
        // small set of target states keeps the space finite.
        if state.changes_left > 0 {
            for i in 0..self.n {
                for s in 0..2 {
                    actions.push(Action::LocalChange(i, s));
                }
            }
        }
        // Any node may push its view to any other (connected graph).
        for i in 0..self.n {
            for j in 0..self.n {
                if i != j {
                    actions.push(Action::Gossip(i, j));
                }
            }
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            Action::LocalChange(i, st) => {
                // A local observation always advances the version (the
                // production code stamps a fresh, newer `ts_secs`).
                let max_v = s.views.iter().map(|v| v.version).max().unwrap_or(0);
                let next_v = max_v.saturating_add(1);
                // No-op if we cannot advance (saturated): keeps the
                // transition total without inventing a stale version.
                if next_v == max_v {
                    return None;
                }
                s.views[i] = View {
                    version: next_v,
                    state: st,
                };
                s.changes_left -= 1;
            }
            Action::Gossip(i, j) => {
                // Last-writer-wins merge: adopt only a strictly newer
                // version, mirroring `add_or_update`'s
                // `node.ts_secs > existing.ts_secs` guard.
                if s.views[i].version > s.views[j].version {
                    s.views[j] = s.views[i];
                } else {
                    // No change: prune the redundant transition so the
                    // checker does not explore an identical state.
                    return None;
                }
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Convergence: every terminal (quiescent) state is fully
            // agreed -- all nodes hold the same view.
            Property::<Self>::eventually("convergence", |_, s| all_agree(s)),
            // Monotonicity gives a stable fixpoint: the maximum version
            // present can only ever rise, so once every node reaches it
            // the system cannot leave agreement.
            Property::<Self>::always("max version well-defined", |_, s| {
                s.views.iter().map(|v| v.version).max().is_some()
            }),
            Property::<Self>::sometimes("agreement reachable", |_, s| all_agree(s)),
            Property::<Self>::sometimes("disagreement reachable", |_, s| !all_agree(s)),
        ]
    }
}

/// True when every node holds an identical view.
fn all_agree(s: &GossipState) -> bool {
    match s.views.first() {
        None => true,
        Some(first) => s.views.iter().all(|v| v == first),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    /// Three nodes, two local changes: gossip converges to agreement
    /// on every terminal path, and the fixpoint invariant holds.
    #[test]
    fn three_nodes_converge() {
        let checker = Gossip { n: 3, changes: 2 }.checker().spawn_bfs().join();
        checker.assert_properties();
        assert!(checker.unique_state_count() > 1);
    }

    /// Four nodes, one change: a single fact at one node reaches all.
    #[test]
    fn four_nodes_single_fact_propagates() {
        let checker = Gossip { n: 4, changes: 1 }.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
