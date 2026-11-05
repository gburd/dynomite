//! Model of read-repair timestamps written by concurrent coordinators.
//!
//! Several coordinators write one key in the same millisecond. A write's
//! stamp is `(ms << 12) | node_id`
//! (`repair_timestamp` in `crates/dynomite/src/cluster/pool.rs`), so two
//! coordinators never share a stamp, while two writes from one
//! coordinator in one millisecond do. Each `(coordinator, replica)` pair
//! is a FIFO channel. A replica applies a write iff it is not older than
//! what it holds, so of two equal-stamp writes it keeps the one that
//! arrived last. A coordinator queues a write on every replica's channel
//! as one step: the dispatcher holds the key slot's ordering lock from
//! stamping to the last enqueue
//! (`ClusterDispatcher::repair_script` in
//! `crates/dynomite/src/cluster/dispatch.rs`), so two writes from one
//! coordinator are queued in one order on every channel. After every
//! write is delivered, a repairing read repairs each replica whose stamp
//! is below the largest (`find_most_updated` in
//! `crates/dynomite/src/proto/redis/repair/make.rs`). In the lossy
//! variant a channel may drop a write (a replica that failed it).
//!
//! # Invariant
//!
//! * **Replicas converge** (`always`): after every delivery and one
//!   repairing read, every replica holds the same value.
//! * **Reachability** (`sometimes`): two writes with one stamp reached
//!   one replica; with losses, the read repaired a replica.
//!
//! # Residual case
//!
//! [`RepairStampModel::lossy`] with two writes per coordinator diverges:
//! when one of two equal-stamp writes fails on some replica, no stamp
//! orders them. The test pins that this case stays outside the
//! guarantee.
//!
//! # Negative controls
//!
//! * [`RepairStampModel::without_node_id`]: stamps carry only the
//!   millisecond, so two coordinators tie. Replicas keep different
//!   writes with equal stamps and the read cannot tell them apart.
//! * [`RepairStampModel::interleaved_fanout`]: a coordinator queues a
//!   write one replica at a time, so the fan-outs of two of its writes
//!   interleave and replicas receive its equal-stamp writes in
//!   different orders.

use stateright::{Model, Property};

/// Bits of a stamp holding the node id.
const NODE_ID_BITS: u32 = 12;
/// The shared millisecond.
const MS: u32 = 5;

/// What a replica holds: `(stamp, value)`; value 0 is "never written".
type Cell = (u32, u8);

/// Variant under test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Variant {
    /// Node id in the stamp, atomic per-key fan-out (production).
    Production,
    /// Negative control: stamp is the millisecond alone.
    NoNodeId,
    /// Negative control: per-replica enqueue, fan-outs may interleave.
    Interleaved,
}

/// State: each replica's cell, each channel's queue, which write has
/// been queued where, and the read's outcome.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    cells: Vec<Cell>,
    /// `queues[c][r]`: writes from coordinator `c` queued to replica `r`,
    /// oldest first.
    queues: Vec<Vec<Vec<u8>>>,
    /// `queued[w][r]`: write `w` has been queued to replica `r`.
    queued: Vec<Vec<bool>>,
    /// A replica applied a write with the stamp it already held.
    tie_applied: bool,
    /// `Some(repaired_any)` once the read has run.
    read: Option<bool>,
}

/// Action.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// Queue write `w` on every replica's channel (atomic fan-out).
    Issue(usize),
    /// Queue write `w` on replica `r`'s channel only (interleaved).
    IssueTo(usize, usize),
    /// Replica `r` takes the oldest write from coordinator `c`.
    Deliver(usize, usize),
    /// The oldest write from coordinator `c` to replica `r` is lost.
    Lose(usize, usize),
    /// A repairing read runs against every replica.
    Read,
}

/// The repair-stamp model: `coordinators` nodes each write the key
/// `per_coord` times in one millisecond, to `replicas` replicas. Writes
/// of one coordinator come from different client connections, so they
/// may be issued in any order.
#[derive(Clone, Debug)]
pub struct RepairStampModel {
    variant: Variant,
    coordinators: usize,
    per_coord: usize,
    replicas: usize,
    lossy: bool,
}

impl RepairStampModel {
    /// Node id in the stamp and atomic per-key fan-out.
    #[must_use]
    pub fn production(coordinators: usize, per_coord: usize, replicas: usize) -> Self {
        Self {
            variant: Variant::Production,
            coordinators,
            per_coord,
            replicas,
            lossy: false,
        }
    }

    /// Let any channel drop the write at its head.
    #[must_use]
    pub fn lossy(self) -> Self {
        Self {
            lossy: true,
            ..self
        }
    }

    /// Negative control: stamps without the node id.
    #[must_use]
    pub fn without_node_id(coordinators: usize, per_coord: usize, replicas: usize) -> Self {
        Self {
            variant: Variant::NoNodeId,
            ..Self::production(coordinators, per_coord, replicas)
        }
    }

    /// Negative control: a coordinator's fan-outs may interleave.
    #[must_use]
    pub fn interleaved_fanout(coordinators: usize, per_coord: usize, replicas: usize) -> Self {
        Self {
            variant: Variant::Interleaved,
            ..Self::production(coordinators, per_coord, replicas)
        }
    }

    fn writes(&self) -> usize {
        self.coordinators * self.per_coord
    }

    fn coordinator(&self, write: usize) -> usize {
        write / self.per_coord
    }

    fn stamp(&self, write: usize) -> u32 {
        let node = u32::try_from(self.coordinator(write)).expect("small");
        match self.variant {
            Variant::NoNodeId => MS,
            Variant::Production | Variant::Interleaved => (MS << NODE_ID_BITS) | node,
        }
    }

    fn enqueue(&self, s: &mut State, write: usize, replica: usize) {
        s.queued[write][replica] = true;
        s.queues[self.coordinator(write)][replica].push(u8::try_from(write).expect("small"));
    }
}

fn value(write: usize) -> u8 {
    u8::try_from(write + 1).expect("small")
}

fn all_delivered(s: &State) -> bool {
    s.queued.iter().all(|q| q.iter().all(|x| *x))
        && s.queues.iter().all(|qs| qs.iter().all(Vec::is_empty))
}

impl Model for RepairStampModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            cells: vec![(0, 0); self.replicas],
            queues: vec![vec![Vec::new(); self.replicas]; self.coordinators],
            queued: vec![vec![false; self.replicas]; self.writes()],
            tie_applied: false,
            read: None,
        }]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.read.is_some() {
            return;
        }
        for (w, queued) in s.queued.iter().enumerate() {
            match self.variant {
                Variant::Production | Variant::NoNodeId => {
                    if queued.iter().all(|q| !*q) {
                        actions.push(Action::Issue(w));
                    }
                }
                Variant::Interleaved => {
                    for (r, q) in queued.iter().enumerate() {
                        if !*q {
                            actions.push(Action::IssueTo(w, r));
                        }
                    }
                }
            }
        }
        for (c, qs) in s.queues.iter().enumerate() {
            for (r, q) in qs.iter().enumerate() {
                if !q.is_empty() {
                    actions.push(Action::Deliver(c, r));
                    if self.lossy {
                        actions.push(Action::Lose(c, r));
                    }
                }
            }
        }
        if all_delivered(s) {
            actions.push(Action::Read);
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Issue(w) => {
                for r in 0..self.replicas {
                    self.enqueue(&mut next, w, r);
                }
            }
            Action::IssueTo(w, r) => self.enqueue(&mut next, w, r),
            Action::Lose(c, r) => {
                next.queues[c][r].remove(0);
            }
            Action::Deliver(c, r) => {
                let w = usize::from(next.queues[c][r].remove(0));
                let stamp = self.stamp(w);
                let cell = next.cells[r];
                // Apply iff not older: an equal stamp overwrites.
                if stamp >= cell.0 {
                    next.tie_applied |= stamp == cell.0 && cell.1 != 0;
                    next.cells[r] = (stamp, value(w));
                }
            }
            Action::Read => {
                let newest = next.cells.iter().map(|c| c.0).max().expect("replicas");
                let winner = *next.cells.iter().find(|c| c.0 == newest).expect("replicas");
                let mut repaired = false;
                for cell in &mut next.cells {
                    if cell.0 < newest {
                        *cell = winner;
                        repaired = true;
                    }
                }
                next.read = Some(repaired);
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("replicas converge", |_, s: &State| {
                s.read.is_none() || s.cells.iter().all(|c| c.1 == s.cells[0].1)
            }),
            Property::sometimes("a replica applied an equal-stamp write", |_, s: &State| {
                s.tie_applied
            }),
            Property::sometimes("the read repaired a replica", |_, s: &State| {
                s.read == Some(true)
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use stateright::Checker;

    use super::*;

    fn diverges(m: RepairStampModel) -> bool {
        m.checker()
            .spawn_bfs()
            .join()
            .discovery("replicas converge")
            .is_some()
    }

    #[test]
    fn node_id_stamps_with_atomic_fanout_converge() {
        for (coordinators, per_coord, replicas) in [(2, 2, 3), (3, 2, 2)] {
            let c = RepairStampModel::production(coordinators, per_coord, replicas)
                .checker()
                .spawn_bfs()
                .join();
            assert!(c.is_done());
            assert!(c.discovery("replicas converge").is_none());
            assert!(c
                .discovery("a replica applied an equal-stamp write")
                .is_some());
        }
    }

    #[test]
    fn read_repair_converges_cross_coordinator_writes_despite_losses() {
        let c = RepairStampModel::production(3, 1, 3)
            .lossy()
            .checker()
            .spawn_bfs()
            .join();
        assert!(c.is_done());
        assert!(c.discovery("replicas converge").is_none());
        assert!(c.discovery("the read repaired a replica").is_some());
    }

    #[test]
    fn negative_control_stamps_without_node_id_diverge() {
        assert!(
            diverges(RepairStampModel::without_node_id(2, 1, 2)),
            "the checker must catch a cross-coordinator tie"
        );
    }

    #[test]
    fn negative_control_interleaved_fanout_diverges() {
        assert!(
            diverges(RepairStampModel::interleaved_fanout(2, 2, 2)),
            "the checker must catch interleaved equal-stamp fan-outs"
        );
    }

    #[test]
    fn residual_lost_equal_stamp_write_is_outside_the_guarantee() {
        assert!(diverges(RepairStampModel::production(1, 2, 2).lossy()));
    }
}
