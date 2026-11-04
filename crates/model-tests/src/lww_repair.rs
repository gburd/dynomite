//! Model of timestamp read repair across replicas.
//!
//! Every write carries the coordinator's timestamp. A replica applies a
//! write only if it is not older than what the replica has recorded for
//! the key (last writer wins; a delete is a write of a tombstone). Writes
//! reach any subset of the replicas in any order. A repairing read asks
//! every replica for `(status, ts, value)`, picks the reply with the
//! largest timestamp, answers the client with it, and writes that state
//! back to every replica whose timestamp is smaller. The choice of winner
//! and of the stale set is the coordinator code in
//! `crates/dynomite/src/proto/redis/repair/make.rs`
//! (`find_most_updated`), driven over the replies the coalescer collects.
//!
//! # Invariants
//!
//! * **Repair converges** (`always`): after a repairing read every replica
//!   holds the state with the largest timestamp any replica had.
//! * **Read returns the newest** (`always`): the client is answered with
//!   that state.
//! * **Reachability** (`sometimes`): a read that repaired a replica, and
//!   one that read a tombstone as newest, are both reachable.
//!
//! # Negative control
//!
//! [`LwwRepairModel::first_reply_wins`] repairs every replica to the FIRST
//! reply instead of the newest one. It violates both invariants, so the
//! checker is shown to catch a reconcile rule that ignores timestamps.

use stateright::{Model, Property};

const REPLICAS: usize = 3;
/// Writes the model can issue: `(timestamp, value)`; value 0 is a delete.
const WRITES: [(u8, u8); 3] = [(1, 1), (2, 0), (3, 2)];

/// What a replica holds for the key: `(timestamp, value)`; timestamp 0 is
/// "never written", value 0 a tombstone.
type Cell = (u8, u8);

/// Reconcile rule under test.
#[derive(Clone, Copy, Debug)]
enum Rule {
    /// Largest timestamp wins (the production rule).
    Newest,
    /// Negative control: the first replica's reply wins.
    First,
}

/// State: each replica's cell, which writes are still undelivered to
/// which replica, and the outcome of the read once it has run.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct State {
    cells: [Cell; REPLICAS],
    /// `pending[w][r]`: write `w` not yet delivered to replica `r`.
    pending: [[bool; REPLICAS]; WRITES.len()],
    /// Whether each write has been issued.
    issued: [bool; WRITES.len()],
    /// `(answer, newest before the read, repaired any)` once read.
    read: Option<(Cell, Cell, bool)>,
}

/// Action.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Action {
    /// The coordinator issues write `w` (to every replica, undelivered).
    Issue(usize),
    /// Write `w` reaches replica `r`.
    Deliver(usize, usize),
    /// A repairing read runs against every replica.
    Read,
}

/// The timestamp read-repair model.
#[derive(Clone, Debug)]
pub struct LwwRepairModel {
    rule: Rule,
}

impl LwwRepairModel {
    /// Largest timestamp wins (the production rule).
    #[must_use]
    pub fn newest() -> Self {
        Self { rule: Rule::Newest }
    }

    /// Negative control: repair to the first reply.
    #[must_use]
    pub fn first_reply_wins() -> Self {
        Self { rule: Rule::First }
    }
}

fn apply(cell: Cell, write: Cell) -> Cell {
    if write.0 >= cell.0 {
        write
    } else {
        cell
    }
}

impl Model for LwwRepairModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            cells: [(0, 0); REPLICAS],
            pending: [[false; REPLICAS]; WRITES.len()],
            issued: [false; WRITES.len()],
            read: None,
        }]
    }

    fn actions(&self, s: &State, actions: &mut Vec<Action>) {
        if s.read.is_some() {
            return;
        }
        for (w, issued) in s.issued.iter().enumerate() {
            if !issued {
                actions.push(Action::Issue(w));
            }
            for r in 0..REPLICAS {
                if s.pending[w][r] {
                    actions.push(Action::Deliver(w, r));
                }
            }
        }
        actions.push(Action::Read);
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Issue(write) => {
                next.issued[write] = true;
                next.pending[write] = [true; REPLICAS];
            }
            Action::Deliver(write, replica) => {
                next.pending[write][replica] = false;
                next.cells[replica] = apply(next.cells[replica], WRITES[write]);
            }
            Action::Read => {
                let newest = *next
                    .cells
                    .iter()
                    .max_by_key(|cell| cell.0)
                    .expect("replicas");
                let winner = match self.rule {
                    Rule::Newest => newest,
                    Rule::First => next.cells[0],
                };
                let mut repaired = false;
                for cell in &mut next.cells {
                    let older = cell.0 < winner.0;
                    let overwritten = matches!(self.rule, Rule::First) && *cell != winner;
                    if older || overwritten {
                        *cell = winner;
                        repaired = true;
                    }
                }
                next.read = Some((winner, newest, repaired));
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("repair converges", |_, s: &State| {
                s.read
                    .is_none_or(|(_, newest, _)| s.cells.iter().all(|c| *c == newest))
            }),
            Property::always("read returns the newest", |_, s: &State| {
                s.read.is_none_or(|(answer, newest, _)| answer == newest)
            }),
            Property::sometimes("a read repaired a replica", |_, s: &State| {
                s.read.is_some_and(|(_, _, repaired)| repaired)
            }),
            Property::sometimes("a tombstone was the newest state", |_, s: &State| {
                s.read
                    .is_some_and(|(_, newest, _)| newest.0 > 0 && newest.1 == 0)
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use stateright::Checker;

    use super::*;

    #[test]
    fn newest_timestamp_repair_converges() {
        let c = LwwRepairModel::newest().checker().spawn_bfs().join();
        c.assert_properties();
    }

    #[test]
    fn negative_control_first_reply_repair_is_caught() {
        let c = LwwRepairModel::first_reply_wins()
            .checker()
            .spawn_bfs()
            .join();
        assert!(
            c.discovery("repair converges").is_some()
                || c.discovery("read returns the newest").is_some(),
            "the checker must catch a repair that ignores timestamps"
        );
    }
}
