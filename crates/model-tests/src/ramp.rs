//! Model of RAMP-Fast read-atomic multi-partition transactions.
//!
//! This models the protocol implemented in `crates/dyniak/src/ramp.rs`
//! (`dyniak::ramp::select`, the pure read-atomic decision core) and
//! `crates/dyniak/src/ramp_store.rs` (`RampCoordinator` write/read
//! rounds). The abstract state machine reproduces the same decision
//! logic the production coordinator runs:
//!
//! * **Write (two-phase, non-blocking).** A writer picks one timestamp
//!   `ts` for its whole batch. In PREPARE it makes every item's version
//!   at `ts` durably present (invisible). In COMMIT it advances each
//!   key's latest-visible pointer to `ts`. The two phases are separate
//!   steps the scheduler can interleave with a reader, so a reader can
//!   observe a *partially committed* writer (pointer advanced on `a`,
//!   not yet on `b`).
//!
//! * **Read (round 1 + conditional round 2).** Round 1 snapshots each
//!   key's latest-visible version and its sibling metadata. The pure
//!   core [`select_missing`] (the model's copy of
//!   `dyniak::ramp::select`) decides which siblings the reader is
//!   behind on; round 2 fetches exactly those versions by timestamp,
//!   which PREPARE guarantees are present. The repaired snapshot is
//!   fracture-free.
//!
//! # Invariants asserted
//!
//! * **Read atomicity / no fractured read** (`always`): a completed
//!   read never observes transaction `T`'s write to one key while
//!   missing `T`'s write to another key that `T` also wrote. This is
//!   the core RAMP guarantee.
//! * **Non-blocking liveness** (`eventually`): every read reaches a
//!   completed state (a reader never waits on a writer, so under fair
//!   scheduling every started read finishes).
//!
//! A deliberately-broken variant ([`BrokenRamp`]) models a reader that
//! SKIPS the second round (returns the round-1 snapshot as-is). Its
//! model-check finds the fractured-read violation, proving the
//! invariant has teeth.

use std::collections::{BTreeMap, BTreeSet};

use stateright::{Model, Property};

/// A key identifier (small integer keyspace).
type Key = u8;
/// A transaction timestamp.
type Ts = u8;

/// The RAMP-Fast read-atomic decision core, mirroring
/// `dyniak::ramp::select`.
///
/// `round1` maps each key the reader is reading to `(ts, siblings)`:
/// the timestamp of the version it currently sees and that version's
/// sibling-key metadata. Returns the `(key, ts)` upgrades the reader
/// must fetch in round 2 -- for every observed item and every sibling
/// it names, if the reader's current version of the sibling is older
/// than the item's ts, the reader is behind and must upgrade to the
/// highest such required ts.
///
/// This is the exact logic of the production `select`, re-expressed
/// over the model's integer keys so the checker's state space stays
/// small. The model drives THIS function, so the gate covers the real
/// decision rule and not a different design.
fn select_missing(round1: &BTreeMap<Key, (Ts, BTreeSet<Key>)>) -> BTreeMap<Key, Ts> {
    let mut required: BTreeMap<Key, Ts> = BTreeMap::new();
    for (ts, siblings) in round1.values() {
        for sib in siblings {
            let have = round1.get(sib).map_or(0, |(t, _)| *t);
            if have < *ts {
                let e = required.entry(*sib).or_insert(*ts);
                if *ts > *e {
                    *e = *ts;
                }
            }
        }
    }
    required
}

/// A committed write transaction: the set of keys it wrote at its ts.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct WriteTxn {
    ts: Ts,
    keys: BTreeSet<Key>,
}

/// A reader's progress through the RAMP-Fast read protocol.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum ReadPhase {
    /// Not started.
    Idle,
    /// Round 1 done: holds the observed `(ts, siblings)` per key.
    Round1(BTreeMap<Key, (Ts, BTreeSet<Key>)>),
    /// Completed: holds the final `key -> observed ts` snapshot.
    Done(BTreeMap<Key, Ts>),
}

/// Whole-system state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct RampState {
    /// Every key's durable version history: which timestamps have been
    /// PREPAREd (present, maybe invisible). A version is `(ts, siblings)`.
    prepared: BTreeMap<Key, BTreeMap<Ts, BTreeSet<Key>>>,
    /// Each key's latest-visible pointer (the ts a reader sees), or
    /// absent if the key has no committed RAMP write.
    visible: BTreeMap<Key, Ts>,
    /// The write transactions and how far each has progressed:
    /// `prepared_keys` (PREPARE issued) and `committed_keys` (pointer
    /// advanced). A write is fully committed when the two match its key
    /// set.
    writes: Vec<WriteProgress>,
    /// The single reader's progress.
    reader: ReadPhase,
}

/// A write transaction's per-phase progress.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct WriteProgress {
    txn: WriteTxn,
    /// Keys whose version has been PREPAREd.
    prepared_keys: BTreeSet<Key>,
    /// Keys whose visible pointer has been advanced to this txn's ts.
    committed_keys: BTreeSet<Key>,
}

/// Faithful RAMP-Fast model.
#[derive(Clone)]
pub struct Ramp {
    /// The keys the reader reads (and the write txns write subsets of).
    pub keys: Vec<Key>,
    /// The write transactions to run, each `(ts, keys)`.
    pub write_specs: Vec<(Ts, Vec<Key>)>,
}

/// Broken RAMP: the reader skips round 2 (returns the round-1 snapshot
/// verbatim). Negative control: the read-atomicity check must find a
/// fractured read against this model.
#[derive(Clone)]
pub struct BrokenRamp {
    /// The keys the reader reads.
    pub keys: Vec<Key>,
    /// The write transactions.
    pub write_specs: Vec<(Ts, Vec<Key>)>,
}

/// Actions the checker may take.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Writer `w` PREPAREs its version for key `k` (durable, invisible).
    Prepare(usize, Key),
    /// Writer `w` COMMITs (advances the visible pointer) for key `k`.
    Commit(usize, Key),
    /// The reader performs round 1 (snapshot latest-visible + metadata).
    ReadRound1,
    /// The reader performs the conditional round 2 and completes.
    ReadFinish,
}

impl Ramp {
    fn init(&self) -> RampState {
        RampState {
            prepared: BTreeMap::new(),
            visible: BTreeMap::new(),
            writes: self
                .write_specs
                .iter()
                .map(|(ts, keys)| WriteProgress {
                    txn: WriteTxn {
                        ts: *ts,
                        keys: keys.iter().copied().collect(),
                    },
                    prepared_keys: BTreeSet::new(),
                    committed_keys: BTreeSet::new(),
                })
                .collect(),
            reader: ReadPhase::Idle,
        }
    }
}

/// Shared action generation for both models (identical: the bug is in
/// the round-2 transition, not the enabled actions).
fn shared_actions(keys: &[Key], state: &RampState, actions: &mut Vec<Action>) {
    for (w, wp) in state.writes.iter().enumerate() {
        // PREPARE any not-yet-prepared key of this write.
        for k in wp.txn.keys.difference(&wp.prepared_keys) {
            actions.push(Action::Prepare(w, *k));
        }
        // COMMIT any prepared-but-not-committed key.
        for k in wp.prepared_keys.difference(&wp.committed_keys) {
            actions.push(Action::Commit(w, *k));
        }
    }
    match &state.reader {
        ReadPhase::Idle => actions.push(Action::ReadRound1),
        ReadPhase::Round1(_) => actions.push(Action::ReadFinish),
        ReadPhase::Done(_) => {}
    }
    let _ = keys;
}

/// Shared transition for PREPARE / COMMIT / round 1 (identical in both
/// models). The round-2 finish differs and is handled by the caller.
fn apply_write_or_round1(last: &RampState, action: &Action) -> RampState {
    let mut s = last.clone();
    match action {
        Action::Prepare(w, k) => {
            let wp = &mut s.writes[*w];
            wp.prepared_keys.insert(*k);
            let siblings: BTreeSet<Key> = wp.txn.keys.iter().copied().filter(|x| x != k).collect();
            s.prepared
                .entry(*k)
                .or_default()
                .insert(wp.txn.ts, siblings);
        }
        Action::Commit(w, k) => {
            let (ts, do_commit) = {
                let wp = &mut s.writes[*w];
                wp.committed_keys.insert(*k);
                (wp.txn.ts, true)
            };
            if do_commit {
                // Monotonic: never move the pointer backwards.
                let e = s.visible.entry(*k).or_insert(ts);
                if ts > *e {
                    *e = ts;
                }
            }
        }
        Action::ReadRound1 => {
            let mut snap: BTreeMap<Key, (Ts, BTreeSet<Key>)> = BTreeMap::new();
            for k in last_keys(last) {
                if let Some(ts) = s.visible.get(&k) {
                    let siblings = s
                        .prepared
                        .get(&k)
                        .and_then(|vers| vers.get(ts))
                        .cloned()
                        .unwrap_or_default();
                    snap.insert(k, (*ts, siblings));
                }
            }
            s.reader = ReadPhase::Round1(snap);
        }
        Action::ReadFinish => {}
    }
    s
}

/// The reader's declared key set is fixed in the model config; recover
/// it from the visible + prepared maps' domain plus the write specs.
/// The model stores it implicitly via the initial state, so we thread
/// it through a thread-local-free approach: read keys are exactly the
/// union of all write-txn key sets (the reader reads everything any
/// writer might touch, which is the worst case for fractured reads).
fn last_keys(s: &RampState) -> Vec<Key> {
    let mut set: BTreeSet<Key> = BTreeSet::new();
    for wp in &s.writes {
        for k in &wp.txn.keys {
            set.insert(*k);
        }
    }
    set.into_iter().collect()
}

impl Model for Ramp {
    type State = RampState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![self.init()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        shared_actions(&self.keys, state, actions);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = apply_write_or_round1(last, &action);
        if action == Action::ReadFinish {
            if let ReadPhase::Round1(snap) = &last.reader {
                // Faithful: run the conditional second round via the
                // production decision core, then overlay the repairs.
                let missing = select_missing(snap);
                let mut final_snap: BTreeMap<Key, Ts> =
                    snap.iter().map(|(k, (ts, _))| (*k, *ts)).collect();
                for (k, ts) in missing {
                    final_snap.insert(k, ts);
                }
                s.reader = ReadPhase::Done(final_snap);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("read atomicity (no fractured read)", |_, s| !fractured(s)),
            Property::<Self>::sometimes("reader completed", |_, s| {
                matches!(s.reader, ReadPhase::Done(_))
            }),
            Property::<Self>::eventually("read terminates (non-blocking)", |_, s| {
                matches!(s.reader, ReadPhase::Done(_))
            }),
        ]
    }
}

impl Model for BrokenRamp {
    type State = RampState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![Ramp {
            keys: self.keys.clone(),
            write_specs: self.write_specs.clone(),
        }
        .init()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        shared_actions(&self.keys, state, actions);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = apply_write_or_round1(last, &action);
        if action == Action::ReadFinish {
            if let ReadPhase::Round1(snap) = &last.reader {
                // The bug: skip round 2. Return the round-1 snapshot
                // verbatim, which can be fractured.
                let final_snap: BTreeMap<Key, Ts> =
                    snap.iter().map(|(k, (ts, _))| (*k, *ts)).collect();
                s.reader = ReadPhase::Done(final_snap);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::<Self>::always(
            "read atomicity (no fractured read)",
            |_, s| !fractured(s),
        )]
    }
}

/// True when the reader's completed snapshot is fractured: it observes
/// some transaction `T`'s write to a key while missing `T`'s write to
/// another key that `T` also wrote (and that the reader is reading).
///
/// For a completed snapshot, for every write txn `T` and every key `k`
/// in `T`'s key set that the reader observed at exactly `T.ts`, every
/// OTHER key of `T` that the reader is reading must ALSO be observed at
/// a ts `>= T.ts` (a newer txn overwriting it is fine -- that is
/// all-or-nothing at the granularity of `T`; the reader still saw a
/// consistent cut, just a later one). A key observed at a ts `< T.ts`
/// while a sibling is at `T.ts` is a fractured read.
fn fractured(s: &RampState) -> bool {
    let ReadPhase::Done(snap) = &s.reader else {
        return false;
    };
    for wp in &s.writes {
        let t = &wp.txn;
        // Only fully-considered if the reader observed at least one of
        // T's keys at exactly T.ts.
        let observed_at_t: Vec<Key> = t
            .keys
            .iter()
            .copied()
            .filter(|k| snap.get(k) == Some(&t.ts))
            .collect();
        if observed_at_t.is_empty() {
            continue;
        }
        // Every sibling the reader is reading must be at >= T.ts.
        for sib in &t.keys {
            if observed_at_t.contains(sib) {
                continue;
            }
            if let Some(seen) = snap.get(sib) {
                if *seen < t.ts {
                    return true;
                }
            }
            // Sibling not in the snapshot at all: the reader is not
            // reading it, so no fracture for that pair. (In the model
            // the reader reads every written key, so this case means
            // the sibling has no visible version at all, i.e. T wrote
            // it but its pointer is unadvanced AND no round-2 repair
            // ran -- which the faithful model always repairs.)
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    /// Read atomicity holds across the full reachable state space: a
    /// writer that PREPAREs both keys but COMMITs them in any
    /// interleaving with a concurrent reader never produces a fractured
    /// read, because the faithful reader's second round repairs it.
    #[test]
    fn faithful_ramp_read_atomic() {
        let checker = Ramp {
            keys: vec![0, 1],
            // One txn at ts=2 writing keys {0,1}; the interleaving of
            // its PREPARE/COMMIT steps with the reader's two rounds is
            // the state space (includes the partial-commit window).
            write_specs: vec![(2, vec![0, 1])],
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(
            checker.unique_state_count() > 20,
            "expected a non-trivial state space, got {}",
            checker.unique_state_count()
        );
    }

    /// Read atomicity holds with an older pre-existing version plus a
    /// concurrent multi-key writer: the classic RAMP scenario where a
    /// naive reader would see `a@new, b@old` (a fractured read) but the
    /// second round repairs `b` to the new version.
    #[test]
    fn faithful_ramp_read_atomic_with_prior_version() {
        let checker = Ramp {
            keys: vec![0, 1],
            // ts=1 writes both keys (a committed prior snapshot); ts=3
            // overwrites both. The reader can catch ts=3 mid-commit.
            write_specs: vec![(1, vec![0, 1]), (3, vec![0, 1])],
        }
        .checker()
        .spawn_bfs()
        .join();
        assert!(
            checker
                .discovery("read atomicity (no fractured read)")
                .is_none(),
            "faithful RAMP must never produce a fractured read"
        );
        // Liveness: every terminal path completes the read.
        assert!(
            checker
                .discovery("read terminates (non-blocking)")
                .is_none(),
            "every read must terminate (reads are non-blocking)"
        );
    }

    /// Three keys, two overlapping writers: still fracture-free.
    #[test]
    fn faithful_ramp_three_keys() {
        let checker = Ramp {
            keys: vec![0, 1, 2],
            write_specs: vec![(2, vec![0, 1]), (3, vec![1, 2])],
        }
        .checker()
        .spawn_bfs()
        .join();
        assert!(checker
            .discovery("read atomicity (no fractured read)")
            .is_none());
    }

    /// Negative control: the reader that skips the second round
    /// produces a fractured read, and the checker FINDS it. This proves
    /// the read-atomicity property has teeth.
    #[test]
    fn broken_ramp_produces_fractured_read() {
        let checker = BrokenRamp {
            keys: vec![0, 1],
            write_specs: vec![(1, vec![0, 1]), (3, vec![0, 1])],
        }
        .checker()
        .spawn_bfs()
        .join();
        let discovery = checker.discovery("read atomicity (no fractured read)");
        assert!(
            discovery.is_some(),
            "expected the round-2-skipping reader to produce a fractured \
             read, but no counterexample was found"
        );
    }

    /// Guard against a false-alarm negative control: the FAITHFUL model
    /// at the very same shape the broken one violates finds no
    /// fractured read. The violation is the skipped second round, not
    /// the harness.
    #[test]
    fn faithful_ramp_at_control_shape_is_safe() {
        let checker = Ramp {
            keys: vec![0, 1],
            write_specs: vec![(1, vec![0, 1]), (3, vec![0, 1])],
        }
        .checker()
        .spawn_bfs()
        .join();
        assert!(
            checker
                .discovery("read atomicity (no fractured read)")
                .is_none(),
            "faithful model must not produce a fractured read"
        );
    }

    /// The pure decision core matches the production `select`'s
    /// documented examples: a fracture is detected and repaired to the
    /// highest required ts.
    #[test]
    fn select_core_matches_production_examples() {
        // `a`@5 names `b`; reader saw `b`@2 -> upgrade b to 5.
        let mut r: BTreeMap<Key, (Ts, BTreeSet<Key>)> = BTreeMap::new();
        r.insert(0, (5, [1].into_iter().collect()));
        r.insert(1, (2, BTreeSet::new()));
        let missing = select_missing(&r);
        assert_eq!(missing.get(&1), Some(&5));

        // Consistent snapshot: no upgrades.
        let mut r2: BTreeMap<Key, (Ts, BTreeSet<Key>)> = BTreeMap::new();
        r2.insert(0, (5, [1].into_iter().collect()));
        r2.insert(1, (5, [0].into_iter().collect()));
        assert!(select_missing(&r2).is_empty());
    }
}
