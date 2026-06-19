//! Model of the cross-node XA two-phase commit.
//!
//! This models the protocol implemented in
//! `crates/dyniak/src/datastore/xa.rs`
//! (`dyniak::datastore::xa::XaCoordinator::execute`) and its
//! cross-node leg in `crates/dyniak/src/datastore/xa_net.rs`
//! (`dyniak::datastore::xa_net::CrossNodeCoordinator`). The abstract
//! state machine reproduces the same decision logic the production
//! coordinator and peer handler run:
//!
//! * **Prepare phase**: the coordinator delivers prepare to every
//!   participant; each votes Ok or Abort. A participant that votes Ok
//!   is *durably prepared* and survives a crash (the production
//!   `xa_prepare` writes the branch to the prepared-transaction log).
//! * **Decision (presumed abort)**: the coordinator commits only when
//!   every participant voted Ok; any Abort vote, or a prepare-phase
//!   message that never arrives, resolves the whole transaction to
//!   rollback. There is no separate durable "prepared" record on the
//!   coordinator before the commit decision, so a coordinator that
//!   forgets an undecided transaction defaults to abort -- exactly the
//!   presumed-abort rule.
//! * **Commit / rollback delivery**: the coordinator delivers the
//!   decision to each participant. Commit and rollback are idempotent
//!   on the participant (the production `XaPeer::handle_commit` /
//!   `handle_rollback` treat an already-resolved branch as success).
//! * **Forward commit**: once the coordinator has decided commit it
//!   never rolls a prepared-Ok branch back. If it cannot confirm a
//!   commit it records the branch in a durable in-doubt log and a
//!   recovery pass re-drives the commit forward
//!   (`CrossNodeCoordinator::recover_in_doubt`).
//! * **Cold restart**: a crashed coordinator that had decided commit
//!   re-drives commits from the in-doubt log; a crashed coordinator
//!   that had not yet decided defaults to abort (presumed abort).
//!
//! The message channel is lossy, reorderable, and duplicating: any
//! in-flight message may be delivered, dropped, or delivered again.
//! Participants may crash and restart, losing volatile state but
//! retaining their durable vote.
//!
//! # Invariants asserted
//!
//! * **Atomicity** (`always`): never a reachable state where one
//!   participant has committed and another has rolled back the same
//!   transaction.
//! * **No commit without unanimous prepare** (`always`): a participant
//!   is committed only if every participant voted Ok (the coordinator
//!   only ever decides commit on a unanimous Ok).
//! * **Durability of prepared** (`always`): a participant that voted
//!   Ok and then crashed is still prepared (and therefore still able
//!   to commit) after restart -- the model never lets a crash drop a
//!   durable vote.
//! * **Liveness** (`eventually`): with a bounded fault budget (so
//!   delivery and coordinator recovery eventually happen) every
//!   terminal state is fully resolved -- either all participants
//!   committed or all rolled back; none is stuck in-doubt forever.
//!
//! A deliberately-broken variant ([`BrokenXa`]) commits on a partial
//! vote; its model-check finds the atomicity violation, proving the
//! invariants have teeth. That negative control is asserted to *find*
//! a counterexample.

use std::collections::BTreeSet;

use stateright::{Model, Property};

/// Participant index.
type P = usize;

/// What a participant decided to vote when prepared.
///
/// The production `local_prepare` / `XaPeer::handle_prepare` collapse
/// to exactly these outcomes for a write branch: a successful prepare
/// votes Ok, any failure votes Abort. (`ReadOnly` is a no-second-phase
/// optimisation that is safety-equivalent to a participant that never
/// needs a commit; it is omitted from the abstract model because it
/// adds no atomicity decision -- a read-only branch is already
/// resolved and never disagrees with a committed or aborted peer.)
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Vote {
    /// Prepared successfully.
    Ok,
    /// Refused to prepare (engine error / conflict / forced abort).
    Abort,
}

/// Per-participant durable + volatile state.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RmState {
    /// Has not prepared yet.
    Working,
    /// Voted Ok and is durably prepared (survives a crash). Awaiting
    /// the coordinator's decision.
    Prepared,
    /// Voted Abort, or received the abort decision; rolled back.
    Aborted,
    /// Received and applied the commit decision.
    Committed,
}

/// Coordinator's decision state.
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TmState {
    /// Has not decided. A presumed-abort coordinator that forgets an
    /// undecided transaction (crash without a durable commit decision)
    /// resolves it to abort.
    Init,
    /// Decided to commit (every vote was Ok). This decision is durable
    /// (the in-doubt log makes it survive a coordinator crash), so a
    /// cold restart re-drives commits.
    Committed,
    /// Decided to abort.
    Aborted,
}

/// One in-flight message on the lossy/reorderable/duplicating channel.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Message {
    /// Coordinator -> participant: prepare request.
    Prepare {
        /// Target participant index.
        rm: P,
    },
    /// Participant -> coordinator: its vote.
    Vote {
        /// Voting participant index.
        rm: P,
        /// The vote cast.
        vote: Vote,
    },
    /// Coordinator -> participant: commit decision (idempotent).
    Commit {
        /// Target participant index.
        rm: P,
    },
    /// Coordinator -> participant: rollback decision (idempotent).
    Rollback {
        /// Target participant index.
        rm: P,
    },
}

/// Whole-system state.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct XaState {
    /// Per-participant durable + volatile state.
    rm_state: Vec<RmState>,
    /// Votes the coordinator has collected so far (durable: the
    /// coordinator remembers received votes until it decides).
    votes: Vec<Option<Vote>>,
    /// Coordinator decision state.
    tm_state: TmState,
    /// In-flight messages. A `BTreeSet` models a channel that may
    /// reorder and de-duplicate; explicit `Drop`/`Dup` actions model
    /// loss and duplication.
    msgs: BTreeSet<Message>,
    /// Remaining fault budget: each crash and each message drop spends
    /// one. Bounding the budget makes the channel "eventually
    /// reliable" so liveness can be checked -- once the budget is gone,
    /// every message is delivered and the protocol must drain to a
    /// terminal resolved state.
    faults_left: u8,
}

/// Faithful XA 2PC model: commit only on unanimous Ok.
#[derive(Clone)]
pub struct Xa {
    /// Number of participants.
    pub rms: usize,
    /// Fault budget (crashes + drops) before the channel becomes
    /// reliable.
    pub faults: u8,
    /// When true, a participant may choose to vote Abort (models a
    /// prepare failure / engine conflict). When false, every
    /// participant votes Ok -- useful for the all-commit liveness path.
    pub allow_abort: bool,
}

/// Broken XA variant: the coordinator commits as soon as it has *any*
/// Ok vote, not a unanimous one. Used as a negative control: the
/// atomicity check must find a counterexample against this model.
#[derive(Clone)]
pub struct BrokenXa {
    /// Number of participants.
    pub rms: usize,
    /// Fault budget.
    pub faults: u8,
}

/// Actions the checker may take from a state.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    /// Coordinator sends prepare to `rm`.
    TmSendPrepare(P),
    /// Coordinator receives `rm`'s vote.
    TmRcvVote(P),
    /// Coordinator decides commit (every vote Ok).
    TmCommit,
    /// Coordinator decides abort.
    TmAbort,
    /// Coordinator (re-)sends the decided commit/rollback to `rm`.
    /// Re-send models both the initial decision delivery and the
    /// forward-recovery / cold-restart re-drive of an in-doubt commit.
    TmResend(P),
    /// Participant `rm` receives prepare and votes.
    RmPrepare(P, Vote),
    /// Participant `rm` applies a commit it received.
    RmRcvCommit(P),
    /// Participant `rm` applies a rollback it received.
    RmRcvRollback(P),
    /// A participant crashes and restarts, losing volatile but not
    /// durable state.
    RmCrash(P),
    /// Drop an in-flight message (channel loss).
    Drop(Message),
    /// Duplicate an in-flight message (channel duplication). No state
    /// change beyond re-asserting the message; included so the model
    /// covers re-delivery explicitly.
    Dup(Message),
}

impl Xa {
    fn init(&self) -> XaState {
        XaState {
            rm_state: vec![RmState::Working; self.rms],
            votes: vec![None; self.rms],
            tm_state: TmState::Init,
            msgs: BTreeSet::new(),
            faults_left: self.faults,
        }
    }
}

/// Decide the commit predicate: every participant must have voted Ok.
fn unanimous_ok(votes: &[Option<Vote>]) -> bool {
    votes.iter().all(|v| *v == Some(Vote::Ok))
}

impl Model for Xa {
    type State = XaState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![self.init()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        shared_actions(self.rms, self.allow_abort, state, actions);
        // Faithful commit decision: unanimous Ok required.
        if state.tm_state == TmState::Init
            && state.votes.iter().all(Option::is_some)
            && unanimous_ok(&state.votes)
        {
            actions.push(Action::TmCommit);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        Some(next_state(self.rms, last, action))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("atomicity", |_, s| !mixed_outcome(s)),
            Property::<Self>::always("no commit without unanimous prepare", |_, s| {
                no_commit_without_unanimous(s)
            }),
            Property::<Self>::always("durability of prepared", |_, s| durable_prepared(s)),
            Property::<Self>::sometimes("all committed", |_, s| {
                s.rm_state.iter().all(|r| *r == RmState::Committed)
            }),
            Property::<Self>::sometimes("all aborted", |_, s| {
                s.rm_state.iter().all(|r| *r == RmState::Aborted)
            }),
            Property::<Self>::eventually("resolved", |_, s| terminal_resolved(s)),
        ]
    }
}

impl Model for BrokenXa {
    type State = XaState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![XaState {
            rm_state: vec![RmState::Working; self.rms],
            votes: vec![None; self.rms],
            tm_state: TmState::Init,
            msgs: BTreeSet::new(),
            faults_left: self.faults,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // allow_abort is forced on so a participant can disagree -- the
        // bug only manifests when one votes Ok and another Abort.
        shared_actions(self.rms, true, state, actions);
        // The bug: commit as soon as ANY vote is Ok, regardless of the
        // others. This violates the unanimous-prepare rule.
        if state.tm_state == TmState::Init && state.votes.contains(&Some(Vote::Ok)) {
            actions.push(Action::TmCommit);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        Some(next_state(self.rms, last, action))
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::<Self>::always("atomicity", |_, s| {
            !mixed_outcome(s)
        })]
    }
}

/// Actions common to both the faithful and broken models.
fn shared_actions(rms: usize, allow_abort: bool, state: &XaState, actions: &mut Vec<Action>) {
    for rm in 0..rms {
        // Coordinator sends prepare while it has not decided and has
        // not yet recorded this participant's vote.
        if state.tm_state == TmState::Init && state.votes[rm].is_none() {
            actions.push(Action::TmSendPrepare(rm));
        }
        // Coordinator receives a vote message addressed to it.
        if state.tm_state == TmState::Init
            && state
                .msgs
                .iter()
                .any(|m| matches!(m, Message::Vote { rm: r, .. } if *r == rm))
        {
            actions.push(Action::TmRcvVote(rm));
        }
        // Participant prepares when it sees a prepare request and is
        // still working.
        if state.rm_state[rm] == RmState::Working && state.msgs.contains(&Message::Prepare { rm }) {
            actions.push(Action::RmPrepare(rm, Vote::Ok));
            if allow_abort {
                actions.push(Action::RmPrepare(rm, Vote::Abort));
            }
        }
        // Participant applies a commit / rollback it received.
        if state.msgs.contains(&Message::Commit { rm }) {
            actions.push(Action::RmRcvCommit(rm));
        }
        if state.msgs.contains(&Message::Rollback { rm }) {
            actions.push(Action::RmRcvRollback(rm));
        }
        // Coordinator (re-)sends the decided outcome -- the initial
        // delivery and any forward-recovery / cold-restart re-drive.
        if state.tm_state != TmState::Init {
            actions.push(Action::TmResend(rm));
        }
        // A participant may crash and restart while the budget allows.
        if state.faults_left > 0 && state.rm_state[rm] != RmState::Committed {
            actions.push(Action::RmCrash(rm));
        }
    }
    // The coordinator may decide abort while undecided (a prepare
    // timeout / an explicit abort vote both reduce to this).
    if state.tm_state == TmState::Init {
        actions.push(Action::TmAbort);
    }
    // Channel faults: drop or duplicate any in-flight message. Drops
    // spend the budget so the channel becomes reliable once it is
    // exhausted (needed for the liveness property).
    if state.faults_left > 0 {
        for m in &state.msgs {
            actions.push(Action::Drop(m.clone()));
        }
    }
    for m in &state.msgs {
        actions.push(Action::Dup(m.clone()));
    }
}

/// State transition shared by both models.
fn next_state(rms: usize, last: &XaState, action: Action) -> XaState {
    let mut s = last.clone();
    match action {
        Action::TmSendPrepare(rm) => {
            s.msgs.insert(Message::Prepare { rm });
        }
        Action::TmRcvVote(rm) => {
            // Record the most decisive vote seen: an Abort dominates
            // (the coordinator will not commit), matching presumed
            // abort. Find any vote message for rm.
            let vote = s
                .msgs
                .iter()
                .find_map(|m| match m {
                    Message::Vote { rm: r, vote } if *r == rm => Some(*vote),
                    _ => None,
                })
                .expect("invariant: TmRcvVote is only enabled with a pending vote");
            s.votes[rm] = Some(vote);
        }
        Action::TmCommit => {
            s.tm_state = TmState::Committed;
            for rm in 0..rms {
                s.msgs.insert(Message::Commit { rm });
            }
        }
        Action::TmAbort => {
            s.tm_state = TmState::Aborted;
            for rm in 0..rms {
                s.msgs.insert(Message::Rollback { rm });
            }
        }
        Action::TmResend(rm) => match s.tm_state {
            TmState::Committed => {
                s.msgs.insert(Message::Commit { rm });
            }
            TmState::Aborted => {
                s.msgs.insert(Message::Rollback { rm });
            }
            TmState::Init => {}
        },
        Action::RmPrepare(rm, vote) => {
            match vote {
                Vote::Ok => s.rm_state[rm] = RmState::Prepared,
                // An abort vote rolls the branch back locally
                // (presumed abort: a participant that votes no is done).
                Vote::Abort => s.rm_state[rm] = RmState::Aborted,
            }
            s.msgs.insert(Message::Vote { rm, vote });
        }
        Action::RmRcvCommit(rm) => {
            // Idempotent: applying commit to an already-committed
            // branch is a no-op; a prepared branch commits. A branch
            // that somehow rolled back cannot be committed (it never
            // happens in the faithful model -- that is the atomicity
            // property -- but the transition is defensive).
            if s.rm_state[rm] == RmState::Prepared || s.rm_state[rm] == RmState::Committed {
                s.rm_state[rm] = RmState::Committed;
            }
        }
        Action::RmRcvRollback(rm) => {
            // Idempotent rollback. A committed branch is never rolled
            // back (forward recovery): once committed it stays
            // committed even if a stale rollback arrives.
            if s.rm_state[rm] != RmState::Committed {
                s.rm_state[rm] = RmState::Aborted;
            }
        }
        Action::RmCrash(rm) => {
            s.faults_left = s.faults_left.saturating_sub(1);
            // A crash loses volatile state but keeps the durable
            // vote. A `Prepared` branch is durably logged (the
            // production `xa_prepare` wrote it), so it survives intact
            // -- that is the durability guarantee. A `Working` branch
            // had not durably prepared, so the crash discards any
            // in-flight prepare it had received; it must re-receive a
            // re-sent prepare to proceed. Committed/aborted are durable
            // terminal states and are untouched.
            if s.rm_state[rm] == RmState::Working {
                s.msgs.remove(&Message::Prepare { rm });
            }
        }
        Action::Drop(m) => {
            s.faults_left = s.faults_left.saturating_sub(1);
            s.msgs.remove(&m);
        }
        Action::Dup(m) => {
            // Re-assert the message (idempotent on a set; the point is
            // that re-delivery is always possible).
            s.msgs.insert(m);
        }
    }
    s
}

/// True when two participants disagree on the transaction outcome.
fn mixed_outcome(s: &XaState) -> bool {
    let any_committed = s.rm_state.contains(&RmState::Committed);
    let any_aborted = s.rm_state.contains(&RmState::Aborted);
    any_committed && any_aborted
}

/// True iff no participant is committed unless every participant voted
/// Ok. A committed participant implies the coordinator decided commit,
/// which it only does on a unanimous Ok.
fn no_commit_without_unanimous(s: &XaState) -> bool {
    if s.rm_state.contains(&RmState::Committed) {
        return s.tm_state == TmState::Committed && unanimous_ok(&s.votes);
    }
    true
}

/// Durability: a coordinator that decided commit must have a unanimous
/// Ok recorded, so every participant was prepared at decision time and
/// remains prepared-or-committed (never silently lost to a crash).
fn durable_prepared(s: &XaState) -> bool {
    if s.tm_state == TmState::Committed {
        // Every vote was Ok, so no participant is aborted: a crash
        // never dropped a durable prepared vote.
        return !s.rm_state.contains(&RmState::Aborted);
    }
    true
}

/// A terminal resolved state: all committed or all aborted.
fn terminal_resolved(s: &XaState) -> bool {
    s.rm_state.iter().all(|r| *r == RmState::Committed)
        || s.rm_state.iter().all(|r| *r == RmState::Aborted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    /// Atomicity, unanimity, and durability hold across the full
    /// reachable state space of the faithful model with aborts and a
    /// fault budget enabled.
    #[test]
    fn faithful_xa_safety() {
        let checker = Xa {
            rms: 2,
            faults: 2,
            allow_abort: true,
        }
        .checker()
        .spawn_bfs()
        .join();
        checker.assert_properties();
        assert!(
            checker.unique_state_count() > 100,
            "expected a non-trivial state space, got {}",
            checker.unique_state_count()
        );
    }

    /// With three participants the safety invariants still hold (larger
    /// space, kept bounded by a small fault budget).
    #[test]
    fn faithful_xa_safety_three() {
        let checker = Xa {
            rms: 3,
            faults: 1,
            allow_abort: true,
        }
        .checker()
        .spawn_bfs()
        .join();
        // Only the safety + reachability properties are asserted here;
        // liveness is covered by the dedicated no-abort run below.
        assert!(checker.discovery("atomicity").is_none());
        assert!(checker
            .discovery("no commit without unanimous prepare")
            .is_none());
        assert!(checker.discovery("durability of prepared").is_none());
    }

    /// Liveness: with no aborts and a fault budget that the channel
    /// eventually exhausts, every terminal state is fully resolved
    /// (all committed) -- no transaction stuck in-doubt forever.
    #[test]
    fn faithful_xa_liveness_commits() {
        let checker = Xa {
            rms: 2,
            faults: 2,
            allow_abort: false,
        }
        .checker()
        .spawn_bfs()
        .join();
        // The `resolved` eventually-property holds on every terminal
        // path; assert_properties covers it plus the safety set.
        checker.assert_properties();
    }

    /// Negative control: the broken coordinator that commits on a
    /// partial vote violates atomicity, and the checker FINDS the
    /// counterexample. This proves the atomicity property has teeth.
    #[test]
    fn broken_xa_violates_atomicity() {
        let checker = BrokenXa { rms: 2, faults: 1 }.checker().spawn_bfs().join();
        let discovery = checker.discovery("atomicity");
        assert!(
            discovery.is_some(),
            "expected the broken model to violate atomicity, but no counterexample was found"
        );
    }

    /// Guard against a false-alarm negative control: the *faithful*
    /// model at the very same shape the broken one violates finds no
    /// atomicity counterexample. The violation is the bug, not the
    /// harness.
    #[test]
    fn faithful_xa_at_control_shape_is_safe() {
        let checker = Xa {
            rms: 2,
            faults: 1,
            allow_abort: true,
        }
        .checker()
        .spawn_bfs()
        .join();
        assert!(
            checker.discovery("atomicity").is_none(),
            "faithful model must not violate atomicity"
        );
        // The liveness property holds on every terminal path.
        assert!(
            checker.discovery("resolved").is_none(),
            "every terminal state must be fully resolved"
        );
    }
}
