//! Coverage for the per-replica reply coalescer state machine
//! (`proto::redis::coalesce::CoalesceTracker`).
//!
//! The in-crate unit tests cover the common quorum decisions; this
//! file pins the accessor surface and the deep paths the unit tests
//! do not reach: the empty-local-DC fallback (targets all in a
//! remote DC), the plurality tiebreak when no payload reaches
//! quorum, and the already-decided `Pending` short-circuit for
//! stragglers.

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::response::make_simple_redis;
use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
use dynomite::proto::redis::{CoalesceOutcome, CoalesceTracker};

fn req() -> Msg {
    Msg::new(1, MsgType::ReqRedisGet, true)
}

fn ok_rsp(payload: &[u8]) -> Msg {
    let pool = MbufPool::default();
    make_simple_redis(&req(), &pool, payload)
}

#[test]
fn tracker_accessors() {
    // req_id / expected / received_count / is_decided reflect the
    // tracker's bound state.
    let mut t = CoalesceTracker::new(
        7,
        ConsistencyLevel::DcQuorum,
        vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
        "dc1",
    );
    assert_eq!(t.req_id(), 7);
    assert_eq!(t.expected(), 3);
    assert_eq!(t.received_count(), 0);
    assert!(!t.is_decided());
    let _ = t.record_reply(0, ok_rsp(b"+OK\r\n"));
    assert_eq!(t.received_count(), 1);
}

#[test]
fn dc_quorum_all_targets_remote_uses_expected_fallback() {
    // When no target is in the local DC, local_count falls back to
    // `expected` (the empty-local-targets branch). A 3-target quorum
    // is reached once two matching replies arrive.
    let mut t = CoalesceTracker::new(
        1,
        ConsistencyLevel::DcQuorum,
        vec![(0, "dcX".into()), (1, "dcX".into()), (2, "dcX".into())],
        "dc1",
    );
    assert!(matches!(
        t.record_reply(0, ok_rsp(b"+OK\r\n")),
        CoalesceOutcome::Pending
    ));
    // Second matching reply reaches the 2-of-3 quorum.
    match t.record_reply(1, ok_rsp(b"+OK\r\n")) {
        CoalesceOutcome::Ready { .. } => {}
        other => panic!("expected Ready, got {other:?}"),
    }
    assert!(t.is_decided());
}

#[test]
fn dc_quorum_already_decided_reports_pending() {
    // After a decision the tracker pins itself and reports Pending
    // for straggler replies.
    let mut t = CoalesceTracker::new(
        1,
        ConsistencyLevel::DcQuorum,
        vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
        "dc1",
    );
    let _ = t.record_reply(0, ok_rsp(b"+OK\r\n"));
    let decided = t.record_reply(1, ok_rsp(b"+OK\r\n"));
    assert!(matches!(decided, CoalesceOutcome::Ready { .. }));
    // A late third reply is dropped via the decided guard.
    assert!(matches!(
        t.record_reply(2, ok_rsp(b"+OK\r\n")),
        CoalesceOutcome::Pending
    ));
}

#[test]
fn dc_quorum_all_disagree_uses_plurality() {
    // Three replies that all disagree never reach the 2-of-3
    // quorum; once every local reply is in, the plurality tiebreak
    // picks a winner and reports the others as divergent.
    let mut t = CoalesceTracker::new(
        1,
        ConsistencyLevel::DcQuorum,
        vec![(0, "dc1".into()), (1, "dc1".into()), (2, "dc1".into())],
        "dc1",
    );
    assert!(matches!(
        t.record_reply(0, ok_rsp(b"$1\r\na\r\n")),
        CoalesceOutcome::Pending
    ));
    assert!(matches!(
        t.record_reply(1, ok_rsp(b"$1\r\nb\r\n")),
        CoalesceOutcome::Pending
    ));
    // Third (last) reply with a third distinct payload: plurality
    // breaks the tie and emits a winner with divergent targets.
    match t.record_reply(2, ok_rsp(b"$1\r\nc\r\n")) {
        CoalesceOutcome::Ready {
            divergent_targets, ..
        } => {
            assert!(
                !divergent_targets.is_empty(),
                "all-disagree plurality should report divergent targets"
            );
        }
        other => panic!("expected Ready, got {other:?}"),
    }
}

#[test]
fn dc_one_first_reply_wins_and_drops_stragglers() {
    // DC_ONE returns the first reply and drops later ones.
    let mut t = CoalesceTracker::new(
        1,
        ConsistencyLevel::DcOne,
        vec![(0, "dc1".into()), (1, "dc1".into())],
        "dc1",
    );
    match t.record_reply(0, ok_rsp(b"+FIRST\r\n")) {
        CoalesceOutcome::Ready { .. } => {}
        other => panic!("expected Ready, got {other:?}"),
    }
    assert!(matches!(
        t.record_reply(1, ok_rsp(b"+SECOND\r\n")),
        CoalesceOutcome::Pending
    ));
}
