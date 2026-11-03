//! Regression: under `DC_QUORUM` the local datastore's reply reached the
//! replica coalescer without a source peer index, so the coalescer filed it
//! under `u32::MAX`, not the local target, and never counted it. With one
//! replica unreachable, the local vote plus one peer vote is a quorum, yet
//! the request hung until the client timed out. Found by the optest
//! `dc_quorum_survives_minority_partition` scenario (C answers in
//! milliseconds; Rust timed out on every request).

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
use dynomite::proto::redis::{CoalesceOutcome, CoalesceTracker};

fn ok_reply(id: u64) -> Msg {
    let pool = MbufPool::default();
    let mut m = Msg::new(id, MsgType::Unknown, false);
    let mut b = pool.get();
    b.recv(b"+OK\r\n");
    m.mbufs_mut().push_back(b);
    m.recompute_mlen();
    m
}

#[test]
fn local_plus_one_peer_is_a_quorum_of_three() {
    let targets = vec![
        (0, "dc1".to_string()),
        (1, "dc1".to_string()),
        (2, "dc1".to_string()),
    ];
    let mut t = CoalesceTracker::new(7, ConsistencyLevel::DcQuorum, targets, "dc1");
    // Local datastore (peer index 0) answers, then peer 1; peer 2 never does.
    assert!(matches!(
        t.record_reply(0, ok_reply(7)),
        CoalesceOutcome::Pending
    ));
    assert!(
        matches!(
            t.record_reply(1, ok_reply(7)),
            CoalesceOutcome::Ready { .. }
        ),
        "two of three matching replies must decide DC_QUORUM"
    );
}

#[test]
fn a_reply_without_its_target_index_does_not_vote() {
    // Pins why the source index must be carried: an unattributed reply is
    // not one of the request's targets and cannot complete the quorum.
    let targets = vec![
        (0, "dc1".to_string()),
        (1, "dc1".to_string()),
        (2, "dc1".to_string()),
    ];
    let mut t = CoalesceTracker::new(7, ConsistencyLevel::DcQuorum, targets, "dc1");
    assert!(matches!(
        t.record_reply(u32::MAX, ok_reply(7)),
        CoalesceOutcome::Pending
    ));
    assert!(matches!(
        t.record_reply(1, ok_reply(7)),
        CoalesceOutcome::Pending
    ));
}

/// Regression: a peer reply arrived as raw bytes with `is_error` unset
/// while the local reply was parsed (`is_error` set). The equivalence key
/// includes `is_error`, so three identical `-NOSCRIPT` replies looked
/// divergent and `DC_SAFE_QUORUM` failed. Replies must be compared after
/// classification; this pins that identical classified errors agree.
#[test]
fn identical_error_replies_agree_under_safe_quorum() {
    fn err(id: u64) -> Msg {
        let mut m = Msg::new(id, MsgType::Unknown, false);
        let pool = MbufPool::default();
        m.append_bytes(&pool, b"-NOSCRIPT No matching script.\r\n");
        m.set_type(MsgType::RspRedisErrorNoscript);
        m.set_is_error(true);
        m
    }
    let targets = vec![
        (0, "dc1".to_string()),
        (1, "dc1".to_string()),
        (2, "dc1".to_string()),
    ];
    let mut t = CoalesceTracker::new(9, ConsistencyLevel::DcSafeQuorum, targets, "dc1");
    assert!(matches!(
        t.record_reply(0, err(9)),
        CoalesceOutcome::Pending
    ));
    assert!(matches!(
        t.record_reply(1, err(9)),
        CoalesceOutcome::Pending
    ));
    assert!(matches!(
        t.record_reply(2, err(9)),
        CoalesceOutcome::Ready { .. }
    ));
}
