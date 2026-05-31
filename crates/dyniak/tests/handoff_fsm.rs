//! Integration tests for the explicit handoff FSM.
//!
//! Covers every named test from the brief plus a property test
//! that drives the FSM through arbitrary chunk sizes, range
//! sizes, and ack reorderings.

use std::time::Duration;

use dyniak::handoff::{
    Chunk, Event, HandoffHandler, HandoffOutcome, SendRequest, State, FINALIZING_STATE_TIMEOUT,
    FLUSHING_STATE_TIMEOUT, NEGOTIATING_STATE_TIMEOUT, SENDING_EVENT_TIMEOUT,
};
use dynomite::events::{PeerId, TokenRange};
use dynomite::hashkit::DynToken;
use gen_fsm::{Action, EventType, FsmHandler, TimeoutKind, Transition};
use hegel::generators as gs;
use hegel::TestCase;

const SRC: PeerId = 1;
const DST: PeerId = 2;

fn range() -> TokenRange {
    TokenRange::new(DynToken::from_u32(0), DynToken::from_u32(8192))
}

fn fresh(total: u64) -> HandoffHandler {
    // chunk_size 64, in_flight 4, throttle 1M chunks/s so the
    // throttle never gates correctness in the unit cases.
    HandoffHandler::with_settings(SRC, DST, range(), total, 64, 4, 1_000_000)
}

fn send_request(total: u64) -> SendRequest {
    SendRequest {
        src_peer: SRC,
        dst_peer: DST,
        token_range: range(),
        total_keys: total,
    }
}

fn assert_actions_contain_set_state(transition: &Transition<HandoffHandler>, expected: Duration) {
    match transition {
        Transition::Keep(actions) | Transition::Next(_, actions) => {
            let found = actions
                .iter()
                .any(|a| matches!(a, Action::SetStateTimeout(d) if *d == expected));
            assert!(
                found,
                "expected SetStateTimeout({expected:?}); got {actions:?}"
            );
        }
        Transition::Stop(_) => panic!("expected Keep/Next, got Stop"),
    }
}

fn assert_actions_contain_set_event(transition: &Transition<HandoffHandler>, expected: Duration) {
    match transition {
        Transition::Keep(actions) | Transition::Next(_, actions) => {
            let found = actions
                .iter()
                .any(|a| matches!(a, Action::SetEventTimeout(d) if *d == expected));
            assert!(
                found,
                "expected SetEventTimeout({expected:?}); got {actions:?}"
            );
        }
        Transition::Stop(_) => panic!("expected Keep/Next, got Stop"),
    }
}

#[test]
fn init_state_sets_state_timeout() {
    // Init has no per-state timer of its own (it is awaiting a
    // wire-side SendRequest that the receiver will eventually
    // dispatch); the sender's first armed timer fires when the
    // FSM transitions into Negotiating. Drive the transition
    // and confirm the 10s state timeout is requested.
    let mut h = fresh(8);
    assert_eq!(h.initial(), State::Init);
    let t = h.handle(
        State::Init,
        EventType::Cast,
        Event::SendRequestReceived(send_request(8)),
    );
    match t {
        Transition::Next(State::Negotiating, _) => {}
        other => panic!("expected Next(Negotiating), got {other:?}"),
    }
    let entry = h.on_enter(State::Negotiating);
    assert_actions_contain_set_state(&entry, NEGOTIATING_STATE_TIMEOUT);
}

#[test]
fn negotiating_accepted_advances_to_sending() {
    let mut h = fresh(8);
    let _ = h.handle(
        State::Init,
        EventType::Cast,
        Event::SendRequestReceived(send_request(8)),
    );
    let _ = h.on_enter(State::Negotiating);
    let t = h.handle(
        State::Negotiating,
        EventType::Cast,
        Event::NegotiationAck {
            accepted: true,
            max_chunk_size: 0,
        },
    );
    match t {
        Transition::Next(State::Sending, _) => {}
        other => panic!("expected Next(Sending), got {other:?}"),
    }
    // Confirm the 30s event timeout is armed on entry.
    let entry = h.on_enter(State::Sending);
    assert_actions_contain_set_event(&entry, SENDING_EVENT_TIMEOUT);
}

#[test]
fn negotiating_rejected_advances_to_failed() {
    let mut h = fresh(8);
    let t = h.handle(
        State::Negotiating,
        EventType::Cast,
        Event::NegotiationAck {
            accepted: false,
            max_chunk_size: 0,
        },
    );
    match t {
        Transition::Next(State::Failed, _) => {}
        other => panic!("expected Next(Failed), got {other:?}"),
    }
    let stop = h.on_enter(State::Failed);
    match stop {
        Transition::Stop(HandoffOutcome::Failed { reason, .. }) => {
            assert!(
                reason.contains("rejected"),
                "expected reason to mention 'rejected', got {reason:?}"
            );
        }
        other => panic!("expected Stop(Failed), got {other:?}"),
    }
}

#[test]
fn sending_acks_increment_offset() {
    let mut h = fresh(256);
    // Mark four chunks as built.
    for chunk_id in 0..4 {
        let t = h.handle(
            State::Sending,
            EventType::Cast,
            Event::NextChunkBuilt(Chunk { chunk_id, keys: 64 }),
        );
        // Each NextChunkBuilt re-arms the per-event timer.
        assert_actions_contain_set_event(&t, SENDING_EVENT_TIMEOUT);
    }
    assert_eq!(h.sent_chunks(), 4);
    assert_eq!(h.acked_chunks(), 0);
    // Ack two of them.
    for chunk_id in [0u64, 1u64] {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id },
        );
    }
    assert_eq!(h.acked_chunks(), 2);
    // Backpressure window: max_in_flight=4 and sent-acked==2,
    // so two more chunks of slack.
    assert!(h.has_in_flight_capacity());
}

#[test]
fn sending_event_timeout_advances_to_failed() {
    let mut h = fresh(64);
    let t = h.on_timeout(State::Sending, TimeoutKind::Event);
    match t {
        Transition::Next(State::Failed, _) => {}
        other => panic!("expected Next(Failed), got {other:?}"),
    }
    let stop = h.on_enter(State::Failed);
    match stop {
        Transition::Stop(HandoffOutcome::Failed {
            reason, last_state, ..
        }) => {
            assert!(
                reason.contains("event timeout"),
                "expected reason to mention 'event timeout', got {reason:?}"
            );
            assert_eq!(last_state, State::Sending);
        }
        other => panic!("expected Stop(Failed), got {other:?}"),
    }
}

#[test]
fn batch_done_advances_to_flushing() {
    let mut h = fresh(64);
    let t = h.handle(State::Sending, EventType::Cast, Event::BatchDone);
    match t {
        Transition::Next(State::Flushing, _) => {}
        other => panic!("expected Next(Flushing), got {other:?}"),
    }
    let entry = h.on_enter(State::Flushing);
    assert_actions_contain_set_state(&entry, FLUSHING_STATE_TIMEOUT);
}

#[test]
fn flushing_finalize_advances_to_finalizing() {
    let mut h = fresh(64);
    let t = h.handle(State::Flushing, EventType::Cast, Event::BatchAcked);
    match t {
        Transition::Next(State::Finalizing, _) => {}
        other => panic!("expected Next(Finalizing), got {other:?}"),
    }
    let entry = h.on_enter(State::Finalizing);
    assert_actions_contain_set_state(&entry, FINALIZING_STATE_TIMEOUT);
}

#[test]
fn finalizing_ack_stops_with_completed_outcome() {
    let mut h = fresh(64);
    // Pre-load some acked work so the completed outcome
    // reports a non-zero key count.
    let _ = h.handle(
        State::Sending,
        EventType::Cast,
        Event::NextChunkBuilt(Chunk {
            chunk_id: 0,
            keys: 64,
        }),
    );
    let _ = h.handle(
        State::Sending,
        EventType::Cast,
        Event::ChunkAcked { chunk_id: 0 },
    );
    let stop = h.handle(State::Finalizing, EventType::Cast, Event::FinalizeAcked);
    match stop {
        Transition::Stop(HandoffOutcome::Completed {
            keys_transferred, ..
        }) => {
            assert_eq!(keys_transferred, 64);
        }
        other => panic!("expected Stop(Completed), got {other:?}"),
    }
}

#[test]
fn peer_error_at_any_state_stops_with_failed() {
    for state in [
        State::Init,
        State::Negotiating,
        State::Sending,
        State::Flushing,
        State::Finalizing,
    ] {
        let mut h = fresh(64);
        let t = h.handle(
            state,
            EventType::Cast,
            Event::PeerError(format!("boom in {state:?}")),
        );
        match t {
            Transition::Next(State::Failed, _) => {}
            other => panic!("expected Next(Failed) from {state:?}, got {other:?}"),
        }
        let stop = h.on_enter(State::Failed);
        match stop {
            Transition::Stop(HandoffOutcome::Failed {
                reason, last_state, ..
            }) => {
                assert!(reason.contains("boom"));
                assert_eq!(last_state, state);
            }
            other => panic!("expected Stop(Failed) from {state:?}, got {other:?}"),
        }
    }
}

#[test]
fn out_of_order_chunk_acks_tolerated() {
    let mut h = fresh(256);
    // Build four chunks.
    for chunk_id in 0..4 {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::NextChunkBuilt(Chunk { chunk_id, keys: 64 }),
        );
    }
    // Ack them in reverse order with a duplicate sprinkled in.
    let order = [3u64, 1u64, 2u64, 1u64, 0u64];
    for chunk_id in order {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id },
        );
    }
    // Four unique chunks, four acks.
    assert_eq!(h.acked_chunks(), 4);
}

#[test]
fn chunk_count_matches_acked_count_at_completion() {
    let mut h = fresh(256);
    // Drive a full 4-chunk run end to end.
    let _ = h.handle(
        State::Init,
        EventType::Cast,
        Event::SendRequestReceived(send_request(256)),
    );
    let _ = h.on_enter(State::Negotiating);
    let _ = h.handle(
        State::Negotiating,
        EventType::Cast,
        Event::NegotiationAck {
            accepted: true,
            max_chunk_size: 0,
        },
    );
    let _ = h.on_enter(State::Sending);
    for chunk_id in 0..4 {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::NextChunkBuilt(Chunk { chunk_id, keys: 64 }),
        );
    }
    for chunk_id in 0..4 {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id },
        );
    }
    let _ = h.handle(State::Sending, EventType::Cast, Event::BatchDone);
    let _ = h.on_enter(State::Flushing);
    let _ = h.handle(State::Flushing, EventType::Cast, Event::BatchAcked);
    let _ = h.on_enter(State::Finalizing);
    let stop = h.handle(State::Finalizing, EventType::Cast, Event::FinalizeAcked);
    match stop {
        Transition::Stop(HandoffOutcome::Completed {
            keys_transferred, ..
        }) => {
            assert_eq!(h.sent_chunks(), 4);
            assert_eq!(h.acked_chunks(), 4);
            assert_eq!(keys_transferred, 256);
        }
        other => panic!("expected Stop(Completed), got {other:?}"),
    }
}

#[test]
fn flushing_state_timeout_advances_to_failed() {
    let mut h = fresh(64);
    let t = h.on_timeout(State::Flushing, TimeoutKind::State);
    match t {
        Transition::Next(State::Failed, _) => {}
        other => panic!("expected Next(Failed), got {other:?}"),
    }
    let stop = h.on_enter(State::Failed);
    match stop {
        Transition::Stop(HandoffOutcome::Failed {
            reason, last_state, ..
        }) => {
            assert!(reason.contains("state timeout"));
            assert_eq!(last_state, State::Flushing);
        }
        other => panic!("expected Stop(Failed), got {other:?}"),
    }
}

#[test]
fn finalizing_state_timeout_advances_to_failed() {
    let mut h = fresh(64);
    let t = h.on_timeout(State::Finalizing, TimeoutKind::State);
    match t {
        Transition::Next(State::Failed, _) => {}
        other => panic!("expected Next(Failed), got {other:?}"),
    }
}

#[test]
fn partial_count_reflects_acked_keys_on_failure() {
    let mut h = fresh(256);
    // Two chunks acked, then a peer error.
    for chunk_id in 0..2 {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::NextChunkBuilt(Chunk { chunk_id, keys: 64 }),
        );
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id },
        );
    }
    let _ = h.handle(
        State::Sending,
        EventType::Cast,
        Event::PeerError("link dropped".into()),
    );
    let stop = h.on_enter(State::Failed);
    match stop {
        Transition::Stop(HandoffOutcome::Failed {
            reason,
            partial_count,
            last_state,
        }) => {
            assert_eq!(partial_count, 128);
            assert_eq!(last_state, State::Sending);
            assert!(reason.contains("link dropped"));
        }
        other => panic!("expected Stop(Failed), got {other:?}"),
    }
}

#[test]
fn backpressure_blocks_when_window_full() {
    let mut h = HandoffHandler::with_settings(SRC, DST, range(), 256, 64, 2, 1_000_000);
    assert!(h.has_in_flight_capacity());
    for chunk_id in 0..2 {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::NextChunkBuilt(Chunk { chunk_id, keys: 64 }),
        );
    }
    assert!(!h.has_in_flight_capacity(), "window should be full at 2/2");
    // Ack one, slack returns.
    let _ = h.handle(
        State::Sending,
        EventType::Cast,
        Event::ChunkAcked { chunk_id: 0 },
    );
    assert!(h.has_in_flight_capacity());
}

#[test]
fn throttle_admits_up_to_burst() {
    let h = HandoffHandler::with_settings(SRC, DST, range(), 256, 64, 4, 4);
    // Burst capacity equals chunks_per_sec (default), so the
    // first 4 admit attempts succeed, the fifth fails until
    // a token refills.
    for _ in 0..4 {
        assert!(h.try_admit_chunk());
    }
    assert!(!h.try_admit_chunk());
}

// ---- Property test --------------------------------------------------------
//
// The brief asks: for arbitrary chunk sizes, range sizes, and
// ack reorderings, the FSM transfers all keys exactly once if
// no errors fire, OR stops in Failed with partial_count <=
// total_keys.

fn shuffle(tc: &TestCase, mut v: Vec<u64>) -> Vec<u64> {
    // Fisher-Yates with hegel-drawn swaps. Deterministic per
    // test case so shrinking still works.
    if v.len() <= 1 {
        return v;
    }
    let max_idx = v.len() - 1;
    for i in (1..v.len()).rev() {
        let j = tc.draw(gs::integers::<usize>().min_value(0).max_value(max_idx));
        let j = j % (i + 1);
        v.swap(i, j);
    }
    v
}

#[hegel::test(test_cases = 256)]
fn handoff_drives_to_completed_or_failed_with_bounded_partial(tc: TestCase) {
    let chunk_size = tc.draw(gs::integers::<u64>().min_value(1).max_value(8192));
    let total_keys = tc.draw(gs::integers::<u64>().min_value(1).max_value(10_000));
    let max_in_flight = tc.draw(gs::integers::<u64>().min_value(1).max_value(8));
    let inject_error = tc.draw(gs::booleans());
    let error_after = tc.draw(
        gs::integers::<u64>()
            .min_value(0)
            .max_value(total_keys.div_ceil(chunk_size)),
    );

    let mut h = HandoffHandler::with_settings(
        SRC,
        DST,
        range(),
        total_keys,
        chunk_size,
        max_in_flight,
        1_000_000_000,
    );

    let _ = h.handle(
        State::Init,
        EventType::Cast,
        Event::SendRequestReceived(send_request(total_keys)),
    );
    let _ = h.on_enter(State::Negotiating);
    let _ = h.handle(
        State::Negotiating,
        EventType::Cast,
        Event::NegotiationAck {
            accepted: true,
            max_chunk_size: chunk_size,
        },
    );
    let _ = h.on_enter(State::Sending);

    let chunks = total_keys.div_ceil(chunk_size);
    let mut produced: u64 = 0;
    let mut keys_left = total_keys;
    while produced < chunks {
        let take = keys_left.min(chunk_size);
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::NextChunkBuilt(Chunk {
                chunk_id: produced,
                keys: take,
            }),
        );
        keys_left -= take;
        produced += 1;
    }

    if inject_error && error_after < produced {
        // Ack the chunks up to error_after first, then trip a
        // peer error.
        let mut ids: Vec<u64> = (0..error_after).collect();
        ids = shuffle(&tc, ids);
        for id in ids {
            let _ = h.handle(
                State::Sending,
                EventType::Cast,
                Event::ChunkAcked { chunk_id: id },
            );
        }
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::PeerError("synthetic".into()),
        );
        let stop = h.on_enter(State::Failed);
        match stop {
            Transition::Stop(HandoffOutcome::Failed { partial_count, .. }) => {
                assert!(
                    partial_count <= total_keys,
                    "partial_count {partial_count} > total {total_keys}",
                );
                let acked = h.acked_chunks();
                let expected = (acked.saturating_mul(chunk_size)).min(total_keys);
                assert_eq!(partial_count, expected);
            }
            other => panic!("expected Stop(Failed), got {other:?}"),
        }
        return;
    }

    // Happy path: ack every chunk in a shuffled order with
    // duplicates inserted; the FSM must still converge.
    let mut ids: Vec<u64> = (0..produced).collect();
    ids = shuffle(&tc, ids);
    let dup_count = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    for _ in 0..dup_count {
        if !ids.is_empty() {
            let pick = tc.draw(
                gs::integers::<usize>()
                    .min_value(0)
                    .max_value(ids.len() - 1),
            );
            let id = ids[pick];
            ids.push(id);
        }
    }
    for id in ids {
        let _ = h.handle(
            State::Sending,
            EventType::Cast,
            Event::ChunkAcked { chunk_id: id },
        );
    }
    let _ = h.handle(State::Sending, EventType::Cast, Event::BatchDone);
    let _ = h.on_enter(State::Flushing);
    let _ = h.handle(State::Flushing, EventType::Cast, Event::BatchAcked);
    let _ = h.on_enter(State::Finalizing);
    let stop = h.handle(State::Finalizing, EventType::Cast, Event::FinalizeAcked);
    match stop {
        Transition::Stop(HandoffOutcome::Completed {
            keys_transferred, ..
        }) => {
            assert_eq!(
                keys_transferred, total_keys,
                "expected to transfer all {total_keys} keys, got {keys_transferred}",
            );
            assert_eq!(h.sent_chunks(), produced);
            assert_eq!(h.acked_chunks(), produced);
        }
        other => panic!("expected Stop(Completed), got {other:?}"),
    }
}
