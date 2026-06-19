//! Interval-tree-clock (`Itc`) coverage and property tests.
//!
//! Drives the fork / join / event / peek / send / receive
//! lifecycle, the `encode`/`decode` bit-packed round-trip
//! (including malformed-input rejection), the `Display` impl,
//! and the causal-ordering predicates `leq` /
//! `partial_cmp_event`. The property tests run >= 256 cases.

use std::cmp::Ordering;

use dyniak::datatypes::{Itc, ItcEvent, ItcId};
use hegel::generators as gs;
use hegel::TestCase;

/// Apply a pseudo-random sequence of ITC operations and return
/// the resulting live stamps. `seed` controls the operation
/// mix so the same draw replays deterministically.
fn build_history(ops: &[u8]) -> Vec<Itc> {
    let mut stamps = vec![Itc::seed()];
    for &op in ops {
        match op % 4 {
            0 => {
                // Fork the first authority-holding stamp.
                if let Some(pos) = stamps.iter().position(Itc::has_authority) {
                    let s = stamps.remove(pos);
                    let (a, b) = s.fork();
                    stamps.push(a);
                    stamps.push(b);
                }
            }
            1 => {
                // Event on the first authority-holding stamp.
                if let Some(s) = stamps.iter_mut().find(|s| s.has_authority()) {
                    s.event();
                }
            }
            2 => {
                // Join the last two stamps (always disjoint ids
                // because they all descend from the same seed
                // via forks).
                if stamps.len() >= 2 {
                    let b = stamps.pop().unwrap();
                    let a = stamps.pop().unwrap();
                    stamps.push(a.join(b));
                }
            }
            _ => {
                // Send/receive between the first two stamps.
                if stamps.len() >= 2 {
                    let sender = stamps.remove(0);
                    let (sender, msg) = sender.send();
                    let receiver = stamps.remove(0);
                    stamps.insert(0, receiver.receive(msg));
                    stamps.push(sender);
                }
            }
        }
        if stamps.is_empty() {
            stamps.push(Itc::seed());
        }
    }
    stamps
}

fn arb_ops(tc: &TestCase) -> Vec<u8> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(24));
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(tc.draw(gs::integers::<u8>()));
    }
    out
}

// ---- unit tests ---------------------------------------------------------

#[test]
fn seed_has_authority_and_peek_drops_it() {
    let s = Itc::seed();
    assert!(s.has_authority());
    let p = s.peek();
    assert!(!p.has_authority());
    // The peek shares the same event tree.
    assert_eq!(p.event_tree(), s.event_tree());
    assert_eq!(p.id(), &ItcId::zero());
}

#[test]
fn event_advances_strictly() {
    let mut s = Itc::seed();
    let before = s.clone();
    s.event();
    // After an event, the new stamp strictly succeeds the old.
    assert_eq!(before.partial_cmp_event(&s), Some(Ordering::Less));
    assert!(before.leq(&s));
    assert!(!s.leq(&before));
}

#[test]
fn fork_then_join_recovers_causal_equality() {
    let mut s = Itc::seed();
    s.event();
    let snapshot = s.clone();
    let (a, b) = s.fork();
    // Re-joining the two halves yields a stamp causally equal
    // to the pre-fork snapshot (the event tree is unchanged).
    let rejoined = a.join(b);
    assert_eq!(snapshot.partial_cmp_event(&rejoined), Some(Ordering::Equal));
}

#[test]
fn concurrent_forks_are_incomparable_after_independent_events() {
    let (mut a, mut b) = Itc::seed().fork();
    a.event();
    b.event();
    // Two independent events on disjoint forks are concurrent.
    assert_eq!(a.partial_cmp_event(&b), None);
}

#[test]
fn send_receive_establishes_happens_before() {
    let sender = Itc::seed();
    let (sender_after, msg) = sender.send();
    // A forked half receives the shipped peek; the shipped peek
    // precedes the post-receive stamp.
    let (recv_half, _other) = Itc::seed().fork();
    let merged = recv_half.receive(msg.clone());
    assert!(msg.leq(&merged));
    // The sender's own post-send stamp is reflexively <= itself.
    assert!(sender_after.leq(&sender_after));
}

#[test]
fn encode_decode_round_trip_seed_and_evolved() {
    for stamp in [
        Itc::seed(),
        {
            let mut s = Itc::seed();
            s.event();
            s.event();
            s
        },
        {
            let (a, b) = Itc::seed().fork();
            let mut a = a;
            a.event();
            a.join(b)
        },
    ] {
        let bytes = stamp.encode();
        let back = Itc::decode(&bytes).expect("decode");
        // Round-trip is causally equal and structurally equal.
        assert_eq!(stamp.partial_cmp_event(&back), Some(Ordering::Equal));
        assert_eq!(back.encode(), bytes, "re-encode is stable");
    }
}

#[test]
fn decode_rejects_malformed_bytes() {
    // Too short for the 4-byte length prefix.
    assert!(Itc::decode(&[]).is_none());
    assert!(Itc::decode(&[0, 0, 0]).is_none());
    // Length prefix claims more bits than the payload carries.
    assert!(Itc::decode(&[0, 0, 0, 200, 0x00]).is_none());
    // Payload length does not match the declared bit count.
    assert!(Itc::decode(&[0, 0, 0, 8, 0x00, 0x00]).is_none());
}

#[test]
fn display_is_non_empty_for_node_trees() {
    let (a, b) = Itc::seed().fork();
    let joined = a.join(b);
    let shown = format!("{joined}");
    assert!(!shown.is_empty());
    // A seed stamp also renders.
    assert!(!format!("{}", Itc::seed()).is_empty());
}

#[test]
fn from_parts_normalises() {
    // Build a stamp from raw trees; from_parts normalises them.
    let s = Itc::from_parts(ItcId::one(), ItcEvent::zero());
    assert_eq!(s.partial_cmp_event(&Itc::seed()), Some(Ordering::Equal));
}

// ---- property tests -----------------------------------------------------

/// For any operation history, every live stamp's `encode` then
/// `decode` round-trips to a causally equal stamp and the
/// re-encoding is byte-stable.
#[hegel::test(test_cases = 256)]
fn encode_decode_round_trips_over_histories(tc: TestCase) {
    let ops = arb_ops(&tc);
    for stamp in build_history(&ops) {
        let bytes = stamp.encode();
        let back = Itc::decode(&bytes).expect("decode round-trips");
        assert_eq!(stamp.partial_cmp_event(&back), Some(Ordering::Equal));
        assert_eq!(back.encode(), bytes);
    }
}

/// `partial_cmp_event` is consistent with the two `leq`
/// directions for any pair of stamps drawn from a shared
/// history: Equal iff both leq, Less iff only forward leq, and
/// so on.
#[hegel::test(test_cases = 256)]
fn partial_cmp_consistent_with_leq(tc: TestCase) {
    let ops = arb_ops(&tc);
    let stamps = build_history(&ops);
    if stamps.len() < 2 {
        return;
    }
    let i = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(stamps.len() - 1),
    );
    let j = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(stamps.len() - 1),
    );
    let a = &stamps[i];
    let b = &stamps[j];
    let expected = match (a.leq(b), b.leq(a)) {
        (true, true) => Some(Ordering::Equal),
        (true, false) => Some(Ordering::Less),
        (false, true) => Some(Ordering::Greater),
        (false, false) => None,
    };
    assert_eq!(a.partial_cmp_event(b), expected);
}

/// An `event` always advances the stamp: the post-event stamp
/// is greater-or-equal to the pre-event stamp (strictly greater
/// when the stamp holds authority).
#[hegel::test(test_cases = 256)]
fn event_never_regresses(tc: TestCase) {
    let ops = arb_ops(&tc);
    let stamps = build_history(&ops);
    let idx = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(stamps.len() - 1),
    );
    let before = stamps[idx].clone();
    let mut after = before.clone();
    if !after.has_authority() {
        return;
    }
    after.event();
    // before <= after always holds.
    assert!(before.leq(&after));
}
