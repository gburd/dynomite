//! Bandwidth measurement: state-shipping vs delta-shipping.
//!
//! Drives a realistic mutation sequence on a "source" replica while a
//! "destination" replica reconciles periodically, and compares the
//! total bytes-on-wire under two strategies:
//!
//! * **State shipping** (the current state-based CRDT path): every
//!   reconciliation ships the full [`DeltaOrSet`] state.
//! * **Delta shipping** (this prototype): every reconciliation after
//!   the first ships only the delta-interval since the destination's
//!   last acknowledged point.
//!
//! Byte counts use the same wire accounting (`wire_len`) for both
//! strategies, so the ratio is apples-to-apples. This is a focused
//! measurement test rather than a criterion micro-bench: the quantity
//! we care about is bytes-on-wire, not nanoseconds, and a criterion
//! harness would add plumbing for a number a plain test reports just
//! as well.

use dyniak::aae::{apply_shipment, plan_shipment, Shipment};
use dyniak::datatypes::{ActorId, Crdt, DeltaBuffer, DeltaOrSet};

/// A realistic workload: a large set built up over many rounds, with
/// a modest churn (adds and a few removes) per round. This is the
/// shape anti-entropy sees -- most state is stable, each round only a
/// small fraction changes.
fn workload_rounds() -> usize {
    50
}
fn adds_per_round() -> usize {
    20
}

#[test]
fn delta_shipping_cuts_bandwidth() {
    let actor = ActorId::new("dc1", "src");
    let peer = "dst";

    // --- Delta-shipping run ---
    let mut src = DeltaOrSet::new();
    let mut buf = DeltaBuffer::new();
    let mut dst_delta = DeltaOrSet::new();
    let mut delta_bytes = 0usize;

    let mut counter = 0u64;
    for round in 0..workload_rounds() {
        // Mutate the source: adds plus an occasional remove of an
        // older key to exercise tombstone deltas.
        for _ in 0..adds_per_round() {
            let key = format!("key-{counter:06}");
            buf.record(src.add(&actor, key.into_bytes()));
            counter += 1;
        }
        if round % 5 == 4 && counter > 10 {
            let old = format!("key-{:06}", counter - 10);
            buf.record(src.remove(old.as_bytes()));
        }

        // Reconcile with the destination.
        let ship = plan_shipment(&src, &buf, peer);
        delta_bytes += ship.wire_len();
        if let Some(ack) = apply_shipment(&mut dst_delta, &ship) {
            buf.ack(peer, ack);
        } else if let Shipment::FullState(_) = ship {
            // First contact shipped full state; from now on the peer
            // is current up to the buffer's high-water mark.
            if let Some(hw) = buf.high_water() {
                buf.ack(peer, hw);
            }
        }
        buf.compact();
    }

    // --- State-shipping run (the baseline) ---
    let mut src2 = DeltaOrSet::new();
    let mut dst_state = DeltaOrSet::new();
    let mut state_bytes = 0usize;
    let mut counter2 = 0u64;
    for round in 0..workload_rounds() {
        for _ in 0..adds_per_round() {
            let key = format!("key-{counter2:06}");
            src2.add(&actor, key.into_bytes());
            counter2 += 1;
        }
        if round % 5 == 4 && counter2 > 10 {
            let old = format!("key-{:06}", counter2 - 10);
            src2.remove(old.as_bytes());
        }
        // State shipping always sends the whole state.
        state_bytes += src2.wire_len();
        dst_state.merge(&src2);
    }

    // Both destinations must converge to the same value.
    assert_eq!(dst_delta.value(), src.value());
    assert_eq!(dst_state.value(), src2.value());
    assert_eq!(dst_delta.value(), dst_state.value());

    // Integer ratio avoids a lossy usize->f64 cast; bytes are far
    // below f64's exact range but pedantic clippy forbids the cast.
    let ratio = state_bytes / delta_bytes.max(1);
    eprintln!(
        "bandwidth: state-shipping={state_bytes} bytes, \
         delta-shipping={delta_bytes} bytes, \
         state/delta ratio={ratio}x"
    );

    // Over a stable-with-churn workload, delta shipping should be at
    // least several times cheaper. The first round ships full state
    // in both, so the win compounds with the number of rounds.
    assert!(
        state_bytes > delta_bytes * 3,
        "expected delta shipping >3x cheaper, got {ratio}x \
         (state={state_bytes}, delta={delta_bytes})"
    );
}
