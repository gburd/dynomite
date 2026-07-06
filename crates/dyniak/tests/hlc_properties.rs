//! HLC property tests.
//!
//! Exercises the Hybrid Logical Clock invariants from
//! `crates/dyniak/src/datatypes/hlc.rs` over generated event
//! schedules and random causal DAGs. Each `#[hegel::test]` runs at
//! least 256 generated cases under the default profile.
//!
//! Properties covered:
//!
//! * monotonicity over arbitrary local `tick`/`update` sequences;
//! * `pack`/`unpack` and `encode`/`decode` round-trips;
//! * the packed scalar's numeric order equals the HLC total order;
//! * counter-overflow handling (no panic, no silent wrap);
//! * causality capture over a random DAG of events across nodes: for
//!   every happens-before edge `e -> f`, `hlc(e) < hlc(f)`.

use dyniak::datatypes::{hlc_cmp, Hlc, HlcError};
use hegel::generators as gs;
use hegel::TestCase;

/// A physical-time reading kept well inside the 48-bit range so
/// generated schedules never trip [`HlcError::LogicalOverflow`].
fn arb_pt(tc: &TestCase) -> u64 {
    tc.draw(gs::integers::<u64>().min_value(0).max_value(10_000))
}

/// A well-formed random stamp with a small logical value.
fn arb_stamp(tc: &TestCase) -> Hlc {
    let l = tc.draw(gs::integers::<u64>().min_value(0).max_value(10_000));
    let c = tc.draw(gs::integers::<u16>().min_value(0).max_value(4_000));
    Hlc::from_parts(l, c).expect("in range")
}

// ---- Monotonicity ----------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn tick_is_strictly_monotone(tc: TestCase) {
    // Every local event yields a stamp strictly greater than the
    // previous one, for any schedule of physical-time readings
    // (including backwards jumps and stalls).
    let mut node = Hlc::zero();
    let mut prev = Hlc::zero();
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(30));
    for _ in 0..n {
        let pt = arb_pt(&tc);
        let t = node.tick(pt);
        assert!(t > prev, "tick regressed: {t} !> {prev} at pt={pt}");
        prev = t;
    }
}

#[hegel::test(test_cases = 256)]
fn tick_and_update_mix_is_monotone(tc: TestCase) {
    // Interleaving local events and receives never moves a node's
    // clock backwards.
    let mut node = Hlc::zero();
    let mut prev = Hlc::zero();
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(30));
    for _ in 0..n {
        let pt = arb_pt(&tc);
        let is_receive = tc.draw(gs::booleans());
        let t = if is_receive {
            let msg = arb_stamp(&tc);
            node.update(&msg, pt)
        } else {
            node.tick(pt)
        };
        assert!(t >= prev, "clock regressed: {t} < {prev}");
        prev = t;
    }
}

// ---- Round-trips -----------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn pack_unpack_round_trips(tc: TestCase) {
    let a = arb_stamp(&tc);
    assert_eq!(Hlc::unpack(a.pack()), a);
}

#[hegel::test(test_cases = 256)]
fn encode_decode_round_trips(tc: TestCase) {
    let a = arb_stamp(&tc);
    let bytes = a.encode();
    assert_eq!(Hlc::decode(&bytes), Ok(a));
}

#[hegel::test(test_cases = 256)]
fn decode_rejects_wrong_length(tc: TestCase) {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(16));
    if len == 8 {
        return; // 8 bytes is the valid case, covered above.
    }
    let buf = vec![0u8; len];
    assert_eq!(Hlc::decode(&buf), Err(HlcError::BadEncoding));
}

// ---- Total order -----------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn packed_numeric_order_equals_hlc_order(tc: TestCase) {
    let a = arb_stamp(&tc);
    let b = arb_stamp(&tc);
    assert_eq!(a.pack().cmp(&b.pack()), hlc_cmp(&a, &b));
    // Antisymmetry / totality sanity: exactly one of <, ==, > holds.
    let lt = a < b;
    let gt = a > b;
    let eq = a == b;
    assert_eq!([lt, eq, gt].iter().filter(|x| **x).count(), 1);
}

#[hegel::test(test_cases = 256)]
fn encoded_bytes_sort_in_hlc_order(tc: TestCase) {
    // memcmp on the big-endian encoding must agree with the HLC order,
    // which is what lets version keys sort correctly as key suffixes.
    let a = arb_stamp(&tc);
    let b = arb_stamp(&tc);
    assert_eq!(a.encode().cmp(&b.encode()), hlc_cmp(&a, &b));
}

// ---- Counter overflow ------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn counter_overflow_is_reported_not_wrapped(tc: TestCase) {
    // At the counter ceiling with non-advancing physical time, a tick
    // must error rather than wrap to a smaller stamp (which would
    // violate monotonicity). Advancing physical time must recover.
    let l = tc.draw(gs::integers::<u64>().min_value(1).max_value(10_000));
    let mut node = Hlc::from_parts(l, u16::MAX).expect("ceiling stamp");
    // Non-advancing physical time: overflow.
    let stall = tc.draw(gs::integers::<u64>().min_value(0).max_value(l));
    assert_eq!(node.try_tick(stall), Err(HlcError::CounterOverflow));
    // Advancing physical time: recovers with counter reset to 0.
    let t = node.try_tick(l + 1).expect("advance recovers");
    assert_eq!(t.counter(), 0);
    assert_eq!(t.logical(), l + 1);
}

// ---- Causality capture over a random DAG -----------------------------------

/// One node in the causal-DAG model.
#[derive(Clone)]
struct Node {
    clock: Hlc,
    /// The stamp produced by this node's most recent event, used as
    /// the "message" a later receive edge carries.
    last: Hlc,
}

impl Node {
    /// Local event: advance this node's clock against its own
    /// physical reading and return the produced stamp.
    fn tick(&mut self, physical_now: u64) -> Hlc {
        self.clock.tick(physical_now)
    }

    /// Receive event: merge a received stamp into this node's clock
    /// against its own physical reading and return the produced stamp.
    fn update(&mut self, received: &Hlc, physical_now: u64) -> Hlc {
        self.clock.update(received, physical_now)
    }
}

#[hegel::test(test_cases = 256)]
fn causality_is_captured_over_random_dag(tc: TestCase) {
    // Build a random schedule of events across a few nodes. Each event
    // is either a local tick or a receive of some earlier event's
    // stamp. We record, for every event, the stamp it produced and the
    // set of events that happen-before it. Then we assert that for
    // every happens-before edge e -> f, hlc(e) < hlc(f).
    let node_count = tc.draw(gs::integers::<usize>().min_value(2).max_value(4));
    let mut nodes = vec![
        Node {
            clock: Hlc::zero(),
            last: Hlc::zero(),
        };
        node_count
    ];

    // Per event: (owning node, produced stamp). `edges` lists
    // (cause_event_index, effect_event_index) happens-before pairs.
    let mut stamps: Vec<Hlc> = Vec::new();
    let mut owner: Vec<usize> = Vec::new();
    let mut edges: Vec<(usize, usize)> = Vec::new();
    // The index of each node's most recent event, for the local
    // program-order edge.
    let mut last_event: Vec<Option<usize>> = vec![None; node_count];

    let events = tc.draw(gs::integers::<usize>().min_value(1).max_value(40));
    // Physical time can stall or jump; each node reads its own skewed
    // clock, modelled as a per-node base offset plus a shared tick.
    let skew: Vec<u64> = (0..node_count)
        .map(|_| tc.draw(gs::integers::<u64>().min_value(0).max_value(50)))
        .collect();

    for _ in 0..events {
        let n = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(node_count - 1),
        );
        let base = tc.draw(gs::integers::<u64>().min_value(0).max_value(5_000));
        let pt = base + skew[n];

        let is_receive = tc.draw(gs::booleans()) && !stamps.is_empty();
        let this_index = stamps.len();

        if is_receive {
            // Receive some earlier event's stamp (its cause).
            let src = tc.draw(
                gs::integers::<usize>()
                    .min_value(0)
                    .max_value(stamps.len() - 1),
            );
            let msg = stamps[src];
            let produced = nodes[n].update(&msg, pt);
            nodes[n].last = produced;
            stamps.push(produced);
            owner.push(n);
            // send(src) -> receive(this) is a happens-before edge.
            edges.push((src, this_index));
        } else {
            let produced = nodes[n].tick(pt);
            nodes[n].last = produced;
            stamps.push(produced);
            owner.push(n);
        }

        // Program-order edge from this node's previous event.
        if let Some(prev) = last_event[n] {
            edges.push((prev, this_index));
        }
        last_event[n] = Some(this_index);
    }

    // Causality capture: every happens-before edge is strictly
    // increasing in HLC.
    for &(cause, effect) in &edges {
        assert!(
            stamps[cause] < stamps[effect],
            "causality inversion: event {cause} (node {}, {}) !< event {effect} (node {}, {})",
            owner[cause],
            stamps[cause],
            owner[effect],
            stamps[effect],
        );
    }
}
