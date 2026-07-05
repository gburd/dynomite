//! Delta-state CRDT property tests.
//!
//! Exercises the delta-CRDT laws for [`DeltaOrSet`]: the lattice
//! laws hold on deltas (a delta is a value in the same lattice as a
//! full state), the delta-mutation theorem (state after N
//! delta-merges equals state after the equivalent full-state
//! merges), add-wins semantics are preserved, and out-of-order /
//! duplicated delta delivery still converges.
//!
//! Each `#[hegel::test]` runs at least 256 generated cases.

use dyniak::datatypes::{ActorId, Crdt, DeltaOrSet, OrSetDelta};
use hegel::generators as gs;
use hegel::TestCase;

const DOMAIN: [&[u8]; 4] = [b"x", b"y", b"z", b"w"];

fn arb_actor(tc: &TestCase) -> ActorId {
    let dc = tc.draw(gs::sampled_from(&["dc1".to_string(), "dc2".to_string()]));
    let peer = tc.draw(gs::sampled_from(&[
        "p1".to_string(),
        "p2".to_string(),
        "p3".to_string(),
    ]));
    ActorId::new(dc, peer)
}

/// One generated mutation.
#[derive(Clone, Debug)]
enum Op {
    Add(ActorId, usize),
    Remove(usize),
}

fn arb_ops(tc: &TestCase) -> Vec<Op> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(12));
    let mut ops = Vec::with_capacity(n);
    for _ in 0..n {
        let pick = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(DOMAIN.len() - 1),
        );
        if tc.draw(gs::booleans()) {
            ops.push(Op::Remove(pick));
        } else {
            ops.push(Op::Add(arb_actor(tc), pick));
        }
    }
    ops
}

/// Apply an op log to a set, collecting the non-empty deltas.
fn run_ops(set: &mut DeltaOrSet, ops: &[Op]) -> Vec<OrSetDelta> {
    let mut deltas = Vec::new();
    for op in ops {
        let d = match op {
            Op::Add(a, i) => set.add(a, DOMAIN[*i].to_vec()),
            Op::Remove(i) => set.remove(DOMAIN[*i]),
        };
        if !d.is_empty() {
            deltas.push(d);
        }
    }
    deltas
}

/// Build a set from an op log (state-based reference).
fn build(tc: &TestCase) -> DeltaOrSet {
    let mut s = DeltaOrSet::new();
    run_ops(&mut s, &arb_ops(tc));
    s
}

// ---- Lattice laws on deltas (a delta is a state-lattice value) ------------

#[hegel::test(test_cases = 256)]
fn delta_join_is_commutative(tc: TestCase) {
    let mut sa = DeltaOrSet::new();
    let da = run_ops(&mut sa, &arb_ops(&tc));
    let mut sb = DeltaOrSet::new();
    let db = run_ops(&mut sb, &arb_ops(&tc));

    let mut left = DeltaOrSet::new();
    for d in da.iter().chain(db.iter()) {
        left.merge_delta(d);
    }
    let mut right = DeltaOrSet::new();
    for d in db.iter().chain(da.iter()) {
        right.merge_delta(d);
    }
    assert_eq!(left, right);
}

#[hegel::test(test_cases = 256)]
fn delta_join_is_associative(tc: TestCase) {
    // Fold three delta streams two different ways.
    let mut s1 = DeltaOrSet::new();
    let d1 = run_ops(&mut s1, &arb_ops(&tc));
    let mut s2 = DeltaOrSet::new();
    let d2 = run_ops(&mut s2, &arb_ops(&tc));
    let mut s3 = DeltaOrSet::new();
    let d3 = run_ops(&mut s3, &arb_ops(&tc));

    let mut left = DeltaOrSet::new();
    for d in d1.iter().chain(d2.iter()).chain(d3.iter()) {
        left.merge_delta(d);
    }
    // (d2 . d3) then d1 -- collapse d2,d3 into one interval first.
    let mut mid = OrSetDelta::default();
    for d in d2.iter().chain(d3.iter()) {
        mid.join(d);
    }
    let mut right = DeltaOrSet::new();
    for d in &d1 {
        right.merge_delta(d);
    }
    right.merge_delta(&mid);
    assert_eq!(left, right);
}

#[hegel::test(test_cases = 256)]
fn delta_join_is_idempotent(tc: TestCase) {
    let mut s = DeltaOrSet::new();
    let deltas = run_ops(&mut s, &arb_ops(&tc));
    let mut once = DeltaOrSet::new();
    for d in &deltas {
        once.merge_delta(d);
    }
    let mut twice = once.clone();
    for d in &deltas {
        twice.merge_delta(d);
    }
    assert_eq!(once, twice);
}

// ---- The delta-mutation theorem -------------------------------------------

#[hegel::test(test_cases = 256)]
fn delta_merges_equal_full_state_merge(tc: TestCase) {
    // A replica runs an op log, capturing deltas.
    let mut producer = DeltaOrSet::new();
    let deltas = run_ops(&mut producer, &arb_ops(&tc));

    // Path 1: N delta-merges onto a fresh replica.
    let mut via_deltas = DeltaOrSet::new();
    for d in &deltas {
        via_deltas.merge_delta(d);
    }
    // Path 2: one full-state merge of the producer.
    let mut via_state = DeltaOrSet::new();
    via_state.merge(&producer);

    assert_eq!(via_deltas, via_state);
    assert_eq!(via_deltas.value(), via_state.value());
}

#[hegel::test(test_cases = 256)]
fn delta_merge_matches_state_merge_across_two_replicas(tc: TestCase) {
    // Two replicas each run a log; each ships the other its deltas.
    let mut a = DeltaOrSet::new();
    let da = run_ops(&mut a, &arb_ops(&tc));
    let mut b = DeltaOrSet::new();
    let db = run_ops(&mut b, &arb_ops(&tc));

    // Delta path: a gets b's deltas, b gets a's deltas.
    let mut a_delta = a.clone();
    for d in &db {
        a_delta.merge_delta(d);
    }
    let mut b_delta = b.clone();
    for d in &da {
        b_delta.merge_delta(d);
    }

    // State path: full-state merge both ways.
    let mut a_state = a.clone();
    a_state.merge(&b);
    let mut b_state = b.clone();
    b_state.merge(&a);

    // Delta and state paths converge to the same lattice value, and
    // both replicas agree (strong eventual consistency).
    assert_eq!(a_delta, a_state);
    assert_eq!(b_delta, b_state);
    assert_eq!(a_delta.value(), b_delta.value());
}

// ---- Out-of-order and duplicate delivery ----------------------------------

#[hegel::test(test_cases = 256)]
fn shuffled_duplicated_deltas_converge(tc: TestCase) {
    let mut producer = DeltaOrSet::new();
    let deltas = run_ops(&mut producer, &arb_ops(&tc));

    // Draw a delivery schedule that reorders and duplicates deltas.
    let mut schedule: Vec<usize> = Vec::new();
    if !deltas.is_empty() {
        let steps = tc.draw(gs::integers::<usize>().min_value(0).max_value(30));
        for _ in 0..steps {
            schedule.push(
                tc.draw(
                    gs::integers::<usize>()
                        .min_value(0)
                        .max_value(deltas.len() - 1),
                ),
            );
        }
        // Guarantee every delta is delivered at least once so the
        // "same SET of deltas" precondition holds.
        for i in 0..deltas.len() {
            schedule.push(i);
        }
    }

    let mut replica = DeltaOrSet::new();
    for idx in schedule {
        replica.merge_delta(&deltas[idx]);
    }
    // Any two replicas delivered the same set of deltas (in any
    // order, with duplicates) equal the full-state producer.
    assert_eq!(replica.value(), producer.value());
    assert_eq!(replica, producer);
}

// ---- Add-wins semantics preserved -----------------------------------------

#[hegel::test(test_cases = 256)]
fn add_wins_over_concurrent_remove(tc: TestCase) {
    // Two replicas share a seed with the element added. One removes
    // it; the other, concurrently, adds it again with a fresh dot.
    // The concurrent add must survive after cross-shipping deltas.
    let a = arb_actor(&tc);
    let b = arb_actor(&tc);
    let pick = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(DOMAIN.len() - 1),
    );
    let elem = DOMAIN[pick];

    let mut seed = DeltaOrSet::new();
    let seed_delta = seed.add(&a, elem.to_vec());

    let mut left = DeltaOrSet::new();
    left.merge_delta(&seed_delta);
    let rem = left.remove(elem);

    let mut right = DeltaOrSet::new();
    right.merge_delta(&seed_delta);
    let add = right.add(&b, elem.to_vec());

    left.merge_delta(&add);
    right.merge_delta(&rem);

    assert!(left.contains(elem), "add must win the concurrent remove");
    assert!(right.contains(elem));
    assert_eq!(left.value(), right.value());
}

#[hegel::test(test_cases = 256)]
fn value_projection_matches_presence(tc: TestCase) {
    let s = build(&tc);
    let value = s.value();
    for e in &DOMAIN {
        assert_eq!(value.contains(*e), s.contains(e));
    }
}
