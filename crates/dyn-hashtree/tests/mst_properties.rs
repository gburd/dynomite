//! Property tests for `hashtree::mst::Mst`.
//!
//! Each `#[hegel::test]` runs at least 256 generated cases under
//! the default profile. The properties encode the three MST
//! guarantees the AAE reconcile relies on:
//!
//! * structure is deterministic in the key set (same keys ->
//!   same root, any insertion order);
//! * reconcile(A, B) yields A union B on both sides;
//! * diff cost (node comparisons) is bounded by a function of
//!   the symmetric-difference size, not the dataset size.
//!
//! Disabled under `--cfg loom` for the same reason as the
//! `HashTree` property tests.

#![cfg(not(loom))]

use std::collections::BTreeMap;

use hashtree::mst::Mst;
use hashtree::Hash;
use hegel::generators as gs;
use hegel::TestCase;

fn h(seed: u8) -> Hash {
    *blake3::hash(&[seed]).as_bytes()
}

/// Draw a small `(key -> value_hash)` map. Keys are short byte
/// strings from a tiny alphabet so key collisions and shared
/// prefixes both exercise the level/partition logic.
fn arb_map(tc: &TestCase) -> BTreeMap<Vec<u8>, Hash> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(40));
    let mut out = BTreeMap::new();
    for _ in 0..n {
        let key_len = tc.draw(gs::integers::<usize>().min_value(1).max_value(5));
        let mut k = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'f'));
            k.push(c);
        }
        let v = tc.draw(gs::integers::<u8>());
        out.insert(k, h(v));
    }
    out
}

#[hegel::test(test_cases = 256)]
fn root_depends_only_on_key_set_not_order(tc: TestCase) {
    let map = arb_map(&tc);
    let fwd: Vec<(Vec<u8>, Hash)> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
    let mut rev = fwd.clone();
    rev.reverse();

    let a = Mst::from_pairs(fwd);
    let b = Mst::from_pairs(rev);
    assert_eq!(a.root(), b.root());
    assert_eq!(a.len(), map.len());
}

#[hegel::test(test_cases = 256)]
fn self_diff_is_empty(tc: TestCase) {
    let map = arb_map(&tc);
    let pairs: Vec<(Vec<u8>, Hash)> = map.into_iter().collect();
    let t = Mst::from_pairs(pairs);
    let d = t.diff(&t);
    assert_eq!(d.diff_len(), 0);
}

#[hegel::test(test_cases = 256)]
fn reconcile_yields_union_on_both_sides(tc: TestCase) {
    // Two arbitrary maps. After exchanging the diff, both sides
    // must hold the same merged map. Ties: A's value wins for
    // keys present on both (deterministic merge rule).
    let mut a_map = arb_map(&tc);
    let b_map = arb_map(&tc);

    let a = Mst::from_pairs(
        a_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );
    let b = Mst::from_pairs(
        b_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );
    let d = a.diff(&b);

    // Expected converged map: union with A-wins on conflicts.
    let mut expected = b_map.clone();
    for (k, v) in &a_map {
        expected.insert(k.clone(), *v);
    }

    // Apply the diff. only_there keys flow B -> A; only_here keys
    // flow A -> B. On a value conflict both appear (present on
    // both sides at different values); A wins by construction of
    // the apply order below.
    let mut b_applied = b_map.clone();
    for k in d.only_here() {
        let v = *a_map.get(k).expect("only_here key exists on A");
        b_applied.insert(k.clone(), v);
    }
    for k in d.only_there() {
        // A adopts B's value only if A does not already hold the
        // key (A-wins on conflict).
        let v = *b_map.get(k).expect("only_there key exists on B");
        a_map.entry(k.clone()).or_insert(v);
    }

    // A now holds a_map union (b-only keys). B holds b_map union
    // (a-only + conflicts overwritten by A). Both must equal the
    // A-wins union.
    let a2 = Mst::from_pairs(
        a_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );
    let b2 = Mst::from_pairs(
        b_applied
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );

    assert_eq!(a_map, expected, "A side must converge to A-wins union");
    assert_eq!(b_applied, expected, "B side must converge to A-wins union");
    assert_eq!(a2.root(), b2.root(), "converged roots must match");
}

#[hegel::test(test_cases = 256)]
fn diff_len_equals_true_symmetric_difference(tc: TestCase) {
    let a_map = arb_map(&tc);
    let b_map = arb_map(&tc);
    let a = Mst::from_pairs(
        a_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );
    let b = Mst::from_pairs(
        b_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );
    let d = a.diff(&b);

    // The true symmetric difference: keys present on one side
    // only, or present on both at different values.
    let mut expected: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
    let all_keys: std::collections::BTreeSet<&Vec<u8>> = a_map.keys().chain(b_map.keys()).collect();
    for k in all_keys {
        match (a_map.get(k), b_map.get(k)) {
            (Some(va), Some(vb)) if va == vb => {}
            _ => {
                expected.insert(k.clone());
            }
        }
    }
    let got: std::collections::BTreeSet<Vec<u8>> = d.differing_keys().into_iter().collect();
    assert_eq!(got, expected);
}

#[hegel::test(test_cases = 256)]
fn diff_cost_never_exceeds_full_node_count(tc: TestCase) {
    // The comparison count is a bandwidth proxy; it must never
    // exceed what a naive full walk of both trees would cost.
    // (The interesting *win* -- comparisons << N -- is asserted
    // in the unit tests with large N; here we assert the safety
    // upper bound holds for arbitrary small inputs.)
    let a_map = arb_map(&tc);
    let b_map = arb_map(&tc);
    let a = Mst::from_pairs(
        a_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );
    let b = Mst::from_pairs(
        b_map
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>(),
    );
    let d = a.diff(&b);
    let bound = a_map.len() + b_map.len() + 2;
    assert!(
        d.comparisons() <= bound,
        "comparisons {} exceeded loose upper bound {bound}",
        d.comparisons()
    );
}
