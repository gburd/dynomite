//! Property tests for `hashtree::HashTree`.
//!
//! Each `#[hegel::test]` runs at least 256 generated cases
//! under the default profile.

use std::collections::BTreeMap;

use hashtree::{Hash, HashTree};
use hegel::generators as gs;
use hegel::TestCase;

/// Draw a small `(fanout, depth)` shape. The fanout is always a
/// power of two; the depth is small enough that the segment
/// count does not overflow even on the deeper draw.
fn arb_shape(tc: &TestCase) -> (usize, usize) {
    let fanout_log2 = tc.draw(gs::integers::<u32>().min_value(0).max_value(4));
    let fanout = 1usize << fanout_log2;
    let depth_max: u32 = if fanout <= 1 { 4 } else { 3 };
    let depth = tc.draw(gs::integers::<u32>().min_value(0).max_value(depth_max));
    (fanout, depth as usize)
}

/// Draw a small multiset of `(key, value_hash)` pairs. Keys
/// are short ASCII strings drawn from a tiny alphabet so
/// duplicates and per-segment collisions both exercise the
/// data path.
fn arb_pairs(tc: &TestCase) -> Vec<(Vec<u8>, Hash)> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(32));
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let key_len = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
        let mut k = Vec::with_capacity(key_len);
        for _ in 0..key_len {
            let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'd'));
            k.push(c);
        }
        let v = tc.draw(gs::integers::<u8>());
        let value_hash = *blake3::hash(&[v]).as_bytes();
        out.push((k, value_hash));
    }
    out
}

#[hegel::test(test_cases = 256)]
fn root_depends_only_on_multiset_not_insertion_order(tc: TestCase) {
    let (fanout, depth) = arb_shape(&tc);
    let pairs = arb_pairs(&tc);
    // `insert` overwrites duplicates, so the final tree state
    // is determined by the deduplicated `(key -> hash)` map.
    // Drive that final state through two different insertion
    // orders and assert root equivalence.
    let unique: BTreeMap<Vec<u8>, Hash> = pairs.into_iter().collect();

    let mut a = HashTree::new(fanout, depth);
    for (k, v) in &unique {
        a.insert(k, *v);
    }
    let mut b = HashTree::new(fanout, depth);
    for (k, v) in unique.iter().rev() {
        b.insert(k, *v);
    }
    assert_eq!(a.root(), b.root());
}

#[hegel::test(test_cases = 256)]
fn snapshot_round_trip_preserves_root(tc: TestCase) {
    let (fanout, depth) = arb_shape(&tc);
    let pairs = arb_pairs(&tc);
    let mut t = HashTree::new(fanout, depth);
    for (k, v) in &pairs {
        t.insert(k, *v);
    }
    let original_root = t.root();
    let mut buf = Vec::new();
    t.snapshot_to_writer(&mut buf).expect("write");
    let mut cur = std::io::Cursor::new(buf);
    let loaded = HashTree::snapshot_from_reader(&mut cur).expect("read");
    assert_eq!(loaded.fanout(), fanout);
    assert_eq!(loaded.depth(), depth);
    assert_eq!(loaded.segment_count(), t.segment_count());
    assert_eq!(loaded.root(), original_root);
}

#[hegel::test(test_cases = 256)]
fn diff_self_is_empty(tc: TestCase) {
    let (fanout, depth) = arb_shape(&tc);
    let pairs = arb_pairs(&tc);
    let mut t = HashTree::new(fanout, depth);
    for (k, v) in &pairs {
        t.insert(k, *v);
    }
    assert!(t.diff(&t).is_empty());
}

#[hegel::test(test_cases = 256)]
fn segment_for_is_in_range(tc: TestCase) {
    let (fanout, depth) = arb_shape(&tc);
    let key_len = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    let mut k = Vec::with_capacity(key_len);
    for _ in 0..key_len {
        let c = tc.draw(gs::integers::<u8>());
        k.push(c);
    }
    let t = HashTree::new(fanout, depth);
    let idx = t.segment_for(&k);
    assert!(idx < t.segment_count() || t.segment_count() == 0);
}
