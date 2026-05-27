//! Integration tests for the public `hashtree::HashTree` API.

use hashtree::{Hash, HashTree};

fn h(s: &[u8]) -> Hash {
    *blake3::hash(s).as_bytes()
}

#[test]
fn insert_and_root_deterministic() {
    let mut a = HashTree::new(64, 1);
    let mut b = HashTree::new(64, 1);
    for i in 0..256u32 {
        let k = format!("k{i:04}");
        let v = format!("v{i}");
        a.insert(k.as_bytes(), h(v.as_bytes()));
        b.insert(k.as_bytes(), h(v.as_bytes()));
    }
    assert_eq!(a.root(), b.root());
}

#[test]
fn same_keys_same_root_independent_of_insertion_order() {
    let mut a = HashTree::new(64, 1);
    let mut b = HashTree::new(64, 1);
    let pairs: Vec<(Vec<u8>, Hash)> = (0..200u32)
        .map(|i| {
            (
                format!("k{i:04}").into_bytes(),
                h(format!("v{i}").as_bytes()),
            )
        })
        .collect();
    // a: forward order, b: reversed.
    for (k, v) in &pairs {
        a.insert(k, *v);
    }
    for (k, v) in pairs.iter().rev() {
        b.insert(k, *v);
    }
    assert_eq!(a.root(), b.root());
}

#[test]
fn single_diff_localized_to_one_segment() {
    let mut a = HashTree::new(64, 1);
    let mut b = HashTree::new(64, 1);
    for i in 0..1024u32 {
        let k = format!("k{i:04}");
        let v = format!("v{i}");
        a.insert(k.as_bytes(), h(v.as_bytes()));
        b.insert(k.as_bytes(), h(v.as_bytes()));
    }
    assert_eq!(a.root(), b.root());
    // Mutate one key on b.
    b.insert(b"k0042", h(b"updated"));
    let d = a.diff(&b);
    assert_eq!(d.len(), 1, "exactly one segment should diverge, got {d:?}");
    let seg = d[0];
    assert_eq!(seg, a.segment_for(b"k0042"));
}

#[test]
fn snapshot_round_trip_preserves_root() {
    let mut t = HashTree::new(64, 1);
    for i in 0..500u32 {
        let k = format!("k{i:04}");
        let v = format!("v{i}");
        t.insert(k.as_bytes(), h(v.as_bytes()));
    }
    let original_root = t.root();
    let mut buf = Vec::new();
    t.snapshot_to_writer(&mut buf).expect("write snapshot");
    let mut cur = std::io::Cursor::new(buf);
    let loaded = HashTree::snapshot_from_reader(&mut cur).expect("read snapshot");
    assert_eq!(loaded.fanout(), 64);
    assert_eq!(loaded.depth(), 1);
    assert_eq!(loaded.segment_count(), 64);
    assert_eq!(loaded.root(), original_root);
}

#[test]
fn fanout_64_depth_2_segments_4096_round_trips() {
    let mut t = HashTree::new(64, 2);
    assert_eq!(t.segment_count(), 64 * 64);
    // Insert enough keys to spread across segments.
    for i in 0..2048u32 {
        let k = format!("k{i:06}");
        let v = format!("v{i}");
        t.insert(k.as_bytes(), h(v.as_bytes()));
    }
    let root = t.root();
    let mut buf = Vec::new();
    t.snapshot_to_writer(&mut buf).unwrap();
    let mut cur = std::io::Cursor::new(buf);
    let loaded = HashTree::snapshot_from_reader(&mut cur).unwrap();
    assert_eq!(loaded.fanout(), 64);
    assert_eq!(loaded.depth(), 2);
    assert_eq!(loaded.segment_count(), 4096);
    assert_eq!(loaded.root(), root);
    // Spot-check fold on a known-occupied segment.
    let probe_seg = loaded.segment_for(b"k000042");
    let mut found = false;
    loaded.fold_segment(probe_seg, |k, _| {
        if k == b"k000042" {
            found = true;
        }
    });
    assert!(found, "expected k000042 to round-trip into its segment");
}

#[test]
fn empty_tree_round_trips() {
    let t = HashTree::new(64, 1);
    let mut buf = Vec::new();
    t.snapshot_to_writer(&mut buf).unwrap();
    let mut cur = std::io::Cursor::new(buf);
    let loaded = HashTree::snapshot_from_reader(&mut cur).unwrap();
    assert_eq!(loaded.root(), t.root());
}

#[test]
fn truncated_snapshot_returns_error() {
    let mut t = HashTree::new(64, 1);
    t.insert(b"k", h(b"v"));
    let mut buf = Vec::new();
    t.snapshot_to_writer(&mut buf).unwrap();
    buf.truncate(buf.len() / 2);
    let mut cur = std::io::Cursor::new(buf);
    let err = HashTree::snapshot_from_reader(&mut cur).unwrap_err();
    let msg = format!("{err}");
    assert!(!msg.is_empty());
}
