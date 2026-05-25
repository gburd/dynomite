//! Integration tests for the AAE persistence layer.
//!
//! Mirrors the "process restart" scenario: build a tree, write
//! a snapshot, simulate a fresh process by constructing a new
//! Tree from the snapshot, and assert the rebuilt tree's
//! observable surface matches the original.

use std::path::PathBuf;

use dyn_riak::aae::config::ConfAae;
use dyn_riak::aae::persist::PersistError;
use dyn_riak::aae::tictac::{Tree, TreeShape};

fn shape() -> TreeShape {
    TreeShape {
        n_time_buckets: 6,
        n_segments: 128,
        time_window_seconds: 60,
    }
}

fn populate(tree: &mut Tree, range: std::ops::Range<u32>) {
    for i in range {
        let key = format!("key-{i:06}");
        let vc = format!("vclock-{i}");
        // Spread across all time buckets.
        let ts = u64::from(i % (6 * 60));
        tree.insert(b"users", key.as_bytes(), vc.as_bytes(), ts);
    }
}

#[test]
fn snapshot_then_load_in_fresh_process_matches_original() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("tree.snapshot");

    // "Process A" builds and snapshots.
    let mut tree_a = Tree::new(shape());
    populate(&mut tree_a, 0..500);
    tree_a.save_snapshot(&path).expect("save snapshot");

    // "Process B" loads from cold start.
    let tree_b = Tree::load_snapshot(&path).expect("load snapshot");
    assert_eq!(tree_a.shape(), tree_b.shape());
    assert_eq!(tree_a.roots(), tree_b.roots());

    // The merkle tree's three exchange phases must agree
    // segment-for-segment.
    for tb in 0..shape().n_time_buckets {
        let segs_a = tree_a.segments(tb).expect("orig segments");
        let segs_b = tree_b.segments(tb).expect("loaded segments");
        assert_eq!(segs_a, segs_b, "segments differ for tb {tb}");
        for seg in 0..shape().n_segments {
            let keys_a = tree_a.keys_in_segment(tb, seg).expect("orig keys");
            let keys_b = tree_b.keys_in_segment(tb, seg).expect("loaded keys");
            assert_eq!(keys_a, keys_b, "keys differ at (tb={tb}, seg={seg})");
        }
    }
}

#[test]
fn driving_loaded_tree_matches_a_tree_built_in_one_process() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("tree.snapshot");

    // Process A: 500 inserts then snapshot.
    let mut tree_a1 = Tree::new(shape());
    populate(&mut tree_a1, 0..500);
    tree_a1.save_snapshot(&path).expect("save");

    // Process A continues with 500 more inserts.
    let mut tree_a2 = tree_a1.clone();
    populate(&mut tree_a2, 500..1000);

    // Process B: load snapshot, drive the same 500..1000 inserts.
    let mut tree_b = Tree::load_snapshot(&path).expect("load");
    populate(&mut tree_b, 500..1000);

    assert_eq!(
        tree_a2.roots(),
        tree_b.roots(),
        "post-restart driver should converge to in-process tree state"
    );
}

#[test]
fn corrupted_snapshot_does_not_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("tree.snapshot");

    let mut tree = Tree::new(shape());
    populate(&mut tree, 0..50);
    tree.save_snapshot(&path).expect("save");

    // Corrupt by truncating to first 32 bytes.
    let bytes = std::fs::read(&path).expect("read snapshot");
    std::fs::write(&path, &bytes[..32.min(bytes.len())]).expect("truncate");

    let err = Tree::load_snapshot(&path).unwrap_err();
    assert!(
        matches!(err, PersistError::Corrupted(_)),
        "expected Corrupted, got {err:?}"
    );
}

#[test]
fn confaae_snapshot_path_uses_configured_state_dir() {
    let cfg = ConfAae {
        aae_state_dir: PathBuf::from("/var/lib/dynomite/aae"),
        ..ConfAae::default()
    };
    let path = cfg.snapshot_path();
    assert_eq!(path, PathBuf::from("/var/lib/dynomite/aae/tree.snapshot"));
}
