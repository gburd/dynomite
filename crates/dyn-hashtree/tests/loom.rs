//! Loom model-check tests for the lazy-init path on
//! `hashtree::Segment::hash`.
//!
//! These tests are gated on `--cfg loom`; the default build
//! sees an empty file. They verify the contract documented in
//! `src/lazy_hash.rs`:
//!
//! * Concurrent observers of `segment_hash(idx)` agree on a
//!   single, stable digest.
//! * `root()` is consistent under concurrent segment walks.
//! * `diff` returns an empty list when two trees with the same
//!   multiset are compared from concurrent observers, even when
//!   none of the segment caches were populated up-front.
//!
//! Run via `bash scripts/loom.sh` (which sets
//! `RUSTFLAGS='--cfg loom'` and pins release mode for speed).

#![cfg(loom)]

use hashtree::HashTree;
use loom::sync::Arc;
use loom::thread;

/// Two threads call `segment_hash` on the same index. Loom
/// must verify that no interleaving causes them to observe
/// different digests.
#[test]
fn concurrent_segment_hash_consistent() {
    loom::model(|| {
        let mut tree = HashTree::new(4, 1);
        // Pin keys to specific segments so we know index 0 is
        // populated. With fanout=4, depth=1 there are 4
        // segments; inserting two keys gives loom a non-empty
        // segment to hash on at least one of them.
        tree.insert(b"alpha", [0u8; 32]);
        tree.insert(b"beta", [1u8; 32]);
        let tree = Arc::new(tree);

        // Probe the same index from both threads. Loom will
        // explore the interleavings of both `get_or_init`
        // entries.
        let h1 = {
            let t = tree.clone();
            thread::spawn(move || t.segment_hash(0))
        };
        let h2 = {
            let t = tree.clone();
            thread::spawn(move || t.segment_hash(0))
        };

        let r1 = h1.join().expect("thread 1 joined cleanly");
        let r2 = h2.join().expect("thread 2 joined cleanly");
        assert_eq!(
            r1, r2,
            "concurrent observers must see the same segment digest"
        );

        // The lead-thread post-check must also agree.
        let r3 = tree.segment_hash(0);
        assert_eq!(r1, r3, "post-join read must match the racing observers");
    });
}

/// Two threads walk every segment hash of the same tree
/// concurrently and then ask for the root. Each thread's local
/// reduction over its own segment view must equal the tree's
/// `root()` regardless of which thread populated the caches.
#[test]
fn root_under_concurrent_segment_walks() {
    loom::model(|| {
        // Keep the tree small: fanout=2, depth=1 gives 2
        // segments, which is enough to exercise the merkle
        // reduction over more than a single leaf without
        // exploding loom's interleaving budget.
        let mut tree = HashTree::new(2, 1);
        tree.insert(b"k0", [7u8; 32]);
        tree.insert(b"k1", [9u8; 32]);
        let tree = Arc::new(tree);

        let walker1 = {
            let t = tree.clone();
            thread::spawn(move || {
                let s0 = t.segment_hash(0);
                let s1 = t.segment_hash(1);
                (s0, s1, t.root())
            })
        };
        let walker2 = {
            let t = tree.clone();
            thread::spawn(move || {
                // Reverse order: hits the cache from the
                // opposite end so loom interleaves the two
                // get_or_init paths against each other.
                let s1 = t.segment_hash(1);
                let s0 = t.segment_hash(0);
                (s0, s1, t.root())
            })
        };

        let (a0, a1, ar) = walker1.join().expect("walker 1 joined cleanly");
        let (b0, b1, br) = walker2.join().expect("walker 2 joined cleanly");
        assert_eq!(a0, b0, "segment 0 digest must agree across walkers");
        assert_eq!(a1, b1, "segment 1 digest must agree across walkers");
        assert_eq!(ar, br, "root must agree across walkers");
    });
}

/// Two trees built from the same multiset, with all caches
/// cold, are compared via `diff` from two threads. The diff
/// must be empty under every interleaving.
#[test]
fn diff_under_concurrent_init() {
    loom::model(|| {
        let mut a = HashTree::new(2, 1);
        let mut b = HashTree::new(2, 1);
        a.insert(b"x", [3u8; 32]);
        a.insert(b"y", [5u8; 32]);
        b.insert(b"x", [3u8; 32]);
        b.insert(b"y", [5u8; 32]);
        let a = Arc::new(a);
        let b = Arc::new(b);

        // Both threads compare the same pair of trees but
        // start from opposite sides, so loom interleaves
        // `digest()` initialisation across the two trees and
        // both threads.
        let h1 = {
            let a = a.clone();
            let b = b.clone();
            thread::spawn(move || a.diff(&b))
        };
        let h2 = {
            let a = a.clone();
            let b = b.clone();
            thread::spawn(move || b.diff(&a))
        };

        let d1 = h1.join().expect("differ 1 joined cleanly");
        let d2 = h2.join().expect("differ 2 joined cleanly");
        assert!(
            d1.is_empty(),
            "equal multisets must diff to empty (got {d1:?})"
        );
        assert!(
            d2.is_empty(),
            "equal multisets must diff to empty (got {d2:?})"
        );
    });
}
