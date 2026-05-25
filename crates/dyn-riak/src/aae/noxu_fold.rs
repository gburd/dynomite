//! Build the AAE tree directly from a [`NoxuDatastore`]'s
//! storage cursor.
//!
//! The default AAE rebuild path issues one logical `get` per
//! key (either by listing the keyspace and looping, or by
//! draining a snapshot stream). The Noxu path skips that:
//! it walks the underlying B-tree once, in storage order, so
//! every step is a cache-friendly cursor advance with no
//! per-key lock dance.
//!
//! The win is two-fold:
//!
//! * Lower memory peak. The tree is built incrementally; no
//!   intermediate per-key buffer.
//! * Faster than a public-API rebuild because storage-order
//!   reads are sequential and 2i records are filtered at the
//!   prefix level (see [`NoxuDatastore::fold_primary`]).
//!
//! Gated behind the `noxu` cargo feature so the default
//! build does not pull `noxu-db` into compilation.
//!
//! # Constraints / Noxu API gap
//!
//! The Riak AAE tree expects a per-record vector clock. The
//! `NoxuDatastore` value layer does not split `(value,
//! vclock)` -- callers store an opaque object body that may
//! or may not embed a vclock. The fold path therefore feeds
//! the stored value bytes directly into [`Tree::insert`]'s
//! `vclock` slot. The XOR-collision risk is identical to
//! any other content-hashed AAE design (two distinct values
//! must produce two distinct 64-bit FNV hashes, which is the
//! same collision floor [`crate::aae::tictac::KeyEntry::hash`]
//! already accepts).
//!
//! Storage records do not carry a per-key timestamp either.
//! The fold passes `0` for the timestamp argument, which
//! routes every key into time bucket 0. That is acceptable
//! for a cold rebuild: the next ambient `tree.insert` call
//! (driven by a live write or a sweep) re-anchors the key
//! against the wall-clock timestamp of that observation
//! through the XOR identity.
//!
//! # Noxu API gap
//!
//! Noxu does not expose a `fold(start_key, end_key, callback)`
//! primitive directly; the closest is the public `Cursor`
//! handle and the `Get::First` / `Get::Next` modes. The
//! crate-private `scan_prefix` helper in
//! `crate::datastore::noxu` already wraps that into a
//! prefix-bounded walk, and the new
//! [`NoxuDatastore::fold_primary`] surface delegates to it.
//! If a future Noxu release ships a typed fold, the helper
//! is the single switch point.

use crate::aae::tictac::Tree;
use crate::datastore::noxu::{NoxuDatastore, NoxuDatastoreError};

/// Errors raised by [`Tree::build_from_noxu_fold`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NoxuFoldError {
    /// An error from the underlying [`NoxuDatastore`] cursor
    /// or from the per-record callback.
    #[error("noxu fold: {0}")]
    Datastore(#[from] NoxuDatastoreError),
}

impl Tree {
    /// Walk the [`NoxuDatastore`]'s primary records in
    /// storage order and feed every `(bucket, key, value)`
    /// triple into [`Tree::insert`] as if it were a freshly-
    /// observed write.
    ///
    /// The tree is mutated in place; the caller is
    /// responsible for ensuring the tree was empty (or at
    /// least had no overlapping observations) before the
    /// rebuild. A typical caller pattern is:
    ///
    /// 1. `Tree::load_snapshot(...)` -- on success, skip the
    ///    fold rebuild.
    /// 2. On `PersistError::*` (or first start), `Tree::new(...)`
    ///    followed by `tree.build_from_noxu_fold(&ds)`.
    ///
    /// # Errors
    ///
    /// Surfaces [`NoxuFoldError::Datastore`] when the
    /// underlying cursor walk fails or when the callback
    /// returns an error.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "noxu")]
    /// # fn demo() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::path::Path;
    /// use dyn_riak::aae::tictac::{Tree, TreeShape};
    /// use dyn_riak::datastore::NoxuDatastore;
    ///
    /// let ds = NoxuDatastore::open(Path::new("/var/lib/dynomite/riak"))?;
    /// let mut tree = Tree::new(TreeShape {
    ///     n_time_buckets: 24,
    ///     n_segments: 1024,
    ///     time_window_seconds: 3600,
    /// });
    /// tree.build_from_noxu_fold(&ds)?;
    /// # Ok(()) }
    /// ```
    pub fn build_from_noxu_fold(&mut self, db: &NoxuDatastore) -> Result<(), NoxuFoldError> {
        db.fold_primary(|bucket, key, value| {
            // Storage records do not carry a vclock or a
            // timestamp; see the module-level docs for the
            // rationale. The value bytes are treated as the
            // identity blob the AAE merkle tree hashes into
            // its per-segment XOR aggregate.
            self.insert(bucket, key, value, 0);
            Ok(())
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aae::tictac::TreeShape;
    use tempfile::TempDir;

    fn shape() -> TreeShape {
        TreeShape {
            n_time_buckets: 4,
            n_segments: 64,
            time_window_seconds: 60,
        }
    }

    fn open_ds() -> (TempDir, NoxuDatastore) {
        let dir = TempDir::new().expect("tempdir");
        let ds = NoxuDatastore::open_in(dir.path()).expect("open");
        (dir, ds)
    }

    #[test]
    fn empty_datastore_yields_empty_tree() {
        let (_dir, ds) = open_ds();
        let mut tree = Tree::new(shape());
        tree.build_from_noxu_fold(&ds).expect("fold");
        for (_, root) in tree.roots() {
            assert_eq!(root, 0, "fold over empty datastore should leave roots zero");
        }
    }

    #[test]
    fn fold_inserts_every_primary_record() {
        let (_dir, ds) = open_ds();
        for i in 0..1000u32 {
            let key = format!("k{i:06}");
            let val = format!("v{i}");
            ds.put_object(b"users", key.as_bytes(), val.as_bytes(), &[])
                .expect("put");
        }
        let mut tree = Tree::new(shape());
        tree.build_from_noxu_fold(&ds).expect("fold");

        // Total leaf count: every key contributes exactly
        // one observation, so the per-segment directory
        // entries sum to 1000 across all (tb, seg) pairs.
        let mut count = 0usize;
        for tb in 0..shape().n_time_buckets {
            for seg in 0..shape().n_segments {
                count += tree
                    .keys_in_segment(tb, seg)
                    .expect("keys_in_segment")
                    .len();
            }
        }
        assert_eq!(count, 1000, "expected 1000 leaves, got {count}");
    }

    #[test]
    fn fold_skips_2i_records() {
        // Verify that putting an object with a forward-2i
        // entry only produces one tree leaf, not three
        // (one primary + one forward + one reverse).
        let (_dir, ds) = open_ds();
        ds.put_object(
            b"users",
            b"alice",
            b"v1",
            &[(b"age_int".to_vec(), b"42".to_vec())],
        )
        .expect("put");

        let mut tree = Tree::new(shape());
        tree.build_from_noxu_fold(&ds).expect("fold");

        let mut count = 0usize;
        for tb in 0..shape().n_time_buckets {
            for seg in 0..shape().n_segments {
                count += tree
                    .keys_in_segment(tb, seg)
                    .expect("keys_in_segment")
                    .len();
            }
        }
        assert_eq!(count, 1, "only the primary record should be folded");
    }

    #[test]
    fn fold_matches_explicit_inserts_bit_for_bit() {
        let (_dir, ds) = open_ds();
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..256u32)
            .map(|i| {
                (
                    format!("k{i:04}").into_bytes(),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();

        // Path A: drive the tree from the public Datastore API.
        let mut tree_api = Tree::new(shape());
        for (k, v) in &pairs {
            ds.put_object(b"users", k, v, &[]).expect("put");
            // Equivalent of the api-rebuild path: use value
            // bytes as vclock, timestamp 0 (matching the
            // Noxu fold's mapping).
            tree_api.insert(b"users", k, v, 0);
        }

        // Path B: drive the tree from the Noxu fold.
        let mut tree_fold = Tree::new(shape());
        tree_fold.build_from_noxu_fold(&ds).expect("fold");

        assert_eq!(
            tree_api.roots(),
            tree_fold.roots(),
            "api-rebuild and noxu-fold should produce identical roots"
        );
        for tb in 0..shape().n_time_buckets {
            assert_eq!(
                tree_api.segments(tb).expect("api segs"),
                tree_fold.segments(tb).expect("fold segs"),
                "segments differ for tb {tb}"
            );
        }
    }
}
