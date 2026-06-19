//! Tictac merkle tree: rolling per-vnode active-anti-entropy
//! summary.
//!
//! The tree is two-level. The top level partitions keys by a
//! "tic-tac" time bucket (each bucket covers a fixed window of
//! wall-clock seconds). The bottom level partitions a time
//! bucket's keys into `n_segments` segments using a stable hash
//! of the key. Each segment leaf is the XOR of the per-key hashes
//! that fall into it; XOR is its own inverse, which lets the
//! tree absorb in-place updates without a rebuild as long as the
//! caller can supply the prior key hash. A full rebuild is also
//! cheap because the per-segment XOR aggregation does not depend
//! on insertion order.
//!
//! # Example
//!
//! ```
//! use dyniak::aae::tictac::{Tree, TreeShape};
//!
//! let shape = TreeShape {
//!     n_time_buckets: 4,
//!     n_segments: 16,
//!     time_window_seconds: 60,
//! };
//! let mut tree = Tree::new(shape);
//! tree.insert(b"users", b"alice", b"vc1", 0);
//! tree.insert(b"users", b"bob", b"vc1", 0);
//! assert_eq!(tree.roots().len(), 4);
//! ```

use std::collections::{BTreeMap, BTreeSet};

/// Shape of a [`Tree`].
///
/// The shape is fixed for the life of the tree; an operator who
/// wants to rebuild with a different shape destroys the tree and
/// constructs a new one (this is what
/// `crate::aae::scheduler::Scheduler::rebuild` does at full-sweep
/// boundaries).
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct TreeShape {
    /// Top-level fan-out: number of time buckets.
    pub n_time_buckets: u32,
    /// Bottom-level fan-out: segments per time bucket.
    pub n_segments: u32,
    /// Wall-clock width of a single time bucket, in seconds.
    pub time_window_seconds: u64,
}

/// One row of the bottom level of the tree.
#[derive(Debug, Clone)]
struct Bucket {
    /// `n_segments` slots; each slot is the XOR of the key
    /// hashes whose [`segment_id`] equals the slot index.
    segments: Vec<u64>,
    /// Per-segment side directory: maps segment id to the set of
    /// `(bucket-of-keyspace, key, vclock_bytes)` tuples that
    /// contributed to the segment. The directory is what the
    /// `KEY-SYNC` exchange phase uses to enumerate diverging
    /// keys; without it, the merkle hashes alone tell us only
    /// "this segment differs", not "these are the keys to
    /// repair".
    directory: BTreeMap<u32, BTreeSet<KeyEntry>>,
}

/// One entry recorded in a bucket's per-segment directory. The entry
/// represents one observation of a (bucket, key) at a given
/// vclock.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct KeyEntry {
    /// Riak bucket name (the keyspace bucket, NOT the time
    /// bucket).
    pub bucket: Vec<u8>,
    /// The Riak object key.
    pub key: Vec<u8>,
    /// Opaque vector-clock bytes. The tree treats vclocks as
    /// opaque; the XOR-collision risk between two distinct
    /// vclocks for the same key reduces to "two distinct 64-bit
    /// hashes collide", which is the same collision floor every
    /// other AAE design accepts.
    pub vclock: Vec<u8>,
}

impl KeyEntry {
    /// 64-bit hash that contributes to the per-segment XOR.
    ///
    /// This is a stable FNV-1a 64 over `bucket || 0x00 || key ||
    /// 0x00 || vclock`, with a sentinel byte separating each
    /// field so that `("a", "bc")` and `("ab", "c")` do not
    /// collide.
    #[must_use]
    pub fn hash(&self) -> u64 {
        let mut h = FNV1A_OFFSET;
        for byte in &self.bucket {
            h ^= u64::from(*byte);
            h = h.wrapping_mul(FNV1A_PRIME);
        }
        h ^= 0;
        h = h.wrapping_mul(FNV1A_PRIME);
        for byte in &self.key {
            h ^= u64::from(*byte);
            h = h.wrapping_mul(FNV1A_PRIME);
        }
        h ^= 0;
        h = h.wrapping_mul(FNV1A_PRIME);
        for byte in &self.vclock {
            h ^= u64::from(*byte);
            h = h.wrapping_mul(FNV1A_PRIME);
        }
        h
    }

    /// Stable segment id given a `n_segments` fan-out.
    #[must_use]
    pub fn segment_id(&self, n_segments: u32) -> u32 {
        // The directory hash and the segment-id hash MUST use
        // independent reductions so that two distinct keys
        // landing on the same segment id are not also forced
        // onto colliding directory entries. Mixing the high and
        // low 32 bits of the FNV-1a hash gives us that for free.
        let h = self.hash();
        let mixed = (h >> 32) ^ (h & 0xffff_ffff);
        let modded = mixed % u64::from(n_segments);
        u32::try_from(modded).expect("invariant: modulo by u32 fits in u32")
    }
}

const FNV1A_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A Tictac AAE merkle tree.
///
/// `roots()`, `segments()`, and `keys_in_segment()` together
/// implement the three exchange phases (`ROOT-SYNC`, `TREE-SYNC`,
/// `KEY-SYNC`) used by [`crate::aae::exchange`].
#[derive(Debug, Clone)]
pub struct Tree {
    shape: TreeShape,
    buckets: Vec<Bucket>,
}

impl Tree {
    /// Build an empty tree with the given shape.
    ///
    /// # Panics
    /// Panics if `shape.n_time_buckets == 0` or
    /// `shape.n_segments == 0`. Operators should prefer
    /// `crate::aae::config::ConfAae::validate` to surface
    /// configuration errors before the tree is ever
    /// constructed.
    #[must_use]
    pub fn new(shape: TreeShape) -> Self {
        assert!(
            shape.n_time_buckets > 0,
            "TreeShape::n_time_buckets must be > 0"
        );
        assert!(shape.n_segments > 0, "TreeShape::n_segments must be > 0");
        let buckets = (0..shape.n_time_buckets)
            .map(|_| Bucket {
                segments: vec![0u64; shape.n_segments as usize],
                directory: BTreeMap::new(),
            })
            .collect();
        Self { shape, buckets }
    }

    /// The tree's shape.
    #[must_use]
    pub fn shape(&self) -> TreeShape {
        self.shape
    }

    /// Compute the time-bucket index for a wall-clock timestamp
    /// (seconds since the unix epoch). The mapping is
    /// `(timestamp / time_window_seconds) mod n_time_buckets`,
    /// which gives the rolling-window aging behaviour the
    /// "tic-tac" cadence demands: an old time-bucket's slot is
    /// reused once the cadence rolls over, which lets stale
    /// segments age out without an explicit purge.
    #[must_use]
    pub fn time_bucket_id(&self, timestamp_seconds: u64) -> u32 {
        let window = self.shape.time_window_seconds.max(1);
        let bucket = (timestamp_seconds / window) % u64::from(self.shape.n_time_buckets);
        u32::try_from(bucket).expect("invariant: modulo by u32 fits in u32")
    }

    /// Insert a key observation into the tree. Idempotent: if
    /// the same `(bucket, key, vclock)` is inserted twice the
    /// XOR cancels itself and the second call is a no-op for the
    /// merkle hashes (the directory set is also a set, so
    /// duplicates collapse there too). This is what makes the
    /// tree safe to drive from an at-least-once reconciliation
    /// stream.
    pub fn insert(&mut self, bucket: &[u8], key: &[u8], vclock: &[u8], timestamp_seconds: u64) {
        let entry = KeyEntry {
            bucket: bucket.to_vec(),
            key: key.to_vec(),
            vclock: vclock.to_vec(),
        };
        let tb = self.time_bucket_id(timestamp_seconds);
        let seg = entry.segment_id(self.shape.n_segments);
        let row = &mut self.buckets[tb as usize];
        let dir_set = row.directory.entry(seg).or_default();
        if dir_set.insert(entry.clone()) {
            row.segments[seg as usize] ^= entry.hash();
        }
    }

    /// Remove a previously-inserted observation. The caller MUST
    /// pass the same `(bucket, key, vclock, timestamp)` tuple
    /// that was inserted; XOR is its own inverse so the merkle
    /// hash returns to its prior state. If the tuple was never
    /// inserted, the call is a no-op.
    pub fn remove(&mut self, bucket: &[u8], key: &[u8], vclock: &[u8], timestamp_seconds: u64) {
        let entry = KeyEntry {
            bucket: bucket.to_vec(),
            key: key.to_vec(),
            vclock: vclock.to_vec(),
        };
        let tb = self.time_bucket_id(timestamp_seconds);
        let seg = entry.segment_id(self.shape.n_segments);
        let row = &mut self.buckets[tb as usize];
        if let Some(set) = row.directory.get_mut(&seg) {
            if set.remove(&entry) {
                row.segments[seg as usize] ^= entry.hash();
                if set.is_empty() {
                    row.directory.remove(&seg);
                }
            }
        }
    }

    /// Replace the prior observation of a key with a new one.
    /// Equivalent to `remove(old)` followed by `insert(new)`.
    pub fn update(
        &mut self,
        bucket: &[u8],
        key: &[u8],
        old_vclock: &[u8],
        new_vclock: &[u8],
        old_timestamp: u64,
        new_timestamp: u64,
    ) {
        self.remove(bucket, key, old_vclock, old_timestamp);
        self.insert(bucket, key, new_vclock, new_timestamp);
    }

    /// Top-level digest: one entry per time bucket.
    /// `(time_bucket_idx, root_hash)` pairs.
    ///
    /// The root of a time bucket is the XOR of every segment
    /// in that bucket, which is also equal to the XOR of every
    /// key hash in that bucket. The `ROOT-SYNC` exchange phase
    /// compares two peers' root vectors element-wise.
    #[must_use]
    pub fn roots(&self) -> Vec<(u32, u64)> {
        self.buckets
            .iter()
            .enumerate()
            .map(|(i, b)| {
                let root = b.segments.iter().copied().fold(0u64, |a, x| a ^ x);
                let i = u32::try_from(i)
                    .expect("invariant: time bucket index fits in u32 by construction");
                (i, root)
            })
            .collect()
    }

    /// Mid-level digest: every segment in a single time bucket.
    /// Returns `(segment_id, segment_hash)` pairs for the
    /// `n_segments` slots, including empty (zero) slots so the
    /// caller can compare element-wise without worrying about
    /// missing rows.
    ///
    /// # Errors
    /// Returns `Err` if `time_bucket` is out of range.
    pub fn segments(&self, time_bucket: u32) -> Result<Vec<(u32, u64)>, TreeError> {
        let row = self
            .buckets
            .get(time_bucket as usize)
            .ok_or(TreeError::TimeBucketOutOfRange(time_bucket))?;
        Ok(row
            .segments
            .iter()
            .copied()
            .enumerate()
            .map(|(i, h)| {
                let i =
                    u32::try_from(i).expect("invariant: segment index fits in u32 by construction");
                (i, h)
            })
            .collect())
    }

    /// Bottom-level enumeration: every key entry in a given
    /// `(time_bucket, segment)` pair. Used by `KEY-SYNC` to
    /// surface the candidate divergent keys.
    ///
    /// # Errors
    /// Returns `Err` if `time_bucket` is out of range.
    pub fn keys_in_segment(
        &self,
        time_bucket: u32,
        segment: u32,
    ) -> Result<Vec<KeyEntry>, TreeError> {
        let row = self
            .buckets
            .get(time_bucket as usize)
            .ok_or(TreeError::TimeBucketOutOfRange(time_bucket))?;
        Ok(row
            .directory
            .get(&segment)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default())
    }

    /// Compare two roots vectors and return the time-bucket ids
    /// that differ. Order-stable in input order.
    ///
    /// `local` is `self.roots()`; `remote` is the peer's
    /// `roots()` view. The function tolerates trees of different
    /// sizes (a peer that has been reshaped mid-exchange) by
    /// pairing only the indices both sides report.
    #[must_use]
    pub fn diverging_time_buckets(local: &[(u32, u64)], remote: &[(u32, u64)]) -> Vec<u32> {
        let remote_map: BTreeMap<u32, u64> = remote.iter().copied().collect();
        let mut out = Vec::new();
        for (idx, local_root) in local {
            if let Some(remote_root) = remote_map.get(idx) {
                if remote_root != local_root {
                    out.push(*idx);
                }
            } else {
                out.push(*idx);
            }
        }
        out
    }

    /// Compare two segment vectors (for one time bucket) and
    /// return the segment ids that differ.
    #[must_use]
    pub fn diverging_segments(local: &[(u32, u64)], remote: &[(u32, u64)]) -> Vec<u32> {
        let remote_map: BTreeMap<u32, u64> = remote.iter().copied().collect();
        let mut out = Vec::new();
        for (idx, local_hash) in local {
            if let Some(remote_hash) = remote_map.get(idx) {
                if remote_hash != local_hash {
                    out.push(*idx);
                }
            } else {
                out.push(*idx);
            }
        }
        out
    }
}

/// Errors raised by [`Tree`].
#[derive(Debug, thiserror::Error)]
pub enum TreeError {
    /// A method was called with a `time_bucket` index that does
    /// not exist on this tree.
    #[error("time bucket {0} out of range")]
    TimeBucketOutOfRange(u32),
    /// A method was called with a `segment` index that does
    /// not exist for the given time bucket.
    #[error("segment {0} out of range")]
    SegmentOutOfRange(u32),
}

impl Tree {
    /// Install a segment hash and key directory directly,
    /// bypassing the per-key XOR aggregation. Used by the
    /// snapshot loader so the reconstructed segment hash
    /// matches the persisted hash exactly even if the
    /// directory has been tampered with on disk.
    ///
    /// # Errors
    /// Returns [`TreeError::TimeBucketOutOfRange`] when
    /// `time_bucket >= shape().n_time_buckets` and
    /// [`TreeError::SegmentOutOfRange`] when
    /// `segment >= shape().n_segments`.
    pub(crate) fn install_segment(
        &mut self,
        time_bucket: u32,
        segment: u32,
        hash: u64,
        entries: Vec<KeyEntry>,
    ) -> Result<(), TreeError> {
        let row = self
            .buckets
            .get_mut(time_bucket as usize)
            .ok_or(TreeError::TimeBucketOutOfRange(time_bucket))?;
        if segment >= self.shape.n_segments {
            return Err(TreeError::SegmentOutOfRange(segment));
        }
        row.segments[segment as usize] = hash;
        let set: BTreeSet<KeyEntry> = entries.into_iter().collect();
        if set.is_empty() {
            row.directory.remove(&segment);
        } else {
            row.directory.insert(segment, set);
        }
        Ok(())
    }

    /// Walk every `(time_bucket, segment)` pair whose
    /// directory is non-empty and return the materialised
    /// `(time_bucket, segment, segment_hash, entries)`
    /// tuples. Used by the snapshot writer; segments with
    /// empty directories have segment hash 0 by
    /// construction so they need not be persisted.
    pub(crate) fn collect_nonempty_segments(&self) -> Vec<(u32, u32, u64, Vec<KeyEntry>)> {
        let mut out = Vec::new();
        for (tb_idx, row) in self.buckets.iter().enumerate() {
            let tb = u32::try_from(tb_idx)
                .expect("invariant: time bucket index fits in u32 by construction");
            for (seg, set) in &row.directory {
                if set.is_empty() {
                    continue;
                }
                let hash = row.segments[*seg as usize];
                let entries: Vec<KeyEntry> = set.iter().cloned().collect();
                out.push((tb, *seg, hash, entries));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape() -> TreeShape {
        TreeShape {
            n_time_buckets: 4,
            n_segments: 64,
            time_window_seconds: 60,
        }
    }

    #[test]
    fn empty_tree_roots_are_zero() {
        let t = Tree::new(shape());
        for (_, root) in t.roots() {
            assert_eq!(root, 0);
        }
    }

    #[test]
    fn merkle_round_trip_localizes_one_leaf() {
        let mut a = Tree::new(shape());
        let mut b = Tree::new(shape());
        for i in 0..1000u32 {
            let key = format!("k{i}");
            let vc = format!("vc{i}");
            a.insert(b"users", key.as_bytes(), vc.as_bytes(), 0);
            b.insert(b"users", key.as_bytes(), vc.as_bytes(), 0);
        }
        // Trees match before mutation.
        assert_eq!(a.roots(), b.roots());

        // Mutate one key on b. The XOR removes the old entry
        // and adds the new one; with a good hash the two
        // entries land in distinct segments most of the time,
        // so we expect 1 or 2 diverging leaves -- never more.
        b.update(b"users", b"k42", b"vc42", b"vc42-updated", 0, 0);

        let dr = Tree::diverging_time_buckets(&a.roots(), &b.roots());
        assert_eq!(dr.len(), 1, "only one time bucket should diverge");
        let tb = dr[0];

        let ds = Tree::diverging_segments(&a.segments(tb).unwrap(), &b.segments(tb).unwrap());
        assert!(
            (1..=2).contains(&ds.len()),
            "expected 1 or 2 diverging segments, got {ds:?}"
        );

        // Across the diverging segments, exactly one key
        // (k42) appears in either local-side or remote-side
        // entries.
        let mut found_local_old = false;
        let mut found_remote_new = false;
        for seg in &ds {
            for entry in a.keys_in_segment(tb, *seg).unwrap() {
                if entry.key == b"k42" && entry.vclock == b"vc42" {
                    found_local_old = true;
                }
            }
            for entry in b.keys_in_segment(tb, *seg).unwrap() {
                if entry.key == b"k42" && entry.vclock == b"vc42-updated" {
                    found_remote_new = true;
                }
            }
        }
        assert!(found_local_old);
        assert!(found_remote_new);
    }

    #[test]
    fn xor_is_its_own_inverse() {
        let mut t = Tree::new(shape());
        let baseline = t.roots();
        t.insert(b"b", b"k", b"vc", 0);
        assert_ne!(t.roots(), baseline);
        t.remove(b"b", b"k", b"vc", 0);
        assert_eq!(t.roots(), baseline);
    }

    #[test]
    fn duplicate_insert_is_idempotent() {
        let mut t = Tree::new(shape());
        t.insert(b"b", b"k", b"vc", 0);
        let after_one = t.roots();
        t.insert(b"b", b"k", b"vc", 0);
        assert_eq!(t.roots(), after_one);
    }

    #[test]
    fn time_bucket_id_rolls_over() {
        let t = Tree::new(shape());
        let n = t.shape.n_time_buckets;
        let w = t.shape.time_window_seconds;
        assert_eq!(t.time_bucket_id(0), 0);
        assert_eq!(t.time_bucket_id(w), 1);
        assert_eq!(t.time_bucket_id(u64::from(n) * w), 0);
    }

    #[test]
    fn segments_out_of_range_errors() {
        let t = Tree::new(shape());
        assert!(t.segments(999).is_err());
    }

    #[test]
    fn diverging_buckets_handles_size_mismatch() {
        let local = vec![(0u32, 1u64), (1, 2), (2, 3)];
        let remote = vec![(0u32, 1u64), (1, 99)];
        let d = Tree::diverging_time_buckets(&local, &remote);
        // Index 1 differs by hash; index 2 is missing on remote.
        assert_eq!(d, vec![1, 2]);
    }
}
