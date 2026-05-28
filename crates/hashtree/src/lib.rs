//! Generic merkle / hash-tree primitive.
//!
//! `hashtree` is a small, dependency-light merkle tree used as
//! the data-structure half of active anti-entropy (AAE)
//! reconciliation: two peers that hold roughly the same set of
//! `(key, value_hash)` pairs can build a [`HashTree`], exchange
//! its [`HashTree::root`] (and, on mismatch, its per-segment
//! hashes via [`HashTree::segment_hash`]), and converge on the
//! disagreeing keys without enumerating the whole keyspace.
//!
//! The tree is parameterised by a `fanout` (which must be a
//! power of two) and a `depth`. The leaf level holds
//! `fanout.pow(depth as u32)` segments; each segment is a
//! [`BTreeMap`] of `key -> value_hash` whose [BLAKE3] digest is
//! cached lazily via [`OnceLock`]. The interior levels are
//! reduced bottom-up by hashing chunks of `fanout` child
//! digests at a time, giving a true merkle reduction whose
//! intermediate nodes are not materialised but whose root is
//! determined entirely by the leaf segments.
//!
//! # Determinism
//!
//! The root depends only on the multiset of `(key, value_hash)`
//! pairs and on `(fanout, depth)`. Two trees built from the
//! same multiset with the same shape produce the same root
//! regardless of insertion order.
//!
//! # Snapshot format
//!
//! [`HashTree::snapshot_to_writer`] / [`HashTree::snapshot_from_reader`]
//! serialise the leaf segments via [`bincode`]. The format is
//! the crate's private wire shape; no stability guarantee is
//! offered across releases yet, so snapshots should be treated
//! as transient (within a single dynomited process generation
//! or compatible build).
//!
//! # Example
//!
//! ```
//! # #[cfg(loom)] fn main() {}
//! # #[cfg(not(loom))]
//! # fn main() {
//! use hashtree::HashTree;
//!
//! let mut a = HashTree::new(64, 1);
//! let mut b = HashTree::new(64, 1);
//! a.insert(b"alice", *blake3::hash(b"v1").as_bytes());
//! b.insert(b"alice", *blake3::hash(b"v1").as_bytes());
//! assert_eq!(a.root(), b.root());
//! b.insert(b"bob", *blake3::hash(b"v2").as_bytes());
//! assert_ne!(a.root(), b.root());
//! let diverging = a.diff(&b);
//! assert_eq!(diverging.len(), 1);
//! # }
//! ```
//!
//! [BLAKE3]: https://github.com/BLAKE3-team/BLAKE3
//! [`OnceLock`]: std::sync::OnceLock

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

mod lazy_hash;
use lazy_hash::LazyHash;

/// 32-byte BLAKE3 digest. Used both as the per-key value-side
/// hash supplied by the caller and as the internal segment /
/// root digest produced by the tree.
pub type Hash = [u8; 32];

/// All-zero digest, returned as the segment hash of an empty
/// segment.
pub const ZERO_HASH: Hash = [0u8; 32];

/// Errors raised by snapshot read/write.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HashTreeError {
    /// Underlying reader / writer error.
    #[error("hashtree io: {0}")]
    Io(#[from] std::io::Error),

    /// The snapshot's framing or payload could not be decoded.
    #[error("hashtree decode: {0}")]
    Decode(String),

    /// The snapshot's recorded shape did not validate (zero
    /// fan-out, non-power-of-two fan-out, segment count
    /// inconsistent with `fanout.pow(depth)`).
    #[error("hashtree bad shape: {0}")]
    BadShape(String),
}

/// One leaf cell of a [`HashTree`].
///
/// A `Segment` owns the keys whose [`HashTree::segment_for`]
/// index equals its position in [`HashTree::segments`]. The
/// `hash` field is the cached BLAKE3 digest of the segment's
/// contents and is invalidated by [`HashTree::insert`] /
/// [`HashTree::remove`].
#[derive(Debug, Default, Clone)]
pub struct Segment {
    /// Sorted `key -> value_hash` map. The BTreeMap's iteration
    /// order gives the segment hash a deterministic input
    /// regardless of insertion order.
    keys: BTreeMap<Vec<u8>, Hash>,
    /// Lazily-computed segment digest. Invalidated to `None`
    /// on any mutation. The cell is thread-safe so that
    /// concurrent readers calling [`HashTree::segment_hash`]
    /// or [`HashTree::root`] race only on the first observer
    /// to publish; subsequent observers reuse the cached
    /// digest. See [`lazy_hash`] for the model-checked
    /// implementation details.
    hash: LazyHash,
}

impl Segment {
    /// Number of `(key, value_hash)` pairs in this segment.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// `true` if the segment holds no pairs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Recompute (or reuse) and return the BLAKE3 digest over
    /// this segment's `(key, value_hash)` pairs in BTreeMap
    /// order. An empty segment hashes to [`ZERO_HASH`] so two
    /// peers agree on the digest of "this segment is empty"
    /// without serialising any bytes.
    fn digest(&self) -> Hash {
        self.hash.get_or_init(|| {
            if self.keys.is_empty() {
                return ZERO_HASH;
            }
            let mut h = blake3::Hasher::new();
            for (k, v) in &self.keys {
                let k_len = u64::try_from(k.len()).unwrap_or(u64::MAX);
                h.update(&k_len.to_be_bytes());
                h.update(k);
                h.update(v);
            }
            *h.finalize().as_bytes()
        })
    }

    /// Drop the cached digest. Called by every mutator.
    fn invalidate(&mut self) {
        self.hash = LazyHash::new();
    }
}

/// On-disk / on-wire shape of a [`Segment`]. Used by the
/// bincode codec only; the runtime representation keeps the
/// `LazyHash` cache.
#[derive(Serialize, Deserialize)]
struct SegmentDto {
    /// Owned `(key, value_hash)` pairs, sorted in the same
    /// order as the live BTreeMap.
    keys: Vec<(Vec<u8>, Hash)>,
}

impl From<&Segment> for SegmentDto {
    fn from(s: &Segment) -> Self {
        Self {
            keys: s.keys.iter().map(|(k, v)| (k.clone(), *v)).collect(),
        }
    }
}

impl From<SegmentDto> for Segment {
    fn from(d: SegmentDto) -> Self {
        let keys: BTreeMap<Vec<u8>, Hash> = d.keys.into_iter().collect();
        Self {
            keys,
            hash: LazyHash::new(),
        }
    }
}

/// Header + body of a snapshot. The wire format is
/// `bincode::serialize(&Snapshot)`; magic and version live as
/// fields so the decoder can reject mismatches without a custom
/// framing layer.
#[derive(Serialize, Deserialize)]
struct Snapshot {
    /// Constant ASCII tag `b"HTRE"` interpreted as a big-endian
    /// `u32`. Catches accidental loads of arbitrary byte
    /// streams.
    magic: u32,
    /// Snapshot format version. Bumped on any breaking change
    /// to the serialised representation.
    version: u32,
    /// Tree fan-out (number of children per interior node).
    fanout: u64,
    /// Tree depth.
    depth: u64,
    /// One [`SegmentDto`] per leaf segment, in segment-index
    /// order.
    segments: Vec<SegmentDto>,
}

/// Magic word that opens every snapshot. ASCII `"HTRE"` in
/// big-endian byte order.
const SNAPSHOT_MAGIC: u32 = 0x4854_5245;

/// Current snapshot format version.
const SNAPSHOT_VERSION: u32 = 1;

/// A merkle hash tree over `(key, value_hash)` pairs.
///
/// See the [crate-level documentation](crate) for the conceptual
/// overview and an end-to-end example.
#[derive(Debug, Clone)]
pub struct HashTree {
    /// Number of children per interior node. Must be a power
    /// of two.
    fanout: usize,
    /// Tree depth. The leaf level is at `depth`; the root is
    /// at level 0.
    depth: usize,
    /// Leaf segments, indexed `0 .. fanout.pow(depth)`.
    segments: Vec<Segment>,
}

impl HashTree {
    /// Build an empty tree.
    ///
    /// # Panics
    ///
    /// Panics if `fanout == 0`, if `fanout` is not a power of
    /// two, or if `fanout.pow(depth)` overflows `usize`.
    #[must_use]
    pub fn new(fanout: usize, depth: usize) -> Self {
        assert!(fanout > 0, "fanout must be > 0");
        assert!(fanout.is_power_of_two(), "fanout must be a power of two");
        let segment_count =
            checked_segment_count(fanout, depth).expect("fanout.pow(depth) must fit in usize");
        let segments = (0..segment_count).map(|_| Segment::default()).collect();
        Self {
            fanout,
            depth,
            segments,
        }
    }

    /// Tree fan-out.
    #[must_use]
    pub fn fanout(&self) -> usize {
        self.fanout
    }

    /// Tree depth.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Number of leaf segments. Equal to `fanout.pow(depth)`.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Insert (or replace) a `(key, value_hash)` pair. If the
    /// key already exists in its segment, the old value hash
    /// is overwritten; otherwise a new entry is added. Either
    /// way the segment's cached digest is invalidated.
    pub fn insert(&mut self, key: &[u8], value_hash: Hash) {
        let idx = self.segment_for(key);
        let seg = &mut self.segments[idx];
        // Skip the work when the same value hash is re-inserted
        // for the same key: the segment contents are unchanged
        // so the cached digest stays valid.
        if seg.keys.get(key) == Some(&value_hash) {
            return;
        }
        seg.keys.insert(key.to_vec(), value_hash);
        seg.invalidate();
    }

    /// Remove a key, if present. The cached segment digest is
    /// invalidated only on a real removal.
    pub fn remove(&mut self, key: &[u8]) {
        let idx = self.segment_for(key);
        let seg = &mut self.segments[idx];
        if seg.keys.remove(key).is_some() {
            seg.invalidate();
        }
    }

    /// Stable segment index for a key. The mapping is
    /// `BLAKE3(key)[0..8]` reduced mod `segment_count()`. The
    /// reduction is unbiased when `segment_count` is a power
    /// of two, which is the case whenever `fanout` is a power
    /// of two and `depth >= 0`.
    #[must_use]
    pub fn segment_for(&self, key: &[u8]) -> usize {
        let n = self.segments.len();
        if n <= 1 {
            return 0;
        }
        let digest = blake3::hash(key);
        let bytes = digest.as_bytes();
        let mut id = [0u8; 8];
        id.copy_from_slice(&bytes[0..8]);
        let v = u64::from_be_bytes(id);
        // `n` is a power of two -> `% n` is a single mask op.
        let modded =
            v % u64::try_from(n).expect("invariant: segment_count fits in u64 by construction");
        usize::try_from(modded).expect("invariant: modded < n which fits in usize")
    }

    /// Cached BLAKE3 digest of the segment at `segment_idx`.
    ///
    /// Returns [`ZERO_HASH`] when the segment is empty or when
    /// the index is out of range; the latter mirrors
    /// "comparing against a peer that does not have this
    /// segment yet" rather than panicking, which keeps `diff`
    /// total.
    #[must_use]
    pub fn segment_hash(&self, segment_idx: usize) -> Hash {
        self.segments
            .get(segment_idx)
            .map_or(ZERO_HASH, Segment::digest)
    }

    /// Tree root: the merkle reduction over the leaf segments.
    ///
    /// The root is computed bottom-up: at each interior level,
    /// `fanout` consecutive child digests are concatenated and
    /// hashed to form the parent digest. With `depth == 0` the
    /// root is the single leaf segment's digest. With one or
    /// more interior levels the reduction collapses level by
    /// level until a single digest remains.
    #[must_use]
    pub fn root(&self) -> Hash {
        if self.segments.is_empty() {
            return ZERO_HASH;
        }
        let mut level: Vec<Hash> = self.segments.iter().map(Segment::digest).collect();
        for _ in 0..self.depth {
            level = reduce_level(&level, self.fanout);
        }
        // After `depth` reductions the level holds exactly one
        // hash regardless of the leaf count, by construction
        // (`segment_count == fanout.pow(depth)`).
        level.first().copied().unwrap_or(ZERO_HASH)
    }

    /// Segment indices that disagree with `other`.
    ///
    /// Two trees with different `(fanout, depth)` are treated
    /// as fully divergent: the result lists every index up to
    /// `max(self.segment_count(), other.segment_count())`.
    /// Within a matching shape, the result lists exactly the
    /// indices `i` for which `self.segment_hash(i) !=
    /// other.segment_hash(i)`, in ascending order.
    #[must_use]
    pub fn diff(&self, other: &Self) -> Vec<usize> {
        let max = self.segment_count().max(other.segment_count());
        let same_shape = self.fanout == other.fanout && self.depth == other.depth;
        let mut out = Vec::new();
        for i in 0..max {
            if same_shape {
                if self.segment_hash(i) != other.segment_hash(i) {
                    out.push(i);
                }
            } else {
                out.push(i);
            }
        }
        out
    }

    /// Iterate every `(key, value_hash)` pair in
    /// `segment_idx`, calling `f` once per pair in BTreeMap
    /// order.
    ///
    /// Out-of-range indices are silently empty: the closure is
    /// not invoked.
    pub fn fold_segment<F: FnMut(&[u8], &Hash)>(&self, segment_idx: usize, mut f: F) {
        if let Some(seg) = self.segments.get(segment_idx) {
            for (k, v) in &seg.keys {
                f(k, v);
            }
        }
    }

    /// Serialise the tree's leaf state to `w` using the
    /// crate's bincode-based snapshot format.
    ///
    /// The cached segment digests are not written; on reload
    /// they are recomputed lazily from the materialised key
    /// pairs. This keeps the snapshot tamper-evident: a
    /// snapshot whose digests have been edited but whose key
    /// pairs have not will round-trip to a tree whose digests
    /// reflect the (unchanged) key pairs.
    pub fn snapshot_to_writer<W: Write>(&self, w: &mut W) -> Result<(), HashTreeError> {
        let dto = Snapshot {
            magic: SNAPSHOT_MAGIC,
            version: SNAPSHOT_VERSION,
            fanout: u64::try_from(self.fanout)
                .map_err(|e| HashTreeError::BadShape(format!("fanout: {e}")))?,
            depth: u64::try_from(self.depth)
                .map_err(|e| HashTreeError::BadShape(format!("depth: {e}")))?,
            segments: self.segments.iter().map(SegmentDto::from).collect(),
        };
        let bytes =
            bincode::serialize(&dto).map_err(|e| HashTreeError::Decode(format!("encode: {e}")))?;
        let len = u64::try_from(bytes.len())
            .map_err(|e| HashTreeError::BadShape(format!("payload length: {e}")))?;
        w.write_all(&len.to_be_bytes())?;
        w.write_all(&bytes)?;
        Ok(())
    }

    /// Inverse of [`HashTree::snapshot_to_writer`].
    pub fn snapshot_from_reader<R: Read>(r: &mut R) -> Result<Self, HashTreeError> {
        let mut len_buf = [0u8; 8];
        r.read_exact(&mut len_buf)?;
        let len = u64::from_be_bytes(len_buf);
        let len_us = usize::try_from(len)
            .map_err(|e| HashTreeError::BadShape(format!("payload length: {e}")))?;
        let mut payload = vec![0u8; len_us];
        r.read_exact(&mut payload)?;
        let dto: Snapshot = bincode::deserialize(&payload)
            .map_err(|e| HashTreeError::Decode(format!("decode: {e}")))?;
        if dto.magic != SNAPSHOT_MAGIC {
            return Err(HashTreeError::Decode(format!(
                "bad magic: 0x{:08x}",
                dto.magic
            )));
        }
        if dto.version != SNAPSHOT_VERSION {
            return Err(HashTreeError::Decode(format!(
                "version skew: file v{}, expected v{}",
                dto.version, SNAPSHOT_VERSION
            )));
        }
        let fanout = usize::try_from(dto.fanout)
            .map_err(|e| HashTreeError::BadShape(format!("fanout: {e}")))?;
        let depth = usize::try_from(dto.depth)
            .map_err(|e| HashTreeError::BadShape(format!("depth: {e}")))?;
        if fanout == 0 || !fanout.is_power_of_two() {
            return Err(HashTreeError::BadShape(format!(
                "fanout {fanout} must be a non-zero power of two"
            )));
        }
        let expected = checked_segment_count(fanout, depth).ok_or_else(|| {
            HashTreeError::BadShape(format!("fanout {fanout}^depth {depth} overflows"))
        })?;
        if dto.segments.len() != expected {
            return Err(HashTreeError::BadShape(format!(
                "segment count {} != fanout^depth {expected}",
                dto.segments.len()
            )));
        }
        let segments: Vec<Segment> = dto.segments.into_iter().map(Segment::from).collect();
        Ok(Self {
            fanout,
            depth,
            segments,
        })
    }
}

/// Compute one interior level by hashing every `fanout`
/// consecutive child digests into a single parent digest. The
/// caller has guaranteed that `level.len()` is an exact
/// multiple of `fanout` (true by construction when the leaf
/// count is `fanout.pow(depth)`); a partial trailing chunk is
/// tolerated by hashing whatever child digests are present in
/// it, which keeps the function total even for hand-rolled
/// callers that have not validated the shape.
fn reduce_level(level: &[Hash], fanout: usize) -> Vec<Hash> {
    if level.len() <= 1 {
        return level.to_vec();
    }
    let mut out = Vec::with_capacity(level.len().div_ceil(fanout));
    for chunk in level.chunks(fanout) {
        let mut h = blake3::Hasher::new();
        for child in chunk {
            h.update(child);
        }
        out.push(*h.finalize().as_bytes());
    }
    out
}

/// `fanout.pow(depth)` with overflow check. `depth == 0` gives
/// `1` for any positive fan-out.
fn checked_segment_count(fanout: usize, depth: usize) -> Option<usize> {
    let mut count: usize = 1;
    for _ in 0..depth {
        count = count.checked_mul(fanout)?;
    }
    Some(count)
}

// Gated against `loom`: the standard unit tests do not run
// inside a `loom::model` closure and would panic if they
// touched the loom-shadowed `Mutex` in `LazyHash`. Loom
// coverage for `Segment::hash` lives in `tests/loom.rs`.
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn h(s: &[u8]) -> Hash {
        *blake3::hash(s).as_bytes()
    }

    #[test]
    fn empty_tree_root_is_deterministic() {
        let a = HashTree::new(64, 1);
        let b = HashTree::new(64, 1);
        assert_eq!(a.root(), b.root());
    }

    #[test]
    fn single_segment_tree_depth_zero() {
        let mut a = HashTree::new(64, 0);
        a.insert(b"k", h(b"v"));
        assert_eq!(a.segment_count(), 1);
        // With depth 0, root == segment_hash(0).
        assert_eq!(a.root(), a.segment_hash(0));
    }

    #[test]
    fn segment_for_is_stable() {
        let t = HashTree::new(64, 1);
        let s1 = t.segment_for(b"users/alice");
        let s2 = t.segment_for(b"users/alice");
        assert_eq!(s1, s2);
        assert!(s1 < t.segment_count());
    }

    #[test]
    fn remove_undoes_insert_for_root() {
        let mut t = HashTree::new(64, 1);
        let baseline = t.root();
        t.insert(b"k", h(b"v"));
        assert_ne!(t.root(), baseline);
        t.remove(b"k");
        assert_eq!(t.root(), baseline);
    }

    #[test]
    fn idempotent_insert_does_not_invalidate_cache() {
        let mut t = HashTree::new(64, 1);
        t.insert(b"k", h(b"v"));
        let first = t.root();
        t.insert(b"k", h(b"v"));
        assert_eq!(t.root(), first);
    }

    #[test]
    fn diff_empty_when_trees_equal() {
        let mut a = HashTree::new(64, 1);
        let mut b = HashTree::new(64, 1);
        for i in 0..64u32 {
            let k = format!("k{i}");
            a.insert(k.as_bytes(), h(k.as_bytes()));
            b.insert(k.as_bytes(), h(k.as_bytes()));
        }
        assert!(a.diff(&b).is_empty());
    }

    #[test]
    fn diff_reports_shape_mismatch_as_total() {
        let a = HashTree::new(64, 1);
        let b = HashTree::new(64, 2);
        let d = a.diff(&b);
        assert_eq!(d.len(), b.segment_count());
    }

    #[test]
    fn fold_segment_iterates_in_btree_order() {
        let mut t = HashTree::new(2, 0);
        t.insert(b"c", h(b"3"));
        t.insert(b"a", h(b"1"));
        t.insert(b"b", h(b"2"));
        let mut keys = Vec::new();
        t.fold_segment(0, |k, _v| keys.push(k.to_vec()));
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn checked_segment_count_overflow_is_none() {
        // fanout = 2, depth = 64 -> 2^64 overflows usize.
        assert!(checked_segment_count(2, 100).is_none());
    }
}
