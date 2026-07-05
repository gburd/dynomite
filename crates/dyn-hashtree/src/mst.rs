//! Merkle Search Tree (MST): a search tree whose structure is
//! deterministic in its key set, so two peers holding the same
//! keys build byte-identical trees and a top-down hash-pruned
//! diff transfers work proportional to the *difference* between
//! two key sets rather than to their total size.
//!
//! # Why this exists
//!
//! The [`crate::HashTree`] merkle tree buckets keys by a hash
//! into a fixed segment grid. A single differing key makes its
//! whole segment diverge, and comparing two peers still walks
//! every segment root; with membership churn the differing keys
//! scatter across many segments, so reconcile cost grows toward
//! the dataset size. The MST removes the fixed grid: a key's
//! depth in the tree is a deterministic function of a hash of
//! the key, identical subtrees share the same subtree hash, and
//! the diff walk skips any subtree pair whose hashes match. The
//! result is diff cost `O(d + h)` where `d` is the number of
//! differing keys and `h` is the tree height (`~log_B n`).
//!
//! # Structure
//!
//! Following Auvolat and Taiani, "Merkle Search Trees: Efficient
//! State-Based CRDTs in Open Networks" (SRDS 2019):
//!
//! * Each key is assigned a deterministic *level* equal to the
//!   number of leading base-`B` zero digits of `hash(key)`.
//!   With base `B`, a fraction `1/B` of keys land one level up,
//!   `1/B^2` two levels up, and so on -- a randomised but
//!   set-deterministic balanced layout.
//! * A node at level `L` stores the keys whose level is exactly
//!   `L`, in sorted order, interleaved with `k+1` child pointers
//!   to level-`(L-1)` subtrees covering the key ranges between
//!   (and around) its keys.
//! * A node's hash is a Merkle digest over its `(key,
//!   value_hash)` pairs and its child-subtree hashes. Because
//!   both the key placement and the child ordering are fixed by
//!   the key set, the root hash is a pure function of the
//!   `(key -> value_hash)` map.
//!
//! This module builds the tree bottom-up from a sorted key set
//! (the shape the AAE fold produces when it walks storage in
//! key order) and offers a [`Mst::diff`] that returns the exact
//! set of keys that differ between two trees, visiting only
//! divergent subtrees.
//!
//! # Example
//!
//! ```
//! use hashtree::mst::Mst;
//!
//! let a = Mst::from_pairs([
//!     (b"alice".to_vec(), *blake3::hash(b"v1").as_bytes()),
//!     (b"bob".to_vec(), *blake3::hash(b"v2").as_bytes()),
//! ]);
//! let b = Mst::from_pairs([
//!     (b"alice".to_vec(), *blake3::hash(b"v1").as_bytes()),
//!     (b"bob".to_vec(), *blake3::hash(b"v2").as_bytes()),
//!     (b"carol".to_vec(), *blake3::hash(b"v3").as_bytes()),
//! ]);
//! assert_ne!(a.root(), b.root());
//! let d = a.diff(&b);
//! // Only "carol" differs.
//! assert_eq!(d.differing_keys(), &[b"carol".to_vec()]);
//! ```

use std::collections::BTreeMap;

use crate::Hash;

/// Default branching base. A key rises one level for every
/// leading base-`B` zero digit of its hash, so a fraction
/// `1/B` of keys sit at each successive higher level. `B = 16`
/// (one hex digit per level) gives a shallow tree: ~`log_16 n`
/// levels, e.g. depth 5 for a billion keys.
pub const DEFAULT_BASE: u32 = 16;

/// Content digest of a byte string, used as the value side of an
/// MST entry. Two peers holding the same bytes for a key produce
/// the same digest and so agree on that key without transferring
/// the value; a byte difference flips the digest and surfaces the
/// key in the diff.
///
/// # Example
///
/// ```
/// use hashtree::mst::value_hash;
/// assert_eq!(value_hash(b"x"), value_hash(b"x"));
/// assert_ne!(value_hash(b"x"), value_hash(b"y"));
/// ```
#[must_use]
pub fn value_hash(value: &[u8]) -> Hash {
    *blake3::hash(value).as_bytes()
}

/// A single key/value pair carried at a tree node. The value is
/// a caller-supplied 32-byte digest (typically `blake3(value ||
/// vclock)`); the MST treats it as opaque identity bytes.
type Pair = (Vec<u8>, Hash);

/// One node of a [`Mst`].
///
/// A node owns the keys placed at its level (sorted) and,
/// between/around them, `keys.len() + 1` child subtrees at the
/// level below. A `None` child is an empty subtree (hashes to
/// [`crate::ZERO_HASH`]).
#[derive(Debug, Clone)]
struct Node {
    /// Sorted `(key, value_hash)` pairs living at this node's
    /// level.
    keys: Vec<Pair>,
    /// `keys.len() + 1` child subtrees. `children[i]` covers the
    /// key range strictly between `keys[i-1]` and `keys[i]`
    /// (with the ends open at the boundaries).
    children: Vec<Option<Box<Node>>>,
    /// Cached Merkle digest over `keys` and child hashes.
    hash: Hash,
}

/// A Merkle Search Tree over `(key, value_hash)` pairs.
///
/// Construct with [`Mst::from_pairs`]; compare with
/// [`Mst::diff`]. The tree is immutable once built (the AAE
/// path rebuilds it from a storage fold each reconcile), which
/// keeps the diff walk allocation-light and the structure
/// trivially thread-safe to share.
#[derive(Debug, Clone)]
pub struct Mst {
    /// Branching base used to assign key levels.
    base: u32,
    /// Root node, or `None` for an empty tree.
    root: Option<Box<Node>>,
}

/// Level of a key: the count of leading base-`B` zero digits of
/// `blake3(key)`, read most-significant digit first. A key whose
/// hash starts with a non-zero top digit is level 0.
fn key_level(key: &[u8], base: u32) -> u32 {
    let digest = blake3::hash(key);
    let bytes = digest.as_bytes();
    // Interpret the 32-byte digest as a big-endian number and
    // count leading base-`base` zero digits. We only need enough
    // digits to place a key; cap the walk so a degenerate base
    // cannot loop unboundedly.
    let base64 = u64::from(base);
    let mut acc = 0u128;
    // Fold the leading 16 bytes into a u128; that is far more
    // entropy than the level distribution needs (a level above
    // ~30 is astronomically unlikely for any real base).
    for &b in &bytes[0..16] {
        acc = (acc << 8) | u128::from(b);
    }
    let mut level = 0u32;
    // Peel base-`base` digits off the most-significant end.
    // Find the highest power of `base` that fits, then divide
    // down. Simpler: count how many top digits are zero by
    // repeatedly checking the leading digit.
    //
    // We compute the number of base-`base` digits in acc's
    // representation of the full 128-bit width, then count zeros
    // from the top.
    let total_digits = 128u32.div_ceil(base_bits(base));
    for i in (0..total_digits).rev() {
        let shift = i.checked_mul(base_bits(base));
        let Some(shift) = shift else { break };
        if shift >= 128 {
            // Digit position beyond our width is zero -> counts.
            level += 1;
            continue;
        }
        let digit = (acc >> shift) % u128::from(base64);
        if digit == 0 {
            level += 1;
        } else {
            break;
        }
    }
    level
}

/// Bits per base-`base` digit, rounded up. Used only to size the
/// digit walk in [`key_level`]; correctness does not depend on
/// tightness, only on being a positive, monotone function of the
/// base.
fn base_bits(base: u32) -> u32 {
    // ceil(log2(base)), min 1.
    let b = base.max(2);
    (32 - (b - 1).leading_zeros()).max(1)
}

impl Node {
    /// Compute the Merkle digest of a node from its keys and the
    /// hashes of its (already-built) children.
    fn compute_hash(keys: &[Pair], children: &[Option<Box<Node>>]) -> Hash {
        let mut h = blake3::Hasher::new();
        // Domain-separate node hashing from leaf/value hashing.
        h.update(b"mst-node\0");
        let n = u64::try_from(keys.len()).unwrap_or(u64::MAX);
        h.update(&n.to_be_bytes());
        for (k, v) in keys {
            let kl = u64::try_from(k.len()).unwrap_or(u64::MAX);
            h.update(&kl.to_be_bytes());
            h.update(k);
            h.update(v);
        }
        for child in children {
            let ch = child.as_ref().map_or(crate::ZERO_HASH, |c| c.hash);
            h.update(&ch);
        }
        *h.finalize().as_bytes()
    }

    /// Collect every `(key, value_hash)` pair in this subtree
    /// into `out`, in ascending key order.
    fn collect(&self, out: &mut Vec<Pair>) {
        for (i, pair) in self.keys.iter().enumerate() {
            if let Some(child) = &self.children[i] {
                child.collect(out);
            }
            out.push(pair.clone());
        }
        if let Some(last) = self.children.last().and_then(|c| c.as_ref()) {
            last.collect(out);
        }
    }
}

impl Mst {
    /// Build a tree from an iterator of `(key, value_hash)`
    /// pairs using the [`DEFAULT_BASE`] branching factor.
    ///
    /// Duplicate keys keep the last value seen. Input order does
    /// not matter: the tree is sorted and levelled internally,
    /// so two callers with the same key set produce the same
    /// root regardless of iteration order.
    #[must_use]
    pub fn from_pairs<I: IntoIterator<Item = Pair>>(pairs: I) -> Self {
        Self::with_base(pairs, DEFAULT_BASE)
    }

    /// Build a tree with an explicit branching base.
    ///
    /// # Panics
    /// Panics if `base < 2`.
    #[must_use]
    pub fn with_base<I: IntoIterator<Item = Pair>>(pairs: I, base: u32) -> Self {
        assert!(base >= 2, "MST base must be >= 2");
        // Deduplicate (last write wins) and sort by key.
        let map: BTreeMap<Vec<u8>, Hash> = pairs.into_iter().collect();
        let leveled: Vec<(Vec<u8>, Hash, u32)> = map
            .into_iter()
            .map(|(k, v)| {
                let lvl = key_level(&k, base);
                (k, v, lvl)
            })
            .collect();
        let root = build(&leveled, base);
        Self { base, root }
    }

    /// Branching base this tree was built with.
    #[must_use]
    pub fn base(&self) -> u32 {
        self.base
    }

    /// Root Merkle digest. Two trees with the same
    /// `(key -> value_hash)` map and the same base have equal
    /// roots; [`crate::ZERO_HASH`] for an empty tree.
    #[must_use]
    pub fn root(&self) -> Hash {
        self.root.as_ref().map_or(crate::ZERO_HASH, |n| n.hash)
    }

    /// Number of keys in the tree.
    #[must_use]
    pub fn len(&self) -> usize {
        self.root.as_ref().map_or(0, |n| count_node(n))
    }

    /// `true` if the tree holds no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Every `(key, value_hash)` pair, ascending by key.
    #[must_use]
    pub fn pairs(&self) -> Vec<Pair> {
        let mut out = Vec::new();
        if let Some(root) = &self.root {
            root.collect(&mut out);
        }
        out
    }

    /// Compute the set-difference against `other` by walking
    /// only the subtrees whose hashes disagree.
    ///
    /// The returned [`MstDiff`] carries the symmetric difference
    /// (keys present-or-differing on exactly one side) split
    /// into "only/differing here" and "only/differing there",
    /// plus the number of node-hash comparisons the walk made
    /// (the bandwidth proxy the AAE bench reports).
    ///
    /// Trees built with different bases are treated as fully
    /// divergent (every key on both sides is emitted) since
    /// their shapes are not comparable.
    #[must_use]
    pub fn diff(&self, other: &Self) -> MstDiff {
        let mut here: BTreeMap<Vec<u8>, Hash> = BTreeMap::new();
        let mut there: BTreeMap<Vec<u8>, Hash> = BTreeMap::new();
        let mut comparisons = 0usize;

        if self.base == other.base {
            diff_nodes(
                self.root.as_deref(),
                other.root.as_deref(),
                &mut here,
                &mut there,
                &mut comparisons,
            );
        } else {
            // Incomparable shapes: fall back to a full exchange.
            for (k, v) in self.pairs() {
                here.insert(k, v);
            }
            for (k, v) in other.pairs() {
                there.insert(k, v);
            }
        }

        // Reduce to the true symmetric difference: a key present
        // on both sides with the same value is not a difference,
        // even though a divergent sibling dragged it into the
        // walk.
        let mut left_only = Vec::new();
        let mut right_only = Vec::new();
        for (k, v) in &here {
            match there.get(k) {
                Some(remote) if remote == v => {}
                _ => left_only.push(k.clone()),
            }
        }
        for (k, v) in &there {
            match here.get(k) {
                Some(local) if local == v => {}
                _ => right_only.push(k.clone()),
            }
        }
        MstDiff {
            only_here: left_only,
            only_there: right_only,
            comparisons,
        }
    }
}

/// Result of an [`Mst::diff`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MstDiff {
    /// Keys present (or at a different value) on the left tree
    /// only.
    only_here: Vec<Vec<u8>>,
    /// Keys present (or at a different value) on the right tree
    /// only.
    only_there: Vec<Vec<u8>>,
    /// Number of node-hash comparisons made during the walk.
    /// A bandwidth proxy: each comparison is one node-hash pair
    /// exchanged in the wire protocol.
    comparisons: usize,
}

impl MstDiff {
    /// Keys the left tree must send to the right (present or
    /// newer here).
    #[must_use]
    pub fn only_here(&self) -> &[Vec<u8>] {
        &self.only_here
    }

    /// Keys the right tree must send to the left.
    #[must_use]
    pub fn only_there(&self) -> &[Vec<u8>] {
        &self.only_there
    }

    /// Union of both sides, sorted and deduplicated -- the full
    /// set of keys that must be exchanged to converge.
    #[must_use]
    pub fn differing_keys(&self) -> Vec<Vec<u8>> {
        let mut set: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        set.extend(self.only_here.iter().cloned());
        set.extend(self.only_there.iter().cloned());
        set.into_iter().collect()
    }

    /// Total count of differing keys (the symmetric-difference
    /// size).
    #[must_use]
    pub fn diff_len(&self) -> usize {
        self.differing_keys().len()
    }

    /// Number of node-hash comparisons the diff walk made. This
    /// is the divergence-proportional cost metric: it grows with
    /// the number of divergent subtrees, not with the dataset
    /// size.
    #[must_use]
    pub fn comparisons(&self) -> usize {
        self.comparisons
    }
}

/// Count keys in a subtree.
fn count_node(node: &Node) -> usize {
    let mut n = node.keys.len();
    for child in node.children.iter().flatten() {
        n += count_node(child);
    }
    n
}

/// Build a subtree from the `(key, value, level)` triples that
/// belong to it (a contiguous, sorted key range). `top` is the
/// level of the node currently being built; the recursion peels
/// one level per step.
fn build(items: &[(Vec<u8>, Hash, u32)], base: u32) -> Option<Box<Node>> {
    if items.is_empty() {
        return None;
    }
    // The node level is the maximum key level in this range.
    let node_level = items.iter().map(|(_, _, l)| *l).max().unwrap_or(0);
    build_at(items, node_level, base)
}

/// Build a node at exactly `level` over the sorted `items`. Keys
/// whose level equals `level` become this node's keys; the gaps
/// between them recurse to build level-`(level-1)` children.
fn build_at(items: &[(Vec<u8>, Hash, u32)], level: u32, base: u32) -> Option<Box<Node>> {
    if items.is_empty() {
        return None;
    }
    // Indices of the keys that sit at exactly this level.
    let pivots: Vec<usize> = items
        .iter()
        .enumerate()
        .filter(|(_, (_, _, l))| *l == level)
        .map(|(i, _)| i)
        .collect();

    if pivots.is_empty() {
        // No key at this level in this range: descend without
        // consuming a level of structure. This collapses empty
        // levels so the tree height tracks the actual max key
        // level, not the nominal one.
        return build_at(items, level - 1, base);
    }

    let mut keys: Vec<Pair> = Vec::with_capacity(pivots.len());
    let mut children: Vec<Option<Box<Node>>> = Vec::with_capacity(pivots.len() + 1);

    // Left-most child: everything before the first pivot.
    let first = pivots[0];
    children.push(child_below(&items[0..first], level, base));

    for (p_idx, &pivot) in pivots.iter().enumerate() {
        let (k, v, _) = &items[pivot];
        keys.push((k.clone(), *v));
        // Range strictly between this pivot and the next (or the
        // end).
        let start = pivot + 1;
        let end = pivots.get(p_idx + 1).copied().unwrap_or(items.len());
        children.push(child_below(&items[start..end], level, base));
    }

    let hash = Node::compute_hash(&keys, &children);
    Some(Box::new(Node {
        keys,
        children,
        hash,
    }))
}

/// Build the child subtree for a between-pivots range. Guards
/// `level == 0` so we never underflow: at level 0 there is no
/// lower level, so any leftover items must themselves be level-0
/// keys of this node's siblings -- which cannot happen because
/// every item in a level-0 gap has level 0 and would have been a
/// pivot. Thus an empty child.
fn child_below(range: &[(Vec<u8>, Hash, u32)], level: u32, base: u32) -> Option<Box<Node>> {
    if range.is_empty() || level == 0 {
        return None;
    }
    build_at(range, level - 1, base)
}

/// Recursively diff two subtrees, pruning matching subtree
/// hashes. Emits every `(key, value)` reachable under a
/// divergent node into the appropriate side map. The
/// post-filter in [`Mst::diff`] then reduces to the true
/// symmetric difference.
fn diff_nodes(
    a: Option<&Node>,
    b: Option<&Node>,
    here: &mut BTreeMap<Vec<u8>, Hash>,
    there: &mut BTreeMap<Vec<u8>, Hash>,
    comparisons: &mut usize,
) {
    *comparisons += 1;
    match (a, b) {
        (None, None) => {}
        (Some(node), None) => emit(node, here),
        (None, Some(node)) => emit(node, there),
        (Some(na), Some(nb)) => {
            if na.hash == nb.hash {
                // Identical subtree: prune. This is the whole
                // point -- an entire matching subtree costs one
                // comparison.
                return;
            }
            // Nodes differ. If both nodes sit at the same level
            // (same key count is a necessary-not-sufficient
            // proxy; we align by key), walk key-by-key and pair
            // the interleaved children. Because both trees are
            // built by the same deterministic procedure, two
            // nodes that are "the same node" in the abstract
            // tree share the same key partition when their key
            // sets agree; where they disagree we emit the
            // differing keys directly and recurse on the child
            // ranges by key alignment.
            diff_aligned(na, nb, here, there, comparisons);
        }
    }
}

/// Diff two non-identical nodes by merging their key lists and
/// recursing on the child subtrees that flank matching key
/// positions. Keys that appear on only one side (or with a
/// different value) are emitted immediately; the child pointers
/// on either side of a shared key are diffed pairwise.
fn diff_aligned(
    na: &Node,
    nb: &Node,
    here: &mut BTreeMap<Vec<u8>, Hash>,
    there: &mut BTreeMap<Vec<u8>, Hash>,
    comparisons: &mut usize,
) {
    // Merge-walk the two sorted key lists. Between shared keys,
    // recurse into the corresponding child subtrees. Where one
    // side has an extra key, that key (and its flanking child on
    // that side) is emitted wholesale.
    let a_keys = &na.keys;
    let b_keys = &nb.keys;
    let mut ia = 0usize;
    let mut ib = 0usize;

    // `child_a`/`child_b` track the child index to the left of
    // the current key cursor. children[i] is left of keys[i].
    // We diff children[ia] vs children[ib] whenever the cursors
    // are aligned at the same logical gap.
    loop {
        let ka = a_keys.get(ia);
        let kb = b_keys.get(ib);
        match (ka, kb) {
            (None, None) => {
                // Trailing children (right of the last key).
                diff_nodes(
                    na.children.get(ia).and_then(|c| c.as_deref()),
                    nb.children.get(ib).and_then(|c| c.as_deref()),
                    here,
                    there,
                    comparisons,
                );
                break;
            }
            (Some((kav, _)), Some((kbv, _))) if kav == kbv => {
                // Shared key position: diff the child to the
                // left of it, then advance both.
                diff_nodes(
                    na.children.get(ia).and_then(|c| c.as_deref()),
                    nb.children.get(ib).and_then(|c| c.as_deref()),
                    here,
                    there,
                    comparisons,
                );
                // The key itself may differ in value; record
                // both so the post-filter compares them.
                let (k, va) = &na.keys[ia];
                here.insert(k.clone(), *va);
                let (k, vb) = &nb.keys[ib];
                there.insert(k.clone(), *vb);
                ia += 1;
                ib += 1;
            }
            (Some((kav, _)), Some((kbv, _))) if kav < kbv => {
                // `a` has an extra key here: emit it and its
                // left child from the `a` side.
                if let Some(child) = na.children.get(ia).and_then(|c| c.as_deref()) {
                    emit(child, here);
                }
                let (k, va) = &na.keys[ia];
                here.insert(k.clone(), *va);
                ia += 1;
            }
            (Some(_), Some(_)) => {
                // `b` has an extra key here.
                if let Some(child) = nb.children.get(ib).and_then(|c| c.as_deref()) {
                    emit(child, there);
                }
                let (k, vb) = &nb.keys[ib];
                there.insert(k.clone(), *vb);
                ib += 1;
            }
            (Some(_), None) => {
                if let Some(child) = na.children.get(ia).and_then(|c| c.as_deref()) {
                    emit(child, here);
                }
                let (k, va) = &na.keys[ia];
                here.insert(k.clone(), *va);
                ia += 1;
            }
            (None, Some(_)) => {
                if let Some(child) = nb.children.get(ib).and_then(|c| c.as_deref()) {
                    emit(child, there);
                }
                let (k, vb) = &nb.keys[ib];
                there.insert(k.clone(), *vb);
                ib += 1;
            }
        }
    }
}

/// Emit every pair under `node` into `out`.
fn emit(node: &Node, out: &mut BTreeMap<Vec<u8>, Hash>) {
    let mut pairs = Vec::new();
    node.collect(&mut pairs);
    for (k, v) in pairs {
        out.insert(k, v);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    fn h(s: &[u8]) -> Hash {
        *blake3::hash(s).as_bytes()
    }

    fn pairs(n: u32) -> Vec<Pair> {
        (0..n)
            .map(|i| {
                (
                    format!("k{i:06}").into_bytes(),
                    h(format!("v{i}").as_bytes()),
                )
            })
            .collect()
    }

    #[test]
    fn empty_tree_root_is_zero() {
        let t = Mst::from_pairs([]);
        assert_eq!(t.root(), crate::ZERO_HASH);
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn root_is_order_independent() {
        let mut fwd = pairs(500);
        let mut rev = fwd.clone();
        rev.reverse();
        // Shuffle-ish: also rotate.
        fwd.rotate_left(137);
        let a = Mst::from_pairs(fwd);
        let b = Mst::from_pairs(rev);
        assert_eq!(a.root(), b.root());
        assert_eq!(a.len(), 500);
    }

    #[test]
    fn pairs_round_trip_sorted() {
        let t = Mst::from_pairs(pairs(200));
        let got = t.pairs();
        assert_eq!(got.len(), 200);
        for w in got.windows(2) {
            assert!(w[0].0 < w[1].0, "pairs must be ascending");
        }
    }

    #[test]
    fn identical_trees_diff_empty_cheaply() {
        let a = Mst::from_pairs(pairs(2000));
        let b = Mst::from_pairs(pairs(2000));
        assert_eq!(a.root(), b.root());
        let d = a.diff(&b);
        assert_eq!(d.diff_len(), 0);
        // Identical roots prune at the top: one comparison.
        assert_eq!(d.comparisons(), 1);
    }

    #[test]
    fn single_key_diff_is_localized() {
        let base = pairs(5000);
        let a = Mst::from_pairs(base.clone());
        let mut extra = base;
        extra.push((b"zzzznew".to_vec(), h(b"vnew")));
        let b = Mst::from_pairs(extra);
        let d = a.diff(&b);
        assert_eq!(d.differing_keys(), vec![b"zzzznew".to_vec()]);
        assert_eq!(d.only_there(), &[b"zzzznew".to_vec()]);
        assert!(d.only_here().is_empty());
        // The walk must be far cheaper than the 5000-key set.
        assert!(
            d.comparisons() < 200,
            "expected a cheap walk, got {} comparisons",
            d.comparisons()
        );
    }

    #[test]
    fn value_change_is_detected() {
        let base = pairs(1000);
        let a = Mst::from_pairs(base.clone());
        let mut changed = base;
        changed[42].1 = h(b"different");
        let b = Mst::from_pairs(changed);
        let d = a.diff(&b);
        assert_eq!(d.differing_keys(), vec![b"k000042".to_vec()]);
    }

    #[test]
    fn diff_cost_bounded_by_symmetric_difference() {
        // 10000 shared keys; 50 differ. Comparisons should be
        // closer to the diff size (times height) than to 10000.
        let base = pairs(10000);
        let a = Mst::from_pairs(base.clone());
        let mut b_pairs = base;
        for i in 0..50u32 {
            b_pairs.push((format!("new{i:04}").into_bytes(), h(b"x")));
        }
        let b = Mst::from_pairs(b_pairs);
        let d = a.diff(&b);
        assert_eq!(d.diff_len(), 50);
        // The key check: comparisons scale with diff, not with N.
        assert!(
            d.comparisons() < 2000,
            "diff walk not divergence-proportional: {} comparisons for 50 diffs over 10000 keys",
            d.comparisons()
        );
    }

    #[test]
    fn reconcile_converges_both_sides() {
        // Model the reconcile: A has keys 0..1000, B has 500..1500.
        let a_pairs = pairs(1000);
        let b_pairs: Vec<Pair> = (500..1500u32)
            .map(|i| {
                (
                    format!("k{i:06}").into_bytes(),
                    h(format!("v{i}").as_bytes()),
                )
            })
            .collect();
        let a = Mst::from_pairs(a_pairs.clone());
        let b = Mst::from_pairs(b_pairs.clone());
        let d = a.diff(&b);

        // Apply: A gains only_there, B gains only_here. Rebuild.
        let mut a_map: BTreeMap<Vec<u8>, Hash> = a_pairs.into_iter().collect();
        let mut b_map: BTreeMap<Vec<u8>, Hash> = b_pairs.into_iter().collect();
        for k in d.only_there() {
            let v = *b_map.get(k).expect("only_there key must exist on B");
            a_map.insert(k.clone(), v);
        }
        for k in d.only_here() {
            let v = *b_map_or_a(&a_map, k);
            b_map.insert(k.clone(), v);
        }
        let a2 = Mst::from_pairs(a_map.clone());
        let b2 = Mst::from_pairs(b_map.clone());
        assert_eq!(a2.root(), b2.root(), "reconcile must converge roots");
        assert_eq!(a_map, b_map, "merged key sets must be identical");
    }

    fn b_map_or_a<'a>(a_map: &'a BTreeMap<Vec<u8>, Hash>, k: &[u8]) -> &'a Hash {
        a_map.get(k).expect("only_here key must exist on A")
    }

    #[test]
    fn base_mismatch_falls_back_to_full() {
        let a = Mst::with_base(pairs(100), 16);
        let b = Mst::with_base(pairs(100), 4);
        let d = a.diff(&b);
        // Same key set -> no real diff even via the full path.
        assert_eq!(d.diff_len(), 0);
    }

    #[test]
    fn disjoint_sets_diff_everything() {
        let a = Mst::from_pairs(pairs(100));
        let b_pairs: Vec<Pair> = (1000..1100u32)
            .map(|i| (format!("k{i:06}").into_bytes(), h(b"v")))
            .collect();
        let b = Mst::from_pairs(b_pairs);
        let d = a.diff(&b);
        assert_eq!(d.diff_len(), 200);
    }
}
