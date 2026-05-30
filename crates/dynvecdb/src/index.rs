//! Approximate nearest-neighbour index.
//!
//! Implements a minimal Hierarchical Navigable Small World
//! (HNSW) graph following the algorithm of Malkov & Yashunin,
//! "Efficient and robust approximate nearest neighbor search
//! using Hierarchical Navigable Small World graphs"
//! (TPAMI 2018, arXiv:1603.09320).
//!
//! Why hand-rolled rather than `instant-distance` or `hnsw_rs`:
//!
//! * The index needs to interleave with our codec layer so that
//!   the on-disk representation is a [`crate::encoding::EncodedVector`]
//!   not an `f32` slice. Hooking that into a third-party crate
//!   requires either keeping a parallel `Vec<f32>` cache (doubles
//!   memory) or wrapping its `Point` trait in adapters (locks us
//!   into that crate's API surface).
//! * We need explicit `delete` semantics. `instant-distance`
//!   does not expose deletion; we would have to maintain a
//!   tombstone set externally. Inverting that with a hand-rolled
//!   HNSW is a small amount of code and keeps the public API
//!   honest.
//! * No new third-party dependency, no review burden.
//!
//! Defaults:
//! * `M = 16` (max bidirectional connections per layer)
//! * `M0 = 32` (max connections at layer 0)
//! * `ef_construction = 200`
//! * `ef_search = 50`
//! * Layer assignment uses `floor(-ln(rand()) * mL)` with
//!   `mL = 1 / ln(M)` per the original paper.
//!
//! The index is single-threaded; coarser concurrency lives at
//! the [`crate::storage`] layer where a per-table `Mutex` is
//! held across an insert / search call.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::distance::Distance;

/// Stable identifier for the value an index node points back to.
///
/// The storage layer maps `NodeId` to a row key; the index is
/// agnostic to the row format.
pub type NodeId = u64;

/// Tuneable parameters.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct HnswParams {
    /// Max bidirectional links per node at every level above 0.
    pub m: usize,
    /// Max bidirectional links per node at level 0.
    pub m0: usize,
    /// Search beam width during insertion.
    pub ef_construction: usize,
    /// Default search beam width for queries. The query API can
    /// override this per call.
    pub ef_search: usize,
    /// Random seed for layer assignment. Stored for
    /// reproducibility; the `xorshift64` PRNG below is
    /// deterministic for a given seed.
    pub seed: u64,
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: 16,
            m0: 32,
            ef_construction: 200,
            ef_search: 50,
            seed: 0xDEAD_BEEF_CAFE_F00D,
        }
    }
}

/// One node in the graph.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct HnswNode {
    /// External identifier. Used by the storage layer to map back
    /// to the persisted row.
    id: NodeId,
    /// `f32` representation of the vector. The index keeps a
    /// decoded copy because the inner search loops touch every
    /// component on the hot path; re-decoding on every distance
    /// computation would dominate the runtime.
    vector: Vec<f32>,
    /// Adjacency lists, indexed by layer. `levels[0]` is the base
    /// layer; higher indices are the sparser upper layers.
    levels: Vec<Vec<usize>>,
    /// Soft-deleted node. Tombstoned nodes are skipped during
    /// search but their adjacency stays so the graph topology
    /// is preserved until a future compaction rebuilds.
    deleted: bool,
}

impl HnswNode {
    fn level(&self) -> usize {
        self.levels.len().saturating_sub(1)
    }
}

/// Hand-rolled HNSW index over `f32` vectors.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HnswIndex {
    params: HnswParams,
    distance: Distance,
    /// Storage of nodes by internal index. `NodeId -> internal idx`
    /// is in [`Self::id_to_idx`].
    nodes: Vec<HnswNode>,
    /// External-id lookup.
    id_to_idx: HashMap<NodeId, usize>,
    /// Index of the entry-point node, or `None` for an empty
    /// index.
    entry: Option<usize>,
    /// `mL` factor for level assignment, cached because every
    /// insert calls it.
    ml: f64,
    /// PRNG state for layer assignment.
    rng_state: u64,
    /// Vector dimension. Frozen on first insert and enforced on
    /// every subsequent insert; an attempt to insert a different
    /// dimension is rejected by the storage layer before reaching
    /// the index.
    dim: u16,
}

/// Errors returned by the index.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IndexError {
    /// Vector dimension does not match the index dimension.
    #[error("dimension mismatch: index has {expected}, got {got}")]
    DimensionMismatch {
        /// Index's frozen dimension.
        expected: u16,
        /// Caller's vector dimension.
        got: u16,
    },
    /// Tried to insert a [`NodeId`] that already exists.
    #[error("id {0} already present in the index")]
    Duplicate(NodeId),
    /// Empty input vector.
    #[error("empty vector")]
    Empty,
}

/// Result entry from a search query.
#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
    /// External identifier of the matched node.
    pub id: NodeId,
    /// Distance score; smaller is closer.
    pub score: f32,
}

/// Min-heap entry: `Reverse`-style ordering so a [`BinaryHeap`]
/// behaves as a min-heap on the score.
#[derive(Clone, Copy, Debug)]
struct Candidate {
    idx: usize,
    score: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap on score: invert.
        other
            .score
            .partial_cmp(&self.score)
            .unwrap_or(Ordering::Equal)
    }
}

/// Max-heap entry on score; used to keep the top-K furthest in
/// the dynamic candidate set.
#[derive(Clone, Copy, Debug)]
struct MaxCandidate {
    idx: usize,
    score: f32,
}

impl PartialEq for MaxCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}
impl Eq for MaxCandidate {}
impl PartialOrd for MaxCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MaxCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
    }
}

impl HnswIndex {
    /// Build an empty index.
    #[must_use]
    pub fn new(distance: Distance, params: HnswParams) -> Self {
        let ml = if params.m > 1 {
            1.0 / f64::from(u32::try_from(params.m).unwrap_or(u32::MAX)).ln()
        } else {
            1.0
        };
        Self {
            params,
            distance,
            nodes: Vec::new(),
            id_to_idx: HashMap::new(),
            entry: None,
            ml,
            rng_state: params.seed,
            dim: 0,
        }
    }

    /// Number of live (non-deleted) nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.iter().filter(|n| !n.deleted).count()
    }

    /// `true` when the index has no live nodes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Vector dimension, or 0 if the index is empty.
    #[must_use]
    pub fn dim(&self) -> u16 {
        self.dim
    }

    /// Distance metric this index was built with.
    #[must_use]
    pub fn distance(&self) -> Distance {
        self.distance
    }

    /// Insert a new vector under `id`.
    ///
    /// # Errors
    ///
    /// [`IndexError::Empty`] for a zero-dim vector,
    /// [`IndexError::DimensionMismatch`] when the vector's
    /// dimension differs from the index's frozen dimension,
    /// and [`IndexError::Duplicate`] when `id` is already in
    /// the index.
    pub fn insert(&mut self, id: NodeId, vector: Vec<f32>) -> Result<(), IndexError> {
        if vector.is_empty() {
            return Err(IndexError::Empty);
        }
        let got = u16::try_from(vector.len()).unwrap_or(u16::MAX);
        if self.nodes.is_empty() {
            self.dim = got;
        } else if self.dim != got {
            return Err(IndexError::DimensionMismatch {
                expected: self.dim,
                got,
            });
        }
        if self.id_to_idx.contains_key(&id) {
            return Err(IndexError::Duplicate(id));
        }

        let level = self.random_level();
        let mut levels: Vec<Vec<usize>> = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            levels.push(Vec::new());
        }

        let new_idx = self.nodes.len();
        self.nodes.push(HnswNode {
            id,
            vector,
            levels,
            deleted: false,
        });
        self.id_to_idx.insert(id, new_idx);

        let Some(entry) = self.entry else {
            self.entry = Some(new_idx);
            return Ok(());
        };
        let entry_level = self.nodes[entry].level();

        // Phase 1: descend through layers above `level` finding the
        // best entry point for `level`.
        let mut current = entry;
        if entry_level > level {
            for lc in (level + 1..=entry_level).rev() {
                current = self.greedy_search_layer(current, new_idx, lc);
            }
        }

        // Phase 2: at each layer from min(level, entry_level) down to
        // 0, search for ef_construction candidates and connect.
        let start_layer = level.min(entry_level);
        let mut entry_points = vec![current];
        for lc in (0..=start_layer).rev() {
            let neighbours = self.search_layer(
                new_idx,
                &entry_points,
                lc,
                self.params.ef_construction,
                /*include_deleted=*/ true,
            );
            let m = if lc == 0 {
                self.params.m0
            } else {
                self.params.m
            };
            let selected = Self::select_neighbours(&neighbours, m);
            // Bidirectional links.
            for &nb in &selected {
                self.nodes[new_idx].levels[lc].push(nb);
                self.nodes[nb].levels[lc].push(new_idx);
                // Shrink the neighbour's adjacency if it now exceeds
                // the cap.
                let cap = if lc == 0 {
                    self.params.m0
                } else {
                    self.params.m
                };
                if self.nodes[nb].levels[lc].len() > cap {
                    self.shrink_connections(nb, lc, cap);
                }
            }
            entry_points = selected;
            if entry_points.is_empty() {
                entry_points = vec![current];
            }
        }

        // If the new node sits above the previous entry point, it
        // becomes the new entry point.
        if level > entry_level {
            self.entry = Some(new_idx);
        }
        Ok(())
    }

    /// Soft-delete `id`. The node remains in the graph for
    /// connectivity but is filtered out of search results.
    ///
    /// Returns `true` when the id was present, `false` otherwise.
    pub fn delete(&mut self, id: NodeId) -> bool {
        let Some(&idx) = self.id_to_idx.get(&id) else {
            return false;
        };
        if self.nodes[idx].deleted {
            return false;
        }
        self.nodes[idx].deleted = true;
        true
    }

    /// Search for the `k` nearest neighbours of `query`.
    ///
    /// `ef` controls the search beam width. Pass `None` to use the
    /// index's default `ef_search`. A larger `ef` trades CPU for
    /// recall.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::DimensionMismatch`] when the query
    /// vector's dimension does not match the index's frozen
    /// dimension.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<SearchResult>, IndexError> {
        if query.is_empty() {
            return Ok(Vec::new());
        }
        if self.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let got = u16::try_from(query.len()).unwrap_or(u16::MAX);
        if self.dim != got {
            return Err(IndexError::DimensionMismatch {
                expected: self.dim,
                got,
            });
        }

        let mut entry = self.entry.unwrap_or(0);
        let entry_level = self.nodes[entry].level();
        let ef = ef.unwrap_or(self.params.ef_search).max(k);

        // Greedy-descend through upper layers.
        let query_owned = query.to_vec();
        for lc in (1..=entry_level).rev() {
            entry = self.greedy_search_layer_against(&query_owned, entry, lc);
        }

        let candidates = self.search_layer_against(&query_owned, &[entry], 0, ef, true);

        let mut sorted: Vec<MaxCandidate> = candidates;
        sorted.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(sorted
            .into_iter()
            .filter(|c| !self.nodes[c.idx].deleted)
            .take(k)
            .map(|c| SearchResult {
                id: self.nodes[c.idx].id,
                score: c.score,
            })
            .collect())
    }

    /// `true` when `id` is currently a live node in the index.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.id_to_idx
            .get(&id)
            .is_some_and(|&idx| !self.nodes[idx].deleted)
    }

    /// Random level assignment via the original paper's formula:
    /// `level = floor(-ln(uniform(0, 1)) * mL)`.
    fn random_level(&mut self) -> usize {
        let r = self.rand_unit();
        // Guard r > 0 so `ln(0)` is impossible.
        let r = r.max(f64::MIN_POSITIVE);
        let level = (-r.ln() * self.ml).floor();
        // Cap at a sane ceiling so a freak uniform sample does not
        // allocate thousands of empty layers.
        let max_level = 16_f64;
        let clamped = level.clamp(0.0, max_level);
        // The clamp guarantees `clamped` is in [0, 16]; the
        // cast cannot truncate or sign-flip.
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "clamped to [0, 16]"
        )]
        let lvl = clamped as usize;
        lvl
    }

    /// xorshift64* PRNG, deterministic given the seed.
    fn rand_unit(&mut self) -> f64 {
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        let r = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        // Take the top 53 bits and divide by 2^53 to get a
        // uniform [0, 1) double.
        let bits = (r >> 11) & ((1u64 << 53) - 1);
        // bits is < 2^53, fits in f64 exactly.
        #[allow(
            clippy::cast_precision_loss,
            reason = "bits is in [0, 2^53), exactly representable as f64"
        )]
        let f = (bits as f64) / ((1_u64 << 53) as f64);
        f
    }

    /// Greedy search using the freshly-inserted node as the query.
    fn greedy_search_layer(&self, entry: usize, query_idx: usize, lc: usize) -> usize {
        let q = self.nodes[query_idx].vector.clone();
        self.greedy_search_layer_against(&q, entry, lc)
    }

    /// Greedy single-best descent at layer `lc`.
    fn greedy_search_layer_against(&self, query: &[f32], entry: usize, lc: usize) -> usize {
        let mut current = entry;
        let mut current_score = self.distance.score(query, &self.nodes[current].vector);
        loop {
            let mut improved = false;
            if lc < self.nodes[current].levels.len() {
                let neighbours: Vec<usize> = self.nodes[current].levels[lc].clone();
                for nb in neighbours {
                    let s = self.distance.score(query, &self.nodes[nb].vector);
                    if s < current_score {
                        current_score = s;
                        current = nb;
                        improved = true;
                    }
                }
            }
            if !improved {
                break;
            }
        }
        current
    }

    /// Beam search at layer `lc` with the freshly-inserted node as
    /// the query.
    fn search_layer(
        &self,
        query_idx: usize,
        entry_points: &[usize],
        lc: usize,
        ef: usize,
        include_deleted: bool,
    ) -> Vec<MaxCandidate> {
        let q = self.nodes[query_idx].vector.clone();
        self.search_layer_against(&q, entry_points, lc, ef, include_deleted)
    }

    /// Beam search at layer `lc`. Returns up to `ef` candidates.
    fn search_layer_against(
        &self,
        query: &[f32],
        entry_points: &[usize],
        lc: usize,
        ef: usize,
        include_deleted: bool,
    ) -> Vec<MaxCandidate> {
        let mut visited: HashSet<usize> = HashSet::new();
        let mut frontier: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut top: BinaryHeap<MaxCandidate> = BinaryHeap::new();
        for &ep in entry_points {
            if visited.insert(ep) {
                let s = self.distance.score(query, &self.nodes[ep].vector);
                frontier.push(Candidate { idx: ep, score: s });
                if include_deleted || !self.nodes[ep].deleted {
                    top.push(MaxCandidate { idx: ep, score: s });
                }
            }
        }
        while let Some(c) = frontier.pop() {
            // Stop when the closest unprocessed candidate is
            // already worse than the current top.
            if top.len() >= ef {
                if let Some(worst) = top.peek() {
                    if c.score > worst.score {
                        break;
                    }
                }
            }
            if lc < self.nodes[c.idx].levels.len() {
                let neighbours: Vec<usize> = self.nodes[c.idx].levels[lc].clone();
                for nb in neighbours {
                    if !visited.insert(nb) {
                        continue;
                    }
                    let s = self.distance.score(query, &self.nodes[nb].vector);
                    let admit = match top.peek() {
                        Some(worst) => s < worst.score || top.len() < ef,
                        None => true,
                    };
                    if admit {
                        frontier.push(Candidate { idx: nb, score: s });
                        if include_deleted || !self.nodes[nb].deleted {
                            top.push(MaxCandidate { idx: nb, score: s });
                            if top.len() > ef {
                                top.pop();
                            }
                        }
                    }
                }
            }
        }
        top.into_vec()
    }

    /// Pick the top-`m` neighbours from the candidate set using
    /// the simple closest-first heuristic. Sufficient for the MVP;
    /// the original paper offers a more sophisticated
    /// "extend-by-heuristic" rule that we leave for a future tune.
    fn select_neighbours(candidates: &[MaxCandidate], m: usize) -> Vec<usize> {
        let mut sorted: Vec<MaxCandidate> = candidates.to_vec();
        sorted.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted.into_iter().take(m).map(|c| c.idx).collect()
    }

    /// Drop the longest edges from a node's adjacency list at `lc`
    /// until it fits in `cap`.
    fn shrink_connections(&mut self, idx: usize, lc: usize, cap: usize) {
        let q = self.nodes[idx].vector.clone();
        let neighbours = std::mem::take(&mut self.nodes[idx].levels[lc]);
        let mut scored: Vec<(usize, f32)> = neighbours
            .into_iter()
            .map(|nb| {
                let s = self.distance.score(&q, &self.nodes[nb].vector);
                (nb, s)
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(cap);
        self.nodes[idx].levels[lc] = scored.into_iter().map(|(nb, _)| nb).collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distance::Distance;

    fn unit(seed: u64, dim: usize) -> Vec<f32> {
        let mut x = seed;
        let mut v: Vec<f32> = Vec::with_capacity(dim);
        for _ in 0..dim {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Map to [-1, 1). bits is in [0, 2^53), exactly
            // representable in f64; the f64->f32 narrowing is
            // intentional (test data does not need full f64
            // precision).
            let bits = (x >> 11) & ((1_u64 << 53) - 1);
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                reason = "test fixture; PRNG output narrowed to f32"
            )]
            let r = ((bits as f64) / ((1_u64 << 53) as f64)) * 2.0 - 1.0;
            #[allow(
                clippy::cast_possible_truncation,
                reason = "test fixture; f64 -> f32 narrowing is intentional"
            )]
            let rf = r as f32;
            v.push(rf);
        }
        v
    }

    #[test]
    fn insert_and_search_small() {
        let mut idx = HnswIndex::new(Distance::Euclidean, HnswParams::default());
        let target = unit(42, 8);
        idx.insert(0, target.clone()).unwrap();
        for i in 1..50_u64 {
            idx.insert(i, unit(i.wrapping_mul(1_000_003) + 1, 8))
                .unwrap();
        }
        let res = idx.search(&target, 3, None).unwrap();
        assert!(!res.is_empty());
        // The node with id 0 was inserted with the same vector as
        // the query, so it must be the nearest match.
        assert_eq!(res[0].id, 0);
    }

    #[test]
    fn delete_excludes_from_search() {
        let mut idx = HnswIndex::new(Distance::Euclidean, HnswParams::default());
        for i in 0..30_u64 {
            idx.insert(i, unit(i + 1, 8)).unwrap();
        }
        let q = unit(1, 8);
        let before = idx.search(&q, 5, None).unwrap();
        let target = before[0].id;
        assert!(idx.delete(target));
        let after = idx.search(&q, 5, None).unwrap();
        assert!(after.iter().all(|r| r.id != target));
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let mut idx = HnswIndex::new(Distance::Euclidean, HnswParams::default());
        idx.insert(0, vec![0.1, 0.2, 0.3]).unwrap();
        assert!(matches!(
            idx.insert(1, vec![0.1, 0.2]),
            Err(IndexError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut idx = HnswIndex::new(Distance::Euclidean, HnswParams::default());
        idx.insert(7, vec![0.1, 0.2]).unwrap();
        assert!(matches!(
            idx.insert(7, vec![0.3, 0.4]),
            Err(IndexError::Duplicate(7))
        ));
    }

    #[test]
    fn empty_index_search_is_empty() {
        let idx = HnswIndex::new(Distance::Euclidean, HnswParams::default());
        let res = idx.search(&[0.1, 0.2], 5, None).unwrap();
        assert!(res.is_empty());
    }
}
