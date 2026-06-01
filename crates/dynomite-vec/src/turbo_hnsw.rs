//! HNSW topology over `turbovec` packed codes.
//!
//! Combines two existing pieces of this crate:
//!
//! * The hand-rolled HNSW graph from [`crate::index`] (Malkov &
//!   Yashunin, TPAMI 2018) for sub-linear traversal.
//! * The TurboQuant 2/3/4-bit codebook from the `turbovec`
//!   crate for the per-vector storage and the per-pair scoring
//!   kernel.
//!
//! The brute-force [`crate::turbo_index::TurboTable`] dominates
//! at small corpora because its SIMD scan is so fast, but at
//! 100k+ vectors the linear scan caps p99 latency. HNSW gives
//! an `O(log N)` traversal: ~1000 distance ops per query at
//! 100k versus 100k for the brute-force scan. This module pairs
//! that traversal with a turbovec-derived per-pair scorer so
//! the resulting search keeps the codec's compression and
//! quantisation accuracy.
//!
//! # Distance kernel
//!
//! `turbovec`'s public search API (`TurboQuantIndex::search`
//! and `search_with_mask`) is bulk-only: every call scores
//! every block in the index, with at best a per-block skip
//! when a contiguous 32-vector block has no allowed slots.
//! That pattern is the wrong shape for HNSW, where each
//! traversal step needs the score against ~M=16 scattered
//! candidates and would re-pay the LUT-build + full scan
//! cost at every step.
//!
//! Instead this module implements the per-pair scoring kernel
//! in pure safe Rust, on top of the public `turbovec::codebook`
//! and `turbovec::rotation` primitives. Each stored vector is
//! quantised to the codebook lattice and persisted as one
//! `u8` per coordinate (low `BITS` bits used) plus a per-
//! vector `f32` scale. The byte-per-code layout is `BITS`
//! times bigger on disk than `turbovec`'s bit-plane packed
//! format but lets the scoring kernel walk a contiguous
//! `u8` slice and feed a clean dot product, which auto-
//! vectorises through LLVM. The crate-level
//! `forbid(unsafe_code)` rules out the intrinsics-driven
//! SIMD path that `turbovec::search` uses, so the layout
//! choice is what unlocks SIMD here.
//!
//! Compared to the brute-force [`crate::turbo_index::TurboTable`]
//! the per-vec memory is `8 / BITS` times bigger (e.g. 4x
//! at 2-bit, 2x at 4-bit) but still smaller than the f32
//! HNSW path, and the HNSW topology cuts the per-query
//! work from `O(N)` distance calls to `O(log N)`.
//!
//! # Recall
//!
//! TQ+ per-coordinate calibration is disabled (identity shift
//! and scale). Fitting TQ+ requires a batched first-add of at
//! least 1000 vectors to estimate per-coordinate quantiles;
//! the HNSW path is incremental, so an identity calibration is
//! the honest default. The recall tests in
//! `tests/turbo_hnsw.rs` confirm this stays inside the same
//! `>= 85%` budget the brute [`crate::turbo_index::TurboTable`]
//! tests use.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use turbovec::codebook::codebook;
use turbovec::rotation::make_rotation_matrix;

use crate::distance::Distance;
use crate::index::{HnswParams, IndexError, NodeId, SearchResult};

/// Distance abstraction that every `dynvec` ANN container
/// honours.
///
/// The trait is intentionally narrow: a single
/// `(NodeId, NodeId) -> f32` score is enough to drive the HNSW
/// pruning heuristics (`select_neighbours` and
/// `shrink_connections`). Query-to-node scoring during search
/// is handled by each impl directly because the query's f32
/// representation is in scope at that layer. Smaller scores
/// mean closer.
pub trait CodecDistance {
    /// Score the stored vectors at `a` and `b` against each
    /// other.
    ///
    /// The score is in the metric's smaller-is-closer
    /// convention so the same heap comparator works
    /// regardless of the underlying distance.
    fn distance(&self, a: NodeId, b: NodeId) -> f32;
}

/// One node in the HNSW graph held by [`TurboHnswIndex`].
#[derive(Clone, Debug)]
struct TurboHnswNode {
    id: NodeId,
    /// Adjacency lists, one per layer. `levels[0]` is the base
    /// layer; higher indices are the sparser upper layers.
    levels: Vec<Vec<usize>>,
    /// Soft-deleted node. Tombstoned nodes are skipped during
    /// search but their adjacency stays so the graph topology
    /// is preserved until a future compaction rebuilds.
    deleted: bool,
}

impl TurboHnswNode {
    fn level(&self) -> usize {
        self.levels.len().saturating_sub(1)
    }
}

/// HNSW graph over `turbovec`-packed codes.
///
/// `BITS` is the per-coordinate bit width; only `2`, `3`, and
/// `4` are valid. Each vector occupies `dim * BITS / 8` bytes
/// of packed storage plus a single `f32` per-vector scale.
pub struct TurboHnswIndex<const BITS: u8> {
    /// Distance metric the index was built with.
    distance: Distance,
    /// Frozen vector dimension. Must be a positive multiple of 8.
    dim: u16,
    /// HNSW tuning parameters; mirrors the f32 path in
    /// [`crate::index::HnswIndex`].
    params: HnswParams,

    /// Random rotation matrix shared with the `turbovec`
    /// encoder. Row-major, dim x dim.
    rotation: Vec<f32>,
    /// Lloyd-Max codebook boundaries: one f32 per quantisation
    /// edge, with `2^BITS - 1` edges total.
    boundaries: Vec<f32>,
    /// Lloyd-Max codebook centroids: one f32 per quantisation
    /// bucket, with `2^BITS` buckets total.
    centroids: Vec<f32>,

    /// Flat code buffer, one `u8` per coordinate. Slot `i`
    /// occupies `[i * dim, (i + 1) * dim)`; only the low
    /// `BITS` bits of each byte are populated. Sequential
    /// access keeps the scoring loop SIMD-friendly.
    packed: Vec<u8>,
    /// Per-vector scale fitted by the encoder. Smaller-is-
    /// closer scoring multiplies through this scale.
    scales: Vec<f32>,

    /// HNSW node table. Slot index in `nodes` matches slot
    /// index in `packed` and `scales`.
    nodes: Vec<TurboHnswNode>,
    /// External-id lookup so `delete(NodeId)` and
    /// `contains(NodeId)` are O(1).
    id_to_idx: HashMap<NodeId, usize>,
    /// Index of the entry-point node, or `None` for an empty
    /// index.
    entry: Option<usize>,
    /// PRNG state for layer assignment.
    rng_state: u64,
    /// `mL` factor for level assignment, cached because every
    /// insert calls it.
    ml: f64,
}

/// Min-heap entry on score; used as the search frontier.
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

impl<const BITS: u8> TurboHnswIndex<BITS> {
    /// Build an empty turbo-HNSW index for `dim` and the codec
    /// metric.
    ///
    /// # Errors
    ///
    /// [`IndexError::Empty`] when `BITS` is outside `{2, 3, 4}`
    /// or when `dim == 0`. [`IndexError::DimensionMismatch`]
    /// when `dim` is not a positive multiple of 8 (the
    /// `turbovec` codebook constraint); the `expected` field
    /// is rounded up to the next multiple of 8 to give the
    /// caller a workable suggestion.
    pub fn new(distance: Distance, dim: u16, params: HnswParams) -> Result<Self, IndexError> {
        if !(2..=4).contains(&BITS) {
            return Err(IndexError::Empty);
        }
        if dim == 0 {
            return Err(IndexError::Empty);
        }
        if !dim.is_multiple_of(8) {
            return Err(IndexError::DimensionMismatch {
                expected: ((dim / 8) + 1) * 8,
                got: dim,
            });
        }
        let dim_usize = usize::from(dim);
        let bits_usize = usize::from(BITS);
        let rotation = make_rotation_matrix(dim_usize);
        let (boundaries, centroids) = codebook(bits_usize, dim_usize);
        let ml = if params.m > 1 {
            1.0 / f64::from(u32::try_from(params.m).unwrap_or(u32::MAX)).ln()
        } else {
            1.0
        };
        Ok(Self {
            distance,
            dim,
            params,
            rotation,
            boundaries,
            centroids,
            packed: Vec::new(),
            scales: Vec::new(),
            nodes: Vec::new(),
            id_to_idx: HashMap::new(),
            entry: None,
            rng_state: params.seed,
            ml,
        })
    }

    /// Number of live (non-deleted) nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.iter().filter(|n| !n.deleted).count()
    }

    /// `true` when no live nodes exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Frozen vector dimension.
    #[must_use]
    pub fn dim(&self) -> u16 {
        self.dim
    }

    /// Distance metric.
    #[must_use]
    pub fn distance_metric(&self) -> Distance {
        self.distance
    }

    /// Bit width handed to `turbovec`.
    #[must_use]
    pub fn bits(&self) -> u8 {
        BITS
    }

    /// `true` when `id` is currently a live node.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.id_to_idx
            .get(&id)
            .is_some_and(|&idx| !self.nodes[idx].deleted)
    }

    /// Insert a new vector under `id`.
    ///
    /// # Errors
    ///
    /// [`IndexError::Empty`] for a zero-dim vector,
    /// [`IndexError::DimensionMismatch`] when the vector's
    /// dimension does not match the index's frozen dim, and
    /// [`IndexError::Duplicate`] when `id` is already present.
    pub fn insert(&mut self, id: NodeId, vector: Vec<f32>) -> Result<(), IndexError> {
        if vector.is_empty() {
            return Err(IndexError::Empty);
        }
        let got = u16::try_from(vector.len()).unwrap_or(u16::MAX);
        if got != self.dim {
            return Err(IndexError::DimensionMismatch {
                expected: self.dim,
                got,
            });
        }
        if self.id_to_idx.contains_key(&id) {
            return Err(IndexError::Duplicate(id));
        }
        // Cosine and Euclidean both decompose into an inner
        // product on L2-normalised inputs; matching the
        // `TurboTable` policy keeps the on-codec score
        // comparable across the brute and HNSW paths.
        let prepared = match self.distance {
            Distance::Cosine | Distance::Euclidean => l2_normalise(&vector),
            Distance::DotProduct => vector,
        };
        // Reject non-finite or huge-magnitude coordinates
        // before they reach `turbovec::encode`, which would
        // panic the process otherwise.
        for v in &prepared {
            if !v.is_finite() || v.abs() >= 1e16_f32 {
                return Err(IndexError::Empty);
            }
        }
        let dim_usize = usize::from(self.dim);
        let bytes_per_vec = self.bytes_per_vec();
        let (packed, scale) = self.encode_one(&prepared);
        debug_assert_eq!(packed.len(), bytes_per_vec);
        let _ = dim_usize;

        // Encode-and-store happens before the graph wiring so
        // the new node's slot index matches `nodes.len()` once
        // the node record is pushed below.
        self.packed.extend_from_slice(&packed);
        self.scales.push(scale);

        let level = self.random_level();
        let mut levels: Vec<Vec<usize>> = Vec::with_capacity(level + 1);
        for _ in 0..=level {
            levels.push(Vec::new());
        }

        let new_idx = self.nodes.len();
        self.nodes.push(TurboHnswNode {
            id,
            levels,
            deleted: false,
        });
        self.id_to_idx.insert(id, new_idx);

        let Some(entry) = self.entry else {
            self.entry = Some(new_idx);
            return Ok(());
        };
        let entry_level = self.nodes[entry].level();

        // Phase 1: descend through layers above `level`,
        // narrowing to the best entry point at level + 1.
        let q_rot = self.rotate(&prepared);
        let mut current = entry;
        if entry_level > level {
            for lc in (level + 1..=entry_level).rev() {
                current = self.greedy_search_layer(&q_rot, current, lc, new_idx);
            }
        }

        // Phase 2: at every layer from min(level, entry_level)
        // down to 0, beam-search for ef_construction
        // candidates and connect.
        let start_layer = level.min(entry_level);
        let mut entry_points = vec![current];
        for lc in (0..=start_layer).rev() {
            let neighbours = self.search_layer(
                &q_rot,
                &entry_points,
                lc,
                self.params.ef_construction,
                /* skip_idx = */ Some(new_idx),
            );
            let m = if lc == 0 {
                self.params.m0
            } else {
                self.params.m
            };
            let selected = Self::select_neighbours(&neighbours, m);
            for &nb in &selected {
                self.nodes[new_idx].levels[lc].push(nb);
                self.nodes[nb].levels[lc].push(new_idx);
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

        if level > entry_level {
            self.entry = Some(new_idx);
        }
        Ok(())
    }

    /// Soft-delete `id`. Returns `true` when the id was a live
    /// node, `false` otherwise.
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
    /// `ef` overrides the default `ef_search` beam width.
    ///
    /// # Errors
    ///
    /// [`IndexError::DimensionMismatch`] when the query's
    /// dimension does not match the index's frozen dim.
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
        if got != self.dim {
            return Err(IndexError::DimensionMismatch {
                expected: self.dim,
                got,
            });
        }

        let prepared = match self.distance {
            Distance::Cosine | Distance::Euclidean => l2_normalise(query),
            Distance::DotProduct => query.to_vec(),
        };
        let q_rot = self.rotate(&prepared);

        let mut entry = self.entry.unwrap_or(0);
        let entry_level = self.nodes[entry].level();
        let ef = ef.unwrap_or(self.params.ef_search).max(k);

        for lc in (1..=entry_level).rev() {
            entry = self.greedy_search_layer(&q_rot, entry, lc, usize::MAX);
        }

        let candidates = self.search_layer(&q_rot, &[entry], 0, ef, None);

        let mut sorted = candidates;
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

    /// Number of bytes occupied by one stored vector. With
    /// the byte-per-code layout this is just `dim`.
    fn bytes_per_vec(&self) -> usize {
        usize::from(self.dim)
    }

    /// Single-vector encode: rotate, quantise to the codebook
    /// lattice, fit a per-vector scale, and emit one `u8`
    /// code per coordinate.
    ///
    /// The public `turbovec::encode::encode` runs the
    /// quantisation pipeline through Rayon and is sized for
    /// big batches; calling it once per HNSW insert pays the
    /// thread-pool setup on every vector and balloons build
    /// time by orders of magnitude. This routine reproduces
    /// the per-row math (normalise, rotate, quantise, fit
    /// per-vector scale) in scalar code so each insert stays
    /// inexpensive. TQ+ per-coordinate calibration is the
    /// identity (no shift, no scale); fitting it would need
    /// a 1000-vector batch which the incremental HNSW path
    /// does not have at insert time.
    fn encode_one(&self, vector: &[f32]) -> (Vec<u8>, f32) {
        let dim = usize::from(self.dim);
        // 1. Norm and unit vector.
        let mut norm_sq = 0.0_f32;
        for &x in vector {
            norm_sq += x * x;
        }
        let norm = norm_sq.sqrt();
        let inv_norm = if norm > 1e-10 { 1.0 / norm } else { 0.0 };
        let mut unit = vec![0.0_f32; dim];
        for (d, slot) in unit.iter_mut().enumerate().take(dim) {
            *slot = vector[d] * inv_norm;
        }
        // 2. Rotate: u_rot = R @ unit.
        let u_rot = self.rotate(&unit);
        // 3. Quantise to centroid codes; fit the scale by
        // accumulating the unit-vec inner product against the
        // chosen centroids.
        let mut packed = vec![0_u8; dim];
        let mut inner = 0.0_f32;
        for (j, &uj) in u_rot.iter().enumerate().take(dim) {
            let mut code = 0_u8;
            for &b in &self.boundaries {
                if uj > b {
                    code += 1;
                }
            }
            inner += uj * self.centroids[usize::from(code)];
            packed[j] = code;
        }
        // 4. Per-vector scale: norm / <u_rot, x_hat>. Floor at
        // 1e-10 so a vanishing inner product cannot produce
        // an infinite scale.
        let inner = inner.max(1e-10_f32);
        let scale = norm / inner;
        (packed, scale)
    }

    /// Borrow the contiguous byte slice for slot `slot`.
    fn codes(&self, slot: usize) -> &[u8] {
        let dim = usize::from(self.dim);
        let row_start = slot * dim;
        &self.packed[row_start..row_start + dim]
    }

    /// Multiply the rotation matrix by `q` and return `R @ q`.
    fn rotate(&self, q: &[f32]) -> Vec<f32> {
        let dim = usize::from(self.dim);
        let mut out = vec![0.0_f32; dim];
        for (d, slot) in out.iter_mut().enumerate().take(dim) {
            let row = &self.rotation[d * dim..(d + 1) * dim];
            let mut sum = 0.0_f32;
            for (e, &qe) in q.iter().enumerate().take(dim) {
                sum += row[e] * qe;
            }
            *slot = sum;
        }
        out
    }

    /// Inner-product surrogate: `<q_rot, x_hat[slot]> *
    /// scale[slot]`.
    ///
    /// The result is the codec's similarity estimate, in
    /// `(-||q||, +||q||)` for unit-normalised queries. The
    /// metric mapping in [`Self::similarity_to_distance`]
    /// turns that into a smaller-is-closer score.
    fn similarity_query(&self, q_rot: &[f32], slot: usize) -> f32 {
        let dim = usize::from(self.dim);
        let codes = self.codes(slot);
        let centroids = self.centroids.as_slice();
        let mut acc = 0.0_f32;
        for d in 0..dim {
            acc += q_rot[d] * centroids[codes[d] as usize];
        }
        acc * self.scales[slot]
    }

    /// Inner-product surrogate between two stored slots.
    ///
    /// The rotation is orthogonal, so
    /// `<v_a, v_b> ~= scale_a * scale_b * <x_hat_a, x_hat_b>`.
    /// Both vectors are quantised, so the kernel sees the
    /// double-quantisation error; recall on the pair-only path
    /// (used by `shrink_connections`) is tighter than on the
    /// query-to-stored path.
    fn similarity_pair(&self, a: usize, b: usize) -> f32 {
        let dim = usize::from(self.dim);
        let ca = self.codes(a);
        let cb = self.codes(b);
        let centroids = self.centroids.as_slice();
        let mut acc = 0.0_f32;
        for d in 0..dim {
            acc += centroids[ca[d] as usize] * centroids[cb[d] as usize];
        }
        acc * self.scales[a] * self.scales[b]
    }

    /// Map a codec similarity into the smaller-is-closer
    /// distance convention used elsewhere in `dynvec`.
    fn similarity_to_distance(&self, similarity: f32) -> f32 {
        match self.distance {
            Distance::DotProduct => -similarity,
            Distance::Cosine => 1.0 - similarity,
            Distance::Euclidean => (2.0 - 2.0 * similarity).max(0.0).sqrt(),
        }
    }

    fn distance_query(&self, q_rot: &[f32], slot: usize) -> f32 {
        self.similarity_to_distance(self.similarity_query(q_rot, slot))
    }

    fn distance_pair(&self, a: usize, b: usize) -> f32 {
        self.similarity_to_distance(self.similarity_pair(a, b))
    }

    /// Greedy single-best descent at layer `lc`. `skip_idx` is
    /// the slot index of the node currently being inserted, if
    /// any; passing `usize::MAX` disables the filter for
    /// search-time queries.
    fn greedy_search_layer(
        &self,
        q_rot: &[f32],
        entry: usize,
        lc: usize,
        skip_idx: usize,
    ) -> usize {
        let mut current = entry;
        let mut current_score = self.distance_query(q_rot, current);
        loop {
            let mut improved = false;
            let next = if lc < self.nodes[current].levels.len() {
                let neighbours = self.nodes[current].levels[lc].as_slice();
                let mut best = (current, current_score);
                for &nb in neighbours {
                    if nb == skip_idx {
                        continue;
                    }
                    let s = self.distance_query(q_rot, nb);
                    if s < best.1 {
                        best = (nb, s);
                        improved = true;
                    }
                }
                best
            } else {
                (current, current_score)
            };
            current = next.0;
            current_score = next.1;
            if !improved {
                break;
            }
        }
        current
    }

    /// Beam search at layer `lc`. Returns up to `ef`
    /// candidates ordered by the underlying max-heap.
    fn search_layer(
        &self,
        q_rot: &[f32],
        entry_points: &[usize],
        lc: usize,
        ef: usize,
        skip_idx: Option<usize>,
    ) -> Vec<MaxCandidate> {
        let mut visited: HashSet<usize> = HashSet::new();
        let mut frontier: BinaryHeap<Candidate> = BinaryHeap::new();
        let mut top: BinaryHeap<MaxCandidate> = BinaryHeap::new();
        for &ep in entry_points {
            if Some(ep) == skip_idx {
                continue;
            }
            if visited.insert(ep) {
                let s = self.distance_query(q_rot, ep);
                frontier.push(Candidate { idx: ep, score: s });
                top.push(MaxCandidate { idx: ep, score: s });
            }
        }
        while let Some(c) = frontier.pop() {
            if top.len() >= ef {
                if let Some(worst) = top.peek() {
                    if c.score > worst.score {
                        break;
                    }
                }
            }
            if lc < self.nodes[c.idx].levels.len() {
                let neighbours = self.nodes[c.idx].levels[lc].as_slice();
                for &nb in neighbours {
                    if Some(nb) == skip_idx {
                        continue;
                    }
                    if !visited.insert(nb) {
                        continue;
                    }
                    let s = self.distance_query(q_rot, nb);
                    let admit = match top.peek() {
                        Some(worst) => s < worst.score || top.len() < ef,
                        None => true,
                    };
                    if admit {
                        frontier.push(Candidate { idx: nb, score: s });
                        top.push(MaxCandidate { idx: nb, score: s });
                        if top.len() > ef {
                            top.pop();
                        }
                    }
                }
            }
        }
        top.into_vec()
    }

    /// Pick the top-`m` by closest-first heuristic.
    fn select_neighbours(candidates: &[MaxCandidate], m: usize) -> Vec<usize> {
        let mut sorted: Vec<MaxCandidate> = candidates.to_vec();
        sorted.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted.into_iter().take(m).map(|c| c.idx).collect()
    }

    /// Drop the longest edges from a node's adjacency list at
    /// `lc` until it fits in `cap`. Uses [`Self::distance_pair`]
    /// for stored-to-stored scoring; that double-quantisation
    /// error is the cost of dropping the f32 fallback that
    /// [`crate::index::HnswIndex::shrink_connections`] uses.
    fn shrink_connections(&mut self, idx: usize, lc: usize, cap: usize) {
        let neighbours = std::mem::take(&mut self.nodes[idx].levels[lc]);
        let mut scored: Vec<(usize, f32)> = neighbours
            .into_iter()
            .map(|nb| {
                let s = self.distance_pair(idx, nb);
                (nb, s)
            })
            .collect();
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(cap);
        self.nodes[idx].levels[lc] = scored.into_iter().map(|(nb, _)| nb).collect();
    }

    /// xorshift64* PRNG, deterministic for a given seed.
    fn rand_unit(&mut self) -> f64 {
        let mut x = self.rng_state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng_state = x;
        let r = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        let bits = (r >> 11) & ((1_u64 << 53) - 1);
        // `bits` is in [0, 2^53), exactly representable as f64.
        #[allow(
            clippy::cast_precision_loss,
            reason = "bits is in [0, 2^53), exactly representable as f64"
        )]
        let f = (bits as f64) / ((1_u64 << 53) as f64);
        f
    }

    /// Random level assignment: `floor(-ln(uniform(0, 1)) *
    /// mL)` capped at 16 to keep allocations sane.
    fn random_level(&mut self) -> usize {
        let r = self.rand_unit().max(f64::MIN_POSITIVE);
        let level = (-r.ln() * self.ml).floor();
        let clamped = level.clamp(0.0, 16.0);
        // Clamped to [0, 16]; the cast is well-defined.
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "clamped to [0, 16]"
        )]
        let lvl = clamped as usize;
        lvl
    }
}

impl<const BITS: u8> CodecDistance for TurboHnswIndex<BITS> {
    fn distance(&self, a: NodeId, b: NodeId) -> f32 {
        let Some(&sa) = self.id_to_idx.get(&a) else {
            return f32::INFINITY;
        };
        let Some(&sb) = self.id_to_idx.get(&b) else {
            return f32::INFINITY;
        };
        self.distance_pair(sa, sb)
    }
}

fn l2_normalise(v: &[f32]) -> Vec<f32> {
    let n2: f32 = v.iter().map(|x| x * x).sum();
    let n = n2.sqrt();
    if n <= 0.0 {
        return v.to_vec();
    }
    v.iter().map(|x| x / n).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
        let mut x = if seed == 0 { 0xDEAD_BEEF } else { seed };
        let mut v = Vec::with_capacity(dim);
        for _ in 0..dim {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let bits = (x >> 11) & ((1_u64 << 53) - 1);
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                reason = "test fixture: PRNG narrowed to f32"
            )]
            let r = (((bits as f64) / ((1_u64 << 53) as f64)) * 2.0 - 1.0) as f32;
            v.push(r);
        }
        v
    }

    #[test]
    fn insert_and_search_returns_self_first_4bit() {
        let mut idx = TurboHnswIndex::<4>::new(Distance::Cosine, 64, HnswParams::default())
            .expect("4-bit ctor");
        let target = rand_vec(42, 64);
        idx.insert(0, target.clone()).unwrap();
        for i in 1..50_u64 {
            idx.insert(i, rand_vec(i.wrapping_mul(1_000_003) + 1, 64))
                .unwrap();
        }
        let res = idx.search(&target, 3, None).unwrap();
        assert!(!res.is_empty());
        assert_eq!(res[0].id, 0);
    }

    #[test]
    fn delete_excludes_from_search() {
        let mut idx = TurboHnswIndex::<4>::new(Distance::Cosine, 64, HnswParams::default())
            .expect("4-bit ctor");
        for i in 0..30_u64 {
            idx.insert(i, rand_vec(i + 1, 64)).unwrap();
        }
        let q = rand_vec(1, 64);
        let before = idx.search(&q, 5, None).unwrap();
        let target = before[0].id;
        assert!(idx.delete(target));
        let after = idx.search(&q, 5, None).unwrap();
        assert!(after.iter().all(|r| r.id != target));
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut idx = TurboHnswIndex::<4>::new(Distance::Cosine, 64, HnswParams::default())
            .expect("4-bit ctor");
        idx.insert(7, rand_vec(7, 64)).unwrap();
        assert!(matches!(
            idx.insert(7, rand_vec(8, 64)),
            Err(IndexError::Duplicate(7))
        ));
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let mut idx = TurboHnswIndex::<4>::new(Distance::Cosine, 64, HnswParams::default())
            .expect("4-bit ctor");
        assert!(matches!(
            idx.insert(0, vec![0.1; 32]),
            Err(IndexError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn empty_index_search_is_empty() {
        let idx = TurboHnswIndex::<4>::new(Distance::Cosine, 64, HnswParams::default())
            .expect("4-bit ctor");
        let res = idx.search(&rand_vec(0, 64), 5, None).unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn ctor_rejects_misaligned_dim() {
        let r = TurboHnswIndex::<4>::new(Distance::Cosine, 7, HnswParams::default());
        assert!(matches!(
            r,
            Err(IndexError::DimensionMismatch {
                expected: 8,
                got: 7
            })
        ));
    }
}
