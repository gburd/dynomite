//! `turbovec`-backed approximate nearest-neighbour table.
//!
//! This module provides [`TurboTable`], a SIMD-search ANN
//! container that drops in alongside [`crate::index::HnswIndex`]
//! when a table's [`crate::encoding::Codec`] is one of the
//! `Turbovec*` variants. The table holds:
//!
//! * a [`turbovec::TurboQuantIndex`] storing every vector in its
//!   2/3/4-bit packed form (where the 8x to 16x compression and
//!   SIMD scoring kernels live);
//! * a slot-to-id and id-to-slot map so external [`NodeId`]
//!   handles survive turbovec's positional layout;
//! * a parallel `Vec<Option<NodeId>>` indexed by turbovec slot,
//!   used to translate the search results' positional indices
//!   back into stable ids.
//!
//! Concurrency mirrors the HNSW path: the storage layer holds a
//! per-table `Mutex` across an insert / search call.
//!
//! # Distance handling
//!
//! turbovec returns an inner-product-style similarity score per
//! candidate. To honour [`crate::distance::Distance`] semantics,
//! [`TurboTable`] L2-normalises queries and stored vectors at
//! ingest time when the table's metric is `Cosine` or
//! `Euclidean`, so the SIMD score becomes an estimate of
//! `cos(theta)`. The reported [`SearchResult::score`] is then
//! mapped to the metric's smaller-is-closer convention so the
//! result aligns with the rest of the engine.

use std::collections::HashMap;

use turbovec::TurboQuantIndex;

use crate::distance::Distance;
use crate::index::{IndexError, NodeId, SearchResult};

/// Approximate-nearest-neighbour table backed by
/// [`turbovec::TurboQuantIndex`].
pub struct TurboTable {
    bits: u8,
    distance: Distance,
    dim: u16,
    /// turbovec index. Holds compressed packed codes, per-vector
    /// scales and the per-table TQ+ calibration, plus the lazy
    /// SIMD layout cache. Re-built fresh on rehydrate.
    index: TurboQuantIndex,
    /// Parallel to turbovec's positional slot index. `None` for a
    /// soft-deleted slot whose adjacency we still own. Soft
    /// deletes flag the slot here so the search filter can drop
    /// the hit without disturbing turbovec's positional layout.
    slots: Vec<Option<NodeId>>,
    /// External-id lookup so `delete(NodeId)` is O(1).
    id_to_slot: HashMap<NodeId, usize>,
}

impl TurboTable {
    /// Build an empty turbovec-backed table.
    ///
    /// # Errors
    ///
    /// [`IndexError::Empty`] when `dim == 0`, or when `bits` is
    /// not in `{2, 3, 4}`. [`IndexError::DimensionMismatch`]
    /// when `dim` is not a positive multiple of 8 (turbovec's
    /// only dimensional constraint); the `expected` field is
    /// rounded up to the next multiple of 8 to give the caller
    /// a workable suggestion.
    pub fn new(distance: Distance, dim: u16, bits: u8) -> Result<Self, IndexError> {
        if dim == 0 {
            return Err(IndexError::Empty);
        }
        if !dim.is_multiple_of(8) {
            return Err(IndexError::DimensionMismatch {
                expected: ((dim / 8) + 1) * 8,
                got: dim,
            });
        }
        if !(2..=4).contains(&bits) {
            return Err(IndexError::Empty);
        }
        let index = TurboQuantIndex::new(usize::from(dim), usize::from(bits))
            .map_err(|_| IndexError::Empty)?;
        Ok(Self {
            bits,
            distance,
            dim,
            index,
            slots: Vec::new(),
            id_to_slot: HashMap::new(),
        })
    }

    /// Number of live (non-deleted) vectors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.slots.iter().filter(|s| s.is_some()).count()
    }

    /// `true` when there are no live vectors.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Vector dimension.
    #[must_use]
    pub fn dim(&self) -> u16 {
        self.dim
    }

    /// Distance metric this table was built with.
    #[must_use]
    pub fn distance(&self) -> Distance {
        self.distance
    }

    /// Bit width handed to turbovec.
    #[must_use]
    pub fn bits(&self) -> u8 {
        self.bits
    }

    /// Insert a vector under `id`.
    ///
    /// `vector` is taken in the application's `f32` form; the
    /// turbovec encode pipeline handles rotation, calibration
    /// and packing. When the table's metric is `Cosine` or
    /// `Euclidean` the vector is L2-normalised before being
    /// added, so turbovec's inner-product surrogate doubles as
    /// a cosine estimate.
    ///
    /// # Errors
    ///
    /// [`IndexError::Empty`] for a zero-dim vector,
    /// [`IndexError::DimensionMismatch`] when the vector's
    /// dimension differs from the table's frozen dim, and
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
        if self.id_to_slot.contains_key(&id) {
            return Err(IndexError::Duplicate(id));
        }
        let prepared = match self.distance {
            Distance::Cosine | Distance::Euclidean => l2_normalise(&vector),
            Distance::DotProduct => vector,
        };
        // turbovec rejects coordinates whose magnitude exceeds
        // 1e16 or that are non-finite. Map both to
        // `IndexError::Empty` so the storage layer reports them
        // as a generic input-rejection; the encoder layer
        // already filters NaN/Inf before reaching here in the
        // typical path.
        self.index
            .add_2d(&prepared, usize::from(self.dim))
            .map_err(|_| IndexError::Empty)?;
        let slot = self.slots.len();
        self.slots.push(Some(id));
        self.id_to_slot.insert(id, slot);
        Ok(())
    }

    /// Soft-delete the vector at `id`. The slot stays in the
    /// turbovec index for positional integrity but is filtered
    /// out of search results via the bool mask.
    ///
    /// Returns `true` when the id was present, `false`
    /// otherwise.
    pub fn delete(&mut self, id: NodeId) -> bool {
        let Some(slot) = self.id_to_slot.remove(&id) else {
            return false;
        };
        if slot < self.slots.len() {
            self.slots[slot] = None;
        }
        true
    }

    /// `true` when `id` is currently a live vector.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.id_to_slot.contains_key(&id)
    }

    /// Search for the `k` nearest neighbours of `query`. The
    /// `_ef` argument is accepted for HNSW API parity and
    /// ignored; turbovec scans every block and does not expose
    /// a beam-width knob.
    ///
    /// # Errors
    ///
    /// [`IndexError::DimensionMismatch`] when the query
    /// dimension does not match the table's frozen dim.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        _ef: Option<usize>,
    ) -> Result<Vec<SearchResult>, IndexError> {
        if query.is_empty() || self.slots.is_empty() {
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
        let mask: Vec<bool> = self.slots.iter().map(Option::is_some).collect();
        let allowed = mask.iter().filter(|b| **b).count();
        if allowed == 0 {
            return Ok(Vec::new());
        }
        let res = self.index.search_with_mask(&prepared, k, Some(&mask));
        let mut out = Vec::with_capacity(res.k);
        for i in 0..res.k {
            let raw_idx = res.indices[i];
            if raw_idx < 0 {
                // turbovec pads the result row with -1 when
                // fewer than `k` candidates survive the mask.
                continue;
            }
            let Ok(slot) = usize::try_from(raw_idx) else {
                continue;
            };
            let Some(Some(node_id)) = self.slots.get(slot) else {
                continue;
            };
            let similarity = res.scores[i];
            let score = match self.distance {
                Distance::DotProduct => -similarity,
                Distance::Cosine => 1.0 - similarity,
                Distance::Euclidean => (2.0 - 2.0 * similarity).max(0.0).sqrt(),
            };
            out.push(SearchResult {
                id: *node_id,
                score,
            });
        }
        // turbovec returns results sorted descending on
        // similarity; the mappings above flip the sense for
        // Cosine and Euclidean. A final sort enforces the
        // smaller-is-closer convention used elsewhere in the
        // engine.
        out.sort_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        out.truncate(k);
        Ok(out)
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
        let mut x = seed;
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
    fn insert_and_search_returns_self_first() {
        let mut t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        let target = rand_vec(42, 64);
        t.insert(0, target.clone()).unwrap();
        for i in 1..50_u64 {
            t.insert(i, rand_vec(i.wrapping_mul(1_000_003) + 1, 64))
                .unwrap();
        }
        let res = t.search(&target, 3, None).unwrap();
        assert!(!res.is_empty());
        assert_eq!(res[0].id, 0);
    }

    #[test]
    fn delete_excludes_from_search() {
        let mut t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        for i in 0..30_u64 {
            t.insert(i, rand_vec(i + 1, 64)).unwrap();
        }
        let q = rand_vec(1, 64);
        let before = t.search(&q, 5, None).unwrap();
        let target = before[0].id;
        assert!(t.delete(target));
        let after = t.search(&q, 5, None).unwrap();
        assert!(after.iter().all(|r| r.id != target));
    }

    #[test]
    fn empty_table_search_is_empty() {
        let t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        assert!(t.search(&rand_vec(0, 64), 5, None).unwrap().is_empty());
    }

    #[test]
    fn duplicate_id_rejected() {
        let mut t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        t.insert(7, rand_vec(7, 64)).unwrap();
        assert!(matches!(
            t.insert(7, rand_vec(8, 64)),
            Err(IndexError::Duplicate(7))
        ));
    }

    #[test]
    fn dimension_mismatch_rejected() {
        let mut t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        assert!(matches!(
            t.insert(0, vec![0.1; 32]),
            Err(IndexError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn new_rejects_bad_parameters() {
        // Zero dim.
        assert!(matches!(
            TurboTable::new(Distance::Cosine, 0, 4),
            Err(IndexError::Empty)
        ));
        // Dim not a multiple of 8 suggests the next multiple.
        match TurboTable::new(Distance::Cosine, 60, 4) {
            Err(IndexError::DimensionMismatch { expected, got }) => {
                assert_eq!(got, 60);
                assert_eq!(expected, 64);
            }
            Err(other) => panic!("expected DimensionMismatch, got {other:?}"),
            Ok(_) => panic!("expected DimensionMismatch, got Ok"),
        }
        // Bits outside {2,3,4}.
        assert!(matches!(
            TurboTable::new(Distance::Cosine, 64, 1),
            Err(IndexError::Empty)
        ));
        assert!(matches!(
            TurboTable::new(Distance::Cosine, 64, 5),
            Err(IndexError::Empty)
        ));
    }

    #[test]
    fn accessors_report_construction_state() {
        let mut t = TurboTable::new(Distance::Euclidean, 64, 3).unwrap();
        assert_eq!(t.dim(), 64);
        assert_eq!(t.distance(), Distance::Euclidean);
        assert_eq!(t.bits(), 3);
        assert!(t.is_empty());
        assert_eq!(t.len(), 0);
        t.insert(1, rand_vec(1, 64)).unwrap();
        assert!(!t.is_empty());
        assert_eq!(t.len(), 1);
        assert!(t.contains(1));
        assert!(!t.contains(2));
        // Soft delete drops the live count and `contains`.
        assert!(t.delete(1));
        assert_eq!(t.len(), 0);
        assert!(!t.contains(1));
        // Deleting a missing id is a no-op false.
        assert!(!t.delete(99));
    }

    #[test]
    fn insert_rejects_empty_vector() {
        let mut t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        assert!(matches!(t.insert(0, Vec::new()), Err(IndexError::Empty)));
    }

    #[test]
    fn dot_product_metric_round_trips() {
        // DotProduct skips normalisation on both insert and
        // search, and maps similarity to `-similarity`.
        let mut t = TurboTable::new(Distance::DotProduct, 64, 4).unwrap();
        let target = rand_vec(11, 64);
        t.insert(0, target.clone()).unwrap();
        for i in 1..20_u64 {
            t.insert(i, rand_vec(i.wrapping_mul(7) + 3, 64)).unwrap();
        }
        let res = t.search(&target, 3, None).unwrap();
        assert!(!res.is_empty());
        // Smaller-is-closer convention: scores ascend.
        for w in res.windows(2) {
            assert!(w[0].score <= w[1].score);
        }
    }

    #[test]
    fn euclidean_metric_search_maps_score() {
        let mut t = TurboTable::new(Distance::Euclidean, 64, 4).unwrap();
        let target = rand_vec(5, 64);
        t.insert(0, target.clone()).unwrap();
        for i in 1..20_u64 {
            t.insert(i, rand_vec(i + 100, 64)).unwrap();
        }
        let res = t.search(&target, 3, None).unwrap();
        assert!(!res.is_empty());
        // Euclidean score is sqrt(max(2 - 2*sim, 0)) >= 0.
        assert!(res.iter().all(|r| r.score >= 0.0));
        assert_eq!(res[0].id, 0);
    }

    #[test]
    fn search_with_all_slots_deleted_is_empty() {
        let mut t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        for i in 0..5_u64 {
            t.insert(i, rand_vec(i + 1, 64)).unwrap();
        }
        for i in 0..5_u64 {
            assert!(t.delete(i));
        }
        // Slots are non-empty but every entry is masked out.
        let res = t.search(&rand_vec(1, 64), 3, None).unwrap();
        assert!(res.is_empty());
    }

    #[test]
    fn search_dimension_mismatch_rejected() {
        let mut t = TurboTable::new(Distance::Cosine, 64, 4).unwrap();
        t.insert(0, rand_vec(1, 64)).unwrap();
        assert!(matches!(
            t.search(&[0.1; 32], 3, None),
            Err(IndexError::DimensionMismatch { .. })
        ));
    }

    #[test]
    fn l2_normalise_zero_vector_is_returned_unchanged() {
        let zero = vec![0.0_f32; 8];
        assert_eq!(l2_normalise(&zero), zero);
        // A unit-ish vector normalises to magnitude ~1.
        let v = vec![3.0_f32, 4.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let n = l2_normalise(&v);
        let mag: f32 = n.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((mag - 1.0).abs() < 1e-5, "magnitude {mag}");
    }
}
