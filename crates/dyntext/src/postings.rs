//! Inverted-index data structure mapping trigram hash to the
//! set of document ids whose text contains the trigram.
//!
//! The mapping is stored as a [`BTreeMap`] keyed by the trigram
//! `u64` (so iteration is deterministic) with [`RoaringBitmap`]
//! values. Roaring is the right shape for this workload because
//! the postings lists are dense in popular trigrams and sparse
//! in rare ones, and Roaring's hybrid container layout
//! (run / array / bitmap) is competitive with both extremes
//! while keeping the compressed size proportional to the
//! shannon entropy of the doc-id stream rather than to its
//! cardinality.
//!
//! Intersection and union operate on slices of trigram hashes;
//! the slice ordering does not affect the result. Intersection
//! sorts by posting-list cardinality before reducing so the
//! shortest list bounds the working-set size of the reduce.

use std::collections::BTreeMap;

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

/// Inverted index: `trigram_hash -> bitmap of doc ids`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Postings {
    map: BTreeMap<u64, RoaringBitmap>,
}

impl Postings {
    /// Construct an empty postings index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    /// Number of distinct trigrams present in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the index has any entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Insert `doc_id` into the postings list for `trigram`.
    ///
    /// Idempotent: re-inserting the same `(trigram, doc_id)`
    /// pair is a no-op.
    pub fn insert(&mut self, trigram: u64, doc_id: u32) {
        self.map.entry(trigram).or_default().insert(doc_id);
    }

    /// Remove `doc_id` from the postings list for `trigram`.
    ///
    /// If removing leaves the postings list empty, the trigram
    /// entry is dropped from the map (garbage collection).
    pub fn remove(&mut self, trigram: u64, doc_id: u32) {
        let drop = match self.map.get_mut(&trigram) {
            Some(bm) => {
                bm.remove(doc_id);
                bm.is_empty()
            }
            None => false,
        };
        if drop {
            self.map.remove(&trigram);
        }
    }

    /// Borrow the postings bitmap for `trigram`, if any.
    #[must_use]
    pub fn lookup(&self, trigram: u64) -> Option<&RoaringBitmap> {
        self.map.get(&trigram)
    }

    /// Intersect the postings lists for the given trigrams.
    ///
    /// Returns the set of doc ids that appear in EVERY trigram's
    /// list. If `trigrams` is empty, the result is empty (the
    /// index does not invent a "universal" set: callers asking
    /// to intersect "no" trigrams should treat this as a
    /// no-evidence query and either fall back to a full scan or
    /// return the empty set, depending on policy).
    ///
    /// If any trigram has no postings entry, the result is empty
    /// (a missing trigram is provably absent from every doc).
    ///
    /// The reduction order is by ascending posting-list size so
    /// the working set shrinks as fast as possible.
    #[must_use]
    pub fn intersect(&self, trigrams: &[u64]) -> RoaringBitmap {
        if trigrams.is_empty() {
            return RoaringBitmap::new();
        }
        let mut order: Vec<u64> = trigrams.to_vec();
        order.sort_unstable();
        order.dedup();
        order.sort_by_key(|t| self.map.get(t).map_or(0, RoaringBitmap::len));

        let mut iter = order.into_iter();
        let Some(first_trigram) = iter.next() else {
            return RoaringBitmap::new();
        };
        let mut acc = match self.map.get(&first_trigram) {
            Some(b) => b.clone(),
            None => return RoaringBitmap::new(),
        };
        for t in iter {
            if acc.is_empty() {
                return acc;
            }
            match self.map.get(&t) {
                Some(b) => {
                    acc &= b;
                }
                None => return RoaringBitmap::new(),
            }
        }
        acc
    }

    /// Union the postings lists for the given trigrams.
    ///
    /// Returns the set of doc ids that appear in AT LEAST ONE of
    /// the trigrams' lists. Missing trigrams contribute nothing.
    /// An empty input yields the empty set.
    #[must_use]
    pub fn union(&self, trigrams: &[u64]) -> RoaringBitmap {
        let mut acc = RoaringBitmap::new();
        let mut seen: Vec<u64> = trigrams.to_vec();
        seen.sort_unstable();
        seen.dedup();
        for t in seen {
            if let Some(b) = self.map.get(&t) {
                acc |= b;
            }
        }
        acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postings_insert_and_lookup() {
        let mut p = Postings::new();
        p.insert(7, 1);
        p.insert(7, 2);
        p.insert(8, 2);
        let bm = p.lookup(7).expect("trigram 7 present");
        assert!(bm.contains(1));
        assert!(bm.contains(2));
        let bm = p.lookup(8).expect("trigram 8 present");
        assert!(bm.contains(2));
        assert!(!bm.contains(1));
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn postings_lookup_missing_returns_none() {
        let p = Postings::new();
        assert!(p.lookup(99).is_none());
    }

    #[test]
    fn postings_intersection_of_two_trigrams_yields_intersection() {
        let mut p = Postings::new();
        p.insert(1, 10);
        p.insert(1, 20);
        p.insert(1, 30);
        p.insert(2, 20);
        p.insert(2, 30);
        p.insert(2, 40);
        let r = p.intersect(&[1, 2]);
        assert!(r.contains(20));
        assert!(r.contains(30));
        assert!(!r.contains(10));
        assert!(!r.contains(40));
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn postings_intersection_of_disjoint_trigrams_is_empty() {
        let mut p = Postings::new();
        p.insert(1, 10);
        p.insert(1, 20);
        p.insert(2, 30);
        p.insert(2, 40);
        let r = p.intersect(&[1, 2]);
        assert!(r.is_empty());
    }

    #[test]
    fn postings_intersection_with_missing_trigram_is_empty() {
        let mut p = Postings::new();
        p.insert(1, 10);
        let r = p.intersect(&[1, 999]);
        assert!(r.is_empty());
    }

    #[test]
    fn postings_intersection_empty_input_is_empty() {
        let p = Postings::new();
        assert!(p.intersect(&[]).is_empty());
    }

    #[test]
    fn postings_intersection_single_trigram_returns_that_list() {
        let mut p = Postings::new();
        p.insert(1, 10);
        p.insert(1, 20);
        let r = p.intersect(&[1]);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn postings_union_basic() {
        let mut p = Postings::new();
        p.insert(1, 10);
        p.insert(2, 20);
        p.insert(3, 30);
        let r = p.union(&[1, 2]);
        assert!(r.contains(10));
        assert!(r.contains(20));
        assert!(!r.contains(30));
    }

    #[test]
    fn postings_remove_clears_doc_id_from_one_trigram() {
        let mut p = Postings::new();
        p.insert(1, 10);
        p.insert(1, 20);
        p.remove(1, 10);
        let bm = p.lookup(1).expect("trigram 1 still present");
        assert!(!bm.contains(10));
        assert!(bm.contains(20));
    }

    #[test]
    fn postings_dropping_a_trigram_when_no_docs_left_garbage_collects() {
        let mut p = Postings::new();
        p.insert(1, 10);
        assert_eq!(p.len(), 1);
        p.remove(1, 10);
        assert_eq!(p.len(), 0);
        assert!(p.lookup(1).is_none());
    }

    #[test]
    fn postings_remove_missing_is_a_noop() {
        let mut p = Postings::new();
        p.insert(1, 10);
        p.remove(1, 99);
        p.remove(2, 10);
        let bm = p.lookup(1).expect("trigram 1 still present");
        assert!(bm.contains(10));
    }
}
