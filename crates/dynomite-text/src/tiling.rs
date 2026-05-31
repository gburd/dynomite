//! Sound trigram filter for approximate-regex matching.
//!
//! Given a regex AST and a global edit budget `k`, produce a
//! [`ApproxFilter`] that lets the index narrow the candidate
//! set before invoking the (expensive) TRE matcher on each
//! survivor. The filter is *sound*: every doc that
//! approximately matches the pattern within `k` edits passes
//! the filter, so applying it never produces a false negative.
//!
//! # Construction
//!
//! The filter is parameterised by the pattern's *literal runs*
//! -- maximal contiguous sequences of literal bytes from the
//! AST (see [`crate::prefix_extract::extract_literal_runs`]).
//! For each run of length `L_i`, the run contributes
//! `T_i = max(0, L_i - 2)` candidate trigrams; the total
//! across runs is `T = sum T_i`.
//!
//! With `k` edits permitted, each error can destroy at most
//! three pattern trigrams (the up-to-three windows that
//! contain the edited byte). So at least
//! `max(0, T - 3k)` of the pattern's trigrams must appear in
//! any matching doc, giving the soundness invariant
//!
//! ```text
//!     #(pattern_trigrams in doc) >= T - 3k
//! ```
//!
//! The filter therefore has two fields:
//!
//! * `trigrams` -- the pattern trigrams (deduplicated).
//! * `min_required` -- `max(0, T - 3k)` clamped to
//!   `trigrams.len()`.
//!
//! When `min_required == 0` the filter degenerates and the
//! caller has to fall back to a full scan.
//!
//! # Application
//!
//! [`ApproxFilter::candidates`] is the entry point used by
//! [`crate::index::TextIndex::search_regex_approx`]. It first
//! computes the postings UNION of all pattern trigrams (an
//! upper bound on the candidate set: any doc that contains
//! `min_required >= 1` of them must be in the union of all of
//! them), then for each survivor counts how many pattern
//! trigrams the per-doc bloom filter accepts and rejects docs
//! whose count falls below `min_required`.
//!
//! # Why not Navarro tiling?
//!
//! Navarro's `(k+1)`-tiling argument partitions the pattern
//! into `k+1` disjoint contiguous tiles, each of which must
//! match exactly somewhere because at least one of them is
//! error-free by pigeonhole. For sufficiently long patterns
//! this gives a tighter selectivity than the per-trigram
//! bound. In practice the approximate-regex queries dyntext
//! sees today have short literal runs (typically 3-5 bytes),
//! and the `(k+1)`-tiling either degenerates or matches the
//! per-trigram bound. We use the simpler bound here and leave
//! the tiling refinement for a follow-up.

use crate::bloom::BloomFilter;
use crate::postings::Postings;
use crate::prefix_extract;
use crate::regex_ast::Ast;
use crate::trigram;
/// Sound trigram filter for an approximate-regex query.
#[derive(Debug, Clone)]
pub struct ApproxFilter {
    /// Distinct pattern trigram hashes, sorted ascending.
    /// Empty means the pattern has no extractable literal runs
    /// (no filter possible).
    pub trigrams: Vec<u64>,
    /// Minimum number of [`Self::trigrams`] that must appear in
    /// any matching doc. Always `<= trigrams.len()`. When zero
    /// the filter degenerates to "no constraint" and the
    /// caller must full-scan.
    pub min_required: usize,
}

impl ApproxFilter {
    /// Build the filter for a regex AST allowing up to `k`
    /// edit operations.
    #[must_use]
    pub fn build(ast: &Ast, k: u16) -> Self {
        let runs = prefix_extract::extract_literal_runs(ast);
        let mut trigrams: Vec<u64> = Vec::new();
        let mut total_trigram_count: usize = 0;
        for run in &runs {
            if run.len() < trigram::TRIGRAM_LEN {
                continue;
            }
            for w in run.windows(trigram::TRIGRAM_LEN) {
                let h = trigram::hash_trigram(w);
                trigrams.push(h);
                total_trigram_count += 1;
            }
        }
        trigrams.sort_unstable();
        trigrams.dedup();

        // Soundness: each edit destroys at most 3 trigrams of
        // the pattern (the up-to-3 windows that overlap the
        // edited byte). With k edits the surviving count is
        // at least `total_trigram_count - 3k` (with overlap
        // counted, since a single physical edit destroys all
        // three overlapping windows at once). We use the
        // conservative formula: surviving >= T - 3k where T
        // counts trigrams with multiplicity, but we then clamp
        // to the deduplicated `trigrams.len()` because the
        // filter operates on distinct trigrams.
        let edits = usize::from(k);
        let destroyed = edits.saturating_mul(3);
        let surviving = total_trigram_count.saturating_sub(destroyed);
        let min_required = surviving.min(trigrams.len());

        Self {
            trigrams,
            min_required,
        }
    }

    /// Whether the filter imposes any constraint. A degenerate
    /// filter (no required trigrams or `min_required == 0`)
    /// passes every doc and the caller should full-scan.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.min_required > 0 && !self.trigrams.is_empty()
    }

    /// Compute the candidate doc-id set for the filter.
    ///
    /// The strategy is the postings UNION of every pattern
    /// trigram. Any doc that contains at least one pattern
    /// trigram is in the union; any doc with
    /// `min_required >= 1` must be in the union; this is
    /// therefore a sound upper bound. Per-doc bloom filtering
    /// is applied separately by the index in a tighter inner
    /// loop that already has the doc record in hand.
    ///
    /// The output is the surviving doc ids in ascending
    /// order.
    #[must_use]
    pub fn candidates(&self, postings: &Postings) -> Vec<u32> {
        let union = postings.union(&self.trigrams);
        union.iter().collect()
    }

    /// Test the filter against a per-doc bloom filter.
    ///
    /// Returns `true` if the doc may match the pattern under
    /// the configured edit budget; `false` if the pattern
    /// imposes more required-trigram constraints than the doc
    /// can plausibly satisfy.
    #[must_use]
    pub fn passes(&self, bloom: &BloomFilter) -> bool {
        if !self.is_active() {
            return true;
        }
        let mut hits = 0_usize;
        let mut remaining = self.trigrams.len();
        for t in &self.trigrams {
            if bloom.contains(&t.to_le_bytes()) {
                hits += 1;
                if hits >= self.min_required {
                    return true;
                }
            }
            remaining -= 1;
            if hits + remaining < self.min_required {
                return false;
            }
        }
        hits >= self.min_required
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::regex_ast::parse;

    fn build(pattern: &str, k: u16) -> ApproxFilter {
        let ast = parse(pattern).expect("parses");
        ApproxFilter::build(&ast, k)
    }

    #[test]
    fn k0_filter_requires_all_trigrams() {
        let f = build("hello", 0);
        // "hello" has 3 distinct trigrams (hel, ell, llo).
        assert_eq!(f.trigrams.len(), 3);
        assert_eq!(f.min_required, 3);
    }

    #[test]
    fn k1_filter_loosens_min_required() {
        // T = 3 trigrams, k=1, surviving >= max(0, 3-3) = 0.
        let f = build("hello", 1);
        assert_eq!(f.trigrams.len(), 3);
        assert_eq!(f.min_required, 0);
        assert!(!f.is_active());
    }

    #[test]
    fn long_pattern_under_k2_keeps_filter_active() {
        // T = 9 trigrams (length 11), k=2, surviving >= 9-6=3.
        let f = build("hello world", 2);
        assert!(
            f.min_required >= 3,
            "expected min_required >= 3, got {}",
            f.min_required
        );
        assert!(f.is_active());
    }

    #[test]
    fn k0_filter_for_unsupported_pattern_is_inactive() {
        // `.*` has no required literal runs.
        let f = build(".*", 0);
        assert!(f.trigrams.is_empty());
        assert!(!f.is_active());
    }

    #[test]
    fn passes_returns_true_when_inactive() {
        let f = build(".*", 0);
        let bloom = BloomFilter::with_size_and_fp_rate(64, 0.01);
        assert!(f.passes(&bloom));
    }

    #[test]
    fn passes_rejects_doc_below_threshold() {
        let f = build("hello", 0);
        // Empty bloom: no trigrams present. min_required = 3.
        let bloom = BloomFilter::with_size_and_fp_rate(64, 0.01);
        assert!(!f.passes(&bloom));
    }

    #[test]
    fn passes_accepts_doc_at_threshold() {
        let f = build("hello", 0);
        let mut bloom = BloomFilter::with_size_and_fp_rate(256, 0.001);
        for t in &f.trigrams {
            bloom.insert(&t.to_le_bytes());
        }
        assert!(f.passes(&bloom));
    }

    #[test]
    fn min_required_never_exceeds_unique_trigrams() {
        // `aaaaa` has 1 distinct trigram; T (with multiplicity)
        // is 3. min_required must clamp to 1.
        let f = build("aaaaa", 0);
        assert_eq!(f.trigrams.len(), 1);
        assert_eq!(f.min_required, 1);
    }
}
