//! Top-level [`TextIndex`] type combining trigram extraction,
//! the inverted postings index, and the per-document bloom
//! filter into a single insert-and-search facility.
//!
//! # Search algorithm
//!
//! The substring query path is the four-tier filter funnel from
//! the design doc:
//!
//! 1. Extract the query's trigrams.
//! 2. Intersect their postings lists into a candidate doc-id
//!    set (tier 2).
//! 3. For each candidate doc, check the per-document bloom
//!    filter for the query's trigrams. This is a cheap
//!    membership test that usually agrees with the postings
//!    intersection but acts as defence in depth (tier 3).
//! 4. For each survivor, run a real substring match against
//!    the doc's stored bytes (tier 4 / final recheck).
//!
//! Results are returned in insertion order so callers can rely
//! on a deterministic ranking.
//!
//! # Short queries
//!
//! Queries shorter than [`MIN_TRIGRAM_QUERY_LEN`] cannot be
//! resolved through the trigram index and fall back to a full
//! scan of the stored corpus. Callers that want to avoid the
//! full-scan cost should reject short queries themselves.

use std::collections::BTreeMap;

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use crate::bloom::BloomFilter;
use crate::postings::Postings;
use crate::prefix_extract;
use crate::regex_ast::{self, RegexError};
use crate::tiling::ApproxFilter;
use crate::tre::{TreCompiledPattern, TreError, TreMatchOpts};
use crate::trigram;

/// Minimum query length in bytes that the trigram index can
/// directly serve. Shorter queries fall back to a full scan
/// over the stored corpus.
pub const MIN_TRIGRAM_QUERY_LEN: usize = 3;

/// Default expected trigram count per document for sizing the
/// per-document bloom filter. Most short text fields produce
/// far fewer; a tighter sizing keeps the per-doc memory
/// footprint reasonable.
const DEFAULT_BLOOM_N: usize = 256;

/// Default false positive rate for the per-document bloom.
const DEFAULT_BLOOM_FP: f64 = 0.01;

/// Threshold above which `search_regex_approx` parallelises
/// the per-doc TRE recheck. Below this size the rayon
/// scheduling overhead is comparable to the savings, so the
/// sequential path is faster. Tuned empirically against the
/// 256-byte alnum bench corpus.
const PARALLEL_RECHECK_THRESHOLD: usize = 1024;

/// Chunk size used by the parallel-recheck path. Enough work
/// per task to amortise rayon's task-spawn overhead, small
/// enough to balance load across cores. Empirically 256 docs
/// per chunk hits the sweet spot for the 100k-doc corpus on
/// an 8-core box.
const PARALLEL_RECHECK_CHUNK_SIZE: usize = 256;

/// One indexed document: its raw text (for tier-4 substring
/// recheck) and its trigram bloom filter (for tier-3 cheap
/// recheck).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedDoc {
    /// Original byte sequence as inserted.
    pub text: Vec<u8>,
    /// Bloom filter over the doc's trigram set.
    pub bloom: BloomFilter,
}

impl IndexedDoc {
    /// Construct an indexed doc by computing its trigram set
    /// and bloom filter from `text`.
    fn new(text: Vec<u8>) -> Self {
        let tris = trigram::extract_trigram_set(&text);
        let mut bloom =
            BloomFilter::with_size_and_fp_rate(DEFAULT_BLOOM_N.max(tris.len()), DEFAULT_BLOOM_FP);
        for t in &tris {
            bloom.insert(&t.to_le_bytes());
        }
        Self { text, bloom }
    }
}

/// Trigram-based text index supporting incremental insert,
/// remove, and exact-substring search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextIndex {
    /// Inverted index of trigram hash to doc-id bitmap.
    postings: Postings,
    /// Per-doc state. The map is keyed by doc id; iteration
    /// order is therefore insertion order because doc ids are
    /// monotonically assigned.
    docs: BTreeMap<u32, IndexedDoc>,
    /// Next doc id to hand out. Starts at 0; never recycled
    /// (a removed doc id is gone forever).
    next_doc_id: u32,
}

impl Default for TextIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl TextIndex {
    /// Construct an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            postings: Postings::new(),
            docs: BTreeMap::new(),
            next_doc_id: 0,
        }
    }

    /// Number of documents currently in the index.
    #[must_use]
    pub fn doc_count(&self) -> usize {
        self.docs.len()
    }

    /// Borrow the inverted postings index.
    #[must_use]
    pub fn postings(&self) -> &Postings {
        &self.postings
    }

    /// Borrow the document store.
    #[must_use]
    pub fn docs(&self) -> &BTreeMap<u32, IndexedDoc> {
        &self.docs
    }

    /// Insert `text` and return the assigned doc id.
    ///
    /// The doc id is assigned monotonically; doc ids are not
    /// recycled, so a removed id is not handed out again.
    pub fn insert(&mut self, text: Vec<u8>) -> u32 {
        let doc_id = self.next_doc_id;
        self.next_doc_id = self
            .next_doc_id
            .checked_add(1)
            .expect("invariant: doc ids fit in u32; saturate at 2^32-1 by removing old docs");

        let tris = trigram::extract_trigram_set(&text);
        for t in &tris {
            self.postings.insert(*t, doc_id);
        }
        let doc = IndexedDoc::new(text);
        self.docs.insert(doc_id, doc);
        doc_id
    }

    /// Remove the document at `doc_id` and return its raw
    /// bytes, if any.
    ///
    /// All trigram entries for the doc are pulled from the
    /// postings index. A trigram whose postings list becomes
    /// empty after removal is garbage collected.
    pub fn remove(&mut self, doc_id: u32) -> Option<Vec<u8>> {
        let doc = self.docs.remove(&doc_id)?;
        let tris = trigram::extract_trigram_set(&doc.text);
        for t in &tris {
            self.postings.remove(*t, doc_id);
        }
        Some(doc.text)
    }

    /// Search for documents whose text contains `query` as a
    /// contiguous byte substring.
    ///
    /// Results are returned in insertion order. A document that
    /// has been removed is not returned even if its postings
    /// entries were missed by a buggy remove (we always
    /// re-verify against the doc store).
    ///
    /// Queries shorter than [`MIN_TRIGRAM_QUERY_LEN`] cannot be
    /// resolved through the trigram index and fall back to a
    /// full scan.
    #[must_use]
    pub fn search_substring(&self, query: &[u8]) -> Vec<u32> {
        if query.is_empty() {
            // An empty substring matches every doc by
            // definition; preserve insertion order.
            return self.docs.keys().copied().collect();
        }

        if query.len() < MIN_TRIGRAM_QUERY_LEN {
            return self.full_scan(query);
        }

        let qtris = trigram::extract_query_trigram_set(query);
        if qtris.is_empty() {
            return self.full_scan(query);
        }

        // Tier 2: postings intersection.
        let candidates = self.postings.intersect(&qtris);
        if candidates.is_empty() {
            return Vec::new();
        }

        let mut hits: Vec<u32> = Vec::new();
        for doc_id in &candidates {
            let Some(doc) = self.docs.get(&doc_id) else {
                continue;
            };
            // Tier 3: per-doc bloom filter.
            if !qtris.iter().all(|t| doc.bloom.contains(&t.to_le_bytes())) {
                continue;
            }
            // Tier 4: real substring recheck.
            if Self::contains_substring(&doc.text, query) {
                hits.push(doc_id);
            }
        }
        // The Roaring bitmap iterates ascending, which is also
        // insertion order because doc ids are monotonic. Sort
        // anyway to make the contract explicit.
        hits.sort_unstable();
        hits
    }

    /// Full scan over the document store; used for queries too
    /// short for the trigram index.
    fn full_scan(&self, query: &[u8]) -> Vec<u32> {
        let mut out = Vec::new();
        for (id, doc) in &self.docs {
            if Self::contains_substring(&doc.text, query) {
                out.push(*id);
            }
        }
        out
    }

    /// Search for documents whose text matches `pattern` as a
    /// regular expression.
    ///
    /// The query path is the same four-tier filter funnel as
    /// [`Self::search_substring`], plus a Phase-2 prefix
    /// extraction step:
    ///
    /// 1. Parse `pattern` into the internal AST and extract
    ///    the trigrams that any matching string MUST contain
    ///    (see [`crate::prefix_extract`]).
    /// 2. Intersect those trigrams' postings lists into a
    ///    candidate doc-id set. If the AST cannot be lowered
    ///    (named capture group, etc.) or yields no required
    ///    trigrams, fall back to scanning every doc.
    /// 3. Per-doc bloom filter recheck (skipped on full scan).
    /// 4. If the AST starts with `^literal`, prune candidates
    ///    whose first bytes do not equal the literal prefix.
    /// 5. Compile the pattern with [`regex::bytes::Regex`] and
    ///    re-run it against each candidate's stored bytes.
    ///
    /// Results are returned in insertion order.
    ///
    /// # Errors
    ///
    /// Returns [`RegexError::Parse`] if the pattern is
    /// syntactically invalid or uses a regex feature that the
    /// underlying `regex` crate does not support (lookarounds,
    /// backreferences, ...). A pattern that parses cleanly but
    /// trips the prefix extractor's unsupported-feature path
    /// (named capture groups) does NOT surface as an error:
    /// the search still runs, just via the slower full-scan +
    /// recheck path.
    pub fn search_regex(&self, pattern: &str) -> Result<Vec<u32>, RegexError> {
        // Compile the matcher first; a pattern the matcher
        // cannot handle is a hard error to the caller. We use
        // `regex::bytes::Regex` because the corpus is byte
        // slices, not UTF-8.
        let re = regex::bytes::Regex::new(pattern).map_err(|e| RegexError::Parse(e.to_string()))?;

        // Required trigrams from the AST. If extraction fails
        // (PrefixUnsupported) or yields no constraint, we have
        // to scan every doc; the recheck still runs correctly.
        let ast = regex_ast::parse(pattern).ok();
        let trigram_hashes: Vec<u64> = ast
            .as_ref()
            .map(prefix_extract::required_trigram_hashes)
            .unwrap_or_default();
        let anchored_prefix = ast.as_ref().and_then(prefix_extract::anchored_prefix);

        let candidates: Vec<u32> = if trigram_hashes.is_empty() {
            self.docs.keys().copied().collect()
        } else {
            self.postings.intersect(&trigram_hashes).iter().collect()
        };

        let mut hits: Vec<u32> = Vec::new();
        for doc_id in candidates {
            let Some(doc) = self.docs.get(&doc_id) else {
                continue;
            };
            // Tier 3: per-doc bloom filter -- only meaningful
            // when we have required trigrams. On a full scan
            // it would be a tautology because we have no
            // membership query to make.
            if !trigram_hashes.is_empty()
                && !trigram_hashes
                    .iter()
                    .all(|t| doc.bloom.contains(&t.to_le_bytes()))
            {
                continue;
            }
            // Anchor fast-path: if the pattern is anchored and
            // followed by a literal prefix, reject docs whose
            // first bytes do not equal the prefix. This is
            // cheap (a single byte-compare), sound for K=0,
            // and lets the much heavier `regex` matcher off
            // the hook for the common `^literal\w+...` shape.
            if let Some(prefix) = anchored_prefix.as_ref() {
                if doc.text.len() < prefix.len() || &doc.text[..prefix.len()] != prefix.as_slice() {
                    continue;
                }
            }
            // Tier 4: real regex recheck.
            if re.is_match(&doc.text) {
                hits.push(doc_id);
            }
        }
        hits.sort_unstable();
        Ok(hits)
    }

    /// Byte-level substring match.
    fn contains_substring(haystack: &[u8], needle: &[u8]) -> bool {
        if needle.is_empty() {
            return true;
        }
        if needle.len() > haystack.len() {
            return false;
        }
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// Search for documents that match `pattern` as an
    /// approximate POSIX extended regular expression with up
    /// to `max_errors` edit operations.
    ///
    /// The path mirrors [`Self::search_regex`] but the tier-4
    /// recheck delegates to [`TreCompiledPattern`] instead of
    /// the std `regex` matcher because TRE is the only engine
    /// in the workspace that implements approximate match
    /// semantics. The pre-recheck filter funnel uses the
    /// pigeonhole bound `surviving_trigrams >= T - 3k`
    /// (see [`ApproxFilter`]) to stay sound under the edit
    /// budget.
    ///
    /// For a `^literal...` pattern the anchor fast-path
    /// rejects every candidate whose first bytes are too far
    /// (in Hamming distance) from the literal prefix. For
    /// `max_errors == 0` this is a byte-equality check; for
    /// `max_errors >= 1` it is a Hamming-distance check
    /// against the prefix length, which is sound for the
    /// substitution-only case TRE optimises and conservative
    /// (always returns `true` for prefixes whose Hamming
    /// distance is within `max_errors`) for the
    /// insert/delete cases.
    ///
    /// When the surviving candidate set after filtering is
    /// large (`>= PARALLEL_RECHECK_THRESHOLD`), the per-doc
    /// TRE recheck is dispatched to a Rayon parallel iterator
    /// to fan the cost across CPU cores. TRE's compiled
    /// `regex_t` is `!Send`, so each parallel worker compiles
    /// its own copy from the original pattern bytes.
    ///
    /// Results are returned in ascending document-id order.
    ///
    /// # Errors
    ///
    /// Returns [`TreError::Compile`] if the pattern fails to
    /// compile under the given options.
    pub fn search_regex_approx(
        &self,
        pattern: &str,
        max_errors: u16,
    ) -> Result<Vec<u32>, TreError> {
        let opts = TreMatchOpts {
            max_errors,
            ..TreMatchOpts::default()
        };
        let pat = TreCompiledPattern::compile(pattern.as_bytes(), opts)?;

        // Build the trigram filter and the anchor fast-path
        // from the AST. A pattern that does not lower into the
        // internal AST (named captures, regex-syntax features
        // we don't model) falls back to the no-filter scan.
        let ast = regex_ast::parse(pattern).ok();
        let filter = ast.as_ref().map_or_else(
            || ApproxFilter {
                trigrams: Vec::new(),
                min_required: 0,
            },
            |a| ApproxFilter::build(a, max_errors),
        );
        let anchored_prefix = ast.as_ref().and_then(prefix_extract::anchored_prefix);

        // Tier 3 + anchor fast-path: cheap rejections before
        // we hand the doc to TRE.
        let prefilter = |doc: &IndexedDoc| -> bool {
            if filter.is_active() && !filter.passes(&doc.bloom) {
                return false;
            }
            if let Some(prefix) = anchored_prefix.as_ref() {
                if !anchor_prefix_compatible(&doc.text, prefix, max_errors) {
                    return false;
                }
            }
            true
        };

        // First sweep: build the (doc_id, &text) survivor list.
        // When the filter is active we walk the candidate vec
        // returned by the postings union; when it is inactive
        // we iterate `self.docs` directly to avoid a chain of
        // 100k BTreeMap lookups that would dominate the cost
        // of the per-query setup.
        let survivors: Vec<(u32, &[u8])> = if filter.is_active() {
            let candidates = filter.candidates(&self.postings);
            candidates
                .into_iter()
                .filter_map(|doc_id| {
                    let doc = self.docs.get(&doc_id)?;
                    if !prefilter(doc) {
                        return None;
                    }
                    Some((doc_id, doc.text.as_slice()))
                })
                .collect()
        } else {
            self.docs
                .iter()
                .filter_map(|(id, doc)| {
                    if !prefilter(doc) {
                        return None;
                    }
                    Some((*id, doc.text.as_slice()))
                })
                .collect()
        };

        let hits: Vec<u32> = if survivors.len() >= PARALLEL_RECHECK_THRESHOLD {
            run_parallel_recheck(pattern, opts, &survivors)?
        } else {
            survivors
                .into_iter()
                .filter_map(|(id, text)| if pat.is_match(text) { Some(id) } else { None })
                .collect()
        };

        let mut out = hits;
        out.sort_unstable();
        Ok(out)
    }
}

/// Whether `doc` could plausibly be the start of an
/// approximate match for the anchored literal `prefix` under
/// an edit budget of `max_errors`.
///
/// The check is sound (never returns `false` when the doc
/// could match) and cheap (compares at most
/// `prefix.len() + max_errors` bytes per shift).
///
/// Strategy: iterate every alignment offset in
/// `[0, max_errors]` and compute the edit distance between
/// `prefix` and the corresponding doc prefix using a banded
/// dynamic-programming table of width `2*max_errors + 1`. If
/// any alignment is within `max_errors` edits, accept.
///
/// For `max_errors == 0` this collapses to a byte-equality
/// check on the prefix length. For small `max_errors` (the
/// common case in dyntext queries) the check costs
/// O(prefix.len() * (2 * max_errors + 1)) byte comparisons.
fn anchor_prefix_compatible(doc: &[u8], prefix: &[u8], max_errors: u16) -> bool {
    if max_errors == 0 {
        return doc.len() >= prefix.len() && &doc[..prefix.len()] == prefix;
    }
    let k = usize::from(max_errors);
    // Restrict the doc window to the first prefix.len() + k
    // bytes; an anchored match cannot reach further. If the
    // doc is shorter than prefix.len() - k we still try -- the
    // edit distance computation handles the length mismatch.
    let window_end = (prefix.len() + k).min(doc.len());
    let window = &doc[..window_end];
    let dist = bounded_edit_distance(prefix, window, k);
    dist <= k
}

/// Compute the prefix-edit distance between `pat` and `txt`,
/// returning the smallest number of edits that turns `pat` into
/// some prefix of `txt`. The result is capped at `k + 1` so
/// the caller can reject early when the budget is exceeded.
///
/// This is the standard banded edit-distance recurrence,
/// O(|pat| * (2 * k + 1)). It is symmetric in the
/// substitution / insertion / deletion costs (all 1).
fn bounded_edit_distance(pat: &[u8], txt: &[u8], k: usize) -> usize {
    let m = pat.len();
    let n = txt.len();
    if m == 0 {
        return 0;
    }
    // dp[i][j] = edits to turn pat[..i] into some prefix of
    // txt[..j]. Only cells with |i - j| <= k matter; we keep
    // two rows to avoid the |pat| x |txt| matrix.
    let mut prev = vec![usize::MAX; n + 1];
    let mut curr = vec![usize::MAX; n + 1];
    prev[0] = 0;
    for cell in prev.iter_mut().take(n.min(k) + 1).skip(1) {
        *cell = 0; // free leading text characters (prefix-match)
    }
    for i in 1..=m {
        curr[0] = i;
        let lo = i.saturating_sub(k);
        let hi = (i + k).min(n);
        for j in 1..=n {
            if j < lo || j > hi {
                curr[j] = usize::MAX;
                continue;
            }
            let cost = usize::from(pat[i - 1] != txt[j - 1]);
            let sub = prev[j - 1].saturating_add(cost);
            let del = prev[j].saturating_add(1);
            let ins = curr[j - 1].saturating_add(1);
            curr[j] = sub.min(del).min(ins);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    // Best alignment: the smallest dp[m][j] over j in
    // [m - k, m + k] intersected with [0, n].
    let lo = m.saturating_sub(k);
    let hi = (m + k).min(n);
    let mut best = usize::MAX;
    for cell in prev.iter().take(hi + 1).skip(lo) {
        best = best.min(*cell);
    }
    best.min(k + 1)
}

/// Run the TRE recheck across `survivors` in parallel.
///
/// `TreCompiledPattern` is `!Send`, so the parallel iterator
/// compiles a fresh pattern in each worker via
/// [`rayon::iter::ParallelIterator::map_init`]. The compile
/// cost is amortised across the worker's chunk of survivors,
/// which is much smaller than the per-doc TRE match cost.
fn run_parallel_recheck(
    pattern: &str,
    opts: TreMatchOpts,
    survivors: &[(u32, &[u8])],
) -> Result<Vec<u32>, TreError> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let pattern_bytes = pattern.as_bytes();
    let compile_failed = AtomicBool::new(false);
    let hits: Vec<u32> = survivors
        .par_chunks(PARALLEL_RECHECK_CHUNK_SIZE)
        .map_init(
            || TreCompiledPattern::compile(pattern_bytes, opts).ok(),
            |worker_pat: &mut Option<TreCompiledPattern>, chunk: &[(u32, &[u8])]| -> Vec<u32> {
                let Some(pat) = worker_pat.as_ref() else {
                    compile_failed.store(true, Ordering::Relaxed);
                    return Vec::new();
                };
                let mut local = Vec::new();
                for &(id, text) in chunk {
                    if pat.is_match(text) {
                        local.push(id);
                    }
                }
                local
            },
        )
        .flatten()
        .collect();
    if compile_failed.load(Ordering::Relaxed) {
        return Err(TreError::Internal(
            "parallel worker failed to compile pattern".into(),
        ));
    }
    Ok(hits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_then_search_finds_the_doc() {
        let mut idx = TextIndex::new();
        let id = idx.insert(b"hello world".to_vec());
        let hits = idx.search_substring(b"hello");
        assert_eq!(hits, vec![id]);
    }

    #[test]
    fn search_substring_returns_only_true_positives() {
        let mut idx = TextIndex::new();
        let a = idx.insert(b"the quick brown fox".to_vec());
        let _b = idx.insert(b"jumped over a lazy dog".to_vec());
        let c = idx.insert(b"a brown fox is quick".to_vec());
        let hits = idx.search_substring(b"brown fox");
        // Only docs containing the literal byte sequence
        // "brown fox" qualify.
        assert!(hits.contains(&a));
        assert!(hits.contains(&c));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn search_substring_no_false_negatives_on_corpus() {
        let mut store = TextIndex::new();
        let corpus: &[&[u8]] = &[
            b"alpha beta gamma",
            b"beta cake",
            b"the alphabet starts with alpha",
            b"omega only",
        ];
        let ids: Vec<u32> = corpus.iter().map(|t| store.insert(t.to_vec())).collect();
        for q in [b"alpha".as_slice(), b"beta", b"omega", b"the"] {
            let hits = store.search_substring(q);
            for (i, doc) in corpus.iter().enumerate() {
                if doc.windows(q.len()).any(|w| w == q) {
                    assert!(
                        hits.contains(&ids[i]),
                        "false negative: query {q:?} should hit doc {i} {doc:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn search_returns_results_in_insertion_order() {
        let mut idx = TextIndex::new();
        let id_a = idx.insert(b"hello a".to_vec());
        let id_b = idx.insert(b"hello b".to_vec());
        let id_c = idx.insert(b"hello c".to_vec());
        let hits = idx.search_substring(b"hello");
        assert_eq!(hits, vec![id_a, id_b, id_c]);
    }

    #[test]
    fn remove_excludes_doc_from_subsequent_searches() {
        let mut idx = TextIndex::new();
        let a = idx.insert(b"the quick brown fox".to_vec());
        let b = idx.insert(b"another brown fox here".to_vec());
        let removed = idx.remove(a).expect("doc a present");
        assert_eq!(removed, b"the quick brown fox");
        let hits = idx.search_substring(b"brown fox");
        assert_eq!(hits, vec![b]);
    }

    #[test]
    fn remove_garbage_collects_unique_trigrams() {
        let mut idx = TextIndex::new();
        let a = idx.insert(b"unique-string-only-here".to_vec());
        let postings_before = idx.postings().len();
        assert!(postings_before > 0);
        idx.remove(a);
        // After removing the only doc, every postings entry
        // should be empty and gone.
        assert_eq!(idx.postings().len(), 0);
        assert_eq!(idx.doc_count(), 0);
    }

    #[test]
    fn remove_missing_doc_id_returns_none() {
        let mut idx = TextIndex::new();
        idx.insert(b"abc".to_vec());
        assert!(idx.remove(9999).is_none());
    }

    #[test]
    fn query_shorter_than_three_chars_uses_full_scan() {
        let mut idx = TextIndex::new();
        let a = idx.insert(b"abcdef".to_vec());
        let _b = idx.insert(b"xyz".to_vec());
        let c = idx.insert(b"ab".to_vec());
        let hits = idx.search_substring(b"ab");
        assert!(hits.contains(&a));
        assert!(hits.contains(&c));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn empty_query_matches_every_doc() {
        let mut idx = TextIndex::new();
        let a = idx.insert(b"x".to_vec());
        let b = idx.insert(b"y".to_vec());
        let hits = idx.search_substring(b"");
        assert_eq!(hits, vec![a, b]);
    }

    #[test]
    fn unicode_query_byte_level_works() {
        let mut idx = TextIndex::new();
        // "cafe" with combining e-acute (UTF-8 bytes
        // 0xC3 0xA9). Insert one doc with the e-acute form,
        // one with plain ASCII, and search for the e-acute
        // suffix.
        let a = idx.insert(b"caf\xc3\xa9 noir".to_vec());
        let b = idx.insert(b"cafe noir".to_vec());
        let hits = idx.search_substring(b"\xc3\xa9");
        assert_eq!(hits, vec![a]);
        let hits = idx.search_substring(b"noir");
        // Both contain the literal bytes "noir".
        assert!(hits.contains(&a));
        assert!(hits.contains(&b));
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn search_for_nonexistent_substring_returns_empty() {
        let mut idx = TextIndex::new();
        idx.insert(b"hello world".to_vec());
        idx.insert(b"another doc".to_vec());
        assert!(idx.search_substring(b"completely-absent").is_empty());
    }

    #[test]
    fn search_on_empty_index_returns_empty() {
        let idx = TextIndex::new();
        assert!(idx.search_substring(b"anything").is_empty());
        // The empty-query special case still returns an empty
        // vec when there are no docs to enumerate.
        assert!(idx.search_substring(b"").is_empty());
    }
}
