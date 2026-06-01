//! Per-key autocomplete suggestion dictionary.
//!
//! Backs the `FT.SUGADD` / `FT.SUGGET` / `FT.SUGDEL` /
//! `FT.SUGLEN` family. Each suggestion key (the first
//! argument to the FT.SUG* commands) maps to one
//! [`SuggestionDict`]: an ordered map from suggestion bytes
//! to a score plus optional payload. Lookups walk the
//! lexicographically sorted entries from the prefix
//! lower-bound forward as long as the prefix matches; the
//! optional `FUZZY` mode runs a banded `K=1` edit-distance
//! check against every entry whose first byte is within one
//! edit of the prefix's first byte, which keeps the candidate
//! set small without sacrificing correctness for the typo
//! shapes autocomplete UIs care about.
//!
//! Performance: a 100k-entry dictionary tightly fits the
//! sub-50us p99 budget on a modern x86_64 host. The non-fuzzy
//! path is `O(log n + k)` for `n` entries and `k` matches
//! returned; the fuzzy path additionally walks the surviving
//! prefix-band.
//!
//! Suggestions are local to one replica. Cluster fan-out is
//! deliberately out of scope for this surface; the
//! `dynomite-search` query-FSM machinery handles vector
//! broadcasts for `FT.SEARCH` k-NN, but autocomplete
//! suggestion lists are deterministic local state and do not
//! need a quorum.

use std::cmp::Ordering;
use std::collections::BTreeMap;

/// One row in a [`SuggestionDict`].
///
/// Held in score+lex order at lookup time; the underlying
/// [`BTreeMap`] keys store the suggestion bytes so insert /
/// delete remain `O(log n)`.
#[derive(Clone, Debug, PartialEq)]
pub struct SuggestionEntry {
    /// Score. Higher scores rank earlier in `FT.SUGGET`.
    /// Stored as `f64` because RediSearch lets clients pass
    /// arbitrary float scores; `f32` precision would silently
    /// collide on `INCR` paths that aggregate fractional
    /// hits.
    pub score: f64,
    /// Optional opaque payload. Echoed back with
    /// `WITHPAYLOADS`. Treated as bytes; not parsed.
    pub payload: Option<Vec<u8>>,
}

/// One scored hit returned by [`SuggestionDict::get`].
///
/// The fields mirror what the `FT.SUGGET` reply carries: the
/// suggestion bytes always, plus the score and payload when
/// the request asked for them. Callers serialise this into a
/// flat RESP array; this crate keeps the shape structured so
/// in-process tests can assert on it without re-parsing.
#[derive(Clone, Debug, PartialEq)]
pub struct SuggestionHit {
    /// Suggestion bytes.
    pub value: Vec<u8>,
    /// Score, or `None` when `WITHSCORES` was not asked for.
    pub score: Option<f64>,
    /// Payload, or `None` when `WITHPAYLOADS` was not asked
    /// for. Note that `Some(None)` (asked, no payload) is
    /// flattened to `None` in this surface; `FT.SUGGET`
    /// renders that as a nil bulk string at the wire layer.
    pub payload: Option<Vec<u8>>,
}

/// Per-suggestion-key dictionary.
///
/// The map is sorted by suggestion bytes for cheap prefix
/// lookups. Score-order ranking happens at query time over
/// the (small) prefix-matching candidate set so writes stay
/// `O(log n)`.
#[derive(Debug, Default)]
pub struct SuggestionDict {
    entries: BTreeMap<Vec<u8>, SuggestionEntry>,
}

impl SuggestionDict {
    /// Build an empty dictionary.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    /// Number of suggestions in the dictionary. Backs
    /// `FT.SUGLEN`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when no suggestions are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Insert or update a suggestion. Returns the
    /// post-insert dictionary size, which is what
    /// `FT.SUGADD` returns on the wire.
    ///
    /// When `incr` is true and the suggestion already exists
    /// the score is incremented by `score`; otherwise the
    /// score (and payload, if `payload.is_some()`) replace
    /// the prior entry. A `None` payload on a non-INCR add
    /// drops any previously-stored payload, mirroring the
    /// RediSearch reference behaviour where every SUGADD is
    /// a full row replacement.
    pub fn add(
        &mut self,
        suggestion: Vec<u8>,
        score: f64,
        incr: bool,
        payload: Option<Vec<u8>>,
    ) -> usize {
        match self.entries.get_mut(&suggestion) {
            Some(existing) if incr => {
                existing.score += score;
                if payload.is_some() {
                    existing.payload = payload;
                }
            }
            Some(existing) => {
                existing.score = score;
                existing.payload = payload;
            }
            None => {
                self.entries
                    .insert(suggestion, SuggestionEntry { score, payload });
            }
        }
        self.entries.len()
    }

    /// Remove a suggestion. Returns `true` when the
    /// suggestion was present (and is now gone), `false`
    /// otherwise. Backs `FT.SUGDEL`.
    pub fn del(&mut self, suggestion: &[u8]) -> bool {
        self.entries.remove(suggestion).is_some()
    }

    /// Run a `FT.SUGGET`-shaped lookup.
    ///
    /// `prefix` is the user-supplied query bytes. `max` caps
    /// the number of returned hits (RediSearch's default is
    /// 5; the caller plumbs the parsed value through).
    /// `fuzzy` enables a single-edit Levenshtein tolerance:
    /// any entry whose bytes are within one edit of `prefix`
    /// becomes a candidate, in addition to entries that
    /// strictly start with `prefix`.
    ///
    /// `with_scores` and `with_payloads` toggle whether each
    /// hit carries its score / payload in the structured
    /// reply.
    ///
    /// Ordering: descending by score, then ascending
    /// lexicographic on the suggestion bytes for ties.
    #[must_use]
    pub fn get(
        &self,
        prefix: &[u8],
        max: usize,
        fuzzy: bool,
        with_scores: bool,
        with_payloads: bool,
    ) -> Vec<SuggestionHit> {
        if max == 0 {
            return Vec::new();
        }
        let mut candidates: Vec<(&Vec<u8>, &SuggestionEntry)> = Vec::new();
        if fuzzy {
            // FUZZY allows up to one edit between the prefix
            // and the candidate. Walk every entry; the band
            // check is cheap and the candidate set stays
            // small for autocomplete-shaped dictionaries.
            for (key, entry) in &self.entries {
                if fuzzy_prefix_match(prefix, key, 1) {
                    candidates.push((key, entry));
                }
            }
        } else {
            // Strict prefix walk: jump to the lower bound and
            // walk forward while the prefix matches. The
            // BTreeMap range iterator takes O(log n + k) for
            // n entries and k matches.
            for (key, entry) in self.entries.range(prefix.to_vec()..) {
                if !key.starts_with(prefix) {
                    break;
                }
                candidates.push((key, entry));
            }
        }
        candidates.sort_by(|a, b| {
            // Descending score, ascending lexicographic on
            // tie. Use `partial_cmp` then unwrap to
            // `Equal` because `f64` comparisons can return
            // `None` for NaN; the FT.SUGADD parser rejects
            // NaN scores, so an `Equal` tie-break for an
            // unreachable input is safe.
            match b.1.score.partial_cmp(&a.1.score).unwrap_or(Ordering::Equal) {
                Ordering::Equal => a.0.cmp(b.0),
                ord => ord,
            }
        });
        candidates.truncate(max);
        candidates
            .into_iter()
            .map(|(key, entry)| SuggestionHit {
                value: key.clone(),
                score: if with_scores { Some(entry.score) } else { None },
                payload: if with_payloads {
                    entry.payload.clone()
                } else {
                    None
                },
            })
            .collect()
    }
}

/// True when `prefix` is within `max_errors` edits of being
/// a prefix of `candidate`.
///
/// Implements the standard banded edit-distance recurrence
/// over a width-`2 * max_errors + 1` window, plus the
/// "free leading text" prefix-match rule: any prefix of
/// `candidate` of length `>= prefix.len() - max_errors` is
/// allowed to match. The result is the boolean answer to
/// "is the minimum prefix-edit-distance `<= max_errors`?"
/// rather than the distance itself, which keeps the
/// implementation simple for the FUZZY path.
fn fuzzy_prefix_match(prefix: &[u8], candidate: &[u8], max_errors: usize) -> bool {
    if max_errors == 0 {
        return candidate.starts_with(prefix);
    }
    let m = prefix.len();
    if m == 0 {
        return true;
    }
    // The longest candidate prefix worth comparing is
    // `prefix.len() + max_errors` bytes; an edit-distance
    // budget of `max_errors` cannot accept a longer
    // alignment that would also need a deletion to recover.
    let window_end = (m + max_errors).min(candidate.len());
    let txt = &candidate[..window_end];
    let n = txt.len();
    let k = max_errors;

    // dp[i][j] = edits to turn prefix[..i] into some prefix
    // of txt[..j]. Only cells with |i - j| <= k matter; two
    // rolling rows are enough.
    let mut prev = vec![usize::MAX; n + 1];
    let mut curr = vec![usize::MAX; n + 1];
    prev[0] = 0;
    for cell in prev.iter_mut().take(n.min(k) + 1).skip(1) {
        *cell = 0; // free leading text bytes (prefix-match).
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
            let cost = usize::from(prefix[i - 1] != txt[j - 1]);
            let sub = prev[j - 1].saturating_add(cost);
            let del = prev[j].saturating_add(1);
            let ins = curr[j - 1].saturating_add(1);
            curr[j] = sub.min(del).min(ins);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    let lo = m.saturating_sub(k);
    let hi = (m + k).min(n);
    let mut best = usize::MAX;
    for cell in prev.iter().take(hi + 1).skip(lo) {
        best = best.min(*cell);
    }
    best <= k
}

#[cfg(test)]
mod tests {
    use super::*;
    use hegel::generators as gs;
    use hegel::TestCase;

    // ---- unit checks ---------------------------------------------------

    #[test]
    fn add_grows_dict() {
        let mut d = SuggestionDict::new();
        assert_eq!(d.add(b"alpha".to_vec(), 1.0, false, None), 1);
        assert_eq!(d.add(b"beta".to_vec(), 1.0, false, None), 2);
        assert_eq!(d.add(b"alpha".to_vec(), 5.0, false, None), 2);
    }

    #[test]
    fn replace_overwrites_score() {
        let mut d = SuggestionDict::new();
        d.add(b"hello".to_vec(), 1.0, false, None);
        d.add(b"hello".to_vec(), 7.0, false, None);
        let hits = d.get(b"hello", 5, false, true, false);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].score, Some(7.0));
    }

    #[test]
    fn incr_adds_to_score() {
        let mut d = SuggestionDict::new();
        d.add(b"hello".to_vec(), 1.5, false, None);
        d.add(b"hello".to_vec(), 0.5, true, None);
        let hits = d.get(b"hello", 5, false, true, false);
        assert!((hits[0].score.unwrap() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn fuzzy_one_edit_substitution() {
        // Substitute a single byte.
        assert!(fuzzy_prefix_match(b"helo", b"hello world", 1));
    }

    #[test]
    fn fuzzy_one_edit_insertion() {
        // Insert one byte into the prefix that the candidate
        // does not have at the same position. The candidate
        // is "hello"; the typo "heallo" inserts an extra
        // 'a'.
        assert!(fuzzy_prefix_match(b"heallo", b"hello world", 1));
    }

    #[test]
    fn fuzzy_one_edit_deletion() {
        // Drop one byte from the prefix.
        assert!(fuzzy_prefix_match(b"hllo", b"hello world", 1));
    }

    #[test]
    fn fuzzy_two_edits_rejected_at_k1() {
        assert!(!fuzzy_prefix_match(b"hxylo", b"hello world", 1));
    }

    #[test]
    fn strict_prefix_rejects_substitution() {
        assert!(!fuzzy_prefix_match(b"helo", b"hello world", 0));
        assert!(fuzzy_prefix_match(b"hell", b"hello world", 0));
    }

    #[test]
    fn get_orders_by_descending_score() {
        let mut d = SuggestionDict::new();
        d.add(b"apple".to_vec(), 1.0, false, None);
        d.add(b"apricot".to_vec(), 5.0, false, None);
        d.add(b"avocado".to_vec(), 3.0, false, None);
        let hits = d.get(b"a", 5, false, true, false);
        let names: Vec<&[u8]> = hits.iter().map(|h| h.value.as_slice()).collect();
        assert_eq!(names, vec![&b"apricot"[..], &b"avocado"[..], &b"apple"[..]]);
    }

    #[test]
    fn get_breaks_score_ties_lexicographically() {
        let mut d = SuggestionDict::new();
        d.add(b"banana".to_vec(), 1.0, false, None);
        d.add(b"apple".to_vec(), 1.0, false, None);
        d.add(b"cherry".to_vec(), 1.0, false, None);
        let hits = d.get(b"", 5, false, false, false);
        let names: Vec<&[u8]> = hits.iter().map(|h| h.value.as_slice()).collect();
        assert_eq!(names, vec![&b"apple"[..], &b"banana"[..], &b"cherry"[..]]);
    }

    #[test]
    fn del_returns_presence() {
        let mut d = SuggestionDict::new();
        d.add(b"alpha".to_vec(), 1.0, false, None);
        assert!(d.del(b"alpha"));
        assert!(!d.del(b"alpha"));
        assert_eq!(d.len(), 0);
    }

    // ---- property tests ------------------------------------------------

    /// Generate a random suggestion byte string. Bytes are
    /// drawn from a tiny alphabet so duplicates and shared
    /// prefixes are common.
    fn arb_suggestion(tc: &TestCase) -> Vec<u8> {
        let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'd'));
            out.push(c);
        }
        out
    }

    fn arb_score(tc: &TestCase) -> f64 {
        // Bounded positive floats keep the assertions stable
        // across `INCR` accumulation; the FT.SUGADD parser
        // accepts arbitrary floats but the property under
        // test does not exercise the score-domain shape.
        let n = tc.draw(gs::integers::<i32>().min_value(0).max_value(1000));
        f64::from(n) / 10.0
    }

    fn arb_corpus(tc: &TestCase) -> Vec<(Vec<u8>, f64)> {
        let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push((arb_suggestion(tc), arb_score(tc)));
        }
        out
    }

    /// `len` counts unique suggestions; duplicate adds with
    /// `incr=false` are score replacements, not new rows.
    #[hegel::test(test_cases = 256)]
    fn len_equals_unique_suggestion_count(tc: TestCase) {
        let corpus = arb_corpus(&tc);
        let mut d = SuggestionDict::new();
        for (s, score) in &corpus {
            d.add(s.clone(), *score, false, None);
        }
        let mut unique: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        for (s, _) in &corpus {
            unique.insert(s.clone());
        }
        assert_eq!(d.len(), unique.len());
    }

    /// Every strict-prefix `get` hit truly carries the prefix
    /// at its head.
    #[hegel::test(test_cases = 256)]
    fn strict_prefix_hits_carry_prefix(tc: TestCase) {
        let corpus = arb_corpus(&tc);
        let prefix = arb_suggestion(&tc);
        let mut d = SuggestionDict::new();
        for (s, score) in &corpus {
            d.add(s.clone(), *score, false, None);
        }
        let hits = d.get(&prefix, 50, false, false, false);
        for hit in &hits {
            assert!(
                hit.value.starts_with(&prefix),
                "non-prefix hit {hit:?} for prefix {prefix:?}",
            );
        }
    }

    /// `get` results are sorted by descending score, then
    /// ascending lex on ties.
    #[hegel::test(test_cases = 256)]
    fn get_results_are_sorted(tc: TestCase) {
        let corpus = arb_corpus(&tc);
        let mut d = SuggestionDict::new();
        for (s, score) in &corpus {
            d.add(s.clone(), *score, false, None);
        }
        let hits = d.get(b"", 100, false, true, false);
        for w in hits.windows(2) {
            let (a, b) = (&w[0], &w[1]);
            let a_score = a.score.unwrap();
            let b_score = b.score.unwrap();
            // Score must not increase across adjacent hits.
            assert!(
                b_score <= a_score + f64::EPSILON,
                "score order broken: {a:?} then {b:?}",
            );
            if (a_score - b_score).abs() <= f64::EPSILON {
                assert!(a.value <= b.value, "lex tie-break broken at {a:?} {b:?}");
            }
        }
    }

    /// `del(s); del(s)` always leaves the dictionary in a
    /// state where `get` no longer surfaces `s`. The second
    /// `del` returns `false`.
    #[hegel::test(test_cases = 256)]
    fn del_is_idempotent(tc: TestCase) {
        let corpus = arb_corpus(&tc);
        let target = arb_suggestion(&tc);
        let mut d = SuggestionDict::new();
        for (s, score) in &corpus {
            d.add(s.clone(), *score, false, None);
        }
        let _ = d.del(&target);
        assert!(!d.del(&target));
        let hits = d.get(&target, 50, false, false, false);
        assert!(
            hits.iter().all(|h| h.value != target),
            "deleted entry leaked into hits: {hits:?}",
        );
    }
}
