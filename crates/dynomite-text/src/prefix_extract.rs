//! Required-trigram extraction from a regex AST.
//!
//! Given a parsed [`Ast`], compute the set of byte trigrams
//! that *must* appear in any string that the regex matches.
//! Intersecting the postings lists for those trigrams gives a
//! candidate set for the recheck step, which is much smaller
//! than the full corpus on selective regexes.
//!
//! # Algorithm
//!
//! The extractor walks the AST while maintaining a "literal
//! run" -- a buffer of bytes that are guaranteed to appear
//! *contiguously* in any matching string. Every time a node is
//! encountered that breaks the run (an opaque character class,
//! an anchor, a `min == 0` repetition, or an alternation), the
//! run is flushed: every length-3 window is added to the
//! required set, and the buffer is cleared.
//!
//! Combinator rules (matching the simplified Russ-Cox
//! formulation):
//!
//! * `Empty`: no-op.
//! * `Literal(bytes)`: append `bytes` to the run.
//! * `AnyChar`: flush the run.
//! * `Anchor`: flush the run. Anchors are zero-width but do not
//!   guarantee adjacency of the literal text on either side
//!   (a `^` between two literals only matches across line
//!   boundaries, where the bytes are separated by a newline),
//!   so we conservatively break the run.
//! * `Concat(parts)`: walk each child in order, with the run
//!   continuing through the boundary so adjacent literals
//!   coalesce.
//! * `Alt(branches)`: flush the parent run, extract each
//!   branch's required trigrams in a fresh accumulator, then
//!   take the intersection across branches and contribute
//!   that intersection to the parent. The extractor also looks
//!   at the surrounding concat to splice a common prefix or
//!   suffix into each branch before computing trigrams, so
//!   patterns like `(foo|bar)baz` contribute the trigram
//!   `baz` and patterns like `pre(foo|bar)` contribute the
//!   trigram derived from `prefoo`/`prebar`'s common run.
//! * `Repeat { sub, min, max }`:
//!     * `min == 0`: flush (sub may not appear).
//!     * `min >= 1`: inline sub once. If `min >= 2` we inline
//!       a second copy so any trigrams that span the boundary
//!       between two repetitions also become required. For a
//!       single-byte `Literal` repeated with `min >= 3` we
//!       inline a third copy so the homogeneous trigram (e.g.
//!       `aaa`) becomes required.
//!
//! The extractor is *conservative*: it never reports a trigram
//! that is not actually required (no false positives in the
//! required set), but it may miss trigrams an oracle could
//! infer (false negatives are fine -- they only cost us a
//! larger candidate set, never correctness).
//!
//! # Examples
//!
//! ```
//! use dyntext::prefix_extract::required_trigrams;
//! use dyntext::regex_ast::parse;
//!
//! let ast = parse("error").expect("parses");
//! let tris = required_trigrams(&ast);
//! // "error" -> ["err", "rro", "ror"].
//! assert_eq!(tris.len(), 3);
//! assert!(tris.contains(b"err"));
//! assert!(tris.contains(b"rro"));
//! assert!(tris.contains(b"ror"));
//! ```

use crate::regex_ast::Ast;
use crate::trigram;

/// Cap on how many copies of a `min >= 1` repetition we inline
/// while expanding the literal run.
///
/// Three is enough to capture the trigrams that span the boundary
/// between iterations of a single-byte literal (`aaa`, `bbb`,
/// ...); any further iterations only repeat trigrams we have
/// already discovered. For multi-byte sub-expressions two copies
/// are sufficient.
const REPEAT_INLINE_CAP_MULTI: u32 = 2;
const REPEAT_INLINE_CAP_SINGLE: u32 = 3;

/// Compute the set of byte trigrams that any string matching
/// `ast` must contain.
///
/// The returned vector is sorted lexicographically and contains
/// no duplicates. An empty result means the regex imposes no
/// trigram constraints; the caller should fall back to a full
/// scan.
#[must_use]
pub fn required_trigrams(ast: &Ast) -> Vec<[u8; 3]> {
    let mut state = LinState::new();
    state.append(ast);
    state.flush();
    state.required.sort_unstable();
    state.required.dedup();
    state.required
}

/// Hash-form of [`required_trigrams`]. The hashes match the
/// hashes that the postings index uses, so the output can be
/// fed directly into [`crate::postings::Postings::intersect`].
#[must_use]
pub fn required_trigram_hashes(ast: &Ast) -> Vec<u64> {
    let mut hashes: Vec<u64> = required_trigrams(ast)
        .iter()
        .map(|t| trigram::hash_trigram(t))
        .collect();
    hashes.sort_unstable();
    hashes.dedup();
    hashes
}

/// Extract the maximal contiguous literal runs that must appear
/// in any matching string.
///
/// A "run" is a contiguous byte sequence that must appear *as a
/// substring* in any string the regex matches. Non-literal
/// nodes (`.`, character classes, anchors, alternation,
/// repetition with `min == 0`) break a run; a repetition with
/// `min >= 1` is inlined, the same as the trigram extractor.
///
/// The runs are returned in pattern-order; an empty vector
/// means the pattern has no required literal substrings.
///
/// # Examples
///
/// ```
/// use dyntext::prefix_extract::extract_literal_runs;
/// use dyntext::regex_ast::parse;
///
/// let ast = parse("abc.def").expect("parses");
/// let runs = extract_literal_runs(&ast);
/// assert_eq!(runs, vec![b"abc".to_vec(), b"def".to_vec()]);
/// ```
#[must_use]
pub fn extract_literal_runs(ast: &Ast) -> Vec<Vec<u8>> {
    let mut state = RunState::new();
    state.append(ast);
    state.flush();
    state.runs
}

/// Whether the AST's top-level concatenation begins with a
/// `^` anchor. Used by the index's anchor-aware fast path.
#[must_use]
pub fn has_top_level_start_anchor(ast: &Ast) -> bool {
    match ast {
        Ast::Anchor => true,
        Ast::Concat(parts) => parts.first().is_some_and(has_top_level_start_anchor),
        _ => false,
    }
}

/// If the AST's top-level concatenation starts with a `^`
/// anchor immediately followed by a literal run, return that
/// literal prefix. The prefix is the maximal contiguous literal
/// sub-run that follows the anchor.
///
/// Returns `None` for unanchored patterns or for patterns whose
/// anchor is followed by a non-literal node.
#[must_use]
pub fn anchored_prefix(ast: &Ast) -> Option<Vec<u8>> {
    let Ast::Concat(parts) = ast else {
        return None;
    };
    let mut iter = parts.iter();
    match iter.next()? {
        Ast::Anchor => {}
        _ => return None,
    }
    let mut prefix = Vec::new();
    for p in iter {
        match p {
            Ast::Literal(bytes) => prefix.extend_from_slice(bytes),
            _ => break,
        }
    }
    if prefix.is_empty() {
        None
    } else {
        Some(prefix)
    }
}

/// Linearizer state: a literal-byte run and the accumulated
/// required-trigram set.
struct LinState {
    run: Vec<u8>,
    required: Vec<[u8; 3]>,
}

impl LinState {
    fn new() -> Self {
        Self {
            run: Vec::new(),
            required: Vec::new(),
        }
    }

    /// Flush the current literal run: emit every length-3
    /// window into the required set, then clear the run.
    fn flush(&mut self) {
        for w in self.run.windows(trigram::TRIGRAM_LEN) {
            let t: [u8; 3] = [w[0], w[1], w[2]];
            if !self.required.contains(&t) {
                self.required.push(t);
            }
        }
        self.run.clear();
    }

    /// Walk the AST node, mutating `run` and `required` per the
    /// algorithm in the module-level docs.
    fn append(&mut self, ast: &Ast) {
        match ast {
            Ast::Empty => {}
            Ast::Literal(bytes) => self.run.extend_from_slice(bytes),
            Ast::AnyChar | Ast::Anchor => self.flush(),
            Ast::Concat(parts) => self.append_concat(parts),
            Ast::Alt(branches) => self.append_alt(branches, &[], &[]),
            Ast::Repeat { sub, min, max: _ } => self.append_repeat(sub, *min),
        }
    }

    /// Walk a concat, looking ahead one step so an `Alt` child
    /// can splice in a common prefix (the bytes accumulated in
    /// `self.run` so far) and a common suffix (the literal-byte
    /// prefix of the next concat child) when computing each
    /// branch's trigrams.
    fn append_concat(&mut self, parts: &[Ast]) {
        let n = parts.len();
        let mut i = 0;
        while i < n {
            let part = &parts[i];
            if let Ast::Alt(branches) = part {
                let prefix = self.run.clone();
                let suffix = next_literal_prefix(parts, i + 1);
                self.append_alt(branches, &prefix, &suffix);
                // The alternation's flush already consumed any
                // run state. Continue with the rest of the
                // concat.
                i += 1;
                continue;
            }
            self.append(part);
            i += 1;
        }
    }

    fn append_alt(&mut self, branches: &[Ast], context_prefix: &[u8], context_suffix: &[u8]) {
        // The parent run cannot extend through an alternation:
        // each branch starts at this position and the bytes
        // each branch contributes here may differ.
        self.flush();

        // No branches -> alternation matches nothing usable;
        // contribute no trigrams.
        let Some((first, rest)) = branches.split_first() else {
            return;
        };

        // Compute required trigrams for the first branch with
        // the surrounding context spliced in. The context is
        // the byte prefix that the parent concat has already
        // accumulated and the byte suffix at the start of the
        // next concat child (if any). Splicing produces real
        // trigrams that span the alt boundary -- e.g. for
        // `pre(foo|bar)baz`, the first branch's "pre" prefix
        // and "baz" suffix mean the linearised stream becomes
        // "prefoobaz", which legitimately yields trigrams
        // `pre`, `ref`, `efo`, `foo`, `oob`, `oba`, `baz`.
        let mut common: Vec<[u8; 3]> = branch_trigrams(first, context_prefix, context_suffix);

        // Intersect with every other branch.
        for b in rest {
            if common.is_empty() {
                // Intersection with anything is empty; bail
                // early.
                return;
            }
            let other = branch_trigrams(b, context_prefix, context_suffix);
            common.retain(|t| other.contains(t));
        }

        // Merge the intersection into the parent's required
        // set. The parent run remains empty after the flush;
        // alternation does not contribute literal bytes the
        // parent can extend.
        for t in common {
            if !self.required.contains(&t) {
                self.required.push(t);
            }
        }
    }

    fn append_repeat(&mut self, sub: &Ast, min: u32) {
        if min == 0 {
            // Sub may not appear at all.
            self.flush();
            return;
        }
        // At least one iteration is guaranteed. Inline up to
        // REPEAT_INLINE_CAP_* copies so trigrams spanning the
        // boundary between iterations also become required.
        // For a single-byte literal (e.g. `a{3,}`), inline three
        // copies so the homogeneous trigram is captured.
        let cap = if let Ast::Literal(bytes) = sub {
            if bytes.len() == 1 {
                REPEAT_INLINE_CAP_SINGLE
            } else {
                REPEAT_INLINE_CAP_MULTI
            }
        } else {
            REPEAT_INLINE_CAP_MULTI
        };
        let copies = min.min(cap);
        for _ in 0..copies {
            self.append(sub);
        }
    }
}

/// Run-extraction state. Like `LinState` but emits the contents
/// of the literal run buffer at every flush, instead of every
/// length-3 window.
struct RunState {
    run: Vec<u8>,
    runs: Vec<Vec<u8>>,
}

impl RunState {
    fn new() -> Self {
        Self {
            run: Vec::new(),
            runs: Vec::new(),
        }
    }

    fn flush(&mut self) {
        if !self.run.is_empty() {
            self.runs.push(std::mem::take(&mut self.run));
        }
    }

    fn append(&mut self, ast: &Ast) {
        match ast {
            Ast::Empty => {}
            Ast::Literal(bytes) => self.run.extend_from_slice(bytes),
            Ast::AnyChar | Ast::Anchor => self.flush(),
            Ast::Concat(parts) => {
                for p in parts {
                    self.append(p);
                }
            }
            Ast::Alt(branches) => {
                self.flush();
                // Compute each branch's runs and intersect
                // their union. A run is required by an
                // alternation iff it appears as a sub-run in
                // every branch. We approximate: intersect on
                // exact equality of the per-branch
                // longest-required run. Practical patterns
                // rarely need richer alternation reasoning;
                // missing a run here is fine (it just costs us
                // a larger candidate set, never correctness).
                let Some((first, rest)) = branches.split_first() else {
                    return;
                };
                let first_runs = extract_literal_runs(first);
                let mut common: Vec<Vec<u8>> = first_runs;
                for b in rest {
                    if common.is_empty() {
                        return;
                    }
                    let other = extract_literal_runs(b);
                    common.retain(|r| other.iter().any(|o| o == r));
                }
                self.runs.extend(common);
            }
            Ast::Repeat { sub, min, max: _ } => {
                if *min == 0 {
                    self.flush();
                    return;
                }
                let cap = if let Ast::Literal(bytes) = sub.as_ref() {
                    if bytes.len() == 1 {
                        REPEAT_INLINE_CAP_SINGLE
                    } else {
                        REPEAT_INLINE_CAP_MULTI
                    }
                } else {
                    REPEAT_INLINE_CAP_MULTI
                };
                let copies = (*min).min(cap);
                for _ in 0..copies {
                    self.append(sub);
                }
            }
        }
    }
}

/// Return the literal-byte prefix of the concat starting at
/// `parts[start..]`. Stops at the first non-literal node.
fn next_literal_prefix(parts: &[Ast], start: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts.iter().skip(start) {
        match p {
            Ast::Literal(bytes) => out.extend_from_slice(bytes),
            _ => break,
        }
    }
    out
}

/// Compute one branch's required trigrams, with the given
/// surrounding bytes spliced into the linearised stream so
/// trigrams that span the alt boundary become candidates for
/// inter-branch intersection.
fn branch_trigrams(branch: &Ast, prefix: &[u8], suffix: &[u8]) -> Vec<[u8; 3]> {
    let mut sub = LinState::new();
    sub.run.extend_from_slice(prefix);
    sub.append(branch);
    sub.run.extend_from_slice(suffix);
    sub.flush();
    sub.required
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::regex_ast::parse;

    fn extract(pattern: &str) -> Vec<[u8; 3]> {
        let ast = parse(pattern).expect("parses");
        required_trigrams(&ast)
    }

    fn runs(pattern: &str) -> Vec<Vec<u8>> {
        let ast = parse(pattern).expect("parses");
        extract_literal_runs(&ast)
    }

    #[test]
    fn empty_pattern_yields_no_trigrams() {
        assert!(extract("").is_empty());
    }

    #[test]
    fn literal_yields_its_trigrams() {
        let tris = extract("abcde");
        assert_eq!(tris.len(), 3);
        assert!(tris.contains(b"abc"));
        assert!(tris.contains(b"bcd"));
        assert!(tris.contains(b"cde"));
    }

    #[test]
    fn dot_breaks_the_run() {
        let tris = extract("ab.cd");
        // "ab" and "cd" are each only 2 bytes -> no trigrams
        // span the dot break.
        assert!(tris.is_empty());
    }

    #[test]
    fn anchors_break_the_run() {
        // Anchors split the literal run conservatively, so
        // bytes on either side of the anchor cannot fuse.
        let tris = extract("ab^cd");
        assert!(tris.is_empty());
    }

    #[test]
    fn repeated_literal_inlines_twice() {
        // `(ab){2,}` guarantees at least two iterations, so the
        // boundary trigrams `bab` and `aba` become required.
        let tris = extract("(ab){2,}");
        assert!(tris.contains(b"aba"));
        assert!(tris.contains(b"bab"));
    }

    #[test]
    fn optional_group_yields_empty() {
        // `error` outside the optional yields its trigrams; the
        // optional `(foo)?` adds nothing because foo may not
        // appear.
        let tris = extract("(foo)?");
        assert!(tris.is_empty());
    }

    #[test]
    fn single_byte_repeat_min_three_yields_homogeneous_trigram() {
        // `a{3,}` guarantees at least three `a`s in a row, so
        // the trigram `aaa` becomes required.
        let tris = extract("a{3,}");
        assert!(tris.contains(b"aaa"), "expected 'aaa' in {tris:?}");
    }

    #[test]
    fn alternation_with_common_suffix_extracts_suffix_trigrams() {
        // `(foo|bar)baz` is followed by literal `baz`; the
        // suffix gets spliced into each branch so every branch
        // carries a `baz` trigram, and the intersection
        // contains it.
        let tris = extract("(foo|bar)baz");
        assert!(tris.contains(b"baz"), "expected 'baz' in {tris:?}");
    }

    #[test]
    fn alternation_with_common_prefix_extracts_prefix_trigrams() {
        // `pre(foo|bar)` -- both branches share the literal
        // prefix `pre`, so each linearised stream is
        // `prefoo` or `prebar` and the common trigram is
        // `pre`.
        let tris = extract("pre(foo|bar)");
        assert!(tris.contains(b"pre"), "expected 'pre' in {tris:?}");
    }

    #[test]
    fn extract_literal_runs_single_literal() {
        let r = runs("hello");
        assert_eq!(r, vec![b"hello".to_vec()]);
    }

    #[test]
    fn extract_literal_runs_split_by_dot() {
        let r = runs("foo.bar");
        assert_eq!(r, vec![b"foo".to_vec(), b"bar".to_vec()]);
    }

    #[test]
    fn extract_literal_runs_split_by_class() {
        let r = runs(r"abc\w+def");
        assert_eq!(r, vec![b"abc".to_vec(), b"def".to_vec()]);
    }

    #[test]
    fn extract_literal_runs_anchor_does_not_emit_runs() {
        let r = runs("^abc$");
        assert_eq!(r, vec![b"abc".to_vec()]);
    }

    #[test]
    fn extract_literal_runs_optional_group_drops_run() {
        let r = runs("(abc)?def");
        assert_eq!(r, vec![b"def".to_vec()]);
    }

    #[test]
    fn anchored_prefix_returns_prefix_after_caret() {
        let ast = parse("^errno: ").expect("parses");
        assert_eq!(anchored_prefix(&ast), Some(b"errno: ".to_vec()));
    }

    #[test]
    fn anchored_prefix_none_for_unanchored() {
        let ast = parse("errno: ").expect("parses");
        assert!(anchored_prefix(&ast).is_none());
    }

    #[test]
    fn anchored_prefix_none_when_anchor_followed_by_non_literal() {
        let ast = parse(r"^\w+abc").expect("parses");
        assert!(anchored_prefix(&ast).is_none());
    }

    #[test]
    fn has_top_level_start_anchor_detects_caret() {
        let ast = parse("^abc").expect("parses");
        assert!(has_top_level_start_anchor(&ast));
        let ast = parse("abc").expect("parses");
        assert!(!has_top_level_start_anchor(&ast));
    }
}
