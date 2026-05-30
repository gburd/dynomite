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
//!   that intersection to the parent.
//! * `Repeat { sub, min, max }`:
//!     * `min == 0`: flush (sub may not appear).
//!     * `min >= 1`: inline sub once. If `min >= 2` we inline
//!       a second copy so any trigrams that span the boundary
//!       between two repetitions also become required.
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
/// Two is enough to capture the trigrams that span the boundary
/// between the first two iterations; any further iterations
/// only repeat trigrams we have already discovered.
const REPEAT_INLINE_CAP: u32 = 2;

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
            Ast::Concat(parts) => {
                for p in parts {
                    self.append(p);
                }
            }
            Ast::Alt(branches) => self.append_alt(branches),
            Ast::Repeat { sub, min, max: _ } => self.append_repeat(sub, *min),
        }
    }

    fn append_alt(&mut self, branches: &[Ast]) {
        // The parent run cannot extend through an alternation:
        // each branch starts at this position and the bytes
        // each branch contributes here may differ.
        self.flush();

        // No branches -> alternation matches nothing usable;
        // contribute no trigrams.
        let Some((first, rest)) = branches.split_first() else {
            return;
        };

        // Compute required trigrams for the first branch.
        let mut common: Vec<[u8; 3]> = {
            let mut sub = LinState::new();
            sub.append(first);
            sub.flush();
            sub.required
        };

        // Intersect with every other branch.
        for b in rest {
            if common.is_empty() {
                // Intersection with anything is empty; bail
                // early.
                return;
            }
            let mut sub = LinState::new();
            sub.append(b);
            sub.flush();
            common.retain(|t| sub.required.contains(t));
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
        // REPEAT_INLINE_CAP copies so trigrams spanning the
        // boundary between iterations also become required.
        let copies = min.min(REPEAT_INLINE_CAP);
        for _ in 0..copies {
            self.append(sub);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::regex_ast::parse;

    fn extract(pattern: &str) -> Vec<[u8; 3]> {
        let ast = parse(pattern).expect("parses");
        required_trigrams(&ast)
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
}
