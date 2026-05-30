//! Internal regex AST used by the Phase 2 prefix extractor.
//!
//! The full `regex` crate parses and compiles a pattern down to
//! a finite-state machine; for *prefix extraction* we only need
//! the structural shape of the pattern, not its matcher. We
//! reuse [`regex_syntax`]'s high-level intermediate
//! representation for the heavy lifting (tokenisation,
//! `\w`/`\d`/`\s` expansion, character class normalisation,
//! repetition canonicalisation) and project it into a tiny AST
//! that the prefix extractor in [`crate::prefix_extract`] walks.
//!
//! # Supported subset
//!
//! The AST and its [`parse`] entry point accept the subset of
//! regular expressions that [`regex_syntax`] can lower to its
//! HIR -- this excludes lookarounds, backreferences, and other
//! features the [`regex`] crate does not implement. Inside that
//! subset, named capture groups (`(?P<foo>...)`) are flagged
//! as [`RegexError::PrefixUnsupported`]. All other shapes
//! (literals, anchors, `.`, character classes, alternation,
//! grouping, and `*`/`+`/`?`/`{m,n}` repetition) project into
//! the AST below.
//!
//! Callers that hit [`RegexError::PrefixUnsupported`] can still
//! evaluate the regex; they just have to fall back to a full
//! scan + recheck instead of the trigram-driven candidate
//! pruning.
//!
//! # Examples
//!
//! ```
//! use dyntext::regex_ast::{parse, Ast};
//!
//! let ast = parse("foo|bar").expect("parses");
//! assert!(matches!(ast, Ast::Alt(_)));
//! ```

use regex_syntax::hir::{Hir, HirKind};
use thiserror::Error;

/// Errors surfaced by the regex parser and prefix extractor.
#[derive(Debug, Error)]
pub enum RegexError {
    /// The pattern is syntactically invalid or uses a feature
    /// outside [`regex_syntax`]'s supported subset (lookahead,
    /// backreferences, etc).
    #[error("regex parse error: {0}")]
    Parse(String),

    /// The pattern parsed but uses a feature the prefix
    /// extractor cannot lower into trigram constraints.
    /// Callers should fall back to a full scan.
    #[error("regex prefix extraction unsupported: {0}")]
    PrefixUnsupported(&'static str),
}

/// Internal regex AST used by the prefix extractor.
///
/// Variants are intentionally coarse: the prefix extractor only
/// distinguishes nodes that contribute literal bytes, nodes that
/// break a literal run (any opaque-character node), and the
/// structural combinators (concat, alt, repeat).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ast {
    /// Empty regex (matches the empty string). Produced by
    /// `()`, `a{0}`, etc.
    Empty,

    /// A run of literal bytes that must appear contiguously in
    /// any matching string. The byte vector is non-empty.
    Literal(Vec<u8>),

    /// A zero-width assertion: `^`, `$`, `\b`, `\B`, and the
    /// multi-line variants. Carries no literal-byte
    /// information and is treated by the extractor as a hard
    /// break in any surrounding literal run, since adjacent
    /// literals across an anchor are not guaranteed to be
    /// adjacent in the matching text.
    Anchor,

    /// Any single character: `.`, a character class `[abc]`, or
    /// `\w`/`\d`/`\s` expansions. The extractor treats this as
    /// opaque and breaks any surrounding literal run.
    AnyChar,

    /// Concatenation of sub-patterns. Always has at least two
    /// children when produced by [`parse`]; nullary or unary
    /// concatenations collapse to [`Ast::Empty`] or the single
    /// child respectively.
    Concat(Vec<Ast>),

    /// Alternation of sub-patterns. Always has at least two
    /// children when produced by [`parse`].
    Alt(Vec<Ast>),

    /// Repetition operator with a minimum and optional maximum
    /// count. Covers `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`.
    Repeat {
        /// Sub-expression being repeated.
        sub: Box<Ast>,
        /// Lower bound on iteration count.
        min: u32,
        /// Upper bound, or [`None`] for unbounded repetition.
        max: Option<u32>,
    },
}

impl Ast {
    /// Parse a pattern into the internal AST.
    ///
    /// # Errors
    ///
    /// * [`RegexError::Parse`] if the pattern is syntactically
    ///   invalid or uses a feature [`regex_syntax`] does not
    ///   accept (lookarounds, backreferences, ...).
    /// * [`RegexError::PrefixUnsupported`] if the pattern
    ///   parses but contains a feature the extractor cannot
    ///   lower (named capture groups).
    pub fn parse(pattern: &str) -> Result<Self, RegexError> {
        let hir = regex_syntax::parse(pattern).map_err(|e| RegexError::Parse(e.to_string()))?;
        Self::from_hir(&hir)
    }

    fn from_hir(hir: &Hir) -> Result<Self, RegexError> {
        match hir.kind() {
            HirKind::Empty => Ok(Ast::Empty),
            HirKind::Literal(lit) => {
                if lit.0.is_empty() {
                    // Defensive: HIR's smart constructors are
                    // supposed to elide empty literals into
                    // `Empty`, but we collapse here just in
                    // case a future regex-syntax version slips
                    // one through.
                    Ok(Ast::Empty)
                } else {
                    Ok(Ast::Literal(lit.0.to_vec()))
                }
            }
            HirKind::Class(_) => Ok(Ast::AnyChar),
            HirKind::Look(_) => Ok(Ast::Anchor),
            HirKind::Repetition(rep) => {
                let sub = Self::from_hir(&rep.sub)?;
                Ok(Ast::Repeat {
                    sub: Box::new(sub),
                    min: rep.min,
                    max: rep.max,
                })
            }
            HirKind::Capture(cap) => {
                if cap.name.is_some() {
                    return Err(RegexError::PrefixUnsupported("named capture group"));
                }
                Self::from_hir(&cap.sub)
            }
            HirKind::Concat(parts) => {
                let mut v = Vec::with_capacity(parts.len());
                for p in parts {
                    v.push(Self::from_hir(p)?);
                }
                Ok(Self::collapse_concat(v))
            }
            HirKind::Alternation(parts) => {
                let mut v = Vec::with_capacity(parts.len());
                for p in parts {
                    v.push(Self::from_hir(p)?);
                }
                if v.len() == 1 {
                    Ok(v.into_iter().next().expect("len == 1"))
                } else {
                    Ok(Ast::Alt(v))
                }
            }
        }
    }

    fn collapse_concat(parts: Vec<Ast>) -> Ast {
        match parts.len() {
            0 => Ast::Empty,
            1 => parts.into_iter().next().expect("len == 1"),
            _ => Ast::Concat(parts),
        }
    }
}

/// Convenience free-function form of [`Ast::parse`].
///
/// # Examples
///
/// ```
/// use dyntext::regex_ast::{parse, Ast};
///
/// let ast = parse("ab+").expect("parses");
/// // "ab+" is concat(literal "a", repeat(literal "b", min=1)).
/// assert!(matches!(ast, Ast::Concat(_)));
/// ```
///
/// # Errors
///
/// Returns [`RegexError`] under the same conditions as
/// [`Ast::parse`].
pub fn parse(pattern: &str) -> Result<Ast, RegexError> {
    Ast::parse(pattern)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_literal_yields_literal_bytes() {
        let ast = parse("hello").expect("parses");
        assert_eq!(ast, Ast::Literal(b"hello".to_vec()));
    }

    #[test]
    fn parse_concat_of_literal_and_dot_yields_concat() {
        let ast = parse("ab.").expect("parses");
        match ast {
            Ast::Concat(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Ast::Literal(b"ab".to_vec()));
                assert_eq!(parts[1], Ast::AnyChar);
            }
            other => panic!("expected concat, got {other:?}"),
        }
    }

    #[test]
    fn parse_alternation_two_branches_yields_alt() {
        let ast = parse("foo|bar").expect("parses");
        match ast {
            Ast::Alt(branches) => {
                assert_eq!(branches.len(), 2);
            }
            other => panic!("expected alt, got {other:?}"),
        }
    }

    #[test]
    fn parse_unsupported_lookahead_returns_error() {
        // Lookarounds are not representable in regex-syntax's
        // HIR; the parser rejects them.
        let err = parse("(?=foo)").expect_err("lookahead must error");
        assert!(matches!(err, RegexError::Parse(_)));
    }

    #[test]
    fn parse_named_capture_returns_prefix_unsupported() {
        let err = parse("(?P<name>abc)").expect_err("named capture errors");
        assert!(matches!(err, RegexError::PrefixUnsupported(_)));
    }

    #[test]
    fn parse_anchors_show_up_as_anchor_nodes() {
        let ast = parse("^a$").expect("parses");
        match ast {
            Ast::Concat(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Ast::Anchor);
                assert_eq!(parts[1], Ast::Literal(b"a".to_vec()));
                assert_eq!(parts[2], Ast::Anchor);
            }
            other => panic!("expected concat, got {other:?}"),
        }
    }

    #[test]
    fn parse_grouping_with_quantifier_yields_repeat() {
        let ast = parse("(ab)+").expect("parses");
        match ast {
            Ast::Repeat { sub, min, max } => {
                assert_eq!(*sub, Ast::Literal(b"ab".to_vec()));
                assert_eq!(min, 1);
                assert!(max.is_none());
            }
            other => panic!("expected repeat, got {other:?}"),
        }
    }
}
