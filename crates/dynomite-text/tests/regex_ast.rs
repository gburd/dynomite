//! Integration tests for the regex AST parser.
//!
//! These tests exercise [`dyntext::regex_ast::parse`] -- the
//! conversion from a regex pattern string into the small
//! internal AST that the prefix extractor walks.

use dyntext::regex_ast::{parse, Ast, RegexError};

#[test]
fn parse_simple_literal() {
    let ast = parse("hello").expect("parses");
    assert_eq!(ast, Ast::Literal(b"hello".to_vec()));
}

#[test]
fn parse_alternation_two_branches() {
    let ast = parse("foo|bar").expect("parses");
    match ast {
        Ast::Alt(branches) => {
            assert_eq!(branches.len(), 2);
            assert!(branches.contains(&Ast::Literal(b"foo".to_vec())));
            assert!(branches.contains(&Ast::Literal(b"bar".to_vec())));
        }
        other => panic!("expected alt, got {other:?}"),
    }
}

#[test]
fn parse_alternation_three_branches() {
    let ast = parse("foo|bar|baz").expect("parses");
    match ast {
        Ast::Alt(branches) => {
            assert_eq!(branches.len(), 3);
        }
        other => panic!("expected alt, got {other:?}"),
    }
}

#[test]
fn parse_grouping_with_quantifier() {
    let ast = parse("(abc)+").expect("parses");
    match ast {
        Ast::Repeat { sub, min, max } => {
            assert_eq!(*sub, Ast::Literal(b"abc".to_vec()));
            assert_eq!(min, 1);
            assert!(max.is_none());
        }
        other => panic!("expected repeat, got {other:?}"),
    }
}

#[test]
fn parse_optional_group_yields_repeat_min_zero() {
    let ast = parse("(abc)?").expect("parses");
    match ast {
        Ast::Repeat { sub, min, max } => {
            assert_eq!(*sub, Ast::Literal(b"abc".to_vec()));
            assert_eq!(min, 0);
            assert_eq!(max, Some(1));
        }
        other => panic!("expected repeat, got {other:?}"),
    }
}

#[test]
fn parse_bounded_repetition_yields_repeat_min_max() {
    let ast = parse("a{3,5}").expect("parses");
    match ast {
        Ast::Repeat { sub, min, max } => {
            assert_eq!(*sub, Ast::Literal(b"a".to_vec()));
            assert_eq!(min, 3);
            assert_eq!(max, Some(5));
        }
        other => panic!("expected repeat, got {other:?}"),
    }
}

#[test]
fn parse_unsupported_lookahead_returns_error() {
    // Lookarounds are not supported by the regex crate / HIR.
    let err = parse("(?=foo)").expect_err("lookahead must error");
    assert!(matches!(err, RegexError::Parse(_)));
}

#[test]
fn parse_unsupported_lookbehind_returns_error() {
    let err = parse("(?<=foo)bar").expect_err("lookbehind must error");
    assert!(matches!(err, RegexError::Parse(_)));
}

#[test]
fn parse_unsupported_backreference_returns_error() {
    // The regex crate does not implement backreferences; the
    // parser rejects them.
    let err = parse(r"(\w+)\1").expect_err("backref must error");
    assert!(matches!(err, RegexError::Parse(_)));
}

#[test]
fn parse_named_capture_returns_prefix_unsupported() {
    let err = parse("(?P<foo>abc)").expect_err("named capture errors");
    assert!(matches!(err, RegexError::PrefixUnsupported(_)));
}

#[test]
fn parse_anchors_dont_constrain_required() {
    // Anchors at any position appear as `Anchor` nodes; the
    // prefix extractor does not derive literals from them.
    let ast = parse("^abc$").expect("parses");
    match ast {
        Ast::Concat(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Ast::Anchor);
            assert_eq!(parts[1], Ast::Literal(b"abc".to_vec()));
            assert_eq!(parts[2], Ast::Anchor);
        }
        other => panic!("expected concat, got {other:?}"),
    }
}

#[test]
fn parse_word_boundary_is_an_anchor() {
    let ast = parse(r"\bword\b").expect("parses");
    match ast {
        Ast::Concat(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Ast::Anchor);
            assert_eq!(parts[1], Ast::Literal(b"word".to_vec()));
            assert_eq!(parts[2], Ast::Anchor);
        }
        other => panic!("expected concat, got {other:?}"),
    }
}

#[test]
fn parse_dot_yields_any_char() {
    let ast = parse("a.b").expect("parses");
    match ast {
        Ast::Concat(parts) => {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Ast::Literal(b"a".to_vec()));
            assert_eq!(parts[1], Ast::AnyChar);
            assert_eq!(parts[2], Ast::Literal(b"b".to_vec()));
        }
        other => panic!("expected concat, got {other:?}"),
    }
}

#[test]
fn parse_character_class_yields_any_char() {
    let ast = parse("[abc]").expect("parses");
    assert_eq!(ast, Ast::AnyChar);
}

#[test]
fn parse_word_class_yields_any_char() {
    let ast = parse(r"\w").expect("parses");
    assert_eq!(ast, Ast::AnyChar);
}

#[test]
fn parse_invalid_pattern_returns_parse_error() {
    let err = parse("[unclosed").expect_err("must error");
    assert!(matches!(err, RegexError::Parse(_)));
}
