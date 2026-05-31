//! Integration tests for the regex prefix extractor.
//!
//! Each test parses a regex pattern, extracts the trigrams that
//! every matching string MUST contain, and asserts the result
//! against the expected required-set.

use std::collections::HashSet;

use dyntext::prefix_extract::required_trigrams;
use dyntext::regex_ast::parse;
use dyntext::trigram;

/// Convenience: parse and extract in one step.
fn extract(pattern: &str) -> HashSet<[u8; 3]> {
    let ast = parse(pattern).expect("parses");
    required_trigrams(&ast).into_iter().collect()
}

/// Reference: every length-3 window of `s`.
fn trigrams_of(s: &[u8]) -> HashSet<[u8; 3]> {
    s.windows(trigram::TRIGRAM_LEN)
        .map(|w| [w[0], w[1], w[2]])
        .collect()
}

#[test]
fn prefix_of_simple_literal_yields_its_trigrams() {
    let got = extract("hello");
    let expected = trigrams_of(b"hello");
    assert_eq!(got, expected);
}

#[test]
fn prefix_of_short_literal_yields_no_trigrams() {
    // "ab" is too short to have a trigram of its own.
    let got = extract("ab");
    assert!(got.is_empty());
}

#[test]
fn prefix_of_alternation_yields_intersection() {
    // "abcde" -> {abc, bcd, cde}; "xbcdy" -> {xbc, bcd, cdy}.
    // Intersection: {bcd}.
    let got = extract("abcde|xbcdy");
    let expected: HashSet<[u8; 3]> = [*b"bcd"].into_iter().collect();
    assert_eq!(got, expected);
}

#[test]
fn prefix_of_alternation_with_disjoint_branches_yields_empty() {
    // "abcd" trigrams and "wxyz" trigrams have no overlap.
    let got = extract("abcd|wxyz");
    assert!(got.is_empty());
}

#[test]
fn prefix_of_optional_group_yields_empty() {
    // The contents of an optional group might not appear at all,
    // so no trigram is required.
    let got = extract("(hello)?");
    assert!(got.is_empty());
}

#[test]
fn prefix_of_required_group_yields_inner_trigrams() {
    // `(hello)+` requires at least one occurrence of "hello".
    let got = extract("(hello)+");
    // The minimum count is 1, so we inline once. Trigrams of
    // "hello" are: hel, ell, llo.
    assert!(got.contains(b"hel"));
    assert!(got.contains(b"ell"));
    assert!(got.contains(b"llo"));
}

#[test]
fn prefix_of_double_required_group_yields_boundary_trigrams() {
    // `(ab){2,}` requires at least two iterations, so the
    // trigrams that span the boundary become required too.
    let got = extract("(ab){2,}");
    // Inlined: "abab" -> {aba, bab}.
    assert!(got.contains(b"aba"));
    assert!(got.contains(b"bab"));
}

#[test]
fn prefix_of_dot_star_pattern_yields_empty() {
    let got = extract(".*");
    assert!(got.is_empty());
}

#[test]
fn prefix_of_dot_separated_literals_yields_no_spanning_trigrams() {
    // `abc.def` -- the dot breaks the run between "abc" and
    // "def"; each side is just a 3-byte literal that yields a
    // single trigram on its own.
    let got = extract("abc.def");
    let expected: HashSet<[u8; 3]> = [*b"abc", *b"def"].into_iter().collect();
    assert_eq!(got, expected);
}

#[test]
fn prefix_of_anchored_literal_yields_literal_trigrams() {
    let got = extract("^hello$");
    let expected = trigrams_of(b"hello");
    assert_eq!(got, expected);
}

#[test]
fn prefix_of_complex_realistic_regex_yields_useful_trigrams() {
    // The brief's worked example: an anchored error-log
    // pattern. `\w+` is opaque; the literals on either side
    // contribute their own trigrams.
    let got = extract(r"^error: \w+ refused$");
    // "error: " trigrams: err, rro, ror, or:, r: (5)
    // " refused" trigrams: " re", ref, efu, fus, use, sed (6)
    // The leading anchor + trailing anchor break the run on
    // either edge, but the literal runs internally are intact.
    let want_err = trigrams_of(b"error: ");
    let want_ref = trigrams_of(b" refused");
    for t in &want_err {
        assert!(got.contains(t), "missing trigram {t:?} from 'error: '");
    }
    for t in &want_ref {
        assert!(got.contains(t), "missing trigram {t:?} from ' refused'");
    }
}

#[test]
fn prefix_of_concat_with_optional_middle_yields_outer_trigrams() {
    // `errno: (foo)? refused` -- the optional middle group
    // breaks the run, but each outer literal contributes its
    // own trigrams.
    let got = extract(r"errno: (foo)? refused");
    let want_left = trigrams_of(b"errno: ");
    let want_right = trigrams_of(b" refused");
    for t in &want_left {
        assert!(got.contains(t), "missing trigram {t:?} from 'errno: '");
    }
    for t in &want_right {
        assert!(got.contains(t), "missing trigram {t:?} from ' refused'");
    }
    // Trigrams that would span the optional group's interior
    // should NOT be required (foo may not appear).
    assert!(!got.contains(b"foo"));
}

#[test]
fn prefix_of_character_class_breaks_run() {
    // The class breaks the run between the literals, so
    // trigrams cannot span it.
    let got = extract("ab[xy]cd");
    // Each side is too short for a trigram.
    assert!(got.is_empty());
}

#[test]
fn required_set_is_sorted_and_deduplicated() {
    // `aaaa|aaaa` should not produce duplicate trigrams.
    use dyntext::prefix_extract::required_trigrams as rt;
    let ast = parse("aaaa|aaaa").expect("parses");
    let v = rt(&ast);
    let mut sorted = v.clone();
    sorted.sort_unstable();
    assert_eq!(v, sorted, "result must be sorted");
    let mut dedup = sorted.clone();
    dedup.dedup();
    assert_eq!(v.len(), dedup.len(), "result must be deduplicated");
}
