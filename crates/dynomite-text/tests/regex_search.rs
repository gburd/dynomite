//! Integration tests for [`dyntext::index::TextIndex::search_regex`].
//!
//! End-to-end tests that drive a populated index with regex
//! queries and compare the results against the regex's actual
//! match set on the corpus.

use std::collections::HashSet;

use dyntext::index::TextIndex;
use dyntext::regex_ast::RegexError;

fn build_index(corpus: &[&[u8]]) -> (TextIndex, Vec<u32>) {
    let mut index = TextIndex::new();
    let ids: Vec<u32> = corpus.iter().map(|t| index.insert(t.to_vec())).collect();
    (index, ids)
}

#[test]
fn regex_search_finds_docs_matching_simple_anchored_pattern() {
    let corpus: &[&[u8]] = &[
        b"error: connection refused",
        b"info: connection accepted",
        b"error: timeout",
        b"debug: hello",
    ];
    let (index, ids) = build_index(corpus);

    let hits = index.search_regex("^error:").expect("matches");
    let observed: HashSet<u32> = hits.into_iter().collect();
    let expected: HashSet<u32> = [ids[0], ids[2]].into_iter().collect();
    assert_eq!(observed, expected);
}

#[test]
fn regex_search_handles_alternation() {
    let corpus: &[&[u8]] = &[
        b"the quick brown fox",
        b"the lazy dog",
        b"a brown bear",
        b"a green fox",
    ];
    let (index, ids) = build_index(corpus);

    // Matches docs containing "fox" OR "bear".
    let hits = index.search_regex("fox|bear").expect("matches");
    let observed: HashSet<u32> = hits.into_iter().collect();
    let expected: HashSet<u32> = [ids[0], ids[2], ids[3]].into_iter().collect();
    assert_eq!(observed, expected);
}

#[test]
fn regex_search_returns_empty_for_no_matches() {
    let corpus: &[&[u8]] = &[b"hello world", b"foo bar baz"];
    let (index, _) = build_index(corpus);

    let hits = index
        .search_regex("nonexistent_pattern_xyzzy")
        .expect("matches");
    assert!(hits.is_empty());
}

#[test]
fn regex_search_returns_empty_for_empty_index() {
    let index = TextIndex::new();
    let hits = index.search_regex("anything").expect("matches");
    assert!(hits.is_empty());
}

#[test]
fn regex_search_with_dot_star_falls_back_to_full_scan() {
    // `.*` has no required trigrams; the path still works via
    // full-scan + recheck.
    let corpus: &[&[u8]] = &[b"foo", b"bar", b"baz"];
    let (index, ids) = build_index(corpus);

    let hits = index.search_regex(".*").expect("matches");
    let observed: HashSet<u32> = hits.into_iter().collect();
    let expected: HashSet<u32> = ids.iter().copied().collect();
    assert_eq!(observed, expected);
}

#[test]
fn regex_search_falls_back_to_full_scan_on_unsupported_pattern() {
    // Named capture groups trip the prefix extractor, but the
    // search still runs by falling back to a full scan.
    let corpus: &[&[u8]] = &[b"hello there", b"goodbye there", b"unrelated"];
    let (index, ids) = build_index(corpus);

    let hits = index
        .search_regex("(?P<greet>hello|goodbye)")
        .expect("matches via fallback");
    let observed: HashSet<u32> = hits.into_iter().collect();
    let expected: HashSet<u32> = [ids[0], ids[1]].into_iter().collect();
    assert_eq!(observed, expected);
}

#[test]
fn regex_search_invalid_pattern_returns_error() {
    let index = TextIndex::new();
    let err = index
        .search_regex("[unclosed")
        .expect_err("invalid regex must error");
    assert!(matches!(err, RegexError::Parse(_)));
}

#[test]
fn regex_search_results_in_insertion_order() {
    let corpus: &[&[u8]] = &[b"alpha brown fox", b"beta brown fox", b"gamma brown fox"];
    let (index, ids) = build_index(corpus);

    let hits = index.search_regex("brown fox").expect("matches");
    assert_eq!(hits, ids);
}

#[test]
fn regex_search_with_anchors_and_class() {
    let corpus: &[&[u8]] = &[
        b"errno: 13 refused",
        b"error: 200 ok",
        b"errno: timeout refused",
        b"unrelated",
    ];
    let (index, ids) = build_index(corpus);

    let hits = index
        .search_regex(r"^errno: \w+ refused$")
        .expect("matches");
    let observed: HashSet<u32> = hits.into_iter().collect();
    let expected: HashSet<u32> = [ids[0], ids[2]].into_iter().collect();
    assert_eq!(observed, expected);
}

#[test]
fn regex_search_byte_level_pattern_matches_non_utf8_doc() {
    // A pattern over bytes can match arbitrary byte payloads.
    let mut index = TextIndex::new();
    let a = index.insert(b"caf\xc3\xa9 noir".to_vec());
    let b = index.insert(b"cafe noir".to_vec());

    let hits = index.search_regex("caf").expect("matches");
    let observed: HashSet<u32> = hits.into_iter().collect();
    assert!(observed.contains(&a));
    assert!(observed.contains(&b));
}

// ---- Property test ----

mod property {
    use super::*;
    use hegel::generators as gs;
    use hegel::TestCase;

    /// Generate a random text byte string from a tiny ASCII
    /// alphabet so the trigram space stays small.
    fn arb_text(tc: &TestCase) -> Vec<u8> {
        let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(20));
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            // Alphabet "abc": 3 symbols.
            let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'c'));
            out.push(c);
        }
        out
    }

    fn arb_corpus(tc: &TestCase) -> Vec<Vec<u8>> {
        let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            out.push(arb_text(tc));
        }
        out
    }

    /// Build a regex that we know is in the supported subset:
    /// pick from a small set of pattern templates over the
    /// "abc" alphabet.
    fn arb_pattern(tc: &TestCase) -> String {
        let kind = tc.draw(gs::integers::<u8>().min_value(0).max_value(8));
        match kind {
            0 => "a".to_string(),
            1 => "ab".to_string(),
            2 => "abc".to_string(),
            3 => "a.c".to_string(),
            4 => "ab|bc".to_string(),
            5 => "(ab)+".to_string(),
            6 => "(ab)?".to_string(),
            7 => "^abc$".to_string(),
            _ => "a*b".to_string(),
        }
    }

    /// `search_regex` must agree with the oracle
    /// `regex::bytes::Regex::is_match` on every corpus.
    #[hegel::test(test_cases = 256)]
    fn regex_search_agrees_with_oracle_on_supported_subset(tc: TestCase) {
        let corpus = arb_corpus(&tc);
        let pattern = arb_pattern(&tc);

        let oracle = regex::bytes::Regex::new(&pattern).expect("oracle compiles");

        let mut index = TextIndex::new();
        let ids: Vec<u32> = corpus.iter().map(|t| index.insert(t.clone())).collect();

        let observed: HashSet<u32> = index
            .search_regex(&pattern)
            .expect("supported pattern")
            .into_iter()
            .collect();
        let expected: HashSet<u32> = corpus
            .iter()
            .enumerate()
            .filter(|(_, t)| oracle.is_match(t))
            .map(|(i, _)| ids[i])
            .collect();

        assert_eq!(observed, expected, "pattern={pattern:?} corpus={corpus:?}");
    }
}
