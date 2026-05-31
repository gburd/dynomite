//! Property tests for the regex-driven trigram extraction.
//!
//! Two soundness invariants are exercised:
//!
//! * `required_trigrams_are_present_in_every_match`: every
//!   trigram returned by [`dyntext::required_trigrams`] for a
//!   pattern must appear in any string that the pattern
//!   matches. This is the K=0 soundness property: the filter
//!   must never reject a true positive.
//!
//! * `approx_filter_passes_every_approximate_match`: every
//!   doc that approximately matches the pattern within the
//!   configured edit budget must pass the
//!   [`dyntext::ApproxFilter`] returned by
//!   [`dyntext::ApproxFilter::build`]. This is the K>=1
//!   soundness property: the filter must never reject a true
//!   positive even when edits destroy some pattern trigrams.
//!
//! Both tests use Hegel's `#[hegel::test]` shrinker so a
//! counterexample is reported in minimal form.

use std::collections::HashSet;

use dyntext::regex_ast::parse;
use dyntext::trigram::{extract_trigram_set, hash_trigram};
use dyntext::{required_trigrams, ApproxFilter, BloomFilter, TextIndex};
use hegel::generators as gs;
use hegel::TestCase;

/// Tiny ASCII alphabet so collisions between trigrams are
/// frequent enough to make the test interesting.
fn arb_text(tc: &TestCase) -> Vec<u8> {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(20));
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'd'));
        out.push(c);
    }
    out
}

/// A small set of supported regex patterns over the same
/// alphabet, plus a few featuring alternation, anchors,
/// repetition, and character classes.
fn arb_pattern(tc: &TestCase) -> String {
    let kind = tc.draw(gs::integers::<u8>().min_value(0).max_value(13));
    match kind {
        0 => "abc".to_string(),
        1 => "abcd".to_string(),
        2 => "abcde".to_string(),
        3 => "a.c".to_string(),
        4 => "ab|cd".to_string(),
        5 => "(ab)+".to_string(),
        6 => "(ab)?".to_string(),
        7 => "^abc$".to_string(),
        8 => "a*b".to_string(),
        9 => "(abc|abd)e".to_string(),
        10 => "pre(ab|cd)".to_string(),
        11 => "(ab|cd)pre".to_string(),
        12 => "a{3}".to_string(),
        _ => "abcd|cdab".to_string(),
    }
}

#[hegel::test(test_cases = 256)]
fn required_trigrams_are_present_in_every_match(tc: TestCase) {
    let pattern = arb_pattern(&tc);
    let text = arb_text(&tc);

    let oracle = regex::bytes::Regex::new(&pattern).expect("oracle compiles");
    if !oracle.is_match(&text) {
        return; // not a counterexample candidate
    }

    let Ok(ast) = parse(&pattern) else {
        return;
    };
    let req = required_trigrams(&ast);
    let doc_set: HashSet<u64> = extract_trigram_set(&text).into_iter().collect();
    for t in &req {
        let h = hash_trigram(t);
        assert!(
            doc_set.contains(&h),
            "soundness violation: pattern={pattern:?} required {t:?} \
             but matching text {text:?} does not contain it",
        );
    }
}

#[hegel::test(test_cases = 256)]
fn approx_filter_passes_every_approximate_match(tc: TestCase) {
    let pattern = arb_pattern(&tc);
    let text = arb_text(&tc);
    let k = tc.draw(gs::integers::<u16>().min_value(0).max_value(2));

    // Use TRE as the oracle for approximate matching since it
    // is the engine the index actually delegates to.
    let opts = dyntext::TreMatchOpts {
        max_errors: k,
        ..dyntext::TreMatchOpts::default()
    };
    let Ok(pat) = dyntext::TreCompiledPattern::compile(pattern.as_bytes(), opts) else {
        return;
    };
    if !pat.is_match(&text) {
        return; // doc does not approx-match -> filter is free
    }

    let Ok(ast) = parse(&pattern) else {
        return;
    };
    let filter = ApproxFilter::build(&ast, k);

    // Build a per-doc bloom filter from the text's trigrams,
    // matching how `TextIndex` populates it.
    let tris = extract_trigram_set(&text);
    let mut bloom = BloomFilter::with_size_and_fp_rate(256.max(tris.len()), 0.01);
    for t in &tris {
        bloom.insert(&t.to_le_bytes());
    }

    // The filter must accept this text -- it approximately
    // matches, so any sound filter is required to pass it.
    assert!(
        filter.passes(&bloom),
        "soundness violation: pattern={pattern:?} k={k} text={text:?} \
         approx-matches but ApproxFilter rejects it",
    );
}

#[hegel::test(test_cases = 128)]
fn search_regex_approx_finds_every_oracle_match(tc: TestCase) {
    // End-to-end soundness: build a small index, query it via
    // search_regex_approx, and confirm every doc the TRE oracle
    // approves shows up in the result.
    let pattern = arb_pattern(&tc);
    let n_docs = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    let mut texts = Vec::with_capacity(n_docs);
    for _ in 0..n_docs {
        texts.push(arb_text(&tc));
    }
    let k = tc.draw(gs::integers::<u16>().min_value(0).max_value(2));

    let opts = dyntext::TreMatchOpts {
        max_errors: k,
        ..dyntext::TreMatchOpts::default()
    };
    let Ok(oracle) = dyntext::TreCompiledPattern::compile(pattern.as_bytes(), opts) else {
        return;
    };

    let mut idx = TextIndex::new();
    let ids: Vec<u32> = texts.iter().map(|t| idx.insert(t.clone())).collect();
    let observed: HashSet<u32> = match idx.search_regex_approx(&pattern, k) {
        Ok(v) => v.into_iter().collect(),
        Err(_) => return,
    };
    let expected: HashSet<u32> = texts
        .iter()
        .enumerate()
        .filter(|(_, t)| oracle.is_match(t))
        .map(|(i, _)| ids[i])
        .collect();
    assert!(
        expected.is_subset(&observed),
        "false negative: pattern={pattern:?} k={k} expected={expected:?} observed={observed:?} \
         texts={texts:?}",
    );
}
