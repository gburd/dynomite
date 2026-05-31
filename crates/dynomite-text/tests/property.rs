//! Property tests for the dyntext index.
//!
//! Each `#[hegel::test]` runs at least 256 generated cases under
//! the default profile.

use std::collections::HashSet;

use dyntext::index::TextIndex;
use hegel::generators as gs;
use hegel::TestCase;

/// Generate a random text byte string.
///
/// Bytes are drawn from a tiny alphabet so duplicates,
/// substring overlaps, and trigram collisions all exercise the
/// data path. The empty string is allowed.
fn arb_text(tc: &TestCase) -> Vec<u8> {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(40));
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'd'));
        out.push(c);
    }
    out
}

/// Generate a random small corpus of text byte strings.
fn arb_corpus(tc: &TestCase) -> Vec<Vec<u8>> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(arb_text(tc));
    }
    out
}

/// Generate a random query byte string. Allowed to be empty
/// or longer than any corpus document.
fn arb_query(tc: &TestCase) -> Vec<u8> {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'd'));
        out.push(c);
    }
    out
}

/// True positives must always be returned: every doc that truly
/// contains the query as a byte substring must appear in the
/// search result.
#[hegel::test(test_cases = 256)]
fn substring_search_no_false_negatives_under_arbitrary_corpus(tc: TestCase) {
    let corpus = arb_corpus(&tc);
    let query = arb_query(&tc);

    let mut idx = TextIndex::new();
    let ids: Vec<u32> = corpus.iter().map(|t| idx.insert(t.clone())).collect();

    let observed: HashSet<u32> = idx.search_substring(&query).into_iter().collect();
    let expected: HashSet<u32> = corpus
        .iter()
        .enumerate()
        .filter(|(_, t)| substring_match(t, &query))
        .map(|(i, _)| ids[i])
        .collect();

    assert!(
        expected.is_subset(&observed),
        "true positives missed: expected={expected:?} observed={observed:?} \
         corpus={corpus:?} query={query:?}",
    );
}

/// Every doc returned must actually contain the query.
#[hegel::test(test_cases = 256)]
fn substring_search_returns_only_true_positives(tc: TestCase) {
    let corpus = arb_corpus(&tc);
    let query = arb_query(&tc);

    let mut idx = TextIndex::new();
    let ids: Vec<u32> = corpus.iter().map(|t| idx.insert(t.clone())).collect();

    for got in idx.search_substring(&query) {
        let pos = ids.iter().position(|id| *id == got).expect("id known");
        let doc = &corpus[pos];
        assert!(
            substring_match(doc, &query),
            "false positive: doc {doc:?} returned for query {query:?}",
        );
    }
}

/// Insert-then-remove restores the search behaviour as if the
/// removed doc had never been inserted.
#[hegel::test(test_cases = 256)]
fn insert_remove_round_trip(tc: TestCase) {
    let corpus = arb_corpus(&tc);
    let query = arb_query(&tc);
    if corpus.is_empty() {
        return;
    }

    let drop_idx = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(corpus.len() - 1),
    );

    // Index without the dropped doc.
    let mut without = TextIndex::new();
    let mut ids_without = Vec::new();
    for (i, t) in corpus.iter().enumerate() {
        if i == drop_idx {
            continue;
        }
        ids_without.push(without.insert(t.clone()));
    }

    // Index with the dropped doc, then remove it.
    let mut with = TextIndex::new();
    let mut all_ids = Vec::new();
    for t in &corpus {
        all_ids.push(with.insert(t.clone()));
    }
    let dropped_id = all_ids[drop_idx];
    with.remove(dropped_id);

    let r1: HashSet<u32> = without.search_substring(&query).into_iter().collect();
    let r2: HashSet<u32> = with.search_substring(&query).into_iter().collect();
    // Doc ids differ (insertion order changes); compare on
    // matched-doc set by reading the corpus through each index.
    let m1: HashSet<&[u8]> = r1
        .iter()
        .map(|id| {
            let pos = ids_without.iter().position(|i| i == id).unwrap();
            // map id back to the corpus entry, skipping the
            // dropped doc.
            let mut k = 0;
            for (i, t) in corpus.iter().enumerate() {
                if i == drop_idx {
                    continue;
                }
                if k == pos {
                    return t.as_slice();
                }
                k += 1;
            }
            panic!("id mapping failed");
        })
        .collect();
    let m2: HashSet<&[u8]> = r2
        .iter()
        .map(|id| {
            let pos = all_ids.iter().position(|i| i == id).unwrap();
            corpus[pos].as_slice()
        })
        .collect();
    assert_eq!(m1, m2);
}

/// Reference substring matcher used to compute the expected
/// answer.
fn substring_match(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}
