//! Integration tests for `dyntext::TextIndex`.

use dyntext::index::TextIndex;

#[test]
fn index_insert_then_search_finds_the_doc() {
    let mut idx = TextIndex::new();
    let id = idx.insert(b"hello world".to_vec());
    assert_eq!(idx.search_substring(b"hello"), vec![id]);
    assert_eq!(idx.search_substring(b"world"), vec![id]);
}

#[test]
fn index_substring_search_no_false_negatives() {
    let corpus: Vec<Vec<u8>> = vec![
        b"the quick brown fox".to_vec(),
        b"jumped over a lazy dog".to_vec(),
        b"another brown fox here".to_vec(),
        b"a totally unrelated phrase".to_vec(),
    ];
    let mut store = TextIndex::new();
    let ids: Vec<u32> = corpus.iter().map(|t| store.insert(t.clone())).collect();
    let queries: &[&[u8]] = &[b"brown", b"fox", b"the", b"a", b"another", b"missing"];
    for q in queries {
        let observed = store.search_substring(q);
        for (i, doc) in corpus.iter().enumerate() {
            let truly_contains =
                !q.is_empty() && doc.len() >= q.len() && doc.windows(q.len()).any(|w| w == *q);
            if truly_contains {
                assert!(
                    observed.contains(&ids[i]),
                    "false negative: query {q:?} should hit {i} ({:?})",
                    String::from_utf8_lossy(doc),
                );
            }
        }
    }
}

#[test]
fn index_substring_search_returns_only_true_positives() {
    let mut idx = TextIndex::new();
    idx.insert(b"first doc with abcdef".to_vec());
    idx.insert(b"second doc with abxdef".to_vec()); // shares the trigrams "abc"-not-quite, "def"
    idx.insert(b"third doc plainly here".to_vec());
    let hits = idx.search_substring(b"abcdef");
    // Only the first doc actually contains the literal substring.
    assert_eq!(hits.len(), 1);
}

#[test]
fn index_remove_excludes_doc_from_subsequent_searches() {
    let mut idx = TextIndex::new();
    let a = idx.insert(b"alpha beta gamma".to_vec());
    let b = idx.insert(b"beta cake".to_vec());
    idx.remove(a);
    let hits = idx.search_substring(b"beta");
    assert_eq!(hits, vec![b]);
}

#[test]
fn index_query_shorter_than_3_chars_returns_via_full_scan() {
    let mut idx = TextIndex::new();
    let a = idx.insert(b"ab".to_vec());
    let b = idx.insert(b"cab".to_vec());
    let _c = idx.insert(b"xyz".to_vec());
    let hits = idx.search_substring(b"ab");
    assert!(hits.contains(&a));
    assert!(hits.contains(&b));
    assert_eq!(hits.len(), 2);
}

#[test]
fn index_unicode_query_byte_level_works() {
    let mut idx = TextIndex::new();
    let a = idx.insert(b"naive caf\xc3\xa9".to_vec());
    let b = idx.insert(b"naive cafe".to_vec());
    let hits = idx.search_substring(b"\xc3\xa9");
    assert_eq!(hits, vec![a]);
    let hits = idx.search_substring(b"naive");
    assert!(hits.contains(&a));
    assert!(hits.contains(&b));
}

#[test]
fn index_returns_results_in_insertion_order() {
    let mut idx = TextIndex::new();
    let a = idx.insert(b"hello a".to_vec());
    let b = idx.insert(b"hello b".to_vec());
    let c = idx.insert(b"hello c".to_vec());
    assert_eq!(idx.search_substring(b"hello"), vec![a, b, c]);
}

#[test]
fn index_search_empty_query_returns_all_docs_in_insertion_order() {
    let mut idx = TextIndex::new();
    let a = idx.insert(b"abc".to_vec());
    let b = idx.insert(b"def".to_vec());
    assert_eq!(idx.search_substring(b""), vec![a, b]);
}
