//! Integration tests for `TextIndex::search_regex_approx`.

use dyntext::TextIndex;

fn build_corpus() -> (TextIndex, Vec<u32>) {
    let mut idx = TextIndex::new();
    let doc_ids = vec![
        idx.insert(b"errno: connection refused".to_vec()),
        idx.insert(b"errno: connetion refused".to_vec()), // 1 deletion
        idx.insert(b"errnno: connection refused".to_vec()), // 1 insertion
        idx.insert(b"system: connection accepted".to_vec()),
        idx.insert(b"completely unrelated message".to_vec()),
    ];
    (idx, doc_ids)
}

#[test]
fn search_regex_approx_finds_documents_with_typos() {
    let (idx, doc_ids) = build_corpus();
    let hits = idx
        .search_regex_approx(r"errno: connection refused", 1)
        .expect("regex compiles");

    // Doc 0 is the exact match; docs 1 and 2 are within 1 edit
    // of the pattern; docs 3 and 4 are not. We assert
    // membership rather than exact equality because TRE may
    // legitimately match the substring in docs that share a
    // prefix.
    assert!(hits.contains(&doc_ids[0]), "exact match must be present");
    assert!(hits.contains(&doc_ids[1]), "1 deletion must be present");
    assert!(hits.contains(&doc_ids[2]), "1 insertion must be present");
    assert!(
        !hits.contains(&doc_ids[3]),
        "non-matching doc must be absent"
    );
    assert!(!hits.contains(&doc_ids[4]), "unrelated doc must be absent");
}

#[test]
fn search_regex_approx_zero_errors_matches_exact_only() {
    let (idx, doc_ids) = build_corpus();
    let hits = idx
        .search_regex_approx(r"errno: connection refused", 0)
        .expect("regex compiles");
    assert_eq!(hits, vec![doc_ids[0]]);
}

#[test]
fn search_regex_approx_returns_results_in_doc_id_order() {
    let mut idx = TextIndex::new();
    let first = idx.insert(b"hello world".to_vec());
    let second = idx.insert(b"hellp world".to_vec()); // 1 substitution
    let third = idx.insert(b"goodbye world".to_vec());
    let _ = third;
    let hits = idx
        .search_regex_approx(r"hello", 1)
        .expect("regex compiles");
    assert_eq!(hits, vec![first, second]);
}

#[test]
fn search_regex_approx_invalid_pattern_returns_error() {
    let (idx, _) = build_corpus();
    let err = idx
        .search_regex_approx("[unbalanced", 1)
        .expect_err("invalid regex must fail to compile");
    let s = format!("{err}");
    assert!(s.contains("compile"), "error display includes context: {s}");
}

#[test]
fn search_regex_approx_on_empty_index_is_ok_and_empty() {
    let idx = TextIndex::new();
    let hits = idx.search_regex_approx(r"foo", 0).expect("regex compiles");
    assert!(hits.is_empty());
}
