//! Round-trip persistence tests for [`dyntext::persist`].
//!
//! These tests verify that snapshotting and reloading a
//! [`TextIndex`] preserves its observable behaviour: the
//! reloaded index must return the same doc ids for any
//! substring query that the in-memory index would have
//! returned.
//!
//! The integration test is gated on the `noxu` feature so the
//! crate still builds without the optional Noxu dependency.

#![cfg(feature = "noxu")]

use std::collections::HashSet;

use dyntext::index::TextIndex;
use dyntext::persist::{NoxuPersister, PersistConfig};
use hegel::generators as gs;
use hegel::TestCase;
use tempfile::TempDir;

/// Open a fresh persister rooted at `dir`. Each test gets its
/// own temporary directory so the on-disk environments do not
/// share state across tests.
fn fresh_persister(dir: &TempDir) -> NoxuPersister {
    let cfg = PersistConfig {
        env_path: dir.path().to_path_buf(),
        ..PersistConfig::default()
    };
    NoxuPersister::open(cfg).expect("open persister")
}

/// Reference substring matcher for cross-checking the index
/// returns true positives only.
fn substring_match(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if needle.len() > haystack.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[test]
fn snapshot_then_load_preserves_doc_count() {
    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);

    let mut idx = TextIndex::new();
    let corpus: &[&[u8]] = &[
        b"alpha beta gamma",
        b"the quick brown fox",
        b"another brown fox here",
        b"omega only",
    ];
    for text in corpus {
        idx.insert(text.to_vec());
    }
    p.snapshot(&idx).expect("snapshot");

    let restored = p.load().expect("load");
    assert_eq!(restored.doc_count(), idx.doc_count());
    assert_eq!(restored.doc_count(), corpus.len());
}

#[test]
fn snapshot_then_load_preserves_search_results() {
    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);

    let mut idx = TextIndex::new();
    let id_a = idx.insert(b"the quick brown fox".to_vec());
    let id_b = idx.insert(b"jumped over a lazy dog".to_vec());
    let id_c = idx.insert(b"another brown fox here".to_vec());
    let id_d = idx.insert(b"unique-string-only-here".to_vec());

    p.snapshot(&idx).expect("snapshot");
    let restored = p.load().expect("load");

    for query in [
        b"brown fox".as_slice(),
        b"lazy",
        b"unique-string",
        b"completely-absent",
        b"the",
    ] {
        let original: Vec<u32> = idx.search_substring(query);
        let after_load: Vec<u32> = restored.search_substring(query);
        assert_eq!(original, after_load, "search drift for query {query:?}");
    }

    // Spot-check the doc ids assigned by the restore path.
    assert!(restored.search_substring(b"brown fox").contains(&id_a));
    assert!(restored.search_substring(b"brown fox").contains(&id_c));
    assert!(restored.search_substring(b"lazy").contains(&id_b));
    assert!(restored.search_substring(b"unique-string").contains(&id_d));
}

#[test]
fn append_doc_incremental_matches_full_snapshot() {
    let dir_full = TempDir::new().expect("tempdir full");
    let dir_inc = TempDir::new().expect("tempdir incremental");

    // Reference: full snapshot of the complete corpus.
    let mut idx_full = TextIndex::new();
    let corpus: &[&[u8]] = &[
        b"first doc with hello",
        b"second doc with world",
        b"third doc has hello world",
        b"fourth doc says nothing",
        b"fifth doc says hello again",
    ];
    let mut full_ids = Vec::with_capacity(corpus.len());
    for text in corpus {
        full_ids.push(idx_full.insert(text.to_vec()));
    }
    let p_full = fresh_persister(&dir_full);
    p_full.snapshot(&idx_full).expect("snapshot full");
    let restored_full = p_full.load().expect("load full");

    // Incremental: snapshot a prefix, then append the rest one
    // at a time. The persister must end up in a state that is
    // observationally equivalent to the full-snapshot path.
    let mut idx_inc = TextIndex::new();
    let mut inc_ids = Vec::with_capacity(corpus.len());
    for text in &corpus[..2] {
        inc_ids.push(idx_inc.insert(text.to_vec()));
    }
    let p_inc = fresh_persister(&dir_inc);
    p_inc.snapshot(&idx_inc).expect("snapshot prefix");
    for text in &corpus[2..] {
        let id = idx_inc.insert(text.to_vec());
        inc_ids.push(id);
        p_inc.append_doc(id, &idx_inc).expect("append incremental");
    }
    let restored_inc = p_inc.load().expect("load incremental");

    assert_eq!(restored_full.doc_count(), restored_inc.doc_count());
    assert_eq!(full_ids, inc_ids);

    for query in [
        b"hello".as_slice(),
        b"world",
        b"hello world",
        b"nothing",
        b"absent",
    ] {
        let from_full = restored_full.search_substring(query);
        let from_inc = restored_inc.search_substring(query);
        assert_eq!(from_full, from_inc, "drift for query {query:?}");
    }
}

#[test]
fn load_from_empty_env_returns_empty_index() {
    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);
    let idx = p.load().expect("load empty");
    assert_eq!(idx.doc_count(), 0);
    assert!(idx.search_substring(b"anything").is_empty());
}

#[test]
fn snapshot_overwrites_previous_state() {
    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);

    // First snapshot: docs A and B.
    let mut idx = TextIndex::new();
    idx.insert(b"first hello".to_vec());
    idx.insert(b"second world".to_vec());
    p.snapshot(&idx).expect("snapshot 1");
    assert_eq!(p.load().expect("load 1").doc_count(), 2);

    // Second snapshot: docs A only. The on-disk state must
    // not still contain doc B's records.
    let mut idx2 = TextIndex::new();
    idx2.insert(b"first hello".to_vec());
    p.snapshot(&idx2).expect("snapshot 2");
    let restored = p.load().expect("load 2");
    assert_eq!(restored.doc_count(), 1);
    assert!(restored.search_substring(b"world").is_empty());
    assert!(!restored.search_substring(b"hello").is_empty());
}

#[test]
fn snapshot_then_load_preserves_unicode_payload_bytes() {
    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);

    let mut idx = TextIndex::new();
    idx.insert(b"caf\xc3\xa9 noir".to_vec());
    idx.insert(b"plain cafe noir".to_vec());
    p.snapshot(&idx).expect("snapshot");

    let restored = p.load().expect("load");
    let q: &[u8] = b"\xc3\xa9";
    let original = idx.search_substring(q);
    let after_load = restored.search_substring(q);
    assert_eq!(original, after_load);
    assert_eq!(after_load.len(), 1);
}

#[test]
fn persister_is_clone_safe_across_handles() {
    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);
    let p2 = p.clone();

    let mut idx = TextIndex::new();
    idx.insert(b"shared corpus doc one".to_vec());
    p.snapshot(&idx).expect("snapshot through handle 1");

    let restored = p2.load().expect("load through handle 2");
    assert_eq!(restored.doc_count(), 1);
    assert_eq!(
        idx.search_substring(b"shared"),
        restored.search_substring(b"shared")
    );
}

#[test]
fn append_doc_preserves_round_trip_for_single_doc() {
    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);

    // Snapshot an empty index, then append a single doc.
    let mut idx = TextIndex::new();
    p.snapshot(&idx).expect("snapshot empty");
    let id = idx.insert(b"hello via append".to_vec());
    p.append_doc(id, &idx).expect("append");

    let restored = p.load().expect("load");
    assert_eq!(restored.doc_count(), 1);
    assert_eq!(restored.search_substring(b"hello"), vec![id]);
}

// ---- property tests ----

/// Generate a small random doc text drawn from a tight
/// alphabet so duplicates and trigram collisions occur.
fn arb_text(tc: &TestCase) -> Vec<u8> {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(20));
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'd'));
        out.push(c);
    }
    out
}

/// Generate a small corpus of docs.
fn arb_corpus(tc: &TestCase) -> Vec<Vec<u8>> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(6));
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(arb_text(tc));
    }
    out
}

/// Generate a substring query.
fn arb_query(tc: &TestCase) -> Vec<u8> {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(6));
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        let c = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'd'));
        out.push(c);
    }
    out
}

/// For any corpus and any substring query, the snapshot +
/// load + search_substring path returns the same hits as the
/// in-memory search_substring path.
#[hegel::test(test_cases = 64)]
fn snapshot_load_search_matches_in_memory(tc: TestCase) {
    let corpus = arb_corpus(&tc);
    let query = arb_query(&tc);

    let mut idx = TextIndex::new();
    let mut ids = Vec::with_capacity(corpus.len());
    for t in &corpus {
        ids.push(idx.insert(t.clone()));
    }

    let dir = TempDir::new().expect("tempdir");
    let p = fresh_persister(&dir);
    p.snapshot(&idx).expect("snapshot");
    let restored = p.load().expect("load");

    let from_mem: Vec<u32> = idx.search_substring(&query);
    let from_disk: Vec<u32> = restored.search_substring(&query);
    assert_eq!(
        from_mem, from_disk,
        "search drift: corpus={corpus:?} query={query:?}",
    );

    // Spot-check that the hits are real: every doc returned
    // really does contain the query.
    let observed: HashSet<u32> = from_disk.into_iter().collect();
    for got in &observed {
        let pos = ids.iter().position(|id| id == got).expect("id known");
        let doc = &corpus[pos];
        assert!(
            substring_match(doc, &query),
            "false positive after load: doc {doc:?} for query {query:?}",
        );
    }
}
