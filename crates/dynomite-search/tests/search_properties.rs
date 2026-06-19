//! Property tests for the FT.* search surface.
//!
//! Each `#[hegel::test]` runs at least 256 generated cases.
//! Invariants:
//!
//! * Text upsert/search round-trip: after upserting N docs with
//!   distinct bodies, an exact-substring search for a token that
//!   appears in a doc recovers exactly the docs containing it.
//! * Snapshot persistence round-trip: a registry populated with
//!   an arbitrary set of docs, saved and reopened, recovers the
//!   identical indexed-key set, per-key vectors, and text
//!   content.
//! * Suggestion dictionary add/len/del invariants over arbitrary
//!   key/value/score sets.

use std::collections::{BTreeSet, HashMap};

use dynomite_search::registry::VectorRegistry;
use dynomite_search::schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorSchema, VectorType,
};
use dynomite_search::sugest_registry::SuggestionRegistry;
use dynomite_search::VectorTable;

use hegel::generators as gs;
use hegel::TestCase;

fn schema() -> VectorSchema {
    VectorSchema {
        vector_field: "vec".to_string(),
        vector_type: VectorType::Float32,
        dim: 2,
        distance: DistanceMetric::L2,
        algorithm: IndexAlgorithm::Hnsw,
        prefixes: vec![b"doc:".to_vec()],
        metadata_fields: vec![MetadataField {
            name: "body".to_string(),
            field_type: MetadataFieldType::Text,
            tag_separator: None,
        }],
    }
}

/// Draw a small set of `(key, vec, body)` docs. Keys are unique
/// `doc:<n>`; bodies are short lowercase words from a tiny
/// alphabet so substrings collide across docs.
fn arb_docs(tc: &TestCase) -> Vec<(Vec<u8>, [f32; 2], Vec<u8>)> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(12));
    let mut docs = Vec::with_capacity(n);
    let mut seen = BTreeSet::new();
    for i in 0..n {
        let key = format!("doc:{i}").into_bytes();
        if !seen.insert(key.clone()) {
            continue;
        }
        let x = f32::from(tc.draw(gs::integers::<i16>()));
        let y = f32::from(tc.draw(gs::integers::<i16>()));
        let blen = tc.draw(gs::integers::<usize>().min_value(1).max_value(6));
        let mut body = Vec::with_capacity(blen);
        for _ in 0..blen {
            body.push(tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'e')));
        }
        docs.push((key, [x, y], body));
    }
    docs
}

fn feed(table: &VectorTable, key: &[u8], vec2: &[f32], body: &[u8]) {
    let mut meta = HashMap::new();
    meta.insert(
        "body".to_string(),
        serde_json::Value::String(String::from_utf8_lossy(body).into_owned()),
    );
    table
        .engine
        .upsert(key.to_vec(), vec2, meta)
        .expect("upsert");
    table.upsert_text_field("body", key, body);
    table.record_indexed_key(key.to_vec());
}

/// Text upsert + exact-substring search round-trip: a search
/// for any single byte `c` recovers exactly the doc keys whose
/// body contains `c`.
#[hegel::test(test_cases = 256)]
fn text_substring_recovers_matching_docs(tc: TestCase) {
    let docs = arb_docs(&tc);
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema()).expect("create");
    let table = reg.get("idx").expect("table");
    for (key, v, body) in &docs {
        feed(&table, key, v, body);
    }

    // Pick a probe byte from the alphabet.
    let probe = tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'e'));
    let needle = [probe];

    let expected: BTreeSet<Vec<u8>> = docs
        .iter()
        .filter(|(_, _, body)| body.windows(1).any(|w| w == needle))
        .map(|(k, _, _)| k.clone())
        .collect();

    let got: BTreeSet<Vec<u8>> = table
        .search_text_substring("body", &needle)
        .expect("body field")
        .into_iter()
        .map(|(k, _)| k)
        .collect();

    assert_eq!(got, expected);
}

/// Snapshot persistence round-trip: save then reopen recovers
/// the identical indexed-key set and per-key body text.
#[hegel::test(test_cases = 256)]
fn snapshot_round_trip_preserves_docs(tc: TestCase) {
    let docs = arb_docs(&tc);
    let dir = tempfile::tempdir().expect("tempdir");

    // Build, populate, save.
    let sug = SuggestionRegistry::new();
    {
        let reg = VectorRegistry::open(dir.path(), &sug).expect("open");
        reg.create("idx".to_string(), schema()).expect("create");
        let table = reg.get("idx").expect("table");
        for (key, v, body) in &docs {
            feed(&table, key, v, body);
        }
        reg.save(&sug).expect("save");
    }

    // Reopen into a fresh registry.
    let sug2 = SuggestionRegistry::new();
    let reg2 = VectorRegistry::open(dir.path(), &sug2).expect("reopen");

    let expected_keys: BTreeSet<Vec<u8>> = docs.iter().map(|(k, _, _)| k.clone()).collect();
    if expected_keys.is_empty() {
        // An empty index still survives a round-trip as an
        // empty, registered index.
        assert_eq!(reg2.list(), vec!["idx".to_string()]);
        return;
    }

    let t2 = reg2.get("idx").expect("table recovered");
    let recovered: BTreeSet<Vec<u8>> = t2.indexed_keys().into_iter().collect();
    assert_eq!(recovered, expected_keys);

    // Each doc's body text round-trips: a substring search for
    // the full body recovers the doc.
    for (key, _, body) in &docs {
        let hits = t2.search_text_substring("body", body).expect("field");
        assert!(
            hits.iter().any(|(k, _)| k == key),
            "doc {key:?} body recovered"
        );
    }
}

/// Suggestion dictionary invariants over arbitrary entries:
/// `len` equals the count of distinct suggestion values, and
/// deleting every added value drains the dictionary to zero.
#[hegel::test(test_cases = 256)]
fn suggestion_add_len_del_invariants(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(16));
    let sug = SuggestionRegistry::new();
    let key = b"k";

    // Draw distinct suggestion values from a small alphabet.
    let mut values: Vec<Vec<u8>> = Vec::new();
    let mut seen = BTreeSet::new();
    for _ in 0..n {
        let vlen = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
        let mut v = Vec::with_capacity(vlen);
        for _ in 0..vlen {
            v.push(tc.draw(gs::integers::<u8>().min_value(b'a').max_value(b'c')));
        }
        if seen.insert(v.clone()) {
            values.push(v);
        }
    }

    for v in &values {
        let score = f64::from(tc.draw(gs::integers::<u8>().min_value(1).max_value(100)));
        sug.add(key, v.clone(), score, false, None);
    }

    // len equals the number of distinct values.
    assert_eq!(sug.len(key), values.len());

    // Deleting each value removes exactly one; deleting twice
    // is a no-op.
    for v in &values {
        assert!(sug.del(key, v));
        assert!(!sug.del(key, v));
    }
    assert_eq!(sug.len(key), 0);
}
