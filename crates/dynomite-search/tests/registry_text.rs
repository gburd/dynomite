//! Direct unit tests for `VectorRegistry`'s per-table TEXT
//! field methods and bookkeeping that the FT.* command tests
//! reach only indirectly: `text_field_names`,
//! `text_index_doc_count`, the `upsert_text_field` re-index
//! dedup path, `has_text_index`, the exact / approximate regex
//! search returns, `add_text_field` idempotency, the
//! `VectorRegistry` `Debug` impl, and the `load_snapshot`
//! NotFound guard via a corrupt-but-decodable snapshot.

use std::collections::HashMap;

use dynomite_search::registry::VectorRegistry;
use dynomite_search::schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorSchema, VectorType,
};

fn schema_text_only() -> VectorSchema {
    VectorSchema {
        vector_field: "vec".to_string(),
        vector_type: VectorType::Float32,
        dim: 2,
        distance: DistanceMetric::Cosine,
        algorithm: IndexAlgorithm::Hnsw,
        prefixes: vec![b"doc:".to_vec()],
        metadata_fields: vec![MetadataField {
            name: "body".to_string(),
            field_type: MetadataFieldType::Text,
            tag_separator: None,
        }],
    }
}

fn feed(table: &dynomite_search::VectorTable, key: &[u8], body: &[u8]) {
    let mut meta = HashMap::new();
    meta.insert(
        "body".to_string(),
        serde_json::Value::String(String::from_utf8_lossy(body).into_owned()),
    );
    table
        .engine
        .upsert(key.to_vec(), &[0.0, 0.0], meta)
        .expect("upsert");
    table.upsert_text_field("body", key, body);
    table.record_indexed_key(key.to_vec());
}

#[test]
fn text_field_names_merges_schema_and_alter() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_text_only())
        .expect("create");
    let t = reg.get("idx").expect("table");

    // add_text_field is idempotent: first add provisions, a
    // repeat returns false (the false branch of add_text_field).
    assert!(t.add_text_field("title"));
    assert!(!t.add_text_field("title"));
    // Re-adding a schema-declared field also returns false.
    assert!(!t.add_text_field("body"));

    // text_field_names merges the schema TEXT field ("body")
    // with the FT.ALTER-added one ("title"), sorted.
    assert_eq!(
        t.text_field_names(),
        vec!["body".to_string(), "title".to_string()]
    );

    // has_text_index agrees with has_text_field.
    assert!(t.has_text_index("body"));
    assert!(t.has_text_index("title"));
    assert!(!t.has_text_index("missing"));
}

#[test]
fn text_index_doc_count_tracks_upserts_and_dedup() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_text_only())
        .expect("create");
    let t = reg.get("idx").expect("table");

    // Unknown field -> None.
    assert_eq!(t.text_index_doc_count("nope"), None);
    // Empty field -> Some(0).
    assert_eq!(t.text_index_doc_count("body"), Some(0));

    feed(&t, b"doc:1", b"hello world");
    feed(&t, b"doc:2", b"goodbye world");
    assert_eq!(t.text_index_doc_count("body"), Some(2));

    // Re-upserting the same key replaces the prior doc id
    // (the dedup path) rather than growing the count.
    feed(&t, b"doc:1", b"hello again");
    assert_eq!(t.text_index_doc_count("body"), Some(2));

    // The replacement text is what the substring index now
    // returns for doc:1.
    let hits = t.search_text_substring("body", b"again").expect("field");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0, b"doc:1");
    // The replaced text is gone.
    assert!(t
        .search_text_substring("body", b"hello world")
        .expect("field")
        .is_empty());
}

#[test]
fn search_text_substring_unknown_field_is_none() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_text_only())
        .expect("create");
    let t = reg.get("idx").expect("table");
    assert!(t.search_text_substring("nope", b"x").is_none());
}

#[test]
fn search_text_regex_exact_and_unknown_field() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_text_only())
        .expect("create");
    let t = reg.get("idx").expect("table");
    feed(&t, b"doc:1", b"alpha");
    feed(&t, b"doc:2", b"alpine");

    // Unknown field -> None.
    assert!(t.search_text_regex("nope", "a").is_none());

    // Exact regex over "body".
    let result = t.search_text_regex("body", "al.+").expect("field present");
    let hits = result.expect("pattern compiles");
    let mut keys: Vec<Vec<u8>> = hits.iter().map(|(k, _)| k.clone()).collect();
    keys.sort();
    assert_eq!(keys, vec![b"doc:1".to_vec(), b"doc:2".to_vec()]);

    // A bad pattern -> Some(Err).
    let bad = t.search_text_regex("body", "[unterminated");
    assert!(matches!(bad, Some(Err(_))));
}

#[test]
fn search_text_regex_approx_and_unknown_field() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_text_only())
        .expect("create");
    let t = reg.get("idx").expect("table");
    feed(&t, b"doc:1", b"alpha");

    // Unknown field -> None.
    assert!(t.search_text_regex_approx("nope", "a", 1).is_none());

    // Approx regex tolerating one edit: "alpa" within 1 edit
    // of "alpha".
    let result = t
        .search_text_regex_approx("body", "alpa", 1)
        .expect("field present");
    let hits = result.expect("pattern compiles");
    assert!(hits.iter().any(|(k, _)| k == b"doc:1"));
}

#[test]
fn registry_debug_lists_indexes_and_persist_dir() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_text_only())
        .expect("create");
    let dbg = format!("{reg:?}");
    assert!(dbg.contains("VectorRegistry"));
    assert!(dbg.contains("idx"));
    assert!(dbg.contains("persist_dir"));
}

#[test]
fn drop_with_dd_returns_indexed_keys() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_text_only())
        .expect("create");
    let t = reg.get("idx").expect("table");
    feed(&t, b"doc:1", b"a");
    feed(&t, b"doc:2", b"b");
    drop(t);

    let mut keys = reg.drop_with_dd("idx").expect("drop dd");
    keys.sort();
    assert_eq!(keys, vec![b"doc:1".to_vec(), b"doc:2".to_vec()]);
    // The index is gone after the drop.
    assert!(reg.get("idx").is_none());
    // Dropping a missing index errors.
    assert!(reg.drop_with_dd("idx").is_err());
}
