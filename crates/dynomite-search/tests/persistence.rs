//! Snapshot persistence round-trip tests for the FT.* search
//! registry.
//!
//! Covers the durable-mode contract: a registry built with a
//! persistence directory snapshots its index definitions,
//! indexed documents, text fields, and suggestion
//! dictionaries to disk, and a fresh registry opened on the
//! same directory recovers every one of them identically. Also
//! covers the in-memory default (no path -> no disk touch) and
//! the atomic-write guarantee (a stray `*.tmp` is ignored).

use std::collections::HashMap;

use dynomite_search::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, SuggestionRegistry,
    VectorRegistry, VectorSchema, VectorType,
};

/// Schema with a vector field plus one TEXT metadata field
/// ("body") declared in the SCHEMA clause.
fn schema_with_text() -> VectorSchema {
    VectorSchema {
        vector_field: "vec".to_string(),
        vector_type: VectorType::Float32,
        dim: 4,
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

/// Push a document into the index through the same public
/// surface the FT.* HSET interception uses: it upserts the
/// vector plus metadata into the engine, mirrors text into the
/// trigram index, then records the key as indexed.
fn feed_doc(table: &dynomite_search::VectorTable, key: &[u8], vector: &[f32], body: &[u8]) {
    let mut meta = HashMap::new();
    meta.insert(
        "body".to_string(),
        serde_json::Value::String(String::from_utf8_lossy(body).into_owned()),
    );
    table
        .engine
        .upsert(key.to_vec(), vector, meta)
        .expect("engine upsert");
    table.upsert_text_field("body", key, body);
    table.record_indexed_key(key.to_vec());
}

#[test]
fn round_trip_recovers_index_schema_docs_text_and_suggestions() {
    let dir = tempfile::tempdir().expect("tempdir");

    // ---- build + populate a durable registry --------------
    let sug = SuggestionRegistry::new();
    let reg = VectorRegistry::open(dir.path(), &sug).expect("open empty");
    assert!(reg.is_persistent());
    assert!(reg.list().is_empty());

    reg.create("idx".to_string(), schema_with_text())
        .expect("create");
    let table = reg.get("idx").expect("table");
    // A TEXT field added after FT.CREATE (FT.ALTER ADD ...).
    assert!(table.add_text_field("title"));

    feed_doc(&table, b"doc:1", &[1.0, 0.0, 0.0, 0.0], b"hello world");
    feed_doc(&table, b"doc:2", &[0.0, 1.0, 0.0, 0.0], b"goodbye world");
    // Title text only lives in the alter-added field.
    table.upsert_text_field("title", b"doc:1", b"greeting");

    sug.add(b"sk", b"alpha".to_vec(), 3.0, false, Some(b"pa".to_vec()));
    sug.add(b"sk", b"alphabet".to_vec(), 1.0, false, None);
    sug.add(b"other", b"beta".to_vec(), 2.0, false, None);

    reg.save(&sug).expect("save");
    drop(reg);
    drop(table);

    // ---- reopen into a fresh registry ---------------------
    let sug2 = SuggestionRegistry::new();
    let reg2 = VectorRegistry::open(dir.path(), &sug2).expect("reopen");

    // Index + schema recovered.
    assert_eq!(reg2.list(), vec!["idx".to_string()]);
    let t2 = reg2.get("idx").expect("table recovered");
    assert_eq!(t2.schema.dim, 4);
    assert_eq!(t2.schema.distance, DistanceMetric::Cosine);
    assert_eq!(t2.schema.prefixes, vec![b"doc:".to_vec()]);

    // Indexed keys recovered.
    let mut keys = t2.indexed_keys();
    keys.sort();
    assert_eq!(keys, vec![b"doc:1".to_vec(), b"doc:2".to_vec()]);

    // Vectors recovered (round-trips through the engine: a
    // top-1 search for doc:1's vector returns doc:1).
    let hits = t2
        .engine
        .search(&[1.0, 0.0, 0.0, 0.0], 1, None)
        .expect("search");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0.key, b"doc:1");

    // Schema-declared TEXT field recovered.
    let body_hits = t2
        .search_text_substring("body", b"world")
        .expect("body field present");
    let mut body_keys: Vec<Vec<u8>> = body_hits.iter().map(|(k, _)| k.clone()).collect();
    body_keys.sort();
    assert_eq!(body_keys, vec![b"doc:1".to_vec(), b"doc:2".to_vec()]);

    // FT.ALTER-added TEXT field recovered (both the field
    // provisioning and its content).
    assert!(t2.has_text_field("title"));
    let title_hits = t2
        .search_text_substring("title", b"greet")
        .expect("title field present");
    assert_eq!(title_hits.len(), 1);
    assert_eq!(title_hits[0].0, b"doc:1");

    // Suggestions recovered with scores + payloads.
    assert_eq!(sug2.len(b"sk"), 2);
    assert_eq!(sug2.len(b"other"), 1);
    let got = sug2.get(b"sk", b"alpha", 5, false, true, true);
    assert_eq!(got.len(), 2);
    // Highest score first: "alpha" (3.0) before "alphabet" (1.0).
    assert_eq!(got[0].value, b"alpha");
    assert_eq!(got[0].score, Some(3.0));
    assert_eq!(got[0].payload, Some(b"pa".to_vec()));
    assert_eq!(got[1].value, b"alphabet");
    assert_eq!(got[1].score, Some(1.0));
}

#[test]
fn stray_tmp_file_is_ignored_and_valid_snapshot_loads() {
    let dir = tempfile::tempdir().expect("tempdir");

    let sug = SuggestionRegistry::new();
    let reg = VectorRegistry::open(dir.path(), &sug).expect("open");
    reg.create("idx".to_string(), schema_with_text())
        .expect("create");
    reg.save(&sug).expect("save");

    // Simulate a crash mid-write: a leftover `.tmp` sibling.
    let tmp = dir.path().join("search-snapshot.cbor.tmp");
    std::fs::write(&tmp, b"garbage not cbor").expect("write tmp");

    // Reopen still loads the valid snapshot and ignores the
    // stray temp file.
    let sug2 = SuggestionRegistry::new();
    let reg2 = VectorRegistry::open(dir.path(), &sug2).expect("reopen with stray tmp");
    assert_eq!(reg2.list(), vec!["idx".to_string()]);
}

#[test]
fn open_on_empty_dir_starts_empty() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sug = SuggestionRegistry::new();
    let reg = VectorRegistry::open(dir.path(), &sug).expect("open empty");
    assert!(reg.list().is_empty());
    assert!(sug.is_empty());
    // No snapshot file is written until `save` is called.
    assert!(!dir.path().join("search-snapshot.cbor").exists());
}

#[test]
fn new_is_in_memory_and_never_touches_disk() {
    // The in-memory constructor reports non-persistent and its
    // `save` is a no-op even when handed a populated
    // suggestion registry.
    let reg = VectorRegistry::new();
    assert!(!reg.is_persistent());
    reg.create("idx".to_string(), schema_with_text())
        .expect("create");
    let sug = SuggestionRegistry::new();
    sug.add(b"k", b"v".to_vec(), 1.0, false, None);
    // No directory configured: save returns Ok and writes
    // nothing (there is nowhere to write).
    reg.save(&sug).expect("save is a no-op");
}

#[test]
fn save_is_atomic_and_reopen_sees_latest() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sug = SuggestionRegistry::new();
    let reg = VectorRegistry::open(dir.path(), &sug).expect("open");

    reg.create("a".to_string(), schema_with_text()).expect("a");
    reg.save(&sug).expect("save 1");

    // A second index, then a second save: the snapshot is
    // replaced atomically and the reopen sees both.
    reg.create("b".to_string(), schema_with_text()).expect("b");
    reg.save(&sug).expect("save 2");
    // No stray tmp left behind after a clean save.
    assert!(!dir.path().join("search-snapshot.cbor.tmp").exists());

    let sug2 = SuggestionRegistry::new();
    let reg2 = VectorRegistry::open(dir.path(), &sug2).expect("reopen");
    let mut names = reg2.list();
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}
