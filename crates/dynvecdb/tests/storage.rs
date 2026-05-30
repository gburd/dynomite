//! Integration tests for [`dynvecdb::storage::VectorStore`].

use std::collections::HashMap;
use std::sync::Arc;

use dynvecdb::distance::Distance;
use dynvecdb::encoding::Codec;
use dynvecdb::index::HnswParams;
use dynvecdb::storage::{MemoryBackend, StoreError, TableSchema, VectorStore};

fn schema(name: &str, dim: u16, codec: Codec, distance: Distance) -> TableSchema {
    TableSchema {
        name: name.to_string(),
        dim,
        codec,
        distance,
        hnsw: HnswParams::default(),
    }
}

#[test]
fn put_get_delete_round_trips() {
    let store = VectorStore::in_memory();
    store
        .create_table(schema("rt", 4, Codec::Int8Quantized, Distance::Euclidean))
        .unwrap();
    let mut md = HashMap::new();
    md.insert("k".to_string(), serde_json::json!("v"));
    store
        .upsert("rt", b"alpha".to_vec(), &[0.1, 0.2, 0.3, 0.4], md.clone())
        .unwrap();
    let fetched = store.get("rt", b"alpha").unwrap().unwrap();
    assert_eq!(fetched.key, b"alpha");
    assert_eq!(fetched.metadata.get("k").unwrap(), &serde_json::json!("v"));
    assert!(store.delete("rt", b"alpha").unwrap());
    assert!(store.get("rt", b"alpha").unwrap().is_none());
}

#[test]
fn upsert_replaces_metadata_and_vector() {
    let store = VectorStore::in_memory();
    store
        .create_table(schema("u", 2, Codec::Fp16, Distance::Cosine))
        .unwrap();
    store
        .upsert("u", b"x".to_vec(), &[1.0, 0.0], HashMap::new())
        .unwrap();
    let mut md = HashMap::new();
    md.insert("v".to_string(), serde_json::json!(42));
    store.upsert("u", b"x".to_vec(), &[0.0, 1.0], md).unwrap();
    let fetched = store.get("u", b"x").unwrap().unwrap();
    assert_eq!(fetched.metadata.get("v").unwrap(), &serde_json::json!(42));
    // Search for [0,1] should return x first.
    let hits = store.search("u", &[0.0, 1.0], 1, None).unwrap();
    assert_eq!(hits[0].0.key, b"x");
}

#[test]
fn search_uses_distance_metric() {
    let store = VectorStore::in_memory();
    store
        .create_table(schema("d", 2, Codec::Fp16, Distance::DotProduct))
        .unwrap();
    store
        .upsert("d", b"p".to_vec(), &[10.0, 0.0], HashMap::new())
        .unwrap();
    store
        .upsert("d", b"q".to_vec(), &[0.0, 1.0], HashMap::new())
        .unwrap();
    let hits = store.search("d", &[5.0, 0.0], 1, None).unwrap();
    assert_eq!(hits[0].0.key, b"p");
}

#[test]
fn unknown_table_rejected() {
    let store = VectorStore::in_memory();
    assert!(matches!(
        store.upsert("missing", b"k".to_vec(), &[0.0], HashMap::new()),
        Err(StoreError::UnknownTable(_))
    ));
}

#[test]
fn dimension_mismatch_on_search() {
    let store = VectorStore::in_memory();
    store
        .create_table(schema("dm", 4, Codec::Int8Quantized, Distance::Euclidean))
        .unwrap();
    assert!(matches!(
        store.search("dm", &[0.0, 0.0], 1, None),
        Err(StoreError::DimensionMismatch { .. })
    ));
}

#[test]
fn rehydration_preserves_data() {
    let backend = Arc::new(MemoryBackend::new());
    {
        let store = VectorStore::open(backend.clone()).unwrap();
        store
            .create_table(schema("r", 3, Codec::Int8Quantized, Distance::Cosine))
            .unwrap();
        for i in 0..20_u8 {
            store
                .upsert(
                    "r",
                    format!("k{i}").into_bytes(),
                    &[f32::from(i), f32::from(i) * 0.5, f32::from(i) * 0.25],
                    HashMap::new(),
                )
                .unwrap();
        }
    }
    let reopened = VectorStore::open(backend).unwrap();
    let stats = reopened.stats("r").unwrap();
    assert_eq!(stats.live_rows, 20);
    assert_eq!(stats.dim, 3);
    let row = reopened.get("r", b"k7").unwrap().unwrap();
    assert_eq!(row.key, b"k7");
}

#[test]
fn delete_idempotent() {
    let store = VectorStore::in_memory();
    store
        .create_table(schema("idem", 2, Codec::Fp16, Distance::Cosine))
        .unwrap();
    store
        .upsert("idem", b"a".to_vec(), &[1.0, 2.0], HashMap::new())
        .unwrap();
    assert!(store.delete("idem", b"a").unwrap());
    assert!(!store.delete("idem", b"a").unwrap());
    assert!(!store.delete("idem", b"never").unwrap());
}
