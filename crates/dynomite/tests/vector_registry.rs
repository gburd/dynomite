//! Integration tests for [`dynomite::vector::registry::VectorRegistry`].
//!
//! Phase B of the dynvec-fold-into-redis-path work lands the
//! registry as the single source of truth for FT.* command
//! handlers. These tests exercise the public registry surface
//! directly without the FT.* parser (Phase C), which is the
//! follow-up.

use std::sync::Arc;
use std::thread;

use dynomite::vector::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, RegistryError,
    VectorRegistry, VectorSchema, VectorType,
};

fn schema_with(algorithm: IndexAlgorithm, dim: u16) -> VectorSchema {
    VectorSchema {
        vector_field: "vec".to_string(),
        vector_type: VectorType::Float32,
        dim,
        distance: DistanceMetric::Cosine,
        algorithm,
        prefixes: Vec::new(),
        metadata_fields: vec![MetadataField {
            name: "title".to_string(),
            field_type: MetadataFieldType::Text,
            tag_separator: None,
        }],
    }
}

#[test]
fn create_then_get_returns_table() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_with(IndexAlgorithm::Hnsw, 8))
        .expect("create succeeds");
    let table = reg.get("idx").expect("table is registered");
    assert_eq!(table.name, "idx");
    assert_eq!(table.schema.dim, 8);
    assert_eq!(table.schema.distance, DistanceMetric::Cosine);
    assert_eq!(table.engine.table_name(), "idx");
}

#[test]
fn create_duplicate_name_errors() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_with(IndexAlgorithm::Hnsw, 4))
        .unwrap();
    let err = reg
        .create("idx".to_string(), schema_with(IndexAlgorithm::Hnsw, 4))
        .expect_err("second create must fail");
    assert!(matches!(err, RegistryError::AlreadyExists(_)));
}

#[test]
fn drop_removes_table() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_with(IndexAlgorithm::Hnsw, 4))
        .unwrap();
    let removed = reg.drop("idx").expect("present");
    assert_eq!(removed.name, "idx");
    assert!(reg.get("idx").is_none());
    let err = reg.drop("idx").expect_err("second drop must fail");
    assert!(matches!(err, RegistryError::NotFound(_)));
}

#[test]
fn info_returns_dim_distance_algorithm() {
    let reg = VectorRegistry::new();
    reg.create("idx".to_string(), schema_with(IndexAlgorithm::Hnsw, 16))
        .unwrap();
    let info = reg.info("idx").expect("info available");
    assert_eq!(info.name, "idx");
    assert_eq!(info.dim, 16);
    assert_eq!(info.distance, DistanceMetric::Cosine);
    assert_eq!(info.algorithm, IndexAlgorithm::Hnsw);
    assert_eq!(info.live_rows, 0);
    assert_eq!(info.tracked_rows, 0);
    assert!(reg.info("nope").is_none());
}

#[test]
fn list_returns_all_table_names() {
    let reg = VectorRegistry::new();
    for name in ["alpha", "bravo", "charlie"] {
        reg.create(name.to_string(), schema_with(IndexAlgorithm::Hnsw, 4))
            .unwrap();
    }
    let names = reg.list();
    // The registry stores names in a BTreeMap, so the list is
    // sorted; the assertion locks both contents and ordering.
    assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
}

#[test]
fn unsupported_algorithm_errors() {
    let reg = VectorRegistry::new();
    let err = reg
        .create("idx".to_string(), schema_with(IndexAlgorithm::Flat, 4))
        .expect_err("flat must error");
    assert!(matches!(
        err,
        RegistryError::UnsupportedAlgorithm(IndexAlgorithm::Flat)
    ));
}

#[test]
fn concurrent_creates_serialize_through_rwlock() {
    let reg = Arc::new(VectorRegistry::new());
    // Spawn 16 threads, each tries to create the same index
    // name. The RwLock-backed write path must serialise them so
    // exactly one succeeds and the rest report
    // `AlreadyExists`.
    let n = 16;
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let reg = Arc::clone(&reg);
        handles.push(thread::spawn(move || {
            reg.create(
                "contended".to_string(),
                schema_with(IndexAlgorithm::Hnsw, 4),
            )
        }));
    }
    let mut successes = 0_usize;
    let mut already_exists = 0_usize;
    for h in handles {
        match h.join().unwrap() {
            Ok(()) => successes += 1,
            Err(RegistryError::AlreadyExists(_)) => already_exists += 1,
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(already_exists, n - 1);
    assert_eq!(reg.list(), vec!["contended"]);
}
