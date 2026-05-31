//! Redis-Stack RediSearch FT.* command surface integration
//! tests.
//!
//! These tests exercise the FT.* command pipeline introduced
//! in Phase C of the dynvec fold. They build argument lists
//! directly (skipping the Redis wire parser) so the tests
//! focus on the command parsing and execution semantics, not
//! the RESP framing.

use dynomite_search::ft::{self, DocType, FtCommand, FtError, FtOutcome, InfoValue, SearchHit};
use dynomite_search::registry::VectorRegistry;
use dynomite_search::schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorType,
};

/// Build the standard 4-dim cosine HNSW FT.CREATE argument
/// vector against a `docs:` prefix.
fn create_args(idx: &[u8], dim: u16, distance: &[u8]) -> Vec<Vec<u8>> {
    let dim_str = dim.to_string();
    vec![
        b"FT.CREATE".to_vec(),
        idx.to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        b"docs:".to_vec(),
        b"SCHEMA".to_vec(),
        b"title".to_vec(),
        b"TEXT".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        dim_str.into_bytes(),
        b"DISTANCE_METRIC".to_vec(),
        distance.to_vec(),
    ]
}

fn as_slices(v: &[Vec<u8>]) -> Vec<&[u8]> {
    v.iter().map(Vec::as_slice).collect()
}

fn float_le_bytes(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

#[test]
fn ft_create_hash_hnsw_cosine_returns_ok() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"COSINE");
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert_eq!(bytes, b"+OK\r\n");
    let table = registry.get("myidx").expect("registered");
    assert_eq!(table.schema.dim, 4);
    assert_eq!(table.schema.distance, DistanceMetric::Cosine);
    assert_eq!(table.schema.algorithm, IndexAlgorithm::Hnsw);
    assert_eq!(table.schema.vector_field, "vec");
    assert_eq!(table.schema.prefixes, vec![b"docs:".to_vec()]);
    assert_eq!(table.schema.vector_type, VectorType::Float32);
    assert_eq!(
        table.schema.metadata_fields,
        vec![MetadataField {
            name: "title".to_string(),
            field_type: MetadataFieldType::Text,
            tag_separator: None,
        }]
    );
}

#[test]
fn ft_create_unsupported_algorithm_returns_err() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"idx".to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        b"docs:".to_vec(),
        b"SCHEMA".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"FLAT".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"4".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"COSINE".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    let body = std::str::from_utf8(&bytes).unwrap();
    assert!(
        body.starts_with("-ERR ") && body.contains("FLAT"),
        "expected FLAT-not-supported error, got {body:?}"
    );
    assert!(registry.get("idx").is_none(), "no index should be created");
}

#[test]
fn ft_create_unsupported_doctype_returns_err() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"idx".to_vec(),
        b"ON".to_vec(),
        b"JSON".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        b"docs:".to_vec(),
        b"SCHEMA".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"4".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"COSINE".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    let body = std::str::from_utf8(&bytes).unwrap();
    assert!(
        body.starts_with("-ERR ") && body.to_uppercase().contains("JSON"),
        "expected JSON-not-supported error, got {body:?}"
    );
}

#[test]
fn ft_info_returns_schema_and_counters() {
    let registry = VectorRegistry::new();
    let create = create_args(b"myidx", 8, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&create));
    let cmd = ft::parse_command(&[b"FT.INFO".as_slice(), b"myidx".as_slice()]).unwrap();
    let outcome = ft::execute(&registry, cmd).unwrap();
    let FtOutcome::Info(pairs) = outcome else {
        panic!("expected Info outcome");
    };
    let lookup: std::collections::HashMap<&str, &InfoValue> =
        pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();
    assert_eq!(
        lookup.get("index_name"),
        Some(&&InfoValue::String("myidx".to_string()))
    );
    assert_eq!(
        lookup.get("algorithm"),
        Some(&&InfoValue::String("HNSW".to_string()))
    );
    assert_eq!(
        lookup.get("distance_metric"),
        Some(&&InfoValue::String("COSINE".to_string()))
    );
    assert_eq!(lookup.get("dim"), Some(&&InfoValue::Integer(8)));
    assert_eq!(lookup.get("num_docs"), Some(&&InfoValue::Integer(0)));
    assert_eq!(lookup.get("tracked_rows"), Some(&&InfoValue::Integer(0)));
    let prefixes_value = lookup.get("prefixes").unwrap();
    let InfoValue::Array(items) = prefixes_value else {
        panic!("prefixes should be an array");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], InfoValue::String("docs:".to_string()));
}

#[test]
fn ft_info_unknown_index_returns_err() {
    let registry = VectorRegistry::new();
    let bytes = ft::dispatch(&registry, &[b"FT.INFO".as_slice(), b"nope".as_slice()]);
    let body = std::str::from_utf8(&bytes).unwrap();
    assert!(body.starts_with("-ERR "));
    assert!(body.contains("nope"), "error must reference the name");
}

#[test]
fn ft_list_returns_all_indexes() {
    let registry = VectorRegistry::new();
    for name in ["alpha", "bravo", "charlie"] {
        let args = create_args(name.as_bytes(), 4, b"COSINE");
        let _ = ft::dispatch(&registry, &as_slices(&args));
    }
    let outcome = ft::execute(&registry, FtCommand::List).unwrap();
    let FtOutcome::List(names) = outcome else {
        panic!("expected list outcome");
    };
    assert_eq!(names, vec!["alpha", "bravo", "charlie"]);

    let bytes = ft::dispatch(&registry, &[b"FT.LIST".as_slice()]);
    let expected = b"*3\r\n$5\r\nalpha\r\n$5\r\nbravo\r\n$7\r\ncharlie\r\n";
    assert_eq!(bytes, expected);
}

#[test]
fn ft_dropindex_removes_index() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&args));
    assert!(registry.get("myidx").is_some());
    let bytes = ft::dispatch(
        &registry,
        &[b"FT.DROPINDEX".as_slice(), b"myidx".as_slice()],
    );
    assert_eq!(bytes, b"+OK\r\n");
    assert!(registry.get("myidx").is_none());

    // Second drop should error.
    let bytes = ft::dispatch(
        &registry,
        &[b"FT.DROPINDEX".as_slice(), b"myidx".as_slice()],
    );
    assert!(bytes.starts_with(b"-ERR "));
}

#[test]
fn ft_dropindex_dd_also_drops_underlying_keys() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&args));

    // Drive HSET interception twice so two keys are tracked.
    let vec_bytes = float_le_bytes(&[1.0, 0.0, 0.0, 0.0]);
    let hset = vec![
        b"docs:1".to_vec(),
        b"title".to_vec(),
        b"first".to_vec(),
        b"vec".to_vec(),
        vec_bytes.clone(),
    ];
    let absorbed = ft::maybe_index_hset(&registry, &as_slices(&hset)).unwrap();
    assert_eq!(absorbed.as_deref(), Some("myidx"));

    let hset2 = vec![
        b"docs:2".to_vec(),
        b"title".to_vec(),
        b"second".to_vec(),
        b"vec".to_vec(),
        vec_bytes,
    ];
    let absorbed = ft::maybe_index_hset(&registry, &as_slices(&hset2)).unwrap();
    assert_eq!(absorbed.as_deref(), Some("myidx"));

    // Confirm the table tracked both.
    let table = registry.get("myidx").unwrap();
    let mut tracked = table.indexed_keys();
    tracked.sort();
    assert_eq!(tracked, vec![b"docs:1".to_vec(), b"docs:2".to_vec()]);

    // FT.DROPINDEX myidx DD -> drop succeeds and surfaces both keys.
    let cmd = ft::parse_command(&[
        b"FT.DROPINDEX".as_slice(),
        b"myidx".as_slice(),
        b"DD".as_slice(),
    ])
    .unwrap();
    let outcome = ft::execute(&registry, cmd).unwrap();
    let FtOutcome::DropOk {
        deleted_documents,
        document_count,
    } = outcome
    else {
        panic!("expected DropOk");
    };
    assert!(deleted_documents);
    assert_eq!(document_count, 2);
    assert!(registry.get("myidx").is_none());
}

#[test]
fn hset_with_indexed_prefix_inserts_vector() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&args));
    let vec_bytes = float_le_bytes(&[1.0, 2.0, 3.0, 4.0]);
    let hset = vec![
        b"docs:1".to_vec(),
        b"vec".to_vec(),
        vec_bytes,
        b"title".to_vec(),
        b"hello".to_vec(),
    ];
    let absorbed = ft::maybe_index_hset(&registry, &as_slices(&hset)).unwrap();
    assert_eq!(absorbed.as_deref(), Some("myidx"));
    let table = registry.get("myidx").unwrap();
    let row = table.engine.get(b"docs:1").unwrap().expect("row stored");
    assert_eq!(row.key, b"docs:1");
    assert_eq!(row.vector.dim, 4);
    let info = registry.info("myidx").unwrap();
    assert_eq!(info.live_rows, 1);
}

#[test]
fn hset_with_unmatched_prefix_is_ignored() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&args));
    let hset = vec![b"other:1".to_vec(), b"field".to_vec(), b"value".to_vec()];
    let absorbed = ft::maybe_index_hset(&registry, &as_slices(&hset)).unwrap();
    assert!(absorbed.is_none(), "non-matching prefix must be ignored");
    let info = registry.info("myidx").unwrap();
    assert_eq!(info.live_rows, 0);
}

#[test]
fn hset_with_indexed_prefix_with_text_field_stores_metadata() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&args));
    let vec_bytes = float_le_bytes(&[0.5, 0.5, 0.5, 0.5]);
    let hset = vec![
        b"docs:42".to_vec(),
        b"title".to_vec(),
        b"the answer".to_vec(),
        b"vec".to_vec(),
        vec_bytes,
    ];
    let _ = ft::maybe_index_hset(&registry, &as_slices(&hset)).unwrap();
    let table = registry.get("myidx").unwrap();
    let row = table.engine.get(b"docs:42").unwrap().unwrap();
    let title = row
        .metadata
        .get("title")
        .expect("metadata stored under the field name");
    let title_str = match title {
        serde_json::Value::String(s) => s.as_str(),
        other => panic!("title metadata must be a string, got {other:?}"),
    };
    assert_eq!(title_str, "the answer");
}

#[test]
fn hset_missing_vector_field_errors() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&args));
    let hset = vec![b"docs:1".to_vec(), b"title".to_vec(), b"only".to_vec()];
    let err = ft::maybe_index_hset(&registry, &as_slices(&hset)).unwrap_err();
    match err {
        FtError::Syntax(msg) => assert!(msg.contains("vec")),
        other => panic!("expected syntax error, got {other:?}"),
    }
}

#[test]
fn ft_search_knn_5_returns_5_doc_ids_in_distance_order() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"L2");
    let _ = ft::dispatch(&registry, &as_slices(&args));

    // Insert 8 documents at increasing distances from origin.
    for i in 0_u8..8 {
        let dist = f32::from(i);
        let key = format!("docs:{i}").into_bytes();
        let vector = float_le_bytes(&[dist, 0.0, 0.0, 0.0]);
        let hset = vec![
            key,
            b"title".to_vec(),
            format!("doc-{i}").into_bytes(),
            b"vec".to_vec(),
            vector,
        ];
        ft::maybe_index_hset(&registry, &as_slices(&hset)).unwrap();
    }

    let query_bytes = float_le_bytes(&[0.0, 0.0, 0.0, 0.0]);
    let search_args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"myidx".to_vec(),
        b"*=>[KNN 5 @vec $blob]".to_vec(),
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        b"blob".to_vec(),
        query_bytes,
    ];
    let cmd = ft::parse_command(&as_slices(&search_args)).unwrap();
    let outcome = ft::execute(&registry, cmd).unwrap();
    let FtOutcome::Search { total, hits } = outcome else {
        panic!("expected search outcome");
    };
    assert_eq!(total, 5);
    assert_eq!(hits.len(), 5);
    assert_eq!(hits[0].doc_id, b"docs:0");
    let ids: Vec<&[u8]> = hits.iter().map(|h| h.doc_id.as_slice()).collect();
    assert_eq!(
        ids,
        vec![
            b"docs:0".as_slice(),
            b"docs:1".as_slice(),
            b"docs:2".as_slice(),
            b"docs:3".as_slice(),
            b"docs:4".as_slice(),
        ]
    );
    // Scores must be non-decreasing (closer first).
    for window in hits.windows(2) {
        assert!(
            window[0].score <= window[1].score,
            "hits must be sorted closest-first"
        );
    }
}

#[test]
fn ft_search_knn_against_empty_index_returns_empty() {
    let registry = VectorRegistry::new();
    let args = create_args(b"empty", 4, b"COSINE");
    let _ = ft::dispatch(&registry, &as_slices(&args));
    let query_bytes = float_le_bytes(&[1.0, 0.0, 0.0, 0.0]);
    let search_args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"empty".to_vec(),
        b"*=>[KNN 5 @vec $blob]".to_vec(),
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        b"blob".to_vec(),
        query_bytes,
    ];
    let cmd = ft::parse_command(&as_slices(&search_args)).unwrap();
    let outcome = ft::execute(&registry, cmd).unwrap();
    let FtOutcome::Search { total, hits } = outcome else {
        panic!("expected search outcome");
    };
    assert_eq!(total, 0);
    assert!(hits.is_empty());
}

#[test]
fn ft_search_distance_score_in_response_is_a_float() {
    let registry = VectorRegistry::new();
    let args = create_args(b"myidx", 4, b"L2");
    let _ = ft::dispatch(&registry, &as_slices(&args));
    let key_bytes = float_le_bytes(&[1.0, 0.0, 0.0, 0.0]);
    let hset = vec![
        b"docs:a".to_vec(),
        b"vec".to_vec(),
        key_bytes,
        b"title".to_vec(),
        b"only".to_vec(),
    ];
    ft::maybe_index_hset(&registry, &as_slices(&hset)).unwrap();
    let query_bytes = float_le_bytes(&[0.0, 0.0, 0.0, 0.0]);
    let search_args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"myidx".to_vec(),
        b"*=>[KNN 1 @vec $blob]".to_vec(),
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        b"blob".to_vec(),
        query_bytes,
    ];
    let cmd = ft::parse_command(&as_slices(&search_args)).unwrap();
    let outcome = ft::execute(&registry, cmd).unwrap();
    let FtOutcome::Search { total, hits } = outcome else {
        panic!("expected search outcome");
    };
    assert_eq!(total, 1);
    let hit: &SearchHit = &hits[0];
    let (k, v) = hit
        .fields
        .iter()
        .find(|(k, _)| k == "__vec_score")
        .expect("score field present");
    assert_eq!(k, "__vec_score");
    let s = std::str::from_utf8(v).unwrap();
    let parsed: f32 = s
        .parse()
        .unwrap_or_else(|_| panic!("score field {s:?} must parse as f32"));
    assert!(parsed.is_finite(), "distance score must be finite");
    assert!(
        parsed >= 0.0,
        "L2 distance must be non-negative, got {parsed}"
    );
}

#[test]
fn ft_dispatch_unknown_command_returns_err() {
    let registry = VectorRegistry::new();
    let bytes = ft::dispatch(&registry, &[b"FT.NUKE".as_slice()]);
    assert!(bytes.starts_with(b"-ERR "));
}

#[test]
fn ft_create_doc_type_default_is_inferred_from_brief() {
    // Sanity check the parser's exposed structure: the parsed
    // FT.CREATE always populates DocType::Hash for the only
    // accepted shape, mirroring the FT.CREATE clauses tested
    // above. This locks the public DocType enum so a
    // future-flavoured JSON path will surface as a new
    // variant rather than a silent re-interpretation.
    let args = create_args(b"x", 4, b"COSINE");
    let cmd = ft::parse_command(&as_slices(&args)).unwrap();
    let FtCommand::Create(req) = cmd else {
        panic!("expected create");
    };
    assert_eq!(req.doc_type, DocType::Hash);
}
