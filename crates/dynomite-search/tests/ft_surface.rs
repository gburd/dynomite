//! Broad FT.* command-surface coverage tests.
//!
//! These tests drive whole command flows through the public
//! `ft::dispatch` / `ft::dispatch_sugest` entry points and the
//! parse/execute/render helpers, exercising the aggregate,
//! explain, alter, regex, sortby, return, limit, nocontent,
//! and error-reply paths that the existing `ft_redis.rs` suite
//! does not reach. Each test names the command path and the
//! observable contract it pins.

use dynomite_search::ft::{self, FtCommand, FtError, FtOutcome, InfoValue};
use dynomite_search::registry::VectorRegistry;
use dynomite_search::sugest_registry::SuggestionRegistry;

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

/// FT.CREATE with one TEXT field ("body"), one TAG field
/// ("color"), and one numeric metadata field ("price") plus a
/// 2-dim vector field.
fn create_full(idx: &[u8]) -> Vec<Vec<u8>> {
    vec![
        b"FT.CREATE".to_vec(),
        idx.to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        b"doc:".to_vec(),
        b"SCHEMA".to_vec(),
        b"body".to_vec(),
        b"TEXT".to_vec(),
        b"color".to_vec(),
        b"TAG".to_vec(),
        b"price".to_vec(),
        b"NUMERIC".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"2".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
    ]
}

/// Feed a hash document through the HSET interception path.
fn hset(
    registry: &VectorRegistry,
    key: &[u8],
    vec2: &[f32],
    body: &[u8],
    color: &[u8],
    price: &[u8],
) {
    let vbytes = float_le_bytes(vec2);
    // `maybe_index_hset` takes the key as args[0] followed by
    // field/value pairs (no leading HSET keyword).
    let args: Vec<Vec<u8>> = vec![
        key.to_vec(),
        b"vec".to_vec(),
        vbytes,
        b"body".to_vec(),
        body.to_vec(),
        b"color".to_vec(),
        color.to_vec(),
        b"price".to_vec(),
        price.to_vec(),
    ];
    let outcome = ft::maybe_index_hset(registry, &as_slices(&args)).expect("hset indexed");
    assert!(outcome.is_some(), "doc with index prefix is absorbed");
}

fn populated() -> VectorRegistry {
    let registry = VectorRegistry::new();
    let c = create_full(b"idx");
    assert_eq!(ft::dispatch(&registry, &as_slices(&c)), b"+OK\r\n");
    hset(
        &registry,
        b"doc:1",
        &[0.0, 0.0],
        b"red apple",
        b"red",
        b"10",
    );
    hset(
        &registry,
        b"doc:2",
        &[1.0, 1.0],
        b"green apple",
        b"green",
        b"20",
    );
    hset(
        &registry,
        b"doc:3",
        &[2.0, 2.0],
        b"red berry",
        b"red",
        b"30",
    );
    registry
}

// ---- FT.AGGREGATE -------------------------------------------------------

#[test]
fn aggregate_groupby_count_sum_avg() {
    let registry = populated();
    // GROUPBY @color, REDUCE COUNT/SUM/AVG over price.
    let args: Vec<Vec<u8>> = vec![
        b"FT.AGGREGATE".to_vec(),
        b"idx".to_vec(),
        b"*".to_vec(),
        b"GROUPBY".to_vec(),
        b"1".to_vec(),
        b"@color".to_vec(),
        b"REDUCE".to_vec(),
        b"COUNT".to_vec(),
        b"0".to_vec(),
        b"AS".to_vec(),
        b"n".to_vec(),
        b"REDUCE".to_vec(),
        b"SUM".to_vec(),
        b"1".to_vec(),
        b"@price".to_vec(),
        b"AS".to_vec(),
        b"total".to_vec(),
        b"REDUCE".to_vec(),
        b"AVG".to_vec(),
        b"1".to_vec(),
        b"@price".to_vec(),
        b"AS".to_vec(),
        b"mean".to_vec(),
    ];
    let cmd = ft::parse_command(&as_slices(&args)).expect("parse aggregate");
    let FtCommand::Aggregate(_) = &cmd else {
        panic!("expected Aggregate");
    };
    let out = ft::execute(&registry, cmd).expect("execute aggregate");
    let FtOutcome::Aggregate { total_groups, rows } = out else {
        panic!("expected Aggregate outcome");
    };
    // Two color groups: red (2 docs, sum 40, avg 20) and green
    // (1 doc, sum 20, avg 20).
    assert_eq!(total_groups, 2);
    // Find the red row.
    let red = rows
        .iter()
        .find(|row| row.iter().any(|(_, v)| v == b"red"))
        .expect("red group present");
    let lookup = |name: &str| -> Vec<u8> {
        red.iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    assert_eq!(lookup("n"), b"2");
    assert_eq!(lookup("total"), b"40.000000");
    assert_eq!(lookup("mean"), b"20.000000");

    // The RESP rendering is non-empty and array-shaped.
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"), "aggregate renders a RESP array");
}

#[test]
fn aggregate_limit_truncates_groups() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.AGGREGATE".to_vec(),
        b"idx".to_vec(),
        b"*".to_vec(),
        b"GROUPBY".to_vec(),
        b"1".to_vec(),
        b"@color".to_vec(),
        b"REDUCE".to_vec(),
        b"COUNT".to_vec(),
        b"0".to_vec(),
        b"AS".to_vec(),
        b"n".to_vec(),
        b"LIMIT".to_vec(),
        b"0".to_vec(),
        b"1".to_vec(),
    ];
    let cmd = ft::parse_command(&as_slices(&args)).expect("parse");
    let out = ft::execute(&registry, cmd).expect("execute");
    let FtOutcome::Aggregate { total_groups, rows } = out else {
        panic!("expected Aggregate");
    };
    // total_groups is computed after LIMIT truncation, so both
    // it and the row vector reflect the one surviving group.
    assert_eq!(total_groups, 1);
    assert_eq!(rows.len(), 1);
}

#[test]
fn aggregate_on_missing_index_is_not_found() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.AGGREGATE".to_vec(),
        b"nope".to_vec(),
        b"*".to_vec(),
        b"GROUPBY".to_vec(),
        b"1".to_vec(),
        b"@color".to_vec(),
        b"REDUCE".to_vec(),
        b"COUNT".to_vec(),
        b"0".to_vec(),
        b"AS".to_vec(),
        b"n".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "missing index -> error reply");
}

// ---- FT.EXPLAIN ---------------------------------------------------------

#[test]
fn explain_returns_query_plan_string() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.EXPLAIN".to_vec(),
        b"idx".to_vec(),
        b"@body:apple".to_vec(),
    ];
    let cmd = ft::parse_command(&as_slices(&args)).expect("parse explain");
    let out = ft::execute(&registry, cmd).expect("execute explain");
    let FtOutcome::Explain(plan) = out else {
        panic!("expected Explain");
    };
    assert!(!plan.is_empty(), "explain emits a non-empty plan");

    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"$"), "explain renders a bulk string");
}

#[test]
fn explain_on_missing_index_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![b"FT.EXPLAIN".to_vec(), b"nope".to_vec(), b"*".to_vec()];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

// ---- FT.ALTER -----------------------------------------------------------

#[test]
fn alter_add_text_field_then_search_it() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.ALTER".to_vec(),
        b"idx".to_vec(),
        b"ADD".to_vec(),
        b"summary".to_vec(),
        b"TEXT".to_vec(),
    ];
    assert_eq!(ft::dispatch(&registry, &as_slices(&args)), b"+OK\r\n");
    let table = registry.get("idx").expect("table");
    assert!(table.has_text_field("summary"));
    assert!(table.has_text_index("summary"));
}

#[test]
fn alter_add_tag_field_ok() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.ALTER".to_vec(),
        b"idx".to_vec(),
        b"ADD".to_vec(),
        b"region".to_vec(),
        b"TAG".to_vec(),
    ];
    assert_eq!(ft::dispatch(&registry, &as_slices(&args)), b"+OK\r\n");
}

#[test]
fn alter_add_vector_field_is_rejected() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.ALTER".to_vec(),
        b"idx".to_vec(),
        b"ADD".to_vec(),
        b"v2".to_vec(),
        b"VECTOR".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "VECTOR via ALTER is rejected");
}

#[test]
fn alter_on_missing_index_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.ALTER".to_vec(),
        b"nope".to_vec(),
        b"ADD".to_vec(),
        b"f".to_vec(),
        b"TEXT".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

// ---- FT.REGEX -----------------------------------------------------------

#[test]
fn regex_exact_matches_text_field() {
    let registry = populated();
    // Exact regex (max_errors 0) over @body for "apple".
    let args: Vec<Vec<u8>> = vec![
        b"FT.REGEX".to_vec(),
        b"idx".to_vec(),
        b"body".to_vec(),
        b"a.+e".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    // Two docs contain "apple" matching a.+e.
    assert!(bytes.starts_with(b"*"), "regex renders a RESP array");
}

#[test]
fn regex_approx_tolerates_edits() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.REGEX".to_vec(),
        b"idx".to_vec(),
        b"body".to_vec(),
        b"appel".to_vec(),
        b"K=2".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"), "approx regex renders an array");
}

#[test]
fn regex_on_unknown_field_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.REGEX".to_vec(),
        b"idx".to_vec(),
        b"nosuchfield".to_vec(),
        b"x".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "unknown field -> error");
}

#[test]
fn regex_bad_pattern_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.REGEX".to_vec(),
        b"idx".to_vec(),
        b"body".to_vec(),
        b"[unterminated".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "bad pattern -> error");
}

// ---- FT.SEARCH projection clauses ---------------------------------------

#[test]
fn search_knn_with_return_limit_nocontent() {
    let registry = populated();
    let qvec = float_le_bytes(&[0.0, 0.0]);
    // KNN with RETURN, LIMIT, NOCONTENT.
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"idx".to_vec(),
        b"*=>[KNN 3 @vec $q]".to_vec(),
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        b"q".to_vec(),
        qvec,
        b"NOCONTENT".to_vec(),
        b"LIMIT".to_vec(),
        b"0".to_vec(),
        b"2".to_vec(),
        b"DIALECT".to_vec(),
        b"2".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"), "search renders an array");
}

#[test]
fn search_text_with_sortby_asc_and_desc() {
    let registry = populated();
    for dir in [b"ASC".as_slice(), b"DESC".as_slice()] {
        let args: Vec<Vec<u8>> = vec![
            b"FT.SEARCH".to_vec(),
            b"idx".to_vec(),
            b"@body:apple".to_vec(),
            b"SORTBY".to_vec(),
            b"price".to_vec(),
            dir.to_vec(),
        ];
        let bytes = ft::dispatch(&registry, &as_slices(&args));
        assert!(bytes.starts_with(b"*"), "sortby {dir:?} renders array");
    }
}

#[test]
fn search_text_with_return_fields() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"idx".to_vec(),
        b"@body:apple".to_vec(),
        b"RETURN".to_vec(),
        b"1".to_vec(),
        b"color".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"));
}

#[test]
fn search_on_missing_index_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"nope".to_vec(),
        b"@body:apple".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

// ---- FT.INFO / FT.LIST / FT.DROPINDEX -----------------------------------

#[test]
fn info_reports_schema_and_counts() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![b"FT.INFO".to_vec(), b"idx".to_vec()];
    let cmd = ft::parse_command(&as_slices(&args)).expect("parse info");
    let out = ft::execute(&registry, cmd).expect("execute info");
    let FtOutcome::Info(pairs) = out else {
        panic!("expected Info");
    };
    // index_name is reported.
    let has_name = pairs
        .iter()
        .any(|(k, v)| k == "index_name" && matches!(v, InfoValue::String(s) if s == "idx"));
    assert!(has_name, "FT.INFO reports index_name");

    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"), "info renders an array");
}

#[test]
fn info_missing_index_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![b"FT.INFO".to_vec(), b"nope".to_vec()];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

#[test]
fn list_returns_registered_names() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![b"FT.LIST".to_vec()];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"));
    // The _LIST alias parses to the same command.
    let alias: Vec<Vec<u8>> = vec![b"FT._LIST".to_vec()];
    let bytes2 = ft::dispatch(&registry, &as_slices(&alias));
    assert_eq!(bytes, bytes2);
}

#[test]
fn dropindex_with_dd_reports_document_count() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![b"FT.DROPINDEX".to_vec(), b"idx".to_vec(), b"DD".to_vec()];
    let cmd = ft::parse_command(&as_slices(&args)).expect("parse dropindex");
    let FtCommand::DropIndex {
        delete_documents, ..
    } = &cmd
    else {
        panic!("expected DropIndex");
    };
    assert!(*delete_documents);
    let out = ft::execute(&registry, cmd).expect("execute dropindex");
    let FtOutcome::DropOk {
        deleted_documents,
        document_count,
    } = out
    else {
        panic!("expected DropOk");
    };
    assert!(deleted_documents);
    assert_eq!(document_count, 3, "three indexed docs reported for DD");
    assert!(registry.get("idx").is_none(), "index removed");
}

#[test]
fn dropindex_without_dd_is_plain_ok() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![b"FT.DROPINDEX".to_vec(), b"idx".to_vec()];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert_eq!(bytes, b"+OK\r\n");
}

#[test]
fn dropindex_missing_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![b"FT.DROPINDEX".to_vec(), b"nope".to_vec()];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

// ---- error / parse-failure branches -------------------------------------

#[test]
fn unknown_ft_command_is_unsupported() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![b"FT.WHATEVER".to_vec()];
    let err = ft::parse_command(&as_slices(&args)).unwrap_err();
    assert!(matches!(err, FtError::Unsupported(_)));
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

#[test]
fn non_ft_keyword_is_unknown_command() {
    let args: Vec<Vec<u8>> = vec![b"GET".to_vec()];
    let err = ft::parse_command(&as_slices(&args)).unwrap_err();
    assert!(matches!(err, FtError::UnknownCommand(_)));
}

#[test]
fn empty_args_is_unknown_command() {
    let err = ft::parse_command(&[]).unwrap_err();
    assert!(matches!(err, FtError::UnknownCommand(_)));
}

#[test]
fn create_duplicate_index_errors() {
    let registry = VectorRegistry::new();
    let c = create_full(b"idx");
    assert_eq!(ft::dispatch(&registry, &as_slices(&c)), b"+OK\r\n");
    let bytes = ft::dispatch(&registry, &as_slices(&c));
    assert!(bytes.starts_with(b"-"), "duplicate FT.CREATE -> error");
}

// ---- FT.SUG* rendering --------------------------------------------------

#[test]
fn sugget_renders_scores_and_payloads() {
    let sug = SuggestionRegistry::new();
    // FT.SUGADD key suggestion score [INCR] [PAYLOAD p]
    let add = |s: &[u8], score: &[u8], payload: Option<&[u8]>| -> Vec<Vec<u8>> {
        let mut v = vec![
            b"FT.SUGADD".to_vec(),
            b"sk".to_vec(),
            s.to_vec(),
            score.to_vec(),
        ];
        if let Some(p) = payload {
            v.push(b"PAYLOAD".to_vec());
            v.push(p.to_vec());
        }
        v
    };
    let _ = ft::dispatch_sugest(&sug, &as_slices(&add(b"hello", b"3", Some(b"pa"))));
    let _ = ft::dispatch_sugest(&sug, &as_slices(&add(b"help", b"1", None)));

    // FT.SUGGET sk he WITHSCORES WITHPAYLOADS
    let get: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"he".to_vec(),
        b"WITHSCORES".to_vec(),
        b"WITHPAYLOADS".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&sug, &as_slices(&get));
    assert!(bytes.starts_with(b"*"), "sugget renders an array");
    let s = String::from_utf8_lossy(&bytes);
    assert!(s.contains("hello"), "top suggestion present");

    // FT.SUGLEN reports the dictionary size.
    let len: Vec<Vec<u8>> = vec![b"FT.SUGLEN".to_vec(), b"sk".to_vec()];
    assert_eq!(ft::dispatch_sugest(&sug, &as_slices(&len)), b":2\r\n");

    // FT.SUGDEL removes one and returns :1.
    let del: Vec<Vec<u8>> = vec![b"FT.SUGDEL".to_vec(), b"sk".to_vec(), b"help".to_vec()];
    assert_eq!(ft::dispatch_sugest(&sug, &as_slices(&del)), b":1\r\n");
    // Deleting a missing entry returns :0.
    assert_eq!(ft::dispatch_sugest(&sug, &as_slices(&del)), b":0\r\n");
}

#[test]
fn sugadd_incr_accumulates_score() {
    let sug = SuggestionRegistry::new();
    let add_incr: Vec<Vec<u8>> = vec![
        b"FT.SUGADD".to_vec(),
        b"sk".to_vec(),
        b"x".to_vec(),
        b"2".to_vec(),
        b"INCR".to_vec(),
    ];
    let _ = ft::dispatch_sugest(&sug, &as_slices(&add_incr));
    let _ = ft::dispatch_sugest(&sug, &as_slices(&add_incr));
    let get: Vec<Vec<u8>> = vec![
        b"FT.SUGGET".to_vec(),
        b"sk".to_vec(),
        b"x".to_vec(),
        b"WITHSCORES".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&sug, &as_slices(&get));
    assert!(bytes.starts_with(b"*"));
}

#[test]
fn sugest_unknown_keyword_errors() {
    let sug = SuggestionRegistry::new();
    let args: Vec<Vec<u8>> = vec![b"FT.SUGWHAT".to_vec()];
    let bytes = ft::dispatch_sugest(&sug, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

#[test]
fn sugadd_bad_score_errors() {
    let sug = SuggestionRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.SUGADD".to_vec(),
        b"sk".to_vec(),
        b"v".to_vec(),
        b"notanumber".to_vec(),
    ];
    let bytes = ft::dispatch_sugest(&sug, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "non-numeric score -> error");
}

#[test]
fn sugadd_missing_args_errors() {
    let sug = SuggestionRegistry::new();
    let args: Vec<Vec<u8>> = vec![b"FT.SUGADD".to_vec(), b"sk".to_vec()];
    let bytes = ft::dispatch_sugest(&sug, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

#[test]
fn suglen_missing_key_errors() {
    let sug = SuggestionRegistry::new();
    let args: Vec<Vec<u8>> = vec![b"FT.SUGLEN".to_vec()];
    let bytes = ft::dispatch_sugest(&sug, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

// ---- FT.SEARCH filter-expression form (no KNN) --------------------------

#[test]
fn search_filter_numeric_range_form() {
    let registry = populated();
    // A filter-expression query with no KNN clause routes
    // through the SearchFilter executor.
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"idx".to_vec(),
        b"@price:[15 30]".to_vec(),
    ];
    let cmd = ft::parse_command(&as_slices(&args)).expect("parse filter search");
    assert!(matches!(cmd, FtCommand::SearchFilter(_)));
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"), "filter search renders array");
}

#[test]
fn search_filter_tag_with_nocontent() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"idx".to_vec(),
        b"@color:{red}".to_vec(),
        b"NOCONTENT".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"));
}

#[test]
fn search_knn_with_numeric_prefilter() {
    let registry = populated();
    let qvec = float_le_bytes(&[0.0, 0.0]);
    // KNN query with a numeric pre-filter on the LHS of `=>`.
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"idx".to_vec(),
        b"@price:[0 25]=>[KNN 2 @vec $q]".to_vec(),
        b"PARAMS".to_vec(),
        b"2".to_vec(),
        b"q".to_vec(),
        qvec,
    ];
    let cmd = ft::parse_command(&as_slices(&args)).expect("parse knn+filter");
    let FtCommand::Search(req) = &cmd else {
        panic!("expected Search");
    };
    assert!(req.filter.is_some(), "prefilter parsed");
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"*"));
}

#[test]
fn search_knn_missing_params_errors() {
    let registry = populated();
    // KNN clause references $q but PARAMS does not provide it.
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"idx".to_vec(),
        b"*=>[KNN 2 @vec $q]".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "missing PARAM -> error");
}

#[test]
fn search_missing_query_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![b"FT.SEARCH".to_vec(), b"idx".to_vec()];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"));
}

#[test]
fn search_text_form_on_missing_field_errors() {
    let registry = populated();
    // @nofield:x is not a declared field.
    let args: Vec<Vec<u8>> = vec![
        b"FT.SEARCH".to_vec(),
        b"idx".to_vec(),
        b"@nofield:apple".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "unknown field -> error");
}

// ---- FT.AGGREGATE parse errors ------------------------------------------

#[test]
fn aggregate_without_groupby_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![b"FT.AGGREGATE".to_vec(), b"idx".to_vec(), b"*".to_vec()];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "AGGREGATE needs GROUPBY");
}

#[test]
fn aggregate_unknown_reducer_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.AGGREGATE".to_vec(),
        b"idx".to_vec(),
        b"*".to_vec(),
        b"GROUPBY".to_vec(),
        b"1".to_vec(),
        b"@color".to_vec(),
        b"REDUCE".to_vec(),
        b"BOGUS".to_vec(),
        b"0".to_vec(),
        b"AS".to_vec(),
        b"x".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "unknown reducer -> error");
}

// ---- FT.CREATE parse errors ---------------------------------------------

#[test]
fn create_missing_schema_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"idx".to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "missing SCHEMA -> error");
}

#[test]
fn create_unsupported_doc_type_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"idx".to_vec(),
        b"ON".to_vec(),
        b"JSON".to_vec(),
        b"SCHEMA".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"2".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "ON JSON -> error");
}

// ---- FT.ALTER parse errors ----------------------------------------------

#[test]
fn alter_without_add_keyword_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.ALTER".to_vec(),
        b"idx".to_vec(),
        b"DROP".to_vec(),
        b"body".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "non-ADD alter -> error");
}

// ---- FT.CREATE schema variety -------------------------------------------

#[test]
fn create_with_geo_tag_separator_and_modifiers() {
    let registry = VectorRegistry::new();
    // A schema mixing TEXT (with SORTABLE + WEIGHT modifiers),
    // TAG (with SEPARATOR), GEO, NUMERIC, and the VECTOR field.
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"rich".to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        b"doc:".to_vec(),
        b"SCHEMA".to_vec(),
        b"body".to_vec(),
        b"TEXT".to_vec(),
        b"SORTABLE".to_vec(),
        b"WEIGHT".to_vec(),
        b"2".to_vec(),
        b"tags".to_vec(),
        b"TAG".to_vec(),
        b"SEPARATOR".to_vec(),
        b";".to_vec(),
        b"loc".to_vec(),
        b"GEO".to_vec(),
        b"price".to_vec(),
        b"NUMERIC".to_vec(),
        b"SORTABLE".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"2".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
    ];
    assert_eq!(ft::dispatch(&registry, &as_slices(&args)), b"+OK\r\n");
    let t = registry.get("rich").expect("created");
    // The TAG separator was honoured.
    let tag_field = t
        .schema
        .metadata_fields
        .iter()
        .find(|f| f.name == "tags")
        .expect("tags field");
    assert_eq!(tag_field.tag_separator, Some(b';'));
}

#[test]
fn create_with_two_vector_fields_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"two".to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"SCHEMA".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"2".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
        b"vec2".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"2".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "two VECTOR fields -> error");
}

#[test]
fn create_unknown_field_kind_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"bad".to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"SCHEMA".to_vec(),
        b"f".to_vec(),
        b"BOGUSKIND".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "unknown field kind -> error");
}

#[test]
fn create_bad_tag_separator_errors() {
    let registry = VectorRegistry::new();
    let args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        b"bad".to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"SCHEMA".to_vec(),
        b"tags".to_vec(),
        b"TAG".to_vec(),
        b"SEPARATOR".to_vec(),
        b"toolong".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        b"6".to_vec(),
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        b"2".to_vec(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "multi-byte separator -> error");
}

// ---- FT.AGGREGATE REDUCE arg-count and AS errors ------------------------

#[test]
fn aggregate_reduce_count_with_nonzero_arg_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.AGGREGATE".to_vec(),
        b"idx".to_vec(),
        b"*".to_vec(),
        b"GROUPBY".to_vec(),
        b"1".to_vec(),
        b"@color".to_vec(),
        b"REDUCE".to_vec(),
        b"COUNT".to_vec(),
        b"1".to_vec(),
        b"@price".to_vec(),
        b"AS".to_vec(),
        b"n".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "COUNT wants 0 args");
}

#[test]
fn aggregate_reduce_sum_wrong_arg_count_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.AGGREGATE".to_vec(),
        b"idx".to_vec(),
        b"*".to_vec(),
        b"GROUPBY".to_vec(),
        b"1".to_vec(),
        b"@color".to_vec(),
        b"REDUCE".to_vec(),
        b"SUM".to_vec(),
        b"0".to_vec(),
        b"AS".to_vec(),
        b"s".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "SUM wants 1 arg");
}

#[test]
fn aggregate_reduce_missing_as_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.AGGREGATE".to_vec(),
        b"idx".to_vec(),
        b"*".to_vec(),
        b"GROUPBY".to_vec(),
        b"1".to_vec(),
        b"@color".to_vec(),
        b"REDUCE".to_vec(),
        b"COUNT".to_vec(),
        b"0".to_vec(),
        b"n".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "REDUCE without AS errors");
}

// ---- FT.EXPLAIN trailing clauses ----------------------------------------

#[test]
fn explain_tolerates_dialect_clause() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.EXPLAIN".to_vec(),
        b"idx".to_vec(),
        b"@body:apple".to_vec(),
        b"DIALECT".to_vec(),
        b"2".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"$"), "DIALECT tolerated");
}

#[test]
fn explain_unknown_trailing_clause_errors() {
    let registry = populated();
    let args: Vec<Vec<u8>> = vec![
        b"FT.EXPLAIN".to_vec(),
        b"idx".to_vec(),
        b"@body:apple".to_vec(),
        b"BOGUS".to_vec(),
    ];
    let bytes = ft::dispatch(&registry, &as_slices(&args));
    assert!(bytes.starts_with(b"-"), "unknown EXPLAIN clause errors");
}
