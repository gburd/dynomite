//! Filter-expression grammar tests for `ft_filter`.
//!
//! `parse_expr` is exercised directly across every AST shape
//! (numeric range with all four bound kinds, text substring,
//! tag set, AND / OR / NOT, parenthesised grouping) and its
//! syntax-error branches. `evaluate` is driven against a small
//! populated index so each leaf kind, the field-kind guards,
//! and the boolean combinators all run. `NumericBound::satisfies`
//! is pinned across its bound matrix and as a hegel property.

use std::collections::BTreeSet;
use std::collections::HashMap;

use dynomite_search::ft_filter::{self, FilterExpr, NumericBound};
use dynomite_search::registry::VectorRegistry;
use dynomite_search::schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorSchema, VectorType,
};
use dynomite_search::VectorTable;

use hegel::generators as gs;
use hegel::TestCase;

/// Build a table with a TEXT field "body", a TAG field
/// "color" (pipe-separated multi-tag), and a NUMERIC field
/// "price".
fn table_with_fields() -> std::sync::Arc<VectorTable> {
    let registry = VectorRegistry::new();
    let schema = VectorSchema {
        vector_field: "vec".to_string(),
        vector_type: VectorType::Float32,
        dim: 2,
        distance: DistanceMetric::L2,
        algorithm: IndexAlgorithm::Hnsw,
        prefixes: vec![b"doc:".to_vec()],
        metadata_fields: vec![
            MetadataField {
                name: "body".to_string(),
                field_type: MetadataFieldType::Text,
                tag_separator: None,
            },
            MetadataField {
                name: "color".to_string(),
                field_type: MetadataFieldType::Tag,
                tag_separator: Some(b'|'),
            },
            MetadataField {
                name: "price".to_string(),
                field_type: MetadataFieldType::Numeric,
                tag_separator: None,
            },
        ],
    };
    registry.create("idx".to_string(), schema).expect("create");
    let table = registry.get("idx").expect("table");

    let feed = |key: &[u8], vec2: &[f32], body: &str, color: &str, price: &str| {
        let mut meta = HashMap::new();
        meta.insert(
            "body".to_string(),
            serde_json::Value::String(body.to_string()),
        );
        meta.insert(
            "color".to_string(),
            serde_json::Value::String(color.to_string()),
        );
        meta.insert(
            "price".to_string(),
            serde_json::Value::String(price.to_string()),
        );
        table
            .engine
            .upsert(key.to_vec(), vec2, meta)
            .expect("upsert");
        table.upsert_text_field("body", key, body.as_bytes());
        table.record_indexed_key(key.to_vec());
    };

    feed(b"doc:1", &[0.0, 0.0], "red apple", "red|fruit", "10");
    feed(b"doc:2", &[1.0, 1.0], "green apple", "green|fruit", "20");
    feed(b"doc:3", &[2.0, 2.0], "red berry", "red", "30");
    table
}

fn universe(table: &VectorTable) -> BTreeSet<Vec<u8>> {
    table.indexed_keys().into_iter().collect()
}

fn eval(table: &VectorTable, query: &[u8]) -> BTreeSet<Vec<u8>> {
    let expr = ft_filter::parse_expr(query).expect("parse");
    ft_filter::evaluate(&expr, table, &universe(table)).expect("evaluate")
}

// ---- parse_expr AST shapes ----------------------------------------------

#[test]
fn parse_star_is_all() {
    assert_eq!(ft_filter::parse_expr(b"*").unwrap(), FilterExpr::All);
}

#[test]
fn parse_numeric_range_inclusive() {
    let e = ft_filter::parse_expr(b"@price:[10 20]").unwrap();
    let FilterExpr::NumericRange { field, min, max } = e else {
        panic!("expected NumericRange");
    };
    assert_eq!(field, "price");
    assert_eq!(min, NumericBound::Inclusive(10.0));
    assert_eq!(max, NumericBound::Inclusive(20.0));
}

#[test]
fn parse_numeric_range_exclusive_and_infinities() {
    let e = ft_filter::parse_expr(b"@price:[(10 +inf]").unwrap();
    let FilterExpr::NumericRange { min, max, .. } = e else {
        panic!("expected NumericRange");
    };
    assert_eq!(min, NumericBound::Exclusive(10.0));
    assert_eq!(max, NumericBound::PosInf);

    let e2 = ft_filter::parse_expr(b"@price:[-inf (30]").unwrap();
    let FilterExpr::NumericRange { min, max, .. } = e2 else {
        panic!("expected NumericRange");
    };
    assert_eq!(min, NumericBound::NegInf);
    assert_eq!(max, NumericBound::Exclusive(30.0));
}

#[test]
fn parse_tag_set() {
    let e = ft_filter::parse_expr(b"@color:{red|green}").unwrap();
    let FilterExpr::TagSet { field, tags } = e else {
        panic!("expected TagSet");
    };
    assert_eq!(field, "color");
    assert_eq!(tags, vec![b"red".to_vec(), b"green".to_vec()]);
}

#[test]
fn parse_text_substring() {
    let e = ft_filter::parse_expr(b"@body:apple").unwrap();
    let FilterExpr::TextSubstring { field, query } = e else {
        panic!("expected TextSubstring");
    };
    assert_eq!(field, "body");
    assert_eq!(query, b"apple");
}

#[test]
fn parse_and_or_not_and_grouping() {
    // AND via whitespace.
    let and = ft_filter::parse_expr(b"@body:apple @color:{red}").unwrap();
    assert!(matches!(and, FilterExpr::And(ref v) if v.len() == 2));

    // OR via pipe between top-level terms.
    let or = ft_filter::parse_expr(b"@color:{red} | @color:{green}").unwrap();
    assert!(matches!(or, FilterExpr::Or(ref v) if v.len() == 2));

    // NOT via leading minus.
    let not = ft_filter::parse_expr(b"-@color:{red}").unwrap();
    assert!(matches!(not, FilterExpr::Not(_)));

    // Parenthesised grouping round-trips to an inner expr.
    let grouped = ft_filter::parse_expr(b"(@body:apple)").unwrap();
    assert!(matches!(grouped, FilterExpr::TextSubstring { .. }));
}

// ---- parse_expr error branches ------------------------------------------

#[test]
fn parse_rejects_extra_tokens() {
    // A trailing non-whitespace token after a complete expr.
    let err = ft_filter::parse_expr(b"* )").unwrap_err();
    assert!(format!("{err}").contains("filter") || format!("{err}").contains("syntax"));
}

#[test]
fn parse_rejects_unbalanced_paren() {
    assert!(ft_filter::parse_expr(b"(@body:apple").is_err());
}

#[test]
fn parse_rejects_bad_numeric_range() {
    // Non-numeric bound.
    assert!(ft_filter::parse_expr(b"@price:[abc 20]").is_err());
    // Missing closing bracket.
    assert!(ft_filter::parse_expr(b"@price:[10 20").is_err());
}

#[test]
fn parse_rejects_unterminated_tag_set() {
    assert!(ft_filter::parse_expr(b"@color:{red").is_err());
}

#[test]
fn parse_rejects_missing_field_marker() {
    // A bare colon with no @field.
    assert!(ft_filter::parse_expr(b":foo").is_err());
}

// ---- evaluate against a populated table ---------------------------------

#[test]
fn evaluate_numeric_range_selects_rows() {
    let table = table_with_fields();
    // price in [15, 30] -> doc:2 (20), doc:3 (30).
    let hits = eval(&table, b"@price:[15 30]");
    assert_eq!(hits, [b"doc:2".to_vec(), b"doc:3".to_vec()].into());
}

#[test]
fn evaluate_exclusive_bounds() {
    let table = table_with_fields();
    // price in ((10) (30)) -> only doc:2 (20).
    let hits = eval(&table, b"@price:[(10 (30]");
    assert_eq!(hits, [b"doc:2".to_vec()].into());
}

#[test]
fn evaluate_tag_set_membership() {
    let table = table_with_fields();
    // color in {red} -> doc:1 (red|fruit), doc:3 (red).
    let hits = eval(&table, b"@color:{red}");
    assert_eq!(hits, [b"doc:1".to_vec(), b"doc:3".to_vec()].into());
}

#[test]
fn evaluate_text_substring() {
    let table = table_with_fields();
    // body contains "apple" -> doc:1, doc:2.
    let hits = eval(&table, b"@body:apple");
    assert_eq!(hits, [b"doc:1".to_vec(), b"doc:2".to_vec()].into());
}

#[test]
fn evaluate_and_or_not() {
    let table = table_with_fields();
    // AND: red apple -> doc:1 only.
    assert_eq!(
        eval(&table, b"@body:apple @color:{red}"),
        [b"doc:1".to_vec()].into()
    );
    // OR: green or berry-bodied -> doc:2 (green) + doc:3 (berry).
    assert_eq!(
        eval(&table, b"@color:{green} | @body:berry"),
        [b"doc:2".to_vec(), b"doc:3".to_vec()].into()
    );
    // NOT red -> doc:2.
    assert_eq!(eval(&table, b"-@color:{red}"), [b"doc:2".to_vec()].into());
}

#[test]
fn evaluate_all_returns_universe() {
    let table = table_with_fields();
    assert_eq!(eval(&table, b"*"), universe(&table));
}

#[test]
fn evaluate_wrong_field_kind_errors() {
    let table = table_with_fields();
    let uni = universe(&table);
    // Numeric range against a TEXT field is a type error.
    let bad = ft_filter::parse_expr(b"@body:[1 2]").unwrap();
    assert!(ft_filter::evaluate(&bad, &table, &uni).is_err());
    // Tag set against a NUMERIC field is a type error.
    let bad2 = ft_filter::parse_expr(b"@price:{x}").unwrap();
    assert!(ft_filter::evaluate(&bad2, &table, &uni).is_err());
}

// ---- NumericBound::satisfies matrix -------------------------------------

#[test]
fn numeric_bound_satisfies_matrix() {
    // Infinities.
    assert!(NumericBound::NegInf.satisfies(0.0, true));
    assert!(!NumericBound::NegInf.satisfies(0.0, false));
    assert!(NumericBound::PosInf.satisfies(0.0, false));
    assert!(!NumericBound::PosInf.satisfies(0.0, true));
    // Inclusive.
    assert!(NumericBound::Inclusive(5.0).satisfies(5.0, true));
    assert!(NumericBound::Inclusive(5.0).satisfies(5.0, false));
    assert!(!NumericBound::Inclusive(5.0).satisfies(4.0, true));
    assert!(!NumericBound::Inclusive(5.0).satisfies(6.0, false));
    // Exclusive.
    assert!(!NumericBound::Exclusive(5.0).satisfies(5.0, true));
    assert!(!NumericBound::Exclusive(5.0).satisfies(5.0, false));
    assert!(NumericBound::Exclusive(5.0).satisfies(6.0, true));
    assert!(NumericBound::Exclusive(5.0).satisfies(4.0, false));
}

/// Property: for any inclusive `[lo, hi]` range and any value,
/// the value is selected exactly when `lo <= v <= hi`. This
/// pins the lower/upper-bound interaction of
/// `NumericBound::satisfies` over the whole real line.
#[hegel::test(test_cases = 256)]
fn numeric_bound_inclusive_range_matches_arithmetic(tc: TestCase) {
    let lo = f64::from(tc.draw(gs::integers::<i32>().min_value(-1000).max_value(1000)));
    let hi = f64::from(tc.draw(gs::integers::<i32>().min_value(-1000).max_value(1000)));
    let v = f64::from(tc.draw(gs::integers::<i32>().min_value(-2000).max_value(2000)));

    let lower = NumericBound::Inclusive(lo);
    let upper = NumericBound::Inclusive(hi);
    let selected = lower.satisfies(v, true) && upper.satisfies(v, false);
    assert_eq!(selected, v >= lo && v <= hi);
}

// ---- additional parse-error branches ------------------------------------

#[test]
fn parse_rejects_quoted_phrases_as_unsupported() {
    // A quoted phrase at atom position is explicitly unsupported.
    assert!(ft_filter::parse_expr(b"\"hello world\"").is_err());
    // And as a field value.
    assert!(ft_filter::parse_expr(b"@body:\"quoted\"").is_err());
}

#[test]
fn parse_rejects_bare_unqualified_term() {
    // A bare term not qualified by @field: is a syntax error.
    assert!(ft_filter::parse_expr(b"apple").is_err());
}

#[test]
fn parse_rejects_unexpected_leading_chars() {
    for bad in [&b"{x}"[..], &b"[1 2]"[..], &b"|"[..], &b")"[..], &b":x"[..]] {
        assert!(ft_filter::parse_expr(bad).is_err(), "{bad:?} should fail");
    }
}

#[test]
fn parse_rejects_empty_field_and_value() {
    // Empty field name after '@'.
    assert!(ft_filter::parse_expr(b"@:value").is_err());
    // Empty value after the colon.
    assert!(ft_filter::parse_expr(b"@body:").is_err());
    // Missing colon after the field.
    assert!(ft_filter::parse_expr(b"@body").is_err());
}

#[test]
fn parse_rejects_empty_expression() {
    assert!(ft_filter::parse_expr(b"").is_err());
    assert!(ft_filter::parse_expr(b"   ").is_err());
}

#[test]
fn parse_negation_binds_tightly() {
    // `-@a:apple @b:berry` parses as `(Not a) AND b`, not
    // `Not (a AND b)`.
    let e = ft_filter::parse_expr(b"-@body:apple @body:berry").unwrap();
    let FilterExpr::And(children) = e else {
        panic!("expected And at top level");
    };
    assert_eq!(children.len(), 2);
    assert!(matches!(children[0], FilterExpr::Not(_)));
    assert!(matches!(children[1], FilterExpr::TextSubstring { .. }));
}

#[test]
fn evaluate_grouped_or_inside_and() {
    let table = table_with_fields();
    // (@color:{red} | @color:{green}) @body:apple
    // -> docs that are red-or-green AND have apple in body
    //    = doc:1 (red apple), doc:2 (green apple).
    let hits = eval(&table, b"(@color:{red} | @color:{green}) @body:apple");
    assert_eq!(hits, [b"doc:1".to_vec(), b"doc:2".to_vec()].into());
}

// ---- numeric-bound and tag-set edge errors ------------------------------

#[test]
fn parse_rejects_geo_style_third_token_in_range() {
    // A third value inside [...] is read as a geo filter and
    // rejected as Unsupported.
    assert!(ft_filter::parse_expr(b"@price:[10 20 30]").is_err());
}

#[test]
fn parse_rejects_exclusive_infinity() {
    // '(' cannot be applied to an infinity bound.
    assert!(ft_filter::parse_expr(b"@price:[(+inf 20]").is_err());
    assert!(ft_filter::parse_expr(b"@price:[(-inf 20]").is_err());
}

#[test]
fn parse_accepts_plain_inf_bounds() {
    let e = ft_filter::parse_expr(b"@price:[inf +inf]").unwrap();
    let FilterExpr::NumericRange { min, max, .. } = e else {
        panic!("expected NumericRange");
    };
    assert_eq!(min, NumericBound::PosInf);
    assert_eq!(max, NumericBound::PosInf);
}

#[test]
fn parse_rejects_empty_tag() {
    // An empty tag between separators.
    assert!(ft_filter::parse_expr(b"@color:{red||green}").is_err());
    // An empty tag set.
    assert!(ft_filter::parse_expr(b"@color:{}").is_err());
}

#[test]
fn parse_multi_tag_with_whitespace() {
    let e = ft_filter::parse_expr(b"@color:{ red | green | blue }").unwrap();
    let FilterExpr::TagSet { tags, .. } = e else {
        panic!("expected TagSet");
    };
    assert_eq!(
        tags,
        vec![b"red".to_vec(), b"green".to_vec(), b"blue".to_vec()]
    );
}
