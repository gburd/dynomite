//! Built-in MapReduce phase functions.
//!
//! The set is intentionally small but complete enough to express
//! useful jobs: extract values, count, sum, sort, set-union,
//! per-field projection. The set grows in follow-up slices as new
//! demand surfaces.
//!
//! Naming follows Riak's convention: `map_*` for map functions,
//! `reduce_*` for reduce functions. Riak's stock module is
//! `riak_kv_mapreduce`; the names here are the unqualified suffix
//! so callers can ignore the module.
//!
//! [`default_registry`] returns a [`crate::mapreduce::PhaseRegistry`]
//! pre-populated with every built-in below. Hosts that want a
//! restricted set can build their own registry and register the
//! subset they want.

use std::sync::Arc;

use serde_json::Value;

use crate::mapreduce::executor::MrError;
use crate::mapreduce::registry::{MapFn, PhaseRegistry, ReduceFn};

/// Build a [`PhaseRegistry`] populated with every built-in map and
/// reduce function.
#[must_use]
pub fn default_registry() -> PhaseRegistry {
    let mut r = PhaseRegistry::new();

    // Map functions.
    let f: MapFn = Arc::new(map_object_value);
    r.register_map("map_object_value", f);
    let f: MapFn = Arc::new(map_object_value_list);
    r.register_map("map_object_value_list", f);
    let f: MapFn = Arc::new(map_extract_field);
    r.register_map("map_extract_field", f);
    let f: MapFn = Arc::new(map_identity);
    r.register_map("map_identity", f);

    // Reduce functions.
    let f: ReduceFn = Arc::new(reduce_count);
    r.register_reduce("reduce_count", f);
    let f: ReduceFn = Arc::new(reduce_sum);
    r.register_reduce("reduce_sum", f);
    let f: ReduceFn = Arc::new(reduce_sort);
    r.register_reduce("reduce_sort", f);
    let f: ReduceFn = Arc::new(reduce_set_union);
    r.register_reduce("reduce_set_union", f);
    let f: ReduceFn = Arc::new(reduce_identity);
    r.register_reduce("reduce_identity", f);

    r
}

// ----------------------------------------------------------------
// Map functions
// ----------------------------------------------------------------

/// `map_object_value` -- extract the `value` field from a Riak object
/// JSON shape and emit it as the result. Inputs missing a `value`
/// field emit JSON `null`.
pub fn map_object_value(input: &Value, _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    Ok(vec![input.get("value").cloned().unwrap_or(Value::Null)])
}

/// `map_object_value_list` -- emit the value(s) of an object. If the
/// `value` is a JSON array, every element is emitted in order; if
/// scalar, the scalar is emitted; if absent, nothing is emitted.
pub fn map_object_value_list(input: &Value, _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    match input.get("value") {
        Some(Value::Array(items)) => Ok(items.clone()),
        Some(Value::Null) | None => Ok(Vec::new()),
        Some(other) => Ok(vec![other.clone()]),
    }
}

/// `map_extract_field` -- parse the object's `value` as JSON and
/// emit `value[field]`. The field name is taken from the phase
/// `arg` (either a JSON string or a JSON object `{"field": "..."}`).
///
/// If `value` is already a JSON object the field is read directly.
/// If it is a string, we attempt to parse it as JSON first; if
/// parsing fails we treat the string as opaque and emit JSON null.
pub fn map_extract_field(input: &Value, arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    let field = extract_field_name(arg)?;
    let value_field = input.get("value").cloned().unwrap_or(Value::Null);

    let parsed = match value_field {
        Value::Object(_) | Value::Array(_) => value_field,
        Value::String(s) => serde_json::from_str(&s).unwrap_or(Value::Null),
        Value::Null => Value::Null,
        other => other,
    };

    let extracted = parsed.get(&field).cloned().unwrap_or(Value::Null);
    Ok(vec![extracted])
}

/// `map_identity` -- emit the input unchanged. Useful as a passthrough
/// stage when assembling a longer pipeline programmatically.
pub fn map_identity(input: &Value, _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    Ok(vec![input.clone()])
}

// ----------------------------------------------------------------
// Reduce functions
// ----------------------------------------------------------------

/// `reduce_count` -- emit a single integer: the number of inbound
/// items.
pub fn reduce_count(inputs: &[Value], _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    let n = u64::try_from(inputs.len()).map_err(|e| MrError::Json(format!("count: {e}")))?;
    Ok(vec![Value::from(n)])
}

/// `reduce_sum` -- emit a single number: the sum of every numeric
/// inbound item. Non-numeric items are an error.
pub fn reduce_sum(inputs: &[Value], _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    let mut int_acc: i64 = 0;
    let mut float_acc: f64 = 0.0;
    let mut have_float = false;

    for v in inputs {
        match v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    int_acc = int_acc
                        .checked_add(i)
                        .ok_or_else(|| MrError::Json("sum overflowed i64".into()))?;
                } else if let Some(u) = n.as_u64() {
                    let i = i64::try_from(u)
                        .map_err(|_| MrError::Json("sum: u64 input out of i64 range".into()))?;
                    int_acc = int_acc
                        .checked_add(i)
                        .ok_or_else(|| MrError::Json("sum overflowed i64".into()))?;
                } else if let Some(f) = n.as_f64() {
                    have_float = true;
                    float_acc += f;
                } else {
                    return Err(MrError::Json("sum: numeric value not representable".into()));
                }
            }
            Value::Null => {
                // Skip nulls so a `map_object_value` over keys that
                // had no value field does not break the sum.
            }
            other => {
                return Err(MrError::Json(format!("sum: non-numeric input: {other}")));
            }
        }
    }

    if have_float {
        // Mix integer accumulator into the float side. Loss of
        // precision for very large integer sums is documented
        // behaviour: clients that want exact integer arithmetic
        // should not mix floats in.
        let int_as_float = int_to_float_lossy(int_acc);
        let total = float_acc + int_as_float;
        let n = serde_json::Number::from_f64(total)
            .ok_or_else(|| MrError::Json("sum: NaN or infinity in result".into()))?;
        Ok(vec![Value::Number(n)])
    } else {
        Ok(vec![Value::Number(int_acc.into())])
    }
}

/// Convert an `i64` to `f64` for the mixed-numeric path. The
/// precision loss is intentional and documented in the calling
/// site; this helper exists so the conversion has a single seam
/// the reviewer can reason about.
fn int_to_float_lossy(i: i64) -> f64 {
    // The cast is the standard idiom and the only way to mix
    // integer and floating-point in JSON-numeric arithmetic.
    #[allow(clippy::cast_precision_loss)]
    let r = i as f64;
    r
}

/// `reduce_sort` -- emit every inbound item, sorted in JSON-canonical
/// order. The sort is stable.
pub fn reduce_sort(inputs: &[Value], _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    let mut sorted = inputs.to_vec();
    sorted.sort_by(canonical_compare);
    Ok(sorted)
}

/// `reduce_set_union` -- emit every distinct inbound item, with
/// duplicates removed. Order matches the first-seen order of each
/// distinct item.
pub fn reduce_set_union(inputs: &[Value], _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    let mut out: Vec<Value> = Vec::new();
    for v in inputs {
        if !out.iter().any(|existing| existing == v) {
            out.push(v.clone());
        }
    }
    Ok(out)
}

/// `reduce_identity` -- emit every inbound item unchanged. Useful as
/// a passthrough to flatten captured-output groupings.
pub fn reduce_identity(inputs: &[Value], _arg: Option<&Value>) -> Result<Vec<Value>, MrError> {
    Ok(inputs.to_vec())
}

// ----------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------

/// Pull the field name out of a phase `arg`.
///
/// Accepts a plain JSON string `"name"` or a JSON object `{"field":
/// "name"}`. Anything else surfaces as a typed error.
fn extract_field_name(arg: Option<&Value>) -> Result<String, MrError> {
    let arg = arg.ok_or_else(|| MrError::Json("map_extract_field requires arg".into()))?;
    match arg {
        Value::String(s) => Ok(s.clone()),
        Value::Object(map) => match map.get("field") {
            Some(Value::String(s)) => Ok(s.clone()),
            _ => Err(MrError::Json(
                "map_extract_field: arg.field must be a string".into(),
            )),
        },
        _ => Err(MrError::Json(
            "map_extract_field: arg must be a string or {field: ...}".into(),
        )),
    }
}

/// Canonical JSON ordering used by [`reduce_sort`].
///
/// The order is: null < bool < number < string < array < object,
/// with totals within a kind ordered the obvious way. This is a
/// total order (unlike `serde_json::Value`'s default `PartialOrd`,
/// which the type does not even implement).
fn canonical_compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::{Equal, Greater, Less};
    fn rank(v: &Value) -> u8 {
        match v {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Number(_) => 2,
            Value::String(_) => 3,
            Value::Array(_) => 4,
            Value::Object(_) => 5,
        }
    }
    let ra = rank(a);
    let rb = rank(b);
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Number(x), Value::Number(y)) => {
            // Compare via f64; ties are broken by the textual form
            // so the order is deterministic for ints that share an
            // f64 representation.
            let xf = x.as_f64().unwrap_or(0.0);
            let yf = y.as_f64().unwrap_or(0.0);
            xf.partial_cmp(&yf)
                .unwrap_or(Equal)
                .then_with(|| x.to_string().cmp(&y.to_string()))
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => {
            for (xa, yb) in x.iter().zip(y.iter()) {
                let cmp = canonical_compare(xa, yb);
                if cmp != Equal {
                    return cmp;
                }
            }
            x.len().cmp(&y.len())
        }
        (Value::Object(x), Value::Object(y)) => {
            // Compare key sets first, then values for shared keys.
            // We delegate to the JSON serialisation so the order is
            // total and deterministic.
            let xs = serde_json::to_string(x).unwrap_or_default();
            let ys = serde_json::to_string(y).unwrap_or_default();
            xs.cmp(&ys)
        }
        // Different ranks were handled above.
        _ => {
            if ra < rb {
                Less
            } else {
                Greater
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_holds_every_builtin() {
        let r = default_registry();
        for name in [
            "map_object_value",
            "map_object_value_list",
            "map_extract_field",
            "map_identity",
        ] {
            assert!(r.map_fn(name).is_some(), "missing map fn: {name}");
        }
        for name in [
            "reduce_count",
            "reduce_sum",
            "reduce_sort",
            "reduce_set_union",
            "reduce_identity",
        ] {
            assert!(r.reduce_fn(name).is_some(), "missing reduce fn: {name}");
        }
    }

    #[test]
    fn map_object_value_extracts_value_field() {
        let v = serde_json::json!({"bucket": "b", "key": "k", "value": 42});
        let out = map_object_value(&v, None).expect("ok");
        assert_eq!(out, vec![serde_json::json!(42)]);
    }

    #[test]
    fn map_object_value_emits_null_when_absent() {
        let v = serde_json::json!({"bucket": "b", "key": "k"});
        let out = map_object_value(&v, None).expect("ok");
        assert_eq!(out, vec![Value::Null]);
    }

    #[test]
    fn map_object_value_list_flattens_arrays() {
        let v = serde_json::json!({"value": [1, 2, 3]});
        let out = map_object_value_list(&v, None).expect("ok");
        assert_eq!(
            out,
            vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ]
        );
    }

    #[test]
    fn map_object_value_list_passes_scalar_through() {
        let v = serde_json::json!({"value": "hi"});
        let out = map_object_value_list(&v, None).expect("ok");
        assert_eq!(out, vec![serde_json::json!("hi")]);
    }

    #[test]
    fn map_object_value_list_emits_nothing_for_null() {
        let v = serde_json::json!({"value": null});
        let out = map_object_value_list(&v, None).expect("ok");
        assert!(out.is_empty());
    }

    #[test]
    fn map_extract_field_from_object_value() {
        let v = serde_json::json!({"value": {"name": "alice", "age": 30}});
        let out = map_extract_field(&v, Some(&serde_json::json!("name"))).expect("ok");
        assert_eq!(out, vec![serde_json::json!("alice")]);
    }

    #[test]
    fn map_extract_field_from_string_value() {
        let v = serde_json::json!({"value": "{\"name\":\"bob\"}"});
        let out = map_extract_field(&v, Some(&serde_json::json!({"field": "name"}))).expect("ok");
        assert_eq!(out, vec![serde_json::json!("bob")]);
    }

    #[test]
    fn map_extract_field_emits_null_for_missing_field() {
        let v = serde_json::json!({"value": {"x": 1}});
        let out = map_extract_field(&v, Some(&serde_json::json!("y"))).expect("ok");
        assert_eq!(out, vec![Value::Null]);
    }

    #[test]
    fn map_extract_field_requires_arg() {
        let v = serde_json::json!({"value": {"x": 1}});
        let err = map_extract_field(&v, None).expect_err("error");
        assert!(matches!(err, MrError::Json(_)));
    }

    #[test]
    fn reduce_count_returns_zero_for_empty() {
        let out = reduce_count(&[], None).expect("ok");
        assert_eq!(out, vec![serde_json::json!(0)]);
    }

    #[test]
    fn reduce_count_returns_n() {
        let out = reduce_count(
            &[
                serde_json::json!(1),
                serde_json::json!("x"),
                serde_json::json!(true),
            ],
            None,
        )
        .expect("ok");
        assert_eq!(out, vec![serde_json::json!(3)]);
    }

    #[test]
    fn reduce_sum_integers() {
        let out = reduce_sum(
            &[
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ],
            None,
        )
        .expect("ok");
        assert_eq!(out, vec![serde_json::json!(6)]);
    }

    #[test]
    fn reduce_sum_skips_nulls() {
        let out = reduce_sum(
            &[
                serde_json::json!(1),
                Value::Null,
                serde_json::json!(2),
                Value::Null,
            ],
            None,
        )
        .expect("ok");
        assert_eq!(out, vec![serde_json::json!(3)]);
    }

    #[test]
    fn reduce_sum_floats() {
        let out = reduce_sum(&[serde_json::json!(1.5), serde_json::json!(2.5)], None).expect("ok");
        assert_eq!(out.len(), 1);
        let n = out[0].as_f64().expect("f64");
        assert!((n - 4.0).abs() < 1e-9);
    }

    #[test]
    fn reduce_sum_rejects_non_numeric() {
        let err = reduce_sum(&[serde_json::json!("nope")], None).expect_err("error");
        assert!(matches!(err, MrError::Json(_)));
    }

    #[test]
    fn reduce_sort_orders_numbers() {
        let out = reduce_sort(
            &[
                serde_json::json!(3),
                serde_json::json!(1),
                serde_json::json!(2),
            ],
            None,
        )
        .expect("ok");
        assert_eq!(
            out,
            vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ]
        );
    }

    #[test]
    fn reduce_sort_orders_mixed_types_by_rank() {
        let out = reduce_sort(
            &[
                serde_json::json!("z"),
                serde_json::json!(1),
                serde_json::json!(null),
                serde_json::json!(true),
            ],
            None,
        )
        .expect("ok");
        // null < bool < number < string
        assert!(out[0].is_null());
        assert!(out[1].is_boolean());
        assert!(out[2].is_number());
        assert!(out[3].is_string());
    }

    #[test]
    fn reduce_set_union_dedupes_preserving_first_seen_order() {
        let out = reduce_set_union(
            &[
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(1),
                serde_json::json!(3),
                serde_json::json!(2),
            ],
            None,
        )
        .expect("ok");
        assert_eq!(
            out,
            vec![
                serde_json::json!(1),
                serde_json::json!(2),
                serde_json::json!(3),
            ]
        );
    }

    #[test]
    fn reduce_set_union_handles_complex_values() {
        let out = reduce_set_union(
            &[
                serde_json::json!({"a": 1}),
                serde_json::json!({"a": 1}),
                serde_json::json!({"a": 2}),
            ],
            None,
        )
        .expect("ok");
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn reduce_identity_passes_through() {
        let inputs = vec![
            serde_json::json!(1),
            serde_json::json!("x"),
            serde_json::json!({"a": 1}),
        ];
        let out = reduce_identity(&inputs, None).expect("ok");
        assert_eq!(out, inputs);
    }

    #[test]
    fn map_identity_passes_through() {
        let v = serde_json::json!({"any": "value"});
        let out = map_identity(&v, None).expect("ok");
        assert_eq!(out, vec![v]);
    }

    #[test]
    fn map_extract_field_from_array_value_reads_index_like_field() {
        // A `value` that is already a JSON array stays as-is; reading
        // a non-existent string field yields null (covers the
        // Array(_) arm of the parse match).
        let v = serde_json::json!({"value": [1, 2, 3]});
        let out = map_extract_field(&v, Some(&serde_json::json!("name"))).expect("ok");
        assert_eq!(out, vec![Value::Null]);
    }

    #[test]
    fn map_extract_field_opaque_string_value_is_null() {
        // A `value` string that is not JSON parses to null.
        let v = serde_json::json!({"value": "not json at all"});
        let out = map_extract_field(&v, Some(&serde_json::json!("name"))).expect("ok");
        assert_eq!(out, vec![Value::Null]);
    }

    #[test]
    fn map_extract_field_scalar_value_passes_through_to_null() {
        // A scalar (non-object/array/string) `value` hits the `other`
        // arm and then has no field, yielding null.
        let v = serde_json::json!({"value": 7});
        let out = map_extract_field(&v, Some(&serde_json::json!("name"))).expect("ok");
        assert_eq!(out, vec![Value::Null]);
    }

    #[test]
    fn map_extract_field_rejects_object_arg_without_string_field() {
        let v = serde_json::json!({"value": {"x": 1}});
        let err = map_extract_field(&v, Some(&serde_json::json!({"field": 5})))
            .expect_err("non-string field");
        assert!(matches!(err, MrError::Json(_)));
    }

    #[test]
    fn map_extract_field_rejects_non_string_non_object_arg() {
        let v = serde_json::json!({"value": {"x": 1}});
        let err = map_extract_field(&v, Some(&serde_json::json!(42))).expect_err("bad arg shape");
        assert!(matches!(err, MrError::Json(_)));
    }

    #[test]
    fn reduce_sum_rejects_u64_above_i64_range() {
        // A JSON integer above i64::MAX is stored as a u64; the sum
        // path cannot fit it into the i64 accumulator and rejects it.
        let too_big: u64 = u64::try_from(i64::MAX).expect("i64::MAX fits u64") + 1;
        let err = reduce_sum(&[serde_json::json!(too_big)], None)
            .expect_err("u64 over i64 range is rejected");
        assert!(matches!(err, MrError::Json(_)));
    }

    #[test]
    fn map_extract_field_null_value_yields_null() {
        // A `value` that is explicitly JSON null hits the Null arm of
        // the parse match and extracts to null.
        let v = serde_json::json!({"value": null});
        let out = map_extract_field(&v, Some(&serde_json::json!("name"))).expect("ok");
        assert_eq!(out, vec![Value::Null]);
    }

    #[test]
    fn reduce_sum_mixes_int_and_float() {
        let out = reduce_sum(&[serde_json::json!(10), serde_json::json!(2.5)], None).expect("ok");
        let n = out[0].as_f64().expect("f64");
        assert!((n - 12.5).abs() < 1e-9);
    }

    #[test]
    fn reduce_sort_breaks_number_ties_deterministically() {
        // 1 and 1.0 share an f64 value; the tie-break by textual form
        // keeps the order total and stable.
        let a = reduce_sort(&[serde_json::json!(1.0), serde_json::json!(1)], None).expect("ok");
        let b = reduce_sort(&[serde_json::json!(1), serde_json::json!(1.0)], None).expect("ok");
        assert_eq!(a, b, "sort order is independent of input order");
    }

    #[test]
    fn reduce_sort_orders_arrays_and_objects() {
        // Arrays compared element-wise then by length; objects by
        // canonical serialisation; equal nulls/bools are stable.
        let out = reduce_sort(
            &[
                serde_json::json!([1, 2]),
                serde_json::json!([1]),
                serde_json::json!({"b": 2}),
                serde_json::json!({"a": 1}),
                serde_json::json!(null),
                serde_json::json!(null),
                serde_json::json!(true),
                serde_json::json!(false),
                serde_json::json!("s"),
            ],
            None,
        )
        .expect("ok");
        // null, null, bool, bool, string, array, array, object, object
        assert!(out[0].is_null() && out[1].is_null());
        assert!(out[2].is_boolean() && out[3].is_boolean());
        assert!(out[4].is_string());
        // [1] sorts before [1,2] (shorter array is less when prefix equal).
        assert_eq!(out[5], serde_json::json!([1]));
        assert_eq!(out[6], serde_json::json!([1, 2]));
        // {"a":1} sorts before {"b":2} by canonical serialisation.
        assert_eq!(out[7], serde_json::json!({"a": 1}));
        assert_eq!(out[8], serde_json::json!({"b": 2}));
    }
}
