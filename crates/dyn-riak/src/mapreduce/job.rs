//! Job and result types for the MapReduce pipeline.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mapreduce::phase::Phase;

/// A single inline input item carrying an optional bucket / key /
/// pre-fetched value. Mirrors Riak's `[bucket, key, keydata]`
/// triple.
///
/// When `value` is `None` and the input is not inline, the executor
/// has nothing to feed downstream and emits a JSON object of the
/// form `{"bucket": ..., "key": ..., "value": null}` so downstream
/// phases can still see the routing metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyDatum {
    /// Bucket name. Required.
    pub bucket: String,
    /// Object key. Required.
    pub key: String,
    /// Inline value, if the caller already has it. JSON-typed.
    #[serde(default)]
    pub value: Option<Value>,
    /// Free-form per-key metadata, threaded through to map functions.
    #[serde(default)]
    pub data: Option<Value>,
}

impl KeyDatum {
    /// Build a [`KeyDatum`] with the given bucket, key, and inline
    /// value.
    #[must_use]
    pub fn with_value(bucket: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            value: Some(value),
            data: None,
        }
    }

    /// Build a [`KeyDatum`] carrying only routing information.
    #[must_use]
    pub fn pair(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            value: None,
            data: None,
        }
    }

    /// Render the datum as the JSON object that flows through the
    /// pipeline: `{"bucket": ..., "key": ..., "value": ..., "data": ...}`.
    /// Missing pieces become JSON `null`.
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::json!({
            "bucket": self.bucket,
            "key": self.key,
            "value": self.value.clone().unwrap_or(Value::Null),
            "data": self.data.clone().unwrap_or(Value::Null),
        })
    }
}

/// MapReduce input source.
///
/// Mirrors Riak's three input shapes:
///
/// * a literal list of `(bucket, key)` pairs
/// * an inline list of [`KeyDatum`] values (with values inline)
/// * a bucket name (all keys in the bucket; not implemented in this
///   slice and rejected at execution time)
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Inputs {
    /// `[[bucket, key], ...]` -- the most common Riak shape.
    Pairs(Vec<(String, String)>),
    /// `[{bucket, key, value?, data?}, ...]` -- inline values.
    KeyData(Vec<KeyDatum>),
    /// `"bucketname"` -- enumerate every key in the bucket.
    /// Not implemented in this slice.
    Bucket(String),
}

impl Inputs {
    /// Enumerate the input items in order.
    ///
    /// Returns `Err` for [`Inputs::Bucket`] because that variant
    /// requires a list-keys path that is not wired through in this
    /// slice; the caller surfaces the failure as
    /// [`crate::mapreduce::MrError::UnsupportedInputs`].
    pub fn items(&self) -> Option<Vec<KeyDatum>> {
        match self {
            Self::Pairs(pairs) => Some(
                pairs
                    .iter()
                    .map(|(b, k)| KeyDatum::pair(b.clone(), k.clone()))
                    .collect(),
            ),
            Self::KeyData(data) => Some(data.clone()),
            Self::Bucket(_) => None,
        }
    }
}

/// A complete MapReduce job: inputs plus an ordered phase list.
///
/// Round-trips through JSON in the shape Riak documents:
///
/// ```json
/// {
///   "inputs":  [["bucket","key"], ...],
///   "query":   [{"map": {...}}, {"reduce": {...}}],
///   "timeout": 60000
/// }
/// ```
///
/// The phase list field is named `query` for parity with Riak.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MapReduceJob {
    /// Pipeline inputs.
    pub inputs: Inputs,
    /// Pipeline phases. Riak names this field `query`.
    #[serde(rename = "query")]
    pub phases: Vec<Phase>,
    /// Per-job timeout in milliseconds. `None` means "use the
    /// executor default".
    #[serde(default, rename = "timeout")]
    pub timeout_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_datum_to_value_fills_nulls() {
        let kd = KeyDatum::pair("b", "k");
        let v = kd.to_value();
        assert_eq!(v["bucket"], "b");
        assert_eq!(v["key"], "k");
        assert!(v["value"].is_null());
        assert!(v["data"].is_null());
    }

    #[test]
    fn inputs_pairs_roundtrip_to_json() {
        let i = Inputs::Pairs(vec![("b".into(), "k".into())]);
        let s = serde_json::to_string(&i).expect("encode");
        let back: Inputs = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, i);
    }

    #[test]
    fn inputs_bucket_roundtrips_to_json() {
        let i = Inputs::Bucket("users".into());
        let s = serde_json::to_string(&i).expect("encode");
        let back: Inputs = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, i);
    }

    #[test]
    fn inputs_keydata_roundtrips_to_json() {
        let i = Inputs::KeyData(vec![KeyDatum::with_value(
            "b",
            "k",
            serde_json::json!({"value": 7}),
        )]);
        let s = serde_json::to_string(&i).expect("encode");
        let back: Inputs = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, i);
    }

    #[test]
    fn job_roundtrips_through_json() {
        let job = MapReduceJob {
            inputs: Inputs::Pairs(vec![("b".into(), "k".into())]),
            phases: vec![Phase::Map {
                fn_name: "map_object_value".into(),
                arg: None,
                keep: false,
            }],
            timeout_ms: Some(60_000),
        };
        let s = serde_json::to_string(&job).expect("encode");
        let back: MapReduceJob = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, job);
    }

    #[test]
    fn job_decodes_riak_style_json() {
        let s = r#"{
            "inputs": [["b","k1"],["b","k2"]],
            "query":  [{"map": {"name": "map_object_value", "keep": false}}],
            "timeout": 1000
        }"#;
        let job: MapReduceJob = serde_json::from_str(s).expect("decode");
        assert_eq!(job.timeout_ms, Some(1000));
        assert_eq!(job.phases.len(), 1);
        match &job.inputs {
            Inputs::Pairs(p) => assert_eq!(p.len(), 2),
            _ => panic!("expected Pairs"),
        }
    }
}
