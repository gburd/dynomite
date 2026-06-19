//! Phase types for a Riak-style MapReduce pipeline.
//!
//! The pipeline is an ordered list of phases. Each phase reads from
//! an inbound mpsc, runs its function, and pushes outputs to the
//! next phase's inbound. Phases are dispatched by name through the
//! [`crate::mapreduce::registry::PhaseRegistry`] except for
//! [`Phase::Link`] (whose semantics are baked into the executor) and
//! [`Phase::WasmModule`] (dispatched through the MapReduce
//! Wasm module store (`crate::mapreduce::wasm::WasmModuleStore`,
//! available with the `wasm` feature) when one is wired into
//! the executor; without a store the executor returns
//! [`crate::mapreduce::MrError::WasmNotImplemented`]).

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A single phase in a MapReduce pipeline.
///
/// The variants mirror Riak's `{map, ...} | {reduce, ...} | {link,
/// ...}` shapes from the HTTP `/mapred` schema, plus a fourth
/// `WasmModule` slot for dispatching a registered Wasm module.
///
/// `keep` matches Riak's `keep` flag: when set, the phase's outputs
/// are also captured into the final response. The last phase is
/// always considered to keep its outputs even when the flag is
/// `false`, mirroring Riak's behaviour.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Map phase: invoke `fn_name` once per inbound item; emit zero
    /// or more outputs.
    Map {
        /// Registered map function name.
        #[serde(rename = "name")]
        fn_name: String,
        /// Optional JSON argument forwarded to the map function.
        #[serde(default)]
        arg: Option<Value>,
        /// Keep the phase's outputs in the final response.
        #[serde(default)]
        keep: bool,
    },
    /// Reduce phase: invoke `fn_name` once over every inbound item;
    /// emit zero or more outputs.
    Reduce {
        /// Registered reduce function name.
        #[serde(rename = "name")]
        fn_name: String,
        /// Optional JSON argument forwarded to the reduce function.
        #[serde(default)]
        arg: Option<Value>,
        /// Keep the phase's outputs in the final response.
        #[serde(default)]
        keep: bool,
    },
    /// Link phase: walk links from each inbound object and emit
    /// `(bucket, key)` pairs for matching links. The link semantics
    /// follow Riak's `Link-Walking` syntax: `bucket` and `tag` are
    /// optional patterns, with `None` meaning "match any".
    Link {
        /// Bucket pattern; `None` matches any bucket.
        #[serde(default)]
        bucket: Option<String>,
        /// Tag pattern; `None` matches any tag.
        #[serde(default)]
        tag: Option<String>,
        /// Keep the phase's outputs in the final response.
        #[serde(default)]
        keep: bool,
    },
    /// Wasm phase: invoke `fn_name` in the registered Wasm module
    /// `module_id` for each phase invocation. Present in the enum
    /// so the JSON schema is forwards-compatible. The executor
    /// dispatches this variant through the MapReduce Wasm module
    /// store (`crate::mapreduce::wasm::WasmModuleStore`, available
    /// with the `wasm` feature) when one is wired
    /// in; without a store it returns
    /// [`crate::mapreduce::MrError::WasmNotImplemented`].
    WasmModule {
        /// Wasm module identifier (registry-style name).
        module_id: String,
        /// Function name within the module.
        fn_name: String,
        /// Optional JSON argument forwarded to the function.
        #[serde(default)]
        arg: Option<Value>,
        /// Keep the phase's outputs in the final response.
        #[serde(default)]
        keep: bool,
    },
}

impl Phase {
    /// Whether the phase's outputs should be captured in the final
    /// response.
    #[must_use]
    pub fn keep(&self) -> bool {
        match self {
            Self::Map { keep, .. }
            | Self::Reduce { keep, .. }
            | Self::Link { keep, .. }
            | Self::WasmModule { keep, .. } => *keep,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_is_per_variant() {
        let p = Phase::Map {
            fn_name: "f".into(),
            arg: None,
            keep: true,
        };
        assert!(p.keep());
        let p = Phase::Reduce {
            fn_name: "f".into(),
            arg: None,
            keep: false,
        };
        assert!(!p.keep());
    }

    #[test]
    fn map_phase_round_trips_through_json() {
        let p = Phase::Map {
            fn_name: "map_object_value".into(),
            arg: Some(serde_json::json!({"k": "v"})),
            keep: true,
        };
        let s = serde_json::to_string(&p).expect("encode");
        let back: Phase = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, p);
    }

    #[test]
    fn reduce_phase_round_trips_through_json() {
        let p = Phase::Reduce {
            fn_name: "reduce_sum".into(),
            arg: None,
            keep: false,
        };
        let s = serde_json::to_string(&p).expect("encode");
        let back: Phase = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, p);
    }

    #[test]
    fn link_phase_round_trips_through_json() {
        let p = Phase::Link {
            bucket: Some("friends".into()),
            tag: Some("knows".into()),
            keep: true,
        };
        let s = serde_json::to_string(&p).expect("encode");
        let back: Phase = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, p);
    }

    #[test]
    fn wasm_phase_is_present_in_enum() {
        // The Wasm variant must round-trip through the JSON schema
        // so clients can submit Wasm-bearing jobs; the executor
        // dispatches them through a registered module store or
        // returns a typed error when no store is wired in.
        let p = Phase::WasmModule {
            module_id: "m".into(),
            fn_name: "f".into(),
            arg: None,
            keep: false,
        };
        let s = serde_json::to_string(&p).expect("encode");
        let back: Phase = serde_json::from_str(&s).expect("decode");
        assert_eq!(back, p);
    }
}
