//! End-to-end integration tests for the Wasm MapReduce phase
//! fitting. Gated behind the `wasm` cargo feature; without it, the
//! test file is empty and the binary contains zero tests.
//!
//! The test ships a tiny WAT module (the `phase_apply` identity)
//! and runs it through a 2-phase pipeline: a Wasm map phase
//! followed by the built-in `reduce_count`. The final count must
//! match the input length.

#![cfg(feature = "wasm")]

use std::sync::Arc;

use dyniak::mapreduce::{
    builtins::default_registry, run_job_with_wasm, Inputs, KeyDatum, MapReduceJob, Phase, WasmHook,
    WasmModuleStore,
};

/// Identity phase module: hand the inbound CBOR bytes straight
/// back through the output meta. Same WAT as the in-crate unit
/// test fixture; duplicated here so the integration binary is
/// self-contained.
const IDENTITY_WAT: &str = r#"
    (module
      (memory (export "memory") 1)
      (global $heap_top (mut i32) (i32.const 1024))
      (func $alloc_inner (param $len i32) (result i32)
        (local $ptr i32)
        (local.set $ptr (global.get $heap_top))
        (global.set $heap_top
          (i32.add (global.get $heap_top) (local.get $len)))
        (local.get $ptr))
      (func (export "phase_alloc") (param $len i32) (result i32)
        (call $alloc_inner (local.get $len)))
      (func (export "phase_apply")
        (param $in_ptr i32) (param $in_len i32)
        (param $out_ptr_ptr i32) (param $out_len_ptr i32)
        (result i32)
        (local $out_buf i32)
        (local.set $out_buf (call $alloc_inner (local.get $in_len)))
        (memory.copy
          (local.get $out_buf)
          (local.get $in_ptr)
          (local.get $in_len))
        (i32.store (local.get $out_ptr_ptr) (local.get $out_buf))
        (i32.store (local.get $out_len_ptr) (local.get $in_len))
        (i32.const 0)))
"#;

#[tokio::test]
async fn wasm_map_then_builtin_reduce_count() {
    let store = WasmModuleStore::new().expect("wasm store");
    store
        .register("identity", IDENTITY_WAT.as_bytes())
        .expect("register identity wat");
    let hook: Arc<dyn WasmHook> = Arc::new(store);

    let inputs: Vec<KeyDatum> = (0..7u32)
        .map(|i| KeyDatum::with_value("b", format!("k{i}"), serde_json::json!(i)))
        .collect();
    let expected_count = inputs.len();

    let job = MapReduceJob {
        inputs: Inputs::KeyData(inputs),
        phases: vec![
            Phase::WasmModule {
                module_id: "identity".into(),
                fn_name: "apply".into(),
                arg: None,
                keep: false,
            },
            Phase::Reduce {
                fn_name: "reduce_count".into(),
                arg: None,
                keep: true,
            },
        ],
        timeout_ms: None,
    };

    let registry = Arc::new(default_registry());
    let outs = run_job_with_wasm(job, registry, Some(hook))
        .await
        .expect("run ok");

    // Final phase output: a single integer matching input length.
    assert_eq!(outs.len(), 1);
    assert_eq!(outs[0].phase, 1);
    assert_eq!(outs[0].value, serde_json::json!(expected_count));
}

#[tokio::test]
async fn wasm_phase_without_hook_returns_typed_error() {
    let job = MapReduceJob {
        inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(1))]),
        phases: vec![Phase::WasmModule {
            module_id: "identity".into(),
            fn_name: "apply".into(),
            arg: None,
            keep: false,
        }],
        timeout_ms: None,
    };
    let registry = Arc::new(default_registry());
    let err = run_job_with_wasm(job, registry, None)
        .await
        .expect_err("should error without hook");
    assert!(matches!(
        err,
        dyniak::mapreduce::MrError::WasmNotImplemented
    ));
}
