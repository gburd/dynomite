//! MapReduce pipeline executor.
//!
//! The executor walks the [`crate::mapreduce::MapReduceJob`]'s phases
//! in order. Each phase runs as its own tokio task, reading from a
//! [`tokio::sync::mpsc`] inbound channel and writing to its outbound
//! channel. The previous phase's outbound is the next phase's
//! inbound; the final phase's outbound is collected into the
//! response envelope.
//!
//! ```text
//!     inputs ----> mpsc ----> phase 0 ----> mpsc ----> phase 1 ----> mpsc ---> ... ----> sink
//! ```
//!
//! # Determinism
//!
//! Phases process their inbound queue serially. `tokio::sync::mpsc`
//! preserves FIFO. Built-in phase functions are pure Rust. The
//! result is byte-identical across runs of the same job over the
//! same inputs.
//!
//! # Errors
//!
//! Any phase error short-circuits the pipeline: the executor cancels
//! every still-running phase task, drops the channels, and returns
//! the error. See [`MrError`].

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::mapreduce::job::MapReduceJob;
use crate::mapreduce::phase::Phase;
use crate::mapreduce::registry::PhaseRegistry;

/// Errors produced by the MapReduce executor.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MrError {
    /// Phase referenced a name that is not in the registry.
    #[error("unknown {kind} function: {name}")]
    UnknownFunction {
        /// Either `"map"` or `"reduce"`.
        kind: &'static str,
        /// Function name the phase asked for.
        name: String,
    },

    /// Phase function returned an error.
    #[error("phase {phase} ({kind}) failed: {message}")]
    PhaseFailed {
        /// Zero-based phase index.
        phase: u32,
        /// Phase kind: `"map"`, `"reduce"`, or `"link"`.
        kind: &'static str,
        /// Human-readable failure message.
        message: String,
    },

    /// Inputs variant is not implemented in this slice.
    #[error("unsupported MapReduce inputs: {0}")]
    UnsupportedInputs(&'static str),

    /// Wasm fitting is not enabled on this executor (no
    /// [`WasmHook`] was provided to [`run_job_with_wasm`], or the
    /// caller used the no-Wasm entry point [`run_job`]).
    #[error("wasm-module phases are not enabled on this executor")]
    WasmNotImplemented,

    /// Wasm phase referenced a `module_id` that is not registered
    /// in the [`WasmHook`].
    #[error("wasm module not found: {0}")]
    WasmModuleNotFound(String),

    /// Wasm phase exceeded its fuel budget or wall-clock timeout.
    /// Both fuel exhaustion and epoch-based interruption surface
    /// here so callers do not have to discriminate between them.
    #[error("wasm phase exceeded execution time / fuel limit")]
    WasmExecutionTimeout,

    /// Wasm phase tried to grow its linear memory beyond the
    /// configured per-call cap.
    #[error("wasm phase exceeded memory limit")]
    WasmMemoryLimit,

    /// Wasm runtime surfaced an error other than the typed
    /// memory / time / fuel cases (trap, missing export, bad
    /// pointer, instantiation failure, ...).
    #[error("wasm runtime error: {0}")]
    WasmRuntime(String),

    /// Wasm phase input or output failed CBOR encoding /
    /// decoding.
    #[error("wasm phase encoding error: {0}")]
    WasmEncoding(String),

    /// Link phase needs a datastore that can resolve `(bucket, key)`
    /// references and walk their links. The current substrate does
    /// not expose a content fetch; the executor surfaces this rather
    /// than silently emitting an empty result.
    #[error("link phases are not implemented in this slice")]
    LinkNotImplemented,

    /// Internal channel send / receive error. Surfaces if the
    /// caller drops the executor mid-flight.
    #[error("internal pipeline error: {0}")]
    Pipeline(String),

    /// JSON shaping failure inside a phase.
    #[error("json: {0}")]
    Json(String),
}

/// Hook invoked when a [`Phase::WasmModule`] is encountered.
///
/// The executor itself is Wasm-runtime-agnostic. A concrete
/// implementation (such as `crate::mapreduce::wasm::WasmModuleStore`,
/// available behind the `wasm` cargo feature) is plugged in by the
/// embedder via [`run_job_with_wasm`]. Without a hook, Wasm phases
/// surface as [`MrError::WasmNotImplemented`].
///
/// Implementations are passed the entire input batch for one phase
/// invocation and are expected to return the entire output batch.
/// The trait method is intentionally synchronous: the executor
/// dispatches Wasm calls onto a [`tokio::task::spawn_blocking`]
/// thread so the runtime is not blocked by long-running modules.
pub trait WasmHook: Send + Sync {
    /// Run one Wasm-fitted phase invocation. The hook owns memory,
    /// fuel, and timeout enforcement; errors surface through
    /// [`MrError`].
    fn apply_phase(
        &self,
        module_id: &str,
        fn_name: &str,
        inputs: &[Value],
    ) -> Result<Vec<Value>, MrError>;
}

/// One phase output entry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseOutput {
    /// Zero-based phase index.
    pub phase: u32,
    /// JSON value emitted by the phase.
    pub value: Value,
}

/// Captured outputs of a single phase, emitted as one batch by the
/// streaming executor entry points.
///
/// The buffered [`run_job`] flattens these into a `Vec<PhaseOutput>`
/// after the whole pipeline has run; the streaming entry points
/// [`run_job_streaming`] / [`run_job_streaming_with_wasm`] yield one
/// [`PhaseBatch`] per phase as soon as that phase finishes, so the
/// HTTP / PBC writers can frame the wire output incrementally.
///
/// Empty batches (a phase that captured no outputs) are not emitted.
/// The receiver therefore observes at most one batch per phase, in
/// ascending phase order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PhaseBatch {
    /// Zero-based phase index.
    pub phase: u32,
    /// JSON values captured for this phase, in emission order.
    pub data: Vec<Value>,
}

/// Run a MapReduce job to completion.
///
/// Returns the captured outputs of every phase that has `keep:
/// true`, plus the final phase's outputs unconditionally (Riak
/// behaviour). Outputs preserve insertion order.
///
/// # Errors
///
/// Returns [`MrError`] on the first phase failure or unsupported
/// shape encountered. The pipeline is cancelled at the first error.
pub async fn run_job(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
) -> Result<Vec<PhaseOutput>, MrError> {
    run_job_with_wasm(job, registry, None).await
}

/// Run a MapReduce job, streaming captured per-phase outputs back
/// to the caller as a [`tokio::sync::mpsc::Receiver`].
///
/// One [`PhaseBatch`] is sent per phase that has `keep: true` (the
/// final phase always keeps unconditionally). Empty batches are
/// suppressed. On the first phase failure the receiver observes
/// `Some(Err(MrError))` followed by channel close; callers should
/// stop reading once they see the error.
///
/// The buffered [`run_job`] is implemented in terms of this entry
/// point in spirit only: the buffered path is kept on its own
/// implementation so existing callers continue to see the same
/// allocation pattern. The streaming path is what powers the HTTP
/// `/mapred` multipart writer.
#[must_use]
pub fn run_job_streaming(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
) -> mpsc::Receiver<Result<PhaseBatch, MrError>> {
    run_job_streaming_with_wasm(job, registry, None)
}

/// Streaming variant of [`run_job_with_wasm`]. See
/// [`run_job_streaming`] for the wire shape.
#[must_use]
pub fn run_job_streaming_with_wasm(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
    wasm: Option<Arc<dyn WasmHook>>,
) -> mpsc::Receiver<Result<PhaseBatch, MrError>> {
    // Channel size 4 matches the inbound / outbound channels in
    // `run_phase`: the consumer (HTTP body writer / PBC frame
    // writer) is expected to drain at line speed, but a small
    // buffer absorbs scheduling jitter without making the
    // executor block on the consumer.
    let (tx, rx) = mpsc::channel::<Result<PhaseBatch, MrError>>(4);
    tokio::spawn(async move {
        let result = stream_job_inner(job, registry, wasm, tx.clone()).await;
        if let Err(e) = result {
            // The error path is always reported through the
            // receiver; ignoring the send result is fine because
            // the only reason it can fail is the consumer dropping
            // the receiver, which means it does not care anymore.
            let _ = tx.send(Err(e)).await;
        }
    });
    rx
}

/// Streaming pipeline driver. Runs phases in order, sending one
/// [`PhaseBatch`] per kept phase down `tx`. On phase failure,
/// returns `Err(MrError)`; the caller publishes that error to the
/// receiver.
async fn stream_job_inner(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
    wasm: Option<Arc<dyn WasmHook>>,
    tx: mpsc::Sender<Result<PhaseBatch, MrError>>,
) -> Result<(), MrError> {
    let items = job
        .inputs
        .items()
        .ok_or(MrError::UnsupportedInputs("bucket scan"))?;
    let initial: Vec<Value> = items.into_iter().map(|kd| kd.to_value()).collect();
    materialised_initial_inputs_must_be_iterable(&initial);

    if job.phases.is_empty() {
        // Identity job: the inputs themselves are the phase-0 batch.
        if !initial.is_empty() {
            let batch = PhaseBatch {
                phase: 0,
                data: initial,
            };
            // A receiver gone away just means the consumer is no
            // longer interested; we drop the batch silently and
            // exit cleanly.
            let _ = tx.send(Ok(batch)).await;
        }
        return Ok(());
    }

    let n_phases = job.phases.len();
    let mut current: Vec<Value> = initial;

    for (idx, phase) in job.phases.iter().enumerate() {
        let phase_idx = u32::try_from(idx)
            .map_err(|_| MrError::Pipeline("phase index exceeds u32 range".into()))?;
        let is_last = idx + 1 == n_phases;
        let outputs = run_phase(phase_idx, phase, current, &registry, wasm.as_ref()).await?;

        if (phase.keep() || is_last) && !outputs.is_empty() {
            let batch = PhaseBatch {
                phase: phase_idx,
                data: outputs.clone(),
            };
            if tx.send(Ok(batch)).await.is_err() {
                // Consumer dropped; stop the pipeline. This is not
                // an error from the executor's perspective.
                return Ok(());
            }
        }
        current = outputs;
    }

    Ok(())
}

/// Run a MapReduce job with an optional Wasm-phase hook.
///
/// Identical to [`run_job`] except [`Phase::WasmModule`] phases are
/// dispatched through `wasm` when it is `Some`. When `wasm` is
/// `None`, Wasm phases surface as [`MrError::WasmNotImplemented`],
/// preserving the no-Wasm path for embedders who do not enable the
/// `wasm` feature.
///
/// # Errors
///
/// Returns [`MrError`] on the first phase failure or unsupported
/// shape. The pipeline is cancelled at the first error.
pub async fn run_job_with_wasm(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
    wasm: Option<Arc<dyn WasmHook>>,
) -> Result<Vec<PhaseOutput>, MrError> {
    // Resolve inputs into a Vec<Value>. The executor materialises
    // every input upfront; large bucket-scan inputs would land in
    // the streaming follow-up, alongside an InputSource trait.
    let items = job
        .inputs
        .items()
        .ok_or(MrError::UnsupportedInputs("bucket scan"))?;
    let initial: Vec<Value> = items.into_iter().map(|kd| kd.to_value()).collect();
    materialised_initial_inputs_must_be_iterable(&initial);

    if job.phases.is_empty() {
        // No phases: the job is the identity map. Riak emits the
        // inputs themselves under phase 0.
        let out: Vec<PhaseOutput> = initial
            .into_iter()
            .map(|v| PhaseOutput { phase: 0, value: v })
            .collect();
        return Ok(out);
    }

    let n_phases = job.phases.len();
    let mut current: Vec<Value> = initial;
    let mut captured: Vec<PhaseOutput> = Vec::new();

    for (idx, phase) in job.phases.iter().enumerate() {
        let phase_idx = u32::try_from(idx)
            .map_err(|_| MrError::Pipeline("phase index exceeds u32 range".into()))?;
        let is_last = idx + 1 == n_phases;
        let outputs = run_phase(phase_idx, phase, current, &registry, wasm.as_ref()).await?;

        if phase.keep() || is_last {
            for v in &outputs {
                captured.push(PhaseOutput {
                    phase: phase_idx,
                    value: v.clone(),
                });
            }
        }
        current = outputs;
    }

    Ok(captured)
}

/// Run a single phase: feed `inputs` through an mpsc, run the phase
/// task, collect outputs from the outbound mpsc, return them.
async fn run_phase(
    phase_idx: u32,
    phase: &Phase,
    inputs: Vec<Value>,
    registry: &Arc<PhaseRegistry>,
    wasm: Option<&Arc<dyn WasmHook>>,
) -> Result<Vec<Value>, MrError> {
    // Channel sizing: 64 is plenty for serial map/reduce processing
    // since each phase task drains as fast as the previous one
    // produces. Backpressure here is handled by the channel itself.
    let (tx_in, rx_in) = mpsc::channel::<Value>(64);
    let (tx_out, mut rx_out) = mpsc::channel::<Value>(64);

    // Feed inputs into the inbound channel. We spawn a task so
    // backpressure works: if the phase task is slow, sends will
    // pause without blocking the executor's main loop.
    tokio::spawn(async move {
        for v in inputs {
            // If the receiver has been dropped (phase failed), bail
            // out silently; the phase task's error is already on
            // its way to the caller.
            if tx_in.send(v).await.is_err() {
                return;
            }
        }
    });

    // Run the phase task. We do not spawn it: the executor's caller
    // owns the runtime and we want errors to surface synchronously.
    let phase_clone = phase.clone();
    let registry_clone = Arc::clone(registry);
    let wasm_clone = wasm.cloned();
    let phase_join = tokio::spawn(async move {
        run_phase_task(
            phase_idx,
            phase_clone,
            rx_in,
            tx_out,
            registry_clone,
            wasm_clone,
        )
        .await
    });

    // Collect outbound items.
    let mut outputs = Vec::new();
    while let Some(v) = rx_out.recv().await {
        outputs.push(v);
    }

    // Surface the phase task's outcome.
    match phase_join.await {
        Ok(Ok(())) => Ok(outputs),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(MrError::Pipeline(format!("join: {e}"))),
    }
}

/// Inner phase task: read from `rx`, run the phase function, push
/// to `tx`.
async fn run_phase_task(
    phase_idx: u32,
    phase: Phase,
    mut rx: mpsc::Receiver<Value>,
    tx: mpsc::Sender<Value>,
    registry: Arc<PhaseRegistry>,
    wasm: Option<Arc<dyn WasmHook>>,
) -> Result<(), MrError> {
    match phase {
        Phase::Map { fn_name, arg, .. } => {
            let f = registry.map_fn(&fn_name).ok_or(MrError::UnknownFunction {
                kind: "map",
                name: fn_name.clone(),
            })?;
            // Clone the Arc up front so the function lives long
            // enough for the task body.
            let f = f.clone();
            while let Some(v) = rx.recv().await {
                let outs = (f)(&v, arg.as_ref()).map_err(|e| MrError::PhaseFailed {
                    phase: phase_idx,
                    kind: "map",
                    message: e.to_string(),
                })?;
                for o in outs {
                    if tx.send(o).await.is_err() {
                        return Err(MrError::Pipeline(
                            "downstream phase dropped its inbound channel".into(),
                        ));
                    }
                }
            }
            Ok(())
        }
        Phase::Reduce { fn_name, arg, .. } => {
            let f = registry
                .reduce_fn(&fn_name)
                .ok_or(MrError::UnknownFunction {
                    kind: "reduce",
                    name: fn_name.clone(),
                })?;
            let f = f.clone();
            // Drain inbound first; reduce runs once over the whole
            // accumulated set.
            let mut buf: Vec<Value> = Vec::new();
            while let Some(v) = rx.recv().await {
                buf.push(v);
            }
            let outs = (f)(&buf, arg.as_ref()).map_err(|e| MrError::PhaseFailed {
                phase: phase_idx,
                kind: "reduce",
                message: e.to_string(),
            })?;
            for o in outs {
                if tx.send(o).await.is_err() {
                    return Err(MrError::Pipeline(
                        "downstream phase dropped its inbound channel".into(),
                    ));
                }
            }
            Ok(())
        }
        Phase::Link { .. } => {
            // The substrate's Datastore::dispatch does not yet expose
            // object content; following links requires a content
            // fetch. Returning a typed error makes the limitation
            // visible to clients without lying about the result.
            //
            // The Phase::Link variant is preserved in the public
            // enum and the JSON schema so a follow-up slice can
            // implement execution mechanically once the K/V trait
            // lands.
            Err(MrError::LinkNotImplemented)
        }
        Phase::WasmModule {
            module_id, fn_name, ..
        } => {
            let hook = wasm.ok_or(MrError::WasmNotImplemented)?;
            // Reduce-style: drain inbound, hand the whole batch
            // to the Wasm module, push its output downstream.
            let mut buf: Vec<Value> = Vec::new();
            while let Some(v) = rx.recv().await {
                buf.push(v);
            }
            let mid = module_id.clone();
            let fname = fn_name.clone();
            let outs = tokio::task::spawn_blocking(move || hook.apply_phase(&mid, &fname, &buf))
                .await
                .map_err(|e| MrError::Pipeline(format!("wasm join: {e}")))?
                .map_err(|e| match e {
                    MrError::WasmModuleNotFound(_)
                    | MrError::WasmExecutionTimeout
                    | MrError::WasmMemoryLimit
                    | MrError::WasmRuntime(_)
                    | MrError::WasmEncoding(_)
                    | MrError::WasmNotImplemented => e,
                    other => MrError::PhaseFailed {
                        phase: phase_idx,
                        kind: "wasm",
                        message: other.to_string(),
                    },
                })?;
            for o in outs {
                if tx.send(o).await.is_err() {
                    return Err(MrError::Pipeline(
                        "downstream phase dropped its inbound channel".into(),
                    ));
                }
            }
            Ok(())
        }
    }
}

/// Compile-time-style assertion that the materialised inputs are
/// drainable. `Vec<Value>` already satisfies this; this function is
/// a single-call seam so the executor's documentation can point at
/// "the materialise step".
fn materialised_initial_inputs_must_be_iterable<T>(_: &[T]) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapreduce::builtins::default_registry;
    use crate::mapreduce::job::{Inputs, KeyDatum};

    fn registry() -> Arc<PhaseRegistry> {
        Arc::new(default_registry())
    }

    #[tokio::test]
    async fn empty_phase_list_is_identity() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(1))]),
            phases: vec![],
            timeout_ms: None,
        };
        let out = run_job(job, registry()).await.expect("ok");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].phase, 0);
        assert_eq!(out[0].value["bucket"], "b");
    }

    #[tokio::test]
    async fn map_then_reduce_pipeline() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![
                KeyDatum::with_value("b", "k1", serde_json::json!(2)),
                KeyDatum::with_value("b", "k2", serde_json::json!(3)),
                KeyDatum::with_value("b", "k3", serde_json::json!(4)),
            ]),
            phases: vec![
                Phase::Map {
                    fn_name: "map_object_value".into(),
                    arg: None,
                    keep: false,
                },
                Phase::Reduce {
                    fn_name: "reduce_sum".into(),
                    arg: None,
                    keep: true,
                },
            ],
            timeout_ms: None,
        };
        let out = run_job(job, registry()).await.expect("ok");
        // Last phase + keep=true on reduce yields one output.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].phase, 1);
        assert_eq!(out[0].value, serde_json::json!(9));
    }

    #[tokio::test]
    async fn keep_intermediate_phase_outputs_are_captured() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![
                KeyDatum::with_value("b", "k1", serde_json::json!(5)),
                KeyDatum::with_value("b", "k2", serde_json::json!(7)),
            ]),
            phases: vec![
                Phase::Map {
                    fn_name: "map_object_value".into(),
                    arg: None,
                    keep: true,
                },
                Phase::Reduce {
                    fn_name: "reduce_sum".into(),
                    arg: None,
                    keep: true,
                },
            ],
            timeout_ms: None,
        };
        let out = run_job(job, registry()).await.expect("ok");
        // Two from the map (keep=true) + one from the reduce.
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].phase, 0);
        assert_eq!(out[1].phase, 0);
        assert_eq!(out[2].phase, 1);
        assert_eq!(out[2].value, serde_json::json!(12));
    }

    #[tokio::test]
    async fn unknown_map_function_is_typed_error() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(1))]),
            phases: vec![Phase::Map {
                fn_name: "no_such_function".into(),
                arg: None,
                keep: false,
            }],
            timeout_ms: None,
        };
        let err = run_job(job, registry()).await.expect_err("error");
        assert!(matches!(err, MrError::UnknownFunction { kind: "map", .. }));
    }

    #[tokio::test]
    async fn unknown_reduce_function_is_typed_error() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![]),
            phases: vec![Phase::Reduce {
                fn_name: "no_such_reduce".into(),
                arg: None,
                keep: false,
            }],
            timeout_ms: None,
        };
        let err = run_job(job, registry()).await.expect_err("error");
        assert!(matches!(
            err,
            MrError::UnknownFunction { kind: "reduce", .. }
        ));
    }

    #[tokio::test]
    async fn bucket_inputs_unsupported() {
        let job = MapReduceJob {
            inputs: Inputs::Bucket("b".into()),
            phases: vec![],
            timeout_ms: None,
        };
        let err = run_job(job, registry()).await.expect_err("error");
        assert!(matches!(err, MrError::UnsupportedInputs(_)));
    }

    #[tokio::test]
    async fn wasm_phase_returns_typed_error() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(1))]),
            phases: vec![Phase::WasmModule {
                module_id: "m".into(),
                fn_name: "f".into(),
                arg: None,
                keep: false,
            }],
            timeout_ms: None,
        };
        let err = run_job(job, registry()).await.expect_err("error");
        assert!(matches!(err, MrError::WasmNotImplemented));
    }

    #[tokio::test]
    async fn link_phase_returns_typed_error() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::pair("b", "k")]),
            phases: vec![Phase::Link {
                bucket: None,
                tag: None,
                keep: false,
            }],
            timeout_ms: None,
        };
        let err = run_job(job, registry()).await.expect_err("error");
        assert!(matches!(err, MrError::LinkNotImplemented));
    }

    #[tokio::test]
    async fn determinism_under_one_hundred_inputs_three_phases() {
        // Build 100 inputs and a 3-phase pipeline. Run the same
        // job twice and assert byte-equal output. Built-in
        // functions are pure, mpsc preserves FIFO, and each phase
        // processes inbound serially -- so determinism follows.
        let mut data = Vec::new();
        for i in 0..100u32 {
            data.push(KeyDatum::with_value(
                "b",
                format!("k{i}"),
                serde_json::json!(i),
            ));
        }
        let phases = vec![
            Phase::Map {
                fn_name: "map_object_value".into(),
                arg: None,
                keep: false,
            },
            Phase::Reduce {
                fn_name: "reduce_sort".into(),
                arg: None,
                keep: false,
            },
            Phase::Reduce {
                fn_name: "reduce_count".into(),
                arg: None,
                keep: true,
            },
        ];
        let job = MapReduceJob {
            inputs: Inputs::KeyData(data.clone()),
            phases: phases.clone(),
            timeout_ms: None,
        };
        let job2 = MapReduceJob {
            inputs: Inputs::KeyData(data),
            phases,
            timeout_ms: None,
        };
        let r1 = run_job(job, registry()).await.expect("run1");
        let r2 = run_job(job2, registry()).await.expect("run2");
        assert_eq!(r1, r2);
        // reduce_count ends the pipeline with a single integer.
        assert_eq!(r1.len(), 1);
        assert_eq!(r1[0].value, serde_json::json!(100));
    }

    #[tokio::test]
    async fn pairs_inputs_are_materialised_with_null_value() {
        // Inputs without inline values still flow through; the
        // value field is JSON null and a downstream map function
        // can decide what to do. This is the same shape Riak
        // emits when a key cannot be fetched.
        let job = MapReduceJob {
            inputs: Inputs::Pairs(vec![("b".into(), "k1".into()), ("b".into(), "k2".into())]),
            phases: vec![],
            timeout_ms: None,
        };
        let out = run_job(job, registry()).await.expect("ok");
        assert_eq!(out.len(), 2);
        assert!(out[0].value["value"].is_null());
        assert_eq!(out[0].value["bucket"], "b");
        assert_eq!(out[0].value["key"], "k1");
    }

    // ---- streaming entry point -------------------------------------

    async fn drain_stream(
        mut rx: mpsc::Receiver<Result<PhaseBatch, MrError>>,
    ) -> Vec<Result<PhaseBatch, MrError>> {
        let mut out = Vec::new();
        while let Some(item) = rx.recv().await {
            out.push(item);
        }
        out
    }

    #[tokio::test]
    async fn streaming_emits_one_batch_per_kept_phase() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![
                KeyDatum::with_value("b", "k1", serde_json::json!(1)),
                KeyDatum::with_value("b", "k2", serde_json::json!(2)),
                KeyDatum::with_value("b", "k3", serde_json::json!(3)),
            ]),
            phases: vec![
                Phase::Map {
                    fn_name: "map_object_value".into(),
                    arg: None,
                    keep: true,
                },
                Phase::Reduce {
                    fn_name: "reduce_sum".into(),
                    arg: None,
                    keep: true,
                },
            ],
            timeout_ms: None,
        };
        let rx = run_job_streaming(job, registry());
        let items = drain_stream(rx).await;
        assert_eq!(items.len(), 2, "two kept phases yield two batches");
        let b0 = items[0].as_ref().expect("phase 0 ok");
        assert_eq!(b0.phase, 0);
        assert_eq!(b0.data.len(), 3);
        let b1 = items[1].as_ref().expect("phase 1 ok");
        assert_eq!(b1.phase, 1);
        assert_eq!(b1.data, vec![serde_json::json!(6)]);
    }

    #[tokio::test]
    async fn streaming_skips_non_keep_intermediate_phases() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(2))]),
            phases: vec![
                Phase::Map {
                    fn_name: "map_object_value".into(),
                    arg: None,
                    keep: false,
                },
                Phase::Reduce {
                    fn_name: "reduce_sum".into(),
                    arg: None,
                    keep: true,
                },
            ],
            timeout_ms: None,
        };
        let rx = run_job_streaming(job, registry());
        let items = drain_stream(rx).await;
        assert_eq!(items.len(), 1, "only the kept reduce emits a batch");
        let b = items[0].as_ref().expect("ok");
        assert_eq!(b.phase, 1);
    }

    #[tokio::test]
    async fn streaming_surfaces_phase_error_as_terminal_item() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::pair("b", "k")]),
            phases: vec![Phase::Map {
                fn_name: "no_such_function".into(),
                arg: None,
                keep: true,
            }],
            timeout_ms: None,
        };
        let rx = run_job_streaming(job, registry());
        let items = drain_stream(rx).await;
        assert_eq!(items.len(), 1);
        let err = items[0].as_ref().expect_err("unknown function");
        assert!(matches!(err, MrError::UnknownFunction { kind: "map", .. }));
    }

    #[tokio::test]
    async fn streaming_empty_phase_list_emits_initial_inputs() {
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(42))]),
            phases: vec![],
            timeout_ms: None,
        };
        let rx = run_job_streaming(job, registry());
        let items = drain_stream(rx).await;
        assert_eq!(items.len(), 1);
        let b = items[0].as_ref().expect("ok");
        assert_eq!(b.phase, 0);
        assert_eq!(b.data.len(), 1);
        assert_eq!(b.data[0]["value"], serde_json::json!(42));
    }
}
