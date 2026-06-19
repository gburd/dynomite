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

use dynomite::embed::Datastore;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::mapreduce::job::{Inputs, KeyDatum, MapReduceJob};
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

    /// Inputs variant cannot be materialised in this execution
    /// context. A well-formed [`Inputs::Bucket`] needs a datastore
    /// to enumerate its keys; this surfaces when no datastore was
    /// wired into the executor (the no-store entry points) or when
    /// the supplied input spec is otherwise unrunnable.
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

    /// Link phase needs a datastore-backed job: it fetches each
    /// inbound object through [`Datastore::riak_get`] to read its
    /// stored links. The buffered / streaming entry points that run
    /// without a datastore (the pure in-memory paths) cannot fetch
    /// objects, so a link phase on those paths surfaces this rather
    /// than emitting an empty (and silently wrong) result. The
    /// datastore-backed `run_job_full` / HTTP `POST /mapred` path
    /// walks links normally.
    #[error("link phases require a datastore-backed job")]
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
    run_job_full(job, registry, None, None).await
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
    run_job_streaming_full(job, registry, None, None)
}

/// Streaming variant of [`run_job_with_wasm`]. See
/// [`run_job_streaming`] for the wire shape.
#[must_use]
pub fn run_job_streaming_with_wasm(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
    wasm: Option<Arc<dyn WasmHook>>,
) -> mpsc::Receiver<Result<PhaseBatch, MrError>> {
    run_job_streaming_full(job, registry, wasm, None)
}

/// Streaming entry point that also accepts a datastore for
/// resolving [`Inputs::Bucket`] jobs.
///
/// When `datastore` is `Some`, an [`Inputs::Bucket`] job enumerates
/// every key in the named bucket through
/// [`Datastore::list_keys_stream`] and seeds the pipeline with one
/// `(bucket, key)` datum per key (no inline value). When
/// `datastore` is `None`, an [`Inputs::Bucket`] job surfaces as
/// [`MrError::UnsupportedInputs`], preserving the no-store path.
///
/// See [`run_job_streaming`] for the wire shape.
#[must_use]
pub fn run_job_streaming_full(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
    wasm: Option<Arc<dyn WasmHook>>,
    datastore: Option<Arc<dyn Datastore>>,
) -> mpsc::Receiver<Result<PhaseBatch, MrError>> {
    // Channel size 4 matches the inbound / outbound channels in
    // `run_phase`: the consumer (HTTP body writer / PBC frame
    // writer) is expected to drain at line speed, but a small
    // buffer absorbs scheduling jitter without making the
    // executor block on the consumer.
    let (tx, rx) = mpsc::channel::<Result<PhaseBatch, MrError>>(4);
    tokio::spawn(async move {
        let result = stream_job_inner(job, registry, wasm, datastore, tx.clone()).await;
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
    datastore: Option<Arc<dyn Datastore>>,
    tx: mpsc::Sender<Result<PhaseBatch, MrError>>,
) -> Result<(), MrError> {
    let initial = resolve_inputs(&job.inputs, datastore.as_ref()).await?;
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
        let outputs = run_phase(
            phase_idx,
            phase,
            current,
            &registry,
            wasm.as_ref(),
            datastore.as_ref(),
        )
        .await?;

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
    run_job_full(job, registry, wasm, None).await
}

/// Buffered entry point that also accepts a datastore for resolving
/// [`Inputs::Bucket`] jobs.
///
/// Identical to [`run_job_with_wasm`] except a well-formed
/// [`Inputs::Bucket`] job enumerates its keys through
/// [`Datastore::list_keys_stream`] when `datastore` is `Some`. When
/// `datastore` is `None`, an [`Inputs::Bucket`] job surfaces as
/// [`MrError::UnsupportedInputs`].
///
/// # Errors
///
/// Returns [`MrError`] on the first phase failure, an unresolvable
/// input shape, or a datastore enumeration failure. The pipeline is
/// cancelled at the first error.
pub async fn run_job_full(
    job: MapReduceJob,
    registry: Arc<PhaseRegistry>,
    wasm: Option<Arc<dyn WasmHook>>,
    datastore: Option<Arc<dyn Datastore>>,
) -> Result<Vec<PhaseOutput>, MrError> {
    // Resolve inputs into a Vec<Value>. The executor materialises
    // every input upfront; a bucket-scan input streams its keys
    // through the datastore here before the pipeline runs.
    let initial = resolve_inputs(&job.inputs, datastore.as_ref()).await?;
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
        let outputs = run_phase(
            phase_idx,
            phase,
            current,
            &registry,
            wasm.as_ref(),
            datastore.as_ref(),
        )
        .await?;

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
    datastore: Option<&Arc<dyn Datastore>>,
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
    let datastore_clone = datastore.cloned();
    let phase_join = tokio::spawn(async move {
        run_phase_task(
            phase_idx,
            phase_clone,
            rx_in,
            tx_out,
            registry_clone,
            wasm_clone,
            datastore_clone,
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
    datastore: Option<Arc<dyn Datastore>>,
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
        Phase::Link { bucket, tag, .. } => {
            run_link_phase(
                phase_idx,
                bucket.as_deref(),
                tag.as_deref(),
                &mut rx,
                &tx,
                datastore,
            )
            .await
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

/// Walk the links stored on each inbound object and emit the
/// matching `(bucket, key)` targets.
///
/// Each inbound item is a routing datum carrying at least
/// `{bucket, key}` (see [`KeyDatum::to_value`]); the object is
/// fetched through the datastore, its
/// [`crate::proto::http::object::HttpObject`] envelope is decoded,
/// and every stored link is filtered against the phase's
/// `{bucket, tag}` patterns (a `None` pattern matches any value). A
/// matching link is emitted as the same `{bucket, key}`-shaped datum
/// a map phase emits, so the next phase consumes link output
/// identically.
///
/// A missing object (`riak_get -> None`) contributes no links and is
/// not an error. Walking links needs a datastore-backed job: the
/// in-memory streaming path passes `None` here and the phase reports
/// [`MrError::LinkNotImplemented`] rather than silently emitting
/// nothing.
async fn run_link_phase(
    phase_idx: u32,
    bucket: Option<&str>,
    tag: Option<&str>,
    rx: &mut mpsc::Receiver<Value>,
    tx: &mpsc::Sender<Value>,
    datastore: Option<Arc<dyn Datastore>>,
) -> Result<(), MrError> {
    let store = datastore.ok_or(MrError::LinkNotImplemented)?;
    while let Some(v) = rx.recv().await {
        let (in_bucket, in_key) = link_input_target(&v).ok_or_else(|| MrError::PhaseFailed {
            phase: phase_idx,
            kind: "link",
            message: "link-phase input is missing bucket/key".into(),
        })?;
        let stored = store
            .riak_get(in_bucket.as_bytes(), in_key.as_bytes())
            .await
            .map_err(|e| MrError::PhaseFailed {
                phase: phase_idx,
                kind: "link",
                message: format!("riak get {in_bucket}/{in_key}: {e}"),
            })?;
        let Some(stored) = stored else {
            // Missing object: no links, no error.
            continue;
        };
        let obj =
            crate::proto::http::object::HttpObject::from_storage_bytes(&stored).map_err(|e| {
                MrError::PhaseFailed {
                    phase: phase_idx,
                    kind: "link",
                    message: format!("decode {in_bucket}/{in_key}: {e}"),
                }
            })?;
        for link in &obj.links {
            let bucket_ok = bucket.is_none_or(|b| b == link.bucket);
            let tag_ok = tag.is_none_or(|t| t == link.tag);
            if bucket_ok && tag_ok {
                let out = KeyDatum::pair(link.bucket.clone(), link.key.clone()).to_value();
                if tx.send(out).await.is_err() {
                    return Err(MrError::Pipeline(
                        "downstream phase dropped its inbound channel".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Compile-time-style assertion that the materialised inputs are
/// drainable. `Vec<Value>` already satisfies this; this function is
/// a single-call seam so the executor's documentation can point at
/// "the materialise step".
fn materialised_initial_inputs_must_be_iterable<T>(_: &[T]) {}

/// Extract the `(bucket, key)` routing pair from a phase datum for a
/// link phase to fetch.
///
/// Phase data flow as JSON objects shaped
/// `{"bucket": ..., "key": ..., "value": ..., "data": ...}` (see
/// [`KeyDatum::to_value`]). A link phase only needs the routing
/// pair; both fields must be JSON strings. Returns `None` when
/// either is absent or not a string, so the executor can surface a
/// typed phase error rather than silently dropping the input.
fn link_input_target(v: &Value) -> Option<(String, String)> {
    let bucket = v.get("bucket")?.as_str()?.to_string();
    let key = v.get("key")?.as_str()?.to_string();
    Some((bucket, key))
}

/// Materialise a job's inputs into the seed `Vec<Value>` the
/// pipeline drains from phase 0.
///
/// [`Inputs::Pairs`] and [`Inputs::KeyData`] resolve inline via
/// [`Inputs::items`]. [`Inputs::Bucket`] enumerates every key in the
/// named bucket through [`Datastore::list_keys_stream`]; each key
/// becomes a routing-only [`KeyDatum`] (bucket + key, no inline
/// value), exactly as Riak seeds a whole-bucket MapReduce input.
/// An empty or nonexistent bucket yields an empty seed list, not an
/// error. Without a datastore, [`Inputs::Bucket`] is reported as
/// [`MrError::UnsupportedInputs`].
async fn resolve_inputs(
    inputs: &Inputs,
    datastore: Option<&Arc<dyn Datastore>>,
) -> Result<Vec<Value>, MrError> {
    if let Some(items) = inputs.items() {
        return Ok(items.into_iter().map(|kd| kd.to_value()).collect());
    }
    // The only shape `items()` cannot materialise inline is
    // `Inputs::Bucket`; enumerate its keys through the datastore.
    let Inputs::Bucket(bucket) = inputs else {
        return Err(MrError::UnsupportedInputs("unrunnable input spec"));
    };
    let store = datastore.ok_or(MrError::UnsupportedInputs("bucket scan"))?;
    let mut stream = store.list_keys_stream(bucket.as_bytes());
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        let key = item.map_err(|e| MrError::Pipeline(format!("bucket scan: {e}")))?;
        // Keys are object names; lossy UTF-8 keeps the JSON datum
        // ASCII-clean while preserving any printable key verbatim.
        let key = String::from_utf8_lossy(&key).into_owned();
        out.push(KeyDatum::pair(bucket.clone(), key).to_value());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapreduce::builtins::default_registry;
    use crate::mapreduce::job::{Inputs, KeyDatum};

    fn registry() -> Arc<PhaseRegistry> {
        Arc::new(default_registry())
    }

    /// Minimal in-test datastore whose `riak_get` is scripted: it can
    /// return a fixed byte body, raw (non-envelope) bytes, or an
    /// error, so the link-phase error arms can be exercised without a
    /// real storage engine.
    struct ScriptedStore {
        body: Option<Result<Option<Vec<u8>>, ()>>,
    }

    impl dynomite::embed::Datastore for ScriptedStore {
        fn protocol(&self) -> dynomite::embed::Protocol {
            dynomite::embed::Protocol::Custom
        }
        fn dispatch(
            &self,
            req: dynomite::msg::Msg,
        ) -> dynomite::embed::BoxFuture<
            '_,
            Result<dynomite::msg::Msg, dynomite::embed::DatastoreError>,
        > {
            Box::pin(async move {
                Ok(dynomite::msg::Msg::new(
                    req.id(),
                    dynomite::msg::MsgType::Unknown,
                    false,
                ))
            })
        }
        fn riak_get<'a>(
            &'a self,
            _bucket: &'a [u8],
            _key: &'a [u8],
        ) -> dynomite::embed::BoxFuture<'a, Result<Option<Vec<u8>>, dynomite::embed::DatastoreError>>
        {
            let body = self.body.clone();
            Box::pin(async move {
                match body {
                    Some(Ok(v)) => Ok(v),
                    Some(Err(())) | None => Err(dynomite::embed::DatastoreError::Backend(
                        "scripted failure".into(),
                    )),
                }
            })
        }
    }

    fn store_with(body: Option<Result<Option<Vec<u8>>, ()>>) -> Arc<dyn Datastore> {
        Arc::new(ScriptedStore { body })
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
    async fn bucket_inputs_without_datastore_are_unsupported() {
        // The no-store entry points cannot enumerate a bucket's
        // keys, so a bucket-scan input still surfaces as a typed
        // UnsupportedInputs error there.
        let job = MapReduceJob {
            inputs: Inputs::Bucket("b".into()),
            phases: vec![],
            timeout_ms: None,
        };
        let err = run_job(job, registry()).await.expect_err("error");
        assert!(matches!(err, MrError::UnsupportedInputs(_)));
    }

    #[tokio::test]
    async fn bucket_inputs_enumerate_keys_through_datastore() {
        use dynomite::embed::hooks::MemoryDatastore;
        let ds = MemoryDatastore::new();
        ds.insert(b"users", b"alice");
        ds.insert(b"users", b"bob");
        ds.insert(b"users", b"carol");
        // A key in another bucket must not leak into the scan.
        ds.insert(b"orders", b"o1");
        let ds: Arc<dyn Datastore> = Arc::new(ds);

        let job = MapReduceJob {
            inputs: Inputs::Bucket("users".into()),
            phases: vec![],
            timeout_ms: None,
        };
        let out = run_job_full(job, registry(), None, Some(ds))
            .await
            .expect("ok");
        // Identity (no phases): one phase-0 datum per enumerated key.
        assert_eq!(out.len(), 3);
        let mut keys: Vec<String> = out
            .iter()
            .map(|o| o.value["key"].as_str().unwrap().to_string())
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["alice", "bob", "carol"]);
        for o in &out {
            assert_eq!(o.value["bucket"], "users");
            assert!(o.value["value"].is_null());
        }
    }

    #[tokio::test]
    async fn bucket_inputs_feed_map_reduce_pipeline() {
        use dynomite::embed::hooks::MemoryDatastore;
        let ds = MemoryDatastore::new();
        for i in 0..5u32 {
            ds.insert(b"nums", format!("k{i}").as_bytes());
        }
        let ds: Arc<dyn Datastore> = Arc::new(ds);

        // map_identity passes each key datum through; reduce_count
        // aggregates to the key count.
        let job = MapReduceJob {
            inputs: Inputs::Bucket("nums".into()),
            phases: vec![
                Phase::Map {
                    fn_name: "map_identity".into(),
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
        let out = run_job_full(job, registry(), None, Some(ds))
            .await
            .expect("ok");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value, serde_json::json!(5));
    }

    #[tokio::test]
    async fn bucket_inputs_over_empty_bucket_yield_empty_result() {
        use dynomite::embed::hooks::MemoryDatastore;
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let job = MapReduceJob {
            inputs: Inputs::Bucket("nonexistent".into()),
            phases: vec![Phase::Reduce {
                fn_name: "reduce_sum".into(),
                arg: None,
                keep: true,
            }],
            timeout_ms: None,
        };
        // An empty / nonexistent bucket is not an error: the reduce
        // runs over an empty set and the pipeline completes.
        let out = run_job_full(job, registry(), None, Some(ds))
            .await
            .expect("ok, not error");
        assert_eq!(
            out,
            vec![PhaseOutput {
                phase: 0,
                value: serde_json::json!(0)
            }]
        );
    }

    #[tokio::test]
    async fn bucket_inputs_stream_through_streaming_entry_point() {
        use dynomite::embed::hooks::MemoryDatastore;
        let ds = MemoryDatastore::new();
        ds.insert(b"b", b"k1");
        ds.insert(b"b", b"k2");
        let ds: Arc<dyn Datastore> = Arc::new(ds);
        let job = MapReduceJob {
            inputs: Inputs::Bucket("b".into()),
            phases: vec![],
            timeout_ms: None,
        };
        let rx = run_job_streaming_full(job, registry(), None, Some(ds));
        let items = drain_stream(rx).await;
        assert_eq!(items.len(), 1);
        let b = items[0].as_ref().expect("ok");
        assert_eq!(b.phase, 0);
        assert_eq!(b.data.len(), 2);
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
    async fn link_phase_without_datastore_returns_typed_error() {
        // `run_job` has no datastore, so a link phase cannot fetch
        // objects to read their links: it surfaces the typed error
        // rather than emitting an empty result. The datastore-backed
        // path is exercised in the integration tests.
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

    // ---- link-phase error arms (scripted datastore) ----------------

    fn link_job() -> MapReduceJob {
        MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
            phases: vec![Phase::Link {
                bucket: None,
                tag: None,
                keep: true,
            }],
            timeout_ms: None,
        }
    }

    #[tokio::test]
    async fn link_phase_riak_get_error_is_phase_failed() {
        let ds = store_with(Some(Err(())));
        let err = run_job_full(link_job(), registry(), None, Some(ds))
            .await
            .expect_err("riak get fails");
        assert!(matches!(err, MrError::PhaseFailed { kind: "link", .. }));
    }

    #[tokio::test]
    async fn link_phase_undecodable_object_is_phase_failed() {
        // riak_get returns bytes that are not a valid HttpObject
        // envelope; decode fails and the link phase reports it.
        let ds = store_with(Some(Ok(Some(vec![0xff, 0xff, 0xff]))));
        let err = run_job_full(link_job(), registry(), None, Some(ds))
            .await
            .expect_err("decode fails");
        assert!(matches!(err, MrError::PhaseFailed { kind: "link", .. }));
    }

    #[tokio::test]
    async fn link_phase_missing_object_yields_empty_via_scripted_store() {
        // riak_get returns None: no links, no error.
        let ds = store_with(Some(Ok(None)));
        let out = run_job_full(link_job(), registry(), None, Some(ds))
            .await
            .expect("missing is not an error");
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn link_phase_input_missing_bucket_key_is_phase_failed() {
        // A link phase fed a datum with no bucket/key strings cannot
        // resolve a target and reports a typed phase error. A reduce
        // upstream produces an integer datum (no bucket/key).
        let ds = store_with(Some(Ok(None)));
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(1))]),
            phases: vec![
                Phase::Map {
                    fn_name: "map_object_value".into(),
                    arg: None,
                    keep: false,
                },
                Phase::Link {
                    bucket: None,
                    tag: None,
                    keep: true,
                },
            ],
            timeout_ms: None,
        };
        let err = run_job_full(job, registry(), None, Some(ds))
            .await
            .expect_err("non-routing datum into link");
        assert!(matches!(err, MrError::PhaseFailed { kind: "link", .. }));
    }

    #[tokio::test]
    async fn link_phase_emits_matching_targets_via_scripted_store() {
        // A valid HttpObject envelope with links drives the matching
        // and emit path of run_link_phase without needing the noxu
        // engine.
        let obj = crate::proto::http::object::HttpObject {
            value: b"src".to_vec(),
            content_type: None,
            indexes: Vec::new(),
            links: vec![
                crate::proto::http::object::HttpLink {
                    bucket: "people".into(),
                    key: "b".into(),
                    tag: "friend".into(),
                },
                crate::proto::http::object::HttpLink {
                    bucket: "work".into(),
                    key: "acme".into(),
                    tag: "colleague".into(),
                },
            ],
        };
        let ds = store_with(Some(Ok(Some(obj.to_storage_bytes()))));
        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::pair("people", "a")]),
            phases: vec![Phase::Link {
                bucket: Some("people".into()),
                tag: Some("friend".into()),
                keep: true,
            }],
            timeout_ms: None,
        };
        let out = run_job_full(job, registry(), None, Some(ds))
            .await
            .expect("ok");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value["bucket"], "people");
        assert_eq!(out[0].value["key"], "b");
    }

    // ---- WasmModule error mapping ----------------------------------

    struct ScriptedWasm {
        outcome: WasmOutcome,
    }

    enum WasmOutcome {
        Ok(Vec<Value>),
        WasmError,
        GenericError,
    }

    impl WasmHook for ScriptedWasm {
        fn apply_phase(
            &self,
            _module_id: &str,
            _fn_name: &str,
            _inputs: &[Value],
        ) -> Result<Vec<Value>, MrError> {
            match &self.outcome {
                WasmOutcome::Ok(v) => Ok(v.clone()),
                WasmOutcome::WasmError => Err(MrError::WasmExecutionTimeout),
                WasmOutcome::GenericError => Err(MrError::Json("boom".into())),
            }
        }
    }

    fn wasm_job() -> MapReduceJob {
        MapReduceJob {
            inputs: Inputs::KeyData(vec![KeyDatum::with_value("b", "k", serde_json::json!(1))]),
            phases: vec![Phase::WasmModule {
                module_id: "m".into(),
                fn_name: "f".into(),
                arg: None,
                keep: true,
            }],
            timeout_ms: None,
        }
    }

    #[tokio::test]
    async fn wasm_phase_success_threads_output() {
        let hook: Arc<dyn WasmHook> = Arc::new(ScriptedWasm {
            outcome: WasmOutcome::Ok(vec![serde_json::json!("done")]),
        });
        let out = run_job_with_wasm(wasm_job(), registry(), Some(hook))
            .await
            .expect("ok");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value, serde_json::json!("done"));
    }

    #[tokio::test]
    async fn wasm_phase_wasm_error_passes_through_untouched() {
        // A Wasm-specific error variant is surfaced verbatim, not
        // re-wrapped as a generic PhaseFailed.
        let hook: Arc<dyn WasmHook> = Arc::new(ScriptedWasm {
            outcome: WasmOutcome::WasmError,
        });
        let err = run_job_with_wasm(wasm_job(), registry(), Some(hook))
            .await
            .expect_err("wasm timeout");
        assert!(matches!(err, MrError::WasmExecutionTimeout));
    }

    #[tokio::test]
    async fn wasm_phase_generic_error_is_wrapped_as_phase_failed() {
        // A non-Wasm error from the hook is wrapped with the phase
        // index and "wasm" kind.
        let hook: Arc<dyn WasmHook> = Arc::new(ScriptedWasm {
            outcome: WasmOutcome::GenericError,
        });
        let err = run_job_with_wasm(wasm_job(), registry(), Some(hook))
            .await
            .expect_err("wrapped");
        assert!(matches!(err, MrError::PhaseFailed { kind: "wasm", .. }));
    }

    #[tokio::test]
    async fn streaming_with_wasm_threads_phase_output() {
        // The wasm streaming entry point yields one batch for the
        // kept wasm phase.
        let hook: Arc<dyn WasmHook> = Arc::new(ScriptedWasm {
            outcome: WasmOutcome::Ok(vec![serde_json::json!("w")]),
        });
        let rx = run_job_streaming_with_wasm(wasm_job(), registry(), Some(hook));
        let items = drain_stream(rx).await;
        assert_eq!(items.len(), 1);
        let b = items[0].as_ref().expect("ok");
        assert_eq!(b.data, vec![serde_json::json!("w")]);
    }

    #[tokio::test]
    async fn bucket_scan_stream_error_surfaces_as_pipeline_error() {
        // ScriptedStore does not override list_keys_stream, so the
        // default yields a single Unsupported error item; the bucket
        // resolver maps it to a Pipeline error.
        let ds = store_with(Some(Ok(None)));
        let job = MapReduceJob {
            inputs: Inputs::Bucket("b".into()),
            phases: vec![],
            timeout_ms: None,
        };
        let err = run_job_full(job, registry(), None, Some(ds))
            .await
            .expect_err("scan errors");
        assert!(matches!(err, MrError::Pipeline(_)));
    }

    #[tokio::test]
    async fn scripted_store_protocol_and_dispatch_are_exercised() {
        // Cover the ScriptedStore trampoline helpers directly so the
        // test fixture itself does not drag coverage.
        let store = ScriptedStore { body: None };
        assert_eq!(store.protocol(), dynomite::embed::Protocol::Custom);
        let rsp = store
            .dispatch(dynomite::msg::Msg::new(
                7,
                dynomite::msg::MsgType::Unknown,
                true,
            ))
            .await
            .expect("dispatch ok");
        assert_eq!(rsp.id(), 7);
    }
}
