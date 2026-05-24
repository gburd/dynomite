//! MapReduce ("pipes") for the Riak compatibility layer.
//!
//! Riak ships a JavaScript / Erlang MapReduce engine. This crate
//! takes a different bet: a fixed registry of named built-in phase
//! functions written in Rust. Operators name a function in a
//! [`Phase`] (for example `"map_object_value"`), and the framework
//! dispatches to the matching Rust impl. The rationale is recorded
//! in `docs/journal/2026-05-24-dyn-riak-mapreduce.md`.
//!
//! # Architecture
//!
//! A [`MapReduceJob`] has two pieces:
//!
//! * [`Inputs`] -- the seed values for the pipeline. Either an
//!   explicit list of `(bucket, key)` pairs, an inline list of
//!   `KeyDatum` objects (with values inline), or a bucket name (all
//!   keys in the bucket; not implemented in this slice and rejected
//!   with [`MrError::UnsupportedInputs`]).
//! * [`Phase`] list -- an ordered pipeline of [`Phase::Map`],
//!   [`Phase::Reduce`], [`Phase::Link`] and the reserved
//!   [`Phase::WasmModule`] variants.
//!
//! Execution flows through the [`executor::run_job`] entry point.
//! Each phase is wired between two [`tokio::sync::mpsc`] channels;
//! the executor keeps one task per phase, the previous phase's
//! outbound is the next phase's inbound, and the final phase
//! outbound is collected into the response envelope. The pipe
//! shape mirrors Riak's "pipe of phases" pattern from
//! `riak_pipe_*.erl`.
//!
//! # Determinism
//!
//! Built-in phase functions are pure Rust and side-effect-free.
//! `tokio::sync::mpsc` preserves FIFO. Each phase processes its
//! inbound queue serially. The result of running the same job
//! against the same input list is therefore byte-identical across
//! runs.
//!
//! # Examples
//!
//! ```
//! use dyn_riak::mapreduce::{
//!     builtins, executor::run_job, registry::PhaseRegistry, Inputs,
//!     KeyDatum, MapReduceJob, Phase,
//! };
//! use std::sync::Arc;
//!
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all().build().unwrap().block_on(async {
//! let registry = Arc::new(builtins::default_registry());
//! let job = MapReduceJob {
//!     inputs: Inputs::KeyData(vec![
//!         KeyDatum::with_value("b", "k1", serde_json::json!(1)),
//!         KeyDatum::with_value("b", "k2", serde_json::json!(2)),
//!         KeyDatum::with_value("b", "k3", serde_json::json!(3)),
//!     ]),
//!     phases: vec![
//!         Phase::Map { fn_name: "map_object_value".into(), arg: None, keep: false },
//!         Phase::Reduce { fn_name: "reduce_sum".into(), arg: None, keep: true },
//!     ],
//!     timeout_ms: None,
//! };
//! let out = run_job(job, registry).await.unwrap();
//! assert_eq!(out.len(), 1);
//! assert_eq!(out[0].value, serde_json::json!(6));
//! # });
//! ```

pub mod builtins;
pub mod executor;
pub mod job;
pub mod phase;
pub mod registry;

pub use crate::mapreduce::executor::{run_job, MrError, PhaseOutput};
pub use crate::mapreduce::job::{Inputs, KeyDatum, MapReduceJob};
pub use crate::mapreduce::phase::Phase;
pub use crate::mapreduce::registry::{MapFn, PhaseRegistry, ReduceFn};
