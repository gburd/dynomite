//! `dyniak_bench` -- a load-generation and benchmarking engine.
//!
//! This crate provides the building blocks for a small, self-contained
//! benchmarking tool that can drive workloads against three backends:
//!
//! * a RESP-2 server (Redis, dynomited, KeyDB, etc.);
//! * a Riak Protocol Buffer Client (PBC) server;
//! * an optional HTTP backend (feature `http`) for Riak's HTTP API.
//!
//! The crate is structured around a [`Driver`](crate::driver::Driver)
//! trait, a [`KeyGen`](crate::keygen::KeyGen) sampler, a
//! [`ValGen`](crate::valgen::ValGen) sampler, an
//! [`Engine`](crate::engine::Engine) that owns the worker pool, an
//! [`StatsAggregator`](crate::stats::StatsAggregator) that collects
//! HdrHistogram-based latency snapshots, a [`report`] writer that emits
//! CSV files, and a [`plot`] renderer that produces PNG graphs.
//!
//! The CLI entry point lives in `src/main.rs`. The crate also exposes
//! the same building blocks as a library so integration tests can
//! exercise individual components in-process.

#![doc(html_no_source)]

pub mod config;
pub mod driver;
pub mod engine;
pub mod error;
pub mod keygen;
pub mod plot;
pub mod report;
pub mod stats;
pub mod valgen;

pub use config::{Config, DriverKind, OpsConfig, RateConfig, RunConfig};
pub use error::{BenchError, DriverErrorClass};
