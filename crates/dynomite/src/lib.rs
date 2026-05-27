//! Dynomite is a distributed replication layer for Redis and Memcached
//! datastores. This crate provides the engine as a library so it can be
//! embedded in another Rust program, and is also driven by the `dynomited`
//! binary as a standalone server.
//!
//! # Embedding
//!
//! The public embedding API lives in [`embed`]. Build a [`Server`] with
//! [`ServerBuilder`] and drive it via the returned [`ServerHandle`]:
//!
//! ```no_run
//! use dynomite::{Server, ServerBuilder};
//! use dynomite::conf::DataStore;
//! # tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
//! let server = ServerBuilder::new("dyn_o_mite")
//!     .listen("127.0.0.1:0".parse().unwrap())
//!     .dyn_listen("127.0.0.1:0".parse().unwrap())
//!     .data_store(DataStore::Redis)
//!     .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
//!     .tokens_str("0")
//!     .build()
//!     .unwrap();
//! let handle = server.start().await.unwrap();
//! handle.shutdown().await.unwrap();
//! # });
//! ```
//!
//! The full reference manual lives in `docs/book/`. See the [`embed`]
//! module for the complete embedding cookbook.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod admin;
pub mod cluster;
pub mod conf;
pub mod core;
pub mod crypto;
pub mod embed;
pub mod entropy;
pub mod hashkit;
pub mod io;
pub mod msg;
pub mod net;
pub mod proto;
pub mod seeds;
pub mod stats;
pub mod util;

pub use crate::core::types::{DynError, Msec, MsgId, Sec, SecureServerOption, Status, Usec};
pub use crate::embed::{Server, ServerBuilder, ServerHandle};
