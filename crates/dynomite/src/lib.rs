//! Dynomite is a distributed replication layer for Redis and Memcached
//! datastores. This crate provides the engine as a library so it can be
//! embedded in another Rust program, and is also driven by the `dynomited`
//! binary as a standalone server.
//!
//! The full reference manual lives in the `docs/book/` directory of this
//! repository. See the `embed` module for the public embedding API.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod conf;
pub mod core;
pub mod crypto;
pub mod hashkit;
pub mod io;
pub mod msg;
pub mod net;
pub mod proto;
pub mod stats;
pub mod util;

pub use crate::core::types::{DynError, Msec, MsgId, Sec, SecureServerOption, Status, Usec};
