//! `dyn-admin` -- cluster admin CLI for the Dynomite Rust port.
//!
//! Operators use this binary to inspect a running node or cluster
//! over the same PBC and HTTP surfaces that `dynomited` exposes.
//! The crate is modelled on `dyn-hash-tool`: a small CLI that links
//! the engine library, with one module per subcommand and a shared
//! [`client`] helper that wraps the Riak PBC framer and a minimal
//! HTTP/1.1 GET.
//!
//! See the crate `README.md` for usage examples and the deferred
//! mutating-subcommands list.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod commands;
pub mod error;
pub mod output;

pub use crate::error::AdminError;
