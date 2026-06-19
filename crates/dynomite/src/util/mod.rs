//! Small utility helpers shared across the engine: byte-slice helpers,
//! numeric parsing, time, address resolution, and the typed map
//! abstractions that wrap third-party data structures.
//!
//! The Cassandra-style estimated histogram lives next to its only
//! consumer in the `stats` module (see [`crate::stats::Histogram`]).

pub mod atoi;
pub mod dict;
pub mod dyn_string;
pub mod rbtree;
pub mod sockinfo;
pub mod time;
