//! Vector subsystem.
//!
//! This module is the in-process home of the Redis-Stack-style
//! RediSearch FT.* command surface. The architecture decision
//! lives in `docs/dynvec/fold-into-redis-path.md`; the short
//! version is:
//!
//! 1. The [`dynvec`] crate provides the per-table storage +
//!    HNSW index engine.
//! 2. This module wraps each registered index in a
//!    [`VectorTable`] and tracks them in a [`VectorRegistry`].
//! 3. The Redis parser recognises FT.* commands and routes
//!    them through this registry (Phase C, follow-up work).
//! 4. Distributed k-NN queries are coordinated by the
//!    [`query_fsm`] state machine, which fans out to every
//!    primary peer covering the index's key range and merges
//!    the per-peer top-K replies.
//!
//! Phase B (this commit) lands the registry, the schema
//! types, and the moved query coordinator. The FT.* command
//! parser, the cluster-machinery integration in
//! [`query_fsm`], and the deprecation of the standalone
//! [`dynvec::api`] HTTP surface are each follow-up work.

pub mod query_fsm;
pub mod registry;
pub mod schema;
pub mod wire;

pub use registry::{RegistryError, VectorRegistry, VectorTable, VectorTableInfo};
pub use schema::{
    DistanceMetric, IndexAlgorithm, MetadataField, MetadataFieldType, VectorSchema, VectorType,
};
