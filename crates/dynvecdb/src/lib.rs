//! Distributed vector database engine.
//!
//! `dynvecdb` couples three things into one engine:
//!
//! 1. A node-local vector row store with two encodings (per-vector
//!    int8 quantisation and IEEE 754 half-precision floats) and
//!    three distance metrics (euclidean, cosine, dot product).
//! 2. An HNSW approximate-nearest-neighbour index for k-NN queries.
//! 3. A cluster query coordinator -- a [`gen_fsm`]-driven state
//!    machine -- that fans a search out to peers, gathers their
//!    per-peer top-K, and merges them into a global top-K.
//!
//! # Module layout
//!
//! * [`encoding`] -- vector compression codecs.
//! * [`distance`] -- L2 / cosine / dot product scoring.
//! * [`index`] -- the HNSW graph.
//! * [`storage`] -- row store + per-table HNSW index, with a
//!   pluggable [`storage::Backend`] trait so the same surface
//!   works against an in-memory backend (default) or a Noxu DB
//!   (off-by-default `noxu` feature).
//! * [`cluster_query`] -- the distributed search coordinator.
//! * [`api`] -- the HTTP API (gated on the `http` feature).
//!
//! # Quick start
//!
//! ```
//! use std::collections::HashMap;
//! use dynvecdb::distance::Distance;
//! use dynvecdb::encoding::Codec;
//! use dynvecdb::index::HnswParams;
//! use dynvecdb::storage::{TableSchema, VectorStore};
//!
//! let store = VectorStore::in_memory();
//! store.create_table(TableSchema {
//!     name: "demo".to_string(),
//!     dim: 3,
//!     codec: Codec::Int8Quantized,
//!     distance: Distance::Cosine,
//!     hnsw: HnswParams::default(),
//! }).unwrap();
//! store
//!     .upsert("demo", b"a".to_vec(), &[1.0, 0.0, 0.0], HashMap::new())
//!     .unwrap();
//! store
//!     .upsert("demo", b"b".to_vec(), &[0.0, 1.0, 0.0], HashMap::new())
//!     .unwrap();
//! let hits = store.search("demo", &[0.95, 0.05, 0.0], 1, None).unwrap();
//! assert_eq!(hits[0].0.key, b"a");
//! ```
//!
//! # CQL stretch goal
//!
//! A drop-in CQL native-protocol surface that exposes the
//! [`storage`] layer through Cassandra-compatible CREATE TABLE
//! and SELECT statements is not part of this MVP. The
//! architectural shape and effort estimate live in
//! `docs/dynvecdb/cql-stretch.md`. Decision points in this crate
//! that a CQL surface would need to reach into are annotated
//! with `// CQL future:`.

#![doc(html_root_url = "https://docs.rs/dynvecdb/0.0.1")]

pub mod cluster_query;
pub mod distance;
pub mod encoding;
pub mod index;
pub mod storage;

#[cfg(feature = "http")]
pub mod api;

pub use crate::distance::Distance;
pub use crate::encoding::{Codec, EncodedVector, Encoder, Fp16, Int8Quantized};
pub use crate::index::{HnswIndex, HnswParams, NodeId, SearchResult};
pub use crate::storage::{
    Backend, MemoryBackend, RowKey, StoreError, TableSchema, TableStats, VectorRow, VectorStore,
};
