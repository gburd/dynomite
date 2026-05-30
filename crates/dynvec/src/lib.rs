//! Vector storage + HNSW ANN index engine.
//!
//! `dynvec` provides three things:
//!
//! 1. A node-local vector row store with two encodings (per-vector
//!    int8 quantisation and IEEE 754 half-precision floats) and
//!    three distance metrics (euclidean, cosine, dot product).
//! 2. An HNSW approximate-nearest-neighbour index for k-NN queries.
//! 3. A per-table [`Engine`] handle, which is the unit the
//!    `dynomite::vector` registry hands out when serving Redis
//!    Stack RediSearch FT.* commands.
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
//! * [`engine`] -- per-table handle exposed to embedders.
//! * [`api`] -- the HTTP API (gated on the `http` feature; kept
//!   as a debug surface only).
//!
//! The distributed k-NN coordinator that used to live here has
//! moved to `dynomite::vector::query_fsm`, where it can sit
//! against the cluster machinery directly.
//!
//! # Quick start
//!
//! ```
//! use std::collections::HashMap;
//! use dynvec::distance::Distance;
//! use dynvec::encoding::Codec;
//! use dynvec::index::HnswParams;
//! use dynvec::storage::{TableSchema, VectorStore};
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

#![doc(html_root_url = "https://docs.rs/dynvec/0.0.1")]

pub mod distance;
pub mod encoding;
pub mod engine;
pub mod index;
pub mod storage;

#[cfg(feature = "http")]
pub mod api;

pub use crate::distance::Distance;
pub use crate::encoding::{Codec, EncodedVector, Encoder, Fp16, Int8Quantized};
pub use crate::engine::Engine;
pub use crate::index::{HnswIndex, HnswParams, NodeId, SearchResult};
pub use crate::storage::{
    Backend, MemoryBackend, RowKey, StoreError, TableSchema, TableStats, VectorRow, VectorStore,
};
