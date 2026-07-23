//! Riak-compatible protocol layer for the Dynomite Rust port.
//!
//! `dyniak` is an optional layer that operators can put in front of
//! the Dynomite cluster substrate to expose a Riak client API. The
//! crate is intentionally narrow: it owns only the Riak-specific
//! pieces (wire codec, request dispatch, storage bridge) and reuses
//! the substrate already shipped in `crates/dynomite/` (hashing,
//! gossip, vnodes, quorum, dispatch).
//!
//! # Public surface
//!
//! * [`proto::pb`] -- the Riak Protocol Buffers wire format. Hand-rolled
//!   `prost::Message` structs for the v0.0.1 operation set
//!   ([`proto::pb::RpbPingReq`] / [`proto::pb::RpbPingResp`],
//!   [`proto::pb::RpbGetReq`] /
//!   [`proto::pb::RpbGetResp`], [`proto::pb::RpbPutReq`] /
//!   [`proto::pb::RpbPutResp`], [`proto::pb::RpbDelReq`]) plus an
//!   error response. The framing layer is exposed through
//!   [`proto::pb::framer`].
//! * [`server::serve_pbc`] -- TCP accept loop that reads PBC frames,
//!   dispatches each request to a [`dynomite::embed::Datastore`], and
//!   writes framed replies.
//! * [`server::handle_conn`] -- the per-connection driver. Generic
//!   over [`tokio::io::AsyncRead`] / [`tokio::io::AsyncWrite`] so
//!   tests can drive it through `tokio::io::duplex` without a real
//!   socket.
//! * [`error::RiakError`] -- the crate's top-level error type.
//! * [`datastore::NoxuDatastore`] -- gated behind the `noxu` Cargo
//!   feature; bridges this crate to the in-process Noxu DB storage
//!   engine.
//! * `serve_http_with_search` -- gated behind the `search` Cargo
//!   feature; serves the HTTP gateway with a
//!   `dynomite_search::VectorRegistry` wired in, lighting up the
//!   per-bucket text (substring + approximate-regex) and vector-KNN
//!   index-management and search routes. Without the feature, or
//!   without a registry, those routes reply `501 Not Implemented`
//!   and the object / list / transaction surface is unchanged.
//!
//! # Architecture
//!
//! ```text
//! TCP listener (tokio::net::TcpListener)
//!         |
//!         v
//! serve_pbc()  -- accept loop, spawns one task per conn
//!         |
//!         v
//! handle_conn(stream, datastore)
//!   - decode 4-byte BE length
//!   - decode 1-byte msg code
//!   - decode protobuf body via prost
//!   - dispatch through dynomite::embed::Datastore
//!   - encode response via prost
//!   - write framed response
//! ```
//!
//! # Encoding
//!
//! The PBC path is hard-coded to `application/x-protobuf`; the bytes
//! travel through a [`dyn_encoding::ProtobufCodec`] wired up in
//! [`proto::pb::codec_registry`]. The HTTP gateway ([`serve_http`])
//! negotiates JSON / CBOR / protobuf per-request through the same
//! registry.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod datastore;
pub mod error;
pub mod proto;
pub mod ramp;
#[cfg(feature = "noxu")]
pub mod ramp_store;
pub mod server;
pub mod txn;

pub use crate::error::RiakError;
pub use crate::proto::http::{serve_http, serve_http_tls};
pub use crate::proto::http::{serve_http_tls_with_routing, serve_http_with_routing};
#[cfg(feature = "search")]
pub use crate::proto::http::{serve_http_tls_with_search, serve_http_with_search, SearchState};
#[cfg(all(feature = "search", feature = "wasm"))]
pub use crate::proto::http::{
    serve_http_tls_with_search_and_wasm, serve_http_with_search_and_wasm,
};
#[cfg(feature = "wasm")]
pub use crate::proto::http::{serve_http_tls_with_wasm, serve_http_with_wasm};
pub use crate::server::{handle_conn, serve_pbc, serve_pbc_tls};

// Cluster admin RPC entry points -- v0.0.4 admin slice. Wired
// to the `dyn-admin` cluster-* subcommands. Re-exported below
// the existing block so parallel branches do not conflict.
pub use crate::server::{handle_conn_with_admin, serve_pbc_tls_with_admin, serve_pbc_with_admin};

// Routing-hooks entry point -- bucket-property knobs slice.
// Re-exported below the prior block so parallel branches do
// not conflict.
pub use crate::server::{handle_conn_with_hooks, serve_pbc_with_routing};

// AAE-status entry points -- AAE R5 slice. Re-exported below
// the prior block so parallel branches do not conflict.
pub use crate::server::{handle_conn_with_aae_status, serve_pbc_with_aae_status};

// QUIC PBC accept loops. Gated on the `quic` feature, which
// forwards to the engine's shared quiche integration. The
// handler that drives each accepted connection is the same one
// the TCP and TLS-over-TCP paths use; only the byte transport
// differs.
#[cfg(feature = "quic")]
pub use crate::server::{serve_pbc_quic, serve_pbc_quic_with_admin};

pub mod aae;
pub mod datatypes;
pub mod handoff;

// TTL-driven sibling and tombstone garbage-collection FSM.
// Re-exported below the prior block so parallel branches do
// not conflict.
pub mod reaper;

// MapReduce framework added by the v0.0.3 slice. The module owns
// its own public surface; see `crate::mapreduce` for entry points.
pub mod mapreduce;

// Walk-N-successors replication and bucket-routing helpers.
// Re-exported below the prior block so parallel branches do
// not conflict.
pub mod bucket_props;
pub mod crdt_store;
pub mod replica_apply;
pub mod replication;
pub mod router;

pub use crate::bucket_props::{BucketProps, BucketPropsRegistry};
pub use crate::replica_apply::ReplicaApplier;
pub use crate::replication::{
    plan_replicas, ReplicationPlan, ReplicationStrategy, ReplicationStrategyError, RingPoint,
    RingView,
};
pub use crate::router::{BucketRouter, PeerOp, PeerOutbound, RouteDecision, RoutingHooks};

// Multi-key transaction surface -- dyniak extension beyond Riak's
// per-key eventual consistency. The HTTP gateway exposes it through
// the `POST /transactions` and `POST /buckets/{bucket}/transactions`
// routes wired into [`serve_http`]; no separate server entry point is
// needed. The PBC `DynRpbTxn*` extension is a tracked follow-up (see
// `docs/journal/2026-06-05-dyniak-xa.md`).
pub use crate::txn::{TransactionalStore, TxnBatch, TxnOp, TxnOutcome, TxnStoreError};

pub use crate::ramp::{select, RampClock, RampItem, Timestamp};
#[cfg(feature = "noxu")]
pub use crate::ramp_store::{
    ramp_read, ramp_write, HttpRampReadRequest, HttpRampReadResponse, HttpRampWrite,
    HttpRampWriteRequest, HttpRampWriteResponse, RampCoordinator, RampError, RampStore, RampWrite,
};
