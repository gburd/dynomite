//! Riak-compatible protocol layer for the Dynomite Rust port.
//!
//! `dyn-riak` is an optional layer that operators can put in front of
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
//!   ([`proto::pb::RpbPing`], [`proto::pb::RpbGetReq`] /
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
//! [`proto::pb::codec_registry`]. The `dyn-encoding` machinery is in
//! place so the upcoming HTTP gateway can negotiate JSON / CBOR /
//! protobuf per-request through the same registry.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod datastore;
pub mod error;
pub mod proto;
pub mod server;

pub use crate::error::RiakError;
pub use crate::server::{handle_conn, serve_pbc};

pub mod aae;
