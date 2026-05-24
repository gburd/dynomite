//! Wire-format codec abstraction for the Riak protocol layer.
//!
//! `dyn-encoding` defines a small, object-safe trait surface that lets
//! a single connection negotiate between a number of structured
//! encodings on a per-request basis. The crate ships baseline
//! implementations for the three encodings that map cleanest onto
//! the Rust ecosystem:
//!
//! * [`JsonCodec`]  -- `application/json`  (via `serde_json`)
//! * [`CborCodec`]  -- `application/cbor`  (via `ciborium`)
//! * [`ProtobufCodec`] -- `application/x-protobuf` (via `prost`)
//!
//! Four further encodings (FlatBuffers, Cap'n Proto, Bebop, BSON) are
//! deferred. The trait surface is shaped so adding any of them is a
//! mechanical exercise: a new module under `codec/`, a new `register`
//! entry point bounded on the codec's native trait
//! (`flatbuffers::Follow + Push`, `capnp::message::Builder`, ...),
//! and a single line in [`CodecRegistry::with_baseline`].
//!
//! # Two-layer design
//!
//! The codec abstraction is split into two cooperating traits:
//!
//! 1. [`WireValue`]: implemented by the structured types that travel
//!    on the wire. Provides a stable [`WireTypeId`] for content-type
//!    plus type negotiation.
//! 2. [`ErasedWireValue`]: an object-safe view over `WireValue` so
//!    codec implementations can take `&dyn ErasedWireValue` without
//!    being generic. A blanket impl on every `T: WireValue` makes
//!    this transparent at call sites.
//!
//! Each codec is itself object-safe via the [`WireCodec`] trait,
//! so [`CodecRegistry`] can store a heterogeneous bag of codecs
//! and look one up by content-type.
//!
//! # Per-codec type registration
//!
//! Schema-first codecs (protobuf today, FlatBuffers / Cap'n Proto /
//! Bebop tomorrow) require knowing the concrete message type to
//! reify bytes. Schema-less codecs (JSON / CBOR / BSON) could in
//! principle dispatch through `erased_serde`, but doing so would
//! diverge from the schema-first path. To keep all codec impls
//! shaped the same way, every codec carries an internal
//! per-[`WireTypeId`] registration table populated through the
//! codec's own `register::<T>()` entry point.
//!
//! # Example
//!
//! ```
//! use dyn_encoding::{
//!     CodecRegistry, ErasedWireValue, JsonCodec, WireTypeId, WireValue,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
//! struct GetReq {
//!     bucket: String,
//!     key: Vec<u8>,
//!     timeout_ms: u32,
//! }
//!
//! impl WireValue for GetReq {
//!     fn wire_type_id() -> WireTypeId {
//!         WireTypeId::new("riak.GetReq")
//!     }
//! }
//!
//! // Build a codec, register the message type, install into a
//! // registry keyed by content-type.
//! let mut json = JsonCodec::new();
//! json.register::<GetReq>();
//!
//! let mut registry = CodecRegistry::new();
//! registry.register(json);
//!
//! let codec = registry
//!     .for_content_type("application/json")
//!     .expect("json codec is registered");
//!
//! let req = GetReq {
//!     bucket: "bk".into(),
//!     key: b"k".to_vec(),
//!     timeout_ms: 5_000,
//! };
//! let bytes = codec.encode(&req).expect("encode");
//! let back = codec.decode(GetReq::wire_type_id(), &bytes).expect("decode");
//! assert_eq!(back.type_id(), GetReq::wire_type_id());
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod codec;
mod error;
mod registry;
mod value;

pub use crate::codec::cbor::CborCodec;
pub use crate::codec::json::JsonCodec;
pub use crate::codec::protobuf::ProtobufCodec;
pub use crate::error::CodecError;
pub use crate::registry::CodecRegistry;
pub use crate::value::{ErasedWireValue, WireCodec, WireTypeId, WireValue};
