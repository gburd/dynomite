//! Baseline codec implementations.
//!
//! Seven baseline codecs ship in this module today:
//!
//! * [`json`][json] -- `application/json`, via `serde_json`.
//! * [`cbor`][cbor] -- `application/cbor`, via `ciborium`.
//! * [`protobuf`][protobuf] -- `application/x-protobuf`, via `prost`.
//! * [`flatbuffers`][flatbuffers] -- `application/octet-stream;schema=flatbuffers`,
//!   via the `flatbuffers` runtime.
//! * [`capnp`][capnp] -- `application/capnproto`, via the `capnp` runtime.
//! * [`bebop`][bebop] -- `application/x-bebop`, via the `bebop` runtime.
//! * [`bson`][bson] -- `application/bson`, via the `bson` crate.
//!
//! Adding an eighth codec is mechanical: copy one of the modules
//! below, replace the encoder/decoder bodies with the new format's
//! native API, and add a single `register` call to
//! [`crate::CodecRegistry::with_baseline`].

pub mod bebop;
pub mod bson;
pub mod capnp;
pub mod cbor;
pub mod flatbuffers;
pub mod json;
pub mod protobuf;
