//! Baseline codec implementations.
//!
//! Three baseline codecs ship in this module today:
//!
//! * [`json`]: `application/json`, via `serde_json`.
//! * [`cbor`]: `application/cbor`, via `ciborium`.
//! * [`protobuf`]: `application/x-protobuf`, via `prost`.
//!
//! Adding a fourth codec is mechanical: copy one of the modules
//! below, replace the encoder/decoder bodies with the new format's
//! native API, and add a single `register` call to
//! [`crate::CodecRegistry::with_baseline`].

pub mod cbor;
pub mod json;
pub mod protobuf;
