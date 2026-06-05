//! Object envelope exchanged over the Riak HTTP gateway.
//!
//! The HTTP K/V endpoints carry a single logical object in one of the
//! three baseline serialisations the gateway negotiates
//! ([`crate::proto::http::content_type::SUPPORTED_CONTENT_TYPES`]):
//! `application/json`, `application/cbor`, and
//! `application/x-protobuf`. To make a value stored under one
//! encoding fetchable under any other, the gateway does not persist
//! the raw request bytes. Instead it decodes the request body into an
//! [`HttpObject`] and stores a canonical, encoding-independent form of
//! that struct (its protobuf serialisation). A later `GET` decodes the
//! canonical form back into an [`HttpObject`] and re-encodes it with
//! whatever codec the client negotiated, so the same logical object
//! survives a json-in / cbor-out (or protobuf-out) round-trip.
//!
//! # Wire shape
//!
//! The envelope is a fixed-schema struct so the same Rust type can be
//! registered with the JSON, CBOR, and protobuf codecs uniformly.
//! Expressed as JSON it is:
//!
//! ```json
//! {
//!   "value": [104, 101, 108, 108, 111],
//!   "content_type": "text/plain",
//!   "indexes": [{"name": "age_int", "value": "42"}]
//! }
//! ```
//!
//! * `value` -- the opaque object payload bytes. In JSON these are a
//!   numeric array; in CBOR an integer array; in protobuf a `bytes`
//!   field. The bytes themselves are preserved verbatim across codecs.
//! * `content_type` -- optional declared media type of `value`. This
//!   is object metadata; it is distinct from the HTTP `Content-Type`
//!   header, which names the codec that framed the envelope.
//! * `indexes` -- secondary-index `(name, value)` pairs. `name` ends
//!   in `_int` (integer index) or `_bin` (binary index), mirroring the
//!   PBC path's [`crate::proto::pb::messages::RpbPutReq::indexes`].
//!
//! A dedicated envelope is used rather than reusing
//! [`crate::proto::pb::messages::RpbGetResp`], whose `content` field
//! is a flat `repeated bytes` with no place for the per-object
//! content-type or structured index metadata the HTTP path round-trips.

use std::sync::OnceLock;

use dyn_encoding::{CborCodec, CodecRegistry, JsonCodec, ProtobufCodec, WireTypeId, WireValue};
use prost::Message;
use serde::{Deserialize, Serialize};

/// One secondary-index entry attached to an [`HttpObject`].
///
/// `name` selects the index encoding the storage layer applies: a
/// name ending in `_int` is stored as a big-endian integer (so range
/// scans iterate numerically), anything else is stored as raw bytes.
#[derive(Clone, Eq, PartialEq, Message, Serialize, Deserialize)]
pub struct HttpIndex {
    /// Index name, for example `age_int` or `city_bin`.
    #[prost(string, tag = "1")]
    pub name: String,
    /// Index value as supplied by the client (textual form).
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A logical Riak object as carried by the HTTP K/V endpoints.
///
/// The struct is the unit of cross-encoding round-tripping: a body
/// decoded from one codec re-encodes losslessly under any other.
///
/// # Examples
///
/// ```
/// use dyniak::proto::http::object::HttpObject;
/// let obj = HttpObject {
///     value: b"hello".to_vec(),
///     content_type: Some("text/plain".to_string()),
///     indexes: Vec::new(),
/// };
/// assert_eq!(obj.value, b"hello");
/// ```
#[derive(Clone, Eq, PartialEq, Message, Serialize, Deserialize)]
pub struct HttpObject {
    /// Opaque object payload bytes.
    #[prost(bytes = "vec", tag = "1")]
    #[serde(default)]
    pub value: Vec<u8>,
    /// Optional declared media type of [`Self::value`]. Object
    /// metadata, not the codec content-type of the envelope.
    #[prost(string, optional, tag = "2")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Secondary-index entries associated with the object.
    #[prost(message, repeated, tag = "3")]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<HttpIndex>,
}

impl WireValue for HttpObject {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.http.Object")
    }
}

impl HttpObject {
    /// Serialise the object into its canonical, encoding-independent
    /// storage form (its protobuf bytes).
    ///
    /// This is the form persisted under the primary K/V key so a
    /// subsequent fetch can re-encode it under any negotiated codec.
    #[must_use]
    pub fn to_storage_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    /// Reconstruct an object from its canonical storage form.
    ///
    /// # Errors
    ///
    /// Returns [`prost::DecodeError`] when `bytes` is not a valid
    /// protobuf encoding of the envelope (indicating corruption or a
    /// schema mismatch).
    pub fn from_storage_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        Self::decode(bytes)
    }

    /// Convert the envelope's index list into the `(name, value)`
    /// byte-pair form the storage layer's 2i API expects.
    #[must_use]
    pub fn index_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.indexes
            .iter()
            .map(|i| (i.name.clone().into_bytes(), i.value.clone().into_bytes()))
            .collect()
    }
}

/// Codec registry shared by the HTTP object endpoints.
///
/// Holds a JSON, CBOR, and protobuf codec, each with [`HttpObject`]
/// registered, keyed by the canonical content-type strings from
/// [`crate::proto::http::content_type::SUPPORTED_CONTENT_TYPES`]. The
/// registry is built once and reused for the life of the process.
///
/// # Examples
///
/// ```
/// use dyniak::proto::http::object::object_codecs;
/// let registry = object_codecs();
/// assert!(registry.for_content_type("application/json").is_some());
/// assert!(registry.for_content_type("application/cbor").is_some());
/// assert!(registry.for_content_type("application/x-protobuf").is_some());
/// ```
#[must_use]
pub fn object_codecs() -> &'static CodecRegistry {
    static REGISTRY: OnceLock<CodecRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut json = JsonCodec::new();
        json.register::<HttpObject>();
        let mut cbor = CborCodec::new();
        cbor.register::<HttpObject>();
        let mut protobuf = ProtobufCodec::new();
        protobuf.register::<HttpObject>();

        let mut registry = CodecRegistry::new();
        registry.register(json);
        registry.register(cbor);
        registry.register(protobuf);
        registry
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dyn_encoding::WireValue;

    fn fixture() -> HttpObject {
        HttpObject {
            value: b"hello world".to_vec(),
            content_type: Some("text/plain".to_string()),
            indexes: vec![
                HttpIndex {
                    name: "age_int".to_string(),
                    value: "42".to_string(),
                },
                HttpIndex {
                    name: "city_bin".to_string(),
                    value: "seattle".to_string(),
                },
            ],
        }
    }

    #[test]
    fn storage_form_round_trips() {
        let obj = fixture();
        let bytes = obj.to_storage_bytes();
        let back = HttpObject::from_storage_bytes(&bytes).expect("decode");
        assert_eq!(back, obj);
    }

    #[test]
    fn corrupt_storage_form_is_an_error() {
        // A length-delimited field (tag 1, wire type 2) with a length
        // that overruns the buffer is a hard protobuf decode error.
        let err = HttpObject::from_storage_bytes(&[0x0a, 0xff]);
        assert!(err.is_err());
    }

    #[test]
    fn cross_encoding_preserves_logical_object() {
        // Encode through JSON, decode the JSON, re-encode through CBOR
        // and protobuf, and confirm every hop reconstructs the same
        // logical object.
        let obj = fixture();
        let registry = object_codecs();

        let json = registry.for_content_type("application/json").expect("json");
        let cbor = registry.for_content_type("application/cbor").expect("cbor");
        let pb = registry
            .for_content_type("application/x-protobuf")
            .expect("protobuf");

        let json_bytes = json.encode(&obj).expect("json encode");
        let from_json = json
            .decode(HttpObject::wire_type_id(), &json_bytes)
            .expect("json decode");
        let from_json = from_json
            .as_any()
            .downcast_ref::<HttpObject>()
            .expect("downcast json");
        assert_eq!(from_json, &obj);

        let cbor_bytes = cbor.encode(from_json).expect("cbor encode");
        let from_cbor = cbor
            .decode(HttpObject::wire_type_id(), &cbor_bytes)
            .expect("cbor decode");
        let from_cbor = from_cbor
            .as_any()
            .downcast_ref::<HttpObject>()
            .expect("downcast cbor");
        assert_eq!(from_cbor, &obj);

        let pb_bytes = pb.encode(from_cbor).expect("pb encode");
        let from_pb = pb
            .decode(HttpObject::wire_type_id(), &pb_bytes)
            .expect("pb decode");
        let from_pb = from_pb
            .as_any()
            .downcast_ref::<HttpObject>()
            .expect("downcast pb");
        assert_eq!(from_pb, &obj);
    }

    #[test]
    fn index_pairs_render_name_value_bytes() {
        let obj = fixture();
        let pairs = obj.index_pairs();
        assert_eq!(
            pairs,
            vec![
                (b"age_int".to_vec(), b"42".to_vec()),
                (b"city_bin".to_vec(), b"seattle".to_vec()),
            ]
        );
    }

    #[test]
    fn object_codecs_is_stable_across_calls() {
        let a = object_codecs();
        let b = object_codecs();
        assert!(std::ptr::eq(a, b));
    }
}
