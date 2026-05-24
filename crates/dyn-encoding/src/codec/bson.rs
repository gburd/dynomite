//! `application/bson` codec backed by the `bson` crate.
//!
//! BSON is the wire format MongoDB uses; the upstream Rust crate
//! exposes a serde-compatible API. The codec is shaped exactly like
//! [`crate::JsonCodec`] / [`crate::CborCodec`]: registered types are
//! bounded on `Serialize + DeserializeOwned`, and dispatch happens by
//! [`WireTypeId`].
//!
//! BSON's wire model is document-oriented: the top-level value must
//! be a BSON document (a struct or map in serde terms). Registering
//! a type whose serde representation is not a struct or map will
//! still compile, but encoding such a value will surface a
//! [`CodecError::Encode`] from the underlying serializer at runtime.

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::CodecError;
use crate::value::{ErasedWireValue, WireCodec, WireTypeId, WireValue};

type EncodeFn =
    Box<dyn Fn(&dyn ErasedWireValue) -> Result<Vec<u8>, CodecError> + Send + Sync + 'static>;
type DecodeFn =
    Box<dyn Fn(&[u8]) -> Result<Box<dyn ErasedWireValue>, CodecError> + Send + Sync + 'static>;

/// Codec that serialises [`WireValue`] types as BSON via the
/// upstream `bson` crate's serde integration.
#[derive(Default)]
pub struct BsonCodec {
    encoders: HashMap<WireTypeId, EncodeFn>,
    decoders: HashMap<WireTypeId, DecodeFn>,
}

impl BsonCodec {
    /// Construct an empty BSON codec with no registered types.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a [`WireValue`] type with the codec.
    pub fn register<T>(&mut self) -> &mut Self
    where
        T: WireValue + Serialize + DeserializeOwned,
    {
        let id = T::wire_type_id();
        self.encoders.insert(
            id,
            Box::new(
                move |v: &dyn ErasedWireValue| -> Result<Vec<u8>, CodecError> {
                    let concrete = v
                        .as_any()
                        .downcast_ref::<T>()
                        .ok_or(CodecError::TypeMismatch { expected: id })?;
                    bson::serialize_to_vec(concrete).map_err(CodecError::encode_failure)
                },
            ),
        );
        self.decoders.insert(
            id,
            Box::new(
                move |bytes: &[u8]| -> Result<Box<dyn ErasedWireValue>, CodecError> {
                    let value: T =
                        bson::deserialize_from_slice(bytes).map_err(CodecError::decode_failure)?;
                    Ok(Box::new(value))
                },
            ),
        );
        self
    }

    /// Number of message types registered with this codec.
    #[must_use]
    pub fn registered_type_count(&self) -> usize {
        self.encoders.len()
    }
}

impl WireCodec for BsonCodec {
    fn content_type(&self) -> &'static str {
        "application/bson"
    }

    fn encode(&self, value: &dyn ErasedWireValue) -> Result<Vec<u8>, CodecError> {
        let id = value.type_id();
        let encoder = self
            .encoders
            .get(&id)
            .ok_or(CodecError::UnknownTypeId(id))?;
        encoder(value)
    }

    fn decode(
        &self,
        type_id: WireTypeId,
        bytes: &[u8],
    ) -> Result<Box<dyn ErasedWireValue>, CodecError> {
        let decoder = self
            .decoders
            .get(&type_id)
            .ok_or(CodecError::UnknownTypeId(type_id))?;
        decoder(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct Sample {
        name: String,
        seq: u32,
        payload: Vec<u8>,
    }

    impl WireValue for Sample {
        fn wire_type_id() -> WireTypeId {
            WireTypeId::new("test.bson.Sample")
        }
    }

    fn fixture() -> Sample {
        Sample {
            name: "eta".into(),
            seq: 8_192,
            payload: vec![0xa5, 0x5a, 0xa5, 0x5a],
        }
    }

    #[test]
    fn round_trip_recovers_value() {
        let mut codec = BsonCodec::new();
        codec.register::<Sample>();
        let v = fixture();
        let bytes = codec.encode(&v).expect("encode");
        let back = codec
            .decode(Sample::wire_type_id(), &bytes)
            .expect("decode");
        let back = back.as_any().downcast_ref::<Sample>().expect("downcast");
        assert_eq!(back, &v);
    }

    #[test]
    fn idempotent_encode_is_byte_equal() {
        let mut codec = BsonCodec::new();
        codec.register::<Sample>();
        let v = fixture();
        let a = codec.encode(&v).expect("encode 1");
        let b = codec.encode(&v).expect("encode 2");
        assert_eq!(a, b);
        let back = codec.decode(Sample::wire_type_id(), &a).expect("decode");
        let c = codec.encode(back.as_ref()).expect("encode 3");
        assert_eq!(a, c);
    }

    #[test]
    fn unregistered_type_returns_unknown_type_id_on_encode() {
        let codec = BsonCodec::new();
        let v = fixture();
        let err = codec.encode(&v).expect_err("expected unknown type");
        assert!(matches!(err, CodecError::UnknownTypeId(id) if id == Sample::wire_type_id()));
    }

    #[test]
    fn unregistered_type_returns_unknown_type_id_on_decode() {
        let codec = BsonCodec::new();
        let err = codec
            .decode(Sample::wire_type_id(), b"")
            .expect_err("expected unknown type");
        assert!(matches!(err, CodecError::UnknownTypeId(id) if id == Sample::wire_type_id()));
    }

    #[test]
    fn malformed_bytes_yield_decode_failure() {
        let mut codec = BsonCodec::new();
        codec.register::<Sample>();
        // Three bytes is below the BSON document header (u32 length).
        let err = codec
            .decode(Sample::wire_type_id(), &[0x01, 0x02, 0x03])
            .expect_err("expected decode failure");
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
