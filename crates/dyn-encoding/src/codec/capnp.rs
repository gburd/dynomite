//! `application/capnproto` codec backed by the `capnp` runtime.
//!
//! Cap'n Proto is, like FlatBuffers, schema-first: messages are
//! framed as a sequence of segments and the typed accessors are
//! produced by `capnpc` from a `.capnp` schema. The codec deliberately
//! does not depend on `capnpc`; instead, each registered type
//! provides its own conversion through the [`CapnpWire`] trait. The
//! `dyniak` crate is expected to host the schema set and the
//! generated readers/builders; this codec dispatches by
//! [`WireTypeId`] and routes through the trait.

use std::collections::HashMap;

use crate::error::CodecError;
use crate::value::{ErasedWireValue, WireCodec, WireTypeId, WireValue};

type EncodeFn =
    Box<dyn Fn(&dyn ErasedWireValue) -> Result<Vec<u8>, CodecError> + Send + Sync + 'static>;
type DecodeFn =
    Box<dyn Fn(&[u8]) -> Result<Box<dyn ErasedWireValue>, CodecError> + Send + Sync + 'static>;

/// Per-message-type Cap'n Proto encode/decode contract.
///
/// Implementors typically build a `capnp::message::Builder` inside
/// `capnp_encode`, populate it through the type's generated builder
/// API, and write it out via `capnp::serialize::write_message`.
/// `capnp_decode` parses the inverse path through
/// `capnp::serialize::read_message`.
pub trait CapnpWire: WireValue + Sized {
    /// Serialise `self` into a fully-framed Cap'n Proto message.
    fn capnp_encode(&self) -> Result<Vec<u8>, CodecError>;

    /// Parse a fully-framed Cap'n Proto message back into `Self`.
    fn capnp_decode(bytes: &[u8]) -> Result<Self, CodecError>;
}

/// Codec that serialises [`WireValue`] types as Cap'n Proto messages
/// via per-type [`CapnpWire`] implementations.
#[derive(Default)]
pub struct CapnpCodec {
    encoders: HashMap<WireTypeId, EncodeFn>,
    decoders: HashMap<WireTypeId, DecodeFn>,
}

impl CapnpCodec {
    /// Construct an empty Cap'n Proto codec with no registered types.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a [`WireValue`] type with the codec.
    pub fn register<T>(&mut self) -> &mut Self
    where
        T: CapnpWire + 'static,
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
                    concrete.capnp_encode()
                },
            ),
        );
        self.decoders.insert(
            id,
            Box::new(
                move |bytes: &[u8]| -> Result<Box<dyn ErasedWireValue>, CodecError> {
                    let value = T::capnp_decode(bytes)?;
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

impl WireCodec for CapnpCodec {
    fn content_type(&self) -> &'static str {
        "application/capnproto"
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
    use ::capnp::message::{Builder, HeapAllocator, ReaderOptions};
    use ::capnp::serialize;

    /// Hand-rolled Cap'n Proto fixture. The schema, written out, is:
    ///
    /// ```text
    /// # The fixture lives as a heterogeneous list of three Data
    /// # entries. We pack the typed scalar `seq` as a 4-byte
    /// # little-endian Data blob so the entire payload travels
    /// # through capnp's `data_list` accessor without any generated
    /// # `capnpc` code.
    /// ```
    ///
    /// This deliberately uses only the runtime-side API
    /// (`Builder::initn_root`, `data_list::Builder/Reader`,
    /// `serialize::{read_message, write_message}`) and never touches
    /// `capnpc`. A real consumer would replace this with a typed
    /// `data.capnp` schema; the codec dispatch remains unchanged.
    #[derive(Debug, Eq, PartialEq)]
    struct Sample {
        name: String,
        seq: u32,
        payload: Vec<u8>,
    }

    impl WireValue for Sample {
        fn wire_type_id() -> WireTypeId {
            WireTypeId::new("test.capnp.Sample")
        }
    }

    impl CapnpWire for Sample {
        fn capnp_encode(&self) -> Result<Vec<u8>, CodecError> {
            let mut msg = Builder::new(HeapAllocator::new());
            {
                let mut root: ::capnp::data_list::Builder<'_> = msg.initn_root(3);
                root.set(0, self.name.as_bytes());
                let seq_le = self.seq.to_le_bytes();
                root.set(1, &seq_le);
                root.set(2, &self.payload);
            }
            let mut out = Vec::new();
            serialize::write_message(&mut out, &msg).map_err(CodecError::encode_failure)?;
            Ok(out)
        }

        fn capnp_decode(bytes: &[u8]) -> Result<Self, CodecError> {
            let reader = serialize::read_message(bytes, ReaderOptions::new())
                .map_err(CodecError::decode_failure)?;
            let root: ::capnp::data_list::Reader<'_> =
                reader.get_root().map_err(CodecError::decode_failure)?;
            if root.len() != 3 {
                return Err(CodecError::decode_failure(format!(
                    "capnp: expected 3-entry data_list, got {}",
                    root.len()
                )));
            }
            let name_bytes = root.get(0).map_err(CodecError::decode_failure)?;
            let name = std::str::from_utf8(name_bytes)
                .map_err(CodecError::decode_failure)?
                .to_owned();
            let seq_bytes = root.get(1).map_err(CodecError::decode_failure)?;
            if seq_bytes.len() != 4 {
                return Err(CodecError::decode_failure(format!(
                    "capnp: expected 4-byte seq blob, got {}",
                    seq_bytes.len()
                )));
            }
            let seq = u32::from_le_bytes([seq_bytes[0], seq_bytes[1], seq_bytes[2], seq_bytes[3]]);
            let payload = root.get(2).map_err(CodecError::decode_failure)?.to_vec();
            Ok(Sample { name, seq, payload })
        }
    }

    fn fixture() -> Sample {
        Sample {
            name: "epsilon".into(),
            seq: 65_537,
            payload: vec![0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff],
        }
    }

    #[test]
    fn round_trip_recovers_value() {
        let mut codec = CapnpCodec::new();
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
        let mut codec = CapnpCodec::new();
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
        let codec = CapnpCodec::new();
        let v = fixture();
        let err = codec.encode(&v).expect_err("expected unknown type");
        assert!(matches!(err, CodecError::UnknownTypeId(id) if id == Sample::wire_type_id()));
    }

    #[test]
    fn unregistered_type_returns_unknown_type_id_on_decode() {
        let codec = CapnpCodec::new();
        let err = codec
            .decode(Sample::wire_type_id(), b"")
            .expect_err("expected unknown type");
        assert!(matches!(err, CodecError::UnknownTypeId(id) if id == Sample::wire_type_id()));
    }

    #[test]
    fn malformed_bytes_yield_decode_failure() {
        let mut codec = CapnpCodec::new();
        codec.register::<Sample>();
        // Two bytes is far below the minimum capnp segment-table
        // header, so `read_message` will reject it.
        let err = codec
            .decode(Sample::wire_type_id(), &[0xff, 0xff])
            .expect_err("expected decode failure");
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
