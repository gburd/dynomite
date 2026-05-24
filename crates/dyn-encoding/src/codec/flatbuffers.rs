//! `application/octet-stream;schema=flatbuffers` codec backed by the
//! `flatbuffers` runtime.
//!
//! FlatBuffers is a schema-first format: the bytes on the wire are a
//! laid-out table whose vtable encodes every field's offset. There is
//! no general-purpose `Serialize` shape that the runtime can drive,
//! so each registered type carries its own pair of conversion
//! functions, exposed through the [`FlatbuffersWire`] trait. The
//! codec is otherwise a sibling of [`crate::JsonCodec`] /
//! [`crate::CborCodec`] / [`crate::ProtobufCodec`]: per-type
//! registration, dispatch by [`WireTypeId`], and a single
//! [`crate::CodecError`] for both encode and decode failures.
//!
//! Avoiding `flatc` keeps the build hermetic. The downside is that
//! callers must hand-roll their `flatbuffers_encode` /
//! `flatbuffers_decode` bodies (or generate them out-of-band and
//! plug them into a [`FlatbuffersWire`] impl). The
//! `dyn-riak` crate is expected to host the schema set and the
//! generated code; this codec simply dispatches.

use std::collections::HashMap;

use crate::error::CodecError;
use crate::value::{ErasedWireValue, WireCodec, WireTypeId, WireValue};

type EncodeFn =
    Box<dyn Fn(&dyn ErasedWireValue) -> Result<Vec<u8>, CodecError> + Send + Sync + 'static>;
type DecodeFn =
    Box<dyn Fn(&[u8]) -> Result<Box<dyn ErasedWireValue>, CodecError> + Send + Sync + 'static>;

/// Per-message-type FlatBuffers encode/decode contract.
///
/// The trait deliberately does not bound `T` on any
/// `flatbuffers::Follow` / `flatbuffers::Push` shape: those traits
/// model the on-wire view of a flatbuffer table, not the owned Rust
/// representation that travels through the codec API. Implementors
/// build a [`flatbuffers::FlatBufferBuilder`] inside
/// `flatbuffers_encode` and finish it; `flatbuffers_decode` reads the
/// fields back into an owned `Self` (the codec returns
/// `Box<dyn ErasedWireValue>`, which is necessarily owned).
pub trait FlatbuffersWire: WireValue + Sized {
    /// Serialise `self` into a finished, root-prefixed flatbuffer.
    fn flatbuffers_encode(&self) -> Result<Vec<u8>, CodecError>;

    /// Parse a finished, root-prefixed flatbuffer back into `Self`.
    fn flatbuffers_decode(bytes: &[u8]) -> Result<Self, CodecError>;
}

/// Codec that serialises [`WireValue`] types as FlatBuffers via
/// per-type [`FlatbuffersWire`] implementations.
#[derive(Default)]
pub struct FlatbuffersCodec {
    encoders: HashMap<WireTypeId, EncodeFn>,
    decoders: HashMap<WireTypeId, DecodeFn>,
}

impl FlatbuffersCodec {
    /// Construct an empty FlatBuffers codec with no registered types.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a [`WireValue`] type with the codec.
    pub fn register<T>(&mut self) -> &mut Self
    where
        T: FlatbuffersWire + 'static,
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
                    concrete.flatbuffers_encode()
                },
            ),
        );
        self.decoders.insert(
            id,
            Box::new(
                move |bytes: &[u8]| -> Result<Box<dyn ErasedWireValue>, CodecError> {
                    let value = T::flatbuffers_decode(bytes)?;
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

impl WireCodec for FlatbuffersCodec {
    fn content_type(&self) -> &'static str {
        "application/octet-stream;schema=flatbuffers"
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
    use ::flatbuffers::{FlatBufferBuilder, Vector, WIPOffset};

    /// Hand-rolled flatbuffer fixture. The schema, written out, is:
    ///
    /// ```text
    /// table Sample {
    ///   name:    string;   // field 0, vtable slot 4
    ///   seq:     uint32;   // field 1, vtable slot 6
    ///   payload: [ubyte];  // field 2, vtable slot 8
    /// }
    /// root_type Sample;
    /// ```
    ///
    /// Encoder uses [`FlatBufferBuilder`] from the runtime. Decoder
    /// is a hand-written safe parser over the well-defined
    /// FlatBuffers wire format -- the runtime's typed reader path
    /// requires `unsafe { Table::new }` because FlatBuffers does not
    /// validate untrusted bytes by default; we sidestep that here so
    /// the codec module stays under the crate-wide
    /// `forbid(unsafe_code)`.
    #[derive(Debug, Eq, PartialEq)]
    struct Sample {
        name: String,
        seq: u32,
        payload: Vec<u8>,
    }

    impl WireValue for Sample {
        fn wire_type_id() -> WireTypeId {
            WireTypeId::new("test.flatbuffers.Sample")
        }
    }

    const VT_NAME: u16 = 4;
    const VT_SEQ: u16 = 6;
    const VT_PAYLOAD: u16 = 8;

    fn read_u16(buf: &[u8], at: usize) -> Result<u16, CodecError> {
        buf.get(at..at + 2)
            .ok_or_else(|| CodecError::decode_failure("flatbuffers: u16 read out of bounds"))
            .map(|s| u16::from_le_bytes([s[0], s[1]]))
    }

    fn read_u32(buf: &[u8], at: usize) -> Result<u32, CodecError> {
        buf.get(at..at + 4)
            .ok_or_else(|| CodecError::decode_failure("flatbuffers: u32 read out of bounds"))
            .map(|s| u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    impl FlatbuffersWire for Sample {
        fn flatbuffers_encode(&self) -> Result<Vec<u8>, CodecError> {
            let mut b = FlatBufferBuilder::new();
            let name_off = b.create_string(&self.name);
            let payload_off = b.create_vector(&self.payload);
            let table = b.start_table();
            b.push_slot::<WIPOffset<&str>>(VT_NAME, name_off, WIPOffset::new(0));
            b.push_slot::<u32>(VT_SEQ, self.seq, 0);
            b.push_slot::<WIPOffset<Vector<'_, u8>>>(VT_PAYLOAD, payload_off, WIPOffset::new(0));
            let root_off = b.end_table(table);
            b.finish_minimal(root_off);
            Ok(b.finished_data().to_vec())
        }

        fn flatbuffers_decode(bytes: &[u8]) -> Result<Self, CodecError> {
            let root_off = read_u32(bytes, 0)? as usize;
            let vt_off_signed = i32::from_le_bytes([
                *bytes.get(root_off).ok_or_else(|| {
                    CodecError::decode_failure("flatbuffers: vtable offset out of bounds")
                })?,
                *bytes.get(root_off + 1).ok_or_else(|| {
                    CodecError::decode_failure("flatbuffers: vtable offset out of bounds")
                })?,
                *bytes.get(root_off + 2).ok_or_else(|| {
                    CodecError::decode_failure("flatbuffers: vtable offset out of bounds")
                })?,
                *bytes.get(root_off + 3).ok_or_else(|| {
                    CodecError::decode_failure("flatbuffers: vtable offset out of bounds")
                })?,
            ]);
            // The vtable lives at `root_off - vt_off_signed`. Promote
            // both operands to i64 to dodge the sign-vs-width
            // pitfalls of casting `usize` to `isize`.
            let root_i64 = i64::try_from(root_off).map_err(|_| {
                CodecError::decode_failure("flatbuffers: root offset overflows i64")
            })?;
            let vtable_pos_i64 = root_i64 - i64::from(vt_off_signed);
            let vtable_pos = usize::try_from(vtable_pos_i64).map_err(|_| {
                CodecError::decode_failure("flatbuffers: vtable position underflow")
            })?;
            let vt_size = read_u16(bytes, vtable_pos)? as usize;

            let read_slot = |slot: u16| -> Result<Option<usize>, CodecError> {
                let slot = slot as usize;
                if slot + 2 > vt_size {
                    return Ok(None);
                }
                let raw = read_u16(bytes, vtable_pos + slot)?;
                if raw == 0 {
                    Ok(None)
                } else {
                    Ok(Some(root_off + raw as usize))
                }
            };

            let name = match read_slot(VT_NAME)? {
                Some(field_pos) => {
                    let str_pos = field_pos + read_u32(bytes, field_pos)? as usize;
                    let len = read_u32(bytes, str_pos)? as usize;
                    let body = bytes.get(str_pos + 4..str_pos + 4 + len).ok_or_else(|| {
                        CodecError::decode_failure("flatbuffers: string body out of bounds")
                    })?;
                    std::str::from_utf8(body)
                        .map_err(CodecError::decode_failure)?
                        .to_owned()
                }
                None => String::new(),
            };

            let seq = match read_slot(VT_SEQ)? {
                Some(field_pos) => read_u32(bytes, field_pos)?,
                None => 0,
            };

            let payload = match read_slot(VT_PAYLOAD)? {
                Some(field_pos) => {
                    let vec_pos = field_pos + read_u32(bytes, field_pos)? as usize;
                    let len = read_u32(bytes, vec_pos)? as usize;
                    bytes
                        .get(vec_pos + 4..vec_pos + 4 + len)
                        .ok_or_else(|| {
                            CodecError::decode_failure("flatbuffers: vector body out of bounds")
                        })?
                        .to_vec()
                }
                None => Vec::new(),
            };

            Ok(Sample { name, seq, payload })
        }
    }

    fn fixture() -> Sample {
        Sample {
            name: "delta".into(),
            seq: 1024,
            payload: vec![0x10, 0x20, 0x30, 0x40, 0x50],
        }
    }

    #[test]
    fn round_trip_recovers_value() {
        let mut codec = FlatbuffersCodec::new();
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
        let mut codec = FlatbuffersCodec::new();
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
        let codec = FlatbuffersCodec::new();
        let v = fixture();
        let err = codec.encode(&v).expect_err("expected unknown type");
        assert!(matches!(err, CodecError::UnknownTypeId(id) if id == Sample::wire_type_id()));
    }

    #[test]
    fn unregistered_type_returns_unknown_type_id_on_decode() {
        let codec = FlatbuffersCodec::new();
        let err = codec
            .decode(Sample::wire_type_id(), b"")
            .expect_err("expected unknown type");
        assert!(matches!(err, CodecError::UnknownTypeId(id) if id == Sample::wire_type_id()));
    }

    #[test]
    fn malformed_bytes_yield_decode_failure() {
        let mut codec = FlatbuffersCodec::new();
        codec.register::<Sample>();
        // Two bytes is not enough to even hold the root uoffset.
        let err = codec
            .decode(Sample::wire_type_id(), &[0x01, 0x02])
            .expect_err("expected decode failure");
        assert!(matches!(err, CodecError::Decode(_)));
    }
}
