//! Wire value, type id, and codec traits.

use std::any::Any;
use std::fmt;

use crate::error::CodecError;

/// Stable identifier for a structured wire-message type.
///
/// `WireTypeId` is a thin newtype around a `&'static str` so that the
/// registry-side hash-map is cheap to look up and the value is
/// printable in diagnostics. Type ids should be namespaced
/// (`"riak.GetReq"`, `"riak.PutResp"`, ...) to avoid collisions
/// between protocol layers.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WireTypeId(&'static str);

impl WireTypeId {
    /// Construct a type id from a static string.
    #[must_use]
    pub const fn new(s: &'static str) -> Self {
        Self(s)
    }

    /// Borrow the underlying string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for WireTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// A value that can travel on the wire under some [`WireCodec`].
///
/// Implementors give every concrete message type a stable
/// [`WireTypeId`]. The trait carries no encode/decode methods of its
/// own: per-codec bounds (such as `serde::Serialize` for JSON/CBOR or
/// `prost::Message` for protobuf) are enforced at codec
/// registration time.
pub trait WireValue: fmt::Debug + Send + Sync + 'static {
    /// Stable identifier for the message type. The id is used by the
    /// codec registry to dispatch to the correct decoder when bytes
    /// arrive on the wire.
    fn wire_type_id() -> WireTypeId
    where
        Self: Sized;
}

/// Object-safe view over [`WireValue`].
///
/// Codec implementations work in terms of `&dyn ErasedWireValue` so
/// they can be invoked through `&dyn WireCodec` without becoming
/// generic. The blanket impl below makes the conversion implicit at
/// call sites: any `&T` where `T: WireValue` coerces to
/// `&dyn ErasedWireValue`.
pub trait ErasedWireValue: fmt::Debug + Send + Sync + 'static {
    /// The wire type id of the underlying concrete value.
    fn type_id(&self) -> WireTypeId;

    /// Provide an `Any` view so codec impls can downcast to the
    /// concrete type they registered.
    fn as_any(&self) -> &dyn Any;
}

impl<T> ErasedWireValue for T
where
    T: WireValue,
{
    fn type_id(&self) -> WireTypeId {
        <T as WireValue>::wire_type_id()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A wire-format codec that turns a structured request or response
/// value into bytes and back.
///
/// Schema-first codecs (protobuf, FlatBuffers, Cap'n Proto, Bebop)
/// and schema-less codecs (JSON / CBOR / BSON) are both addressable
/// through this trait.
/// Per-type registration happens on the concrete codec; the trait
/// itself is intentionally narrow so it can be stored as
/// `Box<dyn WireCodec>` inside the [`crate::CodecRegistry`].
pub trait WireCodec: Send + Sync + 'static {
    /// Content-type header value that identifies this codec on the
    /// wire (for example `"application/x-protobuf"`,
    /// `"application/json"`, `"application/cbor"`).
    fn content_type(&self) -> &'static str;

    /// Encode a [`WireValue`] to bytes.
    fn encode(&self, value: &dyn ErasedWireValue) -> Result<Vec<u8>, CodecError>;

    /// Decode `bytes` into a value of the requested type.
    ///
    /// The caller is expected to know what shape it asked for and
    /// pass the matching [`WireTypeId`].
    fn decode(
        &self,
        type_id: WireTypeId,
        bytes: &[u8],
    ) -> Result<Box<dyn ErasedWireValue>, CodecError>;
}
