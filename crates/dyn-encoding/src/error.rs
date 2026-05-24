//! Codec error type.

use crate::value::WireTypeId;

/// Errors produced by the codec layer.
///
/// All codec implementations report through this single enum so the
/// [`CodecRegistry`](crate::CodecRegistry) can return `&dyn WireCodec`
/// without erasing per-codec error types behind another `Box<dyn _>`.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// The codec has no encoder/decoder registered for the requested
    /// wire type id.
    #[error("unknown wire type id: {0}")]
    UnknownTypeId(WireTypeId),

    /// The value handed to `encode` does not have the type the
    /// registered encoder expects. This indicates a logic bug in the
    /// caller: the [`WireTypeId`] on the value matches a registration,
    /// but the underlying concrete type is something else.
    #[error(
        "type mismatch for wire type id {expected}: value did not downcast to the registered type"
    )]
    TypeMismatch {
        /// The wire type id the codec saw on the value.
        expected: WireTypeId,
    },

    /// Underlying serializer rejected the value.
    #[error("encode failure: {0}")]
    Encode(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Underlying deserializer rejected the bytes.
    #[error("decode failure: {0}")]
    Decode(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl CodecError {
    /// Wrap an arbitrary serializer error as an encode failure.
    pub fn encode_failure<E>(err: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Encode(err.into())
    }

    /// Wrap an arbitrary deserializer error as a decode failure.
    pub fn decode_failure<E>(err: E) -> Self
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Self::Decode(err.into())
    }
}
