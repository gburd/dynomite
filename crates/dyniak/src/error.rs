//! Top-level error type for the `dyniak` crate.

use dynomite::embed::DatastoreError;

/// Errors produced by the `dyniak` protocol layer.
///
/// The variants cover wire-level failures (framing, decode, encode),
/// protocol-level failures (unknown message code), and downstream
/// failures from the [`dynomite::embed::Datastore`] handed to
/// [`crate::server::serve_pbc`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RiakError {
    /// Underlying socket I/O failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The peer announced a frame longer than the configured maximum.
    /// Used as a hard cap so a malicious peer cannot pin arbitrary
    /// memory by claiming a multi-gigabyte frame.
    #[error("frame too large: announced {announced} bytes, maximum {max} bytes")]
    FrameTooLarge {
        /// Length the peer claimed (excluding the 4-byte length prefix).
        announced: u32,
        /// Configured maximum frame length.
        max: u32,
    },

    /// The peer announced a frame of length zero, which is illegal:
    /// every PBC frame must include at least the 1-byte message code.
    #[error("empty frame: zero-length PBC frames are not allowed")]
    EmptyFrame,

    /// Connection was closed by the peer mid-frame.
    #[error("connection closed mid-frame ({read} of {expected} bytes read)")]
    UnexpectedEof {
        /// Bytes already drained from the socket.
        read: usize,
        /// Bytes the framer was waiting on.
        expected: usize,
    },

    /// The peer sent a message code outside the supported set.
    #[error("unknown PBC message code: {0}")]
    UnknownMessageCode(u8),

    /// `prost` failed to decode the message body.
    #[error("protobuf decode: {0}")]
    Decode(#[from] prost::DecodeError),

    /// `prost` failed to encode the response body.
    #[error("protobuf encode: {0}")]
    Encode(#[from] prost::EncodeError),

    /// The downstream datastore returned an error.
    #[error("datastore: {0}")]
    Datastore(#[from] DatastoreError),
}
