//! Top-level error type for the `dyn-admin` CLI.

use dyn_riak::error::RiakError;

/// Errors surfaced by `dyn-admin` subcommands.
///
/// Each variant carries enough context to be printed verbatim by the
/// binary's `main` (`anyhow::Error`-shaped chain) and a downstream
/// integration test can match the variant directly.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AdminError {
    /// TCP connect failed.
    #[error("connect to {addr}: {source}")]
    Connect {
        /// `host:port` the client tried to reach.
        addr: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Generic underlying I/O error (used after a successful connect).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// PBC-layer wire failure (framing, decode, encode).
    #[error("riak pbc: {0}")]
    Riak(#[from] RiakError),

    /// `prost` failed to decode a message.
    #[error("protobuf decode: {0}")]
    Decode(#[from] prost::DecodeError),

    /// HTTP-layer failure: malformed response or non-2xx status.
    #[error("http: {0}")]
    Http(String),

    /// JSON parse failure when decoding a stats body.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    /// Operation exceeded the configured timeout.
    #[error("timeout: {op}")]
    Timeout {
        /// Short label of the operation that timed out.
        op: String,
    },

    /// Wire response did not match the protocol contract (for example
    /// a get-server-info reply with no node name).
    #[error("protocol: {0}")]
    Protocol(String),

    /// Server returned an `RpbErrorResp` envelope.
    #[error("server error: {errmsg} (errcode={errcode})")]
    Server {
        /// Riak-specific error code; zero is reserved for "unknown".
        errcode: u32,
        /// Human-readable message from the peer.
        errmsg: String,
    },
}
