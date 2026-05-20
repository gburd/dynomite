//! Foundational result and error types used throughout the engine.
//!
//! The C reference uses an `rstatus_t` enum with values `DN_OK`,
//! `DN_ERROR`, `DN_EAGAIN`, and `DN_ENOMEM`. Rust code uses
//! [`Status`] for void-returning fallible calls and richer typed errors
//! at module boundaries.

use std::io;

use thiserror::Error;

/// The shorthand result alias used by most internal APIs.
pub type Status = Result<(), DynError>;

/// Top-level error type for the Dynomite engine.
///
/// This mirrors the small fixed set of error categories used by the C
/// reference, plus an `Io` variant for transport-layer failures and a
/// catch-all `Other` for cases that surface from third-party crates.
#[derive(Debug, Error)]
pub enum DynError {
    /// Generic operational failure with a contextual message.
    #[error("operation failed: {0}")]
    Generic(String),

    /// The operation would block; retry later.
    #[error("operation would block")]
    Again,

    /// Allocation failed.
    #[error("out of memory")]
    OutOfMemory,

    /// Underlying I/O failure.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl DynError {
    /// Construct a generic error from any displayable value.
    pub fn generic<E: std::fmt::Display>(e: E) -> Self {
        Self::Generic(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_carries_message() {
        let err = DynError::generic("boom");
        assert_eq!(err.to_string(), "operation failed: boom");
    }

    #[test]
    fn io_converts_via_from() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionReset, "x");
        let err: DynError = io_err.into();
        assert!(matches!(err, DynError::Io(_)));
    }
}
