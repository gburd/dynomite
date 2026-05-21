//! Error types for the embedding API.
//!
//! Every public surface in [`crate::embed`] flows errors through
//! [`EmbedError`]. The variants are intentionally coarse: the
//! library code beneath the embed facade owns the precise typed
//! error vocabulary; the embed surface is a thin user-facing
//! shim.
//!
//! # Examples
//!
//! ```
//! use dynomite::embed::EmbedError;
//! let err = EmbedError::Build("listen address required".into());
//! assert!(err.to_string().contains("listen"));
//! ```

use std::io;

use thiserror::Error;

use crate::conf::ConfError;

/// Top-level error for every embedding-API call.
///
/// The variants partition the error space by the surface that
/// produced the error: configuration validation
/// ([`EmbedError::Build`]), runtime control
/// ([`EmbedError::Shutdown`], [`EmbedError::Reload`]), traffic
/// path ([`EmbedError::Inject`]), and the underlying I/O
/// ([`EmbedError::Io`]).
///
/// # Examples
///
/// ```
/// use dynomite::embed::EmbedError;
/// let e: EmbedError = std::io::Error::other("boom").into();
/// assert!(matches!(e, EmbedError::Io(_)));
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EmbedError {
    /// Configuration validation failed.
    #[error("build error: {0}")]
    Build(String),

    /// Underlying YAML / pool validation rejected the config.
    #[error("conf error: {0}")]
    Conf(#[from] ConfError),

    /// Graceful shutdown failed.
    #[error("shutdown error: {0}")]
    Shutdown(String),

    /// Reload validation or apply failed.
    #[error("reload error: {0}")]
    Reload(String),

    /// `inject_request` could not produce a response.
    #[error("inject error: {0}")]
    Inject(String),

    /// I/O failure during a runtime operation (bind, accept, ...).
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// The runtime task was cancelled or the channel was closed.
    #[error("server stopped")]
    Stopped,
}
