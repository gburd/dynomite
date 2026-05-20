//! Foundational result and error types used throughout the engine.
//!
//! [`Status`] is the void-returning fallible alias used by most internal
//! APIs. [`DynError`] is the small fixed set of error categories the
//! engine reports and absorbs `std::io::Error` for transport layer
//! failures. The numeric scalars `MsgId`, `Msec`, `Usec`, and `Sec`
//! are typed aliases for the corresponding wire-format integers.

use std::io;

use thiserror::Error;

/// Monotonically increasing message identifier.
pub type MsgId = u64;

/// Milliseconds since the UNIX epoch.
pub type Msec = u64;

/// Microseconds since the UNIX epoch.
pub type Usec = u64;

/// Seconds since the UNIX epoch.
pub type Sec = u64;

/// Result alias used by most internal APIs that do not return data.
///
/// # Examples
///
/// ```
/// use dynomite::core::types::{Status, DynError};
///
/// fn check(ok: bool) -> Status {
///     if ok { Ok(()) } else { Err(DynError::generic("not ok")) }
/// }
/// assert!(check(true).is_ok());
/// assert!(check(false).is_err());
/// ```
pub type Status = Result<(), DynError>;

/// Top-level error type for the Dynomite engine.
///
/// The variants enumerate the small fixed error set the engine
/// reports plus an [`Io`](DynError::Io) variant for transport failures
/// and an unstructured [`Generic`](DynError::Generic) catch-all.
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

    /// The operation is not implemented for the current configuration.
    #[error("not implemented")]
    NotImplemented,

    /// Underlying I/O failure.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl DynError {
    /// Construct a generic error from any displayable value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::core::types::DynError;
    /// let err = DynError::generic("boom");
    /// assert_eq!(err.to_string(), "operation failed: boom");
    /// ```
    pub fn generic<E: std::fmt::Display>(e: E) -> Self {
        Self::Generic(e.to_string())
    }
}

/// Connection-level secure-traffic policy.
///
/// The variants describe which inter-node traffic should be encrypted
/// with the peer-shared AES key established during the DNODE
/// handshake.
///
/// # Examples
///
/// ```
/// use dynomite::core::types::SecureServerOption;
///
/// let opt = SecureServerOption::Rack;
/// assert_eq!(opt as u8, 1);
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum SecureServerOption {
    /// No inter-node encryption.
    None = 0,
    /// Encrypt traffic between peers in the same rack.
    Rack = 1,
    /// Encrypt traffic between peers in the same datacenter.
    Datacenter = 2,
    /// Encrypt all inter-node traffic.
    All = 3,
}

impl SecureServerOption {
    /// Parse a `secure_server_option` config string. Accepts the same
    /// keywords as the YAML config and is case-insensitive.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::core::types::SecureServerOption;
    /// assert_eq!(
    ///     SecureServerOption::parse("rack"),
    ///     Some(SecureServerOption::Rack),
    /// );
    /// assert_eq!(
    ///     SecureServerOption::parse("DATACENTER"),
    ///     Some(SecureServerOption::Datacenter),
    /// );
    /// assert_eq!(SecureServerOption::parse("nope"), None);
    /// ```
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Some(Self::None),
            "rack" => Some(Self::Rack),
            "datacenter" | "dc" => Some(Self::Datacenter),
            "all" => Some(Self::All),
            _ => None,
        }
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

    #[test]
    fn secure_option_parses_case_insensitively() {
        assert_eq!(
            SecureServerOption::parse("None"),
            Some(SecureServerOption::None)
        );
        assert_eq!(
            SecureServerOption::parse("rack"),
            Some(SecureServerOption::Rack)
        );
        assert_eq!(
            SecureServerOption::parse("DC"),
            Some(SecureServerOption::Datacenter)
        );
        assert_eq!(
            SecureServerOption::parse("ALL"),
            Some(SecureServerOption::All)
        );
        assert_eq!(SecureServerOption::parse("bogus"), None);
    }

    #[test]
    fn typed_aliases_have_expected_widths() {
        let id: MsgId = u64::MAX;
        let m: Msec = id;
        let u: Usec = m;
        let s: Sec = 0;
        assert_eq!(id, u);
        assert_eq!(s, 0);
    }
}
