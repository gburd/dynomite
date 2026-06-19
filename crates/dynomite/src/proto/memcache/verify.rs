//! Memcached request verification helper.
//!
//! Request verification always succeeds: there are no
//! Memcached-specific structural checks beyond what the parser
//! already enforces. The helper keeps the same shape as the RESP
//! verifier so callers can treat the two uniformly.

use crate::msg::Msg;

/// Always succeeds.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::memcache::memcache_verify_request;
///
/// let m = Msg::new(0, MsgType::ReqMcSet, true);
/// assert!(memcache_verify_request(&m).is_ok());
/// ```
pub fn memcache_verify_request(_r: &Msg) -> Result<(), VerifyError> {
    Ok(())
}

/// Errors verification can produce. The Memcached path never
/// rejects a parsed request, but the type is exported for parity
/// with the Redis surface.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum VerifyError {
    /// Reserved variant. Memcached has no live failure modes.
    #[error("memcache verify: reserved")]
    Reserved,
}
