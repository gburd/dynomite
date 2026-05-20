//! Memcached request verification helper.
//!
//! In the reference engine, `memcache_verify_request` always returns
//! `DN_OK`: there are no Memcached-specific structural checks beyond
//! what the parser already enforces. The Rust port keeps the same
//! shape so callers can treat the Redis and Memcached helpers
//! uniformly.

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
