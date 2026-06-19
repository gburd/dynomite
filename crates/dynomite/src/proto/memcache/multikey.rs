//! Memcached multi-key request classification.
//!
//! `memcache_is_multikey_request` always returns false: response
//! coalescing for `get`/`gets` is driven by the fragment vector
//! instead of by a multikey flag.

use crate::msg::MsgType;

/// Constant `false`: no Memcached request is flagged as multikey
/// here; multi-key handling for `get`/`gets` flows
/// through [`super::fragment::memcache_fragment`] instead.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::memcache::memcache_is_multikey_request;
/// assert!(!memcache_is_multikey_request(MsgType::ReqMcGet));
/// assert!(!memcache_is_multikey_request(MsgType::ReqMcSet));
/// ```
#[must_use]
pub fn memcache_is_multikey_request(_ty: MsgType) -> bool {
    false
}
