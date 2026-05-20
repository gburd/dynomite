//! Redis multi-key request classification.

use crate::msg::MsgType;

/// True when the request type is one of the multi-key commands the
/// fragmenter splits across shards (`MGET`, `DEL`, `EXISTS`,
/// `MSET`).
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::redis::redis_is_multikey_request;
///
/// assert!(redis_is_multikey_request(MsgType::ReqRedisMget));
/// assert!(redis_is_multikey_request(MsgType::ReqRedisMset));
/// assert!(!redis_is_multikey_request(MsgType::ReqRedisGet));
/// ```
#[must_use]
pub fn redis_is_multikey_request(ty: MsgType) -> bool {
    matches!(
        ty,
        MsgType::ReqRedisMget
            | MsgType::ReqRedisDel
            | MsgType::ReqRedisExists
            | MsgType::ReqRedisMset
    )
}
