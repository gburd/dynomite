//! Memcached command classification helpers.
//!
//! Each predicate mirrors a categorical check in the reference
//! engine's command dispatch (storage / retrieval / arithmetic /
//! delete / touch / cas) and is reused by the parser, fragmenter,
//! and verifier.

use crate::msg::MsgType;

/// True when `ty` denotes a Memcached storage command (`set`,
/// `add`, `replace`, `append`, `prepend`, `cas`).
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::memcache::memcache_storage;
/// assert!(memcache_storage(MsgType::ReqMcSet));
/// assert!(memcache_storage(MsgType::ReqMcCas));
/// assert!(!memcache_storage(MsgType::ReqMcGet));
/// ```
#[must_use]
pub fn memcache_storage(ty: MsgType) -> bool {
    matches!(
        ty,
        MsgType::ReqMcSet
            | MsgType::ReqMcCas
            | MsgType::ReqMcAdd
            | MsgType::ReqMcReplace
            | MsgType::ReqMcAppend
            | MsgType::ReqMcPrepend
    )
}

/// True when `ty` is the Memcached `cas` command.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::memcache::memcache_cas;
/// assert!(memcache_cas(MsgType::ReqMcCas));
/// assert!(!memcache_cas(MsgType::ReqMcSet));
/// ```
#[must_use]
pub fn memcache_cas(ty: MsgType) -> bool {
    matches!(ty, MsgType::ReqMcCas)
}

/// True when `ty` denotes a Memcached retrieval command (`get`,
/// `gets`).
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::memcache::memcache_retrieval;
/// assert!(memcache_retrieval(MsgType::ReqMcGet));
/// assert!(memcache_retrieval(MsgType::ReqMcGets));
/// assert!(!memcache_retrieval(MsgType::ReqMcSet));
/// ```
#[must_use]
pub fn memcache_retrieval(ty: MsgType) -> bool {
    matches!(ty, MsgType::ReqMcGet | MsgType::ReqMcGets)
}

/// True when `ty` denotes a Memcached arithmetic command (`incr`,
/// `decr`).
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::memcache::memcache_arithmetic;
/// assert!(memcache_arithmetic(MsgType::ReqMcIncr));
/// assert!(memcache_arithmetic(MsgType::ReqMcDecr));
/// ```
#[must_use]
pub fn memcache_arithmetic(ty: MsgType) -> bool {
    matches!(ty, MsgType::ReqMcIncr | MsgType::ReqMcDecr)
}

/// True when `ty` is the Memcached `delete` command.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::memcache::memcache_delete;
/// assert!(memcache_delete(MsgType::ReqMcDelete));
/// ```
#[must_use]
pub fn memcache_delete(ty: MsgType) -> bool {
    matches!(ty, MsgType::ReqMcDelete)
}

/// True when `ty` is the Memcached `touch` command.
///
/// # Examples
///
/// ```
/// use dynomite::msg::MsgType;
/// use dynomite::proto::memcache::memcache_touch;
/// assert!(memcache_touch(MsgType::ReqMcTouch));
/// ```
#[must_use]
pub fn memcache_touch(ty: MsgType) -> bool {
    matches!(ty, MsgType::ReqMcTouch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_partition_is_disjoint() {
        for ty in [
            MsgType::ReqMcGet,
            MsgType::ReqMcGets,
            MsgType::ReqMcSet,
            MsgType::ReqMcAdd,
            MsgType::ReqMcReplace,
            MsgType::ReqMcAppend,
            MsgType::ReqMcPrepend,
            MsgType::ReqMcCas,
            MsgType::ReqMcDelete,
            MsgType::ReqMcIncr,
            MsgType::ReqMcDecr,
            MsgType::ReqMcTouch,
            MsgType::ReqMcQuit,
        ] {
            let storage = memcache_storage(ty);
            let retrieval = memcache_retrieval(ty);
            let arithmetic = memcache_arithmetic(ty);
            let delete = memcache_delete(ty);
            let touch = memcache_touch(ty);
            // Every command is at most in one of these categories.
            let count = [storage, retrieval, arithmetic, delete, touch]
                .iter()
                .filter(|x| **x)
                .count();
            assert!(count <= 1, "{ty:?} matched {count} categories");
        }
    }
}
