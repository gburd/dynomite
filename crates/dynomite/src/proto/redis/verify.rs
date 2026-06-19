//! Redis request structural verifier.
//!
//! Most commands have nothing to verify here; the only live check
//! is that an `EVAL` request keeps every key it touches on the
//! same shard. The verifier consults a caller-supplied dispatcher
//! so the cluster-state coupling stays out of the protocol layer.

use crate::msg::{Msg, MsgType};

use super::fragment::FragmentDispatcher;

/// Reserved metadata key prefixes the verifier ignores. These match
/// the `_add-set` / `_rem-set` constants used by the read-repair
/// engine.
pub const ADD_SET_STR: &[u8] = b"._add-set";
/// See [`ADD_SET_STR`].
pub const REM_SET_STR: &[u8] = b"._rem-set";

/// Run the parser-level structural checks against `r`.
///
/// Returns `Ok(())` when no rule was violated. Returns
/// [`VerifyError::ScriptSpansNodes`] when an `EVAL` request touches
/// keys belonging to more than one shard.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{KeyPos, Msg, MsgType};
/// use dynomite::proto::redis::{redis_verify_request, FragmentDispatcher};
///
/// struct AllOnOne;
/// impl FragmentDispatcher for AllOnOne {
///     fn shard_for(&self, _key: &[u8]) -> u32 { 0 }
///     fn shard_count(&self) -> u32 { 1 }
/// }
///
/// let mut r = Msg::new(0, MsgType::ReqRedisEval, true);
/// r.push_key(KeyPos::without_tag(b"a".to_vec()));
/// r.push_key(KeyPos::without_tag(b"b".to_vec()));
/// assert!(redis_verify_request(&r, &AllOnOne).is_ok());
/// ```
pub fn redis_verify_request<D: FragmentDispatcher + ?Sized>(
    r: &Msg,
    dispatcher: &D,
) -> Result<(), VerifyError> {
    if r.ty() != MsgType::ReqRedisEval {
        return Ok(());
    }
    if r.keys().len() <= 1 {
        return Ok(());
    }
    let mut prev_idx: Option<u32> = None;
    for k in r.keys() {
        let key = k.key();
        if key.starts_with(ADD_SET_STR) || key.starts_with(REM_SET_STR) {
            continue;
        }
        let idx = dispatcher.shard_for(k.tag_bytes());
        match prev_idx {
            None => prev_idx = Some(idx),
            Some(prev) if prev == idx => {}
            Some(_) => return Err(VerifyError::ScriptSpansNodes),
        }
    }
    Ok(())
}

/// Errors [`redis_verify_request`] can produce.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum VerifyError {
    /// `EVAL` request touches keys on more than one shard.
    #[error("redis verify: script spans nodes")]
    ScriptSpansNodes,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::KeyPos;

    struct OddEven;
    impl FragmentDispatcher for OddEven {
        fn shard_for(&self, key: &[u8]) -> u32 {
            u32::from(*key.first().unwrap_or(&0)) % 2
        }
        fn shard_count(&self) -> u32 {
            2
        }
    }

    #[test]
    fn non_eval_is_ok() {
        let mut r = Msg::new(0, MsgType::ReqRedisGet, true);
        r.push_key(KeyPos::without_tag(b"a".to_vec()));
        r.push_key(KeyPos::without_tag(b"b".to_vec()));
        assert!(redis_verify_request(&r, &OddEven).is_ok());
    }

    #[test]
    fn eval_one_key_is_ok() {
        let mut r = Msg::new(0, MsgType::ReqRedisEval, true);
        r.push_key(KeyPos::without_tag(b"a".to_vec()));
        assert!(redis_verify_request(&r, &OddEven).is_ok());
    }

    #[test]
    fn eval_disjoint_shards_errors() {
        let mut r = Msg::new(0, MsgType::ReqRedisEval, true);
        r.push_key(KeyPos::without_tag(b"a".to_vec())); // 0x61 -> shard 1
        r.push_key(KeyPos::without_tag(b"b".to_vec())); // 0x62 -> shard 0
        assert_eq!(
            redis_verify_request(&r, &OddEven),
            Err(VerifyError::ScriptSpansNodes),
        );
    }

    #[test]
    fn eval_skips_metadata_keys() {
        let mut r = Msg::new(0, MsgType::ReqRedisEval, true);
        r.push_key(KeyPos::without_tag(b"a".to_vec()));
        r.push_key(KeyPos::without_tag(b"._add-set".to_vec()));
        assert!(redis_verify_request(&r, &OddEven).is_ok());
    }
}
