//! Redis multi-key request fragmenter.
//!
//! Faithfully reproduces `redis_fragment` from the reference
//! engine for `MGET` / `DEL` / `EXISTS` (key step 1) and `MSET`
//! (key step 2). Each fragment is returned as a [`Msg`] populated
//! with the fragment header (`*ntokens$len cmd`), the key bytes,
//! and (for `MSET`) the value bytes.
//!
//! Sharding is delegated to a [`FragmentDispatcher`] so the
//! fragmenter has no compile-time dependency on the cluster layer.

// The fragmenter assigns each input key to a shard index returned
// by the dispatcher (a `u32` by contract); the per-fragment shard
// index is therefore safe to truncate from `usize`.
#![allow(clippy::cast_possible_truncation)]

use crate::io::mbuf::{Mbuf, MbufPool};
use crate::msg::{KeyPos, Msg, MsgType};

/// Trait that maps a key (or its routing tag) to an integer shard
/// index.
pub trait FragmentDispatcher {
    /// Return the shard index for `key_tag`.
    fn shard_for(&self, key_tag: &[u8]) -> u32;
    /// Total number of shards.
    fn shard_count(&self) -> u32;
}

/// Outcome of [`redis_fragment`].
#[derive(Debug)]
pub struct FragmentOutcome {
    /// One sub-message per participating shard.
    pub fragments: Vec<Msg>,
    /// For each input key (in input order) the shard index it
    /// landed on.
    pub shard_for_key: Vec<u32>,
    /// Fragment id stamped on every emitted sub-message.
    pub frag_id: u64,
}

static FRAG_ID_SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_frag_id() -> u64 {
    FRAG_ID_SEED.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Fragment a multi-key Redis request.
///
/// `value_for_key` supplies the value bytes for `MSET` requests,
/// in input-key order. For `MGET` / `DEL` / `EXISTS` the slice is
/// ignored. The `pool` provides mbuf storage for the per-fragment
/// payload.
///
/// Returns `Ok(None)` for non-multikey requests, mirroring the
/// reference engine's `DN_OK` no-op return.
///
/// # Errors
///
/// Returns [`FragmentError::EmptyKeys`] if a multikey request has
/// no parsed keys, or [`FragmentError::MissingValue`] if `MSET`
/// has fewer values than keys.
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::msg::{KeyPos, Msg, MsgType};
/// use dynomite::proto::redis::{redis_fragment, FragmentDispatcher};
///
/// struct AllOne;
/// impl FragmentDispatcher for AllOne {
///     fn shard_for(&self, _: &[u8]) -> u32 { 0 }
///     fn shard_count(&self) -> u32 { 1 }
/// }
///
/// let mut req = Msg::new(0, MsgType::ReqRedisMget, true);
/// req.push_key(KeyPos::without_tag(b"k1".to_vec()));
/// req.push_key(KeyPos::without_tag(b"k2".to_vec()));
/// req.set_ntokens(3);
/// let pool = MbufPool::default();
/// let outcome = redis_fragment(&mut req, &AllOne, &[], &pool).unwrap().unwrap();
/// assert_eq!(outcome.fragments.len(), 1);
/// ```
pub fn redis_fragment<D: FragmentDispatcher + ?Sized>(
    r: &mut Msg,
    dispatcher: &D,
    value_for_key: &[Vec<u8>],
    pool: &MbufPool,
) -> Result<Option<FragmentOutcome>, FragmentError> {
    let key_step: usize = match r.ty() {
        MsgType::ReqRedisMget | MsgType::ReqRedisDel | MsgType::ReqRedisExists => 1,
        MsgType::ReqRedisMset => 2,
        _ => return Ok(None),
    };
    if r.keys().is_empty() {
        return Err(FragmentError::EmptyKeys);
    }
    if key_step == 2 && value_for_key.len() < r.keys().len() {
        return Err(FragmentError::MissingValue);
    }
    if r.keys().len() == 1 {
        // Reference engine returns DN_OK without fragmenting when
        // there is only one key.
        return Ok(None);
    }

    let frag_id = next_frag_id();
    let n_shards = dispatcher.shard_count() as usize;
    let mut bucket: Vec<Option<usize>> = vec![None; n_shards.max(1)];
    let mut fragments: Vec<Msg> = Vec::new();
    let mut shard_for_key = Vec::with_capacity(r.keys().len());
    let mut keys_per_fragment: Vec<Vec<KeyPos>> = Vec::new();
    let mut values_per_fragment: Vec<Vec<Vec<u8>>> = Vec::new();

    for (i, key) in r.keys().iter().enumerate() {
        let idx = dispatcher.shard_for(key.tag_bytes()) as usize;
        if idx >= bucket.len() {
            bucket.resize(idx + 1, None);
        }
        let bucket_idx = if let Some(j) = bucket.get(idx).copied().flatten() {
            j
        } else {
            let mut sub = Msg::new(0, r.ty(), true);
            sub.set_frag_id(frag_id);
            fragments.push(sub);
            keys_per_fragment.push(Vec::new());
            values_per_fragment.push(Vec::new());
            let j = fragments.len() - 1;
            if let Some(slot) = bucket.get_mut(idx) {
                *slot = Some(j);
            }
            j
        };
        if let Some(kf) = keys_per_fragment.get_mut(bucket_idx) {
            kf.push(clone_keypos(key));
        }
        if key_step == 2 {
            if let (Some(vf), Some(v)) = (
                values_per_fragment.get_mut(bucket_idx),
                value_for_key.get(i),
            ) {
                vf.push(v.clone());
            }
        }
        shard_for_key.push(u32::try_from(idx).unwrap_or(u32::MAX));
    }

    // Encode the wire frame for each fragment.
    for (i, frag) in fragments.iter_mut().enumerate() {
        let keys = keys_per_fragment.get(i).cloned().unwrap_or_default();
        let values = values_per_fragment.get(i).cloned().unwrap_or_default();
        encode_fragment(frag, &keys, &values, pool)?;
        for k in &keys {
            frag.push_key(clone_keypos(k));
        }
        let nkeys = u32::try_from(keys.len()).unwrap_or(u32::MAX);
        let multiplier = if key_step == 2 { 2 } else { 1 };
        frag.set_ntokens(1 + nkeys.saturating_mul(multiplier));
    }

    r.set_frag_id(frag_id);
    Ok(Some(FragmentOutcome {
        fragments,
        shard_for_key,
        frag_id,
    }))
}

fn clone_keypos(k: &KeyPos) -> KeyPos {
    KeyPos::new(k.key().to_vec(), k.tag())
}

fn encode_fragment(
    frag: &mut Msg,
    keys: &[KeyPos],
    values: &[Vec<u8>],
    pool: &MbufPool,
) -> Result<(), FragmentError> {
    let verb: &[u8] = match frag.ty() {
        MsgType::ReqRedisMget => b"mget",
        MsgType::ReqRedisDel => b"del",
        MsgType::ReqRedisExists => b"exists",
        MsgType::ReqRedisMset => b"mset",
        _ => return Err(FragmentError::UnsupportedType),
    };
    let mut buf: Vec<u8> = Vec::new();
    let multiplier = if values.is_empty() { 1 } else { 2 };
    let ntokens = 1 + keys.len() * multiplier;
    buf.extend_from_slice(format!("*{ntokens}\r\n").as_bytes());
    buf.extend_from_slice(format!("${}\r\n", verb.len()).as_bytes());
    buf.extend_from_slice(verb);
    buf.extend_from_slice(b"\r\n");
    for (i, k) in keys.iter().enumerate() {
        buf.extend_from_slice(format!("${}\r\n", k.key_len()).as_bytes());
        buf.extend_from_slice(k.key());
        buf.extend_from_slice(b"\r\n");
        if !values.is_empty() {
            buf.extend_from_slice(format!("${}\r\n", values[i].len()).as_bytes());
            buf.extend_from_slice(&values[i]);
            buf.extend_from_slice(b"\r\n");
        }
    }
    write_buf_into_chain(frag, pool, &buf);
    Ok(())
}

fn write_buf_into_chain(frag: &mut Msg, pool: &MbufPool, mut buf: &[u8]) {
    while !buf.is_empty() {
        let mut mb: Mbuf = pool.get();
        let n = mb.recv(buf);
        if n == 0 {
            // Pool returned a zero-capacity mbuf; bail out.
            break;
        }
        frag.mbufs_mut().push_back(mb);
        buf = &buf[n..];
    }
    frag.recompute_mlen();
}

/// Errors [`redis_fragment`] can produce.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum FragmentError {
    /// Caller invoked the fragmenter on a multikey request with no
    /// parsed keys.
    #[error("redis fragment: no keys to fragment")]
    EmptyKeys,
    /// Caller invoked the fragmenter on `MSET` with fewer values
    /// than keys.
    #[error("redis fragment: missing value for key")]
    MissingValue,
    /// Caller invoked the fragmenter on an unsupported message
    /// type.
    #[error("redis fragment: unsupported message type")]
    UnsupportedType,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct OddEven;
    impl FragmentDispatcher for OddEven {
        fn shard_for(&self, key: &[u8]) -> u32 {
            u32::from(*key.first().unwrap_or(&0)) % 2
        }
        fn shard_count(&self) -> u32 {
            2
        }
    }

    fn build_mget(keys: &[&[u8]]) -> Msg {
        let mut m = Msg::new(0, MsgType::ReqRedisMget, true);
        for k in keys {
            m.push_key(KeyPos::without_tag(k.to_vec()));
        }
        m.set_ntokens(1 + keys.len() as u32);
        m
    }

    #[test]
    fn mget_fragments_across_shards() {
        let mut m = build_mget(&[b"a", b"b", b"c"]); // a,c on shard 1, b on shard 0
        let pool = MbufPool::default();
        let outcome = redis_fragment(&mut m, &OddEven, &[], &pool)
            .unwrap()
            .unwrap();
        assert_eq!(outcome.fragments.len(), 2);
        assert_eq!(outcome.shard_for_key.len(), 3);
    }

    #[test]
    fn single_key_does_not_fragment() {
        let mut m = build_mget(&[b"a"]);
        let pool = MbufPool::default();
        assert!(redis_fragment(&mut m, &OddEven, &[], &pool)
            .unwrap()
            .is_none());
    }

    #[test]
    fn non_multikey_is_noop() {
        let mut m = Msg::new(0, MsgType::ReqRedisGet, true);
        let pool = MbufPool::default();
        assert!(redis_fragment(&mut m, &OddEven, &[], &pool)
            .unwrap()
            .is_none());
    }

    #[test]
    fn mset_requires_values() {
        let mut m = Msg::new(0, MsgType::ReqRedisMset, true);
        m.push_key(KeyPos::without_tag(b"a".to_vec()));
        m.push_key(KeyPos::without_tag(b"b".to_vec()));
        let pool = MbufPool::default();
        let res = redis_fragment(&mut m, &OddEven, &[], &pool);
        assert!(matches!(res, Err(FragmentError::MissingValue)));
    }
}
