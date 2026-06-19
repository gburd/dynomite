//! Memcached request fragmenter.
//!
//! Multi-key `get`/`gets` requests must be split into one
//! sub-request per backend shard. The fragmenter walks the parsed
//! key list, groups keys by their shard index, and emits one
//! sub-request per shard with the appropriate verb and trailing
//! CRLF.
//!
//! Sharding is delegated to a [`FragmentDispatcher`] so the
//! fragmenter does not depend on the cluster layer. Tests pass an
//! in-memory dispatcher; the cluster layer supplies one backed by
//! its per-key, per-rack peer routing.

#![allow(clippy::cast_possible_truncation)]

use crate::io::mbuf::{Mbuf, MbufPool};
use crate::msg::{KeyPos, Msg, MsgType};

use super::commands::memcache_retrieval;

/// Trait that maps a key (or its routing tag) to an integer shard
/// index. The fragmenter groups keys by shard index.
pub trait FragmentDispatcher {
    /// Return the shard index for `key_tag`.
    fn shard_for(&self, key_tag: &[u8]) -> u32;
    /// Total number of shards (must be at least
    /// `max(shard_for) + 1`).
    fn shard_count(&self) -> u32;
}

/// Outcome of [`memcache_fragment`].
#[derive(Debug)]
pub struct FragmentOutcome {
    /// Sub-messages to send out, one per participating shard.
    pub fragments: Vec<Msg>,
    /// Per-input-key shard index, in input order.
    pub shard_for_key: Vec<u32>,
    /// Fragment id assigned to every emitted sub-message.
    pub frag_id: u64,
}

static FRAG_ID_SEED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_frag_id() -> u64 {
    FRAG_ID_SEED.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Fragment a multi-key Memcached `get` / `gets` request.
///
/// Returns `Ok(None)` for non-retrieval requests (the reference
/// engine returns `DN_OK` and leaves the message unfragmented).
/// Returns `Ok(Some(FragmentOutcome))` for retrieval requests with
/// at least one parsed key. Each emitted fragment carries a fully
/// formed wire frame (`get k1 k2 ...\r\n` or `gets k1 k2 ...\r\n`)
/// in its mbuf chain.
///
/// # Errors
///
/// Returns [`FragmentError::EmptyKeys`] when called on a
/// retrieval request that has no parsed keys (which should be
/// impossible if the parser ran successfully).
/// Returns [`FragmentError::UnsupportedType`] if the message type
/// is not a memcache retrieval request after the initial check.
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::msg::{KeyPos, Msg, MsgType};
/// use dynomite::proto::memcache::{memcache_fragment, FragmentDispatcher};
///
/// struct Dispatcher;
/// impl FragmentDispatcher for Dispatcher {
///     fn shard_for(&self, key: &[u8]) -> u32 {
///         u32::from(key[0]) % 2
///     }
///     fn shard_count(&self) -> u32 { 2 }
/// }
///
/// let pool = MbufPool::default();
/// let mut req = Msg::new(0, MsgType::ReqMcGet, true);
/// req.push_key(KeyPos::without_tag(b"a".to_vec()));
/// req.push_key(KeyPos::without_tag(b"b".to_vec()));
/// let outcome = memcache_fragment(&mut req, &Dispatcher, &pool).unwrap().unwrap();
/// assert_eq!(outcome.fragments.len(), 2);
/// ```
pub fn memcache_fragment<D: FragmentDispatcher + ?Sized>(
    r: &mut Msg,
    dispatcher: &D,
    pool: &MbufPool,
) -> Result<Option<FragmentOutcome>, FragmentError> {
    if !memcache_retrieval(r.ty()) {
        return Ok(None);
    }
    if r.keys().is_empty() {
        return Err(FragmentError::EmptyKeys);
    }

    let n_shards = dispatcher.shard_count() as usize;
    let mut bucket: Vec<Option<usize>> = vec![None; n_shards.max(1)];
    let mut fragments: Vec<Msg> = Vec::new();
    let mut keys_per_fragment: Vec<Vec<KeyPos>> = Vec::new();
    let mut shard_for_key = Vec::with_capacity(r.keys().len());

    let frag_id = next_frag_id();

    for key in r.keys() {
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
            let j = fragments.len() - 1;
            if let Some(slot) = bucket.get_mut(idx) {
                *slot = Some(j);
            }
            j
        };
        if let Some(klist) = keys_per_fragment.get_mut(bucket_idx) {
            klist.push(clone_keypos(key));
        }
        shard_for_key.push(u32::try_from(idx).unwrap_or(u32::MAX));
    }

    // Emit the wire frame for each fragment, then attach key
    // metadata and recompute the parsed-token count.
    for (i, frag) in fragments.iter_mut().enumerate() {
        let keys = keys_per_fragment.get(i).cloned().unwrap_or_default();
        encode_fragment(frag, &keys, pool)?;
        for k in &keys {
            frag.push_key(clone_keypos(k));
        }
        let nkeys = u32::try_from(keys.len()).unwrap_or(u32::MAX);
        // ntokens for memcache get/gets is 1 (verb) + N keys.
        frag.set_ntokens(1u32.saturating_add(nkeys));
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

fn encode_fragment(frag: &mut Msg, keys: &[KeyPos], pool: &MbufPool) -> Result<(), FragmentError> {
    let verb: &[u8] = match frag.ty() {
        MsgType::ReqMcGet => b"get",
        MsgType::ReqMcGets => b"gets",
        _ => return Err(FragmentError::UnsupportedType),
    };
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(verb);
    for k in keys {
        buf.push(b' ');
        buf.extend_from_slice(k.key());
    }
    buf.extend_from_slice(b"\r\n");
    write_buf_into_chain(frag, pool, &buf);
    Ok(())
}

fn write_buf_into_chain(frag: &mut Msg, pool: &MbufPool, mut buf: &[u8]) {
    while !buf.is_empty() {
        let mut mb: Mbuf = pool.get();
        let n = mb.recv(buf);
        if n == 0 {
            break;
        }
        frag.mbufs_mut().push_back(mb);
        buf = &buf[n..];
    }
    frag.recompute_mlen();
}

/// Errors produced by [`memcache_fragment`].
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum FragmentError {
    /// Caller invoked the fragmenter on a retrieval request with no
    /// parsed keys.
    #[error("memcache fragment: no keys to fragment")]
    EmptyKeys,
    /// Caller invoked the fragmenter on an unsupported message type.
    #[error("memcache fragment: unsupported message type")]
    UnsupportedType,
}
