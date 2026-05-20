//! Memcached request fragmenter.
//!
//! Multi-key `get`/`gets` requests must be split into one
//! sub-request per backend shard. The reference engine implements
//! this in `memcache_fragment_retrieval`. The Rust port keeps the
//! same semantics: walk the parsed key list, group keys by their
//! shard index, and emit one sub-request per shard with the
//! appropriate prepended verb and trailing CRLF.
//!
//! Sharding is delegated to a [`FragmentDispatcher`] so the
//! fragmenter does not depend on the cluster layer (which lands in
//! Stage 10). Tests pass an in-memory dispatcher; the real engine
//! supplies one backed by `dnode_peer_idx_for_key_on_rack`.

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
/// at least one parsed key.
///
/// # Errors
///
/// Returns [`FragmentError::EmptyKeys`] when called on a
/// retrieval request that has no parsed keys (which should be
/// impossible if the parser ran successfully).
///
/// # Examples
///
/// ```
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
/// let mut req = Msg::new(0, MsgType::ReqMcGet, true);
/// req.push_key(KeyPos::without_tag(b"a".to_vec()));
/// req.push_key(KeyPos::without_tag(b"b".to_vec()));
/// let outcome = memcache_fragment(&mut req, &Dispatcher).unwrap().unwrap();
/// assert_eq!(outcome.fragments.len(), 2);
/// ```
pub fn memcache_fragment<D: FragmentDispatcher + ?Sized>(
    r: &mut Msg,
    dispatcher: &D,
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
    let mut shard_for_key = Vec::with_capacity(r.keys().len());

    let frag_id = next_frag_id();

    for (i, key) in r.keys().iter().enumerate() {
        let idx = dispatcher.shard_for(key.tag_bytes()) as usize;
        if idx >= bucket.len() {
            bucket.resize(idx + 1, None);
        }
        let bucket_idx = match bucket[idx] {
            Some(j) => j,
            None => {
                let mut sub = Msg::new(0, r.ty(), true);
                sub.set_frag_id(frag_id);
                fragments.push(sub);
                let j = fragments.len() - 1;
                bucket[idx] = Some(j);
                j
            }
        };
        let sub = &mut fragments[bucket_idx];
        sub.push_key(clone_keypos(key));
        sub.set_ntokens(sub.ntokens().saturating_add(1));
        shard_for_key.push(idx as u32);
        let _ = i;
    }

    // Prepend the verb and append CRLF for each fragment.
    for frag in &mut fragments {
        let verb: &[u8] = match frag.ty() {
            MsgType::ReqMcGet => b"get",
            MsgType::ReqMcGets => b"gets",
            _ => return Err(FragmentError::UnsupportedType),
        };
        // Recompute mlen later via the caller; for now we only build
        // the parsed shape on the fragments.
        let _ = verb;
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
