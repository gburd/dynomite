//! Memcached response coalescing helpers.
//!
//! When a `get` / `gets` request fans out across shards, the
//! per-shard responses need to be stitched back into a single
//! reply. Two helpers handle this: `memcache_pre_coalesce` adjusts
//! each shard response in place to drop the `END` marker, and
//! `memcache_post_coalesce` concatenates the per-shard payloads and
//! appends a single trailing `END\r\n`.
//!
//! These helpers cover the data-shape side; the mbuf-level wiring
//! the reactor needs lives in the connection FSM in [`crate::net`].

use crate::msg::{Msg, MsgType};

/// Adjust `rsp` in place ahead of post-coalesce.
///
/// For `VALUE` and `END` shard responses, the helper records that
/// the trailing `END` marker should be dropped during merge. For
/// any other response on a fragmented request the helper flags the
/// parent request as errored.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::memcache::memcache_pre_coalesce;
///
/// let mut rsp = Msg::new(0, MsgType::RspMcEnd, false);
/// rsp.set_frag_id(1);
/// memcache_pre_coalesce(&mut rsp);
/// // No mbuf side-effects in the data-shape path; the function
/// // returns without panicking.
/// ```
pub fn memcache_pre_coalesce(rsp: &mut Msg) {
    if rsp.is_request() {
        return;
    }
    if rsp.frag_id() == 0 {
        return;
    }
    match rsp.ty() {
        MsgType::RspMcValue | MsgType::RspMcEnd => {
            // The trailing END marker is dropped from the
            // response chain by the connection FSM, which owns the
            // mbuf chain mutation; the data-shape side just records
            // the intent via the existing `end_marker` field.
            // No-op here: the parser already populated `end_marker`.
        }
        _ => {
            rsp.set_is_error(true);
            rsp.set_dyn_error_code(crate::msg::DynErrorCode::BadFormat);
        }
    }
}

/// Post-coalesce hook for the parent request once every shard
/// response has arrived.
///
/// This concatenates each shard's value
/// payload onto the parent response and writes a single trailing
/// `END\r\n`. The data-shape side is to flag the parent request
/// as fully done; the mbuf-level concatenation is done by the
/// connection FSM.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::memcache::memcache_post_coalesce;
///
/// let mut req = Msg::new(0, MsgType::ReqMcGet, true);
/// memcache_post_coalesce(&mut req);
/// // The data-shape path is a no-op when there are no fragments
/// // attached.
/// ```
pub fn memcache_post_coalesce(request: &mut Msg) {
    if !request.is_request() {
        return;
    }
    if request.flags().is_error || request.flags().is_ferror {
        // The owner connection is marked as errored by the
        // connection-coupled path in [`crate::net`].
        return;
    }
    request.set_done(true);
}

/// The reply a client gets when any fragment of a split retrieval failed:
/// the generic server error built from the parent's (zero) error codes.
pub const MEMCACHE_FRAGMENT_ERROR_REPLY: &[u8] = b"SERVER_ERROR unknown: unknown\r\n";

/// Copy one `VALUE` block (`VALUE <key> <flags> <len>[ <cas>]\r\n<data>\r\n`)
/// from the front of `src` into `dst` and return the bytes consumed.
///
/// An empty `src` is a key that does not exist: nothing is copied and
/// `Some(0)` is returned. `None` when `src` is not a whole `VALUE` block.
///
/// # Examples
///
/// ```
/// use dynomite::proto::memcache::memcache_copy_bulk;
///
/// let mut out = Vec::new();
/// assert_eq!(memcache_copy_bulk(&mut out, b"VALUE k 0 2 9\r\nhi\r\nVALUE"), Some(19));
/// assert_eq!(out, b"VALUE k 0 2 9\r\nhi\r\n");
/// assert_eq!(memcache_copy_bulk(&mut out, b""), Some(0));
/// assert_eq!(memcache_copy_bulk(&mut out, b"VALUE k 0 5\r\nhi\r\n"), None);
/// ```
pub fn memcache_copy_bulk(dst: &mut Vec<u8>, src: &[u8]) -> Option<usize> {
    if src.is_empty() {
        return Some(0);
    }
    if src.first() != Some(&b'V') {
        return None;
    }
    // Skip `VALUE`, the key, and the flags.
    let mut p = 0;
    for _ in 0..3 {
        p += src.get(p..)?.iter().position(|&b| b == b' ')? + 1;
    }
    let mut len: usize = 0;
    while let Some(d) = src.get(p).filter(|b| b.is_ascii_digit()) {
        len = len.checked_mul(10)?.checked_add(usize::from(d - b'0'))?;
        p += 1;
    }
    // Skip the cas of a `gets` reply.
    p += src.get(p..)?.iter().position(|&b| b == b'\r')?;
    let total = p.checked_add(len)?.checked_add(4)?;
    let block = src.get(..total)?;
    if !block.ends_with(b"\r\n") {
        return None;
    }
    dst.extend_from_slice(block);
    Some(total)
}

/// One fragment reply with its `END` marker stripped; `None` unless the
/// reply is `VALUE` blocks or a bare `END`.
fn fragment_body(reply: &[u8]) -> Option<&[u8]> {
    let body = reply.strip_suffix(b"END\r\n")?;
    (body.is_empty() || body.starts_with(b"VALUE ")).then_some(body)
}

/// Merge the replies to the fragments of a split `get` / `gets` into the
/// one reply the client gets.
///
/// `frag_for_key[i]` is the fragment key `i` (client order) was placed in;
/// `replies[f]` is fragment `f`'s reply wire bytes, `None` when it produced
/// no reply. Each fragment's `END` is dropped, then every key in turn
/// takes the next `VALUE` block of its fragment (a miss takes nothing),
/// and one `END\r\n` closes the reply. A missing reply, a reply that is
/// not `VALUE` / `END`, or a malformed block makes the whole reply
/// [`MEMCACHE_FRAGMENT_ERROR_REPLY`].
///
/// # Examples
///
/// ```
/// use dynomite::proto::memcache::memcache_coalesce_fragments;
///
/// // get a b c with a,c on fragment 0 (c misses) and b on fragment 1.
/// let replies = vec![
///     Some(b"VALUE a 0 1\r\nA\r\nEND\r\n".to_vec()),
///     Some(b"VALUE b 0 1\r\nB\r\nEND\r\n".to_vec()),
/// ];
/// assert_eq!(
///     memcache_coalesce_fragments(&[0, 1, 0], &replies),
///     b"VALUE a 0 1\r\nA\r\nVALUE b 0 1\r\nB\r\nEND\r\n"
/// );
/// ```
#[must_use]
pub fn memcache_coalesce_fragments(frag_for_key: &[usize], replies: &[Option<Vec<u8>>]) -> Vec<u8> {
    let err = || MEMCACHE_FRAGMENT_ERROR_REPLY.to_vec();
    let mut bodies: Vec<&[u8]> = Vec::with_capacity(replies.len());
    for reply in replies {
        match reply.as_deref().and_then(fragment_body) {
            Some(b) => bodies.push(b),
            None => return err(),
        }
    }
    let mut out = Vec::new();
    let mut cursor = vec![0usize; bodies.len()];
    for &f in frag_for_key {
        let (Some(body), Some(at)) = (bodies.get(f), cursor.get_mut(f)) else {
            return err();
        };
        match body
            .get(*at..)
            .and_then(|rest| memcache_copy_bulk(&mut out, rest))
        {
            Some(n) => *at += n,
            None => return err(),
        }
    }
    out.extend_from_slice(b"END\r\n");
    out
}
