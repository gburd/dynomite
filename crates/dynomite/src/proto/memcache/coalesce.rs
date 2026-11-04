//! Memcached response coalescing for split `get` / `gets` requests.
//!
//! When a retrieval fans out across the owners of the local rack, the
//! per-owner replies are stitched back into a single reply:
//! [`memcache_pre_coalesce`] checks each fragment reply and drops its
//! `END` marker, and [`memcache_post_coalesce`] takes each key's `VALUE`
//! block in client key order and appends a single trailing `END\r\n`.
//! [`memcache_coalesce_fragments`] runs both and is what the dispatcher
//! calls.

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

/// One fragment reply with its `END` marker dropped.
///
/// `None` unless the reply is `VALUE` blocks followed by `END`, or a bare
/// `END`: any other reply to a fragment is an error for the whole request.
///
/// # Examples
///
/// ```
/// use dynomite::proto::memcache::memcache_pre_coalesce;
///
/// assert_eq!(memcache_pre_coalesce(b"VALUE a 0 1\r\nA\r\nEND\r\n"), Some(&b"VALUE a 0 1\r\nA\r\n"[..]));
/// assert_eq!(memcache_pre_coalesce(b"END\r\n"), Some(&b""[..]));
/// assert_eq!(memcache_pre_coalesce(b"SERVER_ERROR oom\r\n"), None);
/// ```
#[must_use]
pub fn memcache_pre_coalesce(reply: &[u8]) -> Option<&[u8]> {
    let body = reply.strip_suffix(b"END\r\n")?;
    (body.is_empty() || body.starts_with(b"VALUE ")).then_some(body)
}

/// Build the client reply from the fragment bodies left by
/// [`memcache_pre_coalesce`]: every key in client order takes the next
/// `VALUE` block of its fragment `frag_for_key[i]` (a miss takes
/// nothing), then `END\r\n`. `None` when a key names no fragment or a
/// block is malformed.
///
/// # Examples
///
/// ```
/// use dynomite::proto::memcache::memcache_post_coalesce;
///
/// let bodies: [&[u8]; 2] = [b"VALUE a 0 1\r\nA\r\n", b""];
/// assert_eq!(
///     memcache_post_coalesce(&[1, 0], &bodies).unwrap(),
///     b"VALUE a 0 1\r\nA\r\nEND\r\n"
/// );
/// ```
#[must_use]
pub fn memcache_post_coalesce(frag_for_key: &[usize], bodies: &[&[u8]]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut cursor = vec![0usize; bodies.len()];
    for &f in frag_for_key {
        let at = cursor.get_mut(f)?;
        *at += memcache_copy_bulk(&mut out, bodies.get(f)?.get(*at..)?)?;
    }
    out.extend_from_slice(b"END\r\n");
    Some(out)
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
    let bodies: Option<Vec<&[u8]>> = replies
        .iter()
        .map(|r| r.as_deref().and_then(memcache_pre_coalesce))
        .collect();
    bodies
        .and_then(|b| memcache_post_coalesce(frag_for_key, &b))
        .unwrap_or_else(|| MEMCACHE_FRAGMENT_ERROR_REPLY.to_vec())
}
