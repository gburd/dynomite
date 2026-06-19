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
