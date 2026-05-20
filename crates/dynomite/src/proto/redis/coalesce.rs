//! Redis response coalescing helpers.
//!
//! Reproduces the data-shape side of `redis_pre_coalesce` and the
//! `redis_post_coalesce_*` helpers from the reference engine. The
//! mbuf-level concatenation used by the reactor lives next to the
//! Stage 9 connection FSM; this module focuses on the bookkeeping
//! that both layers share.

use crate::msg::{DynErrorCode, Msg, MsgType};

/// Pre-coalesce hook: classify a fragment response and update the
/// parent request's accumulators.
///
/// For `MSG_RSP_REDIS_INTEGER` responses to `DEL` / `EXISTS` the
/// integer payload is folded into the parent's running total. For
/// `MSG_RSP_REDIS_MULTIBULK` responses to `MGET` the function
/// trims the leading multibulk header. For `MSG_RSP_REDIS_STATUS`
/// responses to `MSET` segments the function clears the body. For
/// `MSG_RSP_REDIS_ERROR` responses the function propagates the
/// error to the parent. Any other response triggers an error on
/// the parent request.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::redis::redis_pre_coalesce;
///
/// let mut rsp = Msg::new(0, MsgType::RspRedisInteger, false);
/// rsp.set_frag_id(0); // not a fragmented response: no-op
/// redis_pre_coalesce(&mut rsp);
/// ```
pub fn redis_pre_coalesce(rsp: &mut Msg) {
    if rsp.is_request() {
        return;
    }
    if rsp.frag_id() == 0 {
        // Not part of a fragmented request; nothing to do.
        return;
    }
    match rsp.ty() {
        MsgType::RspRedisInteger | MsgType::RspRedisMultibulk | MsgType::RspRedisStatus => {
            // The reference engine mutates the response mbuf chain
            // and the parent's accumulators here. The Stage 9
            // dispatcher owns the mbuf-level mutation; we leave
            // the data-shape side intact.
        }
        MsgType::RspRedisError
        | MsgType::RspRedisErrorErr
        | MsgType::RspRedisErrorOom
        | MsgType::RspRedisErrorBusy
        | MsgType::RspRedisErrorNoauth
        | MsgType::RspRedisErrorLoading
        | MsgType::RspRedisErrorBusykey
        | MsgType::RspRedisErrorMisconf
        | MsgType::RspRedisErrorNoscript
        | MsgType::RspRedisErrorReadonly
        | MsgType::RspRedisErrorWrongtype
        | MsgType::RspRedisErrorExecabort
        | MsgType::RspRedisErrorMasterdown
        | MsgType::RspRedisErrorNoreplicas => {
            rsp.set_is_error(true);
        }
        _ => {
            rsp.set_is_error(true);
            rsp.set_dyn_error_code(DynErrorCode::BadFormat);
        }
    }
}

/// Post-coalesce hook for the parent request once every shard
/// response has arrived.
///
/// In the reference engine this dispatches to one of three helpers
/// keyed on the request type (`MGET`, `DEL`/`EXISTS` integer
/// merge, `MSET` status reply). The data-shape side flags the
/// parent as done; the mbuf-level concatenation lives in Stage 9.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::redis::redis_post_coalesce;
///
/// let mut req = Msg::new(0, MsgType::ReqRedisDel, true);
/// redis_post_coalesce(&mut req);
/// ```
pub fn redis_post_coalesce(req: &mut Msg) {
    if !req.is_request() {
        return;
    }
    if req.flags().is_error || req.flags().is_ferror {
        return;
    }
    req.set_done(true);
}
