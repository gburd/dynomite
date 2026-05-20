//! Response lifecycle helpers.
//!
//! A small number of pure-data helpers - constructing error
//! responses, pairing a response with its request - live here. The
//! connection plumbing (response send / done, queue threading)
//! ships in Stage 9.

use super::message::Msg;
use super::msg_type::MsgType;
use super::DynErrorCode;

/// Build a synthetic error response for `req`.
///
/// The constructed message inherits the request's id (so the
/// dispatcher can pair them), sets `is_request` to false, marks the
/// response as in-error, and stamps the error codes. The response
/// type defaults to [`MsgType::RspRedisError`] for Redis traffic and
/// [`MsgType::RspMcServerError`] for memcached traffic; the caller
/// passes the correct one explicitly.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{response, DynErrorCode, Msg, MsgType};
///
/// let req = Msg::new(7, MsgType::ReqRedisGet, true);
/// let rsp = response::make_error(
///     &req,
///     MsgType::RspRedisError,
///     13,
///     DynErrorCode::PeerHostDown,
/// );
/// assert_eq!(rsp.parent_id(), 7);
/// assert!(rsp.flags().is_error);
/// ```
#[must_use]
pub fn make_error(
    req: &Msg,
    err_type: MsgType,
    error_code: i32,
    dyn_error_code: DynErrorCode,
) -> Msg {
    debug_assert!(matches!(
        err_type,
        MsgType::RspRedisError | MsgType::RspMcServerError | MsgType::RspMcError
    ));
    let mut rsp = Msg::new(req.id(), err_type, false);
    rsp.set_parent_id(req.id());
    rsp.set_is_error(true);
    rsp.set_error_code(error_code);
    rsp.set_dyn_error_code(dyn_error_code);
    rsp
}

/// Pair a response with its request: stamps the response's parent
/// id and sets the request's `selected_rsp` to the response id.
///
/// Returns the previous selected-response id, if any, so callers can
/// release the now-stale response.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{response, Msg, MsgType};
///
/// let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
/// let rsp = Msg::new(2, MsgType::RspRedisStatus, false);
/// let prev = response::link(&mut req, &rsp);
/// assert!(prev.is_none());
/// assert_eq!(req.selected_rsp(), Some(2));
/// ```
pub fn link(req: &mut Msg, rsp: &Msg) -> Option<crate::core::types::MsgId> {
    let prev = req.selected_rsp();
    req.set_selected_rsp(Some(rsp.id()));
    prev
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::MsgType;

    #[test]
    fn error_response_inherits_request_id() {
        let req = Msg::new(42, MsgType::ReqRedisGet, true);
        let rsp = make_error(
            &req,
            MsgType::RspRedisError,
            13,
            DynErrorCode::DynomiteUnknownError,
        );
        assert_eq!(rsp.id(), 42);
        assert_eq!(rsp.parent_id(), 42);
        assert!(rsp.flags().is_error);
        assert_eq!(rsp.error_code(), 13);
    }

    #[test]
    fn link_returns_previous() {
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        let rsp1 = Msg::new(2, MsgType::RspRedisStatus, false);
        let rsp2 = Msg::new(3, MsgType::RspRedisStatus, false);
        assert!(link(&mut req, &rsp1).is_none());
        assert_eq!(link(&mut req, &rsp2), Some(2));
        assert_eq!(req.selected_rsp(), Some(3));
    }
}
