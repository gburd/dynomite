//! Request lifecycle helpers.
//!
//! Request handling splits into two layers: request-side data
//! manipulation (which fragments are done, which are in error,
//! what error to send) and connection-side plumbing (timeout
//! queues, recv/send done callbacks, peer forwarding).
//!
//! This module owns the data-side helpers; the connection-side
//! helpers live in [`crate::net`].

use crate::core::types::MsgId;

use super::message::Msg;
use super::queue::MsgQueue;

/// Mark `req` as in-error with `error_code` (a libc errno-shaped
/// value) and the matching `dyn_error_code`. The flag is set so the
/// response path can synthesise an error reply on the next pass.
///
/// Returns `true` when the message transitions from healthy to
/// error; subsequent calls are no-ops.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{request, DynErrorCode, Msg, MsgType};
///
/// let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
/// assert!(request::set_error(&mut req, 13, DynErrorCode::PeerHostDown));
/// assert!(req.flags().is_error);
/// assert!(!request::set_error(&mut req, 13, DynErrorCode::PeerHostDown));
/// ```
pub fn set_error(req: &mut Msg, error_code: i32, dyn_error_code: super::DynErrorCode) -> bool {
    if req.flags().is_error {
        return false;
    }
    req.set_is_error(true);
    req.flags_mut().done = true;
    req.set_error_code(error_code);
    req.set_dyn_error_code(dyn_error_code);
    true
}

/// True when the request has been resolved end-to-end: a response
/// has been selected, all fragments are accounted for, and (for
/// fragment vectors) the parent fragment has finished aggregating.
///
/// The connection-coupled propagation pass that walks the client
/// queue and marks every sibling fragment lives in [`crate::net`];
/// this helper returns the data-shape answer for a single request.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{request, Msg, MsgType};
///
/// let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
/// assert!(!request::is_done(&req));
/// req.set_selected_rsp(Some(2));
/// assert!(request::is_done(&req));
/// ```
#[must_use]
pub fn is_done(req: &Msg) -> bool {
    if req.selected_rsp().is_none() {
        return false;
    }
    if req.fragment_ids().is_empty() {
        return true;
    }
    req.flags().fdone
}

/// True when the request is in error: either marked directly or
/// flagged through fragment-error propagation.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{request, DynErrorCode, Msg, MsgType};
///
/// let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
/// assert!(!request::is_error(&req));
/// request::set_error(&mut req, 13, DynErrorCode::PeerHostDown);
/// assert!(request::is_error(&req));
/// ```
#[must_use]
pub fn is_error(req: &Msg) -> bool {
    req.flags().is_error || req.flags().is_ferror
}

/// Drain `from` of every request whose `selected_rsp` matches `id`
/// and forward them to `to`.
///
/// This is the data-shape building block the connection-level
/// `req_send_next` / `req_send_done` functions consume; it lets
/// tests exercise the sibling-walk without standing up the full
/// connection FSM.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{request, Msg, MsgQueue, MsgType};
///
/// let mut a = MsgQueue::new();
/// let mut b = MsgQueue::new();
/// let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
/// m.set_selected_rsp(Some(99));
/// a.push_back(m);
/// request::move_completed(&mut a, &mut b, 99);
/// assert!(a.is_empty());
/// assert_eq!(b.len(), 1);
/// ```
pub fn move_completed(from: &mut MsgQueue, to: &mut MsgQueue, id: MsgId) {
    let mut keep = MsgQueue::new();
    while let Some(msg) = from.pop_front() {
        if msg.selected_rsp() == Some(id) {
            to.push_back(msg);
        } else {
            keep.push_back(msg);
        }
    }
    while let Some(msg) = keep.pop_front() {
        from.push_back(msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msg::{DynErrorCode, MsgType};

    #[test]
    fn error_flags_propagate() {
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        assert!(set_error(&mut req, 7, DynErrorCode::PeerHostDown));
        assert!(req.flags().is_error);
        assert!(req.flags().done);
        assert_eq!(req.error_code(), 7);
        assert_eq!(req.dyn_error_code(), DynErrorCode::PeerHostDown);
        assert!(is_error(&req));
        assert!(!set_error(&mut req, 9, DynErrorCode::DynomiteUnknownError));
        assert_eq!(req.error_code(), 7);
    }

    #[test]
    fn fragmented_request_done_requires_fdone() {
        let mut req = Msg::new(1, MsgType::ReqRedisMget, true);
        req.push_fragment_id(2);
        req.set_selected_rsp(Some(99));
        assert!(!is_done(&req));
        req.flags_mut().fdone = true;
        assert!(is_done(&req));
    }

    #[test]
    fn is_done_false_without_selected_rsp() {
        // No selected response => not done (early return arm).
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        assert!(req.selected_rsp().is_none());
        assert!(!is_done(&req));
    }

    #[test]
    fn is_done_true_for_non_fragmented_with_response() {
        // A non-fragmented request with a selected response is done
        // without consulting fdone (the empty fragment_ids arm).
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        req.set_selected_rsp(Some(2));
        assert!(req.fragment_ids().is_empty());
        assert!(is_done(&req));
    }

    #[test]
    fn move_completed_partitions_by_selected_rsp() {
        // Requests whose selected_rsp matches move to `to`; the rest
        // are preserved in `from` in their original order.
        use crate::msg::MsgQueue;
        let mut from = MsgQueue::new();
        let mut to = MsgQueue::new();
        for (id, rsp) in [(1u64, 99u64), (2, 7), (3, 99)] {
            let mut m = Msg::new(id, MsgType::ReqRedisGet, true);
            m.set_selected_rsp(Some(rsp));
            from.push_back(m);
        }
        move_completed(&mut from, &mut to, 99);
        assert_eq!(to.len(), 2);
        // The non-matching request stays in `from`.
        assert_eq!(from.len(), 1);
        assert_eq!(from.front().unwrap().id(), 2);
    }
}
