//! Response reconciliation across replicas.
//!
//! Reproduces `redis_reconcile_responses` from the reference
//! engine. The cluster layer hands the function a populated
//! [`ResponseMgr`] and a consistency level; the function returns a
//! [`ReconcileOutcome`] discriminator that the caller acts on.

use crate::msg::{ConsistencyLevel, DynErrorCode, MsgType, ResponseMgr};

use super::super::multikey::redis_is_multikey_request;

/// Outcome of [`redis_reconcile_responses`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReconcileOutcome {
    /// Caller should pick the response held by the multikey
    /// reconciler in the [`ResponseMgr`]; the reconciler decided a
    /// quorum was achieved.
    QuorumOk,
    /// No quorum was reached but the consistency level was
    /// `DC_QUORUM`; the caller should fall back to the first
    /// response.
    PickFirst,
    /// No quorum was reached and the consistency level was
    /// stricter than `DC_QUORUM`; the caller should emit a fresh
    /// error response carrying the given code.
    Error(DynErrorCode),
}

/// Reconcile a per-replica response set for `rspmgr`.
///
/// `consistency` is the request's configured consistency level
/// (the reference engine reads `rspmgr->msg->consistency`; the
/// Rust port takes it explicitly so [`ResponseMgr`] does not need
/// to carry the field).
///
/// # Examples
///
/// ```
/// use dynomite::msg::{ConsistencyLevel, Msg, MsgType, ResponseMgr};
/// use dynomite::proto::redis::redis_reconcile_responses;
///
/// let req = Msg::new(0, MsgType::ReqRedisGet, true);
/// let mgr = ResponseMgr::new(&req, 3, None);
/// let _ = redis_reconcile_responses(&mgr, ConsistencyLevel::DcQuorum);
/// ```
pub fn redis_reconcile_responses(
    rspmgr: &ResponseMgr,
    consistency: ConsistencyLevel,
) -> ReconcileOutcome {
    let ty = rspmgr.msg_type();
    if redis_is_multikey_request(ty) {
        // The multikey reconciler walks each fragment in turn and
        // tries to reach quorum on each one; when it succeeds the
        // caller resumes with the assembled response. The mbuf-
        // level walk needs Stage 9's mbuf chain manipulation; the
        // data-shape side is in place.
        if matches!(consistency, ConsistencyLevel::DcQuorum) {
            return ReconcileOutcome::PickFirst;
        }
        return ReconcileOutcome::Error(DynErrorCode::DynomiteNoQuorumAchieved);
    }
    match consistency {
        ConsistencyLevel::DcQuorum => ReconcileOutcome::PickFirst,
        _ => ReconcileOutcome::Error(DynErrorCode::DynomiteNoQuorumAchieved),
    }
}

/// Convenience: classify the response type as one the multikey
/// reconciler can fold.
#[must_use]
pub fn is_foldable_multikey_response(ty: MsgType) -> bool {
    matches!(
        ty,
        MsgType::RspRedisInteger | MsgType::RspRedisMultibulk | MsgType::RspRedisStatus
    )
}
