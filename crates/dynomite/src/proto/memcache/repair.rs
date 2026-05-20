//! Memcached repair surface.
//!
//! In the reference engine, every `memcache_*_repair` function and
//! `memcache_reconcile_responses` either returns `DN_OK` after doing
//! nothing or, in the reconciliation case, returns the first
//! response when consistency is `DC_QUORUM` and an error response
//! otherwise. Memcached responses are not rewritten, so the
//! "rewrite with metadata" surface is intentionally empty.
//!
//! The Rust port reproduces this exactly. Each function below
//! mirrors the reference shape for parity with the Redis surface
//! and so the cluster layer can call into either protocol via the
//! same trait set.

use crate::msg::{ConsistencyLevel, DynErrorCode, Msg, ResponseMgr};
#[cfg(test)]
use crate::msg::MsgType;

/// Repair-surface result type. Matches the Redis repair return
/// shape so the cluster dispatcher can use a single
/// `Result<RepairOutcome, RepairError>` discriminant.
#[derive(Debug)]
pub enum RepairOutcome {
    /// No rewrite or repair message produced.
    NoOp,
    /// A rewritten message (Memcached path never produces one).
    Rewritten(Box<Msg>),
}

/// Errors the Memcached repair surface can raise. Memcached has no
/// live failure modes; the variant exists for parity with the
/// Redis surface.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RepairError {
    /// Reserved variant.
    #[error("memcache repair: reserved")]
    Reserved,
}

/// Memcached query rewrite. The reference engine returns `DN_OK`
/// without producing a rewritten message. The Rust port returns
/// [`RepairOutcome::NoOp`] for the same reason.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::memcache::memcache_rewrite_query;
///
/// let mut m = Msg::new(0, MsgType::ReqMcSet, true);
/// let outcome = memcache_rewrite_query(&mut m).unwrap();
/// matches!(outcome, dynomite::proto::memcache::repair::RepairOutcome::NoOp);
/// ```
pub fn memcache_rewrite_query(_orig: &mut Msg) -> Result<RepairOutcome, RepairError> {
    Ok(RepairOutcome::NoOp)
}

/// Memcached query rewrite with timestamp metadata. The reference
/// engine returns `DN_OK` without rewriting; the Rust port matches.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::memcache::memcache_rewrite_query_with_timestamp_md;
///
/// let mut m = Msg::new(0, MsgType::ReqMcSet, true);
/// assert!(memcache_rewrite_query_with_timestamp_md(&mut m).is_ok());
/// ```
pub fn memcache_rewrite_query_with_timestamp_md(
    _orig: &mut Msg,
) -> Result<RepairOutcome, RepairError> {
    Ok(RepairOutcome::NoOp)
}

/// Build a repair query for a Memcached response set. The reference
/// engine returns `DN_OK` without producing a repair query. The
/// Rust port returns [`RepairOutcome::NoOp`].
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType, ResponseMgr};
/// use dynomite::proto::memcache::memcache_make_repair_query;
///
/// let req = Msg::new(0, MsgType::ReqMcGet, true);
/// let mgr = ResponseMgr::new(&req, 1, None);
/// assert!(memcache_make_repair_query(&mgr).is_ok());
/// ```
pub fn memcache_make_repair_query(_rspmgr: &ResponseMgr) -> Result<RepairOutcome, RepairError> {
    Ok(RepairOutcome::NoOp)
}

/// Clear repair metadata for a key. The reference engine returns
/// `DN_OK` without producing a cleanup message. The Rust port
/// matches.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::memcache::memcache_clear_repair_md_for_key;
///
/// let mut req = Msg::new(0, MsgType::ReqMcSet, true);
/// assert!(memcache_clear_repair_md_for_key(&mut req).is_ok());
/// ```
pub fn memcache_clear_repair_md_for_key(_req: &mut Msg) -> Result<RepairOutcome, RepairError> {
    Ok(RepairOutcome::NoOp)
}

/// Reconcile responses across replicas. The reference engine picks
/// the first response under `DC_QUORUM` and otherwise returns an
/// error response. The Rust port reproduces both arms by reporting
/// the chosen index plus an optional fresh error response.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{ConsistencyLevel, Msg, MsgType, ResponseMgr};
/// use dynomite::proto::memcache::memcache_reconcile_responses;
///
/// let mut req = Msg::new(0, MsgType::ReqMcGet, true);
/// req.set_consistency(ConsistencyLevel::DcQuorum);
/// let mut mgr = ResponseMgr::new(&req, 3, None);
/// // Submit one response (Stage 9 plumbing supplies the responses
/// // in real use).
/// let outcome = memcache_reconcile_responses(&mgr, ConsistencyLevel::DcQuorum);
/// matches!(outcome, dynomite::proto::memcache::repair::ReconcileOutcome::PickFirst);
/// ```
pub fn memcache_reconcile_responses(
    _rspmgr: &ResponseMgr,
    consistency: ConsistencyLevel,
) -> ReconcileOutcome {
    match consistency {
        ConsistencyLevel::DcQuorum => ReconcileOutcome::PickFirst,
        _ => ReconcileOutcome::Error(DynErrorCode::DynomiteNoQuorumAchieved),
    }
}

/// Outcome of [`memcache_reconcile_responses`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReconcileOutcome {
    /// Return the first response in the response manager.
    PickFirst,
    /// Allocate and return a fresh error response with the given
    /// error code.
    Error(DynErrorCode),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_is_noop() {
        let mut m = Msg::new(0, MsgType::ReqMcSet, true);
        let outcome = memcache_rewrite_query(&mut m).unwrap();
        assert!(matches!(outcome, RepairOutcome::NoOp));
    }

    #[test]
    fn reconcile_quorum_picks_first() {
        let req = Msg::new(0, MsgType::ReqMcGet, true);
        let mgr = ResponseMgr::new(&req, 3, None);
        let outcome = memcache_reconcile_responses(&mgr, ConsistencyLevel::DcQuorum);
        assert_eq!(outcome, ReconcileOutcome::PickFirst);
    }

    #[test]
    fn reconcile_safe_quorum_emits_error() {
        let req = Msg::new(0, MsgType::ReqMcGet, true);
        let mgr = ResponseMgr::new(&req, 3, None);
        let outcome = memcache_reconcile_responses(&mgr, ConsistencyLevel::DcSafeQuorum);
        assert_eq!(
            outcome,
            ReconcileOutcome::Error(DynErrorCode::DynomiteNoQuorumAchieved),
        );
    }
}
