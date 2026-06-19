//! Memcached repair surface.
//!
//! Memcached responses are never rewritten, so every repair function
//! here returns [`RepairOutcome::NoOp`]. The one exception is
//! reconciliation, which returns the first response when consistency
//! is `DC_QUORUM` and an error response otherwise. The functions
//! exist so the cluster layer can call into either protocol through
//! the same trait set as the RESP surface.

#[cfg(test)]
use crate::msg::MsgType;
use crate::msg::{ConsistencyLevel, DynErrorCode, Msg, ResponseMgr};

/// Repair-surface result type. Matches the RESP repair return
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
/// live failure modes; the variant exists to mirror the RESP
/// surface.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RepairError {
    /// Reserved variant.
    #[error("memcache repair: reserved")]
    Reserved,
}

/// Memcached query rewrite. Memcached responses are not rewritten,
/// so this returns [`RepairOutcome::NoOp`].
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

/// Memcached query rewrite with timestamp metadata. Memcached
/// responses are not rewritten, so this returns
/// [`RepairOutcome::NoOp`].
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

/// Build a repair query for a Memcached response set. Memcached has
/// no repair query, so this returns [`RepairOutcome::NoOp`].
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

/// Clear repair metadata for a key. Memcached carries no repair
/// metadata, so this returns [`RepairOutcome::NoOp`].
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

/// Reconcile responses across replicas. Picks
/// the first response under `DC_QUORUM` and otherwise returns an
/// error response, reporting
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
/// // Submit one response (the connection layer supplies the
/// // responses in real use).
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
