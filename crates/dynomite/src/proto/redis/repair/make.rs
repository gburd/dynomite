//! Repair-message construction.
//!
//! The full implementation depends on per-response argument parsing
//! that the dispatcher would supply; the data-shape side
//! (read-repair toggle, repairable-command catalog) is in place so
//! the rest of the engine can check eligibility.

use crate::msg::{is_read_repairs_enabled, MsgType, ResponseMgr};

use super::{RepairError, RepairOutcome};

/// True when the request type is flagged as "repairable".
#[must_use]
pub fn is_repairable(ty: MsgType) -> bool {
    matches!(
        ty,
        MsgType::ReqRedisGet
            | MsgType::ReqRedisHget
            | MsgType::ReqRedisSismember
            | MsgType::ReqRedisZscore
    )
}

/// Build a repair message for the response set held in `rspmgr`.
///
/// Returns [`RepairOutcome::NoOp`] when read repairs are disabled,
/// when the originating request is not repairable, or when no
/// outdated response was found. Returns
/// [`RepairOutcome::Rewritten`] with a freshly built repair message
/// otherwise.
///
/// The "build a repair message" arm depends on each response's
/// post-parsed argument list, which the dispatcher would populate
/// on every response mbuf. That argument parsing is not yet wired,
/// so this function reports [`RepairOutcome::NoOp`] for every
/// repairable command; the eligibility check is in place.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType, ResponseMgr};
/// use dynomite::proto::redis::redis_make_repair_query;
///
/// let req = Msg::new(0, MsgType::ReqRedisGet, true);
/// let mgr = ResponseMgr::new(&req, 3, None);
/// let outcome = redis_make_repair_query(&mgr).unwrap();
/// matches!(outcome, dynomite::proto::redis::RepairOutcome::NoOp);
/// ```
pub fn redis_make_repair_query(rspmgr: &ResponseMgr) -> Result<RepairOutcome, RepairError> {
    if !is_read_repairs_enabled() {
        return Ok(RepairOutcome::NoOp);
    }
    let ty = rspmgr.msg_type();
    if !is_repairable(ty) {
        return Ok(RepairOutcome::NoOp);
    }
    // Reaching here means the cluster layer has at least one
    // response. The actual build-step decodes per-response
    // timestamps and produces a write that updates outdated
    // replicas; that arm needs per-response argument parsing,
    // which is not yet wired. Until then we report no-op so
    // the caller falls back to checksum-based repair.
    Ok(RepairOutcome::NoOp)
}
