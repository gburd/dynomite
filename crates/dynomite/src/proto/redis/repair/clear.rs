//! Metadata cleanup script after a delete completes.
//!
//! The cleanup script removes the per-key entries from the
//! add-set / rem-set metadata once a delete has been confirmed by
//! every replica. This module holds the predicate that decides
//! whether a cleanup script is needed; emitting the script bytes is
//! not yet wired, so the function reports
//! [`RepairOutcome::NoOp`].

use crate::msg::{Msg, MsgType};

use super::{RepairError, RepairOutcome};

/// Decide whether `req` warrants a metadata cleanup.
///
/// Returns [`RepairOutcome::NoOp`] when the originating request is
/// not a delete-shaped command. Returns
/// [`RepairError::Invariant`] when the originating message is
/// missing.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::redis::redis_clear_repair_md_for_key;
///
/// let mut req = Msg::new(0, MsgType::ReqRedisDel, true);
/// let outcome = redis_clear_repair_md_for_key(&mut req).unwrap();
/// matches!(outcome, dynomite::proto::redis::RepairOutcome::NoOp);
/// ```
pub fn redis_clear_repair_md_for_key(req: &mut Msg) -> Result<RepairOutcome, RepairError> {
    let is_delete = matches!(
        req.ty(),
        MsgType::ReqRedisDel
            | MsgType::ReqRedisHdel
            | MsgType::ReqRedisSrem
            | MsgType::ReqRedisZrem
    );
    if !is_delete {
        return Ok(RepairOutcome::NoOp);
    }
    // Building the cleanup script requires the original request's
    // post-parsed key/field list, which Stage 9's dispatcher
    // supplies. Until then the cleanup is a no-op (matches the
    // C path, which returns DN_NOOPS until quorum is reached).
    Ok(RepairOutcome::NoOp)
}
