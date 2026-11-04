//! Redis read-repair plumbing.
//!
//! Read repair adapts mismatched replica responses into a single
//! authoritative answer and, where supported, schedules a write
//! to bring stragglers back into sync. The cluster layer calls
//! into this module through the following surface:
//!
//! * [`rewrite::redis_rewrite_query`] - command-level rewrites
//!   (the `SMEMBERS -> SORT ALPHA` case under DC_SAFE_QUORUM).
//! * [`request`] - a write or repairable read becomes a repair-script
//!   request carrying the coordinator's timestamp
//!   ([`request::ScriptRequest::from_request`]); replicas run it on their
//!   datastore connection.
//! * [`make`] - pick the freshest timestamped read reply
//!   ([`make::find_most_updated`]) and build the write that repairs the
//!   stale replicas ([`make::repair_request`]).
//!   After every replica has answered a delete, the coordinator sends the
//!   metadata cleanup script for it.
//! * [`logic`] - the timestamp bookkeeping each repair script
//!   performs, evaluated by the engine over a [`logic::RepairStore`].
//! * [`reconcile::redis_reconcile_responses`] - pick a single
//!   response from the per-replica set, optionally producing a
//!   read-repair side effect.
//!
//! The reference script texts the logic is checked against live as
//! constants in [`scripts`].

pub mod logic;
pub mod make;
pub mod reconcile;
pub mod request;
pub mod rewrite;
pub mod scripts;

pub use self::reconcile::redis_reconcile_responses;
pub use self::rewrite::redis_rewrite_query;

use crate::msg::Msg;

/// Outcome of a repair-surface call.
#[derive(Debug)]
pub enum RepairOutcome {
    /// No rewrite or repair message was produced.
    NoOp,
    /// A rewritten message replaces the original request.
    Rewritten(Box<Msg>),
}

/// Errors the Redis repair surface can raise.
#[derive(Copy, Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[non_exhaustive]
pub enum RepairError {
    /// Memory could not be allocated for the rewritten message.
    #[error("redis repair: out of memory")]
    OutOfMemory,
    /// Internal invariant violated while building the repair
    /// message (for example, missing originating message).
    #[error("redis repair: invariant violation: {0}")]
    Invariant(&'static str),
    /// The repair payload would not fit the configured per-message
    /// budget.
    #[error("redis repair: payload too large")]
    PayloadTooLarge,
}
