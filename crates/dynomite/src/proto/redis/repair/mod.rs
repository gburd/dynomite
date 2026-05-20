//! Redis read-repair plumbing.
//!
//! Read repair adapts mismatched replica responses into a single
//! authoritative answer and, where supported, schedules a write
//! to bring stragglers back into sync. The cluster layer calls
//! into this module through the following surface:
//!
//! * [`rewrite::redis_rewrite_query`] - command-level rewrites
//!   (the `SMEMBERS -> SORT ALPHA` case under DC_SAFE_QUORUM).
//! * [`rewrite::redis_rewrite_query_with_timestamp_md`] - rewrite
//!   a write request into a Lua script that records timestamps.
//! * [`make::redis_make_repair_query`] - build a repair message
//!   when responses disagree.
//! * [`clear::redis_clear_repair_md_for_key`] - emit a metadata
//!   cleanup script after a delete completes.
//! * [`reconcile::redis_reconcile_responses`] - pick a single
//!   response from the per-replica set, optionally producing a
//!   read-repair side effect.
//!
//! The Lua templates the rewrite path renders into outgoing
//! requests live as compile-time constants in [`scripts`].

pub mod clear;
pub mod make;
pub mod reconcile;
pub mod rewrite;
pub mod scripts;

pub use self::clear::redis_clear_repair_md_for_key;
pub use self::make::redis_make_repair_query;
pub use self::reconcile::redis_reconcile_responses;
pub use self::rewrite::{redis_rewrite_query, redis_rewrite_query_with_timestamp_md};

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
