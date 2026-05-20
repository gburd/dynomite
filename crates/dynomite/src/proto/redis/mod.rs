//! Redis (RESP) protocol parser, helpers, and repair plumbing.
//!
//! The Redis datastore exposes the unified RESP wire format. The
//! engine consumes it with two byte-driven state machines, one for
//! requests ([`parser::redis_parse_req`]) and one for responses
//! ([`parser::redis_parse_rsp`]). Surrounding helpers cover command
//! classification, multi-key fragmentation, response coalescing,
//! request verification, and the read-repair surface.
//!
//! # Examples
//!
//! ```
//! use dynomite::msg::{Msg, MsgParseResult, MsgType};
//! use dynomite::proto::redis;
//!
//! let mut req = Msg::new(0, MsgType::Unknown, true);
//! let r = redis::redis_parse_req(&mut req, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
//! assert_eq!(r, MsgParseResult::Ok);
//! assert_eq!(req.ty(), MsgType::ReqRedisGet);
//! assert_eq!(req.keys()[0].key(), b"foo");
//! ```

pub mod coalesce;
pub mod commands;
pub mod fragment;
pub mod multikey;
pub mod parser;
pub mod repair;
pub mod verify;

pub use self::coalesce::{accumulate_fragment_integer, redis_post_coalesce, redis_pre_coalesce};
pub use self::commands::CommandClass;
pub use self::fragment::{redis_fragment, FragmentDispatcher, FragmentOutcome};
pub use self::multikey::redis_is_multikey_request;
pub use self::parser::{redis_parse_req, redis_parse_rsp};
pub use self::repair::{
    redis_clear_repair_md_for_key, redis_make_repair_query, redis_reconcile_responses,
    redis_rewrite_query, redis_rewrite_query_with_timestamp_md, RepairError, RepairOutcome,
};
pub use self::verify::{redis_verify_request, VerifyError};
