//! Memcached text-protocol parser, helpers, and repair stubs.
//!
//! The Memcached datastore exposes a small ASCII command grammar. The
//! engine consumes it with a single byte-driven state machine for
//! requests ([`parser::memcache_parse_req`]) and another for
//! responses ([`parser::memcache_parse_rsp`]). Surrounding helpers
//! cover command classification, multi-key fragmentation, response
//! coalescing, and the placeholder repair surface.
//!
//! # Examples
//!
//! ```\n//! use dynomite::msg::{Msg, MsgType};\n//! use dynomite::proto::memcache;\n//!\n//! let mut req = Msg::new(0, MsgType::Unknown, true);\n//! let r = memcache::memcache_parse_req(&mut req, b\"get user:42\\r\\n\");\n//! assert_eq!(r, dynomite::msg::MsgParseResult::Ok);\n//! assert_eq!(req.ty(), MsgType::ReqMcGet);\n//! assert_eq!(req.keys()[0].key(), b\"user:42\");\n//! ```

pub mod coalesce;
pub mod commands;
pub mod fragment;
pub mod multikey;
pub mod parser;
pub mod repair;
pub mod verify;

pub use self::coalesce::{memcache_post_coalesce, memcache_pre_coalesce};
pub use self::commands::{
    memcache_arithmetic, memcache_cas, memcache_delete, memcache_retrieval, memcache_storage,
    memcache_touch,
};
pub use self::fragment::{memcache_fragment, FragmentDispatcher, FragmentOutcome};
pub use self::multikey::memcache_is_multikey_request;
pub use self::parser::{memcache_parse_req, memcache_parse_rsp};
pub use self::repair::{
    memcache_clear_repair_md_for_key, memcache_make_repair_query, memcache_reconcile_responses,
    memcache_rewrite_query, memcache_rewrite_query_with_timestamp_md,
};
pub use self::verify::memcache_verify_request;
