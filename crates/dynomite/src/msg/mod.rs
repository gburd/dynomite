//! Message model: the in-memory shape every request and response
//! takes inside the engine, plus the queues, indices, and per-DC
//! response managers that thread them through the dispatcher.
//!
//! This module exposes the data layer; the connection-coupled
//! lifecycle (recv/send, timeout queues, peer forwarding) ships in
//! Stage 9 once the connection state machine is in place. Helpers
//! that already have a clean data-only definition (error response
//! construction, fragment bookkeeping, quorum decisions) live here.
//!
//! # Examples
//!
//! ```
//! use dynomite::msg::{Msg, MsgQueue, MsgType, ResponseMgr};
//!
//! let mut q = MsgQueue::new();
//! q.push_back(Msg::new(1, MsgType::ReqRedisGet, true));
//!
//! let req = q.front().unwrap();
//! let mgr = ResponseMgr::new(req, 1, None);
//! assert_eq!(mgr.quorum_responses(), 1);
//! ```

use std::sync::OnceLock;

pub mod index;
pub mod message;
pub mod msg_type;
pub mod queue;
pub mod request;
pub mod response;
pub mod response_mgr;

pub use self::index::MsgIndex;
pub use self::message::{ConnId, Msg, MsgFlags, MsgParseResult, MsgRouting};
pub use self::msg_type::MsgType;
pub use self::queue::MsgQueue;
pub use self::response_mgr::{QuorumOutcome, ResponseMgr, MAX_REPLICAS_PER_DC};

/// Cluster consistency level applied to a single message.
///
/// The numeric values match `consistency_t` in the C reference.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum ConsistencyLevel {
    /// Wait for one replica to ack.
    #[default]
    DcOne = 0,
    /// Wait for a per-DC majority.
    DcQuorum = 1,
    /// `DcQuorum` with body checksum agreement; mismatches trigger
    /// read repair.
    DcSafeQuorum = 2,
    /// `DcSafeQuorum` evaluated independently per datacenter.
    DcEachSafeQuorum = 3,
}

impl ConsistencyLevel {
    /// Stable string name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::ConsistencyLevel;
    /// assert_eq!(ConsistencyLevel::DcQuorum.name(), "DC_QUORUM");
    /// ```
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            ConsistencyLevel::DcOne => "DC_ONE",
            ConsistencyLevel::DcQuorum => "DC_QUORUM",
            ConsistencyLevel::DcSafeQuorum => "DC_SAFE_QUORUM",
            ConsistencyLevel::DcEachSafeQuorum => "DC_EACH_SAFE_QUORUM",
        }
    }

    /// Recover the level from its uppercase name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::ConsistencyLevel;
    /// assert_eq!(
    ///     ConsistencyLevel::from_name("DC_SAFE_QUORUM"),
    ///     Some(ConsistencyLevel::DcSafeQuorum),
    /// );
    /// ```
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "DC_ONE" => Some(Self::DcOne),
            "DC_QUORUM" => Some(Self::DcQuorum),
            "DC_SAFE_QUORUM" => Some(Self::DcSafeQuorum),
            "DC_EACH_SAFE_QUORUM" => Some(Self::DcEachSafeQuorum),
            _ => None,
        }
    }
}

/// Dynomite-side error code carried in a message envelope.
///
/// Matches `dyn_error_t` from the C reference verbatim.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum DynErrorCode {
    /// No error.
    #[default]
    Ok = 0,
    /// Unspecified error.
    DynomiteUnknownError = 1,
    /// Engine state forbids the request.
    DynomiteInvalidState = 2,
    /// Admin-only command issued in non-admin mode.
    DynomiteInvalidAdminReq = 3,
    /// Peer refused the connection.
    PeerConnectionRefuse = 4,
    /// Peer reachable but reported down.
    PeerHostDown = 5,
    /// Peer not yet connected.
    PeerHostNotConnected = 6,
    /// Datastore refused the connection.
    StorageConnectionRefuse = 7,
    /// Bad message framing.
    BadFormat = 8,
    /// Quorum not achieved.
    DynomiteNoQuorumAchieved = 9,
    /// Lua script keys span multiple nodes.
    DynomiteScriptSpansNodes = 10,
    /// Payload exceeds the configured limit.
    DynomitePayloadTooLarge = 11,
}

impl DynErrorCode {
    /// Human-readable label used in error responses, mirroring
    /// `dn_strerror` in the C reference.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::DynErrorCode;
    /// assert_eq!(DynErrorCode::PeerHostDown.message(), "Peer Node is down");
    /// ```
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            DynErrorCode::Ok => "Success",
            DynErrorCode::DynomiteUnknownError => "Unknown Error",
            DynErrorCode::DynomiteInvalidState => {
                "Dynomite's current state does not allow this request"
            }
            DynErrorCode::DynomiteInvalidAdminReq => "Invalid request in Dynomite's admin mode",
            DynErrorCode::PeerConnectionRefuse => "Peer Node refused connection",
            DynErrorCode::PeerHostDown => "Peer Node is down",
            DynErrorCode::PeerHostNotConnected => "Peer Node is not connected",
            DynErrorCode::StorageConnectionRefuse => "Datastore refused connection",
            DynErrorCode::BadFormat => "Bad message format",
            DynErrorCode::DynomiteNoQuorumAchieved => "Failed to achieve Quorum",
            DynErrorCode::DynomiteScriptSpansNodes => {
                "Keys in the script cannot span multiple nodes"
            }
            DynErrorCode::DynomitePayloadTooLarge => "MSET/MGET/SCAN payload too large",
        }
    }

    /// Origin label (`Dynomite:`, `Peer:`, `Storage:`, `unknown:`)
    /// used in error response prefixes, mirroring
    /// `dyn_error_source`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::DynErrorCode;
    /// assert_eq!(DynErrorCode::PeerHostDown.source(), "Peer:");
    /// ```
    #[must_use]
    pub fn source(self) -> &'static str {
        match self {
            DynErrorCode::DynomiteInvalidAdminReq
            | DynErrorCode::DynomiteInvalidState
            | DynErrorCode::DynomiteNoQuorumAchieved
            | DynErrorCode::DynomiteScriptSpansNodes
            | DynErrorCode::DynomitePayloadTooLarge => "Dynomite:",
            DynErrorCode::PeerConnectionRefuse
            | DynErrorCode::PeerHostDown
            | DynErrorCode::PeerHostNotConnected => "Peer:",
            DynErrorCode::StorageConnectionRefuse => "Storage:",
            _ => "unknown:",
        }
    }
}

static READ_REPAIRS_ENABLED: OnceLock<bool> = OnceLock::new();

/// Configure whether read repairs are globally enabled.
///
/// Called once during configuration validation; subsequent calls are
/// silently ignored to mirror the reference engine's read-only
/// `g_read_repairs_enabled` global. Returns `true` when the value was
/// installed and `false` when an earlier call already pinned it.
///
/// # Examples
///
/// ```
/// use dynomite::msg::set_read_repairs_enabled;
/// // Calling twice from a single test wins or loses depending on
/// // whether anyone else got there first; the API is idempotent.
/// let _ = set_read_repairs_enabled(true);
/// ```
pub fn set_read_repairs_enabled(enabled: bool) -> bool {
    READ_REPAIRS_ENABLED.set(enabled).is_ok()
}

/// True when read repairs are enabled cluster-wide.
///
/// Defaults to `false`, matching the reference engine's initial
/// value of `g_read_repairs_enabled`.
///
/// # Examples
///
/// ```
/// use dynomite::msg::is_read_repairs_enabled;
/// // Default state is "disabled".
/// let _ = is_read_repairs_enabled();
/// ```
#[must_use]
pub fn is_read_repairs_enabled() -> bool {
    *READ_REPAIRS_ENABLED.get().unwrap_or(&false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistency_round_trip() {
        for level in [
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcQuorum,
            ConsistencyLevel::DcSafeQuorum,
            ConsistencyLevel::DcEachSafeQuorum,
        ] {
            assert_eq!(ConsistencyLevel::from_name(level.name()), Some(level));
        }
        assert!(ConsistencyLevel::from_name("DC_BOGUS").is_none());
    }

    #[test]
    fn dyn_error_code_strings_match_c() {
        assert_eq!(DynErrorCode::Ok.message(), "Success");
        assert_eq!(DynErrorCode::PeerHostDown.source(), "Peer:");
        assert_eq!(DynErrorCode::DynomiteUnknownError.source(), "unknown:");
        assert_eq!(DynErrorCode::StorageConnectionRefuse.source(), "Storage:");
    }
}
