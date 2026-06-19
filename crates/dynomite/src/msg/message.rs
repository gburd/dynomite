//! In-memory representation of a single Dynomite message.
//!
//! [`Msg`] is the unit that flows through the engine: a request from
//! a client connection, a response from the upstream datastore, or
//! an internal control packet. It carries the parsed metadata, the
//! mbuf chain holding the on-the-wire bytes, the parser state used
//! by the protocol decoders, and the bookkeeping flags every layer
//! sets and reads.
//!
//! This module builds the message data shape and exposes the
//! field-level accessors. The connection-coupled lifecycle paths
//! (timeout tracking, queue threading, parser dispatch) live in
//! [`crate::net`].

use crate::core::types::MsgId;

use super::keypos::{ArgPos, KeyPos};
use super::msg_type::MsgType;
use super::response_mgr::ResponseMgr;
use crate::io::mbuf::MbufQueue;
use crate::proto::dnode::{Dmsg, DynParseState};

/// Stable connection identifier carried by [`Msg::owner`].
///
/// A unique 64-bit tag the connection layer stamps on every message
/// it produces.
pub type ConnId = u64;

/// Parser outcome reported by datastore protocol decoders.
///
/// The variants name the possible parse outcomes
/// so downstream callers can dispatch on the same semantics.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
pub enum MsgParseResult {
    /// Parsing completed for this message.
    #[default]
    Ok,
    /// Parser detected unrecoverable framing error.
    Error,
    /// Parser consumed valid bytes but expects to be re-driven on
    /// the trailing bytes after a buffer split.
    Repair,
    /// Multi-key request needs to be fragmented before forwarding.
    Fragment,
    /// Need more bytes; caller must read more before retrying.
    Again,
    /// Parsing succeeded; downstream layer should take no action.
    Noop,
    /// Message was a Dynomite configuration directive.
    DynoConfig,
    /// Out-of-memory while parsing.
    OomError,
}

/// Routing override applied to a request.
///
/// The message routing mode. The default
/// (`Normal`) honors the configured key-hash routing; the other
/// variants short-circuit it for diagnostic and special-purpose
/// paths.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default)]
#[repr(u8)]
pub enum MsgRouting {
    /// Apply the standard key-hash routing.
    #[default]
    Normal = 0,
    /// Send to the local node only, ignoring the key hash.
    LocalNodeOnly = 1,
    /// Apply the key hash but stay within the local rack.
    TokenOwnerLocalRackOnly = 2,
    /// Send to every node in the local rack, ignoring the key hash.
    AllNodesLocalRackOnly = 3,
    /// Send to every node in every rack of every datacenter.
    AllNodesAllRacksAllDcs = 4,
}

impl MsgRouting {
    /// Stable string name for diagnostic output.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::MsgRouting;
    /// assert_eq!(MsgRouting::Normal.name(), "ROUTING_NORMAL");
    /// ```
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            MsgRouting::Normal => "ROUTING_NORMAL",
            MsgRouting::LocalNodeOnly => "ROUTING_LOCAL_NODE_ONLY",
            MsgRouting::TokenOwnerLocalRackOnly => "ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY",
            MsgRouting::AllNodesLocalRackOnly => "ROUTING_ALL_NODES_LOCAL_RACK_ONLY",
            MsgRouting::AllNodesAllRacksAllDcs => "ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS",
        }
    }
}

/// Bag of boolean lifecycle flags used by the request and response
/// pipelines.
#[allow(clippy::struct_excessive_bools)]
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct MsgFlags {
    /// Message is currently in the error state.
    pub is_error: bool,
    /// At least one fragment of this multi-message vector errored.
    pub is_ferror: bool,
    /// Caller issued a `quit` request.
    pub quit: bool,
    /// Caller expects the datastore to produce a reply.
    pub expect_datastore_reply: bool,
    /// Reply has been routed to the caller.
    pub done: bool,
    /// Every fragment of this vector finished.
    pub fdone: bool,
    /// Discard the corresponding response on arrival.
    pub swallow: bool,
    /// A DNODE header has already been prepended to the mbuf chain.
    pub dnode_header_prepended: bool,
    /// Response for this request has been written to the wire.
    pub rsp_sent: bool,
    /// Read request (vs write).
    pub is_read: bool,
    /// Marked by the dispatcher that a repair must be issued.
    pub needs_repair: bool,
    /// Request body can be safely rewritten with a timestamp.
    pub rewrite_with_ts_possible: bool,
    /// Response managers have been initialised.
    pub rspmgrs_inited: bool,
}

impl MsgFlags {
    /// Construct flags with the same default state the reference
    /// engine sets in `_msg_get`.
    #[must_use]
    fn default_for_msg() -> Self {
        Self {
            expect_datastore_reply: true,
            is_read: true,
            rewrite_with_ts_possible: true,
            ..Self::default()
        }
    }
}

/// One Dynomite message: the in-memory representation of a request
/// or a response on its way through the engine.
#[derive(Debug)]
pub struct Msg {
    id: MsgId,
    parent_id: MsgId,
    ty: MsgType,
    orig_type: MsgType,
    is_request: bool,
    mbufs: MbufQueue,
    mlen: u32,
    parse_result: MsgParseResult,
    dyn_parse_state: DynParseState,
    dmsg: Option<Dmsg>,
    routing: MsgRouting,
    consistency: super::ConsistencyLevel,
    timestamp_us: u64,
    error_code: i32,
    dyn_error_code: super::DynErrorCode,
    awaiting_rsps: u32,
    fragment_ids: Vec<MsgId>,
    response_ids: Vec<MsgId>,
    selected_rsp: Option<MsgId>,
    owner: Option<ConnId>,
    flags: MsgFlags,
    rspmgr: Option<ResponseMgr>,
    additional_rspmgrs: Vec<ResponseMgr>,
    parser_state: u32,
    parser_pos: usize,
    parser_token: Option<usize>,
    rlen: u32,
    rntokens: u32,
    ntokens: u32,
    nkeys: u32,
    vlen: u32,
    integer: i64,
    keys: Vec<KeyPos>,
    args: Vec<ArgPos>,
    end_marker: Option<usize>,
    ntoken_start: Option<usize>,
    ntoken_end: Option<usize>,
    frag_id: u64,
}

impl Msg {
    /// Construct a new message with `id`, type tag `ty`, and the
    /// request/response orientation `is_request`. The mbuf chain
    /// starts empty and the parser is reset to `DynParseState::Start`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// let m = Msg::new(1, MsgType::ReqRedisGet, true);
    /// assert_eq!(m.id(), 1);
    /// assert!(m.is_request());
    /// ```
    #[must_use]
    pub fn new(id: MsgId, ty: MsgType, is_request: bool) -> Self {
        Self {
            id,
            parent_id: 0,
            ty,
            orig_type: MsgType::Unknown,
            is_request,
            mbufs: MbufQueue::new(),
            mlen: 0,
            parse_result: MsgParseResult::default(),
            dyn_parse_state: DynParseState::Start,
            dmsg: None,
            routing: MsgRouting::Normal,
            consistency: super::ConsistencyLevel::DcOne,
            timestamp_us: 0,
            error_code: 0,
            dyn_error_code: super::DynErrorCode::Ok,
            awaiting_rsps: 0,
            fragment_ids: Vec::new(),
            response_ids: Vec::new(),
            selected_rsp: None,
            owner: None,
            flags: MsgFlags::default_for_msg(),
            rspmgr: None,
            additional_rspmgrs: Vec::new(),
            parser_state: 0,
            parser_pos: 0,
            parser_token: None,
            rlen: 0,
            rntokens: 0,
            ntokens: 0,
            nkeys: 0,
            vlen: 0,
            integer: 0,
            keys: Vec::new(),
            args: Vec::new(),
            end_marker: None,
            ntoken_start: None,
            ntoken_end: None,
            frag_id: 0,
        }
    }

    /// Borrow the parsed key list. Populated by the protocol parsers.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert!(Msg::new(1, MsgType::Unknown, true).keys().is_empty());
    /// ```
    #[must_use]
    pub fn keys(&self) -> &[KeyPos] {
        &self.keys
    }

    /// Mutably borrow the parsed key list.
    pub fn keys_mut(&mut self) -> &mut Vec<KeyPos> {
        &mut self.keys
    }

    /// Append a parsed key. Used by the protocol parsers.
    pub fn push_key(&mut self, k: KeyPos) {
        self.keys.push(k);
    }

    /// Borrow the parsed argument list.
    #[must_use]
    pub fn args(&self) -> &[ArgPos] {
        &self.args
    }

    /// Mutably borrow the parsed argument list.
    pub fn args_mut(&mut self) -> &mut Vec<ArgPos> {
        &mut self.args
    }

    /// Append a parsed argument.
    pub fn push_arg(&mut self, a: ArgPos) {
        self.args.push(a);
    }

    /// Protocol-specific parser state index. Each parser defines
    /// its own state alphabet keyed on this `u32`.
    #[must_use]
    pub fn parser_state(&self) -> u32 {
        self.parser_state
    }

    /// Set the protocol parser state index.
    pub fn set_parser_state(&mut self, s: u32) {
        self.parser_state = s;
    }

    /// Cursor offset into the input buffer where the next byte
    /// should be read.
    #[must_use]
    pub fn parser_pos(&self) -> usize {
        self.parser_pos
    }

    /// Set the parser cursor offset.
    pub fn set_parser_pos(&mut self, p: usize) {
        self.parser_pos = p;
    }

    /// Optional token marker offset.
    #[must_use]
    pub fn parser_token(&self) -> Option<usize> {
        self.parser_token
    }

    /// Set the optional token marker offset.
    pub fn set_parser_token(&mut self, t: Option<usize>) {
        self.parser_token = t;
    }

    /// Remaining length of the bulk argument the parser is currently
    /// consuming.
    #[must_use]
    pub fn rlen(&self) -> u32 {
        self.rlen
    }

    /// Set the bulk-argument remaining length.
    pub fn set_rlen(&mut self, n: u32) {
        self.rlen = n;
    }

    /// Remaining unprocessed token count for the current parse.
    #[must_use]
    pub fn rntokens(&self) -> u32 {
        self.rntokens
    }

    /// Set the remaining-token counter.
    pub fn set_rntokens(&mut self, n: u32) {
        self.rntokens = n;
    }

    /// Total parsed token count for the current message.
    #[must_use]
    pub fn ntokens(&self) -> u32 {
        self.ntokens
    }

    /// Set the total parsed token count.
    pub fn set_ntokens(&mut self, n: u32) {
        self.ntokens = n;
    }

    /// Number of keys the script (EVAL/EVALSHA) declared.
    #[must_use]
    pub fn nkeys(&self) -> u32 {
        self.nkeys
    }

    /// Set the script-key count.
    pub fn set_nkeys(&mut self, n: u32) {
        self.nkeys = n;
    }

    /// Storage-command value length.
    #[must_use]
    pub fn vlen(&self) -> u32 {
        self.vlen
    }

    /// Set the storage-command value length.
    pub fn set_vlen(&mut self, n: u32) {
        self.vlen = n;
    }

    /// Integer value carried by the response (`:n\r\n`).
    #[must_use]
    pub fn integer(&self) -> i64 {
        self.integer
    }

    /// Set the integer response value.
    pub fn set_integer(&mut self, v: i64) {
        self.integer = v;
    }

    /// Offset of the multi-bulk `END` marker in the response, if any.
    #[must_use]
    pub fn end_marker(&self) -> Option<usize> {
        self.end_marker
    }

    /// Set the response `END` marker offset.
    pub fn set_end_marker(&mut self, m: Option<usize>) {
        self.end_marker = m;
    }

    /// Start offset of the multi-bulk argument count token.
    #[must_use]
    pub fn ntoken_start(&self) -> Option<usize> {
        self.ntoken_start
    }

    /// End offset (exclusive) of the multi-bulk argument count token.
    #[must_use]
    pub fn ntoken_end(&self) -> Option<usize> {
        self.ntoken_end
    }

    /// Set the multi-bulk argument count span.
    pub fn set_ntoken_span(&mut self, start: Option<usize>, end: Option<usize>) {
        self.ntoken_start = start;
        self.ntoken_end = end;
    }

    /// Fragment id grouping all sub-messages produced from a
    /// multi-key request.
    #[must_use]
    pub fn frag_id(&self) -> u64 {
        self.frag_id
    }

    /// Set the fragment id.
    pub fn set_frag_id(&mut self, id: u64) {
        self.frag_id = id;
    }

    /// Message id.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert_eq!(Msg::new(99, MsgType::Unknown, true).id(), 99);
    /// ```
    #[must_use]
    pub fn id(&self) -> MsgId {
        self.id
    }

    /// Parent id (zero when not a fragment).
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert_eq!(Msg::new(1, MsgType::Unknown, true).parent_id(), 0);
    /// ```
    #[must_use]
    pub fn parent_id(&self) -> MsgId {
        self.parent_id
    }

    /// Set the parent id.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// let mut m = Msg::new(2, MsgType::Unknown, true);
    /// m.set_parent_id(1);
    /// assert_eq!(m.parent_id(), 1);
    /// ```
    pub fn set_parent_id(&mut self, parent: MsgId) {
        self.parent_id = parent;
    }

    /// Message type tag.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert_eq!(Msg::new(1, MsgType::ReqMcGet, true).ty(), MsgType::ReqMcGet);
    /// ```
    #[must_use]
    pub fn ty(&self) -> MsgType {
        self.ty
    }

    /// Override the message type. The previous value is preserved as
    /// the original type so query rewriters can recover it.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
    /// m.set_type(MsgType::ReqRedisSet);
    /// assert_eq!(m.ty(), MsgType::ReqRedisSet);
    /// assert_eq!(m.orig_type(), MsgType::ReqRedisGet);
    /// ```
    pub fn set_type(&mut self, ty: MsgType) {
        self.orig_type = self.ty;
        self.ty = ty;
    }

    /// Original message type before any rewrite.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert_eq!(
    ///     Msg::new(1, MsgType::ReqRedisGet, true).orig_type(),
    ///     MsgType::Unknown,
    /// );
    /// ```
    #[must_use]
    pub fn orig_type(&self) -> MsgType {
        self.orig_type
    }

    /// True for requests.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert!(Msg::new(1, MsgType::ReqMcGet, true).is_request());
    /// assert!(!Msg::new(1, MsgType::RspMcStored, false).is_request());
    /// ```
    #[must_use]
    pub fn is_request(&self) -> bool {
        self.is_request
    }

    /// Borrow the underlying mbuf chain.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// let m = Msg::new(1, MsgType::Unknown, true);
    /// assert!(m.mbufs().is_empty());
    /// ```
    #[must_use]
    pub fn mbufs(&self) -> &MbufQueue {
        &self.mbufs
    }

    /// Mutably borrow the mbuf chain.
    ///
    /// # Examples
    /// ```
    /// use dynomite::io::mbuf::MbufPool;
    /// use dynomite::msg::{Msg, MsgType};
    ///
    /// let pool = MbufPool::default();
    /// let mut m = Msg::new(1, MsgType::Unknown, true);
    /// m.mbufs_mut().push_back(pool.get());
    /// assert_eq!(m.mbufs().len(), 1);
    /// ```
    pub fn mbufs_mut(&mut self) -> &mut MbufQueue {
        &mut self.mbufs
    }

    /// Cumulative readable byte count of the chain (`mlen`).
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert_eq!(Msg::new(1, MsgType::Unknown, true).mlen(), 0);
    /// ```
    #[must_use]
    pub fn mlen(&self) -> u32 {
        self.mlen
    }

    /// Refresh `mlen` from the current chain.
    ///
    /// The parser updates the length as it consumes bytes; callers
    /// that mutate the chain directly call this to keep the cached
    /// length consistent with the actual chain content.
    ///
    /// # Examples
    /// ```
    /// use dynomite::io::mbuf::MbufPool;
    /// use dynomite::msg::{Msg, MsgType};
    ///
    /// let pool = MbufPool::default();
    /// let mut m = Msg::new(1, MsgType::Unknown, true);
    /// let mut buf = pool.get();
    /// buf.recv(b"hi");
    /// m.mbufs_mut().push_back(buf);
    /// m.recompute_mlen();
    /// assert_eq!(m.mlen(), 2);
    /// ```
    pub fn recompute_mlen(&mut self) {
        let total: usize = self.mbufs.iter().map(crate::io::mbuf::Mbuf::len).sum();
        self.mlen = u32::try_from(total).unwrap_or(u32::MAX);
    }

    /// Direct setter for `mlen`. Use [`Msg::recompute_mlen`] when the
    /// chain has been mutated; this entry point exists for parsers
    /// that adjust the value as they consume bytes.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// let mut m = Msg::new(1, MsgType::Unknown, true);
    /// m.set_mlen(123);
    /// assert_eq!(m.mlen(), 123);
    /// ```
    pub fn set_mlen(&mut self, mlen: u32) {
        self.mlen = mlen;
    }

    /// Last parse outcome.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgParseResult, MsgType};
    /// assert_eq!(
    ///     Msg::new(1, MsgType::Unknown, true).parse_result(),
    ///     MsgParseResult::Ok,
    /// );
    /// ```
    #[must_use]
    pub fn parse_result(&self) -> MsgParseResult {
        self.parse_result
    }

    /// Set the parse outcome.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgParseResult, MsgType};
    /// let mut m = Msg::new(1, MsgType::Unknown, true);
    /// m.set_parse_result(MsgParseResult::Again);
    /// assert_eq!(m.parse_result(), MsgParseResult::Again);
    /// ```
    pub fn set_parse_result(&mut self, r: MsgParseResult) {
        self.parse_result = r;
    }

    /// Current DNODE parser state.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// use dynomite::proto::dnode::DynParseState;
    /// assert_eq!(
    ///     Msg::new(1, MsgType::Unknown, true).dyn_parse_state(),
    ///     DynParseState::Start,
    /// );
    /// ```
    #[must_use]
    pub fn dyn_parse_state(&self) -> DynParseState {
        self.dyn_parse_state
    }

    /// Set the DNODE parser state.
    pub fn set_dyn_parse_state(&mut self, state: DynParseState) {
        self.dyn_parse_state = state;
    }

    /// Borrow the parsed DNODE header, if any.
    ///
    /// # Examples
    /// ```
    /// use dynomite::msg::{Msg, MsgType};
    /// assert!(Msg::new(1, MsgType::Unknown, true).dmsg().is_none());
    /// ```
    #[must_use]
    pub fn dmsg(&self) -> Option<&Dmsg> {
        self.dmsg.as_ref()
    }

    /// Mutably borrow the parsed DNODE header.
    pub fn dmsg_mut(&mut self) -> Option<&mut Dmsg> {
        self.dmsg.as_mut()
    }

    /// Attach a parsed DNODE header.
    pub fn set_dmsg(&mut self, dmsg: Dmsg) {
        self.dmsg = Some(dmsg);
    }

    /// Routing override.
    #[must_use]
    pub fn routing(&self) -> MsgRouting {
        self.routing
    }

    /// Set the routing override.
    pub fn set_routing(&mut self, routing: MsgRouting) {
        self.routing = routing;
    }

    /// Consistency level for this message.
    #[must_use]
    pub fn consistency(&self) -> super::ConsistencyLevel {
        self.consistency
    }

    /// Set the consistency level.
    pub fn set_consistency(&mut self, level: super::ConsistencyLevel) {
        self.consistency = level;
    }

    /// Microsecond timestamp recorded at request creation.
    #[must_use]
    pub fn timestamp_us(&self) -> u64 {
        self.timestamp_us
    }

    /// Update the request timestamp.
    pub fn set_timestamp_us(&mut self, ts: u64) {
        self.timestamp_us = ts;
    }

    /// Datastore-level error code (`errno`-shaped).
    #[must_use]
    pub fn error_code(&self) -> i32 {
        self.error_code
    }

    /// Set the datastore error code.
    pub fn set_error_code(&mut self, e: i32) {
        self.error_code = e;
    }

    /// Dynomite-level error code.
    #[must_use]
    pub fn dyn_error_code(&self) -> super::DynErrorCode {
        self.dyn_error_code
    }

    /// Set the Dynomite error code.
    pub fn set_dyn_error_code(&mut self, e: super::DynErrorCode) {
        self.dyn_error_code = e;
    }

    /// Number of replies the request is still waiting on.
    #[must_use]
    pub fn awaiting_rsps(&self) -> u32 {
        self.awaiting_rsps
    }

    /// Increment `awaiting_rsps`.
    pub fn incr_awaiting_rsps(&mut self) {
        self.awaiting_rsps = self.awaiting_rsps.saturating_add(1);
    }

    /// Decrement `awaiting_rsps`.
    pub fn decr_awaiting_rsps(&mut self) {
        self.awaiting_rsps = self.awaiting_rsps.saturating_sub(1);
    }

    /// Set `awaiting_rsps` directly. Used by the response manager
    /// initialiser to seed the per-DC count.
    pub fn set_awaiting_rsps(&mut self, n: u32) {
        self.awaiting_rsps = n;
    }

    /// Borrow the fragment-id list.
    #[must_use]
    pub fn fragment_ids(&self) -> &[MsgId] {
        &self.fragment_ids
    }

    /// Append `id` to the fragment-id list.
    pub fn push_fragment_id(&mut self, id: MsgId) {
        self.fragment_ids.push(id);
    }

    /// Borrow the response-id list.
    #[must_use]
    pub fn response_ids(&self) -> &[MsgId] {
        &self.response_ids
    }

    /// Append `id` to the response-id list.
    pub fn push_response_id(&mut self, id: MsgId) {
        self.response_ids.push(id);
    }

    /// Currently-selected response id.
    #[must_use]
    pub fn selected_rsp(&self) -> Option<MsgId> {
        self.selected_rsp
    }

    /// Set the currently-selected response id.
    pub fn set_selected_rsp(&mut self, id: Option<MsgId>) {
        self.selected_rsp = id;
    }

    /// Owner connection id, when the message is bound to one.
    #[must_use]
    pub fn owner(&self) -> Option<ConnId> {
        self.owner
    }

    /// Set the owner connection id.
    pub fn set_owner(&mut self, owner: Option<ConnId>) {
        self.owner = owner;
    }

    /// Borrow the lifecycle flags.
    #[must_use]
    pub fn flags(&self) -> &MsgFlags {
        &self.flags
    }

    /// Mutably borrow the lifecycle flags.
    pub fn flags_mut(&mut self) -> &mut MsgFlags {
        &mut self.flags
    }

    /// Set the `swallow` flag.
    pub fn set_swallow(&mut self, on: bool) {
        self.flags.swallow = on;
    }

    /// Set the `done` flag.
    pub fn set_done(&mut self, on: bool) {
        self.flags.done = on;
    }

    /// Set the `is_error` flag.
    pub fn set_is_error(&mut self, on: bool) {
        self.flags.is_error = on;
    }

    /// Borrow the local-DC response manager.
    #[must_use]
    pub fn rspmgr(&self) -> Option<&ResponseMgr> {
        self.rspmgr.as_ref()
    }

    /// Mutably borrow the local-DC response manager.
    pub fn rspmgr_mut(&mut self) -> Option<&mut ResponseMgr> {
        self.rspmgr.as_mut()
    }

    /// Install a fresh response manager for the local DC.
    pub fn set_rspmgr(&mut self, mgr: ResponseMgr) {
        self.flags.rspmgrs_inited = true;
        self.set_awaiting_rsps(u32::from(mgr.max_responses()));
        self.rspmgr = Some(mgr);
    }

    /// Borrow the per-remote-DC response managers.
    #[must_use]
    pub fn additional_rspmgrs(&self) -> &[ResponseMgr] {
        &self.additional_rspmgrs
    }

    /// Mutably borrow the per-remote-DC response managers.
    pub fn additional_rspmgrs_mut(&mut self) -> &mut Vec<ResponseMgr> {
        &mut self.additional_rspmgrs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_reference() {
        let m = Msg::new(1, MsgType::ReqRedisGet, true);
        assert!(m.flags().expect_datastore_reply);
        assert!(m.flags().is_read);
        assert!(m.flags().rewrite_with_ts_possible);
        assert!(!m.flags().is_error);
        assert!(!m.flags().rspmgrs_inited);
        assert_eq!(m.consistency(), super::super::ConsistencyLevel::DcOne);
        assert_eq!(m.routing(), MsgRouting::Normal);
        assert_eq!(m.dyn_parse_state(), DynParseState::Start);
    }

    #[test]
    fn set_type_preserves_original() {
        let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
        assert_eq!(m.orig_type(), MsgType::Unknown);
        m.set_type(MsgType::ReqRedisSet);
        assert_eq!(m.ty(), MsgType::ReqRedisSet);
        assert_eq!(m.orig_type(), MsgType::ReqRedisGet);
    }

    #[test]
    fn awaiting_saturates() {
        let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
        m.decr_awaiting_rsps();
        assert_eq!(m.awaiting_rsps(), 0);
        m.incr_awaiting_rsps();
        m.incr_awaiting_rsps();
        assert_eq!(m.awaiting_rsps(), 2);
        m.decr_awaiting_rsps();
        assert_eq!(m.awaiting_rsps(), 1);
    }

    #[test]
    fn routing_name_covers_every_variant() {
        // Each MsgRouting variant has a stable diagnostic name.
        assert_eq!(MsgRouting::Normal.name(), "ROUTING_NORMAL");
        assert_eq!(MsgRouting::LocalNodeOnly.name(), "ROUTING_LOCAL_NODE_ONLY");
        assert_eq!(
            MsgRouting::TokenOwnerLocalRackOnly.name(),
            "ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY"
        );
        assert_eq!(
            MsgRouting::AllNodesLocalRackOnly.name(),
            "ROUTING_ALL_NODES_LOCAL_RACK_ONLY"
        );
        assert_eq!(
            MsgRouting::AllNodesAllRacksAllDcs.name(),
            "ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS"
        );
    }

    #[test]
    fn key_and_arg_lists_mutate_through_accessors() {
        // keys_mut / push_key and args expose the parsed lists.
        use crate::msg::keypos::KeyPos;
        let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
        m.push_key(KeyPos::new(b"k".to_vec(), 0..1));
        assert_eq!(m.keys().len(), 1);
        m.keys_mut().clear();
        assert!(m.keys().is_empty());
        assert!(m.args().is_empty());
    }

    #[test]
    fn scalar_setters_round_trip() {
        // The plain setter/getter pairs round-trip their values.
        use crate::msg::{ConsistencyLevel, DynErrorCode};
        let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
        m.set_mlen(123);
        assert_eq!(m.mlen(), 123);
        m.set_parse_result(MsgParseResult::Again);
        assert_eq!(m.parse_result(), MsgParseResult::Again);
        m.set_dyn_parse_state(DynParseState::Start);
        assert_eq!(m.dyn_parse_state(), DynParseState::Start);
        m.set_routing(MsgRouting::LocalNodeOnly);
        assert_eq!(m.routing(), MsgRouting::LocalNodeOnly);
        m.set_consistency(ConsistencyLevel::DcQuorum);
        assert_eq!(m.consistency(), ConsistencyLevel::DcQuorum);
        m.set_timestamp_us(9_999);
        assert_eq!(m.timestamp_us(), 9_999);
        m.set_error_code(13);
        assert_eq!(m.error_code(), 13);
        m.set_dyn_error_code(DynErrorCode::PeerHostDown);
        assert_eq!(m.dyn_error_code(), DynErrorCode::PeerHostDown);
        m.set_awaiting_rsps(4);
        assert_eq!(m.awaiting_rsps(), 4);
        m.set_owner(Some(77));
        assert_eq!(m.owner(), Some(77));
        m.set_selected_rsp(Some(5));
        assert_eq!(m.selected_rsp(), Some(5));
    }

    #[test]
    fn flag_setters_and_id_lists() {
        // swallow / done / is_error setters plus the fragment and
        // response id lists and their accessors.
        let mut m = Msg::new(1, MsgType::ReqRedisMget, true);
        m.set_swallow(true);
        assert!(m.flags().swallow);
        m.set_done(true);
        assert!(m.flags().done);
        m.set_is_error(true);
        assert!(m.flags().is_error);
        m.push_fragment_id(2);
        assert_eq!(m.fragment_ids(), &[2]);
        m.push_response_id(3);
        assert_eq!(m.response_ids(), &[3]);
    }

    #[test]
    fn dmsg_attach_and_borrow() {
        // set_dmsg / dmsg / dmsg_mut attach and expose the DNODE
        // header.
        use crate::proto::dnode::Dmsg;
        let mut m = Msg::new(1, MsgType::Unknown, true);
        assert!(m.dmsg().is_none());
        m.set_dmsg(Dmsg::default());
        assert!(m.dmsg().is_some());
        assert!(m.dmsg_mut().is_some());
    }

    #[test]
    fn rspmgr_install_seeds_awaiting_and_borrows() {
        // set_rspmgr marks rspmgrs_inited, seeds awaiting_rsps from
        // the manager's max, and exposes mutable borrows of both the
        // local and the additional managers.
        use crate::msg::response_mgr::ResponseMgr;
        let mut m = Msg::new(1, MsgType::ReqRedisGet, true);
        let probe = Msg::new(2, MsgType::ReqRedisGet, true);
        let mgr = ResponseMgr::new(&probe, 3, None);
        m.set_rspmgr(mgr);
        assert!(m.flags().rspmgrs_inited);
        assert_eq!(m.awaiting_rsps(), 3);
        assert!(m.rspmgr().is_some());
        assert!(m.rspmgr_mut().is_some());
        assert!(m.additional_rspmgrs().is_empty());
        let extra = ResponseMgr::new(&probe, 1, Some("dc2".to_string()));
        m.additional_rspmgrs_mut().push(extra);
        assert_eq!(m.additional_rspmgrs().len(), 1);
    }
}
