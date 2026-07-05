//! DNODE wire codec.
//!
//! The DNODE protocol frames every Dynomite peer-to-peer message
//! with a small ASCII header followed by an opaque payload. The
//! header carries the message id, type tag, encryption/compression
//! flags, protocol version, same-datacenter bit, an inline data
//! field (either a one-byte placeholder or an RSA-wrapped AES key),
//! and the byte length of the payload that follows after `\r\n`.
//!
//! The parser is a single state machine driven byte-by-byte. This
//! module exposes:
//!
//! * [`DynParseState`] - the parser's state alphabet.
//! * [`DmsgType`] - the full set of message-type discriminators.
//! * [`Dmsg`] - the in-memory header.
//! * [`DnodeParser`] - the state machine, advanced by feeding bytes
//!   through [`DnodeParser::step`].
//! * [`dmsg_write`] / [`dmsg_write_mbuf`] - the canonical encoders.
//! * [`parse_req`] / [`parse_rsp`] - thin sync wrappers around the
//!   parser that operate on a [`crate::msg::Msg`]'s mbuf chain.
//! * [`dmsg_process`] - dispatcher that classifies a parsed
//!   [`Dmsg`] by type for the cluster layer to act on.
//!
//! The encoder accepts an optional `aes_key_payload`: when present,
//! the caller provides the bytes the inline data field should hold
//! (the RSA-wrapped AES key produced by [`crate::crypto::Crypto`]).
//! When absent, the encoder writes the single-byte `'d'` placeholder
//! used after the first handshake message.

// The parser truncates accumulated decimals into the same fixed
// bit widths the wire format uses (`u8` for the type and flags,
// `u32` for the data and payload lengths). The allowance covers
// these intentional `as u8` / `as u32` casts; out-of-range numerals
// are surfaced as
// parse errors elsewhere in the state machine.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::needless_continue)]

use std::net::SocketAddr;

use crate::core::types::MsgId;
use crate::io::mbuf::{Mbuf, MbufQueue};
use crate::msg::message::Msg;
use crate::msg::message::MsgParseResult;

/// Magic literal that opens every DNODE header.
pub const MAGIC: &[u8] = b"$2014$";

/// Default protocol version emitted by [`dmsg_write`] (version 10).
pub const VERSION_10: u8 = 1;

/// CRLF delimiter that separates the DNODE header from its payload.
pub const CRLF: &[u8] = b"\r\n";

/// Single-byte placeholder used by [`dmsg_write`] when no AES key
/// payload accompanies the header.
pub const HANDSHAKE_PLACEHOLDER_DATA: u8 = b'd';

/// Single-byte placeholder used by [`dmsg_write_mbuf`] when no AES
/// key payload accompanies the header. The gossip path emits `'a'`
/// instead of `'d'` to disambiguate the two encoder flavours.
pub const GOSSIP_PLACEHOLDER_DATA: u8 = b'a';

/// Per-frame upper bound on a parser-accepted length field.
///
/// The on-the-wire DNODE header carries `mlen` and `plen` as ASCII
/// decimal numerals that the streaming parser accumulates into a
/// `u64` before casting to the wire's `u32`. Without an explicit
/// cap on the accumulator, a single byte run of `1`s inflates
/// `self.num` past `u32::MAX`; the silent truncation then drives
/// [`Vec::reserve`] into a multi-gigabyte malloc (libfuzzer 1h soak
/// finding 2026-06-02, captured at
/// `crates/fuzz/seeds/dnode_parse/regression-oom-2026-06-02`).
///
/// 256 MiB is well above any legitimate DNODE frame on the wire
/// today (the largest production payloads we have observed are a
/// few hundred KiB) while staying well below an allocation that
/// would produce a real OOM under typical RSS budgets. The parser
/// surfaces [`ParseStep::Error`] the moment any DataLen or
/// PayloadLen accumulator exceeds this bound.
pub const MAX_DATA_LEN: u64 = 256 * 1024 * 1024;

/// Parser state transitions.
///
/// Each variant is one state of the DNODE frame parser. The numeric
/// values are stable so external parity tooling can compare them.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum DynParseState {
    /// Initial state; consumes leading whitespace until the magic
    /// literal is observed.
    #[default]
    Start,
    /// `$2014$` was matched; awaiting the trailing space.
    MagicString,
    /// Reading the decimal message id.
    MsgId,
    /// Reading the decimal message type.
    TypeId,
    /// Reading the decimal flags bit field.
    BitField,
    /// Reading the decimal protocol version.
    Version,
    /// Reading the same-datacenter digit.
    SameDc,
    /// Awaiting the leading `*` before the data length.
    Star,
    /// Reading the decimal data length.
    DataLen,
    /// Consuming the inline data of `mlen` bytes.
    Data,
    /// Skipping spaces before the payload-length marker.
    SpacesBeforePayloadLen,
    /// Reading the decimal payload length.
    PayloadLen,
    /// Awaiting the LF that terminates the header.
    CrlfBeforeDone,
    /// Header complete; payload position recorded.
    Done,
    /// Header complete and post-handshake decryption applied.
    PostDone,
    /// Recovery state after the parser hit a malformed byte.
    Unknown,
}

/// DNODE message type identifier.
///
/// The numeric values are stable wire discriminators
/// because the type travels on the wire as a decimal.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum DmsgType {
    /// Unset / unknown type.
    #[default]
    Unknown = 0,
    /// Diagnostic frame (unused on the live wire; kept for parity).
    Debug = 1,
    /// Parse-error frame (unused on the live wire; kept for parity).
    ParseError = 2,
    /// Datastore request bound for the local DC.
    Req = 3,
    /// Datastore request to be forwarded across DCs.
    ReqForward = 4,
    /// Datastore response.
    Res = 5,
    /// AES key handshake.
    CryptoHandshake = 6,
    /// Gossip SYN.
    GossipSyn = 7,
    /// Gossip SYN reply.
    GossipSynReply = 8,
    /// Gossip ACK.
    GossipAck = 9,
    /// Gossip digest SYN.
    GossipDigestSyn = 10,
    /// Gossip digest ACK.
    GossipDigestAck = 11,
    /// Gossip digest ACK round 2.
    GossipDigestAck2 = 12,
    /// Gossip shutdown notice.
    GossipShutdown = 13,
    /// Explicit handoff chunk frame.
    ///
    /// Carries one chunk of a token-range handoff stream from the
    /// previous owner of the range to the new owner. Distinct from
    /// the AAE exchange variants so the receiver can route handoff
    /// frames to the dedicated handoff coordinator without parsing
    /// the payload first.
    HandoffChunk = 14,
    /// Cluster-wide RediSearch FT.SEARCH request frame.
    ///
    /// Sent by the FT.SEARCH coordinator on the node that
    /// received the client request to every primary peer
    /// covering the index's key range. The payload encodes a
    /// broadcast request (table name, serialised query body,
    /// top-K) - see the `dynomite-search` crate's
    /// `query_fsm::BroadcastRequest`. Routed by the dispatcher
    /// to the dedicated FT.SEARCH coordinator FSM instead of
    /// the data-plane stack so the per-peer query runs against
    /// the local registry rather than being re-forwarded.
    FtSearchReq = 15,
    /// Cluster-wide RediSearch FT.SEARCH reply frame.
    ///
    /// Returned by every peer that received a [`Self::FtSearchReq`]
    /// once its local search completed (or the per-peer
    /// deadline elapsed). The payload encodes the per-peer
    /// top-K hit list plus a `timed_out` flag the coordinator
    /// uses to mark partial results.
    FtSearchRep = 16,
    /// Cross-node XA prepare request.
    ///
    /// Carries one transaction branch's writes to the peer that
    /// owns it. The receiver runs start + apply + end + prepare
    /// against its local resource manager and replies with a
    /// [`Self::XaVote`]. The payload layout is owned by the
    /// `dyniak` transaction layer (`dyniak::datastore::xa`).
    XaPrepare = 17,
    /// Cross-node XA prepare reply carrying a branch's vote
    /// (commit / read-only / abort) for a [`Self::XaPrepare`].
    XaVote = 18,
    /// Cross-node XA commit request for a durably prepared branch.
    /// The receiver commits idempotently and replies
    /// [`Self::XaAck`].
    XaCommit = 19,
    /// Cross-node XA rollback request for a branch. The receiver
    /// rolls back idempotently and replies [`Self::XaAck`].
    XaRollback = 20,
    /// Cross-node XA acknowledgement for a [`Self::XaCommit`] or
    /// [`Self::XaRollback`].
    XaAck = 21,
    /// Dyniak cross-node object-replica op.
    ///
    /// Carries one fire-and-forget replica write (`Put` / `Del`)
    /// or read-repair read (`Get`) forwarded from the node that
    /// received the client request to a peer on the object's
    /// replica list. The payload is the compact `PeerOp` encoding
    /// owned by the `dyniak` routing layer
    /// (`dyniak::proto::replica_wire`). The receiver applies the
    /// op to its LOCAL object store and does NOT re-forward it, so
    /// a replica write fans out exactly once. Bypassed by
    /// [`dmsg_process`] alongside the XA variants so the receive
    /// path routes it to the dyniak replica sink rather than the
    /// data-plane stack.
    RiakReplica = 22,
    /// Cross-node RAMP-Fast prepare / commit / read leg.
    ///
    /// Carries one RAMP transaction's per-peer work (a versioned
    /// invisible write in PREPARE, a visible-pointer advance in
    /// COMMIT, or a versioned read round) to the peer that owns the
    /// key. The payload layout is owned by the `dyniak` RAMP layer
    /// (`dyniak::ramp_store`). This variant is the wire hook for the
    /// cross-node fan-out; the single-node coordinator does not yet
    /// emit it.
    RampPrepare = 23,
}

impl DmsgType {
    /// Build a type from its on-the-wire integer value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::dnode::DmsgType;
    /// assert_eq!(DmsgType::from_u8(3), Some(DmsgType::Req));
    /// assert_eq!(DmsgType::from_u8(99), None);
    /// ```
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            0 => DmsgType::Unknown,
            1 => DmsgType::Debug,
            2 => DmsgType::ParseError,
            3 => DmsgType::Req,
            4 => DmsgType::ReqForward,
            5 => DmsgType::Res,
            6 => DmsgType::CryptoHandshake,
            7 => DmsgType::GossipSyn,
            8 => DmsgType::GossipSynReply,
            9 => DmsgType::GossipAck,
            10 => DmsgType::GossipDigestSyn,
            11 => DmsgType::GossipDigestAck,
            12 => DmsgType::GossipDigestAck2,
            13 => DmsgType::GossipShutdown,
            14 => DmsgType::HandoffChunk,
            15 => DmsgType::FtSearchReq,
            16 => DmsgType::FtSearchRep,
            17 => DmsgType::XaPrepare,
            18 => DmsgType::XaVote,
            19 => DmsgType::XaCommit,
            20 => DmsgType::XaRollback,
            21 => DmsgType::XaAck,
            23 => DmsgType::RampPrepare,
            22 => DmsgType::RiakReplica,
            _ => return None,
        })
    }

    /// Numeric on-the-wire value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::dnode::DmsgType;
    /// assert_eq!(DmsgType::CryptoHandshake.as_u8(), 6);
    /// ```
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Encryption bit in [`Dmsg::flags`].
pub const DMSG_FLAG_ENCRYPTED: u8 = 0x1;

/// Compression bit in [`Dmsg::flags`].
pub const DMSG_FLAG_COMPRESSED: u8 = 0x2;

/// Parsed DNODE header.
///
/// `data` and `payload` hold copies of the on-the-wire bytes. The
/// encoder side fills both before emitting; the parser fills them as
/// it advances through the state machine.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Dmsg {
    /// Message id.
    pub id: MsgId,
    /// Message type.
    pub ty: DmsgType,
    /// Flag bit field; encryption is bit 0, compression is bit 1.
    pub flags: u8,
    /// Protocol version.
    pub version: u8,
    /// True when sender and receiver share a datacenter.
    pub same_dc: bool,
    /// Source address recorded by the recv path. The parser leaves
    /// it `None`; a caller with the connection state may stamp it
    /// after parsing.
    pub source_address: Option<SocketAddr>,
    /// Length (in bytes) of the inline data field.
    pub mlen: u32,
    /// Inline data: either the single-byte placeholder or the
    /// RSA-wrapped AES key during the crypto handshake.
    pub data: Vec<u8>,
    /// Length (in bytes) of the trailing payload framed by the
    /// header.
    pub plen: u32,
    /// Payload bytes, if collected by the parser.
    pub payload: Vec<u8>,
}

impl Dmsg {
    /// Construct an empty `Dmsg` with all fields at their defaults.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::dnode::{Dmsg, DmsgType, VERSION_10};
    /// let d = Dmsg::new();
    /// assert_eq!(d.ty, DmsgType::Unknown);
    /// assert_eq!(d.version, VERSION_10);
    /// assert!(d.same_dc);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: 0,
            ty: DmsgType::Unknown,
            flags: 0,
            version: VERSION_10,
            same_dc: true,
            source_address: None,
            mlen: 0,
            data: Vec::new(),
            plen: 0,
            payload: Vec::new(),
        }
    }

    /// True when the encryption flag is set.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::dnode::{Dmsg, DMSG_FLAG_ENCRYPTED};
    /// let mut d = Dmsg::new();
    /// d.flags = DMSG_FLAG_ENCRYPTED;
    /// assert!(d.is_encrypted());
    /// ```
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.flags & DMSG_FLAG_ENCRYPTED != 0
    }

    /// True when the compression flag is set.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::dnode::{Dmsg, DMSG_FLAG_COMPRESSED};
    /// let mut d = Dmsg::new();
    /// d.flags = DMSG_FLAG_COMPRESSED;
    /// assert!(d.is_compressed());
    /// ```
    #[must_use]
    pub fn is_compressed(&self) -> bool {
        self.flags & DMSG_FLAG_COMPRESSED != 0
    }
}

/// Result of a single [`DnodeParser::step`] invocation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ParseStep {
    /// More bytes are required to advance the state machine. The
    /// `consumed` field records how many of the input bytes the
    /// parser already absorbed.
    NeedMore {
        /// Number of input bytes the parser absorbed before it
        /// stopped waiting for more.
        consumed: usize,
    },
    /// The header (up to and including the trailing LF) has been
    /// parsed. The `consumed` field records the offset just past
    /// the LF, so the caller can read the payload starting at that
    /// index.
    HeaderDone {
        /// Offset just past the trailing LF.
        consumed: usize,
    },
    /// The parser hit an unrecoverable bad byte. The caller should
    /// drop the buffer (or split it at `consumed`) and reset.
    Error {
        /// Offset of the byte that triggered the error.
        consumed: usize,
    },
}

/// Errors that can be raised when encoding or parsing a DNODE
/// header without going through the streaming state machine.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DnodeError {
    /// Buffer too small to encode the header.
    OutOfSpace,
    /// Header does not begin with the magic literal.
    BadMagic,
    /// Numeric field could not be parsed.
    BadNumber,
    /// Trailing CRLF missing.
    MissingCrlf,
    /// Type discriminator out of range.
    BadType,
    /// Inline data shorter than the declared `mlen`.
    TruncatedData,
}

/// Streaming DNODE header parser.
#[derive(Debug)]
pub struct DnodeParser {
    state: DynParseState,
    num: u64,
    dmsg: Dmsg,
    data_remaining: u32,
    magic_progress: u8,
    /// Whether the previous byte was an ASCII digit. The header
    /// state machine only transitions out of the numeric header
    /// fields (MSG_ID, TYPE_ID, BIT_FIELD, VERSION, SAME_DC) when
    /// the byte immediately preceding the field-terminating space
    /// was a digit; the parser reproduces this guard so extra
    /// whitespace (or any other non-digit byte) is rejected with
    /// the wire protocol's strictness.
    prev_was_digit: bool,
}

impl DnodeParser {
    /// Build a fresh parser positioned at [`DynParseState::Start`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::dnode::{DnodeParser, DynParseState};
    /// let p = DnodeParser::new();
    /// assert_eq!(p.state(), DynParseState::Start);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: DynParseState::Start,
            num: 0,
            dmsg: Dmsg::new(),
            data_remaining: 0,
            magic_progress: 0,
            prev_was_digit: false,
        }
    }

    /// Reset the parser to [`DynParseState::Start`] with a fresh
    /// accumulator [`Dmsg`].
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Current state.
    #[must_use]
    pub fn state(&self) -> DynParseState {
        self.state
    }

    /// Borrow the partial [`Dmsg`].
    #[must_use]
    pub fn dmsg(&self) -> &Dmsg {
        &self.dmsg
    }

    /// Move the parsed [`Dmsg`] out of the parser. Only meaningful
    /// after a [`ParseStep::HeaderDone`] step.
    pub fn take_dmsg(&mut self) -> Dmsg {
        let mut out = Dmsg::new();
        std::mem::swap(&mut out, &mut self.dmsg);
        self.state = DynParseState::Start;
        self.num = 0;
        self.data_remaining = 0;
        self.magic_progress = 0;
        self.prev_was_digit = false;
        out
    }

    /// Feed `input` to the parser. The parser advances as far as it
    /// can and returns one of the three [`ParseStep`] variants.
    ///
    /// The state machine is byte-driven and can be reentered with a
    /// fresh slice when [`ParseStep::NeedMore`] indicates the input
    /// was truncated mid-header.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::proto::dnode::{DnodeParser, ParseStep};
    /// let mut p = DnodeParser::new();
    /// let bytes = b"$2014$ 1 3 0 1 1 *1 d *0\r\n";
    /// match p.step(bytes) {
    ///     ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
    ///     other => panic!("unexpected: {other:?}"),
    /// }
    /// ```
    /// The state machine intentionally stays in one function:
    /// splitting the per-state arms across helpers would obscure
    /// the byte-by-byte control flow.
    #[allow(clippy::too_many_lines)]
    pub fn step(&mut self, input: &[u8]) -> ParseStep {
        let mut idx = 0usize;
        while idx < input.len() {
            let ch = input[idx];
            match self.state {
                DynParseState::Start => {
                    // Phase 1: skip leading whitespace.
                    if self.magic_progress == 0 {
                        if ch == b' ' {
                            idx += 1;
                            continue;
                        }
                        if ch != b'$' {
                            return ParseStep::Error { consumed: idx };
                        }
                    }
                    // Phase 2: byte-incrementally match the magic
                    // literal so split inputs are tolerated.
                    let want = MAGIC[usize::from(self.magic_progress)];
                    if ch != want {
                        return ParseStep::Error { consumed: idx };
                    }
                    self.magic_progress += 1;
                    idx += 1;
                    if usize::from(self.magic_progress) == MAGIC.len() {
                        self.state = DynParseState::MagicString;
                        self.magic_progress = 0;
                    }
                    continue;
                }
                DynParseState::MagicString => {
                    if ch == b' ' {
                        self.state = DynParseState::MsgId;
                        self.num = 0;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::MsgId => {
                    // DYN_MSG_ID state: digits accumulate, a single
                    // space terminates the field but only when the
                    // byte immediately
                    // before it was a digit. Anything else is
                    // rejected: the streaming parser surfaces an
                    // error so the caller can drop the buffer.
                    if ch.is_ascii_digit() {
                        self.num = self.num.wrapping_mul(10) + u64::from(ch - b'0');
                        self.prev_was_digit = true;
                        idx += 1;
                        continue;
                    }
                    if ch == b' ' && self.prev_was_digit {
                        self.dmsg.id = self.num;
                        self.state = DynParseState::TypeId;
                        self.num = 0;
                        self.prev_was_digit = false;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::TypeId => {
                    if ch.is_ascii_digit() {
                        self.num = self.num.wrapping_mul(10) + u64::from(ch - b'0');
                        self.prev_was_digit = true;
                        idx += 1;
                        continue;
                    }
                    if ch == b' ' && self.prev_was_digit {
                        self.dmsg.ty = match DmsgType::from_u8(self.num as u8) {
                            Some(t) => t,
                            None => return ParseStep::Error { consumed: idx },
                        };
                        self.state = DynParseState::BitField;
                        self.num = 0;
                        self.prev_was_digit = false;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::BitField => {
                    if ch.is_ascii_digit() {
                        self.num = self.num.wrapping_mul(10) + u64::from(ch - b'0');
                        self.prev_was_digit = true;
                        idx += 1;
                        continue;
                    }
                    if ch == b' ' && self.prev_was_digit {
                        self.dmsg.flags = (self.num as u8) & 0xF;
                        self.state = DynParseState::Version;
                        self.num = 0;
                        self.prev_was_digit = false;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::Version => {
                    if ch.is_ascii_digit() {
                        self.num = self.num.wrapping_mul(10) + u64::from(ch - b'0');
                        self.prev_was_digit = true;
                        idx += 1;
                        continue;
                    }
                    if ch == b' ' && self.prev_was_digit {
                        self.dmsg.version = self.num as u8;
                        self.state = DynParseState::SameDc;
                        self.num = 0;
                        self.prev_was_digit = false;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::SameDc => {
                    if ch.is_ascii_digit() {
                        self.dmsg.same_dc = ch != b'0';
                        self.prev_was_digit = true;
                        idx += 1;
                        continue;
                    }
                    if ch == b' ' && self.prev_was_digit {
                        self.state = DynParseState::DataLen;
                        self.num = 0;
                        self.prev_was_digit = false;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::Star | DynParseState::DataLen => {
                    if ch == b'*' {
                        idx += 1;
                        continue;
                    }
                    if ch.is_ascii_digit() {
                        self.num = self.num.wrapping_mul(10) + u64::from(ch - b'0');
                        // Reject pathological-size length fields
                        // before the cast to u32 wraps and a
                        // downstream Vec::reserve allocates the
                        // wrapped value as bytes. See MAX_DATA_LEN.
                        if self.num > MAX_DATA_LEN {
                            return ParseStep::Error { consumed: idx };
                        }
                        idx += 1;
                        continue;
                    }
                    if ch == b' ' && self.state == DynParseState::DataLen {
                        self.dmsg.mlen = self.num as u32;
                        self.data_remaining = self.dmsg.mlen;
                        self.dmsg.data.clear();
                        self.dmsg.data.reserve(self.data_remaining as usize);
                        self.state = DynParseState::Data;
                        self.num = 0;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::Data => {
                    if self.data_remaining == 0 {
                        self.state = DynParseState::SpacesBeforePayloadLen;
                        continue;
                    }
                    let take = std::cmp::min(self.data_remaining as usize, input.len() - idx);
                    self.dmsg.data.extend_from_slice(&input[idx..idx + take]);
                    self.data_remaining -= take as u32;
                    idx += take;
                    if self.data_remaining == 0 {
                        self.state = DynParseState::SpacesBeforePayloadLen;
                    }
                    continue;
                }
                DynParseState::SpacesBeforePayloadLen => {
                    if ch == b' ' {
                        idx += 1;
                        continue;
                    }
                    if ch == b'*' {
                        self.state = DynParseState::PayloadLen;
                        self.num = 0;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::PayloadLen => {
                    if ch.is_ascii_digit() {
                        self.num = self.num.wrapping_mul(10) + u64::from(ch - b'0');
                        if self.num > MAX_DATA_LEN {
                            return ParseStep::Error { consumed: idx };
                        }
                        idx += 1;
                        continue;
                    }
                    if ch == b'\r' {
                        self.dmsg.plen = self.num as u32;
                        self.state = DynParseState::CrlfBeforeDone;
                        self.num = 0;
                        idx += 1;
                        continue;
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::CrlfBeforeDone => {
                    if ch == b'\n' {
                        self.state = DynParseState::Done;
                        idx += 1;
                        return ParseStep::HeaderDone { consumed: idx };
                    }
                    return ParseStep::Error { consumed: idx };
                }
                DynParseState::Done | DynParseState::PostDone | DynParseState::Unknown => {
                    return ParseStep::HeaderDone { consumed: idx };
                }
            }
        }
        ParseStep::NeedMore { consumed: idx }
    }
}

impl Default for DnodeParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Encode a DNODE header into the writable region of `mbuf`.
///
/// `aes_key_payload`, when `Some`, is written as the inline data
/// field; this is how the crypto handshake transports the
/// RSA-wrapped AES key. When `None`, a single-byte `'d'` placeholder
/// is emitted.
///
/// `flags` is taken verbatim (the encryption bit must be set by the
/// caller, alongside any compression bit).
///
/// The encoder writes the entire header as a single contiguous
/// region; if `mbuf` lacks the necessary capacity,
/// [`DnodeError::OutOfSpace`] is returned.
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::proto::dnode::{dmsg_write, DmsgType};
///
/// let pool = MbufPool::default();
/// let mut buf = pool.get();
/// dmsg_write(
///     &mut buf,
///     /* msg_id */ 1,
///     DmsgType::Req,
///     /* flags */ 0,
///     /* same_dc */ true,
///     /* aes_key_payload */ None,
///     /* plen */ 0,
/// )
/// .unwrap();
/// assert!(buf.readable().starts_with(b"   $2014$ 1 3 0"));
/// ```
pub fn dmsg_write(
    mbuf: &mut Mbuf,
    msg_id: MsgId,
    ty: DmsgType,
    flags: u8,
    same_dc: bool,
    aes_key_payload: Option<&[u8]>,
    plen: u32,
) -> Result<(), DnodeError> {
    let header = build_header(msg_id, ty, flags, same_dc, aes_key_payload, plen, false);
    write_chain(mbuf, &header)
}

/// Encode a gossip-flavored DNODE header.
///
/// Differs from [`dmsg_write`] only in the placeholder byte emitted
/// when no AES key payload accompanies the header (`'a'` instead of
/// `'d'`).
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::proto::dnode::{dmsg_write_mbuf, DmsgType};
///
/// let pool = MbufPool::default();
/// let mut buf = pool.get();
/// dmsg_write_mbuf(
///     &mut buf,
///     /* msg_id */ 5,
///     DmsgType::GossipSyn,
///     /* flags */ 0,
///     /* same_dc */ true,
///     /* aes_key_payload */ None,
///     /* plen */ 64,
/// )
/// .unwrap();
/// assert!(buf.readable().contains(&b'a'));
/// ```
pub fn dmsg_write_mbuf(
    mbuf: &mut Mbuf,
    msg_id: MsgId,
    ty: DmsgType,
    flags: u8,
    same_dc: bool,
    aes_key_payload: Option<&[u8]>,
    plen: u32,
) -> Result<(), DnodeError> {
    let header = build_header(msg_id, ty, flags, same_dc, aes_key_payload, plen, true);
    write_chain(mbuf, &header)
}

fn build_header(
    msg_id: MsgId,
    ty: DmsgType,
    flags: u8,
    same_dc: bool,
    aes_key_payload: Option<&[u8]>,
    plen: u32,
    gossip_placeholder: bool,
) -> Vec<u8> {
    use std::io::Write as _;
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    // Three leading spaces are part of the magic literal as written
    // on the wire; the parser tolerates and skips them in DYN_START.
    buf.extend_from_slice(b"   $2014$ ");
    let _ = write!(buf, "{msg_id}");
    buf.push(b' ');
    let _ = write!(buf, "{}", ty.as_u8());
    buf.push(b' ');
    let _ = write!(buf, "{}", flags & 0xF);
    buf.push(b' ');
    let _ = write!(buf, "{VERSION_10}");
    buf.push(b' ');
    buf.push(if same_dc { b'1' } else { b'0' });
    buf.push(b' ');
    buf.push(b'*');
    if let Some(payload) = aes_key_payload {
        let _ = write!(buf, "{}", payload.len());
        buf.push(b' ');
        buf.extend_from_slice(payload);
    } else {
        buf.extend_from_slice(b"1 ");
        buf.push(if gossip_placeholder {
            GOSSIP_PLACEHOLDER_DATA
        } else {
            HANDSHAKE_PLACEHOLDER_DATA
        });
    }
    buf.push(b' ');
    buf.push(b'*');
    let _ = write!(buf, "{plen}");
    buf.extend_from_slice(CRLF);
    buf
}

fn write_chain(mbuf: &mut Mbuf, payload: &[u8]) -> Result<(), DnodeError> {
    if mbuf.remaining() < payload.len() {
        return Err(DnodeError::OutOfSpace);
    }
    let n = mbuf.recv(payload);
    debug_assert_eq!(n, payload.len());
    Ok(())
}

/// Sync byte parser that drives a request message's DNODE header
/// state machine.
///
/// The parser walks the contiguous bytes spanning the message's
/// mbuf chain and updates the [`Msg`] in place. On a fully parsed
/// header, the function attaches the [`Dmsg`] to the message and
/// returns `MsgParseResult::Ok`. On truncated input the parser
/// returns `MsgParseResult::Again`. On invalid bytes the parser
/// records `MsgParseResult::Error` and surfaces the same value.
///
/// This is the synchronous header parser. The async wrapping
/// (per-connection task scheduling, decryption hand-off when the
/// encryption bit is set) is driven by [`crate::net`].
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::dnode::{parse_req, DmsgType, DynParseState};
///
/// let pool = MbufPool::default();
/// let mut msg = Msg::new(0, MsgType::Unknown, true);
/// let mut mb = pool.get();
/// mb.recv(b"$2014$ 1 3 0 1 1 *1 d *0\r\n");
/// msg.mbufs_mut().push_back(mb);
/// msg.recompute_mlen();
/// let result = parse_req(&mut msg);
/// assert_eq!(msg.dyn_parse_state(), DynParseState::Done);
/// assert_eq!(msg.dmsg().unwrap().ty, DmsgType::Req);
/// drop(result);
/// ```
pub fn parse_req(msg: &mut Msg) -> MsgParseResult {
    parse_msg(msg, false)
}

/// Sync byte parser counterpart to [`parse_req`] for response
/// messages.
///
/// # Examples
///
/// ```
/// use dynomite::io::mbuf::MbufPool;
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::dnode::{parse_rsp, DmsgType};
///
/// let pool = MbufPool::default();
/// let mut msg = Msg::new(0, MsgType::Unknown, false);
/// let mut mb = pool.get();
/// mb.recv(b"$2014$ 9 5 0 1 1 *1 d *0\r\n");
/// msg.mbufs_mut().push_back(mb);
/// msg.recompute_mlen();
/// let _ = parse_rsp(&mut msg);
/// assert_eq!(msg.dmsg().unwrap().ty, DmsgType::Res);
/// ```
pub fn parse_rsp(msg: &mut Msg) -> MsgParseResult {
    parse_msg(msg, true)
}

fn parse_msg(msg: &mut Msg, _is_response: bool) -> MsgParseResult {
    // Flatten the chain into a single buffer for parsing. The
    // parser tolerates splits at arbitrary boundaries, but this
    // entry point drives the state machine over one contiguous
    // slice rather than streaming chunk by chunk.
    let mut bytes: Vec<u8> = Vec::with_capacity(msg.mbufs().total_len());
    for mbuf in msg.mbufs() {
        bytes.extend_from_slice(mbuf.readable());
    }

    let mut parser = DnodeParser::new();
    parser.state = msg.dyn_parse_state();
    match parser.step(&bytes) {
        ParseStep::HeaderDone { .. } => {
            let dmsg = parser.take_dmsg();
            msg.set_dyn_parse_state(DynParseState::Done);
            msg.set_dmsg(dmsg);
            msg.set_parse_result(MsgParseResult::Ok);
            MsgParseResult::Ok
        }
        ParseStep::NeedMore { .. } => {
            msg.set_dyn_parse_state(parser.state);
            msg.set_parse_result(MsgParseResult::Again);
            MsgParseResult::Again
        }
        ParseStep::Error { .. } => {
            msg.set_dyn_parse_state(DynParseState::Unknown);
            msg.set_parse_result(MsgParseResult::Error);
            MsgParseResult::Error
        }
    }
}

/// Outcome of [`dmsg_process`].
///
/// `Bypass` means the header has been recognised as control traffic
/// and the cluster layer should not pass the message further down
/// the protocol stack.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum DmsgDispatch {
    /// Frame consumed by a control-plane handler.
    Bypass,
    /// Frame should continue through the data-plane stack.
    Forward,
}

/// Classify a parsed [`Dmsg`] as control-plane traffic the cluster
/// layer should consume directly (`Bypass`), or data-plane traffic
/// that should continue through the protocol stack (`Forward`).
///
/// This decides the message-shape routing only; decoding the
/// forwarded gossip variants into cluster events is done by the
/// cluster layer, not here.
///
/// # Examples
///
/// ```
/// use dynomite::proto::dnode::{dmsg_process, Dmsg, DmsgDispatch, DmsgType};
///
/// let mut d = Dmsg::new();
/// d.ty = DmsgType::CryptoHandshake;
/// assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass);
///
/// // Gossip variants other than SYN / SYN_REPLY fall through.
/// d.ty = DmsgType::GossipShutdown;
/// assert_eq!(dmsg_process(&d), DmsgDispatch::Forward);
///
/// d.ty = DmsgType::Req;
/// assert_eq!(dmsg_process(&d), DmsgDispatch::Forward);
/// ```
#[must_use]
pub fn dmsg_process(dmsg: &Dmsg) -> DmsgDispatch {
    // Dmsg dispatch table: only CRYPTO_HANDSHAKE,
    // GOSSIP_SYN, and GOSSIP_SYN_REPLY short-circuit; the other
    // gossip variants (ACK, DIGEST_SYN, DIGEST_ACK, DIGEST_ACK2,
    // SHUTDOWN) fall through to the default branch and are
    // forwarded to the cluster handlers. HANDOFF_CHUNK frames are
    // control-plane traffic for the explicit handoff coordinator
    // and bypass the data-plane stack alongside the crypto / gossip
    // handshake variants.
    match dmsg.ty {
        DmsgType::CryptoHandshake
        | DmsgType::GossipSyn
        | DmsgType::GossipSynReply
        | DmsgType::HandoffChunk
        | DmsgType::FtSearchReq
        | DmsgType::FtSearchRep
        | DmsgType::XaPrepare
        | DmsgType::XaVote
        | DmsgType::XaCommit
        | DmsgType::XaRollback
        | DmsgType::XaAck
        | DmsgType::RampPrepare
        | DmsgType::RiakReplica => DmsgDispatch::Bypass,
        _ => DmsgDispatch::Forward,
    }
}

/// Drain `chain` into a contiguous `Vec<u8>` recycling each chunk
/// back to `pool`. Useful for tests and for callers that need a
/// flat buffer of decrypted payload bytes.
pub fn flatten_chain(chain: &mut MbufQueue) -> Vec<u8> {
    let mut out = Vec::with_capacity(chain.total_len());
    while let Some(buf) = chain.pop_front() {
        out.extend_from_slice(buf.readable());
    }
    out
}

/// Peer-handshake control payload exchanged on top of a
/// [`DmsgType::GossipSyn`] frame.
///
/// Today the handshake carries the cluster-wide capability
/// advertisement (see [`crate::cluster::capability`]). Future
/// fields will be appended as new typed records; older peers
/// ignore unknown trailing bytes.
///
/// # Wire format
///
/// ```text
/// magic(4) = "DHS1"
/// flags(2) = 0
/// CapabilityAd (length-prefixed, see
///                `CapabilityAd::encode` for the exact layout)
/// ```
///
/// All multi-byte integers are little-endian. The encoding uses
/// only the standard library; no external codec is pulled in.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::capability::{CapabilityAd, CapabilityAdEntry};
/// use dynomite::proto::dnode::Handshake;
/// let ad = CapabilityAd::from_entries(vec![
///     CapabilityAdEntry::new("framing".into(), vec![vec![1, 0, 0, 0]]),
/// ]);
/// let hs = Handshake::new(ad.clone());
/// let bytes = hs.encode();
/// let back = Handshake::decode(&bytes).unwrap();
/// assert_eq!(back.capabilities(), &ad);
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Handshake {
    capabilities: crate::cluster::capability::CapabilityAd,
}

impl Handshake {
    /// Magic literal that opens every handshake payload.
    pub const MAGIC: [u8; 4] = *b"DHS1";

    /// Build a handshake carrying `capabilities`.
    #[must_use]
    pub fn new(capabilities: crate::cluster::capability::CapabilityAd) -> Self {
        Self { capabilities }
    }

    /// Borrow the embedded capability advertisement.
    #[must_use]
    pub fn capabilities(&self) -> &crate::cluster::capability::CapabilityAd {
        &self.capabilities
    }

    /// Consume the handshake and return the embedded
    /// advertisement.
    #[must_use]
    pub fn into_capabilities(self) -> crate::cluster::capability::CapabilityAd {
        self.capabilities
    }

    /// Serialise the handshake to a length-prefixed byte
    /// stream.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let cap_bytes = self.capabilities.encode();
        let mut out = Vec::with_capacity(Self::MAGIC.len() + 2 + cap_bytes.len());
        out.extend_from_slice(&Self::MAGIC);
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&cap_bytes);
        out
    }

    /// Inverse of [`Handshake::encode`]. Surfaces a typed error
    /// when the magic / version is wrong or the embedded
    /// advertisement is malformed.
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::cluster::capability::CapabilityCodecError> {
        use crate::cluster::capability::CapabilityCodecError;
        if bytes.len() < Self::MAGIC.len() + 2 {
            return Err(CapabilityCodecError::Truncated);
        }
        if bytes[..Self::MAGIC.len()] != Self::MAGIC {
            return Err(CapabilityCodecError::BadMagic);
        }
        // Flags are reserved; the only currently legal value is
        // zero. Any non-zero value is reserved for future use
        // and rejected here so older builds fail closed.
        let flags_off = Self::MAGIC.len();
        let flags = u16::from_le_bytes([bytes[flags_off], bytes[flags_off + 1]]);
        if flags != 0 {
            return Err(CapabilityCodecError::BadMagic);
        }
        let cap_bytes = &bytes[flags_off + 2..];
        let capabilities = crate::cluster::capability::CapabilityAd::decode(cap_bytes)?;
        Ok(Self { capabilities })
    }

    /// Number of bytes the handshake's fixed-size prefix
    /// occupies before the embedded advertisement. Useful in
    /// tests that assert the on-the-wire delta.
    #[must_use]
    pub const fn header_len() -> usize {
        Self::MAGIC.len() + 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::mbuf::MbufPool;

    #[test]
    fn parse_simple_req() {
        let mut p = DnodeParser::new();
        let bytes = b"$2014$ 1 3 0 1 1 *1 d *0\r\n";
        match p.step(bytes) {
            ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
            other => panic!("unexpected: {other:?}"),
        }
        let d = p.take_dmsg();
        assert_eq!(d.id, 1);
        assert_eq!(d.ty, DmsgType::Req);
        assert_eq!(d.flags, 0);
        assert_eq!(d.version, 1);
        assert!(d.same_dc);
        assert_eq!(d.mlen, 1);
        assert_eq!(d.data, b"d");
        assert_eq!(d.plen, 0);
    }

    #[test]
    fn parse_payload_len() {
        let mut p = DnodeParser::new();
        let bytes = b"$2014$ 2 3 0 1 1 *1 d *413\r\n";
        match p.step(bytes) {
            ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(p.dmsg().plen, 413);
    }

    #[test]
    fn parse_three_back_to_back() {
        let mut input: Vec<u8> = Vec::new();
        input.extend_from_slice(b"$2014$ 1 3 0 1 1 *1 d *0\r\n");
        input.extend_from_slice(b"some redis bytes here ignored");
        input.extend_from_slice(b"$2014$ 2 3 0 1 1 *1 d *3\r\nABC");
        input.extend_from_slice(b"$2014$ 3 3 0 1 1 *1 d *0\r\n");
        let mut p = DnodeParser::new();
        let mut idx = 0;
        let mut count = 0;
        while idx < input.len() {
            match p.step(&input[idx..]) {
                ParseStep::HeaderDone { consumed } => {
                    let d = p.take_dmsg();
                    count += 1;
                    let after_header = idx + consumed;
                    if count == 1 {
                        assert_eq!(d.id, 1);
                        // skip past the redis bytes by scanning for the next '$'
                        idx = input[after_header..]
                            .iter()
                            .position(|&b| b == b'$')
                            .map_or(input.len(), |n| after_header + n);
                    } else if count == 2 {
                        assert_eq!(d.id, 2);
                        assert_eq!(d.plen, 3);
                        idx = after_header + d.plen as usize;
                    } else {
                        assert_eq!(d.id, 3);
                        idx = after_header;
                    }
                    p.reset();
                }
                ParseStep::NeedMore { .. } => {
                    break;
                }
                ParseStep::Error { consumed } => {
                    idx += consumed.max(1);
                    p.reset();
                }
            }
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn need_more_when_truncated() {
        let mut p = DnodeParser::new();
        let prefix = b"$2014$ 1 3 0 1 1 *1 d *";
        match p.step(prefix) {
            ParseStep::NeedMore { consumed } => assert_eq!(consumed, prefix.len()),
            other => panic!("unexpected: {other:?}"),
        }
        let suffix = b"42\r\n";
        match p.step(suffix) {
            ParseStep::HeaderDone { consumed } => assert_eq!(consumed, suffix.len()),
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(p.take_dmsg().plen, 42);
    }

    #[test]
    fn parse_error_on_garbage_prefix() {
        let mut p = DnodeParser::new();
        match p.step(b"!nope") {
            ParseStep::Error { consumed } => assert_eq!(consumed, 0),
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// Regression for the libfuzzer 1h soak finding 2026-06-02:
    /// a numeric DataLen field that exceeds [`MAX_DATA_LEN`]
    /// must be rejected with `ParseStep::Error` BEFORE the
    /// downstream `Vec::reserve` would convert the wrapped u32
    /// into a multi-gigabyte malloc.
    #[test]
    fn parse_rejects_oversized_data_len() {
        let mut p = DnodeParser::new();
        // 11 ones drives self.num to 11_111_111_111, which casts
        // to u32 as 2_521_176_519 (~2.4 GiB). Pre-fix the parser
        // accepted this and called Vec::reserve(2_521_176_519).
        let bytes = b"$2014$ 1 3 0 1 1 *11111111111 ";
        match p.step(bytes) {
            ParseStep::Error { consumed: _ } => (),
            other => panic!("expected Error, got {other:?}"),
        }
    }

    /// Regression for the libfuzzer 1h soak finding 2026-06-02:
    /// the captured 112-byte OOM artifact must drive `step()`
    /// to a clean Error rather than allocating gigabytes.
    #[test]
    fn parse_oom_artifact_2026_06_02() {
        let bytes = include_bytes!("../../../fuzz/seeds/dnode_parse/regression-oom-2026-06-02");
        let mut p = DnodeParser::new();
        match p.step(bytes) {
            ParseStep::Error { .. } | ParseStep::HeaderDone { .. } => (),
            ParseStep::NeedMore { .. } => panic!("unexpected NeedMore"),
        }
    }

    #[test]
    fn writer_round_trip_unencrypted() {
        let pool = MbufPool::default();
        let mut buf = pool.get();
        dmsg_write(&mut buf, 42, DmsgType::Req, 0, true, None, 0).unwrap();
        let bytes = buf.readable().to_vec();
        let mut p = DnodeParser::new();
        let step = p.step(&bytes);
        assert!(matches!(step, ParseStep::HeaderDone { .. }));
        let d = p.take_dmsg();
        assert_eq!(d.id, 42);
        assert_eq!(d.ty, DmsgType::Req);
        assert_eq!(d.flags, 0);
        assert!(d.same_dc);
        assert_eq!(d.mlen, 1);
        assert_eq!(d.data, b"d");
        assert_eq!(d.plen, 0);
    }

    #[test]
    fn writer_round_trip_with_aes_payload() {
        let pool = MbufPool::default();
        let mut buf = pool.get();
        let payload = vec![0xAB; 128];
        dmsg_write(
            &mut buf,
            7,
            DmsgType::CryptoHandshake,
            DMSG_FLAG_ENCRYPTED,
            false,
            Some(&payload),
            512,
        )
        .unwrap();
        let bytes = buf.readable().to_vec();
        let mut p = DnodeParser::new();
        match p.step(&bytes) {
            ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
            other => panic!("unexpected: {other:?}"),
        }
        let d = p.take_dmsg();
        assert_eq!(d.id, 7);
        assert_eq!(d.ty, DmsgType::CryptoHandshake);
        assert!(d.is_encrypted());
        assert!(!d.same_dc);
        assert_eq!(d.data, payload);
        assert_eq!(d.plen, 512);
    }

    #[test]
    fn dispatcher_classifies_control_plane() {
        let mut d = Dmsg::new();
        // Pin the exact three variants the C `dmsg_process`
        // bypasses.
        for ty in [
            DmsgType::CryptoHandshake,
            DmsgType::GossipSyn,
            DmsgType::GossipSynReply,
        ] {
            d.ty = ty;
            assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass);
        }
        // Every other gossip variant falls through to the default
        // branch (forward), matching the C switch.
        for ty in [
            DmsgType::GossipAck,
            DmsgType::GossipDigestSyn,
            DmsgType::GossipDigestAck,
            DmsgType::GossipDigestAck2,
            DmsgType::GossipShutdown,
            DmsgType::Req,
            DmsgType::ReqForward,
            DmsgType::Res,
        ] {
            d.ty = ty;
            assert_eq!(dmsg_process(&d), DmsgDispatch::Forward);
        }
        // HandoffChunk routes to the explicit handoff coordinator
        // and is therefore bypassed alongside the handshake
        // variants.
        d.ty = DmsgType::HandoffChunk;
        assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass);
        // FT.SEARCH coordinator messages are routed to the
        // dedicated query-fsm coordinator via the same
        // bypass path used by the handoff coordinator.
        for ty in [DmsgType::FtSearchReq, DmsgType::FtSearchRep] {
            d.ty = ty;
            assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass);
        }
        // Cross-node XA frames are routed to the dyniak XA
        // handler and bypass the data plane the same way.
        for ty in [
            DmsgType::XaPrepare,
            DmsgType::XaVote,
            DmsgType::XaCommit,
            DmsgType::XaRollback,
            DmsgType::XaAck,
        ] {
            d.ty = ty;
            assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass);
        }
        // Cross-node RAMP frames route to the dyniak RAMP handler
        // and bypass the data plane the same way.
        d.ty = DmsgType::RampPrepare;
        assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass);
        // Dyniak cross-node object-replica ops are routed to the
        // dyniak replica sink and bypass the data plane the same
        // way, so a replica apply fans out exactly once.
        d.ty = DmsgType::RiakReplica;
        assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass);
    }
}
