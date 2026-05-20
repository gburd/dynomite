//! Memcached text-protocol parser.
//!
//! The parser is a single byte-driven state machine for requests
//! and another for responses, faithfully reproducing the state set
//! and transitions of `memcache_parse_req` and `memcache_parse_rsp`
//! in the reference engine. It mutates a [`Msg`] in place: the
//! command tag, key list, value-length accumulator, and parser
//! cursor are written back so the streaming caller can resume the
//! machine on more bytes without re-allocating state.
//!
//! The parsers MUST NOT panic on any input. Invalid bytes are
//! reported via [`MsgParseResult::Error`].

// The parser truncates ASCII-decimal accumulators into fixed-width
// counters that match the reference engine (`uint32_t` for vlen,
// `usize` for cursor offsets). The allowance keeps the Rust port
// faithful to the C casts.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::needless_continue)]
// The state machine deliberately keeps the C `if (token == NULL)`
// guard pattern; rewriting as `let-else` collapses two branches
// the reference engine treats independently.
#![allow(clippy::manual_let_else)]
#![allow(clippy::redundant_else)]

use super::commands::{
    memcache_arithmetic, memcache_cas, memcache_delete, memcache_retrieval, memcache_storage,
    memcache_touch,
};
use crate::msg::{KeyPos, Msg, MsgParseResult, MsgType};

/// Maximum allowed Memcached key length in bytes.
pub const MEMCACHE_MAX_KEY_LENGTH: usize = 250;

/// Optional hash tag delimiters. When set, parsed keys carry the
/// inner range between the delimiters as the routing tag.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct HashTag {
    /// Opening byte of the hash tag.
    pub open: u8,
    /// Closing byte of the hash tag.
    pub close: u8,
}

/// State alphabet for [`memcache_parse_req`].
///
/// The numeric values match the reference engine's request state
/// indices so external parity tooling can compare them directly.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[repr(u32)]
pub enum ReqState {
    /// Initial state.
    #[default]
    Start = 0,
    /// Reading the command keyword.
    ReqType = 1,
    /// Skipping spaces before the first key.
    SpacesBeforeKey = 2,
    /// Reading a key.
    Key = 3,
    /// Skipping spaces between keys (`get`/`gets`).
    SpacesBeforeKeys = 4,
    /// Skipping spaces before the storage flags field.
    SpacesBeforeFlags = 5,
    /// Reading the storage flags field.
    Flags = 6,
    /// Skipping spaces before the storage expiry field.
    SpacesBeforeExpiry = 7,
    /// Reading the storage expiry field.
    Expiry = 8,
    /// Skipping spaces before the value-length field.
    SpacesBeforeVlen = 9,
    /// Reading the value-length field.
    Vlen = 10,
    /// Skipping spaces before the CAS unique field.
    SpacesBeforeCas = 11,
    /// Reading the CAS unique field.
    Cas = 12,
    /// Awaiting LF before the value bytes.
    RuntoVal = 13,
    /// Consuming the value bytes.
    Val = 14,
    /// Skipping spaces before the arithmetic numeric argument.
    SpacesBeforeNum = 15,
    /// Reading the arithmetic numeric argument.
    Num = 16,
    /// Eating optional trailing bytes up to CR.
    RuntoCrlf = 17,
    /// Awaiting trailing CR.
    Crlf = 18,
    /// Reading the optional `noreply` token.
    Noreply = 19,
    /// State after consuming `noreply`.
    AfterNoreply = 20,
    /// Awaiting the trailing LF that terminates the request.
    AlmostDone = 21,
}

impl ReqState {
    fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::ReqType,
            2 => Self::SpacesBeforeKey,
            3 => Self::Key,
            4 => Self::SpacesBeforeKeys,
            5 => Self::SpacesBeforeFlags,
            6 => Self::Flags,
            7 => Self::SpacesBeforeExpiry,
            8 => Self::Expiry,
            9 => Self::SpacesBeforeVlen,
            10 => Self::Vlen,
            11 => Self::SpacesBeforeCas,
            12 => Self::Cas,
            13 => Self::RuntoVal,
            14 => Self::Val,
            15 => Self::SpacesBeforeNum,
            16 => Self::Num,
            17 => Self::RuntoCrlf,
            18 => Self::Crlf,
            19 => Self::Noreply,
            20 => Self::AfterNoreply,
            21 => Self::AlmostDone,
            _ => Self::Start,
        }
    }
}

/// State alphabet for [`memcache_parse_rsp`].
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[repr(u32)]
pub enum RspState {
    /// Initial state.
    #[default]
    Start = 0,
    /// Reading a numeric response (`incr`/`decr` reply).
    RspNum = 1,
    /// Reading a textual response keyword.
    RspStr = 2,
    /// Skipping spaces before the key in a `VALUE` reply.
    SpacesBeforeKey = 3,
    /// Reading the key portion of a `VALUE` reply.
    Key = 4,
    /// Skipping spaces before the flags field.
    SpacesBeforeFlags = 5,
    /// Reading the flags field.
    Flags = 6,
    /// Skipping spaces before the value-length field.
    SpacesBeforeVlen = 7,
    /// Reading the value-length field.
    Vlen = 8,
    /// Awaiting LF before the value bytes.
    RuntoVal = 9,
    /// Consuming the value bytes.
    Val = 10,
    /// Awaiting LF after the value.
    ValLf = 11,
    /// Reading the trailing `END` token.
    End = 12,
    /// Eating optional trailing bytes up to CR.
    RuntoCrlf = 13,
    /// Awaiting trailing CR.
    Crlf = 14,
    /// Awaiting the trailing LF that terminates the response.
    AlmostDone = 15,
}

impl RspState {
    fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::RspNum,
            2 => Self::RspStr,
            3 => Self::SpacesBeforeKey,
            4 => Self::Key,
            5 => Self::SpacesBeforeFlags,
            6 => Self::Flags,
            7 => Self::SpacesBeforeVlen,
            8 => Self::Vlen,
            9 => Self::RuntoVal,
            10 => Self::Val,
            11 => Self::ValLf,
            12 => Self::End,
            13 => Self::RuntoCrlf,
            14 => Self::Crlf,
            15 => Self::AlmostDone,
            _ => Self::Start,
        }
    }
}

const CR: u8 = b'\r';
const LF: u8 = b'\n';

fn classify_command(token: &[u8]) -> MsgType {
    match token {
        b"get" => MsgType::ReqMcGet,
        b"gets" => MsgType::ReqMcGets,
        b"set" => MsgType::ReqMcSet,
        b"add" => MsgType::ReqMcAdd,
        b"cas" => MsgType::ReqMcCas,
        b"incr" => MsgType::ReqMcIncr,
        b"decr" => MsgType::ReqMcDecr,
        b"quit" => MsgType::ReqMcQuit,
        b"touch" => MsgType::ReqMcTouch,
        b"append" => MsgType::ReqMcAppend,
        b"delete" => MsgType::ReqMcDelete,
        b"prepend" => MsgType::ReqMcPrepend,
        b"replace" => MsgType::ReqMcReplace,
        _ => MsgType::Unknown,
    }
}

fn classify_response(token: &[u8]) -> MsgType {
    match token {
        b"END" => MsgType::RspMcEnd,
        b"VALUE" => MsgType::RspMcValue,
        b"ERROR" => MsgType::RspMcError,
        b"STORED" => MsgType::RspMcStored,
        b"EXISTS" => MsgType::RspMcExists,
        b"DELETED" => MsgType::RspMcDeleted,
        b"TOUCHED" => MsgType::RspMcTouched,
        b"NOT_FOUND" => MsgType::RspMcNotFound,
        b"NOT_STORED" => MsgType::RspMcNotStored,
        b"CLIENT_ERROR" => MsgType::RspMcClientError,
        b"SERVER_ERROR" => MsgType::RspMcServerError,
        _ => MsgType::Unknown,
    }
}

fn make_keypos(input: &[u8], start: usize, end: usize, hash_tag: Option<HashTag>) -> KeyPos {
    let bytes = input[start..end].to_vec();
    if let Some(tag) = hash_tag {
        if let Some(open_idx) = bytes.iter().position(|&b| b == tag.open) {
            if let Some(close_offset) = bytes[open_idx + 1..].iter().position(|&b| b == tag.close) {
                let tag_start = open_idx + 1;
                let tag_end = open_idx + 1 + close_offset;
                return KeyPos::new(bytes, tag_start..tag_end);
            }
        }
    }
    KeyPos::without_tag(bytes)
}

/// Parse a Memcached request from `input` and update `r` in place.
///
/// The function reproduces the reference engine's
/// `memcache_parse_req` state machine. On success, `r.ty()` is set
/// to the recognised command, parsed keys are appended to
/// [`Msg::keys`], and the parser cursor (`parser_pos`) advances
/// just past the trailing LF. On truncated input the function
/// returns [`MsgParseResult::Again`] and stores the partial state
/// on `r` for resumption. Invalid bytes return
/// [`MsgParseResult::Error`].
///
/// `hash_tag`, when set, configures the routing-tag delimiters used
/// when populating [`KeyPos::tag`].
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgParseResult, MsgType};
/// use dynomite::proto::memcache::memcache_parse_req;
///
/// let mut r = Msg::new(0, MsgType::Unknown, true);
/// let res = memcache_parse_req(&mut r, b"set foo 0 0 3\r\nbar\r\n");
/// assert_eq!(res, MsgParseResult::Ok);
/// assert_eq!(r.ty(), MsgType::ReqMcSet);
/// assert_eq!(r.keys()[0].key(), b"foo");
/// assert_eq!(r.vlen(), 3);
/// ```
///
/// The state machine intentionally lives in a single function to
/// match the reference engine's parser shape.
#[allow(clippy::too_many_lines)]
pub fn memcache_parse_req(r: &mut Msg, input: &[u8]) -> MsgParseResult {
    memcache_parse_req_tagged(r, input, None)
}

/// Variant of [`memcache_parse_req`] that accepts an explicit
/// hash-tag configuration.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgType};
/// use dynomite::proto::memcache::parser::{memcache_parse_req_tagged, HashTag};
///
/// let mut r = Msg::new(0, MsgType::Unknown, true);
/// let tag = Some(HashTag { open: b'{', close: b'}' });
/// let _ = memcache_parse_req_tagged(&mut r, b"get {abc}xyz\r\n", tag);
/// assert_eq!(r.keys()[0].tag_bytes(), b"abc");
/// ```
#[allow(clippy::too_many_lines)]
pub fn memcache_parse_req_tagged(
    r: &mut Msg,
    input: &[u8],
    hash_tag: Option<HashTag>,
) -> MsgParseResult {
    if !r.is_request() {
        r.set_parse_result(MsgParseResult::Error);
        return MsgParseResult::Error;
    }
    let mut state = ReqState::from_u32(r.parser_state());
    let mut p = r.parser_pos();
    let mut token: Option<usize> = r.parser_token();
    let mut vlen = r.vlen();
    let mut ty = r.ty();
    let mut is_read = r.flags().is_read;
    let mut quit = r.flags().quit;
    let mut expect_reply = r.flags().expect_datastore_reply;
    let mut ntokens = r.ntokens();

    'machine: while p < input.len() {
        let ch = input[p];
        match state {
            ReqState::Start => {
                if ch == b' ' {
                    p += 1;
                    continue;
                }
                if !ch.is_ascii_lowercase() {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
                token = Some(p);
                state = ReqState::ReqType;
                // Do not advance; re-enter ReqType on the same byte.
            }
            ReqState::ReqType => {
                if ch == b' ' || ch == CR {
                    let start = match token {
                        Some(s) => s,
                        None => {
                            return finish_error(
                                r, state, p, token, vlen, ty, is_read, quit, ntokens,
                            );
                        }
                    };
                    let cmd = &input[start..p];
                    token = None;
                    ty = classify_command(cmd);
                    ntokens = ntokens.saturating_add(1);
                    is_read = matches!(
                        ty,
                        MsgType::ReqMcGet | MsgType::ReqMcGets | MsgType::ReqMcQuit
                    );
                    if matches!(ty, MsgType::ReqMcQuit) {
                        quit = true;
                        // The C parser sets state to SW_CRLF and steps p back by one.
                        state = ReqState::Crlf;
                        // Do not advance; re-enter on this same byte.
                        continue;
                    }
                    match ty {
                        MsgType::ReqMcGet
                        | MsgType::ReqMcGets
                        | MsgType::ReqMcDelete
                        | MsgType::ReqMcCas
                        | MsgType::ReqMcSet
                        | MsgType::ReqMcAdd
                        | MsgType::ReqMcReplace
                        | MsgType::ReqMcAppend
                        | MsgType::ReqMcPrepend
                        | MsgType::ReqMcIncr
                        | MsgType::ReqMcDecr
                        | MsgType::ReqMcTouch => {
                            if ch == CR {
                                return finish_error(
                                    r, state, p, token, vlen, ty, is_read, quit, ntokens,
                                );
                            }
                            state = ReqState::SpacesBeforeKey;
                            p += 1;
                            continue;
                        }
                        MsgType::Unknown => {
                            return finish_error(
                                r, state, p, token, vlen, ty, is_read, quit, ntokens,
                            );
                        }
                        _ => {
                            return finish_error(
                                r, state, p, token, vlen, ty, is_read, quit, ntokens,
                            );
                        }
                    }
                } else if !ch.is_ascii_lowercase() {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                } else {
                    p += 1;
                }
            }
            ReqState::SpacesBeforeKey => {
                if ch == b' ' {
                    p += 1;
                } else {
                    token = None;
                    state = ReqState::Key;
                    // Do not advance; re-process this byte under Key state.
                }
            }
            ReqState::Key => {
                if token.is_none() {
                    token = Some(p);
                }
                if ch == b' ' || ch == CR {
                    let start = token.expect("token recorded");
                    let keylen = p - start;
                    if keylen == 0 || keylen > MEMCACHE_MAX_KEY_LENGTH {
                        return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                    }
                    let kp = make_keypos(input, start, p, hash_tag);
                    r.push_key(kp);
                    ntokens = ntokens.saturating_add(1);
                    token = None;
                    let storage = memcache_storage(ty);
                    let arithmetic = memcache_arithmetic(ty);
                    let touch = memcache_touch(ty);
                    let delete = memcache_delete(ty);
                    let retrieval = memcache_retrieval(ty);
                    if storage {
                        state = ReqState::SpacesBeforeFlags;
                    } else if arithmetic || touch {
                        state = ReqState::SpacesBeforeNum;
                    } else if delete {
                        state = ReqState::RuntoCrlf;
                    } else if retrieval {
                        state = ReqState::SpacesBeforeKeys;
                    } else {
                        state = ReqState::RuntoCrlf;
                    }
                    if ch == CR {
                        if storage || arithmetic {
                            return finish_error(
                                r, state, p, token, vlen, ty, is_read, quit, ntokens,
                            );
                        }
                        // Re-enter on the CR byte (do not advance).
                    } else {
                        p += 1;
                    }
                } else {
                    p += 1;
                }
            }
            ReqState::SpacesBeforeKeys => {
                match ch {
                    b' ' => {
                        p += 1;
                    }
                    CR => {
                        state = ReqState::AlmostDone;
                        p += 1;
                    }
                    _ => {
                        token = None;
                        state = ReqState::Key;
                        // Do not advance.
                    }
                }
            }
            ReqState::SpacesBeforeFlags => {
                if ch == b' ' {
                    p += 1;
                } else if ch.is_ascii_digit() {
                    token = Some(p);
                    state = ReqState::Flags;
                    p += 1;
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::Flags => {
                if ch.is_ascii_digit() {
                    p += 1;
                } else if ch == b' ' {
                    token = None;
                    state = ReqState::SpacesBeforeExpiry;
                    p += 1;
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::SpacesBeforeExpiry => {
                if ch == b' ' {
                    p += 1;
                } else if ch.is_ascii_digit() {
                    token = Some(p);
                    state = ReqState::Expiry;
                    p += 1;
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::Expiry => {
                if ch.is_ascii_digit() {
                    p += 1;
                } else if ch == b' ' {
                    token = None;
                    state = ReqState::SpacesBeforeVlen;
                    p += 1;
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::SpacesBeforeVlen => {
                if ch == b' ' {
                    p += 1;
                } else if ch.is_ascii_digit() {
                    vlen = u32::from(ch - b'0');
                    state = ReqState::Vlen;
                    p += 1;
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::Vlen => {
                if ch.is_ascii_digit() {
                    vlen = vlen.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if memcache_cas(ty) {
                    if ch != b' ' {
                        return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                    }
                    token = None;
                    state = ReqState::SpacesBeforeCas;
                    // Do not advance; re-enter on the same byte.
                } else if ch == b' ' || ch == CR {
                    token = None;
                    state = ReqState::RuntoCrlf;
                    // Do not advance.
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::SpacesBeforeCas => {
                if ch == b' ' {
                    p += 1;
                } else if ch.is_ascii_digit() {
                    token = Some(p);
                    state = ReqState::Cas;
                    p += 1;
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::Cas => {
                if ch.is_ascii_digit() {
                    p += 1;
                } else if ch == b' ' || ch == CR {
                    token = None;
                    state = ReqState::RuntoCrlf;
                    // Do not advance.
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::RuntoVal => match ch {
                LF => {
                    state = ReqState::Val;
                    p += 1;
                }
                _ => {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            },
            ReqState::Val => {
                let m = p.saturating_add(vlen as usize);
                if m >= input.len() {
                    let consumed = input.len() - p;
                    vlen = vlen.saturating_sub(consumed as u32);
                    p = input.len();
                    break 'machine;
                }
                if input[m] != CR {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
                p = m + 1;
                state = ReqState::AlmostDone;
            }
            ReqState::SpacesBeforeNum => {
                if ch == b' ' {
                    p += 1;
                } else if ch.is_ascii_digit() || ch == b'-' {
                    token = Some(p);
                    state = ReqState::Num;
                    p += 1;
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::Num => {
                if ch.is_ascii_digit() {
                    p += 1;
                } else if ch == b' ' || ch == CR {
                    token = None;
                    state = ReqState::RuntoCrlf;
                    // Do not advance.
                } else {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            }
            ReqState::RuntoCrlf => match ch {
                b' ' => {
                    p += 1;
                }
                b'n' => {
                    if memcache_storage(ty)
                        || memcache_arithmetic(ty)
                        || memcache_delete(ty)
                        || memcache_touch(ty)
                    {
                        token = Some(p);
                        state = ReqState::Noreply;
                        p += 1;
                    } else {
                        return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                    }
                }
                CR => {
                    if memcache_storage(ty) {
                        state = ReqState::RuntoVal;
                    } else {
                        state = ReqState::AlmostDone;
                    }
                    p += 1;
                }
                _ => {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            },
            ReqState::Noreply => match ch {
                b' ' | CR => {
                    let start = match token {
                        Some(s) => s,
                        None => {
                            return finish_error(
                                r, state, p, token, vlen, ty, is_read, quit, ntokens,
                            );
                        }
                    };
                    if p - start == 7 && &input[start..p] == b"noreply" {
                        token = None;
                        expect_reply = false;
                        state = ReqState::AfterNoreply;
                        // Do not advance.
                    } else {
                        return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                    }
                }
                _ => {
                    p += 1;
                }
            },
            ReqState::AfterNoreply => match ch {
                b' ' => {
                    p += 1;
                }
                CR => {
                    if memcache_storage(ty) {
                        state = ReqState::RuntoVal;
                    } else {
                        state = ReqState::AlmostDone;
                    }
                    p += 1;
                }
                _ => {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            },
            ReqState::Crlf => match ch {
                b' ' => {
                    p += 1;
                }
                CR => {
                    state = ReqState::AlmostDone;
                    p += 1;
                }
                _ => {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            },
            ReqState::AlmostDone => match ch {
                LF => {
                    return finish_done(r, p + 1, ty, is_read, quit, expect_reply, ntokens, vlen);
                }
                _ => {
                    return finish_error(r, state, p, token, vlen, ty, is_read, quit, ntokens);
                }
            },
        }
    }

    // Reached end of input without completing.
    r.set_parser_state(state as u32);
    r.set_parser_pos(p);
    r.set_parser_token(token);
    r.set_vlen(vlen);
    r.set_ntokens(ntokens);
    if ty != MsgType::Unknown {
        r.set_type(ty);
    }
    r.flags_mut().is_read = is_read;
    r.flags_mut().quit = quit;
    r.flags_mut().expect_datastore_reply = expect_reply;
    r.set_parse_result(MsgParseResult::Again);
    MsgParseResult::Again
}

#[allow(clippy::too_many_arguments)]
fn finish_done(
    r: &mut Msg,
    next_pos: usize,
    ty: MsgType,
    is_read: bool,
    quit: bool,
    expect_reply: bool,
    ntokens: u32,
    vlen: u32,
) -> MsgParseResult {
    r.set_type(ty);
    r.flags_mut().is_read = is_read;
    r.flags_mut().quit = quit;
    r.flags_mut().expect_datastore_reply = expect_reply;
    r.set_ntokens(ntokens);
    r.set_vlen(vlen);
    r.set_parser_state(ReqState::Start as u32);
    r.set_parser_pos(next_pos);
    r.set_parser_token(None);
    r.set_parse_result(MsgParseResult::Ok);
    MsgParseResult::Ok
}

#[allow(clippy::too_many_arguments)]
fn finish_error(
    r: &mut Msg,
    state: ReqState,
    pos: usize,
    token: Option<usize>,
    vlen: u32,
    ty: MsgType,
    is_read: bool,
    quit: bool,
    ntokens: u32,
) -> MsgParseResult {
    r.set_parser_state(state as u32);
    r.set_parser_pos(pos);
    r.set_parser_token(token);
    r.set_vlen(vlen);
    r.set_ntokens(ntokens);
    if ty != MsgType::Unknown {
        r.set_type(ty);
    }
    r.flags_mut().is_read = is_read;
    r.flags_mut().quit = quit;
    r.set_parse_result(MsgParseResult::Error);
    MsgParseResult::Error
}

/// Parse a Memcached response from `input` and update `r` in place.
///
/// On success the response type is recorded and the parser cursor
/// advances just past the trailing LF. The function never panics
/// on any byte sequence.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgParseResult, MsgType};
/// use dynomite::proto::memcache::memcache_parse_rsp;
///
/// let mut r = Msg::new(0, MsgType::Unknown, false);
/// let res = memcache_parse_rsp(&mut r, b"STORED\r\n");
/// assert_eq!(res, MsgParseResult::Ok);
/// assert_eq!(r.ty(), MsgType::RspMcStored);
/// ```
///
/// The state machine intentionally lives in a single function to
/// match the reference engine.
#[allow(clippy::too_many_lines)]
pub fn memcache_parse_rsp(r: &mut Msg, input: &[u8]) -> MsgParseResult {
    if r.is_request() {
        r.set_parse_result(MsgParseResult::Error);
        return MsgParseResult::Error;
    }
    let mut state = RspState::from_u32(r.parser_state());
    let mut p = r.parser_pos();
    let mut token: Option<usize> = r.parser_token();
    let mut vlen = r.vlen();
    let mut ty = r.ty();
    let mut end_marker = r.end_marker();

    while p < input.len() {
        let ch = input[p];
        match state {
            RspState::Start => {
                if ch.is_ascii_digit() {
                    state = RspState::RspNum;
                } else {
                    state = RspState::RspStr;
                }
                // Do not advance; re-enter under the new state.
            }
            RspState::RspNum => {
                if token.is_none() {
                    token = Some(p);
                }
                if ch.is_ascii_digit() {
                    p += 1;
                } else if ch == b' ' || ch == CR {
                    token = None;
                    ty = MsgType::RspMcNum;
                    state = RspState::Crlf;
                    // Do not advance.
                } else {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            }
            RspState::RspStr => {
                if token.is_none() {
                    token = Some(p);
                }
                if ch == b' ' || ch == CR {
                    let start = token.expect("token recorded");
                    let key_bytes = &input[start..p];
                    ty = classify_response(key_bytes);
                    if ty == MsgType::RspMcEnd {
                        end_marker = Some(start);
                    }
                    match ty {
                        MsgType::Unknown => {
                            return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                        }
                        MsgType::RspMcStored
                        | MsgType::RspMcNotStored
                        | MsgType::RspMcExists
                        | MsgType::RspMcNotFound
                        | MsgType::RspMcDeleted
                        | MsgType::RspMcTouched
                        | MsgType::RspMcEnd
                        | MsgType::RspMcError => {
                            state = RspState::Crlf;
                        }
                        MsgType::RspMcValue => {
                            state = RspState::SpacesBeforeKey;
                        }
                        MsgType::RspMcClientError | MsgType::RspMcServerError => {
                            state = RspState::RuntoCrlf;
                        }
                        _ => {
                            return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                        }
                    }
                    // Do not advance; re-enter on the same byte.
                } else {
                    p += 1;
                }
            }
            RspState::SpacesBeforeKey => {
                if ch == b' ' {
                    p += 1;
                } else {
                    state = RspState::Key;
                    // Do not advance.
                }
            }
            RspState::Key => {
                if ch == b' ' {
                    state = RspState::SpacesBeforeFlags;
                }
                p += 1;
            }
            RspState::SpacesBeforeFlags => {
                if ch == b' ' {
                    p += 1;
                } else if ch.is_ascii_digit() {
                    state = RspState::Flags;
                    // Do not advance.
                } else {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            }
            RspState::Flags => {
                if ch.is_ascii_digit() {
                    p += 1;
                } else if ch == b' ' {
                    state = RspState::SpacesBeforeVlen;
                    p += 1;
                } else {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            }
            RspState::SpacesBeforeVlen => {
                if ch == b' ' {
                    p += 1;
                } else if ch.is_ascii_digit() {
                    state = RspState::Vlen;
                    vlen = 0;
                    // Do not advance.
                } else {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            }
            RspState::Vlen => {
                if ch.is_ascii_digit() {
                    vlen = vlen.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if ch == b' ' || ch == CR {
                    state = RspState::RuntoCrlf;
                    // Do not advance.
                } else {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            }
            RspState::RuntoVal => match ch {
                LF => {
                    state = RspState::Val;
                    token = None;
                    p += 1;
                }
                _ => {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            },
            RspState::Val => {
                let m = p.saturating_add(vlen as usize);
                if m >= input.len() {
                    let consumed = input.len() - p;
                    vlen = vlen.saturating_sub(consumed as u32);
                    p = input.len();
                    break;
                }
                if input[m] != CR {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
                p = m + 1;
                state = RspState::ValLf;
            }
            RspState::ValLf => match ch {
                LF => {
                    state = RspState::RspStr;
                    p += 1;
                }
                _ => {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            },
            RspState::End => {
                if token.is_none() {
                    if ch != b'E' {
                        return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                    }
                    token = Some(p);
                    p += 1;
                } else if ch == CR {
                    let start = token.expect("token recorded");
                    if p - start == 3 && &input[start..p] == b"END" {
                        end_marker = Some(start);
                        state = RspState::AlmostDone;
                        token = None;
                        p += 1;
                    } else {
                        return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                    }
                } else {
                    p += 1;
                }
            }
            RspState::RuntoCrlf => match ch {
                CR => {
                    if ty == MsgType::RspMcValue {
                        state = RspState::RuntoVal;
                    } else {
                        state = RspState::AlmostDone;
                    }
                    p += 1;
                }
                _ => {
                    p += 1;
                }
            },
            RspState::Crlf => match ch {
                b' ' => {
                    p += 1;
                }
                CR => {
                    state = RspState::AlmostDone;
                    p += 1;
                }
                _ => {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            },
            RspState::AlmostDone => match ch {
                LF => {
                    r.set_type(ty);
                    r.set_vlen(vlen);
                    r.set_end_marker(end_marker);
                    r.set_parser_state(RspState::Start as u32);
                    r.set_parser_pos(p + 1);
                    r.set_parser_token(None);
                    r.set_parse_result(MsgParseResult::Ok);
                    return MsgParseResult::Ok;
                }
                _ => {
                    return finish_error_rsp(r, state, p, token, vlen, ty, end_marker);
                }
            },
        }
    }

    r.set_parser_state(state as u32);
    r.set_parser_pos(p);
    r.set_parser_token(token);
    r.set_vlen(vlen);
    r.set_end_marker(end_marker);
    if ty != MsgType::Unknown {
        r.set_type(ty);
    }
    r.set_parse_result(MsgParseResult::Again);
    MsgParseResult::Again
}

fn finish_error_rsp(
    r: &mut Msg,
    state: RspState,
    pos: usize,
    token: Option<usize>,
    vlen: u32,
    ty: MsgType,
    end_marker: Option<usize>,
) -> MsgParseResult {
    r.set_parser_state(state as u32);
    r.set_parser_pos(pos);
    r.set_parser_token(token);
    r.set_vlen(vlen);
    r.set_end_marker(end_marker);
    if ty != MsgType::Unknown {
        r.set_type(ty);
    }
    r.set_parse_result(MsgParseResult::Error);
    MsgParseResult::Error
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_req(input: &[u8]) -> Msg {
        let mut m = Msg::new(0, MsgType::Unknown, true);
        let _ = memcache_parse_req(&mut m, input);
        m
    }

    fn parse_rsp(input: &[u8]) -> Msg {
        let mut m = Msg::new(0, MsgType::Unknown, false);
        let _ = memcache_parse_rsp(&mut m, input);
        m
    }

    #[test]
    fn parse_get() {
        let m = parse_req(b"get key1\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqMcGet);
        assert_eq!(m.keys()[0].key(), b"key1");
        assert!(m.flags().is_read);
    }

    #[test]
    fn parse_set() {
        let m = parse_req(b"set key1 0 0 3\r\nval\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqMcSet);
        assert_eq!(m.keys()[0].key(), b"key1");
        assert_eq!(m.vlen(), 3);
    }

    #[test]
    fn parse_set_noreply() {
        let m = parse_req(b"set key1 0 0 3 noreply\r\nval\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert!(!m.flags().expect_datastore_reply);
    }

    #[test]
    fn parse_delete() {
        let m = parse_req(b"delete key1\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqMcDelete);
    }

    #[test]
    fn parse_incr() {
        let m = parse_req(b"incr counter 1\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqMcIncr);
    }

    #[test]
    fn parse_quit() {
        let m = parse_req(b"quit\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqMcQuit);
        assert!(m.flags().quit);
    }

    #[test]
    fn parse_get_multikey() {
        let m = parse_req(b"get a b c\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        let keys: Vec<&[u8]> = m.keys().iter().map(crate::msg::KeyPos::key).collect();
        assert_eq!(keys, vec![&b"a"[..], b"b", b"c"]);
    }

    #[test]
    fn parse_cas() {
        let m = parse_req(b"cas key1 0 0 3 7\r\nval\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqMcCas);
    }

    #[test]
    fn parse_too_long_key_errors() {
        let mut input = b"get ".to_vec();
        input.extend(std::iter::repeat_n(b'k', MEMCACHE_MAX_KEY_LENGTH + 1));
        input.extend_from_slice(b"\r\n");
        let m = parse_req(&input);
        assert_eq!(m.parse_result(), MsgParseResult::Error);
    }

    #[test]
    fn parse_empty_key_errors() {
        let m = parse_req(b"get \r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Error);
    }

    #[test]
    fn parse_truncated_returns_again() {
        let m = parse_req(b"get key");
        assert_eq!(m.parse_result(), MsgParseResult::Again);
    }

    #[test]
    fn parse_stored_response() {
        let m = parse_rsp(b"STORED\r\n");
        assert_eq!(m.ty(), MsgType::RspMcStored);
    }

    #[test]
    fn parse_value_response() {
        let m = parse_rsp(b"VALUE key 0 3\r\nval\r\nEND\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspMcEnd);
    }

    #[test]
    fn parse_numeric_response() {
        let m = parse_rsp(b"42\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspMcNum);
    }

    #[test]
    fn parse_server_error_response() {
        let m = parse_rsp(b"SERVER_ERROR oops\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspMcServerError);
    }

    #[test]
    fn parse_response_unknown_keyword_errors() {
        let m = parse_rsp(b"BOGUS\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Error);
    }
}
