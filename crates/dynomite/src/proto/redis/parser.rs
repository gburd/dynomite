//! Redis (RESP) wire-protocol parser.
//!
//! The parser walks the bytes of a flattened request or response
//! buffer through a single state machine and updates the message
//! in place. The state alphabet, transitions, and error paths
//! mirror `redis_parse_req` and `redis_parse_rsp` in the reference
//! engine.
//!
//! The parser is byte-driven and never allocates outside the
//! [`Msg`]'s key and argument buffers. It must not panic on any
//! input.

// The parser truncates ASCII-decimal accumulators into the same
// fixed-width counters the reference engine uses (`uint32_t` for
// rlen / rntokens / nkeys, `usize` for cursor positions). The
// allowance keeps the Rust port faithful to the C casts; the
// out-of-range numerals surface as parse errors elsewhere in the
// state machine.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::manual_let_else)]

use super::commands::{classify, error_lookup, lookup, CommandClass, RoutingOverride};
use crate::io::mbuf::MBUF_MAX_SIZE;
use crate::msg::{ArgPos, KeyPos, Msg, MsgParseResult, MsgRouting, MsgType};

const CR: u8 = b'\r';
const LF: u8 = b'\n';

/// Optional hash-tag delimiters. When set the parser carves out
/// the inner range as the routing tag on every parsed key.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct HashTag {
    /// Opening byte of the hash tag.
    pub open: u8,
    /// Closing byte of the hash tag.
    pub close: u8,
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[repr(u32)]
enum ReqState {
    #[default]
    Start = 0,
    Narg = 1,
    NargLf = 2,
    ReqTypeLen = 3,
    ReqTypeLenLf = 4,
    ReqType = 5,
    ReqTypeLf = 6,
    KeyLen = 7,
    KeyLenLf = 8,
    Key = 9,
    KeyLf = 10,
    Arg1Len = 11,
    Arg1LenLf = 12,
    Arg1 = 13,
    Arg1Lf = 14,
    Arg2Len = 15,
    Arg2LenLf = 16,
    Arg2 = 17,
    Arg2Lf = 18,
    Arg3Len = 19,
    Arg3LenLf = 20,
    Arg3 = 21,
    Arg3Lf = 22,
    ArgnLen = 23,
    ArgnLenLf = 24,
    Argn = 25,
    ArgnLf = 26,
    InlinePing = 27,
}

impl ReqState {
    fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::Narg,
            2 => Self::NargLf,
            3 => Self::ReqTypeLen,
            4 => Self::ReqTypeLenLf,
            5 => Self::ReqType,
            6 => Self::ReqTypeLf,
            7 => Self::KeyLen,
            8 => Self::KeyLenLf,
            9 => Self::Key,
            10 => Self::KeyLf,
            11 => Self::Arg1Len,
            12 => Self::Arg1LenLf,
            13 => Self::Arg1,
            14 => Self::Arg1Lf,
            15 => Self::Arg2Len,
            16 => Self::Arg2LenLf,
            17 => Self::Arg2,
            18 => Self::Arg2Lf,
            19 => Self::Arg3Len,
            20 => Self::Arg3LenLf,
            21 => Self::Arg3,
            22 => Self::Arg3Lf,
            23 => Self::ArgnLen,
            24 => Self::ArgnLenLf,
            25 => Self::Argn,
            26 => Self::ArgnLf,
            27 => Self::InlinePing,
            _ => Self::Start,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
#[repr(u32)]
enum RspState {
    #[default]
    Start = 0,
    Status = 1,
    Error = 2,
    Integer = 3,
    IntegerStart = 4,
    Simple = 5,
    Bulk = 6,
    BulkLf = 7,
    BulkArg = 8,
    BulkArgLf = 9,
    Multibulk = 10,
    MultibulkNargLf = 11,
    MultibulkArgnLen = 12,
    MultibulkArgnLenLf = 13,
    MultibulkArgn = 14,
    MultibulkArgnLf = 15,
    RuntoCrlf = 16,
    AlmostDone = 17,
}

impl RspState {
    fn from_u32(v: u32) -> Self {
        match v {
            1 => Self::Status,
            2 => Self::Error,
            3 => Self::Integer,
            4 => Self::IntegerStart,
            5 => Self::Simple,
            6 => Self::Bulk,
            7 => Self::BulkLf,
            8 => Self::BulkArg,
            9 => Self::BulkArgLf,
            10 => Self::Multibulk,
            11 => Self::MultibulkNargLf,
            12 => Self::MultibulkArgnLen,
            13 => Self::MultibulkArgnLenLf,
            14 => Self::MultibulkArgn,
            15 => Self::MultibulkArgnLf,
            16 => Self::RuntoCrlf,
            17 => Self::AlmostDone,
            _ => Self::Start,
        }
    }
}

/// Parse a Redis request from `input` and update `r` in place.
///
/// On success `r.ty()` is set to the recognised command, the
/// parsed keys are appended to [`Msg::keys`], the argument buffer
/// is left untouched (callers that need argument bytes use
/// [`redis_parse_req_with_args`]), and the parser cursor advances
/// just past the trailing LF.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgParseResult, MsgType};
/// use dynomite::proto::redis::redis_parse_req;
///
/// let mut r = Msg::new(0, MsgType::Unknown, true);
/// let res = redis_parse_req(&mut r, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
/// assert_eq!(res, MsgParseResult::Ok);
/// assert_eq!(r.ty(), MsgType::ReqRedisGet);
/// assert_eq!(r.keys()[0].key(), b"foo");
/// ```
pub fn redis_parse_req(r: &mut Msg, input: &[u8]) -> MsgParseResult {
    redis_parse_req_with_args(r, input, None, true)
}

/// Variant of [`redis_parse_req`] that records all bulk arguments
/// (beyond the keys) into [`Msg::args`]. Used by the rewrite path
/// that needs each argument's bytes.
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgParseResult, MsgType};
/// use dynomite::proto::redis::parser::redis_parse_req_with_args;
///
/// let mut r = Msg::new(0, MsgType::Unknown, true);
/// let bytes = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
/// let res = redis_parse_req_with_args(&mut r, bytes, None, true);
/// assert_eq!(res, MsgParseResult::Ok);
/// assert_eq!(r.args()[0].bytes(), b"bar");
/// ```
#[allow(clippy::too_many_lines)]
pub fn redis_parse_req_with_args(
    r: &mut Msg,
    input: &[u8],
    hash_tag: Option<HashTag>,
    record_args: bool,
) -> MsgParseResult {
    if !r.is_request() {
        r.set_parse_result(MsgParseResult::Error);
        return MsgParseResult::Error;
    }
    let mut state = ReqState::from_u32(r.parser_state());
    let mut p = r.parser_pos();
    let mut token: Option<usize> = r.parser_token();
    let mut rlen = r.rlen();
    let mut rntokens = r.rntokens();
    let mut ntokens = r.ntokens();
    let mut nkeys = r.nkeys();
    let mut ty = r.ty();
    let mut is_read = r.flags().is_read;
    let mut quit = r.flags().quit;
    let mut routing = r.routing();

    while p < input.len() {
        let ch = input[p];
        match state {
            ReqState::Start | ReqState::Narg => {
                if token.is_none() {
                    if ch == b'p' || ch == b'P' {
                        state = ReqState::InlinePing;
                        p += 1;
                        continue;
                    }
                    if ch != b'*' {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    token = Some(p);
                    rntokens = 0;
                    state = ReqState::Narg;
                    p += 1;
                } else if ch.is_ascii_digit() {
                    rntokens = rntokens
                        .saturating_mul(10)
                        .saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if ch == CR {
                    if rntokens == 0 {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    ntokens = rntokens;
                    token = None;
                    state = ReqState::NargLf;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::InlinePing => {
                // `pInG` then trailing CRLF (4 more bytes).
                if input.len() - p >= 5
                    && input[p].eq_ignore_ascii_case(&b'i')
                    && input[p + 1].eq_ignore_ascii_case(&b'n')
                    && input[p + 2].eq_ignore_ascii_case(&b'g')
                    && input[p + 3] == CR
                    && input[p + 4] == LF
                {
                    ty = MsgType::ReqRedisPing;
                    is_read = true;
                    routing = MsgRouting::LocalNodeOnly;
                    p += 5;
                    return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                }
                return finish_req_error(
                    r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit, routing,
                );
            }
            ReqState::NargLf => {
                if ch == LF {
                    state = ReqState::ReqTypeLen;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::ReqTypeLen => {
                if token.is_none() {
                    if ch != b'$' {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    token = Some(p);
                    rlen = 0;
                    p += 1;
                } else if ch.is_ascii_digit() {
                    rlen = rlen.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if ch == CR {
                    if rlen == 0 || rntokens == 0 {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    rntokens -= 1;
                    token = None;
                    state = ReqState::ReqTypeLenLf;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::ReqTypeLenLf => {
                if ch == LF {
                    state = ReqState::ReqType;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::ReqType => {
                if token.is_none() {
                    token = Some(p);
                }
                let start = token.expect("token recorded");
                let needed = start + rlen as usize;
                if needed >= input.len() {
                    // Need more bytes.
                    p = input.len();
                    break;
                }
                if input[needed] != CR {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                let cmd_bytes = &input[start..needed];
                p = needed + 1;
                rlen = 0;
                token = None;
                let prev_ty = ty;
                if prev_ty != MsgType::ReqRedisScript {
                    ty = MsgType::Unknown;
                }
                if let Some((found, traits)) = lookup(cmd_bytes) {
                    ty = found;
                    is_read = traits.is_read;
                    quit = traits.quit;
                    routing = match traits.routing {
                        RoutingOverride::None => routing,
                        RoutingOverride::LocalNodeOnly => MsgRouting::LocalNodeOnly,
                        RoutingOverride::TokenOwnerLocalRackOnly => {
                            MsgRouting::TokenOwnerLocalRackOnly
                        }
                        RoutingOverride::AllNodesAllRacksAllDcs => {
                            MsgRouting::AllNodesAllRacksAllDcs
                        }
                    };
                    if ty == MsgType::ReqRedisExists && prev_ty == MsgType::ReqRedisScript {
                        ty = MsgType::ReqRedisScriptExists;
                        routing = MsgRouting::AllNodesAllRacksAllDcs;
                        is_read = true;
                    }
                    if ty == MsgType::ReqRedisPing {
                        // The C parser short-circuits the inline form here.
                        // p was advanced to needed+1 (the LF position).
                        return finish_req_ok(
                            r,
                            p + 1,
                            ty,
                            true,
                            quit,
                            MsgRouting::LocalNodeOnly,
                            ntokens,
                            nkeys,
                        );
                    }
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                state = ReqState::ReqTypeLf;
                continue;
            }
            ReqState::ReqTypeLf => {
                if ty == MsgType::ReqRedisScript {
                    state = ReqState::ReqTypeLen;
                    p += 1;
                    continue;
                }
                if ty == MsgType::HackSettingConnConsistency {
                    state = ReqState::Arg1Len;
                    p += 1;
                    continue;
                }
                if ch != LF {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                p += 1;
                let class = classify(ty);
                if matches!(class, CommandClass::Argz) && rntokens == 0 {
                    return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                }
                if class == CommandClass::ArgUpto1 && rntokens == 0 {
                    return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                }
                if class == CommandClass::ArgUpto1 && rntokens == 1 {
                    state = ReqState::Arg1Len;
                    continue;
                }
                if matches!(
                    ty,
                    MsgType::ReqRedisScriptLoad | MsgType::ReqRedisScriptExists
                ) {
                    state = ReqState::Arg1Len;
                    continue;
                }
                if class == CommandClass::ArgEval {
                    state = ReqState::Arg1Len;
                    continue;
                }
                state = ReqState::KeyLen;
            }
            ReqState::KeyLen => {
                if token.is_none() {
                    if ch != b'$' {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    token = Some(p);
                    rlen = 0;
                    p += 1;
                } else if ch.is_ascii_digit() {
                    rlen = rlen.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if ch == CR {
                    if rlen == 0 || rlen as usize >= MBUF_MAX_SIZE || rntokens == 0 {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    rntokens -= 1;
                    token = None;
                    state = ReqState::KeyLenLf;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::KeyLenLf => {
                if ch == LF {
                    state = ReqState::Key;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::Key => {
                if token.is_none() {
                    token = Some(p);
                }
                let start = token.expect("token recorded");
                let needed = start + rlen as usize;
                if needed >= input.len() {
                    p = input.len();
                    break;
                }
                if input[needed] != CR {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                let kbytes = input[start..needed].to_vec();
                p = needed + 1;
                rlen = 0;
                let kp = if let Some(tag) = hash_tag {
                    if let Some(open_idx) = kbytes.iter().position(|&b| b == tag.open) {
                        if let Some(close_off) =
                            kbytes[open_idx + 1..].iter().position(|&b| b == tag.close)
                        {
                            let s = open_idx + 1;
                            let e = open_idx + 1 + close_off;
                            KeyPos::new(kbytes, s..e)
                        } else {
                            KeyPos::without_tag(kbytes)
                        }
                    } else {
                        KeyPos::without_tag(kbytes)
                    }
                } else {
                    KeyPos::without_tag(kbytes)
                };
                r.push_key(kp);
                token = None;
                state = ReqState::KeyLf;
            }
            ReqState::KeyLf => {
                if ch != LF {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                p += 1;
                let class = classify(ty);
                match class {
                    CommandClass::Arg0 => {
                        if rntokens != 0 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                    }
                    CommandClass::Arg1 => {
                        if rntokens != 1 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg1Len;
                    }
                    CommandClass::Arg2 => {
                        if rntokens != 2 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg1Len;
                    }
                    CommandClass::Arg3 => {
                        if rntokens != 3 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg1Len;
                    }
                    CommandClass::ArgN => {
                        if rntokens == 0 {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                        state = ReqState::Arg1Len;
                    }
                    CommandClass::ArgX => {
                        if rntokens == 0 {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                        state = ReqState::KeyLen;
                    }
                    CommandClass::ArgKvX => {
                        if ntokens % 2 == 0 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg1Len;
                    }
                    CommandClass::ArgEval => {
                        nkeys = nkeys.saturating_sub(1);
                        if nkeys > 0 {
                            state = ReqState::KeyLen;
                        } else if rntokens > 0 {
                            state = ReqState::ArgnLen;
                        } else {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                    }
                    _ => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                }
            }
            ReqState::Arg1Len => {
                match read_bulk_len(input, &mut p, &mut token, &mut rlen, &mut rntokens) {
                    BulkLenStep::More => {}
                    BulkLenStep::Done => state = ReqState::Arg1LenLf,
                    BulkLenStep::Error => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    BulkLenStep::Eof => break,
                }
            }
            ReqState::Arg1LenLf => {
                if ch == LF {
                    state = ReqState::Arg1;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::Arg1 => match read_bulk_arg(input, p, rlen, record_args, r) {
                ArgStep::More => {
                    p = input.len();
                    let consumed = (p - r.parser_pos()) as u32;
                    let _ = consumed;
                    break;
                }
                ArgStep::Done(new_p) => {
                    p = new_p;
                    rlen = 0;
                    state = ReqState::Arg1Lf;
                }
                ArgStep::Error => {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            },
            ReqState::Arg1Lf => {
                if ch != LF {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                p += 1;
                let class = classify(ty);
                match class {
                    CommandClass::ArgUpto1 | CommandClass::Arg1 => {
                        if rntokens != 0 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                    }
                    CommandClass::Arg2 => {
                        if rntokens != 1 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg2Len;
                    }
                    CommandClass::Arg3 => {
                        if rntokens != 2 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg2Len;
                    }
                    CommandClass::ArgN => {
                        if rntokens == 0 {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                        state = ReqState::ArgnLen;
                    }
                    CommandClass::ArgEval => {
                        if rntokens < 2 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg2Len;
                    }
                    CommandClass::ArgKvX => {
                        if rntokens == 0 {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                        state = ReqState::KeyLen;
                    }
                    _ => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                }
            }
            ReqState::Arg2Len => {
                match read_bulk_len(input, &mut p, &mut token, &mut rlen, &mut rntokens) {
                    BulkLenStep::More => {}
                    BulkLenStep::Done => state = ReqState::Arg2LenLf,
                    BulkLenStep::Error => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    BulkLenStep::Eof => break,
                }
            }
            ReqState::Arg2LenLf => {
                if ch == LF {
                    state = ReqState::Arg2;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::Arg2 => {
                let class = classify(ty);
                let token_start = if class == CommandClass::ArgEval && token.is_none() {
                    Some(p)
                } else {
                    token
                };
                token = token_start;
                match read_bulk_arg(input, p, rlen, record_args, r) {
                    ArgStep::More => {
                        p = input.len();
                        break;
                    }
                    ArgStep::Done(new_p) => {
                        p = new_p;
                        if class == CommandClass::ArgEval {
                            // Token holds the start of the integer.
                            let start = token.unwrap_or(0);
                            if start >= p {
                                return finish_req_error(
                                    r, state, p, token, rlen, rntokens, ntokens, nkeys, ty,
                                    is_read, quit, routing,
                                );
                            }
                            let mut nkey: u32 = 0;
                            for &b in &input[start..p] {
                                if b.is_ascii_digit() {
                                    nkey =
                                        nkey.saturating_mul(10).saturating_add(u32::from(b - b'0'));
                                } else {
                                    return finish_req_error(
                                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty,
                                        is_read, quit, routing,
                                    );
                                }
                            }
                            if nkey == 0 || rntokens < nkey {
                                return finish_req_error(
                                    r, state, p, token, rlen, rntokens, ntokens, nkeys, ty,
                                    is_read, quit, routing,
                                );
                            }
                            nkeys = nkey;
                            token = None;
                        }
                        rlen = 0;
                        state = ReqState::Arg2Lf;
                    }
                    ArgStep::Error => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                }
            }
            ReqState::Arg2Lf => {
                if ch != LF {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                p += 1;
                let class = classify(ty);
                match class {
                    CommandClass::Arg2 => {
                        if rntokens != 0 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                    }
                    CommandClass::Arg3 => {
                        if rntokens != 1 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::Arg3Len;
                    }
                    CommandClass::ArgN => {
                        if rntokens == 0 {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                        state = ReqState::ArgnLen;
                    }
                    CommandClass::ArgEval => {
                        if rntokens < 1 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        state = ReqState::KeyLen;
                    }
                    _ => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                }
            }
            ReqState::Arg3Len => {
                match read_bulk_len(input, &mut p, &mut token, &mut rlen, &mut rntokens) {
                    BulkLenStep::More => {}
                    BulkLenStep::Done => state = ReqState::Arg3LenLf,
                    BulkLenStep::Error => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    BulkLenStep::Eof => break,
                }
            }
            ReqState::Arg3LenLf => {
                if ch == LF {
                    state = ReqState::Arg3;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::Arg3 => match read_bulk_arg(input, p, rlen, record_args, r) {
                ArgStep::More => {
                    p = input.len();
                    break;
                }
                ArgStep::Done(new_p) => {
                    p = new_p;
                    rlen = 0;
                    state = ReqState::Arg3Lf;
                }
                ArgStep::Error => {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            },
            ReqState::Arg3Lf => {
                if ch != LF {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                p += 1;
                let class = classify(ty);
                match class {
                    CommandClass::Arg3 => {
                        if rntokens != 0 {
                            return finish_req_error(
                                r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read,
                                quit, routing,
                            );
                        }
                        return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                    }
                    CommandClass::ArgN => {
                        if rntokens == 0 {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                        state = ReqState::ArgnLen;
                    }
                    _ => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                }
            }
            ReqState::ArgnLen => {
                match read_bulk_len(input, &mut p, &mut token, &mut rlen, &mut rntokens) {
                    BulkLenStep::More => {}
                    BulkLenStep::Done => state = ReqState::ArgnLenLf,
                    BulkLenStep::Error => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                    BulkLenStep::Eof => break,
                }
            }
            ReqState::ArgnLenLf => {
                if ch == LF {
                    state = ReqState::Argn;
                    p += 1;
                } else {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            }
            ReqState::Argn => match read_bulk_arg(input, p, rlen, record_args, r) {
                ArgStep::More => {
                    p = input.len();
                    break;
                }
                ArgStep::Done(new_p) => {
                    p = new_p;
                    rlen = 0;
                    state = ReqState::ArgnLf;
                }
                ArgStep::Error => {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
            },
            ReqState::ArgnLf => {
                if ch != LF {
                    return finish_req_error(
                        r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                        routing,
                    );
                }
                p += 1;
                let class = classify(ty);
                match class {
                    CommandClass::ArgN | CommandClass::ArgEval => {
                        if rntokens == 0 {
                            return finish_req_ok(r, p, ty, is_read, quit, routing, ntokens, nkeys);
                        }
                        state = ReqState::ArgnLen;
                    }
                    _ => {
                        return finish_req_error(
                            r, state, p, token, rlen, rntokens, ntokens, nkeys, ty, is_read, quit,
                            routing,
                        );
                    }
                }
            }
        }
    }

    // Reached end of input without completing.
    r.set_parser_state(state as u32);
    r.set_parser_pos(p);
    r.set_parser_token(token);
    r.set_rlen(rlen);
    r.set_rntokens(rntokens);
    r.set_ntokens(ntokens);
    r.set_nkeys(nkeys);
    if ty != MsgType::Unknown {
        r.set_type(ty);
    }
    r.flags_mut().is_read = is_read;
    r.flags_mut().quit = quit;
    r.flags_mut().rewrite_with_ts_possible = false;
    r.set_routing(routing);
    r.set_parse_result(MsgParseResult::Again);
    MsgParseResult::Again
}

#[derive(Debug)]
enum BulkLenStep {
    More,
    Done,
    Error,
    Eof,
}

fn read_bulk_len(
    input: &[u8],
    p: &mut usize,
    token: &mut Option<usize>,
    rlen: &mut u32,
    rntokens: &mut u32,
) -> BulkLenStep {
    if *p >= input.len() {
        return BulkLenStep::Eof;
    }
    let ch = input[*p];
    if token.is_none() {
        if ch != b'$' {
            return BulkLenStep::Error;
        }
        *token = Some(*p);
        *rlen = 0;
        *p += 1;
        return BulkLenStep::More;
    }
    if ch.is_ascii_digit() {
        *rlen = rlen.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
        *p += 1;
        BulkLenStep::More
    } else if ch == CR {
        let start = token.expect("token recorded");
        if *p - start <= 1 || *rntokens == 0 {
            return BulkLenStep::Error;
        }
        *rntokens -= 1;
        *token = None;
        *p += 1;
        BulkLenStep::Done
    } else {
        BulkLenStep::Error
    }
}

#[derive(Debug)]
enum ArgStep {
    /// Need more input bytes.
    More,
    /// Argument fully consumed; new cursor `p` (pointing at CR).
    Done(usize),
    /// Bad framing.
    Error,
}

fn read_bulk_arg(input: &[u8], p: usize, rlen: u32, record: bool, r: &mut Msg) -> ArgStep {
    let needed = match p.checked_add(rlen as usize) {
        Some(n) => n,
        None => return ArgStep::Error,
    };
    if needed >= input.len() {
        return ArgStep::More;
    }
    if input[needed] != CR {
        return ArgStep::Error;
    }
    if record && rlen > 0 {
        r.push_arg(ArgPos::new(input[p..needed].to_vec()));
    }
    ArgStep::Done(needed + 1)
}

#[allow(clippy::too_many_arguments)]
fn finish_req_ok(
    r: &mut Msg,
    pos: usize,
    ty: MsgType,
    is_read: bool,
    quit: bool,
    routing: MsgRouting,
    ntokens: u32,
    nkeys: u32,
) -> MsgParseResult {
    if ty == MsgType::Unknown {
        r.set_parse_result(MsgParseResult::Error);
        return MsgParseResult::Error;
    }
    r.set_type(ty);
    r.flags_mut().is_read = is_read;
    r.flags_mut().quit = quit;
    r.set_routing(routing);
    r.set_ntokens(ntokens);
    r.set_nkeys(nkeys);
    r.set_parser_state(0);
    r.set_parser_pos(pos);
    r.set_parser_token(None);
    r.set_parse_result(MsgParseResult::Ok);
    MsgParseResult::Ok
}

#[allow(clippy::too_many_arguments)]
fn finish_req_error(
    r: &mut Msg,
    state: ReqState,
    pos: usize,
    token: Option<usize>,
    rlen: u32,
    rntokens: u32,
    ntokens: u32,
    nkeys: u32,
    ty: MsgType,
    is_read: bool,
    quit: bool,
    routing: MsgRouting,
) -> MsgParseResult {
    r.set_parser_state(state as u32);
    r.set_parser_pos(pos);
    r.set_parser_token(token);
    r.set_rlen(rlen);
    r.set_rntokens(rntokens);
    r.set_ntokens(ntokens);
    r.set_nkeys(nkeys);
    if ty != MsgType::Unknown {
        r.set_type(ty);
    }
    r.flags_mut().is_read = is_read;
    r.flags_mut().quit = quit;
    r.set_routing(routing);
    r.set_parse_result(MsgParseResult::Error);
    MsgParseResult::Error
}

/// Parse a Redis response from `input` and update `r` in place.
///
/// On success the response type tag is set, the integer payload
/// (for `:n\r\n` responses) is recorded on the message, and the
/// parser cursor advances just past the trailing LF. On truncated
/// input the function returns [`MsgParseResult::Again`]. Invalid
/// bytes return [`MsgParseResult::Error`].
///
/// # Examples
///
/// ```
/// use dynomite::msg::{Msg, MsgParseResult, MsgType};
/// use dynomite::proto::redis::redis_parse_rsp;
///
/// let mut r = Msg::new(0, MsgType::Unknown, false);
/// let res = redis_parse_rsp(&mut r, b"+OK\r\n");
/// assert_eq!(res, MsgParseResult::Ok);
/// assert_eq!(r.ty(), MsgType::RspRedisStatus);
/// ```
#[allow(clippy::too_many_lines)]
pub fn redis_parse_rsp(r: &mut Msg, input: &[u8]) -> MsgParseResult {
    if r.is_request() {
        r.set_parse_result(MsgParseResult::Error);
        return MsgParseResult::Error;
    }
    let mut state = RspState::from_u32(r.parser_state());
    let mut p = r.parser_pos();
    let mut token: Option<usize> = r.parser_token();
    let mut rlen = r.rlen();
    let mut rntokens = r.rntokens();
    let mut ty = r.ty();
    let mut integer = r.integer();
    let mut int_negative = false;
    let mut ntoken_start: Option<usize> = r.ntoken_start();
    let mut ntoken_end: Option<usize> = r.ntoken_end();

    while p < input.len() {
        let ch = input[p];
        match state {
            RspState::Start => {
                ty = MsgType::Unknown;
                match ch {
                    b'+' => {
                        ty = MsgType::RspRedisStatus;
                        state = RspState::RuntoCrlf;
                        p += 1;
                    }
                    b'-' => {
                        ty = MsgType::RspRedisError;
                        state = RspState::Error;
                    }
                    b':' => {
                        ty = MsgType::RspRedisInteger;
                        state = RspState::IntegerStart;
                        integer = 0;
                        int_negative = false;
                        p += 1;
                    }
                    b'$' => {
                        ty = MsgType::RspRedisBulk;
                        state = RspState::Bulk;
                    }
                    b'*' => {
                        ty = MsgType::RspRedisMultibulk;
                        state = RspState::Multibulk;
                    }
                    _ => {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                }
            }
            // Resume-from-state entry: the parser API allows callers
            // to resume from a saved RspState across input chunks.
            // Status and Integer entries are no-op transitions to
            // their respective body states (RuntoCrlf and
            // IntegerStart) so a resumed parse picks up where the
            // caller left off without re-consuming a byte.
            RspState::Status => {
                state = RspState::RuntoCrlf;
            }
            RspState::Error => {
                if token.is_none() {
                    if ch != b'-' {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                    token = Some(p);
                    p += 1;
                } else if ch == b' ' || ch == CR {
                    let start = token.expect("token recorded");
                    if let Some(t) = error_lookup(&input[start..p]) {
                        ty = t;
                    }
                    state = RspState::RuntoCrlf;
                    token = None;
                    // Do not advance.
                } else {
                    p += 1;
                }
            }
            RspState::Integer => {
                // Resume-from-state entry: see RspState::Status.
                state = RspState::IntegerStart;
                integer = 0;
                int_negative = false;
            }
            RspState::IntegerStart => {
                if ch == CR {
                    if int_negative {
                        integer = -integer;
                    }
                    state = RspState::AlmostDone;
                    p += 1;
                } else if ch == b'-' {
                    int_negative = true;
                    p += 1;
                } else if ch.is_ascii_digit() {
                    integer = integer
                        .saturating_mul(10)
                        .saturating_add(i64::from(ch - b'0'));
                    p += 1;
                } else {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
            }
            RspState::Simple => {
                if ch == CR {
                    state = RspState::MultibulkArgnLf;
                    rntokens = rntokens.saturating_sub(1);
                }
                p += 1;
            }
            RspState::RuntoCrlf => {
                if ch == CR {
                    state = RspState::AlmostDone;
                    p += 1;
                } else {
                    p += 1;
                }
            }
            RspState::AlmostDone => {
                if ch != LF {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
                p += 1;
                return finish_rsp_ok(r, p, ty, integer, ntoken_start, ntoken_end);
            }
            RspState::Bulk => {
                if token.is_none() {
                    if ch != b'$' {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                    token = Some(p);
                    rlen = 0;
                    p += 1;
                } else if ch == b'-' {
                    state = RspState::RuntoCrlf;
                    p += 1;
                } else if ch.is_ascii_digit() {
                    rlen = rlen.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if ch == CR {
                    let start = token.expect("token recorded");
                    if p - start <= 1 {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                    token = None;
                    state = RspState::BulkLf;
                    p += 1;
                } else {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
            }
            RspState::BulkLf => {
                if ch == LF {
                    state = RspState::BulkArg;
                    p += 1;
                } else {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
            }
            RspState::BulkArg => {
                let needed = match p.checked_add(rlen as usize) {
                    Some(n) => n,
                    None => {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                };
                if needed >= input.len() {
                    p = input.len();
                    break;
                }
                if input[needed] != CR {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
                p = needed + 1;
                rlen = 0;
                state = RspState::BulkArgLf;
            }
            RspState::BulkArgLf => {
                if ch != CR && ch != LF {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
                if ch == CR {
                    p += 1;
                    continue;
                }
                p += 1;
                return finish_rsp_ok(r, p, ty, integer, ntoken_start, ntoken_end);
            }
            RspState::Multibulk => {
                if token.is_none() {
                    if ch != b'*' {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                    token = Some(p);
                    ntoken_start = Some(p);
                    rntokens = 0;
                    p += 1;
                } else if ch == b'-' {
                    state = RspState::RuntoCrlf;
                    p += 1;
                } else if ch.is_ascii_digit() {
                    rntokens = rntokens
                        .saturating_mul(10)
                        .saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if ch == CR {
                    let start = token.expect("token recorded");
                    if p - start <= 1 {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                    ntoken_end = Some(p);
                    token = None;
                    state = RspState::MultibulkNargLf;
                    p += 1;
                } else {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
            }
            RspState::MultibulkNargLf => {
                if ch != LF {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
                p += 1;
                if rntokens == 0 {
                    return finish_rsp_ok(r, p, ty, integer, ntoken_start, ntoken_end);
                }
                state = RspState::MultibulkArgnLen;
            }
            RspState::MultibulkArgnLen => {
                if token.is_none() {
                    if ch == b'*' {
                        state = RspState::Multibulk;
                        // Do not advance.
                        continue;
                    }
                    if ch == b':' || ch == b'+' || ch == b'-' {
                        state = RspState::Simple;
                        // Do not advance.
                        continue;
                    }
                    if ch != b'$' {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                    token = Some(p);
                    rlen = 0;
                    p += 1;
                } else if ch.is_ascii_digit() {
                    rlen = rlen.saturating_mul(10).saturating_add(u32::from(ch - b'0'));
                    p += 1;
                } else if ch == b'-' {
                    p += 1;
                } else if ch == CR {
                    let start = token.expect("token recorded");
                    if p - start <= 1 || rntokens == 0 {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                    if rlen == 1 && p - start == 3 {
                        rlen = 0;
                        state = RspState::MultibulkArgnLf;
                    } else {
                        state = RspState::MultibulkArgnLenLf;
                    }
                    rntokens = rntokens.saturating_sub(1);
                    token = None;
                    p += 1;
                } else {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
            }
            RspState::MultibulkArgnLenLf => {
                if ch == LF {
                    state = RspState::MultibulkArgn;
                    p += 1;
                } else {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
            }
            RspState::MultibulkArgn => {
                let needed = match p.checked_add(rlen as usize) {
                    Some(n) => n,
                    None => {
                        return finish_rsp_error(
                            r,
                            state,
                            p,
                            token,
                            rlen,
                            rntokens,
                            ty,
                            integer,
                            ntoken_start,
                            ntoken_end,
                        );
                    }
                };
                if needed >= input.len() {
                    p = input.len();
                    break;
                }
                if input[needed] != CR {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
                if rlen > 0 {
                    r.push_arg(ArgPos::new(input[p..needed].to_vec()));
                }
                p = needed + 1;
                rlen = 0;
                state = RspState::MultibulkArgnLf;
            }
            RspState::MultibulkArgnLf => {
                if ch != LF {
                    return finish_rsp_error(
                        r,
                        state,
                        p,
                        token,
                        rlen,
                        rntokens,
                        ty,
                        integer,
                        ntoken_start,
                        ntoken_end,
                    );
                }
                p += 1;
                if rntokens == 0 {
                    return finish_rsp_ok(r, p, ty, integer, ntoken_start, ntoken_end);
                }
                state = RspState::MultibulkArgnLen;
            }
        }
    }

    r.set_parser_state(state as u32);
    r.set_parser_pos(p);
    r.set_parser_token(token);
    r.set_rlen(rlen);
    r.set_rntokens(rntokens);
    r.set_integer(integer);
    r.set_ntoken_span(ntoken_start, ntoken_end);
    if ty != MsgType::Unknown {
        r.set_type(ty);
    }
    r.flags_mut().is_error = super::commands::is_redis_error(ty);
    r.set_parse_result(MsgParseResult::Again);
    MsgParseResult::Again
}

fn finish_rsp_ok(
    r: &mut Msg,
    pos: usize,
    ty: MsgType,
    integer: i64,
    ntoken_start: Option<usize>,
    ntoken_end: Option<usize>,
) -> MsgParseResult {
    if ty == MsgType::Unknown {
        r.set_parse_result(MsgParseResult::Error);
        return MsgParseResult::Error;
    }
    r.set_type(ty);
    r.set_integer(integer);
    r.set_ntoken_span(ntoken_start, ntoken_end);
    r.set_parser_state(0);
    r.set_parser_pos(pos);
    r.set_parser_token(None);
    r.flags_mut().is_error = super::commands::is_redis_error(ty);
    r.set_parse_result(MsgParseResult::Ok);
    MsgParseResult::Ok
}

#[allow(clippy::too_many_arguments)]
fn finish_rsp_error(
    r: &mut Msg,
    state: RspState,
    pos: usize,
    token: Option<usize>,
    rlen: u32,
    rntokens: u32,
    ty: MsgType,
    integer: i64,
    ntoken_start: Option<usize>,
    ntoken_end: Option<usize>,
) -> MsgParseResult {
    r.set_parser_state(state as u32);
    r.set_parser_pos(pos);
    r.set_parser_token(token);
    r.set_rlen(rlen);
    r.set_rntokens(rntokens);
    r.set_integer(integer);
    r.set_ntoken_span(ntoken_start, ntoken_end);
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
        let _ = redis_parse_req(&mut m, input);
        m
    }

    fn parse_rsp(input: &[u8]) -> Msg {
        let mut m = Msg::new(0, MsgType::Unknown, false);
        let _ = redis_parse_rsp(&mut m, input);
        m
    }

    #[test]
    fn parse_get() {
        let m = parse_req(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqRedisGet);
        assert_eq!(m.keys()[0].key(), b"foo");
    }

    #[test]
    fn parse_set() {
        let m = parse_req(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqRedisSet);
        assert_eq!(m.keys()[0].key(), b"foo");
        assert_eq!(m.args()[0].bytes(), b"bar");
    }

    #[test]
    fn parse_del_multikey() {
        let m = parse_req(b"*4\r\n$3\r\nDEL\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqRedisDel);
        let keys: Vec<&[u8]> = m.keys().iter().map(crate::msg::KeyPos::key).collect();
        assert_eq!(keys, vec![&b"a"[..], b"b", b"c"]);
    }

    #[test]
    fn parse_mset() {
        let m = parse_req(b"*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqRedisMset);
        let keys: Vec<&[u8]> = m.keys().iter().map(crate::msg::KeyPos::key).collect();
        assert_eq!(keys, vec![&b"a"[..], b"b"]);
    }

    #[test]
    fn parse_ping_inline() {
        let m = parse_req(b"PING\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqRedisPing);
    }

    #[test]
    fn parse_ping_resp() {
        let m = parse_req(b"*1\r\n$4\r\nPING\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::ReqRedisPing);
    }

    #[test]
    fn parse_unknown_command_errors() {
        let m = parse_req(b"*1\r\n$3\r\nFOO\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Error);
    }

    #[test]
    fn parse_truncated_returns_again() {
        let m = parse_req(b"*2\r\n$3\r\nGET\r\n$3\r\nfo");
        assert_eq!(m.parse_result(), MsgParseResult::Again);
    }

    #[test]
    fn parse_status_response() {
        let m = parse_rsp(b"+OK\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspRedisStatus);
    }

    #[test]
    fn parse_error_response_classifies() {
        let m = parse_rsp(b"-WRONGTYPE Operation not allowed\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspRedisErrorWrongtype);
        assert!(m.flags().is_error);
    }

    #[test]
    fn parse_integer_response() {
        let m = parse_rsp(b":42\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspRedisInteger);
        assert_eq!(m.integer(), 42);
    }

    #[test]
    fn parse_bulk_response() {
        let m = parse_rsp(b"$5\r\nhello\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspRedisBulk);
    }

    #[test]
    fn parse_null_bulk_response() {
        let m = parse_rsp(b"$-1\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspRedisBulk);
    }

    #[test]
    fn parse_empty_multibulk_response() {
        let m = parse_rsp(b"*0\r\n");
        assert_eq!(m.parse_result(), MsgParseResult::Ok);
        assert_eq!(m.ty(), MsgType::RspRedisMultibulk);
    }
}
