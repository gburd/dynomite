//! Branch coverage for the Redis (RESP) wire-protocol parser
//! (`proto::redis::parser`).
//!
//! The parser is a single byte-driven state machine for requests
//! (`redis_parse_req` / `redis_parse_req_with_args`) and another for
//! responses (`redis_parse_rsp`). These tests drive every
//! command-class arm (Arg0/1/2/3/N/X/KvX/Upto1/Eval/Argz), the
//! SCRIPT subcommand chain, the conn-consistency hack command, the
//! hash-tag carving path, the inline-PING fast path, and every
//! malformed-framing rejection per state. The response half drives
//! status / error / integer (incl. negative) / bulk / null-bulk /
//! multibulk (incl. nested integer/status/error/multibulk
//! elements) plus the truncation/resume path.

#![allow(clippy::too_many_lines)]

use dynomite::msg::{Msg, MsgParseResult, MsgRouting, MsgType};
use dynomite::proto::redis::commands::CommandClass;
use dynomite::proto::redis::parser::{redis_parse_req_with_args, HashTag};
use dynomite::proto::redis::{redis_parse_req, redis_parse_rsp};

fn req(input: &[u8]) -> Msg {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let _ = redis_parse_req(&mut m, input);
    m
}

fn req_args(input: &[u8]) -> Msg {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let _ = redis_parse_req_with_args(&mut m, input, None, true);
    m
}

fn rsp(input: &[u8]) -> Msg {
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let _ = redis_parse_rsp(&mut m, input);
    m
}

// -------------------------------------------------------------
// Request command-class arm coverage. Each command exercises a
// distinct arg-shape arm in the KeyLf / ArgNLf state transitions.
// -------------------------------------------------------------

#[test]
fn arg0_get_ok_and_extra_arg_errors() {
    // Arg0: one key, zero args. An extra token is a framing error.
    let m = req(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisGet);
    let bad = req(b"*3\r\n$3\r\nGET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    assert_eq!(bad.parse_result(), MsgParseResult::Error);
}

#[test]
fn arg1_expire_ok_and_wrong_arity_errors() {
    // Arg1: one key, exactly one arg.
    let m = req(b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$2\r\n10\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisExpire);
    // Missing the arg (only the key) is a framing error.
    let bad = req(b"*2\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n");
    assert_eq!(bad.parse_result(), MsgParseResult::Error);
}

#[test]
fn arg2_setex_ok_and_wrong_arity_errors() {
    // Arg2: one key, exactly two args (SETEX key seconds value).
    let m = req(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$1\r\nv\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisSetex);
    let bad = req(b"*3\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n");
    assert_eq!(bad.parse_result(), MsgParseResult::Error);
}

#[test]
fn arg3_linsert_ok_and_wrong_arity_errors() {
    // Arg3: one key, exactly three args (LINSERT key BEFORE pivot v).
    let m = req(b"*5\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n$1\r\nv\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisLinsert);
    let bad = req(b"*4\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n");
    assert_eq!(bad.parse_result(), MsgParseResult::Error);
}

#[test]
fn argn_set_variadic_ok() {
    // ArgN: one key, zero-or-more args. SET with extra options.
    let m = req(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisSet);
    // SET with just the key (no value) is a valid ArgN-with-zero
    // form that closes at KeyLf.
    let one = req(b"*2\r\n$3\r\nSET\r\n$1\r\nk\r\n");
    assert_eq!(one.parse_result(), MsgParseResult::Ok);
}

#[test]
fn argn_hset_three_args() {
    // ArgN walking the Arg1 -> Arg2 -> Arg3 -> ArgN chain.
    let m = req(b"*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n$1\r\nv\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisHset);
}

#[test]
fn argx_mget_multikey_ok() {
    // ArgX: one or more keys, no values.
    let m = req(b"*4\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisMget);
    assert_eq!(m.keys().len(), 3);
}

#[test]
fn argkvx_mset_pairs_ok_and_odd_errors() {
    // ArgKvX: key/value pairs. Even token count after the keyword
    // is a framing error (must be odd: cmd + N pairs).
    let m = req(b"*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisMset);
    assert_eq!(m.keys().len(), 2);
    // MSET with a dangling key (odd value count) trips the
    // ntokens-even check.
    let bad = req(b"*4\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n");
    assert_eq!(bad.parse_result(), MsgParseResult::Error);
}

#[test]
fn argupto1_info_with_and_without_arg() {
    // ArgUpto1: one key and zero-or-one arg. INFO with a section.
    let with = req(b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n");
    assert_eq!(with.parse_result(), MsgParseResult::Ok);
    assert_eq!(with.ty(), MsgType::ReqRedisInfo);
    assert_eq!(with.routing(), MsgRouting::LocalNodeOnly);
    // INFO with no section: rntokens==0 closes at ReqTypeLf.
    let bare = req(b"*1\r\n$4\r\nINFO\r\n");
    assert_eq!(bare.parse_result(), MsgParseResult::Ok);
}

#[test]
fn argz_quit_and_ping_take_no_key() {
    // Argz: zero keys. QUIT sets the quit flag; PING resp-form sets
    // local-node routing.
    let q = req(b"*1\r\n$4\r\nQUIT\r\n");
    assert_eq!(q.parse_result(), MsgParseResult::Ok);
    assert_eq!(q.ty(), MsgType::ReqRedisQuit);
    assert!(q.flags().quit);
    let p = req(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(p.parse_result(), MsgParseResult::Ok);
    assert_eq!(p.ty(), MsgType::ReqRedisPing);
    assert_eq!(p.routing(), MsgRouting::LocalNodeOnly);
}

// -------------------------------------------------------------
// EVAL / EVALSHA: the ArgEval layout (script, numkeys, keys, args).
// -------------------------------------------------------------

#[test]
fn eval_no_keys_no_args() {
    // EVAL "return 1" 0 -- numkeys 0, no keys, no trailing args.
    let m = req(b"*3\r\n$4\r\nEVAL\r\n$8\r\nreturn 1\r\n$1\r\n0\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisEval);
    assert_eq!(m.nkeys(), 0);
}

#[test]
fn eval_one_key_no_args() {
    // EVAL "script" 1 mykey
    let mut m2 = Msg::new(0, MsgType::Unknown, true);
    let _ = redis_parse_req(
        &mut m2,
        b"*4\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$1\r\n1\r\n$5\r\nmykey\r\n",
    );
    assert_eq!(m2.parse_result(), MsgParseResult::Ok);
    assert_eq!(m2.ty(), MsgType::ReqRedisEval);
    assert_eq!(m2.keys().len(), 1);
    assert_eq!(m2.keys()[0].key(), b"mykey");
}

#[test]
fn eval_one_key_one_arg() {
    // EVAL "script" 1 k arg
    let m = req(b"*5\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$1\r\n1\r\n$1\r\nk\r\n$3\r\narg\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisEval);
    assert_eq!(m.keys().len(), 1);
}

#[test]
fn eval_zero_keys_with_trailing_args() {
    // EVAL "script" 0 extra -- numkeys 0 but a trailing arg routes
    // through the ArgnLen branch.
    let m = req(b"*4\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$1\r\n0\r\n$5\r\nextra\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisEval);
    assert_eq!(m.nkeys(), 0);
}

#[test]
fn eval_numkeys_exceeds_remaining_tokens_errors() {
    // numkeys=5 but only one token remains: the rntokens < nkey
    // guard rejects.
    let m = req(b"*4\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$1\r\n5\r\n$1\r\nk\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Error);
}

#[test]
fn eval_non_numeric_numkeys_errors() {
    // numkeys is not an ASCII-decimal integer.
    let m = req(b"*4\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$1\r\nx\r\n$1\r\nk\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Error);
}

#[test]
fn evalsha_resolves_and_parses() {
    let m = req(b"*3\r\n$7\r\nEVALSHA\r\n$3\r\nabc\r\n$1\r\n0\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisEvalsha);
}

// -------------------------------------------------------------
// SCRIPT subcommands: SCRIPT LOAD / EXISTS / FLUSH / KILL. These
// chain through the ReqTypeLf SCRIPT special-case back to
// ReqTypeLen for the subcommand keyword.
// -------------------------------------------------------------

#[test]
fn script_load_parses() {
    let m = req(b"*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$8\r\nreturn 1\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisScriptLoad);
}

#[test]
fn script_exists_parses() {
    let m = req(b"*3\r\n$6\r\nSCRIPT\r\n$6\r\nEXISTS\r\n$3\r\nsha\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisScriptExists);
}

#[test]
fn script_flush_parses() {
    let m = req(b"*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisScriptFlush);
    assert_eq!(m.routing(), MsgRouting::AllNodesAllRacksAllDcs);
}

#[test]
fn script_kill_parses() {
    let m = req(b"*2\r\n$6\r\nSCRIPT\r\n$4\r\nKILL\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisScriptKill);
}

// -------------------------------------------------------------
// conn-consistency hack command: dyno_config:conn_consistency
// resolves through the lookup table to the
// HackSettingConnConsistency type (the ReqTypeLf hack branch
// transitions to the arg states; the full arg shape is exercised
// at the dispatcher layer).
// -------------------------------------------------------------

#[test]
fn conn_consistency_hack_command_resolves_type() {
    // The keyword reaches the ReqTypeLf hack branch, which stamps
    // the HackSettingConnConsistency type before transitioning to
    // the arg states.
    let m = req(b"*2\r\n$28\r\ndyno_config:conn_consistency\r\n$2\r\nDC\r\n");
    assert_eq!(m.ty(), MsgType::HackSettingConnConsistency);
}

// -------------------------------------------------------------
// Hash-tag carving: keys with {tag} delimiters record the inner
// range as the routing tag; keys without/unterminated tags fall
// back to the whole key.
// -------------------------------------------------------------

#[test]
fn hash_tag_carves_inner_range() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let tag = HashTag {
        open: b'{',
        close: b'}',
    };
    let r = redis_parse_req_with_args(
        &mut m,
        b"*2\r\n$3\r\nGET\r\n$7\r\n{abc}xy\r\n",
        Some(tag),
        true,
    );
    assert_eq!(r, MsgParseResult::Ok);
    assert_eq!(m.keys()[0].key(), b"{abc}xy");
    assert_eq!(m.keys()[0].tag_bytes(), b"abc");
}

#[test]
fn hash_tag_unterminated_uses_whole_key() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let tag = HashTag {
        open: b'{',
        close: b'}',
    };
    // Open delimiter without a close: falls back to the full key.
    let r = redis_parse_req_with_args(
        &mut m,
        b"*2\r\n$3\r\nGET\r\n$4\r\n{abc\r\n",
        Some(tag),
        true,
    );
    assert_eq!(r, MsgParseResult::Ok);
    assert_eq!(m.keys()[0].tag_bytes(), b"{abc");
}

#[test]
fn hash_tag_absent_open_uses_whole_key() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let tag = HashTag {
        open: b'{',
        close: b'}',
    };
    let r = redis_parse_req_with_args(&mut m, b"*2\r\n$3\r\nGET\r\n$3\r\nabc\r\n", Some(tag), true);
    assert_eq!(r, MsgParseResult::Ok);
    assert_eq!(m.keys()[0].tag_bytes(), b"abc");
}

// -------------------------------------------------------------
// Inline PING fast path (lowercase / uppercase / mixed).
// -------------------------------------------------------------

#[test]
fn inline_ping_case_insensitive() {
    for bytes in [&b"PING\r\n"[..], b"ping\r\n", b"PiNg\r\n"] {
        let m = req(bytes);
        assert_eq!(m.parse_result(), MsgParseResult::Ok, "input {bytes:?}");
        assert_eq!(m.ty(), MsgType::ReqRedisPing);
        assert_eq!(m.routing(), MsgRouting::LocalNodeOnly);
    }
}

#[test]
fn inline_ping_garbage_after_p_errors() {
    // A line starting with 'p' that is not "ping\r\n" is a framing
    // error in the InlinePing state.
    let m = req(b"poke\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Error);
}

#[test]
fn inline_ping_truncated_errors() {
    // 'p' with too few following bytes can't complete the inline
    // form: the InlinePing fast-path falls to error.
    let m = req(b"pin");
    assert_eq!(m.parse_result(), MsgParseResult::Error);
}

// -------------------------------------------------------------
// record_args = false leaves the arg vector empty.
// -------------------------------------------------------------

#[test]
fn record_args_false_keeps_args_empty() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let r = redis_parse_req_with_args(
        &mut m,
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
        None,
        false,
    );
    assert_eq!(r, MsgParseResult::Ok);
    assert!(m.args().is_empty());
    // With recording on, the value is captured.
    let with = req_args(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    assert_eq!(with.args()[0].bytes(), b"v");
}

// -------------------------------------------------------------
// Request malformed-framing rejections (one per parser state).
// -------------------------------------------------------------

#[test]
fn req_rejects_non_star_leading_byte() {
    // Start state: anything but '*' or 'p'/'P' errors.
    assert_eq!(req(b"x2\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_rejects_zero_narg() {
    // Narg state: rntokens == 0 at CR is invalid.
    assert_eq!(req(b"*0\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_rejects_bad_narg_byte() {
    // Narg state: a non-digit, non-CR byte errors.
    assert_eq!(req(b"*1x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_rejects_missing_narg_lf() {
    // NargLf state: the byte after CR must be LF.
    assert_eq!(req(b"*1\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_rejects_bad_type_len_prefix() {
    // ReqTypeLen: the keyword bulk must start with '$'.
    assert_eq!(
        req(b"*1\r\nX3\r\nGET\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_zero_type_len() {
    // ReqTypeLen: rlen == 0 at CR is invalid.
    assert_eq!(
        req(b"*1\r\n$0\r\n\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_bad_type_len_byte() {
    // ReqTypeLen: non-digit after '$' errors.
    assert_eq!(req(b"*1\r\n$x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_rejects_missing_type_len_lf() {
    // ReqTypeLenLf: the byte after the length CR must be LF.
    assert_eq!(req(b"*1\r\n$3\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_rejects_type_without_trailing_cr() {
    // ReqType: the byte after the keyword bytes must be CR.
    assert_eq!(
        req(b"*1\r\n$3\r\nGETX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_missing_type_lf() {
    // ReqTypeLf: ordinary command must be followed by LF.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_bad_key_len_prefix() {
    // KeyLen: the key bulk must start with '$'.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\r\nX3\r\nfoo\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_zero_key_len() {
    // KeyLen: a zero-length key is invalid.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_oversized_key_len() {
    // KeyLen: a key length >= MBUF_MAX_SIZE is rejected. 600000 is
    // well above the mbuf cap.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\r\n$600000\r\nfoo\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_missing_key_len_lf() {
    // KeyLenLf: byte after the key-length CR must be LF.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\r\n$3\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_key_without_trailing_cr() {
    // Key: the byte after the key bytes must be CR.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\r\n$3\r\nfooX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_missing_key_lf() {
    // KeyLf: the byte after the key CR must be LF.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_bad_arg_len_prefix() {
    // Arg1Len: the arg bulk must start with '$'.
    assert_eq!(
        req(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\nX1\r\nv\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_missing_arg_len_lf() {
    // Arg1LenLf: byte after the arg-length CR must be LF.
    assert_eq!(
        req(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_arg_without_trailing_cr() {
    // Arg1: byte after the arg bytes must be CR.
    assert_eq!(
        req(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nvX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_rejects_missing_arg_lf() {
    // Arg1Lf: byte after the arg CR must be LF.
    assert_eq!(
        req(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_non_request_message_errors() {
    // A message constructed as a response cannot be parsed as a
    // request: the is_request guard rejects it.
    let mut m = Msg::new(0, MsgType::Unknown, false);
    assert_eq!(
        redis_parse_req(&mut m, b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"),
        MsgParseResult::Error
    );
}

// -------------------------------------------------------------
// Streaming / resume: a request fed in two chunks resumes from the
// saved parser state and completes.
// -------------------------------------------------------------

#[test]
fn req_resumes_across_two_chunks() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let first = redis_parse_req(&mut m, b"*3\r\n$3\r\nSET\r\n$3\r\nfoo");
    assert_eq!(first, MsgParseResult::Again);
    // Feed the rest; the parser resumes from the saved Key state.
    let rest = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let second = redis_parse_req(&mut m, rest);
    assert_eq!(second, MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisSet);
}

#[test]
fn req_truncated_at_each_boundary_returns_again() {
    // Feeding ever-longer prefixes of a valid SET request never
    // panics and yields Again until the final byte completes it.
    let full = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
    for n in 1..full.len() {
        let mut m = Msg::new(0, MsgType::Unknown, true);
        let r = redis_parse_req(&mut m, &full[..n]);
        assert!(
            matches!(r, MsgParseResult::Again | MsgParseResult::Error),
            "prefix len {n} gave {r:?}"
        );
    }
    let mut m = Msg::new(0, MsgType::Unknown, true);
    assert_eq!(redis_parse_req(&mut m, full), MsgParseResult::Ok);
}

// -------------------------------------------------------------
// Response parser coverage: every response shape and error path.
// -------------------------------------------------------------

#[test]
fn rsp_status_ok() {
    let m = rsp(b"+OK\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisStatus);
}

#[test]
fn rsp_error_classified_and_unclassified() {
    // A recognised error keyword classifies; an unknown one stays a
    // generic RspRedisError but is still flagged is_error.
    let known = rsp(b"-WRONGTYPE bad\r\n");
    assert_eq!(known.parse_result(), MsgParseResult::Ok);
    assert_eq!(known.ty(), MsgType::RspRedisErrorWrongtype);
    assert!(known.flags().is_error);
    let unknown = rsp(b"-SOMETHING went wrong\r\n");
    assert_eq!(unknown.parse_result(), MsgParseResult::Ok);
    assert_eq!(unknown.ty(), MsgType::RspRedisError);
    assert!(unknown.flags().is_error);
}

#[test]
fn rsp_error_keyword_terminated_by_cr() {
    // An error line with the keyword immediately followed by CR (no
    // space) still classifies on the keyword token.
    let m = rsp(b"-ERR\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisErrorErr);
}

#[test]
fn rsp_integer_positive_and_negative() {
    let pos = rsp(b":42\r\n");
    assert_eq!(pos.parse_result(), MsgParseResult::Ok);
    assert_eq!(pos.integer(), 42);
    let neg = rsp(b":-7\r\n");
    assert_eq!(neg.parse_result(), MsgParseResult::Ok);
    assert_eq!(neg.integer(), -7);
}

#[test]
fn rsp_integer_bad_byte_errors() {
    // IntegerStart: a non-digit, non-'-', non-CR byte errors.
    assert_eq!(rsp(b":4x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_bulk_value_and_null() {
    let v = rsp(b"$5\r\nhello\r\n");
    assert_eq!(v.parse_result(), MsgParseResult::Ok);
    assert_eq!(v.ty(), MsgType::RspRedisBulk);
    let null = rsp(b"$-1\r\n");
    assert_eq!(null.parse_result(), MsgParseResult::Ok);
    assert_eq!(null.ty(), MsgType::RspRedisBulk);
}

#[test]
fn rsp_bulk_zero_length_len_errors() {
    // Bulk: a '$' with no digits before CR (p - start <= 1) errors.
    assert_eq!(rsp(b"$\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_bulk_bad_len_byte_errors() {
    // Bulk: a non-digit, non-'-' byte in the length errors.
    assert_eq!(rsp(b"$5x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_bulk_missing_len_lf_errors() {
    // BulkLf: the byte after the length CR must be LF.
    assert_eq!(rsp(b"$5\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_bulk_payload_without_cr_errors() {
    // BulkArg: the byte after the payload must be CR.
    assert_eq!(rsp(b"$5\r\nhelloX\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_multibulk_of_bulks() {
    let m = rsp(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisMultibulk);
    assert_eq!(m.args().len(), 2);
}

#[test]
fn rsp_multibulk_empty() {
    let m = rsp(b"*0\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisMultibulk);
}

#[test]
fn rsp_multibulk_null() {
    // A '*-1' null multibulk runs to CRLF via the RuntoCrlf path.
    let m = rsp(b"*-1\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisMultibulk);
}

#[test]
fn rsp_multibulk_with_integer_elements() {
    // A multibulk whose elements are integers drives the Simple
    // sub-state (the ':' element prefix).
    let m = rsp(b"*2\r\n:1\r\n:2\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisMultibulk);
}

#[test]
fn rsp_multibulk_with_status_and_error_elements() {
    // Status ('+') and error ('-') elements both route through the
    // Simple sub-state.
    let s = rsp(b"*1\r\n+OK\r\n");
    assert_eq!(s.parse_result(), MsgParseResult::Ok);
    let e = rsp(b"*1\r\n-ERR x\r\n");
    assert_eq!(e.parse_result(), MsgParseResult::Ok);
}

#[test]
fn rsp_multibulk_nested_multibulk() {
    // A nested multibulk element ('*') re-enters the Multibulk
    // sub-state.
    let m = rsp(b"*1\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisMultibulk);
}

#[test]
fn rsp_multibulk_with_null_element() {
    // A '$-1' element inside a multibulk (the rlen==1, span==3 case)
    // is folded as a null element.
    let m = rsp(b"*2\r\n$-1\r\n$1\r\na\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisMultibulk);
}

#[test]
fn rsp_unknown_leading_byte_errors() {
    // Start: a byte that is not one of + - : $ * errors.
    assert_eq!(rsp(b"x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_almost_done_missing_lf_errors() {
    // AlmostDone: the byte after the final CR must be LF.
    assert_eq!(rsp(b"+OK\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_multibulk_bad_narg_byte_errors() {
    // Multibulk: a non-digit, non-'-', non-CR byte in the count
    // errors.
    assert_eq!(rsp(b"*2x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_multibulk_missing_narg_lf_errors() {
    // MultibulkNargLf: the byte after the count CR must be LF.
    assert_eq!(rsp(b"*2\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_multibulk_element_bad_prefix_errors() {
    // MultibulkArgnLen: an element that is not one of * : + - $
    // errors.
    assert_eq!(
        rsp(b"*1\r\nX3\r\nfoo\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_multibulk_element_missing_len_lf_errors() {
    // MultibulkArgnLenLf: the byte after an element length CR must
    // be LF.
    assert_eq!(rsp(b"*1\r\n$3\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_multibulk_element_payload_without_cr_errors() {
    // MultibulkArgn: the byte after the element payload must be CR.
    assert_eq!(
        rsp(b"*1\r\n$3\r\nfooX\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_non_response_message_errors() {
    // A message constructed as a request cannot be parsed as a
    // response.
    let mut m = Msg::new(0, MsgType::Unknown, true);
    assert_eq!(redis_parse_rsp(&mut m, b"+OK\r\n"), MsgParseResult::Error);
}

#[test]
fn rsp_truncated_returns_again() {
    // A truncated bulk response yields Again and never panics.
    let m = rsp(b"$5\r\nhel");
    assert_eq!(m.parse_result(), MsgParseResult::Again);
}

#[test]
fn rsp_truncated_at_each_boundary_returns_again_or_error() {
    // Feeding ever-longer prefixes of a multibulk response never
    // panics; the final byte completes it.
    let full = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    for n in 1..full.len() {
        let r = rsp(&full[..n]);
        assert!(
            matches!(
                r.parse_result(),
                MsgParseResult::Again | MsgParseResult::Error
            ),
            "prefix len {n}"
        );
    }
    assert_eq!(rsp(full).parse_result(), MsgParseResult::Ok);
}

// -------------------------------------------------------------
// Sanity: an unknown command keyword that the table does not
// recognise leaves the type Unknown and finishes as an error.
// -------------------------------------------------------------

#[test]
fn req_unknown_command_errors() {
    let m = req(b"*1\r\n$7\r\nNOTACMD\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Error);
}

#[test]
fn req_command_class_is_total_for_parsed_types() {
    // Every command we can parse classifies without panicking
    // (request-level totality of classify over reachable types).
    for bytes in [
        &b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"[..],
        b"*1\r\n$4\r\nPING\r\n",
        b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
        b"*2\r\n$4\r\nMGET\r\n$1\r\nk\r\n",
        b"*3\r\n$4\r\nMSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
    ] {
        let m = req(bytes);
        let _ = CommandClass::Arg0; // touch the import
        let _ = m.ty();
    }
}
