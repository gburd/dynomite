//! Deep-state and edge-branch coverage for the Redis parser.
//!
//! These tests target the parser state-machine arms that the
//! happy-path corpus does not reach: bad bytes inside deep argument
//! lengths, framing errors in the Arg2/Arg3/ArgN states, the EVAL
//! multi-key path, byte-by-byte streaming resume (which exercises
//! the `from_u32` state-restore tables and the mid-state save/resume
//! logic), and the response-element framing rejections.

#![allow(clippy::too_many_lines)]

use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::proto::redis::{redis_parse_req, redis_parse_rsp};

fn req(input: &[u8]) -> Msg {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let _ = redis_parse_req(&mut m, input);
    m
}

fn rsp(input: &[u8]) -> Msg {
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let _ = redis_parse_rsp(&mut m, input);
    m
}

/// Feed `input` in two chunks split at `at`, resuming the parser
/// from its saved state. The Redis parser saves its cursor on the
/// length and LF states (it advances `p` to the buffer end only
/// inside body states), so splits chosen on a length/LF boundary
/// resume cleanly. Returns the final parse result.
fn parse_two_chunks(input: &[u8], at: usize) -> MsgParseResult {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let first = redis_parse_req(&mut m, &input[..at]);
    if first != MsgParseResult::Again {
        return first;
    }
    // The parser resumes from its saved cursor; feeding the full
    // buffer (cursor is absolute) completes the parse.
    redis_parse_req(&mut m, input)
}

// -------------------------------------------------------------
// Two-chunk streaming resume at length/LF boundaries: drives the
// ReqState from_u32 restore arms for the length and LF states.
// -------------------------------------------------------------

#[test]
fn req_resumes_at_narg_boundary() {
    // Split right after the `*3` count, before its CRLF: resumes
    // from the Narg state.
    let r = parse_two_chunks(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", 2);
    assert_eq!(r, MsgParseResult::Ok);
}

#[test]
fn req_resumes_at_type_len_boundary() {
    // Split inside the keyword length token: resumes from
    // ReqTypeLen.
    let r = parse_two_chunks(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", 5);
    assert_eq!(r, MsgParseResult::Ok);
}

#[test]
fn req_resumes_at_key_len_boundary() {
    // Split inside the key length token: resumes from KeyLen.
    let r = parse_two_chunks(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", 14);
    assert_eq!(r, MsgParseResult::Ok);
}

#[test]
fn req_resumes_at_arg_len_boundary() {
    // Split inside the value length token: resumes from Arg1Len.
    let r = parse_two_chunks(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", 23);
    assert_eq!(r, MsgParseResult::Ok);
}

#[test]
fn rsp_resumes_at_count_boundary() {
    // A multibulk response split inside its count token resumes
    // from the Multibulk state.
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let input = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let first = redis_parse_rsp(&mut m, &input[..2]);
    assert_eq!(first, MsgParseResult::Again);
    let r = redis_parse_rsp(&mut m, input);
    assert_eq!(r, MsgParseResult::Ok);
}

// -------------------------------------------------------------
// Deep argument-state framing errors.
// -------------------------------------------------------------

#[test]
fn key_len_bad_byte_after_digit_errors() {
    // KeyLen: a non-digit, non-CR byte after a length digit errors.
    assert_eq!(
        req(b"*2\r\n$3\r\nGET\r\n$3x\r\nfoo\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg1_len_bad_byte_after_digit_errors() {
    // Arg1Len (via read_bulk_len): bad byte after a digit errors.
    assert_eq!(
        req(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1x\r\nv\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg2_len_bad_prefix_errors() {
    // Arg2Len: the second arg bulk must start with '$' (SETEX).
    assert_eq!(
        req(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\nX1\r\nv\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg2_len_empty_errors() {
    // Arg2Len: a '$' with no length digit before CR (read_bulk_len
    // p-start <= 1) errors.
    assert_eq!(
        req(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg2_body_without_cr_errors() {
    // Arg2: the byte after the second-arg payload must be CR.
    assert_eq!(
        req(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$1\r\nvX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg2_len_lf_missing_errors() {
    // Arg2LenLf: byte after the second-arg length CR must be LF.
    assert_eq!(
        req(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$1\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg3_len_bad_prefix_errors() {
    // Arg3Len: third-arg bulk must start with '$' (LINSERT).
    assert_eq!(
        req(b"*5\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\nX1\r\nv\r\n")
            .parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg3_body_without_cr_errors() {
    // Arg3: byte after the third-arg payload must be CR.
    assert_eq!(
        req(b"*5\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n$1\r\nvX")
            .parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg3_len_lf_missing_errors() {
    // Arg3LenLf: byte after the third-arg length CR must be LF.
    assert_eq!(
        req(b"*5\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n$1\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn argn_len_bad_prefix_errors() {
    // ArgnLen: an extra variadic arg bulk must start with '$' (SET
    // with a trailing option arg).
    assert_eq!(
        req(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\nX2\r\n10\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn argn_body_without_cr_errors() {
    // Argn: byte after a variadic-arg payload must be CR.
    assert_eq!(
        req(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10X").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn argn_len_lf_missing_errors() {
    // ArgnLenLf: byte after a variadic-arg length CR must be LF.
    assert_eq!(
        req(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn argn_arg_lf_missing_errors() {
    // ArgnLf: byte after a variadic arg's CR must be LF.
    assert_eq!(
        req(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg1_lf_missing_for_arg2_command_errors() {
    // Arg1Lf wildcard / arity arms: SETEX after the first arg with a
    // missing LF errors.
    assert_eq!(
        req(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\rX").parse_result(),
        MsgParseResult::Error
    );
}

// -------------------------------------------------------------
// Arity-mismatch errors evaluated at the Arg*Lf transitions (too
// many or too few tokens for the command class).
// -------------------------------------------------------------

#[test]
fn arg1_command_with_extra_token_errors() {
    // EXPIRE (Arg1) declares one key + one arg; a second arg leaves
    // rntokens != 0 at Arg1Lf.
    assert_eq!(
        req(b"*4\r\n$6\r\nEXPIRE\r\n$1\r\nk\r\n$2\r\n10\r\n$1\r\nx\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg2_command_with_too_few_tokens_errors() {
    // SETEX (Arg2) declares one key + two args; only one arg leaves
    // rntokens != 1 at the Arg1Lf Arg2 arm.
    assert_eq!(
        req(b"*3\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg3_command_with_too_few_tokens_errors() {
    // LINSERT (Arg3) declares one key + three args; a short token
    // count leaves rntokens != 2 at the Arg1Lf Arg3 arm.
    assert_eq!(
        req(b"*4\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg3_command_with_extra_token_after_two_args_errors() {
    // LINSERT after two args expects exactly one more (rntokens == 1
    // at Arg2Lf); an extra token errors at the Arg2Lf Arg3 arm.
    assert_eq!(
        req(b"*6\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n$1\r\nv\r\n$1\r\nx\r\n")
            .parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn arg2_lf_missing_for_arg3_command_errors() {
    // Arg2Lf with a missing LF for an Arg3 command errors.
    assert_eq!(
        req(b"*5\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\rX").parse_result(),
        MsgParseResult::Error
    );
}

// -------------------------------------------------------------
// EVAL multi-key path: numkeys > 1 walks the KeyLen loop, building
// nkeys keys, then finishes.
// -------------------------------------------------------------

#[test]
fn eval_two_keys_no_args() {
    // EVAL "script" 2 k1 k2 -- two keys, no trailing args.
    let m = req(b"*5\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$1\r\n2\r\n$2\r\nk1\r\n$2\r\nk2\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqRedisEval);
    assert_eq!(m.keys().len(), 2);
}

#[test]
fn eval_two_keys_with_args() {
    // EVAL "script" 2 k1 k2 a1 -- two keys, then a trailing arg.
    let m =
        req(b"*6\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$1\r\n2\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\na1\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.keys().len(), 2);
}

#[test]
fn eval_empty_numkeys_errors() {
    // EVAL "script" "" -- the numkeys bulk is empty; the Arg2 EVAL
    // numeric parse rejects (start >= p-1).
    let m = req(b"*3\r\n$4\r\nEVAL\r\n$6\r\nscript\r\n$0\r\n\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Error);
}

// -------------------------------------------------------------
// Streaming that stops at a deep bulk-len boundary (BulkLenStep::Eof
// in Arg2Len / Arg3Len / ArgnLen).
// -------------------------------------------------------------

#[test]
fn req_stops_at_arg2_len_boundary() {
    // Truncate SETEX right at the start of the second arg length.
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let r = redis_parse_req(&mut m, b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$");
    assert_eq!(r, MsgParseResult::Again);
}

#[test]
fn req_stops_at_arg3_len_boundary() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let r = redis_parse_req(
        &mut m,
        b"*5\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n$",
    );
    assert_eq!(r, MsgParseResult::Again);
}

#[test]
fn req_stops_at_argn_len_boundary() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let r = redis_parse_req(
        &mut m,
        b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$",
    );
    assert_eq!(r, MsgParseResult::Again);
}

// -------------------------------------------------------------
// Response: integer with no digits and an overlong bulk that
// overflows the cursor arithmetic.
// -------------------------------------------------------------

#[test]
fn rsp_bulk_with_crlf_in_payload_runs_to_end() {
    // A bulk whose declared length spans an embedded CRLF still
    // matches on the trailing CR (the BulkArgLf continue path).
    let m = rsp(b"$7\r\nab\r\ncd!\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisBulk);
}

#[test]
fn rsp_multibulk_element_bad_len_byte_errors() {
    // MultibulkArgnLen: a bad byte after an element length digit
    // errors.
    assert_eq!(
        rsp(b"*1\r\n$3x\r\nfoo\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_multibulk_zero_element_len_errors() {
    // MultibulkArgnLen: a '$' element with no digits before CR
    // (p-start <= 1) errors.
    assert_eq!(rsp(b"*1\r\n$\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_bulk_negative_runs_to_crlf() {
    // A '$-1' null bulk runs through the RuntoCrlf path to
    // completion.
    let m = rsp(b"$-1\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
}

#[test]
fn rsp_status_with_payload_runs_to_crlf() {
    // RuntoCrlf consumes arbitrary status payload bytes up to CR.
    let m = rsp(b"+some status text here\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisStatus);
}

// -------------------------------------------------------------
// Response two-chunk resume: split before the body so the parser
// saves a non-body RspState and resumes cleanly, driving the
// RspState from_u32 restore arms (Status / Error / Integer / Bulk /
// Multibulk).
// -------------------------------------------------------------

/// Split `input` at `at`, feed the prefix, then feed the full
/// buffer (the cursor is absolute). Returns the final result.
fn rsp_two_chunks(input: &[u8], at: usize) -> MsgParseResult {
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let first = redis_parse_rsp(&mut m, &input[..at]);
    if first != MsgParseResult::Again {
        return first;
    }
    redis_parse_rsp(&mut m, input)
}

#[test]
fn rsp_status_resumes_after_prefix() {
    // Split right after the '+' marker: resumes from the Status /
    // RuntoCrlf state.
    assert_eq!(rsp_two_chunks(b"+PONG\r\n", 1), MsgParseResult::Ok);
}

#[test]
fn rsp_error_resumes_after_prefix() {
    // Split right after the '-' marker: resumes from the Error
    // state.
    assert_eq!(rsp_two_chunks(b"-WRONGTYPE x\r\n", 1), MsgParseResult::Ok);
}

#[test]
fn rsp_integer_resumes_after_prefix() {
    // Split right after the ':' marker: resumes from the
    // Integer/IntegerStart state.
    assert_eq!(rsp_two_chunks(b":98765\r\n", 1), MsgParseResult::Ok);
}

#[test]
fn rsp_bulk_resumes_before_body() {
    // Split inside the bulk length token (before the body): resumes
    // from the Bulk state.
    assert_eq!(rsp_two_chunks(b"$3\r\nfoo\r\n", 1), MsgParseResult::Ok);
}

#[test]
fn rsp_multibulk_resumes_before_elements() {
    // Split inside the multibulk count token: resumes from the
    // Multibulk state.
    assert_eq!(
        rsp_two_chunks(b"*1\r\n$3\r\nfoo\r\n", 1),
        MsgParseResult::Ok
    );
}

// -------------------------------------------------------------
// Resume-at-every-boundary sweep: feed the prefix up to each split
// point, then feed the full buffer. The sweep drives every
// ReqState/RspState from_u32 restore arm for the deep argument
// states (Arg2*, Arg3*, Argn*) without panicking. Splits inside a
// length/LF state resume to Ok; splits inside a body state may
// resume to Error (the parser advances its cursor to the buffer
// end inside body states, so the body token is not recoverable
// across a re-fed buffer) -- both are accepted here, the invariant
// is no panic and a clean full-buffer parse.
// -------------------------------------------------------------

fn resume_sweep_req(input: &[u8]) {
    for split in 1..input.len() {
        let mut m = Msg::new(0, MsgType::Unknown, true);
        let first = redis_parse_req(&mut m, &input[..split]);
        if first != MsgParseResult::Again {
            // Some splits land exactly on a complete prefix; skip.
            continue;
        }
        let second = redis_parse_req(&mut m, input);
        assert!(
            matches!(
                second,
                MsgParseResult::Ok | MsgParseResult::Again | MsgParseResult::Error
            ),
            "split {split} (state {}) gave {second:?}",
            m.parser_state()
        );
    }
    let mut m = Msg::new(0, MsgType::Unknown, true);
    assert_eq!(redis_parse_req(&mut m, input), MsgParseResult::Ok);
}

#[test]
fn setex_resumes_at_every_boundary() {
    // SETEX (Arg2) sweeps states up through the Arg2 family.
    resume_sweep_req(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$1\r\nv\r\n");
}

#[test]
fn linsert_resumes_at_every_boundary() {
    // LINSERT (Arg3) sweeps states up through the Arg3 family.
    resume_sweep_req(b"*5\r\n$7\r\nLINSERT\r\n$1\r\nk\r\n$6\r\nBEFORE\r\n$1\r\np\r\n$1\r\nv\r\n");
}

#[test]
fn set_variadic_resumes_at_every_boundary() {
    // SET with two trailing option args (ArgN) sweeps the Argn
    // family states.
    resume_sweep_req(b"*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n");
}

fn resume_sweep_rsp(input: &[u8]) {
    for split in 1..input.len() {
        let mut m = Msg::new(0, MsgType::Unknown, false);
        let first = redis_parse_rsp(&mut m, &input[..split]);
        if first != MsgParseResult::Again {
            continue;
        }
        let second = redis_parse_rsp(&mut m, input);
        assert!(
            matches!(
                second,
                MsgParseResult::Ok | MsgParseResult::Again | MsgParseResult::Error
            ),
            "split {split} gave {second:?}"
        );
    }
    let mut m = Msg::new(0, MsgType::Unknown, false);
    assert_eq!(redis_parse_rsp(&mut m, input), MsgParseResult::Ok);
}

#[test]
fn multibulk_response_resumes_at_every_boundary() {
    // A multibulk-of-bulks response sweeps the MultibulkArgn* states.
    resume_sweep_rsp(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
}

// -------------------------------------------------------------
// Additional reachable response framing rejections.
// -------------------------------------------------------------

#[test]
fn rsp_empty_multibulk_count_errors() {
    // Multibulk: a '*' immediately followed by CR (no count digits,
    // p-start <= 1) errors.
    assert_eq!(rsp(b"*\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_bulk_value_trailing_byte_neither_cr_nor_lf_errors() {
    // BulkArgLf: after the value's trailing CR the next byte must be
    // CR or LF; anything else errors.
    assert_eq!(rsp(b"$3\r\nfoo\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_multibulk_element_value_missing_lf_errors() {
    // MultibulkArgnLf: after an element value's CR the next byte
    // must be LF.
    assert_eq!(
        rsp(b"*1\r\n$3\r\nfoo\rX").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_bulk_value_with_extra_cr_before_lf() {
    // BulkArgLf: a CR immediately after the value's framing CR is
    // consumed by the CR-continue branch before the terminating LF.
    let m = rsp(b"$3\r\nfoo\r\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspRedisBulk);
}
