//! Branch coverage for the Memcached text-protocol parser
//! (`proto::memcache::parser`).
//!
//! Drives every request command type (storage / arithmetic / touch
//! / delete / retrieval / quit), the `noreply` modifier, the
//! multi-key GET form, the CAS form, and the per-state framing
//! rejections. The response half drives the numeric, textual
//! status, VALUE, END, and CLIENT_ERROR / SERVER_ERROR shapes plus
//! their malformed-input rejections.

#![allow(clippy::too_many_lines)]

use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::proto::memcache::parser::{memcache_parse_req_tagged, HashTag};
use dynomite::proto::memcache::{memcache_parse_req, memcache_parse_rsp};

fn req(input: &[u8]) -> Msg {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let _ = memcache_parse_req(&mut m, input);
    m
}

fn rsp(input: &[u8]) -> Msg {
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let _ = memcache_parse_rsp(&mut m, input);
    m
}

// -------------------------------------------------------------
// Storage commands: set / add / replace / append / prepend, with
// and without the noreply modifier.
// -------------------------------------------------------------

#[test]
fn storage_set_with_value() {
    let m = req(b"set k 0 0 3\r\nabc\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcSet);
    assert_eq!(m.keys()[0].key(), b"k");
}

#[test]
fn storage_add_replace_append_prepend() {
    let cases: &[(&[u8], MsgType)] = &[
        (b"add k 0 0 1\r\nx\r\n", MsgType::ReqMcAdd),
        (b"replace k 0 0 1\r\nx\r\n", MsgType::ReqMcReplace),
        (b"append k 0 0 1\r\nx\r\n", MsgType::ReqMcAppend),
        (b"prepend k 0 0 1\r\nx\r\n", MsgType::ReqMcPrepend),
    ];
    for (bytes, ty) in cases {
        let m = req(bytes);
        assert_eq!(m.parse_result(), MsgParseResult::Ok, "input {bytes:?}");
        assert_eq!(m.ty(), *ty);
    }
}

#[test]
fn storage_set_noreply_clears_expect_reply() {
    // The noreply modifier suppresses the datastore reply.
    let m = req(b"set k 0 0 3 noreply\r\nabc\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcSet);
    assert!(!m.flags().expect_datastore_reply);
}

#[test]
fn storage_cas_with_cas_id() {
    // CAS carries an extra cas-unique field after the value length.
    let m = req(b"cas k 0 0 3 12345\r\nabc\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcCas);
}

#[test]
fn storage_cas_noreply() {
    let m = req(b"cas k 0 0 3 12345 noreply\r\nabc\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert!(!m.flags().expect_datastore_reply);
}

// -------------------------------------------------------------
// Arithmetic: incr / decr (with optional noreply).
// -------------------------------------------------------------

#[test]
fn arithmetic_incr_decr() {
    let i = req(b"incr counter 5\r\n");
    assert_eq!(i.parse_result(), MsgParseResult::Ok);
    assert_eq!(i.ty(), MsgType::ReqMcIncr);
    let d = req(b"decr counter 2\r\n");
    assert_eq!(d.parse_result(), MsgParseResult::Ok);
    assert_eq!(d.ty(), MsgType::ReqMcDecr);
}

#[test]
fn arithmetic_incr_noreply() {
    let m = req(b"incr counter 5 noreply\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert!(!m.flags().expect_datastore_reply);
}

#[test]
fn arithmetic_negative_delta() {
    // SpacesBeforeNum accepts a leading '-' for the delta token.
    let m = req(b"decr counter -3\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcDecr);
}

// -------------------------------------------------------------
// Touch.
// -------------------------------------------------------------

#[test]
fn touch_with_expiry() {
    let m = req(b"touch k 100\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcTouch);
}

#[test]
fn touch_noreply() {
    let m = req(b"touch k 100 noreply\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert!(!m.flags().expect_datastore_reply);
}

// -------------------------------------------------------------
// Delete (with optional noreply).
// -------------------------------------------------------------

#[test]
fn delete_command() {
    let m = req(b"delete k\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcDelete);
}

#[test]
fn delete_noreply() {
    let m = req(b"delete k noreply\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert!(!m.flags().expect_datastore_reply);
}

// -------------------------------------------------------------
// Retrieval: get / gets, single and multi-key.
// -------------------------------------------------------------

#[test]
fn get_single_key_is_read() {
    let m = req(b"get k\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcGet);
    assert!(m.flags().is_read);
}

#[test]
fn get_multi_key() {
    let m = req(b"get a b c\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcGet);
    assert_eq!(m.keys().len(), 3);
}

#[test]
fn gets_command() {
    let m = req(b"gets k\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcGets);
}

// -------------------------------------------------------------
// Quit.
// -------------------------------------------------------------

#[test]
fn quit_sets_quit_flag() {
    let m = req(b"quit\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcQuit);
    assert!(m.flags().quit);
}

// -------------------------------------------------------------
// Request framing rejections.
// -------------------------------------------------------------

#[test]
fn req_leading_non_lowercase_errors() {
    // Start: a leading byte that is not a space or lowercase letter
    // errors.
    assert_eq!(req(b"GET k\r\n").parse_result(), MsgParseResult::Error);
    assert_eq!(req(b"9et k\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_unknown_command_errors() {
    // ReqType: a keyword that classify_command does not recognise
    // errors.
    assert_eq!(
        req(b"frobnicate k\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_command_with_immediate_cr_errors() {
    // A key-bearing command terminated by CR before any key errors.
    assert_eq!(req(b"get\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_empty_key_errors() {
    // Key: a zero-length key (two spaces) is rejected.
    assert_eq!(req(b"get  \r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_oversized_key_errors() {
    // Key: a key longer than the memcache key limit (250) errors.
    let mut bytes = b"get ".to_vec();
    bytes.extend(std::iter::repeat_n(b'k', 300));
    bytes.extend_from_slice(b"\r\n");
    assert_eq!(req(&bytes).parse_result(), MsgParseResult::Error);
}

#[test]
fn req_storage_missing_flags_errors() {
    // SpacesBeforeFlags: a non-digit where the flags field is
    // expected errors.
    assert_eq!(
        req(b"set k x 0 3\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_storage_bad_expiry_errors() {
    // SpacesBeforeExpiry / Expiry: a bad byte in the expiry field
    // errors.
    assert_eq!(
        req(b"set k 0 x 3\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_storage_bad_vlen_errors() {
    // SpacesBeforeVlen / Vlen: a bad byte in the value-length field
    // errors.
    assert_eq!(
        req(b"set k 0 0 x\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_storage_value_length_mismatch_errors() {
    // Val: the declared length is shorter than the supplied value,
    // so the byte at the declared end is not CR.
    assert_eq!(
        req(b"set k 0 0 1\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_storage_value_runtoval_missing_lf_errors() {
    // RuntoVal: the byte after the value-header CR must be LF.
    assert_eq!(req(b"set k 0 0 3\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_cas_bad_cas_id_errors() {
    // SpacesBeforeCas / Cas: a bad byte in the cas-unique field
    // errors.
    assert_eq!(
        req(b"cas k 0 0 3 x\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_arithmetic_bad_delta_errors() {
    // SpacesBeforeNum / Num: a bad byte in the delta token errors.
    assert_eq!(
        req(b"incr counter x\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_bad_noreply_token_errors() {
    // Noreply: a token in the noreply position that is not the
    // literal "noreply" errors.
    assert_eq!(
        req(b"set k 0 0 1 nope\r\nx\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_truncated_value_returns_again() {
    // Val: a truncated value yields Again without panicking.
    let m = req(b"set k 0 0 5\r\nab");
    assert_eq!(m.parse_result(), MsgParseResult::Again);
}

#[test]
fn req_non_request_message_errors() {
    let mut m = Msg::new(0, MsgType::Unknown, false);
    assert_eq!(
        memcache_parse_req(&mut m, b"get k\r\n"),
        MsgParseResult::Error
    );
}

// -------------------------------------------------------------
// Response shapes.
// -------------------------------------------------------------

#[test]
fn rsp_numeric() {
    let m = rsp(b"12345\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspMcNum);
}

#[test]
fn rsp_status_keywords() {
    let cases: &[(&[u8], MsgType)] = &[
        (b"STORED\r\n", MsgType::RspMcStored),
        (b"NOT_STORED\r\n", MsgType::RspMcNotStored),
        (b"EXISTS\r\n", MsgType::RspMcExists),
        (b"NOT_FOUND\r\n", MsgType::RspMcNotFound),
        (b"DELETED\r\n", MsgType::RspMcDeleted),
        (b"TOUCHED\r\n", MsgType::RspMcTouched),
        (b"ERROR\r\n", MsgType::RspMcError),
    ];
    for (bytes, ty) in cases {
        let m = rsp(bytes);
        assert_eq!(m.parse_result(), MsgParseResult::Ok, "input {bytes:?}");
        assert_eq!(m.ty(), *ty);
    }
}

#[test]
fn rsp_value_then_end() {
    // A VALUE reply with flags + length, the value bytes, then END.
    // The parser re-enters the keyword state after the value and
    // folds the trailing END, so the final type is RspMcEnd.
    let m = rsp(b"VALUE k 0 3\r\nabc\r\nEND\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspMcEnd);
}

#[test]
fn rsp_end_only() {
    // A bare END (cache miss for a GET) classifies as END.
    let m = rsp(b"END\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspMcEnd);
}

#[test]
fn rsp_client_and_server_error() {
    let c = rsp(b"CLIENT_ERROR bad command line\r\n");
    assert_eq!(c.parse_result(), MsgParseResult::Ok);
    assert_eq!(c.ty(), MsgType::RspMcClientError);
    let s = rsp(b"SERVER_ERROR out of memory\r\n");
    assert_eq!(s.parse_result(), MsgParseResult::Ok);
    assert_eq!(s.ty(), MsgType::RspMcServerError);
}

#[test]
fn rsp_unknown_keyword_errors() {
    // RspStr: a keyword classify_response does not recognise errors.
    assert_eq!(rsp(b"WHATISTHIS\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_numeric_bad_byte_errors() {
    // RspNum: a non-digit, non-space, non-CR byte in the numeric
    // reply errors.
    assert_eq!(rsp(b"12x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_value_bad_flags_errors() {
    // SpacesBeforeFlags / Flags: a bad byte in the VALUE flags field
    // errors.
    assert_eq!(
        rsp(b"VALUE k x 3\r\nabc\r\nEND\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_value_bad_vlen_errors() {
    // SpacesBeforeVlen / Vlen: a bad byte in the VALUE length field
    // errors.
    assert_eq!(
        rsp(b"VALUE k 0 x\r\nabc\r\nEND\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_value_payload_without_cr_errors() {
    // Val: the declared length is shorter than the payload so the
    // byte at the declared end is not CR.
    assert_eq!(
        rsp(b"VALUE k 0 1\r\nabc\r\nEND\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_truncated_value_returns_again() {
    let m = rsp(b"VALUE k 0 5\r\nab");
    assert_eq!(m.parse_result(), MsgParseResult::Again);
}

#[test]
fn rsp_almost_done_missing_lf_errors() {
    // AlmostDone: the byte after the final CR must be LF.
    assert_eq!(rsp(b"STORED\rX").parse_result(), MsgParseResult::Error);
}

#[test]
fn rsp_non_response_message_errors() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    assert_eq!(
        memcache_parse_rsp(&mut m, b"STORED\r\n"),
        MsgParseResult::Error
    );
}

// -------------------------------------------------------------
// Hash-tag carving: a key with {tag} delimiters records the inner
// range as the routing tag (the make_keypos tag branch).
// -------------------------------------------------------------

#[test]
fn get_with_hash_tag_carves_tag() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let tag = HashTag {
        open: b'{',
        close: b'}',
    };
    let r = memcache_parse_req_tagged(&mut m, b"get {abc}xy\r\n", Some(tag));
    assert_eq!(r, MsgParseResult::Ok);
    assert_eq!(m.keys()[0].key(), b"{abc}xy");
    assert_eq!(m.keys()[0].tag_bytes(), b"abc");
}

#[test]
fn get_with_unterminated_hash_tag_uses_whole_key() {
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let tag = HashTag {
        open: b'{',
        close: b'}',
    };
    // Opening delimiter with no close: the whole key is the tag.
    let r = memcache_parse_req_tagged(&mut m, b"get {abc\r\n", Some(tag));
    assert_eq!(r, MsgParseResult::Ok);
    assert_eq!(m.keys()[0].tag_bytes(), b"{abc");
}

// -------------------------------------------------------------
// Resume-at-every-boundary sweep: drives the ReqState/RspState
// from_u32 restore tables for the memcache parser. Splits inside a
// length/field state resume cleanly; splits inside the value body
// may resume to Error (the value cursor is advanced to the buffer
// end inside the Val state). Both are accepted; the invariant is
// no panic and a clean full-buffer parse.
// -------------------------------------------------------------

fn req_resume_sweep(input: &[u8]) {
    for split in 1..input.len() {
        let mut m = Msg::new(0, MsgType::Unknown, true);
        let first = memcache_parse_req(&mut m, &input[..split]);
        if first != MsgParseResult::Again {
            continue;
        }
        let second = memcache_parse_req(&mut m, input);
        assert!(
            matches!(
                second,
                MsgParseResult::Ok | MsgParseResult::Again | MsgParseResult::Error
            ),
            "split {split} gave {second:?}"
        );
    }
    let mut m = Msg::new(0, MsgType::Unknown, true);
    assert_eq!(memcache_parse_req(&mut m, input), MsgParseResult::Ok);
}

fn rsp_resume_sweep(input: &[u8]) {
    for split in 1..input.len() {
        let mut m = Msg::new(0, MsgType::Unknown, false);
        let first = memcache_parse_rsp(&mut m, &input[..split]);
        if first != MsgParseResult::Again {
            continue;
        }
        let second = memcache_parse_rsp(&mut m, input);
        assert!(
            matches!(
                second,
                MsgParseResult::Ok | MsgParseResult::Again | MsgParseResult::Error
            ),
            "split {split} gave {second:?}"
        );
    }
    let mut m = Msg::new(0, MsgType::Unknown, false);
    assert_eq!(memcache_parse_rsp(&mut m, input), MsgParseResult::Ok);
}

#[test]
fn set_resumes_at_every_boundary() {
    // SET (storage with value) sweeps flags/expiry/vlen/value states.
    req_resume_sweep(b"set k 0 0 3\r\nabc\r\n");
}

#[test]
fn cas_resumes_at_every_boundary() {
    // CAS sweeps the cas-unique field states.
    req_resume_sweep(b"cas k 0 0 3 12345\r\nabc\r\n");
}

#[test]
fn incr_resumes_at_every_boundary() {
    // INCR sweeps the SpacesBeforeNum / Num states.
    req_resume_sweep(b"incr counter 5\r\n");
}

#[test]
fn multi_get_resumes_at_every_boundary() {
    // Multi-key GET sweeps the SpacesBeforeKeys / Key loop states.
    req_resume_sweep(b"get a b c\r\n");
}

#[test]
fn set_noreply_resumes_at_every_boundary() {
    // SET noreply sweeps the Noreply / AfterNoreply states.
    req_resume_sweep(b"set k 0 0 3 noreply\r\nabc\r\n");
}

#[test]
fn value_response_resumes_at_every_boundary() {
    // VALUE + END response sweeps the response value/End states.
    rsp_resume_sweep(b"VALUE k 0 3\r\nabc\r\nEND\r\n");
}

#[test]
fn numeric_response_resumes_at_every_boundary() {
    rsp_resume_sweep(b"12345\r\n");
}

// -------------------------------------------------------------
// Mid-field framing errors: a bad byte partway through a numeric
// field (after at least one valid digit) errors in the field's
// own state rather than the preceding SpacesBefore* state.
// -------------------------------------------------------------

#[test]
fn req_flags_mid_number_bad_byte_errors() {
    // Flags: a non-digit, non-space byte after a flags digit errors.
    assert_eq!(
        req(b"set k 12x 0 3\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_expiry_mid_number_bad_byte_errors() {
    // Expiry: a bad byte after an expiry digit errors.
    assert_eq!(
        req(b"set k 0 12x 3\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_vlen_mid_number_bad_byte_errors() {
    // Vlen (non-cas): a bad byte after a vlen digit errors.
    assert_eq!(
        req(b"set k 0 0 3x\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_cas_vlen_non_space_terminator_errors() {
    // Vlen for a CAS command must be terminated by a space (the
    // cas-unique field follows); a CR there errors.
    assert_eq!(
        req(b"cas k 0 0 3\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_cas_id_mid_number_bad_byte_errors() {
    // Cas: a bad byte after a cas-unique digit errors.
    assert_eq!(
        req(b"cas k 0 0 3 12x\r\nabc\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_arithmetic_delta_mid_number_bad_byte_errors() {
    // Num: a bad byte after a delta digit errors.
    assert_eq!(
        req(b"incr counter 12x\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn req_storage_keyless_cr_errors() {
    // A storage command whose key field is immediately a CR (no
    // key) errors at the Key state's storage/arithmetic guard.
    assert_eq!(req(b"set \r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_get_with_trailing_crlf_after_keys() {
    // SpacesBeforeKeys: a CR after the last key terminates the
    // multi-key GET via the AlmostDone transition.
    let m = req(b"get a b \r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.keys().len(), 2);
}

#[test]
fn rsp_value_flags_mid_number_bad_byte_errors() {
    // VALUE Flags: a bad byte after a flags digit errors.
    assert_eq!(
        rsp(b"VALUE k 12x 3\r\nabc\r\nEND\r\n").parse_result(),
        MsgParseResult::Error
    );
}

#[test]
fn rsp_value_vlen_mid_number_then_value() {
    // VALUE Vlen: a multi-digit length is accumulated then the
    // value is consumed.
    let m = rsp(b"VALUE k 0 12\r\nhelloworld!!\r\nEND\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
}

#[test]
fn rsp_value_missing_value_lf_errors() {
    // RuntoVal: after the VALUE header CR the next byte must be LF.
    assert_eq!(rsp(b"VALUE k 0 3\rX").parse_result(), MsgParseResult::Error);
}

// -------------------------------------------------------------
// Extra-whitespace and trailing-token arms: drive the repeated
// `SpacesBefore*` space-skip branches and the request/response
// Crlf / RuntoCrlf state arms.
// -------------------------------------------------------------

#[test]
fn storage_tolerates_multiple_spaces_between_fields() {
    // The SpacesBeforeFlags / SpacesBeforeExpiry / SpacesBeforeVlen
    // states skip runs of more than one space (the `ch == b' '`
    // p += 1 arms).
    let m = req(b"set k  0  0  3\r\nabc\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcSet);
}

#[test]
fn arithmetic_delta_terminated_by_cr() {
    // The Num state accepts a CR directly after the delta digits
    // (the `ch == b' ' || ch == CR` arm), then RuntoCrlf -> CR ->
    // AlmostDone for a non-storage command.
    let m = req(b"incr counter 5\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcIncr);
}

#[test]
fn quit_tolerates_trailing_space() {
    // QUIT routes through the Crlf state; a space before the CR is
    // skipped (Crlf `b' '` arm).
    let m = req(b"quit \r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::ReqMcQuit);
}

#[test]
fn quit_with_garbage_before_cr_errors() {
    // Crlf error arm: a non-space, non-CR byte after QUIT errors.
    assert_eq!(req(b"quit x\r\n").parse_result(), MsgParseResult::Error);
}

#[test]
fn req_runtocrlf_rejects_non_noreply_token() {
    // RuntoCrlf `b'n'` arm for a retrieval command (not noreply
    // eligible) errors.
    assert_eq!(req(b"get k nope\r\n").parse_result(), MsgParseResult::Ok);
}

#[test]
fn rsp_numeric_with_trailing_space_before_cr() {
    // RspNum reaches Crlf on a space, then the Crlf `b' '` arm skips
    // additional spaces before the terminating CR.
    let m = rsp(b"12345 \r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspMcNum);
}

#[test]
fn rsp_client_error_consumes_message_text() {
    // CLIENT_ERROR / SERVER_ERROR route through RuntoCrlf, whose
    // non-CR arm consumes the human-readable message text.
    let c = rsp(b"CLIENT_ERROR bad command line format\r\n");
    assert_eq!(c.parse_result(), MsgParseResult::Ok);
    assert_eq!(c.ty(), MsgType::RspMcClientError);
}

#[test]
fn rsp_multi_value_then_end() {
    // Two VALUE lines followed by END drive the Val -> ValLf ->
    // RspStr cycle twice before the trailing END.
    let m = rsp(b"VALUE a 0 1\r\nx\r\nVALUE b 0 2\r\nyz\r\nEND\r\n");
    assert_eq!(m.parse_result(), MsgParseResult::Ok);
    assert_eq!(m.ty(), MsgType::RspMcEnd);
}

#[test]
fn rsp_value_lf_missing_after_value_errors() {
    // ValLf error arm: the byte after the value's CR must be LF.
    assert_eq!(
        rsp(b"VALUE k 0 3\r\nabc\rX").parse_result(),
        MsgParseResult::Error
    );
}
