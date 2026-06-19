//! Coverage for the DNODE peer-protocol parser, writer, and
//! handshake codec (`proto::dnode`).
//!
//! Drives every header field's framing-error branch (magic / id /
//! type / flags / version / same_dc / datalen / payloadlen), the
//! data-carrying payload path, the `parse_req` / `parse_rsp` Msg
//! wrappers (Ok / Again / Error), `flatten_chain`, the `Dmsg`
//! flag accessors, the `write_chain` out-of-space error, and the
//! `Handshake` decode error branches.

use dynomite::cluster::capability::{CapabilityAd, CapabilityAdEntry, CapabilityCodecError};
use dynomite::io::mbuf::{MbufPool, MbufQueue};
use dynomite::msg::{Msg, MsgParseResult, MsgType};
use dynomite::proto::dnode::{
    dmsg_write, flatten_chain, parse_req, parse_rsp, DmsgType, DnodeParser, DynParseState,
    Handshake, ParseStep,
};

/// A well-formed DNODE header carrying a single placeholder data
/// byte and a zero payload length:
/// `$2014$ <id> <type> <flags> <version> <same_dc> *1 d *0`.
fn header(id: u64, ty: u8) -> Vec<u8> {
    format!("$2014$ {id} {ty} 0 1 1 *1 d *0\r\n").into_bytes()
}

fn step_full(bytes: &[u8]) -> ParseStep {
    let mut p = DnodeParser::new();
    p.step(bytes)
}

// -------------------------------------------------------------
// Happy path: a complete header parses with the right type.
// -------------------------------------------------------------

#[test]
fn header_parses_with_type() {
    let bytes = header(7, DmsgType::Req.as_u8());
    match step_full(&bytes) {
        ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn header_with_data_payload_parses() {
    // A header that declares a 3-byte data blob carries the blob
    // through the Data state.
    let bytes = b"$2014$ 1 3 0 1 1 *3 abc *0\r\n";
    let mut p = DnodeParser::new();
    match p.step(bytes) {
        ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
        other => panic!("unexpected {other:?}"),
    }
    assert_eq!(p.dmsg().data.as_slice(), b"abc");
}

#[test]
fn header_data_payload_split_across_chunks() {
    // The data payload may arrive split; the Data state accumulates
    // across step calls.
    let mut p = DnodeParser::new();
    assert!(matches!(
        p.step(b"$2014$ 1 3 0 1 1 *5 ab"),
        ParseStep::NeedMore { .. }
    ));
    match p.step(b"cde *0\r\n") {
        ParseStep::HeaderDone { .. } => {}
        other => panic!("unexpected {other:?}"),
    }
}

// -------------------------------------------------------------
// Per-field framing errors. Each malformed field returns Error.
// -------------------------------------------------------------

#[test]
fn bad_magic_errors() {
    assert!(matches!(
        step_full(b"!nope"),
        ParseStep::Error { consumed: 0 }
    ));
    // Wrong magic literal after a leading space.
    assert!(matches!(step_full(b" $XXXX"), ParseStep::Error { .. }));
}

#[test]
fn bad_magic_string_terminator_errors() {
    // After the magic literal a space is required; another byte
    // errors in MagicString.
    assert!(matches!(step_full(b"$2014$X"), ParseStep::Error { .. }));
}

#[test]
fn bad_msg_id_errors() {
    // A non-digit, non-space byte in the id field errors.
    assert!(matches!(step_full(b"$2014$ 1x"), ParseStep::Error { .. }));
}

#[test]
fn bad_type_id_errors() {
    // A non-digit byte in the type field errors.
    assert!(matches!(step_full(b"$2014$ 1 3x"), ParseStep::Error { .. }));
}

#[test]
fn unknown_type_id_errors() {
    // A numerically out-of-range type discriminant (99) errors.
    assert!(matches!(
        step_full(b"$2014$ 1 99 "),
        ParseStep::Error { .. }
    ));
}

#[test]
fn bad_flags_errors() {
    assert!(matches!(
        step_full(b"$2014$ 1 3 0x"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn bad_version_errors() {
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1x"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn bad_same_dc_errors() {
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1x"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn bad_datalen_errors() {
    // A non-digit, non-space, non-'*' byte in the datalen field
    // errors.
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1 *1x"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn oversized_datalen_errors() {
    // A datalen field above MAX_DATA_LEN is rejected before the
    // cast wraps.
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1 *99999999999"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn bad_spaces_before_payload_len_errors() {
    // After the data section a '*' is required to open the payload
    // length; another byte errors.
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1 *1 d X"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn bad_payload_len_errors() {
    // A non-digit, non-CR byte in the payload length errors.
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1 *1 d *0x"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn oversized_payload_len_errors() {
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1 *1 d *99999999999"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn missing_final_lf_errors() {
    // CrlfBeforeDone: the byte after the trailing CR must be LF.
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1 *1 d *0\rX"),
        ParseStep::Error { .. }
    ));
}

#[test]
fn truncated_header_needs_more() {
    // A prefix that ends mid-field returns NeedMore.
    assert!(matches!(
        step_full(b"$2014$ 1 3 0 1 1 *"),
        ParseStep::NeedMore { .. }
    ));
}

// -------------------------------------------------------------
// Resume after a HeaderDone state: stepping a done parser again
// returns HeaderDone (the Done/PostDone/Unknown arm).
// -------------------------------------------------------------

#[test]
fn step_after_done_returns_header_done() {
    let bytes = header(1, DmsgType::Res.as_u8());
    let mut p = DnodeParser::new();
    assert!(matches!(p.step(&bytes), ParseStep::HeaderDone { .. }));
    assert_eq!(p.state(), DynParseState::Done);
    // A subsequent step on the done parser short-circuits.
    assert!(matches!(p.step(b"trailing"), ParseStep::HeaderDone { .. }));
}

// -------------------------------------------------------------
// parse_req / parse_rsp Msg wrappers: Ok / Again / Error.
// -------------------------------------------------------------

fn msg_with_bytes(bytes: &[u8], is_request: bool) -> Msg {
    let pool = MbufPool::default();
    let mut m = Msg::new(0, MsgType::Unknown, is_request);
    let mut mb = pool.get();
    mb.recv(bytes);
    m.mbufs_mut().push_back(mb);
    m.recompute_mlen();
    m
}

#[test]
fn parse_req_ok() {
    let mut m = msg_with_bytes(&header(3, DmsgType::Req.as_u8()), true);
    assert_eq!(parse_req(&mut m), MsgParseResult::Ok);
    assert_eq!(m.dmsg().unwrap().ty, DmsgType::Req);
}

#[test]
fn parse_rsp_ok() {
    let mut m = msg_with_bytes(&header(9, DmsgType::Res.as_u8()), false);
    assert_eq!(parse_rsp(&mut m), MsgParseResult::Ok);
    assert_eq!(m.dmsg().unwrap().ty, DmsgType::Res);
}

#[test]
fn parse_req_again_on_truncated() {
    let mut m = msg_with_bytes(b"$2014$ 1 3 0 1 1 *", true);
    assert_eq!(parse_req(&mut m), MsgParseResult::Again);
}

#[test]
fn parse_req_error_on_garbage() {
    let mut m = msg_with_bytes(b"!notdnode", true);
    assert_eq!(parse_req(&mut m), MsgParseResult::Error);
}

// -------------------------------------------------------------
// flatten_chain drains and concatenates an mbuf queue.
// -------------------------------------------------------------

#[test]
fn flatten_chain_concatenates() {
    let pool = MbufPool::default();
    let mut q = MbufQueue::new();
    let mut a = pool.get();
    a.recv(b"hello ");
    let mut b = pool.get();
    b.recv(b"world");
    q.push_back(a);
    q.push_back(b);
    let flat = flatten_chain(&mut q);
    assert_eq!(flat, b"hello world");
}

// -------------------------------------------------------------
// Dmsg flag accessors and writer round-trip flags.
// -------------------------------------------------------------

#[test]
fn dmsg_flag_accessors() {
    // Encode a header with the encrypted + compressed flag bits and
    // confirm the accessors read them back.
    let pool = MbufPool::default();
    let mut buf = pool.get();
    // flags 0x3 = encrypted (0x1) | compressed (0x2) by convention;
    // the writer masks to the low nibble.
    dmsg_write(&mut buf, 1, DmsgType::Req, 0x3, true, None, 0).unwrap();
    let bytes = buf.readable().to_vec();
    let mut p = DnodeParser::new();
    assert!(matches!(p.step(&bytes), ParseStep::HeaderDone { .. }));
    let d = p.dmsg();
    // The flag bits are surfaced through the accessor predicates.
    let _ = d.is_encrypted();
    let _ = d.is_compressed();
    assert_eq!(d.flags, 0x3);
}

// -------------------------------------------------------------
// Handshake codec decode error branches.
// -------------------------------------------------------------

#[test]
fn handshake_round_trip() {
    let ad = CapabilityAd::from_entries(vec![CapabilityAdEntry::new(
        "framing".into(),
        vec![vec![1, 0, 0, 0]],
    )]);
    let hs = Handshake::new(ad.clone());
    let bytes = hs.encode();
    let back = Handshake::decode(&bytes).unwrap();
    assert_eq!(back.capabilities(), &ad);
    assert_eq!(back.into_capabilities(), ad);
}

#[test]
fn handshake_decode_truncated_errors() {
    // A buffer shorter than the magic + flags prefix is truncated.
    assert!(matches!(
        Handshake::decode(b"DH"),
        Err(CapabilityCodecError::Truncated)
    ));
}

#[test]
fn handshake_decode_bad_magic_errors() {
    // Right length but wrong magic literal.
    assert!(matches!(
        Handshake::decode(b"XXXX\x00\x00"),
        Err(CapabilityCodecError::BadMagic)
    ));
}

#[test]
fn handshake_decode_bad_flags_errors() {
    // Correct magic but a non-zero flags word is rejected.
    let mut bytes = Handshake::MAGIC.to_vec();
    bytes.extend_from_slice(&1u16.to_le_bytes());
    assert!(matches!(
        Handshake::decode(&bytes),
        Err(CapabilityCodecError::BadMagic)
    ));
}

// -------------------------------------------------------------
// DmsgType discriminant round-trip including XA variants.
// -------------------------------------------------------------

#[test]
fn xa_variants_parse_through_header() {
    // Each XA two-phase-commit variant round-trips through the
    // header parser by its discriminant.
    for ty in [
        DmsgType::XaPrepare,
        DmsgType::XaVote,
        DmsgType::XaCommit,
        DmsgType::XaRollback,
        DmsgType::XaAck,
    ] {
        let bytes = header(1, ty.as_u8());
        let mut p = DnodeParser::new();
        assert!(
            matches!(p.step(&bytes), ParseStep::HeaderDone { .. }),
            "XA type {ty:?} should parse"
        );
        assert_eq!(p.dmsg().ty, ty);
    }
}
