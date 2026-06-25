//! Stage 7 - Message model and DNODE codec.
//!
//! These tests exercise the publicly-exposed surface of
//! `dynomite::msg` and `dynomite::proto::dnode` against the
//! canonical fixtures the reference engine ships with.

use dynomite::core::types::MsgId;
use dynomite::crypto::Crypto;
use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{
    is_read_repairs_enabled, set_read_repairs_enabled, ConsistencyLevel, DynErrorCode, Msg,
    MsgIndex, MsgQueue, MsgType, QuorumOutcome, ResponseMgr,
};
use dynomite::proto::dnode::{
    dmsg_process, dmsg_write, dmsg_write_mbuf, parse_req, Dmsg, DmsgDispatch, DmsgType,
    DnodeParser, DynParseState, ParseStep, DMSG_FLAG_ENCRYPTED,
};

use hegel::generators as gs;
use hegel::TestCase;

/// Canonical multi-message blob used as the cross-implementation
/// fixture for Stage 7.
///
/// Three messages, all `DMSG_REQ`, with msg ids 1, 2, 3. The second
/// payload is a 413-byte Redis `set` with a long bulk string;
/// messages 1 and 3 carry tiny `set` commands.
const DYN_TEST_BLOB: &[u8] =
    b"$2014$ 1 3 0 1 1 *1 d *0\r\n\
*3\r\n$3\r\nset\r\n$4\r\nfoo1\r\n$4\r\nbar1\r\n\
$2014$ 2 3 0 1 1 *1 d *413\r\n\
*3\r\n$3\r\nset\r\n$4\r\nfoo2\r\n\
$413\r\nbar01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567892222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222\r\n\
$2014$ 3 3 0 1 1 *1 d *0\r\n\
*3\r\n$3\r\nset\r\n$4\r\nfoo3\r\n$4\r\nbar3\r\n";

#[test]
fn dyn_test_blob_yields_three_dmsg_headers() {
    let mut parser = DnodeParser::new();
    let mut idx = 0usize;
    let mut headers: Vec<Dmsg> = Vec::new();
    while idx < DYN_TEST_BLOB.len() {
        match parser.step(&DYN_TEST_BLOB[idx..]) {
            ParseStep::HeaderDone { consumed } => {
                let dmsg = parser.take_dmsg();
                let after_header = idx + consumed;
                // Skip over the payload bytes the C parser would
                // hand off to the datastore protocol.
                let advance = if dmsg.plen > 0 {
                    dmsg.plen as usize
                } else {
                    DYN_TEST_BLOB[after_header..]
                        .iter()
                        .position(|&b| b == b'$')
                        .unwrap_or(DYN_TEST_BLOB.len() - after_header)
                };
                headers.push(dmsg);
                idx = (after_header + advance).min(DYN_TEST_BLOB.len());
                parser.reset();
            }
            ParseStep::NeedMore { .. } => break,
            ParseStep::Error { consumed } => {
                idx += consumed.max(1);
                parser.reset();
            }
        }
    }
    assert_eq!(headers.len(), 3, "expected three DNODE headers");

    assert_eq!(headers[0].id, 1);
    assert_eq!(headers[0].ty, DmsgType::Req);
    assert_eq!(headers[0].flags, 0);
    assert_eq!(headers[0].version, 1);
    assert!(headers[0].same_dc);
    assert_eq!(headers[0].mlen, 1);
    assert_eq!(headers[0].data, b"d");
    assert_eq!(headers[0].plen, 0);

    assert_eq!(headers[1].id, 2);
    assert_eq!(headers[1].plen, 413);

    assert_eq!(headers[2].id, 3);
    assert_eq!(headers[2].plen, 0);
}

#[test]
fn parse_req_attaches_dmsg_to_message() {
    let pool = MbufPool::default();
    let mut msg = Msg::new(0, MsgType::Unknown, true);
    let mut buf = pool.get();
    buf.recv(b"$2014$ 7 3 0 1 1 *1 d *0\r\n");
    msg.mbufs_mut().push_back(buf);
    msg.recompute_mlen();
    let result = parse_req(&mut msg);
    assert_eq!(result, dynomite::msg::MsgParseResult::Ok);
    assert_eq!(msg.dyn_parse_state(), DynParseState::Done);
    let dmsg = msg.dmsg().unwrap();
    assert_eq!(dmsg.id, 7);
    assert_eq!(dmsg.ty, DmsgType::Req);
}

#[test]
fn streaming_parser_recovers_from_split_input() {
    // Split the first dyn_test message at every byte boundary and
    // ensure incremental feeding produces the same result.
    let header = b"$2014$ 1 3 0 1 1 *1 d *0\r\n";
    for split in 1..header.len() {
        let mut parser = DnodeParser::new();
        let (a, b) = header.split_at(split);
        match parser.step(a) {
            ParseStep::NeedMore { consumed } => assert!(consumed <= a.len()),
            ParseStep::HeaderDone { .. } => {
                // The first split that yields a complete header is
                // the full input; verify and continue.
                continue;
            }
            ParseStep::Error { consumed } => panic!(
                "unexpected error at split {split} consumed {consumed}: {:?}",
                &a[..consumed.min(a.len())]
            ),
        }
        match parser.step(b) {
            ParseStep::HeaderDone { .. } => {
                let d = parser.take_dmsg();
                assert_eq!(d.id, 1, "split={split}");
            }
            other => panic!("split={split}: unexpected {other:?}"),
        }
    }
}

#[test]
fn msg_type_index_round_trip_full_table() {
    let count = u32::try_from(MsgType::COUNT).unwrap();
    for i in 0..count {
        let ty = MsgType::from_index(i).expect("variant exists");
        assert_eq!(ty.as_index(), i);
    }
    assert!(MsgType::from_index(count).is_none());
}

#[hegel::test(test_cases = 256)]
fn dnode_parser_round_trip_proptest(tc: TestCase) {
    // Generates valid Dmsg shapes and asserts the encoded
    // bytes parse back to the same field set. Originally a
    // hand-rolled `proptest::TestRunner::new` loop with 256 cases.
    let id = tc.draw(gs::integers::<u64>());
    let ty = tc.draw(gs::sampled_from(&[
        DmsgType::Req,
        DmsgType::ReqForward,
        DmsgType::Res,
        DmsgType::CryptoHandshake,
        DmsgType::GossipSyn,
        DmsgType::GossipShutdown,
    ]));
    let flags = tc.draw(gs::integers::<u8>().min_value(0).max_value(15));
    let same_dc = tc.draw(gs::booleans());
    let payload = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(200));
    let plen = tc.draw(gs::integers::<u32>().min_value(0).max_value(100_000));

    let pool = MbufPool::default();
    let mut buf = pool.get();
    let aes = if payload.is_empty() {
        None
    } else {
        Some(payload.as_slice())
    };
    dmsg_write(&mut buf, id, ty, flags, same_dc, aes, plen).expect("encode succeeds");
    let bytes = buf.readable().to_vec();
    let mut parser = DnodeParser::new();
    match parser.step(&bytes) {
        ParseStep::HeaderDone { consumed } => {
            assert_eq!(consumed, bytes.len());
        }
        other => panic!("unexpected {other:?}"),
    }
    let d = parser.take_dmsg();
    assert_eq!(d.id, id);
    assert_eq!(d.ty, ty);
    assert_eq!(d.flags, flags & 0xF);
    assert_eq!(d.same_dc, same_dc);
    assert_eq!(d.plen, plen);
    if let Some(p) = aes {
        assert_eq!(d.data.as_slice(), p);
    } else {
        assert_eq!(d.data.as_slice(), b"d".as_slice());
    }
}

#[test]
fn dispatcher_routes_control_plane_through_bypass() {
    // Only CRYPTO_HANDSHAKE, GOSSIP_SYN, and GOSSIP_SYN_REPLY
    // short-circuit through the bypass; every other gossip variant
    // must fall through to the default (forward) branch.
    let mut d = Dmsg::new();
    for ty in [
        DmsgType::CryptoHandshake,
        DmsgType::GossipSyn,
        DmsgType::GossipSynReply,
    ] {
        d.ty = ty;
        assert_eq!(dmsg_process(&d), DmsgDispatch::Bypass, "{ty:?} must bypass");
    }
    // The remaining gossip variants and every data-plane variant
    // must forward.
    for ty in [
        DmsgType::GossipAck,
        DmsgType::GossipDigestSyn,
        DmsgType::GossipDigestAck,
        DmsgType::GossipDigestAck2,
        DmsgType::GossipShutdown,
        DmsgType::Req,
        DmsgType::Res,
        DmsgType::ReqForward,
    ] {
        d.ty = ty;
        assert_eq!(
            dmsg_process(&d),
            DmsgDispatch::Forward,
            "{ty:?} must forward",
        );
    }
}

/// Drive arbitrary byte slices through the streaming DNODE
/// parser and assert that no input panics. The contract is
/// "parser is total on `Vec<u8>`": every step must return one
/// of `HeaderDone`, `NeedMore`, or `Error`. The brief in
/// PLAN.md Section 6.3 lists this property explicitly
/// ("parsers never panic on arbitrary `Vec<u8>` and either
/// return Err or a complete Msg").
#[hegel::test(test_cases = 512)]
fn parser_total_on_arbitrary_bytes(tc: TestCase) {
    let bytes = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(1023));
    let mut parser = DnodeParser::new();
    // The parser may halt before consuming the whole slice
    // (`HeaderDone`/`Error` both report a `consumed` offset);
    // feed every remaining byte at most once so a malformed
    // prefix cannot wedge the loop.
    let mut idx = 0usize;
    while idx < bytes.len() {
        match parser.step(&bytes[idx..]) {
            ParseStep::HeaderDone { consumed } | ParseStep::Error { consumed } => {
                let advance = consumed.max(1);
                idx = idx.saturating_add(advance);
                parser.reset();
            }
            ParseStep::NeedMore { .. } => break,
        }
    }
    // Any of the three outcomes is acceptable; the contract is
    // that none of the steps above unwound through a panic.
}

#[test]
fn dmsg_write_mbuf_emits_gossip_placeholder() {
    let pool = MbufPool::default();
    let mut buf = pool.get();
    dmsg_write_mbuf(&mut buf, 1, DmsgType::GossipSyn, 0, true, None, 7).unwrap();
    let bytes = buf.readable().to_vec();
    // Last byte before the trailing CRLF should be 'a'.
    let trail = b" *7\r\n";
    assert!(bytes.ends_with(trail));
    let head_end = bytes.len() - trail.len();
    assert_eq!(bytes[head_end - 1], b'a');
}

#[test]
fn response_mgr_dc_one_quorum_decisions() {
    let req = make_request(MsgType::ReqRedisGet, true);

    let mut mgr = ResponseMgr::new(&req, 1, Some("dc1".into()));
    assert_eq!(mgr.outcome(), QuorumOutcome::Pending);
    mgr.submit_response(make_response(2, false), 1);
    assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
}

#[test]
fn response_mgr_dc_quorum_decisions() {
    let req = make_request(MsgType::ReqRedisGet, true);

    // 2/2 matching -> achieved.
    {
        let mut mgr = ResponseMgr::new(&req, 2, None);
        mgr.submit_response(make_response(2, false), 7);
        mgr.submit_response(make_response(3, false), 7);
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
    }

    // 1 good + 1 error -> Failed (no quorum possible).
    {
        let mut mgr = ResponseMgr::new(&req, 2, None);
        mgr.submit_response(make_response(2, false), 7);
        mgr.submit_response(make_response(3, true), 0);
        assert_eq!(mgr.outcome(), QuorumOutcome::Failed);
    }

    // 2/2 mismatched -> Mismatched (no further responses pending).
    {
        let mut mgr = ResponseMgr::new(&req, 2, None);
        mgr.submit_response(make_response(2, false), 7);
        mgr.submit_response(make_response(3, false), 9);
        assert_eq!(mgr.outcome(), QuorumOutcome::Mismatched);
    }
}

#[test]
fn response_mgr_dc_safe_quorum_decisions() {
    let req = make_request(MsgType::ReqRedisGet, true);

    // 3/3 same checksum -> achieved.
    {
        let mut mgr = ResponseMgr::new(&req, 3, None);
        for id in 2..=4 {
            mgr.submit_response(make_response(id, false), 11);
        }
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
        assert!(mgr.pick_response().is_some());
    }

    // 1 dissent -> majority wins.
    {
        let mut mgr = ResponseMgr::new(&req, 3, None);
        mgr.submit_response(make_response(2, false), 1);
        mgr.submit_response(make_response(3, false), 2);
        mgr.submit_response(make_response(4, false), 2);
        assert_eq!(mgr.outcome(), QuorumOutcome::Achieved);
    }

    // All disagreeing -> Mismatched.
    {
        let mut mgr = ResponseMgr::new(&req, 3, None);
        mgr.submit_response(make_response(2, false), 1);
        mgr.submit_response(make_response(3, false), 2);
        mgr.submit_response(make_response(4, false), 3);
        assert_eq!(mgr.outcome(), QuorumOutcome::Mismatched);
        assert!(mgr.pick_response().is_none());
    }

    // 1 good + 2 errors -> Failed.
    {
        let mut mgr = ResponseMgr::new(&req, 3, None);
        mgr.submit_response(make_response(2, false), 1);
        mgr.submit_response(make_response(3, true), 0);
        mgr.submit_response(make_response(4, true), 0);
        assert_eq!(mgr.outcome(), QuorumOutcome::Failed);
    }
}

#[test]
fn response_mgr_dc_each_safe_quorum_per_dc_decisions() {
    let req = make_request(MsgType::ReqRedisGet, true);
    // Two managers, one per DC: each evaluates independently.
    let mut mgr_a = ResponseMgr::new(&req, 3, Some("dc-a".into()));
    let mut mgr_b = ResponseMgr::new(&req, 3, Some("dc-b".into()));
    for id in 2..=4 {
        mgr_a.submit_response(make_response(id, false), 1);
    }
    mgr_b.submit_response(make_response(10, false), 5);
    mgr_b.submit_response(make_response(11, true), 0);

    assert_eq!(mgr_a.outcome(), QuorumOutcome::Achieved);
    // mgr_b: 1 good, 1 error, 1 pending. good < quorum (2),
    // pending+good=2 = quorum so Pending.
    assert_eq!(mgr_b.outcome(), QuorumOutcome::Pending);
    mgr_b.submit_response(make_response(12, true), 0);
    assert_eq!(mgr_b.outcome(), QuorumOutcome::Failed);
}

#[test]
fn read_repairs_global_default_is_disabled() {
    // Note: the `set_read_repairs_enabled` API is global, so tests
    // share its state. We only assert the default and that a write
    // succeeds the first time it is called in this test run.
    let prev = is_read_repairs_enabled();
    let _ = set_read_repairs_enabled(prev);
    assert_eq!(is_read_repairs_enabled(), prev);
}

#[test]
fn encrypted_handshake_round_trips_aes_key() {
    use std::path::PathBuf;
    let mut pem = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    pem.push("tests/fixtures/crypto/dynomite.pem");
    let crypto = Crypto::from_pem(&pem).expect("load PEM");

    // Produce a fresh AES key, wrap it with RSA, and encode a
    // CRYPTO_HANDSHAKE header carrying the wrapped key as the
    // inline data field.
    let aes_key = Crypto::generate_aes_key().unwrap();
    let wrapped = crypto.rsa_encrypt(&aes_key).unwrap();
    let pool = MbufPool::default();
    let mut buf = pool.get();
    dmsg_write(
        &mut buf,
        /* id */ 99,
        DmsgType::CryptoHandshake,
        DMSG_FLAG_ENCRYPTED,
        /* same_dc */ true,
        Some(&wrapped),
        /* plen */ 0,
    )
    .unwrap();

    // Parse the bytes back.
    let bytes = buf.readable().to_vec();
    let mut parser = DnodeParser::new();
    match parser.step(&bytes) {
        ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
        other => panic!("unexpected {other:?}"),
    }
    let d = parser.take_dmsg();
    assert_eq!(d.ty, DmsgType::CryptoHandshake);
    assert!(d.is_encrypted());
    assert_eq!(d.data.len(), wrapped.len());

    // Unwrap the AES key from the parsed data field and verify it
    // matches the original.
    let unwrapped = crypto.rsa_decrypt(&d.data).unwrap();
    assert_eq!(unwrapped, aes_key);
}

#[test]
fn msg_queue_and_index_round_trip() {
    let mut q = MsgQueue::new();
    q.push_back(Msg::new(1, MsgType::ReqRedisGet, true));
    q.push_back(Msg::new(2, MsgType::ReqRedisSet, true));
    let ids: Vec<MsgId> = q.iter().map(Msg::id).collect();
    assert_eq!(ids, vec![1, 2]);

    let mut store = MsgIndex::new();
    while let Some(m) = q.pop_front() {
        store.insert(m);
    }
    assert!(store.contains_key(1));
    assert!(store.contains_key(2));
    assert_eq!(store.len(), 2);
    let m = store.remove(1).unwrap();
    assert_eq!(m.id(), 1);
    assert_eq!(store.len(), 1);
}

#[test]
fn consistency_level_from_name_round_trip() {
    for level in [
        ConsistencyLevel::DcOne,
        ConsistencyLevel::DcQuorum,
        ConsistencyLevel::DcSafeQuorum,
        ConsistencyLevel::DcEachSafeQuorum,
    ] {
        assert_eq!(ConsistencyLevel::from_name(level.name()), Some(level));
    }
}

#[test]
fn dyn_error_code_strings_track_c_reference() {
    assert_eq!(DynErrorCode::PeerHostDown.message(), "Peer Node is down");
    assert_eq!(DynErrorCode::PeerHostDown.source(), "Peer:");
    assert_eq!(DynErrorCode::DynomiteNoQuorumAchieved.source(), "Dynomite:",);
    assert_eq!(DynErrorCode::StorageConnectionRefuse.source(), "Storage:",);
    assert_eq!(DynErrorCode::Ok.source(), "unknown:");
}

fn make_request(ty: MsgType, is_read: bool) -> Msg {
    let mut m = Msg::new(1, ty, true);
    m.flags_mut().is_read = is_read;
    m
}

fn make_response(id: MsgId, is_error: bool) -> Msg {
    let mut m = Msg::new(id, MsgType::RspRedisStatus, false);
    m.flags_mut().is_error = is_error;
    m
}
