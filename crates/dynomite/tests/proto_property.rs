//! Property tests for the protocol core (`proto::redis`,
//! `proto::memcache`, `proto::dnode`).
//!
//! Invariants exercised:
//! * Parser totality: the RESP request/response parsers, the
//!   memcache request/response parsers, and the DNODE streaming
//!   parser never panic on arbitrary bytes and always return a
//!   well-formed result (Ok / Again / Error, never a panic or a
//!   partial-but-claimed-complete state).
//! * Classification consistency: `classify` is deterministic and
//!   total over the modelled `MsgType` space, and `lookup` is
//!   case-insensitive and self-consistent with `classify`.
//! * Fragment round-trip: fragmenting a multi-key MGET request and
//!   re-parsing each fragment reconstructs exactly the input keys
//!   (no key dropped, none duplicated).
//! * DNODE header round-trip across every message type, including
//!   the recently added XA two-phase-commit variants.

#![allow(clippy::too_many_lines)]

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{KeyPos, Msg, MsgParseResult, MsgType};
use dynomite::proto::dnode::{dmsg_write, Dmsg, DmsgType, DnodeParser, ParseStep};
use dynomite::proto::memcache::{memcache_parse_req, memcache_parse_rsp};
use dynomite::proto::redis::commands::{classify, lookup};
use dynomite::proto::redis::{
    redis_fragment, redis_parse_req, redis_parse_rsp, FragmentDispatcher,
};

use hegel::generators as gs;
use hegel::TestCase;

// -------------------------------------------------------------
// Parser totality. Each parser, fed arbitrary bytes, must return
// one of the three documented results and must not leave the
// message in a "complete but typeless" state when it claims Ok.
// -------------------------------------------------------------

fn assert_result_well_formed(m: &Msg, r: MsgParseResult) {
    // A clean parse must have resolved a concrete type. Again /
    // Error and the dispatcher-side discriminators (Repair /
    // Fragment / Noop / DynoConfig / OomError) are all valid
    // terminal results; the parser never panics reaching any of
    // them.
    if r == MsgParseResult::Ok {
        assert_ne!(m.ty(), MsgType::Unknown, "Ok parse left type Unknown");
    }
}

#[hegel::test(test_cases = 512)]
fn redis_req_parser_is_total(tc: TestCase) {
    // The RESP request parser never panics and always reports a
    // well-formed result for any byte sequence.
    let input = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(300));
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let r = redis_parse_req(&mut m, &input);
    assert_result_well_formed(&m, r);
}

#[hegel::test(test_cases = 512)]
fn redis_rsp_parser_is_total(tc: TestCase) {
    // The RESP response parser never panics and always reports a
    // well-formed result for any byte sequence.
    let input = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(300));
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let r = redis_parse_rsp(&mut m, &input);
    assert_result_well_formed(&m, r);
}

#[hegel::test(test_cases = 512)]
fn redis_req_parser_is_total_on_structured_garbage(tc: TestCase) {
    // Bias the generator toward RESP framing bytes so the parser
    // walks deep into its state machine before rejecting.
    let alphabet = b"*$\r\n0123456789GETSDLMabc \x00";
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(120));
    let mut input = Vec::with_capacity(len);
    for _ in 0..len {
        let i = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(alphabet.len() - 1),
        );
        input.push(alphabet[i]);
    }
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let r = redis_parse_req(&mut m, &input);
    assert_result_well_formed(&m, r);
}

#[hegel::test(test_cases = 512)]
fn redis_rsp_parser_is_total_on_structured_garbage(tc: TestCase) {
    let alphabet = b"*$+-:\r\n0123456789abc \x00";
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(120));
    let mut input = Vec::with_capacity(len);
    for _ in 0..len {
        let i = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(alphabet.len() - 1),
        );
        input.push(alphabet[i]);
    }
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let r = redis_parse_rsp(&mut m, &input);
    assert_result_well_formed(&m, r);
}

#[hegel::test(test_cases = 512)]
fn memcache_req_parser_is_total(tc: TestCase) {
    let input = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(300));
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let _ = memcache_parse_req(&mut m, &input);
}

#[hegel::test(test_cases = 512)]
fn memcache_req_parser_is_total_on_structured_garbage(tc: TestCase) {
    // Bias toward memcache verbs and the space/CRLF framing.
    let alphabet = b"get set add cas delete incr decr quit \r\n0123456789key";
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(120));
    let mut input = Vec::with_capacity(len);
    for _ in 0..len {
        let i = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(alphabet.len() - 1),
        );
        input.push(alphabet[i]);
    }
    let mut m = Msg::new(0, MsgType::Unknown, true);
    let _ = memcache_parse_req(&mut m, &input);
}

#[hegel::test(test_cases = 512)]
fn memcache_rsp_parser_is_total(tc: TestCase) {
    let input = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(300));
    let mut m = Msg::new(0, MsgType::Unknown, false);
    let _ = memcache_parse_rsp(&mut m, &input);
}

#[hegel::test(test_cases = 512)]
fn dnode_parser_is_total(tc: TestCase) {
    // The DNODE streaming parser must never panic on arbitrary
    // bytes; it returns NeedMore / HeaderDone / Error and never
    // consumes past the input.
    let input = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(300));
    let mut parser = DnodeParser::new();
    match parser.step(&input) {
        ParseStep::NeedMore { consumed }
        | ParseStep::Error { consumed }
        | ParseStep::HeaderDone { consumed } => {
            assert!(consumed <= input.len());
        }
    }
}

#[hegel::test(test_cases = 512)]
fn dnode_parser_is_total_on_structured_garbage(tc: TestCase) {
    // Bias toward the DNODE header alphabet so the parser walks the
    // magic / id / type / bitfield / version / payload states.
    let alphabet = b"$2014 0123456789 dac*\r\n";
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(120));
    let mut input = Vec::with_capacity(len);
    for _ in 0..len {
        let i = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(alphabet.len() - 1),
        );
        input.push(alphabet[i]);
    }
    let mut parser = DnodeParser::new();
    let _ = parser.step(&input);
}

// -------------------------------------------------------------
// Classification consistency.
// -------------------------------------------------------------

/// The full keyword catalog (kept in step with commands.rs). Each
/// keyword resolves to a non-Unknown type and a total classify.
const KEYWORDS: &[&[u8]] = &[
    b"get",
    b"set",
    b"del",
    b"mget",
    b"mset",
    b"exists",
    b"expire",
    b"incr",
    b"decr",
    b"append",
    b"hget",
    b"hset",
    b"hdel",
    b"hmget",
    b"hmset",
    b"lpush",
    b"rpush",
    b"lpop",
    b"rpop",
    b"sadd",
    b"srem",
    b"smembers",
    b"zadd",
    b"zrem",
    b"zrange",
    b"setex",
    b"linsert",
    b"eval",
    b"evalsha",
    b"ping",
    b"quit",
    b"info",
    b"scan",
    b"script",
    b"ft.create",
    b"ft.search",
    b"ft.list",
];

#[hegel::test(test_cases = 256)]
fn classify_is_deterministic_over_catalog(tc: TestCase) {
    // For a randomly chosen catalog keyword, classify(lookup(kw))
    // gives the same answer every time it is called, in upper and
    // lower case (lookup is case-insensitive; classify is pure).
    let i = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(KEYWORDS.len() - 1),
    );
    let kw = KEYWORDS[i];
    let (ty_lower, _) = lookup(kw).expect("catalog keyword resolves");
    let upper: Vec<u8> = kw.iter().map(u8::to_ascii_uppercase).collect();
    let (ty_upper, _) = lookup(&upper).expect("uppercase catalog keyword resolves");
    assert_eq!(ty_lower, ty_upper, "case sensitivity for {kw:?}");
    // classify is a pure function: repeated calls agree.
    assert_eq!(classify(ty_lower), classify(ty_lower));
    assert_eq!(classify(ty_lower), classify(ty_upper));
}

#[hegel::test(test_cases = 256)]
fn classify_is_total_over_arbitrary_keyword_bytes(tc: TestCase) {
    // For an arbitrary keyword byte string, lookup either returns
    // None or a type that classify handles without panicking.
    let kw = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(40));
    if let Some((ty, _)) = lookup(&kw) {
        let _ = classify(ty);
    }
}

// -------------------------------------------------------------
// Fragment round-trip: a multi-key MGET split across shards and
// re-parsed reconstructs the exact input key set.
// -------------------------------------------------------------

/// Distribute keys across `n` shards by the first key byte.
struct ByFirstByte(u32);
impl FragmentDispatcher for ByFirstByte {
    fn shard_for(&self, key: &[u8]) -> u32 {
        u32::from(*key.first().unwrap_or(&0)) % self.0.max(1)
    }
    fn shard_count(&self) -> u32 {
        self.0
    }
}

#[hegel::test(test_cases = 256)]
fn mget_fragment_round_trip_preserves_keys(tc: TestCase) {
    // Build an MGET over 2..=8 distinct keys, fragment it across
    // 1..=4 shards, then re-parse every fragment and union the
    // recovered keys. The union must equal the input key set.
    let n_keys = tc.draw(gs::integers::<usize>().min_value(2).max_value(8));
    let n_shards = tc.draw(gs::integers::<u32>().min_value(1).max_value(4));

    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n_keys);
    for k in 0..n_keys {
        // Distinct keys so the union comparison is unambiguous.
        keys.push(format!("key{k:04}").into_bytes());
    }

    let mut req = Msg::new(0, MsgType::ReqRedisMget, true);
    for k in &keys {
        req.push_key(KeyPos::without_tag(k.clone()));
    }
    req.set_ntokens(1 + u32::try_from(n_keys).unwrap());

    let pool = MbufPool::default();
    let outcome = redis_fragment(&mut req, &ByFirstByte(n_shards), &[], &pool)
        .expect("fragment ok")
        .expect("multi-key request fragments");

    // Re-parse each fragment's wire bytes and collect its keys.
    let mut recovered: Vec<Vec<u8>> = Vec::new();
    for frag in &outcome.fragments {
        let bytes: Vec<u8> = frag
            .mbufs()
            .iter()
            .flat_map(|mb| mb.readable().to_vec())
            .collect();
        let mut parsed = Msg::new(0, MsgType::Unknown, true);
        let r = redis_parse_req(&mut parsed, &bytes);
        assert_eq!(r, MsgParseResult::Ok, "fragment re-parse failed: {bytes:?}");
        assert_eq!(parsed.ty(), MsgType::ReqRedisMget);
        for kp in parsed.keys() {
            recovered.push(kp.key().to_vec());
        }
    }

    // Each fragment also carries its keys directly; the union of
    // re-parsed keys must equal the input set.
    recovered.sort();
    let mut expected = keys.clone();
    expected.sort();
    assert_eq!(
        recovered, expected,
        "fragment round-trip lost or duplicated keys"
    );
    // Every input key was assigned a shard.
    assert_eq!(outcome.shard_for_key.len(), n_keys);
}

// -------------------------------------------------------------
// DNODE header round-trip across every message type, including the
// XA two-phase-commit variants.
// -------------------------------------------------------------

const DMSG_TYPES: &[DmsgType] = &[
    DmsgType::Debug,
    DmsgType::Req,
    DmsgType::ReqForward,
    DmsgType::Res,
    DmsgType::CryptoHandshake,
    DmsgType::GossipSyn,
    DmsgType::GossipSynReply,
    DmsgType::GossipAck,
    DmsgType::GossipDigestSyn,
    DmsgType::GossipDigestAck,
    DmsgType::GossipDigestAck2,
    DmsgType::GossipShutdown,
    DmsgType::HandoffChunk,
    DmsgType::FtSearchReq,
    DmsgType::FtSearchRep,
    DmsgType::XaPrepare,
    DmsgType::XaVote,
    DmsgType::XaCommit,
    DmsgType::XaRollback,
    DmsgType::XaAck,
];

#[hegel::test(test_cases = 256)]
fn dnode_header_round_trips_every_type(tc: TestCase) {
    // Encode a header for a randomly chosen message type (covering
    // the XA variants) and assert it parses back to the same fields.
    let ti = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(DMSG_TYPES.len() - 1),
    );
    let ty = DMSG_TYPES[ti];
    let id = tc.draw(gs::integers::<u64>());
    let flags = tc.draw(gs::integers::<u8>().min_value(0).max_value(15));
    let same_dc = tc.draw(gs::booleans());
    let plen = tc.draw(gs::integers::<u32>().min_value(0).max_value(1_000_000));

    let pool = MbufPool::default();
    let mut buf = pool.get();
    dmsg_write(&mut buf, id, ty, flags, same_dc, None, plen).expect("encode succeeds");
    let bytes = buf.readable().to_vec();

    let mut parser = DnodeParser::new();
    match parser.step(&bytes) {
        ParseStep::HeaderDone { consumed } => assert_eq!(consumed, bytes.len()),
        other => panic!("unexpected {other:?} for {ty:?}"),
    }
    let d = parser.take_dmsg();
    assert_eq!(d.id, id);
    assert_eq!(d.ty, ty);
    assert_eq!(d.flags, flags & 0xF);
    assert_eq!(d.same_dc, same_dc);
    assert_eq!(d.plen, plen);
}

#[test]
fn dnode_type_u8_round_trips_every_variant() {
    // from_u8 . as_u8 is the identity on every modelled variant,
    // and from_u8 rejects an out-of-range discriminant.
    for ty in DMSG_TYPES {
        assert_eq!(DmsgType::from_u8(ty.as_u8()), Some(*ty));
    }
    assert_eq!(DmsgType::from_u8(0), Some(DmsgType::Unknown));
    assert_eq!(DmsgType::from_u8(99), None);
    // A default Dmsg is the Unknown type.
    assert_eq!(Dmsg::new().ty, DmsgType::Unknown);
}
