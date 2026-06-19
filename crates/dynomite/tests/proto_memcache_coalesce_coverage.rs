//! Branch coverage for the Memcached response-shaping helpers:
//! `multikey`, `coalesce` (pre/post), and `verify`.
//!
//! These modules hold the data-shape side of multi-key request
//! handling. The mbuf-level wiring lives in the connection FSM and is
//! covered by the conformance suite; here we exercise every reachable
//! arm of the pure helpers offline.

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{DynErrorCode, KeyPos, Msg, MsgType};
use dynomite::proto::memcache::fragment::FragmentError;
use dynomite::proto::memcache::verify::VerifyError;
use dynomite::proto::memcache::{
    memcache_fragment, memcache_is_multikey_request, memcache_post_coalesce, memcache_pre_coalesce,
    memcache_verify_request, FragmentDispatcher,
};

// -------------------------------------------------------------
// multikey: the classifier is a constant false for every type.
// -------------------------------------------------------------

#[test]
fn multikey_is_always_false() {
    // memcache_is_multikey_request returns false for read, write,
    // and unknown types alike; multi-key handling flows through the
    // fragment vector instead.
    for ty in [
        MsgType::ReqMcGet,
        MsgType::ReqMcGets,
        MsgType::ReqMcSet,
        MsgType::ReqMcDelete,
        MsgType::Unknown,
    ] {
        assert!(!memcache_is_multikey_request(ty));
    }
}

// -------------------------------------------------------------
// pre_coalesce: the four reachable arms.
// -------------------------------------------------------------

#[test]
fn pre_coalesce_ignores_requests() {
    // A request short-circuits at the is_request guard with no
    // mutation.
    let mut req = Msg::new(0, MsgType::ReqMcGet, true);
    req.set_frag_id(5);
    memcache_pre_coalesce(&mut req);
    assert!(!req.flags().is_error);
}

#[test]
fn pre_coalesce_ignores_zero_frag_id() {
    // A non-fragmented response (frag_id == 0) returns early.
    let mut rsp = Msg::new(0, MsgType::RspMcEnd, false);
    assert_eq!(rsp.frag_id(), 0);
    memcache_pre_coalesce(&mut rsp);
    assert!(!rsp.flags().is_error);
}

#[test]
fn pre_coalesce_value_and_end_are_clean() {
    // VALUE and END shard responses on a fragmented request are the
    // expected shapes: no error is flagged (the END-drop is the
    // FSM's job).
    for ty in [MsgType::RspMcValue, MsgType::RspMcEnd] {
        let mut rsp = Msg::new(0, ty, false);
        rsp.set_frag_id(7);
        memcache_pre_coalesce(&mut rsp);
        assert!(!rsp.flags().is_error, "type {ty:?}");
    }
}

#[test]
fn pre_coalesce_unexpected_shape_flags_error() {
    // Any other response type on a fragmented request is a protocol
    // error: the message is flagged with BadFormat.
    let mut rsp = Msg::new(0, MsgType::RspMcStored, false);
    rsp.set_frag_id(7);
    memcache_pre_coalesce(&mut rsp);
    assert!(rsp.flags().is_error);
    assert_eq!(rsp.dyn_error_code(), DynErrorCode::BadFormat);
}

// -------------------------------------------------------------
// post_coalesce: the three reachable arms.
// -------------------------------------------------------------

#[test]
fn post_coalesce_ignores_responses() {
    // A response short-circuits at the !is_request guard.
    let mut rsp = Msg::new(0, MsgType::RspMcEnd, false);
    memcache_post_coalesce(&mut rsp);
    assert!(!rsp.flags().done);
}

#[test]
fn post_coalesce_clean_request_marks_done() {
    // A clean parent request is marked done after every shard reply.
    let mut req = Msg::new(0, MsgType::ReqMcGet, true);
    memcache_post_coalesce(&mut req);
    assert!(req.flags().done);
}

#[test]
fn post_coalesce_errored_request_stays_not_done() {
    // An errored parent request is left to the connection-coupled
    // path; it is not marked done here.
    let mut req = Msg::new(0, MsgType::ReqMcGet, true);
    req.set_is_error(true);
    memcache_post_coalesce(&mut req);
    assert!(!req.flags().done);
}

#[test]
fn post_coalesce_ferror_request_stays_not_done() {
    // The is_ferror arm of the error guard also skips set_done.
    let mut req = Msg::new(0, MsgType::ReqMcGet, true);
    req.flags_mut().is_ferror = true;
    memcache_post_coalesce(&mut req);
    assert!(!req.flags().done);
}

// -------------------------------------------------------------
// verify: always succeeds; the error enum exists for parity.
// -------------------------------------------------------------

#[test]
fn verify_request_always_ok() {
    let m = Msg::new(0, MsgType::ReqMcSet, true);
    assert!(memcache_verify_request(&m).is_ok());
}

#[test]
fn verify_error_variant_is_constructible() {
    // The reserved variant is exported for parity with the RESP
    // surface; exercise its Debug/Display/PartialEq surface.
    let e = VerifyError::Reserved;
    assert_eq!(e, VerifyError::Reserved);
    assert!(!format!("{e}").is_empty());
    assert!(!format!("{e:?}").is_empty());
}

// -------------------------------------------------------------
// fragment: edge arms not hit by the happy-path partition test.
// -------------------------------------------------------------

struct OddEven;
impl FragmentDispatcher for OddEven {
    fn shard_for(&self, key: &[u8]) -> u32 {
        u32::from(*key.first().unwrap_or(&0)) % 2
    }
    fn shard_count(&self) -> u32 {
        2
    }
}

#[test]
fn fragment_non_retrieval_returns_none() {
    // A non-retrieval request (e.g. SET) is left unfragmented: the
    // fragmenter returns Ok(None).
    let mut m = Msg::new(0, MsgType::ReqMcSet, true);
    m.push_key(KeyPos::without_tag(b"a".to_vec()));
    let pool = MbufPool::default();
    assert!(memcache_fragment(&mut m, &OddEven, &pool)
        .unwrap()
        .is_none());
}

#[test]
fn fragment_retrieval_without_keys_errors() {
    // A GET with no parsed keys hits the EmptyKeys guard.
    let mut m = Msg::new(0, MsgType::ReqMcGet, true);
    let pool = MbufPool::default();
    assert!(matches!(
        memcache_fragment(&mut m, &OddEven, &pool),
        Err(FragmentError::EmptyKeys),
    ));
}

#[test]
fn fragment_grows_bucket_when_shard_count_underreports() {
    // A dispatcher that returns a shard index beyond the count it
    // advertised drives the `idx >= bucket.len()` resize path.
    struct UnderCount;
    impl FragmentDispatcher for UnderCount {
        fn shard_for(&self, key: &[u8]) -> u32 {
            // Map keys to shard 0 or shard 3 (beyond shard_count).
            u32::from(*key.first().unwrap_or(&0) % 2) * 3
        }
        fn shard_count(&self) -> u32 {
            1
        }
    }
    let mut m = Msg::new(0, MsgType::ReqMcGet, true);
    m.push_key(KeyPos::without_tag(b"a".to_vec())); // 97 % 2 == 1 -> shard 3
    m.push_key(KeyPos::without_tag(b"b".to_vec())); // 98 % 2 == 0 -> shard 0
    let pool = MbufPool::default();
    let outcome = memcache_fragment(&mut m, &UnderCount, &pool)
        .unwrap()
        .unwrap();
    assert_eq!(outcome.fragments.len(), 2);
    assert_eq!(outcome.shard_for_key, vec![3, 0]);
}
