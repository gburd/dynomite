//! Branch coverage for the Memcached response-shaping helpers:
//! `multikey`, `verify`, and `fragment` edge arms.
//!
//! These modules hold the data-shape side of multi-key request
//! handling. The mbuf-level wiring lives in the connection FSM and is
//! covered by the conformance suite; here we exercise every reachable
//! arm of the pure helpers offline.

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{KeyPos, Msg, MsgType};
use dynomite::proto::memcache::fragment::FragmentError;
use dynomite::proto::memcache::verify::VerifyError;
use dynomite::proto::memcache::{
    memcache_fragment, memcache_is_multikey_request, memcache_verify_request, FragmentDispatcher,
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
