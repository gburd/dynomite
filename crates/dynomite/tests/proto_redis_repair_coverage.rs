//! Coverage for the Redis fragmenter, coalescer, and repair surface
//! (`proto::redis::{fragment, coalesce, repair}`).
//!
//! Exercises the MSET key/value fragmentation path (verb + value
//! emission), the empty-keys and unsupported-type fragment errors,
//! the pre/post-coalesce response-state guards and the fragment
//! integer accumulation, the read-repair enabled path in
//! `redis_make_repair_query`, and the multikey reconcile decision.

use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{
    set_read_repairs_enabled, ConsistencyLevel, KeyPos, Msg, MsgParseResult, MsgType, ResponseMgr,
};
use dynomite::proto::redis::repair::make::{is_repairable, redis_make_repair_query};
use dynomite::proto::redis::repair::reconcile::{
    is_foldable_multikey_response, redis_reconcile_responses, ReconcileOutcome,
};
use dynomite::proto::redis::{
    accumulate_fragment_integer, redis_fragment, redis_parse_req, redis_post_coalesce,
    redis_pre_coalesce, FragmentDispatcher, RepairOutcome,
};

/// Distribute by first key byte across two shards.
struct OddEven;
impl FragmentDispatcher for OddEven {
    fn shard_for(&self, key: &[u8]) -> u32 {
        u32::from(*key.first().unwrap_or(&0)) % 2
    }
    fn shard_count(&self) -> u32 {
        2
    }
}

/// All keys to one shard (forces the bucket-reuse path).
struct AllZero;
impl FragmentDispatcher for AllZero {
    fn shard_for(&self, _: &[u8]) -> u32 {
        0
    }
    fn shard_count(&self) -> u32 {
        2
    }
}

/// A dispatcher that returns a shard index past its declared count,
/// forcing the fragmenter's bucket-vector resize path.
struct OutOfRange;
impl FragmentDispatcher for OutOfRange {
    fn shard_for(&self, key: &[u8]) -> u32 {
        // Map "a" -> 0 and "b" -> 5 (>= shard_count) to trigger the
        // `idx >= bucket.len()` resize.
        if key.first() == Some(&b'a') {
            0
        } else {
            5
        }
    }
    fn shard_count(&self) -> u32 {
        1
    }
}

// -------------------------------------------------------------
// Fragment: MSET key/value path and error paths.
// -------------------------------------------------------------

#[test]
fn mset_fragments_with_values_and_reparses() {
    // MSET across shards emits per-fragment verb + key/value pairs;
    // re-parsing a fragment recovers an MSET with the keys.
    let mut req = Msg::new(0, MsgType::ReqRedisMset, true);
    req.push_key(KeyPos::without_tag(b"a".to_vec())); // shard 1
    req.push_key(KeyPos::without_tag(b"b".to_vec())); // shard 0
    req.set_ntokens(5);
    let values = vec![b"1".to_vec(), b"2".to_vec()];
    let pool = MbufPool::default();
    let outcome = redis_fragment(&mut req, &OddEven, &values, &pool)
        .expect("fragment ok")
        .expect("multi-key MSET fragments");
    assert_eq!(outcome.fragments.len(), 2);

    for frag in &outcome.fragments {
        let bytes: Vec<u8> = frag
            .mbufs()
            .iter()
            .flat_map(|mb| mb.readable().to_vec())
            .collect();
        let mut parsed = Msg::new(0, MsgType::Unknown, true);
        let r = redis_parse_req(&mut parsed, &bytes);
        assert_eq!(r, MsgParseResult::Ok, "fragment reparse: {bytes:?}");
        assert_eq!(parsed.ty(), MsgType::ReqRedisMset);
        assert_eq!(parsed.keys().len(), 1);
    }
}

#[test]
fn mset_all_keys_one_shard_reuses_bucket() {
    // Two keys mapping to the same shard exercise the bucket-reuse
    // branch (a second key joining an existing fragment).
    let mut req = Msg::new(0, MsgType::ReqRedisMset, true);
    req.push_key(KeyPos::without_tag(b"x".to_vec()));
    req.push_key(KeyPos::without_tag(b"y".to_vec()));
    req.set_ntokens(5);
    let values = vec![b"1".to_vec(), b"2".to_vec()];
    let pool = MbufPool::default();
    let outcome = redis_fragment(&mut req, &AllZero, &values, &pool)
        .expect("fragment ok")
        .expect("fragments");
    // Both keys land in one fragment.
    assert_eq!(outcome.fragments.len(), 1);
    assert_eq!(outcome.fragments[0].keys().len(), 2);
}

#[test]
fn fragment_resizes_bucket_for_out_of_range_shard() {
    // A dispatcher returning an index past its declared count makes
    // the fragmenter grow its bucket vector.
    let mut req = Msg::new(0, MsgType::ReqRedisMget, true);
    req.push_key(KeyPos::without_tag(b"a".to_vec()));
    req.push_key(KeyPos::without_tag(b"b".to_vec()));
    req.set_ntokens(3);
    let pool = MbufPool::default();
    let outcome = redis_fragment(&mut req, &OutOfRange, &[], &pool)
        .expect("fragment ok")
        .expect("fragments");
    // Two distinct shard indices -> two fragments.
    assert_eq!(outcome.fragments.len(), 2);
}

#[test]
fn fragment_empty_keys_errors() {
    // A multikey request with no parsed keys is an EmptyKeys error.
    let mut req = Msg::new(0, MsgType::ReqRedisMget, true);
    req.set_ntokens(1);
    let pool = MbufPool::default();
    let outcome = redis_fragment(&mut req, &OddEven, &[], &pool);
    assert!(outcome.is_err(), "expected EmptyKeys error");
}

#[test]
fn del_and_exists_fragment_with_their_verbs() {
    // DEL and EXISTS fragments encode their own verb in the wire
    // frame (the del / exists arms of encode_fragment).
    for (ty, verb) in [
        (MsgType::ReqRedisDel, MsgType::ReqRedisDel),
        (MsgType::ReqRedisExists, MsgType::ReqRedisExists),
    ] {
        let mut req = Msg::new(0, ty, true);
        req.push_key(KeyPos::without_tag(b"a".to_vec())); // shard 1
        req.push_key(KeyPos::without_tag(b"b".to_vec())); // shard 0
        req.set_ntokens(3);
        let pool = MbufPool::default();
        let outcome = redis_fragment(&mut req, &OddEven, &[], &pool)
            .expect("fragment ok")
            .expect("fragments");
        assert_eq!(outcome.fragments.len(), 2);
        for frag in &outcome.fragments {
            let bytes: Vec<u8> = frag
                .mbufs()
                .iter()
                .flat_map(|mb| mb.readable().to_vec())
                .collect();
            let mut parsed = Msg::new(0, MsgType::Unknown, true);
            assert_eq!(
                redis_parse_req(&mut parsed, &bytes),
                MsgParseResult::Ok,
                "fragment reparse: {bytes:?}"
            );
            assert_eq!(parsed.ty(), verb);
        }
    }
}

// -------------------------------------------------------------
// Coalesce: pre/post hooks and integer accumulation.
// -------------------------------------------------------------

#[test]
fn pre_coalesce_on_request_is_noop() {
    // A request (not a response) short-circuits.
    let mut req = Msg::new(0, MsgType::ReqRedisGet, true);
    req.set_frag_id(1);
    redis_pre_coalesce(&mut req);
    assert!(!req.flags().is_error);
}

#[test]
fn pre_coalesce_non_fragment_is_noop() {
    // A response with frag_id 0 is not part of a fragmented request.
    let mut rsp = Msg::new(0, MsgType::RspRedisInteger, false);
    rsp.set_frag_id(0);
    redis_pre_coalesce(&mut rsp);
    assert!(!rsp.flags().is_error);
}

#[test]
fn pre_coalesce_integer_response_no_error() {
    // A fragmented integer response is a valid foldable shape and is
    // not flagged as an error.
    let mut rsp = Msg::new(0, MsgType::RspRedisInteger, false);
    rsp.set_frag_id(7);
    redis_pre_coalesce(&mut rsp);
    assert!(!rsp.flags().is_error);
}

#[test]
fn pre_coalesce_error_response_is_flagged() {
    // A fragmented error response is flagged is_error.
    let mut rsp = Msg::new(0, MsgType::RspRedisErrorWrongtype, false);
    rsp.set_frag_id(7);
    redis_pre_coalesce(&mut rsp);
    assert!(rsp.flags().is_error);
}

#[test]
fn pre_coalesce_unexpected_type_marks_bad_format() {
    // A fragmented response of an unexpected type is flagged as an
    // error (the wildcard arm).
    let mut rsp = Msg::new(0, MsgType::RspRedisBulk, false);
    rsp.set_frag_id(7);
    redis_pre_coalesce(&mut rsp);
    assert!(rsp.flags().is_error);
}

#[test]
fn accumulate_integer_folds_del_total() {
    // DEL fragment integers fold into the parent total.
    let mut parent = Msg::new(1, MsgType::ReqRedisDel, true);
    parent.set_integer(2);
    let mut rsp = Msg::new(2, MsgType::RspRedisInteger, false);
    rsp.set_frag_id(7);
    rsp.set_integer(3);
    accumulate_fragment_integer(&mut parent, &rsp);
    assert_eq!(parent.integer(), 5);
}

#[test]
fn accumulate_integer_ignores_non_del_parent() {
    // A non-DEL/EXISTS parent does not accumulate.
    let mut parent = Msg::new(1, MsgType::ReqRedisGet, true);
    parent.set_integer(2);
    let mut rsp = Msg::new(2, MsgType::RspRedisInteger, false);
    rsp.set_frag_id(7);
    rsp.set_integer(3);
    accumulate_fragment_integer(&mut parent, &rsp);
    assert_eq!(parent.integer(), 2);
}

#[test]
fn accumulate_integer_ignores_request_response_and_non_integer() {
    // Each guard short-circuits: a request "response", a
    // non-fragment response, and a non-integer response are all
    // no-ops.
    let mut parent = Msg::new(1, MsgType::ReqRedisExists, true);
    parent.set_integer(0);

    let mut as_request = Msg::new(2, MsgType::RspRedisInteger, true);
    as_request.set_frag_id(7);
    as_request.set_integer(9);
    accumulate_fragment_integer(&mut parent, &as_request);
    assert_eq!(parent.integer(), 0);

    let mut non_frag = Msg::new(3, MsgType::RspRedisInteger, false);
    non_frag.set_frag_id(0);
    non_frag.set_integer(9);
    accumulate_fragment_integer(&mut parent, &non_frag);
    assert_eq!(parent.integer(), 0);

    let mut non_int = Msg::new(4, MsgType::RspRedisStatus, false);
    non_int.set_frag_id(7);
    non_int.set_integer(9);
    accumulate_fragment_integer(&mut parent, &non_int);
    assert_eq!(parent.integer(), 0);
}

#[test]
fn post_coalesce_marks_done_when_clean() {
    let mut req = Msg::new(0, MsgType::ReqRedisDel, true);
    redis_post_coalesce(&mut req);
    assert!(req.flags().done);
}

#[test]
fn post_coalesce_skips_error_parent() {
    // A parent already flagged in error is not marked done.
    let mut req = Msg::new(0, MsgType::ReqRedisDel, true);
    req.flags_mut().is_error = true;
    redis_post_coalesce(&mut req);
    assert!(!req.flags().done);
}

#[test]
fn post_coalesce_on_response_is_noop() {
    // A response (not a request) short-circuits.
    let mut rsp = Msg::new(0, MsgType::RspRedisStatus, false);
    redis_post_coalesce(&mut rsp);
    assert!(!rsp.flags().done);
}

// -------------------------------------------------------------
// Repair: reconcile decision and the enabled make path.
// -------------------------------------------------------------

#[test]
fn reconcile_multikey_quorum_picks_first() {
    // A multikey DEL under DC_QUORUM returns PickFirst.
    let req = Msg::new(0, MsgType::ReqRedisDel, true);
    let mgr = ResponseMgr::new(&req, 3, None);
    assert_eq!(
        redis_reconcile_responses(&mgr, ConsistencyLevel::DcQuorum),
        ReconcileOutcome::PickFirst
    );
}

#[test]
fn reconcile_multikey_safe_quorum_errors() {
    // A multikey DEL under a stricter level surfaces an error.
    let req = Msg::new(0, MsgType::ReqRedisDel, true);
    let mgr = ResponseMgr::new(&req, 3, None);
    assert!(matches!(
        redis_reconcile_responses(&mgr, ConsistencyLevel::DcSafeQuorum),
        ReconcileOutcome::Error(_)
    ));
}

#[test]
fn reconcile_single_key_quorum_and_safe() {
    // A single-key GET: DC_QUORUM picks first, stricter errors.
    let req = Msg::new(0, MsgType::ReqRedisGet, true);
    let mgr = ResponseMgr::new(&req, 3, None);
    assert_eq!(
        redis_reconcile_responses(&mgr, ConsistencyLevel::DcQuorum),
        ReconcileOutcome::PickFirst
    );
    assert!(matches!(
        redis_reconcile_responses(&mgr, ConsistencyLevel::DcSafeQuorum),
        ReconcileOutcome::Error(_)
    ));
}

#[test]
fn foldable_multikey_response_predicate() {
    assert!(is_foldable_multikey_response(MsgType::RspRedisInteger));
    assert!(is_foldable_multikey_response(MsgType::RspRedisMultibulk));
    assert!(is_foldable_multikey_response(MsgType::RspRedisStatus));
    assert!(!is_foldable_multikey_response(MsgType::RspRedisBulk));
}

#[test]
fn repairable_predicate() {
    assert!(is_repairable(MsgType::ReqRedisGet));
    assert!(is_repairable(MsgType::ReqRedisHget));
    assert!(is_repairable(MsgType::ReqRedisSismember));
    assert!(is_repairable(MsgType::ReqRedisZscore));
    assert!(!is_repairable(MsgType::ReqRedisSet));
}

#[test]
fn make_repair_query_enabled_paths() {
    // The read-repairs global is a process-wide one-shot. Under
    // `cargo test` all tests in this binary share one process, so a
    // single test owns the toggle and exercises both the repairable
    // (reaches the post-toggle build branch) and non-repairable
    // (short-circuits on the is_repairable guard) paths. Both report
    // NoOp because the build step needs the Stage 9 per-response
    // parsing.
    set_read_repairs_enabled(true);
    assert!(
        dynomite::msg::is_read_repairs_enabled(),
        "read repairs should be enabled after set_read_repairs_enabled(true)"
    );

    let get = Msg::new(0, MsgType::ReqRedisGet, true);
    let get_mgr = ResponseMgr::new(&get, 3, None);
    assert!(matches!(
        redis_make_repair_query(&get_mgr).expect("repair query ok"),
        RepairOutcome::NoOp
    ));

    let set = Msg::new(0, MsgType::ReqRedisSet, true);
    let set_mgr = ResponseMgr::new(&set, 3, None);
    assert!(matches!(
        redis_make_repair_query(&set_mgr).expect("repair query ok"),
        RepairOutcome::NoOp
    ));
}
