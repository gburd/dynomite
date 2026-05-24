//! End-to-end CRDT integration tests.
//!
//! Exercises the four datatypes together with the PBC envelope
//! types: build a CRDT in memory, project its value through a
//! `DtFetchResp`, encode and decode the response, then re-project.
//! Tests the four scenarios called out in the brief:
//!
//! * PNCounter: actors A and B both increment, merge, total is sum.
//! * OR-Set: A adds X, B adds X+Y, merge contains X and Y; A
//!   removes X concurrent with B's add, merge resolves "add wins".
//! * Register: A assigns at t=1, B at t=2, merge picks B's value.
//! * Flag: A enables, B disables concurrently, merge yields enabled.
//!
//! Plus vector-clock comparison checks for all four `VclockOrder`
//! cases and PBC round-trip checks for the new `DtFetchReq/Resp`
//! and `DtUpdateReq/Resp` messages.

use prost::Message as _;

use dyn_riak::datatypes::{
    ActorId, Crdt, EwFlag, LwwRegister, OrSet, PnCounter, Vclock, VclockOrder,
};
use dyn_riak::proto::pb::{
    CounterOp, DtFetchReq, DtFetchResp, DtOp, DtUpdateReq, DtUpdateResp, DtValue, SetOp,
    DATA_TYPE_COUNTER, DATA_TYPE_SET, DT_FETCH_REQ_CODE, DT_FETCH_RESP_CODE, DT_UPDATE_REQ_CODE,
    DT_UPDATE_RESP_CODE,
};

fn aid(name: &str) -> ActorId {
    ActorId::new("dc1", name)
}

#[test]
fn pncounter_distinct_actors_sum_on_merge() {
    let a = aid("a");
    let b = aid("b");
    let mut left = PnCounter::new();
    left.increment(&a, 7);
    let mut right = PnCounter::new();
    right.increment(&b, 11);
    left.merge(&right);
    assert_eq!(left.value(), 18);
}

#[test]
fn pncounter_decrement_subtracts() {
    let a = aid("a");
    let b = aid("b");
    let mut left = PnCounter::new();
    left.increment(&a, 10);
    let mut right = PnCounter::new();
    right.decrement(&b, 4);
    left.merge(&right);
    assert_eq!(left.value(), 6);
}

#[test]
fn orset_add_and_concurrent_add_merge_to_union() {
    let a = aid("a");
    let b = aid("b");
    let mut left = OrSet::new();
    left.add(&a, b"X".to_vec());
    let mut right = OrSet::new();
    right.add(&b, b"X".to_vec());
    right.add(&b, b"Y".to_vec());
    left.merge(&right);
    let v = left.value();
    assert!(v.contains(b"X".as_slice()));
    assert!(v.contains(b"Y".as_slice()));
    assert_eq!(v.len(), 2);
}

#[test]
fn orset_remove_concurrent_with_add_keeps_element() {
    let a = aid("a");
    let b = aid("b");
    let mut shared = OrSet::new();
    shared.add(&a, b"X".to_vec());

    let mut left = shared.clone();
    left.remove(b"X");

    let mut right = shared.clone();
    right.add(&b, b"X".to_vec());

    left.merge(&right);
    assert!(
        left.contains(b"X"),
        "OR-Set add wins on tie when concurrent with remove"
    );
}

#[test]
fn lww_register_higher_timestamp_wins_on_merge() {
    let a = aid("a");
    let b = aid("b");
    let mut left = LwwRegister::new();
    left.assign(&a, 1, b"first".to_vec());
    let mut right = LwwRegister::new();
    right.assign(&b, 2, b"second".to_vec());
    left.merge(&right);
    assert_eq!(left.value(), b"second".to_vec());
}

#[test]
fn ewflag_enable_wins_concurrent_disable() {
    let a = aid("a");
    let b = aid("b");
    let mut shared = EwFlag::new();
    shared.enable(&a);

    let mut left = shared.clone();
    left.disable();

    let mut right = shared.clone();
    right.enable(&b);

    left.merge(&right);
    assert!(left.value(), "enable-wins flag is enabled after merge");
}

// ---- vector-clock comparison cases -----------------------------------------

#[test]
fn vclock_compare_equal() {
    let mut a = Vclock::new();
    let mut b = Vclock::new();
    a.increment(&aid("p"));
    b.increment(&aid("p"));
    assert_eq!(a.compare(&b), VclockOrder::Equal);
}

#[test]
fn vclock_compare_less() {
    let mut a = Vclock::new();
    let mut b = Vclock::new();
    a.increment(&aid("p"));
    b.increment(&aid("p"));
    b.increment(&aid("p"));
    assert_eq!(a.compare(&b), VclockOrder::Less);
}

#[test]
fn vclock_compare_greater() {
    let mut a = Vclock::new();
    let mut b = Vclock::new();
    a.increment(&aid("p"));
    a.increment(&aid("p"));
    b.increment(&aid("p"));
    assert_eq!(a.compare(&b), VclockOrder::Greater);
}

#[test]
fn vclock_compare_concurrent() {
    let mut a = Vclock::new();
    let mut b = Vclock::new();
    a.increment(&aid("p1"));
    b.increment(&aid("p2"));
    assert_eq!(a.compare(&b), VclockOrder::Concurrent);
}

// ---- PBC round trips for the new ops ---------------------------------------

#[test]
fn dt_fetch_req_round_trips_via_prost() {
    let req = DtFetchReq {
        bucket: b"bucket".to_vec(),
        key: b"key".to_vec(),
        r#type: b"counters".to_vec(),
        r: Some(2),
        pr: None,
        basic_quorum: Some(false),
        notfound_ok: None,
        timeout: Some(1_000),
        sloppy_quorum: None,
        n_val: Some(3),
        include_context: Some(true),
    };
    let bytes = req.encode_to_vec();
    let back = DtFetchReq::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, req);
}

#[test]
fn dt_fetch_resp_round_trips_for_orset() {
    let resp = DtFetchResp {
        context: Some(b"opaque".to_vec()),
        r#type: DATA_TYPE_SET,
        value: Some(DtValue {
            set_value: vec![b"x".to_vec(), b"y".to_vec()],
            ..DtValue::default()
        }),
    };
    let bytes = resp.encode_to_vec();
    let back = DtFetchResp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, resp);
}

#[test]
fn dt_update_req_round_trips_for_counter() {
    let req = DtUpdateReq {
        bucket: b"counters".to_vec(),
        key: Some(b"hits".to_vec()),
        r#type: b"counters".to_vec(),
        context: None,
        op: Some(DtOp {
            counter_op: Some(CounterOp {
                increment: Some(-3),
            }),
            ..DtOp::default()
        }),
        w: Some(2),
        dw: None,
        pw: None,
        return_body: Some(true),
        timeout: None,
        sloppy_quorum: None,
        n_val: Some(3),
        include_context: Some(false),
    };
    let bytes = req.encode_to_vec();
    let back = DtUpdateReq::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, req);
}

#[test]
fn dt_update_resp_round_trips_for_orset() {
    let resp = DtUpdateResp {
        key: Some(b"alice".to_vec()),
        context: Some(b"new-ctx".to_vec()),
        counter_value: None,
        set_value: vec![b"alpha".to_vec(), b"beta".to_vec()],
        hll_value: None,
        gset_value: vec![],
    };
    let bytes = resp.encode_to_vec();
    let back = DtUpdateResp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, resp);
}

#[test]
fn pbc_codes_are_riak_canonical() {
    assert_eq!(DT_FETCH_REQ_CODE, 80);
    assert_eq!(DT_FETCH_RESP_CODE, 81);
    assert_eq!(DT_UPDATE_REQ_CODE, 82);
    assert_eq!(DT_UPDATE_RESP_CODE, 83);
}

// ---- Project a CRDT through a DtFetchResp and back -------------------------

#[test]
fn pncounter_value_round_trips_through_dt_fetch_resp() {
    let a = aid("a");
    let b = aid("b");
    let mut counter = PnCounter::new();
    counter.increment(&a, 5);
    counter.increment(&b, 7);

    let resp = DtFetchResp {
        context: Some(b"ctx".to_vec()),
        r#type: DATA_TYPE_COUNTER,
        value: Some(DtValue {
            counter_value: Some(counter.value()),
            ..DtValue::default()
        }),
    };
    let bytes = resp.encode_to_vec();
    let back = DtFetchResp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back.r#type, DATA_TYPE_COUNTER);
    assert_eq!(back.value.expect("value present").counter_value, Some(12));
}

#[test]
fn orset_value_round_trips_through_dt_fetch_resp() {
    let a = aid("a");
    let mut set = OrSet::new();
    set.add(&a, b"x".to_vec());
    set.add(&a, b"y".to_vec());

    let mut elements: Vec<Vec<u8>> = set.value().into_iter().collect();
    elements.sort();

    let resp = DtFetchResp {
        context: Some(b"ctx".to_vec()),
        r#type: DATA_TYPE_SET,
        value: Some(DtValue {
            set_value: elements.clone(),
            ..DtValue::default()
        }),
    };
    let bytes = resp.encode_to_vec();
    let back = DtFetchResp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back.value.expect("value present").set_value, elements);
}

#[test]
fn dt_op_with_set_payload_round_trips() {
    let op = DtOp {
        set_op: Some(SetOp {
            adds: vec![b"alice".to_vec()],
            removes: vec![b"bob".to_vec()],
        }),
        ..DtOp::default()
    };
    let bytes = op.encode_to_vec();
    let back = DtOp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, op);
}
