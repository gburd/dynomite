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
//! Plus interval-tree-clock comparison checks for all four
//! `Ordering`-or-concurrent cases on [`Itc::partial_cmp_event`]
//! and PBC round-trip checks for the new `DtFetchReq/Resp` and
//! `DtUpdateReq/Resp` messages.

use std::cmp::Ordering;

use prost::Message as _;

use dyniak::datatypes::{ActorId, Crdt, EwFlag, Itc, LwwRegister, OrSet, PnCounter};
use dyniak::proto::pb::{
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

// ---- interval-tree-clock comparison cases ---------------------------------
//
// The previous Riak surface used a vector-clock / DVVSet-shaped
// causality tracker; the production tracker is now
// [`Itc`]. The four cases below pin the four observable
// outcomes of [`Itc::partial_cmp_event`]: strict precedence in
// either direction, equality, and concurrency (`None`).

#[test]
fn itc_compare_equal() {
    let mut a = Itc::seed();
    a.event();
    a.event();
    let b = a.clone();
    assert_eq!(a.partial_cmp_event(&b), Some(Ordering::Equal));
    // A peek shares the same event tree, so the clones are
    // equal under partial_cmp_event regardless of authority.
    assert_eq!(a.partial_cmp_event(&a.peek()), Some(Ordering::Equal));
}

#[test]
fn itc_compare_less() {
    let mut a = Itc::seed();
    let before = a.clone();
    a.event();
    assert_eq!(before.partial_cmp_event(&a), Some(Ordering::Less));
}

#[test]
fn itc_compare_greater() {
    let mut a = Itc::seed();
    let before = a.clone();
    a.event();
    a.event();
    assert_eq!(a.partial_cmp_event(&before), Some(Ordering::Greater));
}

#[test]
fn itc_compare_concurrent() {
    let (mut a, mut b) = Itc::seed().fork();
    a.event();
    b.event();
    assert_eq!(a.partial_cmp_event(&b), None);
    assert_eq!(b.partial_cmp_event(&a), None);
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
        map_value: None,
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

// ---- Map and HLL integration tests (second CRDT slice) ---------------------

use dyniak::datatypes::{
    map::{FieldKey, FieldType, FieldValue, Map, MapOp as MapEngOp, NestedOp},
    HyperLogLog,
};
use dyniak::proto::pb::{
    FlagOp, HllOp, HllValue, MapEntry, MapField, MapOp as MapWireOp, MapUpdate, MapValue,
    RegisterOp, ScalarOp, ScalarValue, MAP_FIELD_TYPE_COUNTER, MAP_FIELD_TYPE_FLAG,
    MAP_FIELD_TYPE_REGISTER,
};

#[test]
fn map_three_typed_fields_add_remove_merge_round_trip() {
    let a = aid("a");
    let b = aid("b");

    let cf = FieldKey::new(b"hits".to_vec(), FieldType::Counter);
    let ff = FieldKey::new(b"on".to_vec(), FieldType::EwFlag);
    let rf = FieldKey::new(b"name".to_vec(), FieldType::LwwRegister);

    let mut left = Map::new();
    left.apply(
        &a,
        &MapEngOp::Update {
            field: cf.clone(),
            op: NestedOp::Counter(5),
        },
    );
    left.apply(
        &a,
        &MapEngOp::Update {
            field: ff.clone(),
            op: NestedOp::Flag(true),
        },
    );
    left.apply(
        &a,
        &MapEngOp::Update {
            field: rf.clone(),
            op: NestedOp::RegisterAssign {
                value: b"alice".to_vec(),
                ts_micros: 100,
            },
        },
    );

    let mut right = Map::new();
    right.apply(
        &b,
        &MapEngOp::Update {
            field: cf.clone(),
            op: NestedOp::Counter(3),
        },
    );
    right.apply(
        &b,
        &MapEngOp::Update {
            field: rf.clone(),
            op: NestedOp::RegisterAssign {
                value: b"bob".to_vec(),
                ts_micros: 200,
            },
        },
    );
    // Concurrently, the right replica removes the flag.
    right.apply(&b, &MapEngOp::Remove { field: ff.clone() });

    left.merge(&right);

    let v = left.value();
    assert_eq!(v.len(), 3, "all three fields present after merge");
    match v.get(&cf) {
        Some(FieldValue::Counter(c)) => assert_eq!(c.value(), 8),
        _ => panic!("counter merged value missing"),
    }
    match v.get(&ff) {
        Some(FieldValue::EwFlag(f)) => {
            assert!(f.value(), "enable on left wins concurrent remove on right");
        }
        _ => panic!("flag missing"),
    }
    match v.get(&rf) {
        Some(FieldValue::LwwRegister(r)) => {
            assert_eq!(r.value(), b"bob".to_vec(), "later timestamp wins");
        }
        _ => panic!("register missing"),
    }
}

#[test]
fn hll_two_replicas_distinct_items_merged_estimate_within_5pct() {
    let mut left = HyperLogLog::new();
    let mut right = HyperLogLog::new();
    for i in 0u32..5_000 {
        left.add(i.to_be_bytes());
    }
    for i in 5_000u32..10_000 {
        right.add(i.to_be_bytes());
    }
    left.merge(&right);
    let est = left.value();
    // 1.04/sqrt(16384) ~= 0.81% expected error; +/-5% is
    // generous and avoids statistical flake at 10k items.
    assert!(
        (9_500..=10_500).contains(&est),
        "merged HLL estimate {est} outside [9500, 10500]"
    );
}

#[test]
fn hll_idempotent_under_set_equality() {
    let mut left = HyperLogLog::new();
    let mut right = HyperLogLog::new();
    for i in 0u32..10_000 {
        left.add(i.to_be_bytes());
        right.add(i.to_be_bytes());
    }
    let est_before = left.value();
    left.merge(&right);
    let est_after = left.value();
    assert!(
        (9_500..=10_500).contains(&est_before),
        "pre-merge estimate {est_before} outside [9500, 10500]"
    );
    assert_eq!(
        est_before, est_after,
        "merging equal HLLs leaves the estimate unchanged"
    );
}

#[test]
fn dt_op_with_map_payload_round_trips_via_prost() {
    let op = DtOp {
        map_op: Some(Box::new(MapWireOp {
            updates: vec![MapUpdate {
                field: Some(MapField {
                    name: b"hits".to_vec(),
                    field_type: MAP_FIELD_TYPE_COUNTER,
                }),
                op: Some(ScalarOp {
                    counter_op: Some(CounterOp { increment: Some(7) }),
                    ..ScalarOp::default()
                }),
            }],
            removes: vec![MapField {
                name: b"old".to_vec(),
                field_type: MAP_FIELD_TYPE_FLAG,
            }],
        })),
        ..DtOp::default()
    };
    let bytes = op.encode_to_vec();
    let back = DtOp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, op);
}

#[test]
fn dt_value_with_map_payload_round_trips_via_prost() {
    let value = DtValue {
        map_value: Some(Box::new(MapValue {
            entries: vec![
                MapEntry {
                    field: Some(MapField {
                        name: b"name".to_vec(),
                        field_type: MAP_FIELD_TYPE_REGISTER,
                    }),
                    value: Some(ScalarValue {
                        register_value: Some(b"alice".to_vec()),
                        ..ScalarValue::default()
                    }),
                },
                MapEntry {
                    field: Some(MapField {
                        name: b"on".to_vec(),
                        field_type: MAP_FIELD_TYPE_FLAG,
                    }),
                    value: Some(ScalarValue {
                        flag_value: Some(true),
                        ..ScalarValue::default()
                    }),
                },
            ],
        })),
        ..DtValue::default()
    };
    let bytes = value.encode_to_vec();
    let back = DtValue::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, value);
}

#[test]
fn dt_op_with_hll_payload_round_trips_via_prost() {
    let op = DtOp {
        hll_op: Some(HllOp {
            add_value: vec![b"alpha".to_vec(), b"beta".to_vec()],
        }),
        ..DtOp::default()
    };
    let bytes = op.encode_to_vec();
    let back = DtOp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, op);
}

#[test]
fn dt_value_with_hll_payload_round_trips_via_prost() {
    let value = DtValue {
        hll_value: Some(123_456),
        ..DtValue::default()
    };
    let bytes = value.encode_to_vec();
    let back = DtValue::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, value);
    assert_eq!(back.hll_value, Some(123_456));
}

#[test]
fn hll_value_message_round_trips() {
    let v = HllValue { cardinality: 42 };
    let bytes = v.encode_to_vec();
    let back = HllValue::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, v);
}

#[test]
fn scalar_op_register_assign_round_trips() {
    let op = ScalarOp {
        register_op: Some(RegisterOp {
            value: b"hello".to_vec(),
            ts_micros: Some(99),
        }),
        ..ScalarOp::default()
    };
    let bytes = op.encode_to_vec();
    let back = ScalarOp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, op);
}

#[test]
fn scalar_op_flag_round_trips() {
    let op = ScalarOp {
        flag_op: Some(FlagOp { enable: true }),
        ..ScalarOp::default()
    };
    let bytes = op.encode_to_vec();
    let back = ScalarOp::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, op);
}
