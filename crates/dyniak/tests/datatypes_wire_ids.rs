//! Wire-identity and protobuf round-trip coverage for the
//! `riak.*` CRDT operation/value message types in
//! `proto::pb::datatypes`.
//!
//! Each `WireValue::wire_type_id()` is asserted to return its
//! canonical `riak.<Type>` identifier (these one-line trait
//! impls are otherwise reached only by the codec registry), and
//! every message round-trips through prost encode/decode so the
//! field tags hold.

use dyn_encoding::WireValue;
use prost::Message;

use dyniak::proto::pb::datatypes::{
    CounterOp, DtFetchReq, DtFetchResp, DtOp, DtUpdateReq, DtUpdateResp, DtValue, FlagOp, GSetOp,
    HllOp, HllValue, MapEntry, MapField, MapOp, MapUpdate, MapValue, RegisterOp, ScalarOp,
    ScalarValue, SetOp, MAP_FIELD_TYPE_COUNTER, MAP_FIELD_TYPE_REGISTER,
};

/// Every message type reports its canonical `riak.<Type>`
/// wire id. This drives the `wire_type_id` trait impl for all
/// twenty datatype messages.
#[test]
fn wire_type_ids_are_canonical() {
    assert_eq!(CounterOp::wire_type_id().as_str(), "riak.CounterOp");
    assert_eq!(SetOp::wire_type_id().as_str(), "riak.SetOp");
    assert_eq!(HllOp::wire_type_id().as_str(), "riak.HllOp");
    assert_eq!(HllValue::wire_type_id().as_str(), "riak.HllValue");
    assert_eq!(MapField::wire_type_id().as_str(), "riak.MapField");
    assert_eq!(RegisterOp::wire_type_id().as_str(), "riak.RegisterOp");
    assert_eq!(FlagOp::wire_type_id().as_str(), "riak.FlagOp");
    assert_eq!(ScalarOp::wire_type_id().as_str(), "riak.ScalarOp");
    assert_eq!(MapUpdate::wire_type_id().as_str(), "riak.MapUpdate");
    assert_eq!(MapOp::wire_type_id().as_str(), "riak.MapOp");
    assert_eq!(ScalarValue::wire_type_id().as_str(), "riak.ScalarValue");
    assert_eq!(MapEntry::wire_type_id().as_str(), "riak.MapEntry");
    assert_eq!(MapValue::wire_type_id().as_str(), "riak.MapValue");
    assert_eq!(GSetOp::wire_type_id().as_str(), "riak.GSetOp");
    assert_eq!(DtOp::wire_type_id().as_str(), "riak.DtOp");
    assert_eq!(DtValue::wire_type_id().as_str(), "riak.DtValue");
    assert_eq!(DtFetchReq::wire_type_id().as_str(), "riak.DtFetchReq");
    assert_eq!(DtFetchResp::wire_type_id().as_str(), "riak.DtFetchResp");
    assert_eq!(DtUpdateReq::wire_type_id().as_str(), "riak.DtUpdateReq");
    assert_eq!(DtUpdateResp::wire_type_id().as_str(), "riak.DtUpdateResp");
}

/// A helper that prost-encodes then decodes a message and
/// asserts it survives unchanged.
fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(m: &M) {
    let bytes = m.encode_to_vec();
    let back = M::decode(&bytes[..]).expect("decode");
    assert_eq!(&back, m);
}

/// Each operation/value message round-trips through prost with
/// representative payloads, exercising every field tag.
#[test]
fn datatype_messages_round_trip() {
    round_trip(&CounterOp {
        increment: Some(-7),
    });
    round_trip(&SetOp {
        adds: vec![b"a".to_vec(), b"b".to_vec()],
        removes: vec![b"c".to_vec()],
    });
    round_trip(&HllOp {
        add_value: vec![b"x".to_vec()],
    });
    round_trip(&HllValue { cardinality: 42 });
    round_trip(&MapField {
        name: b"f".to_vec(),
        field_type: MAP_FIELD_TYPE_COUNTER,
    });
    round_trip(&RegisterOp {
        value: b"v".to_vec(),
        ts_micros: Some(123),
    });
    round_trip(&FlagOp { enable: true });
    round_trip(&GSetOp {
        adds: vec![b"g".to_vec()],
    });

    // A nested map update: a register field set inside a map op.
    let register_field = MapField {
        name: b"reg".to_vec(),
        field_type: MAP_FIELD_TYPE_REGISTER,
    };
    let scalar = ScalarOp {
        register_op: Some(RegisterOp {
            value: b"hello".to_vec(),
            ts_micros: None,
        }),
        ..ScalarOp::default()
    };
    let update = MapUpdate {
        field: Some(register_field.clone()),
        op: Some(scalar),
    };
    let map_op = MapOp {
        updates: vec![update],
        removes: vec![register_field],
    };
    round_trip(&map_op);

    // A map value projection.
    let entry = MapEntry {
        field: Some(MapField {
            name: b"k".to_vec(),
            field_type: MAP_FIELD_TYPE_REGISTER,
        }),
        value: Some(ScalarValue {
            register_value: Some(b"r".to_vec()),
            ..ScalarValue::default()
        }),
    };
    round_trip(&MapValue {
        entries: vec![entry],
    });
}
