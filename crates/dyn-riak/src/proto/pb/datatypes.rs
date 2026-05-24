//! Riak CRDT operations on the Protocol Buffers wire.
//!
//! This module models the Riak `DtFetchReq` / `DtFetchResp` and
//! `DtUpdateReq` / `DtUpdateResp` messages plus the operation
//! payloads they reference.
//!
//! # Field-tag stability
//!
//! Tag numbers match Riak's published `riak_dt.proto`. Tags
//! reserved for nested message types not modelled in this slice
//! (`MapOp`, `MapEntry`, `MapField`) are left as gaps; `prost`
//! skips them on decode and the gap preserves the schema's wire
//! tag invariants.
//!
//! # Datatype enum encoding
//!
//! Riak's `DtFetchResp.type` is an enum (`COUNTER=1, SET=2,
//! MAP=3, HLL=4, GSET=5`). Modelling it as `int32` with the
//! constants below mirrors the approach already used for the 2i
//! query-type field and avoids a separate `prost` enum derive
//! that would inflate the surface for one byte of wire savings.

use prost::Message;

use dyn_encoding::{WireTypeId, WireValue};

/// `DtFetchReq` / `DtFetchResp` PBC code -- request side.
pub const DT_FETCH_REQ_CODE: u8 = 80;
/// `DtFetchResp` PBC code.
pub const DT_FETCH_RESP_CODE: u8 = 81;
/// `DtUpdateReq` PBC code.
pub const DT_UPDATE_REQ_CODE: u8 = 82;
/// `DtUpdateResp` PBC code.
pub const DT_UPDATE_RESP_CODE: u8 = 83;

/// `DtFetchResp.type == COUNTER`. Wire-encoded as `1`.
pub const DATA_TYPE_COUNTER: i32 = 1;
/// `DtFetchResp.type == SET`. Wire-encoded as `2`.
pub const DATA_TYPE_SET: i32 = 2;
/// `DtFetchResp.type == MAP`. Wire-encoded as `3`. Reserved for
/// the map slice; not used by the current crate but published
/// here so a client probing for support sees the canonical code.
pub const DATA_TYPE_MAP: i32 = 3;
/// `DtFetchResp.type == HLL`. Wire-encoded as `4`. Reserved for
/// the HLL slice.
pub const DATA_TYPE_HLL: i32 = 4;
/// `DtFetchResp.type == GSET`. Wire-encoded as `5`. Reserved for
/// the grow-only-set slice.
pub const DATA_TYPE_GSET: i32 = 5;

// ---- operation payloads ----------------------------------------------------

/// `CounterOp` -- Riak counter increment.
///
/// `increment` is signed; positive values increment, negative
/// values decrement. Absent `increment` is a no-op.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct CounterOp {
    /// Signed delta to apply.
    #[prost(sint64, optional, tag = "1")]
    pub increment: Option<i64>,
}

impl WireValue for CounterOp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.CounterOp")
    }
}

/// `SetOp` -- OR-Set add and remove batch.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct SetOp {
    /// Elements to add.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub adds: Vec<Vec<u8>>,
    /// Elements to remove. Riak requires a non-empty `context`
    /// field on the enclosing `DtUpdateReq` whenever this list is
    /// non-empty; the field is documented but not enforced at the
    /// PBC layer.
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub removes: Vec<Vec<u8>>,
}

impl WireValue for SetOp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.SetOp")
    }
}

/// `HllOp` -- HyperLogLog add batch. Reserved for the HLL slice.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct HllOp {
    /// Elements to fold into the HLL register array.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub adds: Vec<Vec<u8>>,
}

impl WireValue for HllOp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.HllOp")
    }
}

/// `GSetOp` -- grow-only set add batch.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct GSetOp {
    /// Elements to add. Grow-only sets do not support removal.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub adds: Vec<Vec<u8>>,
}

impl WireValue for GSetOp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.GSetOp")
    }
}

/// `DtOp` -- one of the per-type operation payloads.
///
/// At most one of the optional fields is present; the field that
/// is set names the datatype the request targets. Tag 3
/// (`map_op`) is reserved for the map slice.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DtOp {
    /// Counter operation.
    #[prost(message, optional, tag = "1")]
    pub counter_op: Option<CounterOp>,
    /// OR-Set operation.
    #[prost(message, optional, tag = "2")]
    pub set_op: Option<SetOp>,
    // Tag 3 reserved for `map_op` (MapOp) -- map slice not shipped.
    /// HyperLogLog operation.
    #[prost(message, optional, tag = "4")]
    pub hll_op: Option<HllOp>,
    /// Grow-only set operation.
    #[prost(message, optional, tag = "5")]
    pub gset_op: Option<GSetOp>,
}

impl WireValue for DtOp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.DtOp")
    }
}

/// `DtValue` -- per-type stored value envelope.
///
/// Exactly one field carries the projection that matches the
/// enclosing response's `type` field. Tag 3 (`map_value`) is
/// reserved for the map slice.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DtValue {
    /// Counter projection.
    #[prost(sint64, optional, tag = "1")]
    pub counter_value: Option<i64>,
    /// OR-Set / G-Set projection.
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub set_value: Vec<Vec<u8>>,
    // Tag 3 reserved for `map_value` (repeated MapEntry).
    /// HLL cardinality estimate.
    #[prost(uint64, optional, tag = "4")]
    pub hll_value: Option<u64>,
    /// G-Set value (Riak emits this on tag 5 for `type == GSET`).
    #[prost(bytes = "vec", repeated, tag = "5")]
    pub gset_value: Vec<Vec<u8>>,
}

impl WireValue for DtValue {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.DtValue")
    }
}

// ---- DtFetch ---------------------------------------------------------------

/// `DtFetchReq` -- fetch a CRDT under (bucket-type, bucket, key).
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DtFetchReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Object key. Required.
    #[prost(bytes = "vec", tag = "2")]
    pub key: Vec<u8>,
    /// Bucket type. Required: every CRDT key lives under a
    /// typed bucket.
    #[prost(bytes = "vec", tag = "3")]
    pub r#type: Vec<u8>,
    /// Replica read count R.
    #[prost(uint32, optional, tag = "4")]
    pub r: Option<u32>,
    /// Primary read count PR.
    #[prost(uint32, optional, tag = "5")]
    pub pr: Option<u32>,
    /// Whether `notfound` should count as a successful read.
    #[prost(bool, optional, tag = "6")]
    pub basic_quorum: Option<bool>,
    /// Whether `notfound` is a successful read.
    #[prost(bool, optional, tag = "7")]
    pub notfound_ok: Option<bool>,
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "8")]
    pub timeout: Option<u32>,
    /// Inhibit sibling resolution.
    #[prost(bool, optional, tag = "9")]
    pub sloppy_quorum: Option<bool>,
    /// Override the bucket's `n_val`.
    #[prost(uint32, optional, tag = "10")]
    pub n_val: Option<u32>,
    /// Whether the response should include the opaque context
    /// blob (defaults to true on Riak; modelled as
    /// `Option<bool>` so the wire-default is preserved).
    #[prost(bool, optional, tag = "11")]
    pub include_context: Option<bool>,
}

impl WireValue for DtFetchReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.DtFetchReq")
    }
}

/// `DtFetchResp` -- response carrying the stored CRDT value.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DtFetchResp {
    /// Opaque conflict-resolution context. Clients echo this
    /// back on the next `DtUpdateReq` so the server can compute
    /// a correct OR-Set tombstone or map-field disable.
    #[prost(bytes = "vec", optional, tag = "1")]
    pub context: Option<Vec<u8>>,
    /// Datatype id; one of [`DATA_TYPE_COUNTER`],
    /// [`DATA_TYPE_SET`], [`DATA_TYPE_MAP`], [`DATA_TYPE_HLL`],
    /// or [`DATA_TYPE_GSET`].
    #[prost(int32, tag = "2")]
    pub r#type: i32,
    /// The stored value. Absent when the key does not exist.
    #[prost(message, optional, tag = "3")]
    pub value: Option<DtValue>,
}

impl WireValue for DtFetchResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.DtFetchResp")
    }
}

// ---- DtUpdate --------------------------------------------------------------

/// `DtUpdateReq` -- apply an operation to a CRDT under
/// (bucket-type, bucket, key).
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DtUpdateReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Object key. Optional: when absent, the server assigns one
    /// and echoes it in [`DtUpdateResp::key`].
    #[prost(bytes = "vec", optional, tag = "2")]
    pub key: Option<Vec<u8>>,
    /// Bucket type. Required.
    #[prost(bytes = "vec", tag = "3")]
    pub r#type: Vec<u8>,
    /// Opaque context returned by a prior [`DtFetchResp`].
    /// Required for set removes and map field disables; ignored
    /// for counters and HLL.
    #[prost(bytes = "vec", optional, tag = "4")]
    pub context: Option<Vec<u8>>,
    /// The operation payload. Required: an empty `DtOp` is a
    /// no-op and a conformant client should not send one.
    #[prost(message, optional, tag = "5")]
    pub op: Option<DtOp>,
    /// Replica write count W.
    #[prost(uint32, optional, tag = "6")]
    pub w: Option<u32>,
    /// Durable replica write count DW.
    #[prost(uint32, optional, tag = "7")]
    pub dw: Option<u32>,
    /// Primary write count PW.
    #[prost(uint32, optional, tag = "8")]
    pub pw: Option<u32>,
    /// Whether the response should echo the post-merge value.
    #[prost(bool, optional, tag = "9")]
    pub return_body: Option<bool>,
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "10")]
    pub timeout: Option<u32>,
    /// Inhibit sibling resolution.
    #[prost(bool, optional, tag = "11")]
    pub sloppy_quorum: Option<bool>,
    /// Override the bucket's `n_val`.
    #[prost(uint32, optional, tag = "12")]
    pub n_val: Option<u32>,
    /// Whether the response should include the opaque context.
    #[prost(bool, optional, tag = "13")]
    pub include_context: Option<bool>,
}

impl WireValue for DtUpdateReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.DtUpdateReq")
    }
}

/// `DtUpdateResp` -- response acknowledging a CRDT update.
///
/// Riak inlines the post-merge value's projection rather than
/// nesting it inside a `DtValue`, so the response carries the
/// per-type fields directly. Tag 5 (`map_value`) is reserved.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DtUpdateResp {
    /// Server-assigned key when the request omitted one.
    #[prost(bytes = "vec", optional, tag = "1")]
    pub key: Option<Vec<u8>>,
    /// Updated context for future operations.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub context: Option<Vec<u8>>,
    /// Counter post-merge value.
    #[prost(sint64, optional, tag = "3")]
    pub counter_value: Option<i64>,
    /// OR-Set post-merge value.
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub set_value: Vec<Vec<u8>>,
    // Tag 5 reserved for `map_value` (repeated MapEntry).
    /// HLL cardinality estimate.
    #[prost(uint64, optional, tag = "6")]
    pub hll_value: Option<u64>,
    /// G-Set post-merge value.
    #[prost(bytes = "vec", repeated, tag = "7")]
    pub gset_value: Vec<Vec<u8>>,
}

impl WireValue for DtUpdateResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.DtUpdateResp")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_op_round_trips() {
        let op = CounterOp {
            increment: Some(-7),
        };
        let bytes = op.encode_to_vec();
        let back = CounterOp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, op);
    }

    #[test]
    fn set_op_round_trips() {
        let op = SetOp {
            adds: vec![b"alice".to_vec(), b"bob".to_vec()],
            removes: vec![b"carol".to_vec()],
        };
        let bytes = op.encode_to_vec();
        let back = SetOp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, op);
    }

    #[test]
    fn dt_op_carries_at_most_one_payload() {
        let op = DtOp {
            counter_op: Some(CounterOp { increment: Some(5) }),
            ..DtOp::default()
        };
        let bytes = op.encode_to_vec();
        let back = DtOp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, op);
        assert!(back.set_op.is_none());
        assert!(back.hll_op.is_none());
        assert!(back.gset_op.is_none());
    }

    #[test]
    fn dt_fetch_req_round_trips() {
        let req = DtFetchReq {
            bucket: b"counters".to_vec(),
            key: b"hits".to_vec(),
            r#type: b"counters".to_vec(),
            r: Some(2),
            pr: Some(1),
            basic_quorum: Some(true),
            notfound_ok: Some(false),
            timeout: Some(5_000),
            sloppy_quorum: None,
            n_val: Some(3),
            include_context: Some(true),
        };
        let bytes = req.encode_to_vec();
        let back = DtFetchReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn dt_fetch_resp_round_trips_for_counter() {
        let resp = DtFetchResp {
            context: Some(b"opaque-ctx".to_vec()),
            r#type: DATA_TYPE_COUNTER,
            value: Some(DtValue {
                counter_value: Some(42),
                ..DtValue::default()
            }),
        };
        let bytes = resp.encode_to_vec();
        let back = DtFetchResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
        assert_eq!(back.r#type, 1);
    }

    #[test]
    fn dt_fetch_resp_round_trips_for_set() {
        let resp = DtFetchResp {
            context: Some(b"ctx".to_vec()),
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
    fn dt_update_req_round_trips() {
        let req = DtUpdateReq {
            bucket: b"counters".to_vec(),
            key: Some(b"hits".to_vec()),
            r#type: b"counters".to_vec(),
            context: Some(b"prev-ctx".to_vec()),
            op: Some(DtOp {
                counter_op: Some(CounterOp { increment: Some(3) }),
                ..DtOp::default()
            }),
            w: Some(2),
            dw: Some(1),
            pw: Some(1),
            return_body: Some(true),
            timeout: Some(2_500),
            sloppy_quorum: None,
            n_val: Some(3),
            include_context: Some(true),
        };
        let bytes = req.encode_to_vec();
        let back = DtUpdateReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn dt_update_resp_round_trips() {
        let resp = DtUpdateResp {
            key: Some(b"alice".to_vec()),
            context: Some(b"new-ctx".to_vec()),
            counter_value: Some(100),
            set_value: vec![],
            hll_value: None,
            gset_value: vec![],
        };
        let bytes = resp.encode_to_vec();
        let back = DtUpdateResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn empty_resp_round_trips() {
        let resp = DtUpdateResp::default();
        let bytes = resp.encode_to_vec();
        let back = DtUpdateResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn datatype_codes_match_riak_schema() {
        assert_eq!(DATA_TYPE_COUNTER, 1);
        assert_eq!(DATA_TYPE_SET, 2);
        assert_eq!(DATA_TYPE_MAP, 3);
        assert_eq!(DATA_TYPE_HLL, 4);
        assert_eq!(DATA_TYPE_GSET, 5);
    }

    #[test]
    fn pbc_codes_match_riak_schema() {
        assert_eq!(DT_FETCH_REQ_CODE, 80);
        assert_eq!(DT_FETCH_RESP_CODE, 81);
        assert_eq!(DT_UPDATE_REQ_CODE, 82);
        assert_eq!(DT_UPDATE_RESP_CODE, 83);
    }
}
