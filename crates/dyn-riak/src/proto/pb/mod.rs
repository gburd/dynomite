//! Riak Protocol Buffers transport.
//!
//! # Wire format
//!
//! Riak frames every Protocol-Buffers request and response identically:
//!
//! ```text
//!   +----------------+--------+--------------------------+
//!   | length (u32 BE)| code u8|       protobuf body      |
//!   +----------------+--------+--------------------------+
//!   |    4 bytes     | 1 byte |    `length - 1` bytes    |
//! ```
//!
//! `length` is the number of bytes that follow the length prefix and
//! therefore covers both the message code byte and the protobuf body.
//! The minimum legal value is `1` (a body-less request such as
//! `RpbPingReq`).
//!
//! Numerical message codes are stable across Riak versions; this
//! module pins the subset the v0.0.1 slice supports
//! ([`messages::MessageCode`]). Codes outside the supported set
//! produce [`crate::error::RiakError::UnknownMessageCode`].
//!
//! # Encoding
//!
//! All bodies in this slice are encoded as protobuf via
//! [`dyn_encoding::ProtobufCodec`]. The codec is built once via
//! [`codec_registry`] and shared across connections. The
//! `dyn-encoding` registry is keyed by content-type, so the upcoming
//! HTTP gateway can register JSON and CBOR codecs alongside this
//! protobuf codec without touching the PBC path.

pub mod codec;
pub mod datatypes;
pub mod framer;
pub mod mapreduce;
pub mod messages;

pub use crate::proto::pb::codec::{codec_registry, PbCodecBundle, PBC_CONTENT_TYPE};
pub use crate::proto::pb::framer::{read_frame, write_frame, Frame, MAX_FRAME_LEN};
pub use crate::proto::pb::messages::{
    MessageCode, RpbDelReq, RpbErrorResp, RpbGetReq, RpbGetResp, RpbPingReq, RpbPingResp,
    RpbPutReq, RpbPutResp,
};

// New message types added by the v0.0.2 PBC ops slice. Re-exported
// below the existing block so parallel branches do not conflict.
pub use crate::proto::pb::messages::{
    RpbBucketProps, RpbGetBucketReq, RpbGetBucketResp, RpbGetServerInfoResp, RpbIndexReq,
    RpbIndexResp, RpbListBucketsReq, RpbListBucketsResp, RpbListKeysReq, RpbListKeysResp, RpbPair,
    RpbServerInfoReq, RpbSetBucketReq, RpbSetBucketResp, INDEX_QUERY_TYPE_EQ,
    INDEX_QUERY_TYPE_RANGE,
};

// CRDT operations -- v0.0.3 slice. Re-exported below the existing
// blocks so parallel branches do not conflict.
pub use crate::proto::pb::datatypes::{
    CounterOp, DtFetchReq, DtFetchResp, DtOp, DtUpdateReq, DtUpdateResp, DtValue, GSetOp, HllOp,
    SetOp, DATA_TYPE_COUNTER, DATA_TYPE_GSET, DATA_TYPE_HLL, DATA_TYPE_MAP, DATA_TYPE_SET,
    DT_FETCH_REQ_CODE, DT_FETCH_RESP_CODE, DT_UPDATE_REQ_CODE, DT_UPDATE_RESP_CODE,
};
// MapReduce ops added by the v0.0.3 slice. Re-exported below the
// previous block so parallel branches do not conflict.
pub use crate::proto::pb::mapreduce::{RpbMapRedReq, RpbMapRedResp};

// Map and HyperLogLog wire types -- second CRDT slice. Re-exported
// below the prior block so parallel branches do not conflict.
pub use crate::proto::pb::datatypes::{
    FlagOp, HllValue, MapEntry, MapField, MapOp, MapUpdate, MapValue, RegisterOp, ScalarOp,
    ScalarValue, MAP_FIELD_TYPE_COUNTER, MAP_FIELD_TYPE_FLAG, MAP_FIELD_TYPE_MAP,
    MAP_FIELD_TYPE_REGISTER, MAP_FIELD_TYPE_SET,
};

// Cluster admin extension messages -- v0.0.4 admin slice. The
// type set covers the five PBC pairs invoked by `dyn-admin`'s
// cluster-list / cluster-join / cluster-leave / cluster-plan /
// cluster-commit subcommands. Re-exported below the prior block
// so parallel branches do not conflict.
pub use crate::proto::pb::messages::{
    DynRpbClusterCommitReq, DynRpbClusterCommitResp, DynRpbClusterJoinReq, DynRpbClusterJoinResp,
    DynRpbClusterLeaveReq, DynRpbClusterLeaveResp, DynRpbClusterPlanReq, DynRpbClusterPlanResp,
    DynRpbListPeersReq, DynRpbListPeersResp, DynRpbPeerInfo, DynRpbStagedChange,
    DYN_STAGED_CHANGE_ADD, DYN_STAGED_CHANGE_REMOVE,
};

// Bucket-property selectors -- bucketonly keyfun and walk-N-
// successors replication slice. Re-exported below the prior block
// so parallel branches do not conflict.
pub use crate::proto::pb::messages::{
    CHASH_KEYFUN_BUCKETONLY, CHASH_KEYFUN_CUSTOM, CHASH_KEYFUN_STD,
    REPLICATION_STRATEGY_SUCCESSORS, REPLICATION_STRATEGY_TOPOLOGY,
};
