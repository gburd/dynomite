//! Riak Protocol Buffers message types.
//!
//! The request/response pairs mirror the field shapes that Riak
//! KV 2.2.3 publishes in `riak_pb/src/riak_kv.proto` and `riak.proto`.
//! The structs are hand-derived rather than generated through
//! `prost-build` so the crate has no `build.rs` and no protoc
//! dependency. Each field tag matches Riak's published schema so a
//! conforming client interoperates byte-for-byte.
//!
//! # Field-tag stability
//!
//! Field tags must never be renumbered. They are part of the public
//! wire format. New tags get appended only. Tags reserved for fields
//! whose nested-message types are not modelled in this slice (for
//! example `RpbBucketProps.precommit` at tag 4) are left as gaps;
//! `prost` skips them on decode.

use prost::Message;

use dyn_encoding::{WireTypeId, WireValue};

/// Numerical message codes used by Riak's PBC framing.
///
/// The set covered here is the subset this crate implements (error,
/// ping, server-info, get, put, del, list-buckets, list-keys,
/// get-bucket, set-bucket, secondary-index). Other codes are reserved
/// for follow-up slices and will be rejected with
/// [`crate::error::RiakError::UnknownMessageCode`] until added.
///
/// # Examples
///
/// ```
/// use dyn_riak::proto::pb::MessageCode;
/// assert_eq!(MessageCode::PingReq.as_u8(), 1);
/// assert_eq!(MessageCode::from_u8(11).unwrap(), MessageCode::PutReq);
/// assert!(MessageCode::from_u8(99).is_err());
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[repr(u8)]
pub enum MessageCode {
    /// `RpbErrorResp` -- 0.
    ErrorResp = 0,
    /// `RpbPingReq` -- 1.
    PingReq = 1,
    /// `RpbPingResp` -- 2.
    PingResp = 2,
    /// `RpbGetReq` -- 9.
    GetReq = 9,
    /// `RpbGetResp` -- 10.
    GetResp = 10,
    /// `RpbPutReq` -- 11.
    PutReq = 11,
    /// `RpbPutResp` -- 12.
    PutResp = 12,
    /// `RpbDelReq` -- 13.
    DelReq = 13,
    /// `RpbDelResp` -- 14. Riak emits this with no body.
    DelResp = 14,
    /// `RpbServerInfoReq` -- 7. Empty body.
    ServerInfoReq = 7,
    /// `RpbGetServerInfoResp` -- 8.
    GetServerInfoResp = 8,
    /// `RpbListBucketsReq` -- 15.
    ListBucketsReq = 15,
    /// `RpbListBucketsResp` -- 16.
    ListBucketsResp = 16,
    /// `RpbListKeysReq` -- 17.
    ListKeysReq = 17,
    /// `RpbListKeysResp` -- 18.
    ListKeysResp = 18,
    /// `RpbGetBucketReq` -- 19.
    GetBucketReq = 19,
    /// `RpbGetBucketResp` -- 20.
    GetBucketResp = 20,
    /// `RpbSetBucketReq` -- 21.
    SetBucketReq = 21,
    /// `RpbSetBucketResp` -- 22. Empty body.
    SetBucketResp = 22,
    /// `RpbIndexReq` -- 25. Riak 2i secondary-index query.
    IndexReq = 25,
    /// `RpbIndexResp` -- 26.
    IndexResp = 26,
}

impl MessageCode {
    /// Map a raw byte to a [`MessageCode`].
    pub fn from_u8(code: u8) -> Result<Self, u8> {
        Ok(match code {
            0 => Self::ErrorResp,
            1 => Self::PingReq,
            2 => Self::PingResp,
            7 => Self::ServerInfoReq,
            8 => Self::GetServerInfoResp,
            9 => Self::GetReq,
            10 => Self::GetResp,
            11 => Self::PutReq,
            12 => Self::PutResp,
            13 => Self::DelReq,
            14 => Self::DelResp,
            15 => Self::ListBucketsReq,
            16 => Self::ListBucketsResp,
            17 => Self::ListKeysReq,
            18 => Self::ListKeysResp,
            19 => Self::GetBucketReq,
            20 => Self::GetBucketResp,
            21 => Self::SetBucketReq,
            22 => Self::SetBucketResp,
            25 => Self::IndexReq,
            26 => Self::IndexResp,
            other => return Err(other),
        })
    }

    /// Return the wire byte for this code.
    #[must_use]
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

// ---- error response --------------------------------------------------------

/// `RpbErrorResp` -- generic error envelope returned in place of a
/// type-specific response when a request fails.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbErrorResp {
    /// Human-readable error message.
    #[prost(bytes = "vec", tag = "1")]
    pub errmsg: Vec<u8>,
    /// Riak-specific error code. Zero is reserved for "unknown".
    #[prost(uint32, tag = "2")]
    pub errcode: u32,
}

impl WireValue for RpbErrorResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbErrorResp")
    }
}

// ---- ping ------------------------------------------------------------------

/// `RpbPingReq` -- empty request body. Sent under message code 1.
///
/// Riak's wire shape for ping is "frame with code 1 and a zero-length
/// body". The struct exists so the codec layer can be uniform across
/// every operation: every request decodes through `prost::Message`.
///
/// # Examples
///
/// ```
/// use dyn_riak::proto::pb::RpbPingReq;
/// use prost::Message;
/// let bytes = RpbPingReq::default().encode_to_vec();
/// assert!(bytes.is_empty());
/// ```
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbPingReq {}

impl WireValue for RpbPingReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbPingReq")
    }
}

/// `RpbPingResp` -- empty response body. Sent under message code 2.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbPingResp {}

impl WireValue for RpbPingResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbPingResp")
    }
}

// ---- get -------------------------------------------------------------------

/// `RpbGetReq` -- fetch the object stored under (bucket, key).
///
/// Mirrors the v0 subset of the upstream message. Optional fields use
/// `prost`'s `optional` form; consumers test `is_some()` to detect
/// "not provided".
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbGetReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Object key. Required.
    #[prost(bytes = "vec", tag = "2")]
    pub key: Vec<u8>,
    /// Replica read count R. Defaults to the bucket configuration.
    #[prost(uint32, optional, tag = "3")]
    pub r: Option<u32>,
    /// Primary read count PR. Defaults to the bucket configuration.
    #[prost(uint32, optional, tag = "4")]
    pub pr: Option<u32>,
    /// Whether `notfound` should count as a successful replica read.
    #[prost(bool, optional, tag = "5")]
    pub basic_quorum: Option<bool>,
    /// Whether to treat `notfound` as a successful read.
    #[prost(bool, optional, tag = "6")]
    pub notfound_ok: Option<bool>,
    /// Vector clock the client believes is current; if this matches
    /// the stored object the server may omit the body.
    #[prost(bytes = "vec", optional, tag = "7")]
    pub if_modified: Option<Vec<u8>>,
    /// Return only the metadata, no body (HEAD-equivalent).
    #[prost(bool, optional, tag = "8")]
    pub head: Option<bool>,
    /// Return only siblings deterministically.
    #[prost(bool, optional, tag = "9")]
    pub deletedvclock: Option<bool>,
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "10")]
    pub timeout: Option<u32>,
    /// Inhibit sibling resolution.
    #[prost(bool, optional, tag = "11")]
    pub sloppy_quorum: Option<bool>,
    /// Override the bucket's `n_val` for this request only.
    #[prost(uint32, optional, tag = "12")]
    pub n_val: Option<u32>,
    /// Bucket type. Riak 2.0+; defaults to "default".
    #[prost(bytes = "vec", optional, tag = "13")]
    pub r#type: Option<Vec<u8>>,
}

impl WireValue for RpbGetReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbGetReq")
    }
}

/// `RpbGetResp` -- response carrying zero or more sibling content
/// records. The v0.0.1 slice models the response as opaque content
/// bytes; full sibling materialisation lands with the `RiakObject`
/// schema in a follow-up slice.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbGetResp {
    /// Raw concatenated content bytes. The follow-up slice replaces
    /// this single field with a repeated `RpbContent` once that
    /// nested message is on the wire.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub content: Vec<Vec<u8>>,
    /// Vector clock for the stored object.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub vclock: Option<Vec<u8>>,
    /// True when the stored value was unchanged relative to the
    /// `if_modified` vector clock.
    #[prost(bool, optional, tag = "3")]
    pub unchanged: Option<bool>,
}

impl WireValue for RpbGetResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbGetResp")
    }
}

// ---- put -------------------------------------------------------------------

/// `RpbPutReq` -- store an object under (bucket, key).
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbPutReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Object key. Optional: when absent, Riak assigns a key.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub key: Option<Vec<u8>>,
    /// Vector clock the client expects to overwrite. Required for
    /// conditional updates.
    #[prost(bytes = "vec", optional, tag = "3")]
    pub vclock: Option<Vec<u8>>,
    /// Object value bytes. The follow-up slice splits this into
    /// `RpbContent { value, content_type, ... }`; for the v0.0.1
    /// slice the body is an opaque blob.
    #[prost(bytes = "vec", tag = "4")]
    pub value: Vec<u8>,
    /// Replica write count W.
    #[prost(uint32, optional, tag = "5")]
    pub w: Option<u32>,
    /// Durable replica write count DW.
    #[prost(uint32, optional, tag = "6")]
    pub dw: Option<u32>,
    /// Whether the response should include the stored object.
    #[prost(bool, optional, tag = "7")]
    pub return_body: Option<bool>,
    /// Primary write count PW.
    #[prost(uint32, optional, tag = "8")]
    pub pw: Option<u32>,
    /// Reject the put if the key already exists.
    #[prost(bool, optional, tag = "9")]
    pub if_not_modified: Option<bool>,
    /// Reject the put if the key already exists (legacy form).
    #[prost(bool, optional, tag = "10")]
    pub if_none_match: Option<bool>,
    /// Return only the head, not the body, of the stored object.
    #[prost(bool, optional, tag = "11")]
    pub return_head: Option<bool>,
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "12")]
    pub timeout: Option<u32>,
    /// Tolerate transient quorum loss.
    #[prost(bool, optional, tag = "13")]
    pub asis: Option<bool>,
    /// Inhibit sibling resolution.
    #[prost(bool, optional, tag = "14")]
    pub sloppy_quorum: Option<bool>,
    /// Override the bucket's `n_val` for this request only.
    #[prost(uint32, optional, tag = "15")]
    pub n_val: Option<u32>,
    /// Bucket type.
    #[prost(bytes = "vec", optional, tag = "16")]
    pub r#type: Option<Vec<u8>>,
}

impl WireValue for RpbPutReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbPutReq")
    }
}

/// `RpbPutResp` -- response acknowledging a put.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbPutResp {
    /// Optional content echoed back when the request asked for
    /// `return_body` or `return_head`.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub content: Vec<Vec<u8>>,
    /// Updated vector clock.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub vclock: Option<Vec<u8>>,
    /// Server-assigned key when the request omitted one.
    #[prost(bytes = "vec", optional, tag = "3")]
    pub key: Option<Vec<u8>>,
}

impl WireValue for RpbPutResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbPutResp")
    }
}

// ---- delete ----------------------------------------------------------------

/// `RpbDelReq` -- delete the object stored under (bucket, key).
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbDelReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Object key. Required.
    #[prost(bytes = "vec", tag = "2")]
    pub key: Vec<u8>,
    /// Replica write count for the tombstone.
    #[prost(uint32, optional, tag = "3")]
    pub rw: Option<u32>,
    /// Vector clock the client expects to overwrite.
    #[prost(bytes = "vec", optional, tag = "4")]
    pub vclock: Option<Vec<u8>>,
    /// Replica read count.
    #[prost(uint32, optional, tag = "5")]
    pub r: Option<u32>,
    /// Replica write count.
    #[prost(uint32, optional, tag = "6")]
    pub w: Option<u32>,
    /// Primary read count.
    #[prost(uint32, optional, tag = "7")]
    pub pr: Option<u32>,
    /// Primary write count.
    #[prost(uint32, optional, tag = "8")]
    pub pw: Option<u32>,
    /// Durable replica write count.
    #[prost(uint32, optional, tag = "9")]
    pub dw: Option<u32>,
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "10")]
    pub timeout: Option<u32>,
    /// Inhibit sibling resolution.
    #[prost(bool, optional, tag = "11")]
    pub sloppy_quorum: Option<bool>,
    /// Override the bucket's `n_val` for this request only.
    #[prost(uint32, optional, tag = "12")]
    pub n_val: Option<u32>,
    /// Bucket type.
    #[prost(bytes = "vec", optional, tag = "13")]
    pub r#type: Option<Vec<u8>>,
}

impl WireValue for RpbDelReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbDelReq")
    }
}

// ---- server info -----------------------------------------------------------

/// `RpbServerInfoReq` -- empty request body. Sent under message code 7.
///
/// The reply is [`RpbGetServerInfoResp`]. Riak names the request
/// `RpbServerInfoReq` and the response `RpbGetServerInfoResp`; the
/// asymmetry is part of the public schema and reproduced here.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbServerInfoReq {}

impl WireValue for RpbServerInfoReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbServerInfoReq")
    }
}

/// `RpbGetServerInfoResp` -- node identity and server version string.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbGetServerInfoResp {
    /// Node name. Optional in the schema.
    #[prost(bytes = "vec", optional, tag = "1")]
    pub node: Option<Vec<u8>>,
    /// Free-form server version string.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub server_version: Option<Vec<u8>>,
}

impl WireValue for RpbGetServerInfoResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbGetServerInfoResp")
    }
}

// ---- list buckets ----------------------------------------------------------

/// `RpbListBucketsReq` -- enumerate every bucket in the cluster.
///
/// Riak supports a streaming variant where the response is split
/// across multiple `RpbListBucketsResp` frames each carrying a
/// `done` flag. This crate ships the non-streaming variant: the
/// server emits one response carrying every bucket. Streaming is
/// listed in `docs/journal/2026-05-24-dyn-riak-pbc-ops-v1.md` as a
/// follow-up.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbListBucketsReq {
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "1")]
    pub timeout: Option<u32>,
    /// Whether the client wants the streaming response variant. The
    /// server may ignore this and return a single response.
    #[prost(bool, optional, tag = "2")]
    pub stream: Option<bool>,
    /// Bucket type. Riak 2.0+; defaults to "default".
    #[prost(bytes = "vec", optional, tag = "3")]
    pub r#type: Option<Vec<u8>>,
}

impl WireValue for RpbListBucketsReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbListBucketsReq")
    }
}

/// `RpbListBucketsResp` -- bucket-name list returned by
/// [`RpbListBucketsReq`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbListBucketsResp {
    /// Bucket names. Empty when the cluster has none.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub buckets: Vec<Vec<u8>>,
    /// Stream-completion marker. The non-streaming variant always
    /// sets this to `true`; the streaming variant sets it to `false`
    /// for every intermediate frame.
    #[prost(bool, optional, tag = "2")]
    pub done: Option<bool>,
}

impl WireValue for RpbListBucketsResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbListBucketsResp")
    }
}

// ---- list keys -------------------------------------------------------------

/// `RpbListKeysReq` -- enumerate every key in a bucket.
///
/// The streaming consideration is the same as for
/// [`RpbListBucketsReq`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbListKeysReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "2")]
    pub timeout: Option<u32>,
    /// Bucket type.
    #[prost(bytes = "vec", optional, tag = "3")]
    pub r#type: Option<Vec<u8>>,
}

impl WireValue for RpbListKeysReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbListKeysReq")
    }
}

/// `RpbListKeysResp` -- key list returned by [`RpbListKeysReq`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbListKeysResp {
    /// Keys in the bucket.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub keys: Vec<Vec<u8>>,
    /// Stream-completion marker. See [`RpbListBucketsResp::done`].
    #[prost(bool, optional, tag = "2")]
    pub done: Option<bool>,
}

impl WireValue for RpbListKeysResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbListKeysResp")
    }
}

// ---- bucket properties -----------------------------------------------------

/// `RpbBucketProps` -- per-bucket configuration.
///
/// The Riak schema places several nested-message types
/// (`RpbCommitHook` at tags 4 and 6, `RpbModFun` at tags 8 and 9,
/// `RpbReplMode` at tag 24) inside this message. Those nested types
/// are not modelled in this slice; their tag numbers are reserved
/// and skipped on decode by `prost`. All scalar and `bytes` fields
/// keep their published tags so a conforming client and server
/// agree on every wire byte for the fields that are modelled.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbBucketProps {
    /// Replication factor.
    #[prost(uint32, optional, tag = "1")]
    pub n_val: Option<u32>,
    /// Whether multiple sibling values may coexist.
    #[prost(bool, optional, tag = "2")]
    pub allow_mult: Option<bool>,
    /// Whether last-write-wins resolution is enforced.
    #[prost(bool, optional, tag = "3")]
    pub last_write_wins: Option<bool>,
    // Tags 4, 6, 8, 9 reserved for nested message types not modelled
    // in this slice.
    /// Whether any precommit hook is configured.
    #[prost(bool, optional, tag = "5")]
    pub has_precommit: Option<bool>,
    /// Whether any postcommit hook is configured.
    #[prost(bool, optional, tag = "7")]
    pub has_postcommit: Option<bool>,
    /// Vector-clock pruning: "old" threshold in milliseconds.
    #[prost(uint32, optional, tag = "10")]
    pub old_vclock: Option<u32>,
    /// Vector-clock pruning: "young" threshold in milliseconds.
    #[prost(uint32, optional, tag = "11")]
    pub young_vclock: Option<u32>,
    /// Vector-clock pruning: "big" entry-count threshold.
    #[prost(uint32, optional, tag = "12")]
    pub big_vclock: Option<u32>,
    /// Vector-clock pruning: "small" entry-count threshold.
    #[prost(uint32, optional, tag = "13")]
    pub small_vclock: Option<u32>,
    /// Default primary-read count.
    #[prost(uint32, optional, tag = "14")]
    pub pr: Option<u32>,
    /// Default replica-read count.
    #[prost(uint32, optional, tag = "15")]
    pub r: Option<u32>,
    /// Default replica-write count.
    #[prost(uint32, optional, tag = "16")]
    pub w: Option<u32>,
    /// Default primary-write count.
    #[prost(uint32, optional, tag = "17")]
    pub pw: Option<u32>,
    /// Default durable-write count.
    #[prost(uint32, optional, tag = "18")]
    pub dw: Option<u32>,
    /// Default replica-write count for tombstones.
    #[prost(uint32, optional, tag = "19")]
    pub rw: Option<u32>,
    /// Whether `notfound` counts toward the read quorum.
    #[prost(bool, optional, tag = "20")]
    pub basic_quorum: Option<bool>,
    /// Whether `notfound` is a successful read.
    #[prost(bool, optional, tag = "21")]
    pub notfound_ok: Option<bool>,
    /// Backend module name.
    #[prost(bytes = "vec", optional, tag = "22")]
    pub backend: Option<Vec<u8>>,
    /// Whether legacy Search 1.0 indexing is enabled.
    #[prost(bool, optional, tag = "23")]
    pub search: Option<bool>,
    // Tag 24 reserved for `repl` (RpbReplMode).
    /// Yokozuna search-index name.
    #[prost(bytes = "vec", optional, tag = "25")]
    pub search_index: Option<Vec<u8>>,
    /// CRDT data-type name (counter, set, map, ...).
    #[prost(bytes = "vec", optional, tag = "26")]
    pub datatype: Option<Vec<u8>>,
    /// Whether the bucket uses strong-consistency mode.
    #[prost(bool, optional, tag = "27")]
    pub consistent: Option<bool>,
    /// Whether the bucket is write-once.
    #[prost(bool, optional, tag = "28")]
    pub write_once: Option<bool>,
    /// HyperLogLog precision parameter (CRDT HLL only).
    #[prost(uint32, optional, tag = "29")]
    pub hll_precision: Option<u32>,
}

impl WireValue for RpbBucketProps {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbBucketProps")
    }
}

/// `RpbGetBucketReq` -- fetch the bucket's [`RpbBucketProps`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbGetBucketReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Bucket type.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub r#type: Option<Vec<u8>>,
}

impl WireValue for RpbGetBucketReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbGetBucketReq")
    }
}

/// `RpbGetBucketResp` -- bucket-properties response.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbGetBucketResp {
    /// Bucket properties. Required in the schema; modelled as
    /// `Option<...>` because `prost`'s message-typed fields are
    /// always optional at the Rust level.
    #[prost(message, optional, tag = "1")]
    pub props: Option<RpbBucketProps>,
}

impl WireValue for RpbGetBucketResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbGetBucketResp")
    }
}

/// `RpbSetBucketReq` -- update the bucket's [`RpbBucketProps`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbSetBucketReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Bucket properties to apply. Required in the schema;
    /// modelled as `Option<...>` for the same reason as
    /// [`RpbGetBucketResp::props`].
    #[prost(message, optional, tag = "2")]
    pub props: Option<RpbBucketProps>,
    /// Bucket type.
    #[prost(bytes = "vec", optional, tag = "3")]
    pub r#type: Option<Vec<u8>>,
}

impl WireValue for RpbSetBucketReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbSetBucketReq")
    }
}

/// `RpbSetBucketResp` -- empty response body. Sent under message
/// code 22.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbSetBucketResp {}

impl WireValue for RpbSetBucketResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbSetBucketResp")
    }
}

// ---- 2i (secondary index) --------------------------------------------------

/// Equality query type for [`RpbIndexReq::qtype`]. Encoded on the
/// wire as `0` per the Riak schema.
pub const INDEX_QUERY_TYPE_EQ: i32 = 0;

/// Range query type for [`RpbIndexReq::qtype`]. Encoded on the wire
/// as `1` per the Riak schema.
pub const INDEX_QUERY_TYPE_RANGE: i32 = 1;

/// `RpbPair` -- generic (key, value) tuple used inside
/// [`RpbIndexResp::results`] for `return_terms` queries.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbPair {
    /// Key bytes. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub key: Vec<u8>,
    /// Value bytes. Optional in the schema.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub value: Option<Vec<u8>>,
}

impl WireValue for RpbPair {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbPair")
    }
}

/// `RpbIndexReq` -- Riak 2i secondary-index query.
///
/// `qtype` is one of [`INDEX_QUERY_TYPE_EQ`] or
/// [`INDEX_QUERY_TYPE_RANGE`]. For an equality query, `key` carries
/// the index term to look up. For a range query, `range_min` and
/// `range_max` bound the query.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbIndexReq {
    /// Bucket name. Required.
    #[prost(bytes = "vec", tag = "1")]
    pub bucket: Vec<u8>,
    /// Index name (typically suffixed `_bin` or `_int`). Required.
    #[prost(bytes = "vec", tag = "2")]
    pub index: Vec<u8>,
    /// Query type: 0 = equality, 1 = range.
    #[prost(int32, tag = "3")]
    pub qtype: i32,
    /// Equality-query lookup key.
    #[prost(bytes = "vec", optional, tag = "4")]
    pub key: Option<Vec<u8>>,
    /// Range-query lower bound (inclusive).
    #[prost(bytes = "vec", optional, tag = "5")]
    pub range_min: Option<Vec<u8>>,
    /// Range-query upper bound (inclusive).
    #[prost(bytes = "vec", optional, tag = "6")]
    pub range_max: Option<Vec<u8>>,
    /// Whether to include matched index terms in the response.
    #[prost(bool, optional, tag = "7")]
    pub return_terms: Option<bool>,
    /// Whether to stream results across multiple response frames.
    /// Treated as a hint; this slice always returns a single frame.
    #[prost(bool, optional, tag = "8")]
    pub stream: Option<bool>,
    /// Maximum number of results to return.
    #[prost(uint32, optional, tag = "9")]
    pub max_results: Option<u32>,
    /// Pagination continuation token.
    #[prost(bytes = "vec", optional, tag = "10")]
    pub continuation: Option<Vec<u8>>,
    /// Per-request timeout in milliseconds.
    #[prost(uint32, optional, tag = "11")]
    pub timeout: Option<u32>,
    /// Bucket type.
    #[prost(bytes = "vec", optional, tag = "12")]
    pub r#type: Option<Vec<u8>>,
    /// Server-side regex applied to terms.
    #[prost(bytes = "vec", optional, tag = "13")]
    pub term_regex: Option<Vec<u8>>,
    /// Whether to sort results when paginating.
    #[prost(bool, optional, tag = "14")]
    pub pagination_sort: Option<bool>,
    /// Coverage-plan continuation context.
    #[prost(bytes = "vec", optional, tag = "15")]
    pub cover_context: Option<Vec<u8>>,
    /// Whether the response should include object bodies.
    #[prost(bool, optional, tag = "16")]
    pub return_body: Option<bool>,
}

impl WireValue for RpbIndexReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbIndexReq")
    }
}

/// `RpbIndexResp` -- Riak 2i query response.
///
/// Either `keys` (when `return_terms` was unset) or `results`
/// (when set) carries the matched objects; the other field is empty.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbIndexResp {
    /// Matched object keys.
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub keys: Vec<Vec<u8>>,
    /// Matched (term, key) pairs when `return_terms` was set.
    #[prost(message, repeated, tag = "2")]
    pub results: Vec<RpbPair>,
    /// Pagination continuation token.
    #[prost(bytes = "vec", optional, tag = "3")]
    pub continuation: Option<Vec<u8>>,
    /// Stream-completion marker.
    #[prost(bool, optional, tag = "4")]
    pub done: Option<bool>,
}

impl WireValue for RpbIndexResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbIndexResp")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_code_round_trips() {
        for code in [
            MessageCode::ErrorResp,
            MessageCode::PingReq,
            MessageCode::PingResp,
            MessageCode::ServerInfoReq,
            MessageCode::GetServerInfoResp,
            MessageCode::GetReq,
            MessageCode::GetResp,
            MessageCode::PutReq,
            MessageCode::PutResp,
            MessageCode::DelReq,
            MessageCode::DelResp,
            MessageCode::ListBucketsReq,
            MessageCode::ListBucketsResp,
            MessageCode::ListKeysReq,
            MessageCode::ListKeysResp,
            MessageCode::GetBucketReq,
            MessageCode::GetBucketResp,
            MessageCode::SetBucketReq,
            MessageCode::SetBucketResp,
            MessageCode::IndexReq,
            MessageCode::IndexResp,
        ] {
            let byte = code.as_u8();
            assert_eq!(MessageCode::from_u8(byte).expect("known code"), code);
        }
    }

    #[test]
    fn message_code_rejects_unknown_byte() {
        // Codes 3-6 (client-id management) and 23, 24, 27+ (auth,
        // search, CRDTs, MapReduce) are reserved by Riak but not
        // implemented in this crate yet.
        assert!(MessageCode::from_u8(3).is_err());
        assert!(MessageCode::from_u8(99).is_err());
        assert!(MessageCode::from_u8(255).is_err());
    }

    #[test]
    fn ping_req_round_trips_via_prost() {
        let req = RpbPingReq::default();
        let bytes = req.encode_to_vec();
        assert!(bytes.is_empty(), "ping body must be empty");
        let back = RpbPingReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn ping_resp_round_trips_via_prost() {
        let resp = RpbPingResp::default();
        let bytes = resp.encode_to_vec();
        assert!(bytes.is_empty());
        let back = RpbPingResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn get_req_round_trips_with_optional_fields() {
        let req = RpbGetReq {
            bucket: b"users".to_vec(),
            key: b"alice".to_vec(),
            r: Some(2),
            pr: Some(1),
            basic_quorum: Some(true),
            notfound_ok: Some(false),
            if_modified: Some(b"vclock-bytes".to_vec()),
            head: Some(false),
            deletedvclock: None,
            timeout: Some(5_000),
            sloppy_quorum: None,
            n_val: Some(3),
            r#type: Some(b"default".to_vec()),
        };
        let bytes = req.encode_to_vec();
        let back = RpbGetReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn get_resp_round_trips() {
        let resp = RpbGetResp {
            content: vec![b"value-a".to_vec(), b"value-b".to_vec()],
            vclock: Some(b"vclk".to_vec()),
            unchanged: Some(false),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbGetResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn put_req_round_trips() {
        let req = RpbPutReq {
            bucket: b"users".to_vec(),
            key: Some(b"alice".to_vec()),
            vclock: Some(b"vclk".to_vec()),
            value: b"hello".to_vec(),
            w: Some(2),
            dw: Some(1),
            return_body: Some(true),
            pw: Some(1),
            if_not_modified: None,
            if_none_match: None,
            return_head: None,
            timeout: Some(2_500),
            asis: None,
            sloppy_quorum: None,
            n_val: Some(3),
            r#type: None,
        };
        let bytes = req.encode_to_vec();
        let back = RpbPutReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn put_resp_round_trips() {
        let resp = RpbPutResp {
            content: vec![b"echoed".to_vec()],
            vclock: Some(b"vclk2".to_vec()),
            key: Some(b"alice".to_vec()),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbPutResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn del_req_round_trips() {
        let req = RpbDelReq {
            bucket: b"users".to_vec(),
            key: b"alice".to_vec(),
            rw: Some(2),
            vclock: Some(b"vclk".to_vec()),
            r: Some(2),
            w: Some(2),
            pr: Some(1),
            pw: Some(1),
            dw: Some(1),
            timeout: Some(1_000),
            sloppy_quorum: Some(false),
            n_val: Some(3),
            r#type: None,
        };
        let bytes = req.encode_to_vec();
        let back = RpbDelReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn error_resp_round_trips() {
        let resp = RpbErrorResp {
            errmsg: b"boom".to_vec(),
            errcode: 42,
        };
        let bytes = resp.encode_to_vec();
        let back = RpbErrorResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn encode_is_byte_stable() {
        let req = RpbGetReq {
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            ..RpbGetReq::default()
        };
        let a = req.encode_to_vec();
        let b = req.encode_to_vec();
        assert_eq!(a, b, "prost encode is deterministic for plain fields");
    }

    #[test]
    fn server_info_round_trips() {
        let req = RpbServerInfoReq::default();
        let bytes = req.encode_to_vec();
        assert!(bytes.is_empty(), "server-info request body must be empty");
        let back = RpbServerInfoReq::decode(bytes.as_slice()).expect("decode req");
        assert_eq!(back, req);

        let resp = RpbGetServerInfoResp {
            node: Some(b"riak@127.0.0.1".to_vec()),
            server_version: Some(b"dyn-riak 0.0.1".to_vec()),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbGetServerInfoResp::decode(bytes.as_slice()).expect("decode resp");
        assert_eq!(back, resp);
    }

    #[test]
    fn list_buckets_round_trips() {
        let req = RpbListBucketsReq {
            timeout: Some(1_000),
            stream: Some(false),
            r#type: Some(b"default".to_vec()),
        };
        let bytes = req.encode_to_vec();
        let back = RpbListBucketsReq::decode(bytes.as_slice()).expect("decode req");
        assert_eq!(back, req);

        let resp = RpbListBucketsResp {
            buckets: vec![b"users".to_vec(), b"sessions".to_vec()],
            done: Some(true),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbListBucketsResp::decode(bytes.as_slice()).expect("decode resp");
        assert_eq!(back, resp);
    }

    #[test]
    fn list_keys_round_trips() {
        let req = RpbListKeysReq {
            bucket: b"users".to_vec(),
            timeout: Some(2_500),
            r#type: None,
        };
        let bytes = req.encode_to_vec();
        let back = RpbListKeysReq::decode(bytes.as_slice()).expect("decode req");
        assert_eq!(back, req);

        let resp = RpbListKeysResp {
            keys: vec![b"alice".to_vec(), b"bob".to_vec(), b"carol".to_vec()],
            done: Some(true),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbListKeysResp::decode(bytes.as_slice()).expect("decode resp");
        assert_eq!(back, resp);
    }

    #[test]
    fn get_bucket_round_trips() {
        let req = RpbGetBucketReq {
            bucket: b"users".to_vec(),
            r#type: Some(b"default".to_vec()),
        };
        let bytes = req.encode_to_vec();
        let back = RpbGetBucketReq::decode(bytes.as_slice()).expect("decode req");
        assert_eq!(back, req);

        let resp = RpbGetBucketResp {
            props: Some(RpbBucketProps {
                n_val: Some(3),
                allow_mult: Some(false),
                last_write_wins: Some(true),
                pr: Some(1),
                r: Some(2),
                w: Some(2),
                pw: Some(1),
                dw: Some(1),
                rw: Some(2),
                basic_quorum: Some(true),
                notfound_ok: Some(false),
                backend: Some(b"leveldb".to_vec()),
                search_index: Some(b"users_idx".to_vec()),
                datatype: None,
                consistent: Some(false),
                write_once: Some(false),
                hll_precision: Some(14),
                ..RpbBucketProps::default()
            }),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbGetBucketResp::decode(bytes.as_slice()).expect("decode resp");
        assert_eq!(back, resp);
    }

    #[test]
    fn set_bucket_round_trips() {
        let req = RpbSetBucketReq {
            bucket: b"users".to_vec(),
            props: Some(RpbBucketProps {
                n_val: Some(5),
                allow_mult: Some(true),
                ..RpbBucketProps::default()
            }),
            r#type: Some(b"default".to_vec()),
        };
        let bytes = req.encode_to_vec();
        let back = RpbSetBucketReq::decode(bytes.as_slice()).expect("decode req");
        assert_eq!(back, req);

        let resp = RpbSetBucketResp::default();
        let bytes = resp.encode_to_vec();
        assert!(bytes.is_empty(), "set-bucket response body must be empty");
        let back = RpbSetBucketResp::decode(bytes.as_slice()).expect("decode resp");
        assert_eq!(back, resp);
    }

    #[test]
    fn index_round_trips() {
        let req = RpbIndexReq {
            bucket: b"users".to_vec(),
            index: b"age_int".to_vec(),
            qtype: INDEX_QUERY_TYPE_RANGE,
            key: None,
            range_min: Some(b"18".to_vec()),
            range_max: Some(b"35".to_vec()),
            return_terms: Some(true),
            stream: Some(false),
            max_results: Some(100),
            continuation: Some(b"opaque-token".to_vec()),
            timeout: Some(5_000),
            r#type: Some(b"default".to_vec()),
            term_regex: None,
            pagination_sort: Some(true),
            cover_context: None,
            return_body: Some(false),
        };
        let bytes = req.encode_to_vec();
        let back = RpbIndexReq::decode(bytes.as_slice()).expect("decode req");
        assert_eq!(back, req);

        let resp = RpbIndexResp {
            keys: vec![b"alice".to_vec(), b"bob".to_vec()],
            results: vec![
                RpbPair {
                    key: b"21".to_vec(),
                    value: Some(b"alice".to_vec()),
                },
                RpbPair {
                    key: b"34".to_vec(),
                    value: Some(b"bob".to_vec()),
                },
            ],
            continuation: Some(b"next-page".to_vec()),
            done: Some(true),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbIndexResp::decode(bytes.as_slice()).expect("decode resp");
        assert_eq!(back, resp);
    }

    #[test]
    fn index_eq_query_round_trips() {
        let req = RpbIndexReq {
            bucket: b"users".to_vec(),
            index: b"city_bin".to_vec(),
            qtype: INDEX_QUERY_TYPE_EQ,
            key: Some(b"seattle".to_vec()),
            ..RpbIndexReq::default()
        };
        let bytes = req.encode_to_vec();
        let back = RpbIndexReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
        assert_eq!(back.qtype, 0);
    }
}
