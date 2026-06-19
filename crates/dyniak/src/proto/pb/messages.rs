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
/// get-bucket, set-bucket, secondary-index). Message codes outside
/// this set are rejected with
/// [`crate::error::RiakError::UnknownMessageCode`].
///
/// # Examples
///
/// ```
/// use dyniak::proto::pb::MessageCode;
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
    /// `RpbMapRedReq` -- 23. MapReduce job submission.
    MapRedReq = 23,
    /// `RpbMapRedResp` -- 24. MapReduce response slice.
    MapRedResp = 24,
    /// `DynRpbListPeersReq` -- 200. Dynomite admin extension:
    /// request the gossip peer table.
    DynListPeersReq = 200,
    /// `DynRpbListPeersResp` -- 201.
    DynListPeersResp = 201,
    /// `DynRpbClusterJoinReq` -- 202. Dynomite admin extension:
    /// stage a peer-join.
    DynClusterJoinReq = 202,
    /// `DynRpbClusterJoinResp` -- 203.
    DynClusterJoinResp = 203,
    /// `DynRpbClusterLeaveReq` -- 204. Dynomite admin extension:
    /// stage a peer-leave.
    DynClusterLeaveReq = 204,
    /// `DynRpbClusterLeaveResp` -- 205.
    DynClusterLeaveResp = 205,
    /// `DynRpbClusterPlanReq` -- 206. Dynomite admin extension:
    /// fetch staged-but-uncommitted changes.
    DynClusterPlanReq = 206,
    /// `DynRpbClusterPlanResp` -- 207.
    DynClusterPlanResp = 207,
    /// `DynRpbClusterCommitReq` -- 208. Dynomite admin extension:
    /// commit every staged change.
    DynClusterCommitReq = 208,
    /// `DynRpbClusterCommitResp` -- 209.
    DynClusterCommitResp = 209,
    /// `DynRpbAaeStatusReq` -- 220. Dynomite admin extension:
    /// fetch a snapshot of the AAE worker state.
    DynAaeStatusReq = 220,
    /// `DynRpbAaeStatusResp` -- 221.
    DynAaeStatusResp = 221,
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
            23 => Self::MapRedReq,
            24 => Self::MapRedResp,
            200 => Self::DynListPeersReq,
            201 => Self::DynListPeersResp,
            202 => Self::DynClusterJoinReq,
            203 => Self::DynClusterJoinResp,
            204 => Self::DynClusterLeaveReq,
            205 => Self::DynClusterLeaveResp,
            206 => Self::DynClusterPlanReq,
            207 => Self::DynClusterPlanResp,
            208 => Self::DynClusterCommitReq,
            209 => Self::DynClusterCommitResp,
            220 => Self::DynAaeStatusReq,
            221 => Self::DynAaeStatusResp,
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
/// use dyniak::proto::pb::RpbPingReq;
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

/// `RpbLink` -- a typed object-to-object pointer nested inside an
/// [`RpbContent`]. A link names a target object by `(bucket, key)`
/// and classifies the relationship with `tag` (Riak's `riaktag`).
///
/// Every field is `optional bytes`, matching Riak's published
/// schema. The HTTP envelope carries the same triple as
/// [`crate::proto::http::object::HttpLink`] with `String` fields;
/// see [`crate::proto::http::object`] for the byte / string mapping.
///
/// # Examples
///
/// ```
/// use dyniak::proto::pb::RpbLink;
/// let link = RpbLink {
///     bucket: Some(b"people".to_vec()),
///     key: Some(b"bob".to_vec()),
///     tag: Some(b"friend".to_vec()),
/// };
/// assert_eq!(link.tag.as_deref(), Some(b"friend".as_slice()));
/// ```
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbLink {
    /// Target object's bucket. Optional in the schema.
    #[prost(bytes = "vec", optional, tag = "1")]
    pub bucket: Option<Vec<u8>>,
    /// Target object's key. Optional in the schema.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub key: Option<Vec<u8>>,
    /// Relationship tag (Riak's `riaktag`). Optional in the schema.
    #[prost(bytes = "vec", optional, tag = "3")]
    pub tag: Option<Vec<u8>>,
}

impl WireValue for RpbLink {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbLink")
    }
}

/// `RpbContent` -- one sibling value plus its metadata.
///
/// This is Riak's nested content message. A get response carries a
/// repeated list of these (one per sibling); a put request carries
/// one. The full published field set is modelled so a conforming
/// Riak client interoperates byte-for-byte; the fields dyniak
/// populates today are `value`, `content_type`, `links`, and
/// `indexes`. The remaining fields (`charset`, `content_encoding`,
/// `vtag`, `last_mod`, `last_mod_usecs`, `usermeta`, `deleted`)
/// decode and encode as `None` / empty, preserving wire-shape
/// parity at no runtime cost.
///
/// # Examples
///
/// ```
/// use dyniak::proto::pb::{RpbContent, RpbLink};
/// let content = RpbContent {
///     value: b"hello".to_vec(),
///     content_type: Some(b"text/plain".to_vec()),
///     links: vec![RpbLink {
///         bucket: Some(b"people".to_vec()),
///         key: Some(b"bob".to_vec()),
///         tag: Some(b"friend".to_vec()),
///     }],
///     ..RpbContent::default()
/// };
/// assert_eq!(content.value, b"hello");
/// assert_eq!(content.links.len(), 1);
/// ```
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbContent {
    /// Object value bytes. Required in the schema.
    #[prost(bytes = "vec", tag = "1")]
    pub value: Vec<u8>,
    /// Declared media type of [`Self::value`].
    #[prost(bytes = "vec", optional, tag = "2")]
    pub content_type: Option<Vec<u8>>,
    /// Character set of [`Self::value`].
    #[prost(bytes = "vec", optional, tag = "3")]
    pub charset: Option<Vec<u8>>,
    /// Content encoding (for example `gzip`) of [`Self::value`].
    #[prost(bytes = "vec", optional, tag = "4")]
    pub content_encoding: Option<Vec<u8>>,
    /// Sibling version tag assigned by the server.
    #[prost(bytes = "vec", optional, tag = "5")]
    pub vtag: Option<Vec<u8>>,
    /// Typed object-to-object links.
    #[prost(message, repeated, tag = "6")]
    pub links: Vec<RpbLink>,
    /// Last-modification time, whole seconds since the Unix epoch.
    #[prost(uint32, optional, tag = "7")]
    pub last_mod: Option<u32>,
    /// Last-modification time, microsecond remainder.
    #[prost(uint32, optional, tag = "8")]
    pub last_mod_usecs: Option<u32>,
    /// User-supplied metadata `(key, value)` pairs.
    #[prost(message, repeated, tag = "9")]
    pub usermeta: Vec<RpbPair>,
    /// Secondary-index `(name, value)` pairs. `name` ends in `_int`
    /// (integer index) or `_bin` (binary index).
    #[prost(message, repeated, tag = "10")]
    pub indexes: Vec<RpbPair>,
    /// Whether this sibling is a tombstone.
    #[prost(bool, optional, tag = "11")]
    pub deleted: Option<bool>,
}

impl WireValue for RpbContent {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbContent")
    }
}

/// `RpbGetResp` -- response carrying zero or more sibling content
/// records. Each [`RpbContent`] is one sibling value with its
/// metadata (`content_type`, `links`, `indexes`, ...).
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbGetResp {
    /// Sibling content records. Empty when the key is absent.
    #[prost(message, repeated, tag = "1")]
    pub content: Vec<RpbContent>,
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
    /// Object content: value plus its metadata (`content_type`,
    /// `links`, `indexes`, ...). Riak nests all per-object payload
    /// in this single message at tag 4. The links carried here
    /// persist onto the shared storage envelope, so a PBC put can
    /// attach links natively and an HTTP get sees them.
    #[prost(message, optional, tag = "4")]
    pub content: Option<RpbContent>,
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
    /// Sibling content echoed back when the request asked for
    /// `return_body` or `return_head`. Each [`RpbContent`] is one
    /// sibling value with its metadata.
    #[prost(message, repeated, tag = "1")]
    pub content: Vec<RpbContent>,
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
/// listed in `docs/journal/2026-05-24-dyniak-pbc-ops-v1.md` as a
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
///
/// Tags 30 and 31 are Dynomite-specific extensions that carry the
/// numeric `chash_keyfun` and `replication_strategy` selectors.
/// They occupy fresh tag slots (above the canonical Riak schema
/// surface) so a conforming Riak client that does not know about
/// them simply skips the bytes; a Dynomite client honours them
/// per-bucket-type.
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
    /// Hash-key function selector. Carries the canonical Riak
    /// pre-hash strategy: `0 = STD` (hash bucket+key), `1 =
    /// BUCKETONLY` (hash bucket only so every key in the bucket
    /// lands on the same partition), `99 = CUSTOM` (user-defined,
    /// reserved -- not implemented in this slice).
    ///
    /// Riak's published schema models the same field as a
    /// `RpbModFun` message at tag 9. Dynomite carries the choice
    /// as a numeric enum at tag 30 to keep the wire shape
    /// roundtrip-able without modelling the (mod, fun) tuple.
    /// See [`crate::datatypes::keyfun::KeyFun`] for the in-memory
    /// type.
    #[prost(uint32, optional, tag = "30")]
    pub chash_keyfun: Option<u32>,
    /// Replication strategy selector. `0 = TOPOLOGY` (Dynomite's
    /// classic per-DC, per-rack quorum fan-out), `1 = SUCCESSORS`
    /// (Riak-style walk-N-successors).
    ///
    /// See [`crate::replication::ReplicationStrategy`] for the
    /// in-memory type. The default is mode-aware: non-Riak
    /// pools always run `TOPOLOGY`; Riak-mode pools default to
    /// `SUCCESSORS` for newly created bucket-types.
    #[prost(uint32, optional, tag = "31")]
    pub replication_strategy: Option<u32>,
    /// Custom keyfun module id, used only when [`Self::chash_keyfun`]
    /// is `CUSTOM` (99).
    ///
    /// Riak names the user-defined keyfun via a `{modfun, Mod,
    /// Fun}` tuple carried in the `chash_keyfun` `RpbModFun`
    /// message. dyniak realises the user-defined keyfun as an
    /// operator-supplied WASM module and carries the module id in
    /// this dyniak-extension field (tag 32) instead of modelling
    /// the Erlang tuple. The id must name a module registered with
    /// the keyfun WASM store; the bucket-property write path
    /// rejects a `CUSTOM` selection whose module is absent or
    /// unregistered. See
    /// [`crate::datatypes::keyfun::KeyFun::Custom`].
    #[prost(bytes = "vec", optional, tag = "32")]
    pub chash_keyfun_module: Option<Vec<u8>>,
}

/// `chash_keyfun = STD`: hash `<bucket>/<key>` (default).
pub const CHASH_KEYFUN_STD: u32 = 0;
/// `chash_keyfun = BUCKETONLY`: hash `<bucket>` only so every
/// key in the bucket maps to the same partition.
pub const CHASH_KEYFUN_BUCKETONLY: u32 = 1;
/// `chash_keyfun = CUSTOM`: hash input is produced by an
/// operator-supplied WASM module named by the bucket's keyfun
/// module id. Decoded to [`crate::datatypes::keyfun::KeyFun::Custom`]
/// and routed through the keyfun WASM store by
/// [`crate::router::BucketRouter`].
pub const CHASH_KEYFUN_CUSTOM: u32 = 99;

/// `replication_strategy = TOPOLOGY`: Dynomite's per-DC, per-
/// rack quorum fan-out (default outside Riak mode).
pub const REPLICATION_STRATEGY_TOPOLOGY: u32 = 0;
/// `replication_strategy = SUCCESSORS`: walk-N-successors on
/// the token ring (Riak-style; default for new Riak-mode bucket
/// types).
pub const REPLICATION_STRATEGY_SUCCESSORS: u32 = 1;

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
        // Codes 3-6 (client-id management) and 27+ (auth, search,
        // CRDTs) are reserved by Riak but not implemented in this
        // crate yet. Codes 23 and 24 (MapReduce) are now
        // recognised by this crate.
        assert!(MessageCode::from_u8(3).is_err());
        assert!(MessageCode::from_u8(99).is_err());
        assert!(MessageCode::from_u8(255).is_err());
    }

    #[test]
    fn message_code_round_trips_mapred() {
        for code in [MessageCode::MapRedReq, MessageCode::MapRedResp] {
            assert_eq!(MessageCode::from_u8(code.as_u8()).expect("known"), code);
        }
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
            content: vec![
                RpbContent {
                    value: b"value-a".to_vec(),
                    content_type: Some(b"text/plain".to_vec()),
                    ..RpbContent::default()
                },
                RpbContent {
                    value: b"value-b".to_vec(),
                    ..RpbContent::default()
                },
            ],
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
            content: Some(RpbContent {
                value: b"hello".to_vec(),
                content_type: Some(b"application/json".to_vec()),
                indexes: vec![RpbPair {
                    key: b"age_int".to_vec(),
                    value: Some(b"42".to_vec()),
                }],
                ..RpbContent::default()
            }),
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
    fn legacy_flat_value_put_migration() {
        // Migration note: before this slice, RpbPutReq carried a flat
        // `value: bytes` at tag 4 and a top-level `indexes` at tag
        // 100. Tag 4 is now a nested `RpbContent` message (wire type
        // 2, the same as the old bytes field). A legacy client that
        // sends `value: bytes` at tag 4 therefore has those bytes
        // re-interpreted as an `RpbContent` submessage on decode.
        //
        // Build the legacy wire shape explicitly and confirm the new
        // decoder treats tag-4 bytes as a nested content message.
        #[derive(Clone, PartialEq, ::prost::Message)]
        struct LegacyPutReq {
            #[prost(bytes = "vec", tag = "1")]
            bucket: Vec<u8>,
            #[prost(bytes = "vec", optional, tag = "2")]
            key: Option<Vec<u8>>,
            #[prost(bytes = "vec", tag = "4")]
            value: Vec<u8>,
            #[prost(message, repeated, tag = "100")]
            indexes: Vec<RpbPair>,
        }

        // A legacy value whose bytes happen NOT to be a valid
        // protobuf message fails to decode under the new schema. This
        // is the documented, accepted break: dyniak is pre-1.0 and
        // the prior slice flagged the RpbContent refactor as the
        // planned breaking follow-up.
        let legacy_bad = LegacyPutReq {
            bucket: b"users".to_vec(),
            key: Some(b"alice".to_vec()),
            value: vec![0xff, 0xff, 0xff],
            indexes: Vec::new(),
        };
        let bytes = legacy_bad.encode_to_vec();
        assert!(
            RpbPutReq::decode(bytes.as_slice()).is_err(),
            "legacy flat value that is not a valid submessage is rejected"
        );

        // The top-level tag-100 indexes shim is gone; bytes carrying
        // a tag-100 field are skipped by prost (unknown field), so an
        // index set sent the old way is silently dropped under the
        // new schema. A migrating client must move indexes into
        // `content.indexes`.
        let new_shape = RpbPutReq {
            bucket: b"users".to_vec(),
            key: Some(b"alice".to_vec()),
            content: Some(RpbContent {
                value: b"hello".to_vec(),
                indexes: vec![RpbPair {
                    key: b"age_int".to_vec(),
                    value: Some(b"42".to_vec()),
                }],
                ..RpbContent::default()
            }),
            ..RpbPutReq::default()
        };
        let back = RpbPutReq::decode(new_shape.encode_to_vec().as_slice()).expect("decode");
        let content = back.content.expect("content present");
        assert_eq!(content.value, b"hello");
        assert_eq!(content.indexes.len(), 1);
    }

    #[test]
    fn put_resp_round_trips() {
        let resp = RpbPutResp {
            content: vec![RpbContent {
                value: b"echoed".to_vec(),
                ..RpbContent::default()
            }],
            vclock: Some(b"vclk2".to_vec()),
            key: Some(b"alice".to_vec()),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbPutResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn rpb_link_round_trips() {
        let link = RpbLink {
            bucket: Some(b"people".to_vec()),
            key: Some(b"bob".to_vec()),
            tag: Some(b"friend".to_vec()),
        };
        let bytes = link.encode_to_vec();
        let back = RpbLink::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, link);

        // Empty link (every field absent) encodes to nothing and
        // round-trips to the default.
        let empty = RpbLink::default();
        assert!(empty.encode_to_vec().is_empty());
        assert_eq!(RpbLink::decode([].as_slice()).expect("decode"), empty);
    }

    #[test]
    fn rpb_content_round_trips_with_links_and_indexes() {
        let content = RpbContent {
            value: b"payload".to_vec(),
            content_type: Some(b"text/plain".to_vec()),
            links: vec![
                RpbLink {
                    bucket: Some(b"people".to_vec()),
                    key: Some(b"bob".to_vec()),
                    tag: Some(b"friend".to_vec()),
                },
                RpbLink {
                    bucket: Some(b"work".to_vec()),
                    key: Some(b"acme".to_vec()),
                    tag: Some(b"employer".to_vec()),
                },
            ],
            indexes: vec![RpbPair {
                key: b"age_int".to_vec(),
                value: Some(b"42".to_vec()),
            }],
            ..RpbContent::default()
        };
        let bytes = content.encode_to_vec();
        let back = RpbContent::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, content);
        assert_eq!(back.links.len(), 2);
    }

    #[test]
    fn rpb_content_round_trips_empty_links() {
        let content = RpbContent {
            value: b"v".to_vec(),
            ..RpbContent::default()
        };
        let bytes = content.encode_to_vec();
        let back = RpbContent::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, content);
        assert!(back.links.is_empty());
    }

    #[test]
    fn rpb_content_carries_full_field_set() {
        // Every published field is present so a conforming Riak
        // client round-trips byte-for-byte.
        let content = RpbContent {
            value: b"v".to_vec(),
            content_type: Some(b"text/plain".to_vec()),
            charset: Some(b"utf-8".to_vec()),
            content_encoding: Some(b"gzip".to_vec()),
            vtag: Some(b"1a2b".to_vec()),
            links: vec![RpbLink {
                bucket: Some(b"b".to_vec()),
                key: Some(b"k".to_vec()),
                tag: Some(b"t".to_vec()),
            }],
            last_mod: Some(1_700_000_000),
            last_mod_usecs: Some(123),
            usermeta: vec![RpbPair {
                key: b"meta".to_vec(),
                value: Some(b"data".to_vec()),
            }],
            indexes: vec![RpbPair {
                key: b"age_int".to_vec(),
                value: Some(b"7".to_vec()),
            }],
            deleted: Some(false),
        };
        let bytes = content.encode_to_vec();
        let back = RpbContent::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, content);
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
            server_version: Some(b"dyniak 0.0.1".to_vec()),
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
    fn bucket_props_chash_keyfun_round_trips() {
        let props = RpbBucketProps {
            n_val: Some(3),
            chash_keyfun: Some(CHASH_KEYFUN_BUCKETONLY),
            ..RpbBucketProps::default()
        };
        let bytes = props.encode_to_vec();
        let back = RpbBucketProps::decode(bytes.as_slice()).expect("decode props");
        assert_eq!(back.chash_keyfun, Some(CHASH_KEYFUN_BUCKETONLY));
        assert_eq!(back.n_val, Some(3));
        assert_eq!(back, props);
    }

    #[test]
    fn bucket_props_replication_strategy_round_trips() {
        let props = RpbBucketProps {
            replication_strategy: Some(REPLICATION_STRATEGY_SUCCESSORS),
            n_val: Some(3),
            ..RpbBucketProps::default()
        };
        let bytes = props.encode_to_vec();
        let back = RpbBucketProps::decode(bytes.as_slice()).expect("decode props");
        assert_eq!(
            back.replication_strategy,
            Some(REPLICATION_STRATEGY_SUCCESSORS)
        );
        assert_eq!(back, props);
    }

    #[test]
    fn bucket_props_default_omits_new_selectors() {
        // Pre-existing tests must continue to round-trip when the
        // `chash_keyfun` and `replication_strategy` fields are
        // left unset; the encoder must not emit bytes for them.
        let props = RpbBucketProps {
            n_val: Some(3),
            ..RpbBucketProps::default()
        };
        assert_eq!(props.chash_keyfun, None);
        assert_eq!(props.replication_strategy, None);
        let bytes = props.encode_to_vec();
        let back = RpbBucketProps::decode(bytes.as_slice()).expect("decode props");
        assert_eq!(back, props);
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

// ---- Dynomite cluster admin extension --------------------------------------
//
// The Dynomite admin RPCs use message codes in the 200-209 range
// to avoid colliding with any Riak code reserved for future use.
// Each request / response pair is hand-rolled `prost::Message` with
// stable field tags so a `dyn-admin` client and a `dyniak` server
// agree byte-for-byte on every wire field.

/// `DynRpbPeerInfo` -- one peer's view as returned by
/// [`DynRpbListPeersResp`] and embedded in [`DynRpbStagedChange`].
///
/// The token list is rendered as decimal-string bytes so the
/// admin client can echo it back unchanged in the human and JSON
/// renderings without round-tripping through `u32`.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbPeerInfo {
    /// Peer index (zero is the local node by convention).
    #[prost(uint32, tag = "1")]
    pub idx: u32,
    /// Datacenter name.
    #[prost(bytes = "vec", tag = "2")]
    pub dc: Vec<u8>,
    /// Rack name.
    #[prost(bytes = "vec", tag = "3")]
    pub rack: Vec<u8>,
    /// Hostname or IP.
    #[prost(bytes = "vec", tag = "4")]
    pub host: Vec<u8>,
    /// TCP port.
    #[prost(uint32, tag = "5")]
    pub port: u32,
    /// Token list, each entry rendered as decimal-string bytes
    /// (matches what the engine emits in seed-list blobs).
    #[prost(bytes = "vec", repeated, tag = "6")]
    pub tokens: Vec<Vec<u8>>,
    /// Lifecycle state name (`UNKNOWN`, `JOINING`, `NORMAL`,
    /// `STANDBY`, `DOWN`, `RESET`, `LEAVING`).
    #[prost(bytes = "vec", tag = "7")]
    pub state: Vec<u8>,
    /// True for the local peer.
    #[prost(bool, tag = "8")]
    pub is_local: bool,
    /// True when the peer expects an encrypted dnode link.
    /// Optional: defaults to false.
    #[prost(bool, optional, tag = "9")]
    pub is_secure: Option<bool>,
}

impl WireValue for DynRpbPeerInfo {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbPeerInfo")
    }
}

/// `DynRpbStagedChange` -- one entry in the
/// [`DynRpbClusterPlanResp`] list and the body of every
/// [`DynRpbClusterJoinResp`] / [`DynRpbClusterLeaveResp`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbStagedChange {
    /// Direction of the change. `1` = Add, `2` = Remove. Other
    /// values are reserved.
    #[prost(uint32, tag = "1")]
    pub kind: u32,
    /// Peer index targeted by a Remove change.
    #[prost(uint32, optional, tag = "2")]
    pub peer_idx: Option<u32>,
    /// Peer description carried by an Add change.
    #[prost(message, optional, tag = "3")]
    pub peer: Option<DynRpbPeerInfo>,
}

impl WireValue for DynRpbStagedChange {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbStagedChange")
    }
}

/// `DynRpbStagedChange::kind` value for an Add change.
pub const DYN_STAGED_CHANGE_ADD: u32 = 1;
/// `DynRpbStagedChange::kind` value for a Remove change.
pub const DYN_STAGED_CHANGE_REMOVE: u32 = 2;

/// `DynRpbListPeersReq` -- empty request body. Sent under
/// message code 200.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbListPeersReq {}

impl WireValue for DynRpbListPeersReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbListPeersReq")
    }
}

/// `DynRpbListPeersResp` -- response carrying every peer the
/// gossip layer has seen.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbListPeersResp {
    /// Peer entries.
    #[prost(message, repeated, tag = "1")]
    pub peers: Vec<DynRpbPeerInfo>,
}

impl WireValue for DynRpbListPeersResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbListPeersResp")
    }
}

/// `DynRpbClusterJoinReq` -- stage a peer-join.
///
/// The `target` field is a `host:port` string the server parses
/// as a [`std::net::SocketAddr`] before staging the change.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterJoinReq {
    /// `host:port` of the peer to join.
    #[prost(bytes = "vec", tag = "1")]
    pub target: Vec<u8>,
}

impl WireValue for DynRpbClusterJoinReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterJoinReq")
    }
}

/// `DynRpbClusterJoinResp` -- response carrying the staged
/// [`DynRpbStagedChange`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterJoinResp {
    /// The staged change.
    #[prost(message, optional, tag = "1")]
    pub change: Option<DynRpbStagedChange>,
}

impl WireValue for DynRpbClusterJoinResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterJoinResp")
    }
}

/// `DynRpbClusterLeaveReq` -- stage a peer-leave.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterLeaveReq {
    /// Peer index to remove.
    #[prost(uint32, tag = "1")]
    pub peer_idx: u32,
}

impl WireValue for DynRpbClusterLeaveReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterLeaveReq")
    }
}

/// `DynRpbClusterLeaveResp` -- response carrying the staged
/// [`DynRpbStagedChange`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterLeaveResp {
    /// The staged change.
    #[prost(message, optional, tag = "1")]
    pub change: Option<DynRpbStagedChange>,
}

impl WireValue for DynRpbClusterLeaveResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterLeaveResp")
    }
}

/// `DynRpbClusterPlanReq` -- empty request body. Sent under
/// message code 206.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterPlanReq {}

impl WireValue for DynRpbClusterPlanReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterPlanReq")
    }
}

/// `DynRpbClusterPlanResp` -- staged-but-uncommitted change list.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterPlanResp {
    /// Pending staged changes, in order of staging.
    #[prost(message, repeated, tag = "1")]
    pub changes: Vec<DynRpbStagedChange>,
}

impl WireValue for DynRpbClusterPlanResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterPlanResp")
    }
}

/// `DynRpbClusterCommitReq` -- empty request body. Sent under
/// message code 208.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterCommitReq {}

impl WireValue for DynRpbClusterCommitReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterCommitReq")
    }
}

/// `DynRpbClusterCommitResp` -- response carrying the number of
/// changes that were applied.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbClusterCommitResp {
    /// Count of changes that were applied.
    #[prost(uint32, tag = "1")]
    pub applied: u32,
}

impl WireValue for DynRpbClusterCommitResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbClusterCommitResp")
    }
}

/// `DynRpbAaePeerStatus` -- per-peer slice of the AAE status
/// snapshot returned by [`DynRpbAaeStatusResp`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbAaePeerStatus {
    /// Peer index.
    #[prost(uint32, tag = "1")]
    pub peer_idx: u32,
    /// Datacenter of the peer.
    #[prost(bytes = "vec", tag = "2")]
    pub dc: Vec<u8>,
    /// Rack of the peer.
    #[prost(bytes = "vec", tag = "3")]
    pub rack: Vec<u8>,
    /// Wall-clock seconds (UNIX epoch) when this peer's most
    /// recent exchange completed. Zero means "never".
    #[prost(uint64, tag = "4")]
    pub last_exchange_unix: u64,
    /// Cumulative count of divergent keys observed since the
    /// last full sweep finished.
    #[prost(uint64, tag = "5")]
    pub divergent_keys_since_last_full_sweep: u64,
    /// Cumulative count of repair tasks dispatched against
    /// this peer.
    #[prost(uint64, tag = "6")]
    pub repair_dispatched_total: u64,
}

impl WireValue for DynRpbAaePeerStatus {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbAaePeerStatus")
    }
}

/// `DynRpbAaeStatusReq` -- empty request body. Sent under
/// message code 220.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbAaeStatusReq {}

impl WireValue for DynRpbAaeStatusReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbAaeStatusReq")
    }
}

/// `DynRpbAaeStatusResp` -- snapshot of the AAE worker's
/// state. Returned under message code 221 in response to
/// [`DynRpbAaeStatusReq`].
#[derive(Clone, Eq, PartialEq, Message)]
pub struct DynRpbAaeStatusResp {
    /// One row per peer the AAE worker exchanges with.
    #[prost(message, repeated, tag = "1")]
    pub peers: Vec<DynRpbAaePeerStatus>,
    /// Path of the local snapshot file. Empty when the
    /// embedding has not configured a snapshot path.
    #[prost(bytes = "vec", tag = "2")]
    pub snapshot_path: Vec<u8>,
    /// Wall-clock seconds (UNIX epoch) of the most recent
    /// successful snapshot save. Zero means "never".
    #[prost(uint64, tag = "3")]
    pub snapshot_last_save_unix: u64,
    /// Wall-clock seconds (UNIX epoch) of the most recent
    /// successful snapshot load. Zero means "never".
    #[prost(uint64, tag = "4")]
    pub snapshot_last_load_unix: u64,
    /// Cumulative count of snapshot writes.
    #[prost(uint64, tag = "5")]
    pub snapshot_save_total: u64,
    /// Cumulative count of snapshot loads.
    #[prost(uint64, tag = "6")]
    pub snapshot_load_total: u64,
    /// Cumulative count of corrupted-snapshot rejections.
    #[prost(uint64, tag = "7")]
    pub snapshot_corruption_total: u64,
    /// Number of top-level time buckets in the local tree.
    #[prost(uint32, tag = "8")]
    pub tree_n_time_buckets: u32,
    /// Number of bottom-level segments per time bucket.
    #[prost(uint32, tag = "9")]
    pub tree_n_segments: u32,
    /// Width of one time bucket, in seconds.
    #[prost(uint64, tag = "10")]
    pub tree_time_window_seconds: u64,
    /// Rough estimate of the local tree's resident memory,
    /// in bytes.
    #[prost(uint64, tag = "11")]
    pub tree_memory_estimate_bytes: u64,
}

impl WireValue for DynRpbAaeStatusResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("dynomite.DynRpbAaeStatusResp")
    }
}

#[cfg(test)]
mod admin_tests {
    use super::*;

    #[test]
    fn admin_message_codes_round_trip() {
        for code in [
            MessageCode::DynListPeersReq,
            MessageCode::DynListPeersResp,
            MessageCode::DynClusterJoinReq,
            MessageCode::DynClusterJoinResp,
            MessageCode::DynClusterLeaveReq,
            MessageCode::DynClusterLeaveResp,
            MessageCode::DynClusterPlanReq,
            MessageCode::DynClusterPlanResp,
            MessageCode::DynClusterCommitReq,
            MessageCode::DynClusterCommitResp,
            MessageCode::DynAaeStatusReq,
            MessageCode::DynAaeStatusResp,
        ] {
            let byte = code.as_u8();
            assert_eq!(MessageCode::from_u8(byte).expect("known"), code);
        }
    }

    #[test]
    fn list_peers_req_is_empty() {
        let req = DynRpbListPeersReq::default();
        assert!(req.encode_to_vec().is_empty());
    }

    #[test]
    fn peer_info_round_trips() {
        let info = DynRpbPeerInfo {
            idx: 1,
            dc: b"dc1".to_vec(),
            rack: b"r1".to_vec(),
            host: b"127.0.0.1".to_vec(),
            port: 8101,
            tokens: vec![b"42".to_vec(), b"7777".to_vec()],
            state: b"NORMAL".to_vec(),
            is_local: false,
            is_secure: Some(true),
        };
        let bytes = info.encode_to_vec();
        let back = DynRpbPeerInfo::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, info);
    }

    #[test]
    fn list_peers_resp_round_trips() {
        let resp = DynRpbListPeersResp {
            peers: vec![
                DynRpbPeerInfo {
                    idx: 0,
                    dc: b"dc1".to_vec(),
                    rack: b"r1".to_vec(),
                    host: b"10.0.0.1".to_vec(),
                    port: 8101,
                    tokens: vec![b"0".to_vec()],
                    state: b"NORMAL".to_vec(),
                    is_local: true,
                    is_secure: None,
                },
                DynRpbPeerInfo {
                    idx: 1,
                    dc: b"dc1".to_vec(),
                    rack: b"r1".to_vec(),
                    host: b"10.0.0.2".to_vec(),
                    port: 8101,
                    tokens: vec![b"2147483648".to_vec()],
                    state: b"DOWN".to_vec(),
                    is_local: false,
                    is_secure: None,
                },
            ],
        };
        let bytes = resp.encode_to_vec();
        let back = DynRpbListPeersResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn join_req_resp_round_trips() {
        let req = DynRpbClusterJoinReq {
            target: b"127.0.0.1:8103".to_vec(),
        };
        let back = DynRpbClusterJoinReq::decode(req.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, req);
        let resp = DynRpbClusterJoinResp {
            change: Some(DynRpbStagedChange {
                kind: DYN_STAGED_CHANGE_ADD,
                peer_idx: None,
                peer: Some(DynRpbPeerInfo {
                    idx: 0,
                    dc: b"dc1".to_vec(),
                    rack: b"r1".to_vec(),
                    host: b"127.0.0.1".to_vec(),
                    port: 8103,
                    tokens: vec![b"123".to_vec()],
                    state: b"".to_vec(),
                    is_local: false,
                    is_secure: Some(false),
                }),
            }),
        };
        let back = DynRpbClusterJoinResp::decode(resp.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn leave_req_resp_round_trips() {
        let req = DynRpbClusterLeaveReq { peer_idx: 5 };
        let back = DynRpbClusterLeaveReq::decode(req.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, req);
        let resp = DynRpbClusterLeaveResp {
            change: Some(DynRpbStagedChange {
                kind: DYN_STAGED_CHANGE_REMOVE,
                peer_idx: Some(5),
                peer: None,
            }),
        };
        let back = DynRpbClusterLeaveResp::decode(resp.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn plan_round_trips() {
        let req = DynRpbClusterPlanReq::default();
        assert!(req.encode_to_vec().is_empty());
        let resp = DynRpbClusterPlanResp {
            changes: vec![
                DynRpbStagedChange {
                    kind: DYN_STAGED_CHANGE_ADD,
                    peer_idx: None,
                    peer: Some(DynRpbPeerInfo {
                        idx: 0,
                        dc: b"d".to_vec(),
                        rack: b"r".to_vec(),
                        host: b"h".to_vec(),
                        port: 1,
                        tokens: vec![b"1".to_vec()],
                        state: b"".to_vec(),
                        is_local: false,
                        is_secure: None,
                    }),
                },
                DynRpbStagedChange {
                    kind: DYN_STAGED_CHANGE_REMOVE,
                    peer_idx: Some(2),
                    peer: None,
                },
            ],
        };
        let back = DynRpbClusterPlanResp::decode(resp.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn commit_round_trips() {
        let req = DynRpbClusterCommitReq::default();
        assert!(req.encode_to_vec().is_empty());
        let resp = DynRpbClusterCommitResp { applied: 7 };
        let back =
            DynRpbClusterCommitResp::decode(resp.encode_to_vec().as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn aae_status_req_is_empty() {
        let req = DynRpbAaeStatusReq::default();
        assert!(req.encode_to_vec().is_empty());
    }

    #[test]
    fn aae_status_resp_round_trips() {
        let resp = DynRpbAaeStatusResp {
            peers: vec![
                DynRpbAaePeerStatus {
                    peer_idx: 0,
                    dc: b"dc1".to_vec(),
                    rack: b"rA".to_vec(),
                    last_exchange_unix: 1_700_000_000,
                    divergent_keys_since_last_full_sweep: 12,
                    repair_dispatched_total: 9,
                },
                DynRpbAaePeerStatus {
                    peer_idx: 1,
                    dc: b"dc1".to_vec(),
                    rack: b"rB".to_vec(),
                    last_exchange_unix: 0,
                    divergent_keys_since_last_full_sweep: 0,
                    repair_dispatched_total: 0,
                },
            ],
            snapshot_path: b"/var/lib/dynomite/aae/tree.snapshot".to_vec(),
            snapshot_last_save_unix: 1_700_000_300,
            snapshot_last_load_unix: 1_700_000_100,
            snapshot_save_total: 5,
            snapshot_load_total: 1,
            snapshot_corruption_total: 0,
            tree_n_time_buckets: 24,
            tree_n_segments: 1024,
            tree_time_window_seconds: 3600,
            tree_memory_estimate_bytes: 4096,
        };
        let bytes = resp.encode_to_vec();
        let back = DynRpbAaeStatusResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn every_message_reports_its_wire_type_id() {
        // Each WireValue impl maps its type to a stable, unique
        // protobuf type name. Asserting them all in one place covers
        // every wire_type_id() body and guards against an accidental
        // duplicate (which would mis-route a frame through the codec).
        let ids = [
            RpbErrorResp::wire_type_id(),
            RpbPingReq::wire_type_id(),
            RpbPingResp::wire_type_id(),
            RpbGetReq::wire_type_id(),
            RpbLink::wire_type_id(),
            RpbContent::wire_type_id(),
            RpbGetResp::wire_type_id(),
            RpbPutReq::wire_type_id(),
            RpbPutResp::wire_type_id(),
            RpbDelReq::wire_type_id(),
            RpbServerInfoReq::wire_type_id(),
            RpbGetServerInfoResp::wire_type_id(),
            RpbListBucketsReq::wire_type_id(),
            RpbListBucketsResp::wire_type_id(),
            RpbListKeysReq::wire_type_id(),
            RpbListKeysResp::wire_type_id(),
            RpbBucketProps::wire_type_id(),
            RpbGetBucketReq::wire_type_id(),
            RpbGetBucketResp::wire_type_id(),
            RpbSetBucketReq::wire_type_id(),
            RpbSetBucketResp::wire_type_id(),
            RpbPair::wire_type_id(),
            RpbIndexReq::wire_type_id(),
            RpbIndexResp::wire_type_id(),
            DynRpbPeerInfo::wire_type_id(),
            DynRpbStagedChange::wire_type_id(),
            DynRpbListPeersReq::wire_type_id(),
            DynRpbListPeersResp::wire_type_id(),
            DynRpbClusterJoinReq::wire_type_id(),
            DynRpbClusterJoinResp::wire_type_id(),
            DynRpbClusterLeaveReq::wire_type_id(),
            DynRpbClusterLeaveResp::wire_type_id(),
            DynRpbClusterPlanReq::wire_type_id(),
            DynRpbClusterPlanResp::wire_type_id(),
            DynRpbClusterCommitReq::wire_type_id(),
            DynRpbClusterCommitResp::wire_type_id(),
            DynRpbAaePeerStatus::wire_type_id(),
            DynRpbAaeStatusReq::wire_type_id(),
            DynRpbAaeStatusResp::wire_type_id(),
        ];
        // Spot-check a couple of representative names.
        assert_eq!(RpbLink::wire_type_id(), WireTypeId::new("riak.RpbLink"));
        assert_eq!(
            RpbContent::wire_type_id(),
            WireTypeId::new("riak.RpbContent")
        );
        assert_eq!(
            DynRpbAaeStatusResp::wire_type_id(),
            WireTypeId::new("dynomite.DynRpbAaeStatusResp")
        );
        // No two message types share a wire type id.
        let mut seen = std::collections::HashSet::new();
        for id in ids {
            assert!(seen.insert(id), "duplicate wire type id: {id}");
        }
        assert_eq!(seen.len(), ids.len());
    }
}
