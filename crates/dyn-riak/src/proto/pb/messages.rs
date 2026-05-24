//! Riak Protocol Buffers message types for the v0.0.1 operation slice.
//!
//! The four request/response pairs mirror the field shapes that Riak
//! KV 2.2.3 publishes in `riak_pb/src/riak_kv.proto` and `riak.proto`.
//! The structs are hand-derived rather than generated through
//! `prost-build` so the crate has no `build.rs` and no protoc
//! dependency. Each field tag matches Riak's published schema so a
//! conforming client interoperates byte-for-byte.
//!
//! # Field-tag stability
//!
//! Field tags must never be renumbered. They are part of the public
//! wire format. New tags get appended only.

use prost::Message;

use dyn_encoding::{WireTypeId, WireValue};

/// Numerical message codes used by Riak's PBC framing.
///
/// The set covered here is the subset the v0.0.1 slice uses
/// (`Error`, `Ping`, `Get`, `Put`, `Del`). Other codes are reserved
/// for follow-up slices and will be rejected with
/// [`crate::error::RiakError::UnknownMessageCode`] until added.
///
/// # Examples
///
/// ```
/// use dyn_riak::proto::pb::MessageCode;
/// assert_eq!(MessageCode::PingReq.as_u8(), 1);
/// assert_eq!(MessageCode::from_u8(11).unwrap(), MessageCode::PutReq);
/// assert!(MessageCode::from_u8(7).is_err());
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
}

impl MessageCode {
    /// Map a raw byte to a [`MessageCode`].
    pub fn from_u8(code: u8) -> Result<Self, u8> {
        Ok(match code {
            0 => Self::ErrorResp,
            1 => Self::PingReq,
            2 => Self::PingResp,
            9 => Self::GetReq,
            10 => Self::GetResp,
            11 => Self::PutReq,
            12 => Self::PutResp,
            13 => Self::DelReq,
            14 => Self::DelResp,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_code_round_trips() {
        for code in [
            MessageCode::ErrorResp,
            MessageCode::PingReq,
            MessageCode::PingResp,
            MessageCode::GetReq,
            MessageCode::GetResp,
            MessageCode::PutReq,
            MessageCode::PutResp,
            MessageCode::DelReq,
            MessageCode::DelResp,
        ] {
            let byte = code.as_u8();
            assert_eq!(MessageCode::from_u8(byte).expect("known code"), code);
        }
    }

    #[test]
    fn message_code_rejects_unknown_byte() {
        assert!(MessageCode::from_u8(7).is_err());
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
}
