//! Riak MapReduce PBC operation: `RpbMapRedReq` (code 23) and
//! `RpbMapRedResp` (code 24).
//!
//! # Wire shape
//!
//! ```text
//!   message RpbMapRedReq  { required bytes request = 1;
//!                            required bytes content_type = 2; }
//!   message RpbMapRedResp { optional uint32 phase    = 1;
//!                            optional bytes  response = 2;
//!                            optional bool   done     = 3; }
//! ```
//!
//! `request` carries the job description in the body's
//! content-type. The only content-type this slice supports is
//! `application/json`; that is what real Riak clients use in
//! practice and the only shape modelled by [`crate::mapreduce::job`].
//!
//! # Streaming
//!
//! Riak emits one `RpbMapRedResp` per phase that has `keep: true`,
//! plus a final body-less frame with `done: true`. The PBC server
//! ([`crate::server::serve_pbc`]) implements that contract: each
//! per-phase batch produced by [`crate::mapreduce::run_job_streaming`]
//! becomes one `RpbMapRedResp` carrying that phase's JSON payload
//! and `done: false`, followed by a body-less terminator with
//! `done: true`. Executor errors short-circuit to a single
//! `RpbErrorResp` frame.

use prost::Message;

use dyn_encoding::{WireTypeId, WireValue};

/// `RpbMapRedReq` -- submit a MapReduce job.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbMapRedReq {
    /// Encoded job. Content interpretation governed by `content_type`.
    #[prost(bytes = "vec", tag = "1")]
    pub request: Vec<u8>,
    /// MIME type of `request`. Currently only `application/json` is
    /// supported; other content-types surface as `RpbErrorResp`.
    #[prost(bytes = "vec", tag = "2")]
    pub content_type: Vec<u8>,
}

impl WireValue for RpbMapRedReq {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbMapRedReq")
    }
}

/// `RpbMapRedResp` -- one slice of a MapReduce response.
#[derive(Clone, Eq, PartialEq, Message)]
pub struct RpbMapRedResp {
    /// Zero-based phase index for this slice. Absent when `done`
    /// alone is being signalled.
    #[prost(uint32, optional, tag = "1")]
    pub phase: Option<u32>,
    /// Response body slice. Encoding governed by the request
    /// `content_type`. For this slice, JSON.
    #[prost(bytes = "vec", optional, tag = "2")]
    pub response: Option<Vec<u8>>,
    /// `true` when the response is complete.
    #[prost(bool, optional, tag = "3")]
    pub done: Option<bool>,
}

impl WireValue for RpbMapRedResp {
    fn wire_type_id() -> WireTypeId {
        WireTypeId::new("riak.RpbMapRedResp")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn req_round_trips() {
        let req = RpbMapRedReq {
            request: b"{\"inputs\":[],\"query\":[]}".to_vec(),
            content_type: b"application/json".to_vec(),
        };
        let bytes = req.encode_to_vec();
        let back = RpbMapRedReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn resp_round_trips_with_all_fields() {
        let resp = RpbMapRedResp {
            phase: Some(1),
            response: Some(b"[42]".to_vec()),
            done: Some(true),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbMapRedResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn resp_round_trips_with_only_done() {
        let resp = RpbMapRedResp {
            phase: None,
            response: None,
            done: Some(true),
        };
        let bytes = resp.encode_to_vec();
        let back = RpbMapRedResp::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, resp);
    }

    #[test]
    fn empty_req_is_decodable() {
        let req = RpbMapRedReq::default();
        let bytes = req.encode_to_vec();
        let back = RpbMapRedReq::decode(bytes.as_slice()).expect("decode");
        assert_eq!(back, req);
    }

    #[test]
    fn wire_type_ids_are_stable() {
        assert_eq!(
            RpbMapRedReq::wire_type_id(),
            WireTypeId::new("riak.RpbMapRedReq")
        );
        assert_eq!(
            RpbMapRedResp::wire_type_id(),
            WireTypeId::new("riak.RpbMapRedResp")
        );
    }
}
