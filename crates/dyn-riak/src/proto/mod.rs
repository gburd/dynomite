//! Riak wire protocols.
//!
//! `proto::pb` carries the Protocol Buffers transport that production
//! Riak clients use; an HTTP gateway is on the roadmap and will live
//! alongside it under `proto::http`.

pub mod http;
pub mod pb;
