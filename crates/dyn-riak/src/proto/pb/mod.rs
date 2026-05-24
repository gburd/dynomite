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
pub mod framer;
pub mod messages;

pub use crate::proto::pb::codec::{codec_registry, PbCodecBundle, PBC_CONTENT_TYPE};
pub use crate::proto::pb::framer::{read_frame, write_frame, Frame, MAX_FRAME_LEN};
pub use crate::proto::pb::messages::{
    MessageCode, RpbDelReq, RpbErrorResp, RpbGetReq, RpbGetResp, RpbPingReq, RpbPingResp,
    RpbPutReq, RpbPutResp,
};
