//! Wire-codec wiring for the Riak PBC operation surface.
//!
//! A single [`dyn_encoding::ProtobufCodec`] is built once and stocked
//! with every message type the v0.0.1 slice exchanges on the wire.
//! The codec is then installed into a [`dyn_encoding::CodecRegistry`]
//! under the canonical Riak content-type
//! `"application/x-protobuf"`. Higher layers ask the registry for
//! `"application/x-protobuf"` to encode and decode bodies, and the
//! upcoming HTTP gateway will register additional codecs (JSON,
//! CBOR) under their own content-types into the same registry.

use dyn_encoding::{CodecRegistry, ProtobufCodec};

use crate::proto::pb::messages::{
    RpbDelReq, RpbErrorResp, RpbGetBucketReq, RpbGetBucketResp, RpbGetReq, RpbGetResp,
    RpbGetServerInfoResp, RpbIndexReq, RpbIndexResp, RpbListBucketsReq, RpbListBucketsResp,
    RpbListKeysReq, RpbListKeysResp, RpbPingReq, RpbPingResp, RpbPutReq, RpbPutResp,
    RpbServerInfoReq, RpbSetBucketReq, RpbSetBucketResp,
};

/// Content-type the Riak PBC transport uses unconditionally.
pub const PBC_CONTENT_TYPE: &str = "application/x-protobuf";

/// Registry returned by [`codec_registry`].
///
/// Carries the [`CodecRegistry`] plus the list of registered
/// content-types so callers can introspect the bundle without
/// re-walking the registry.
pub struct PbCodecBundle {
    /// The codec registry holding the protobuf codec.
    pub registry: CodecRegistry,
}

impl PbCodecBundle {
    /// Borrow the configured codec registry.
    #[must_use]
    pub fn registry(&self) -> &CodecRegistry {
        &self.registry
    }
}

/// Build a codec registry with a [`ProtobufCodec`] populated for
/// every message type the Riak PBC transport touches in v0.0.1.
///
/// # Examples
///
/// ```
/// use dyn_riak::proto::pb::{codec_registry, PBC_CONTENT_TYPE};
/// let bundle = codec_registry();
/// assert!(bundle.registry().for_content_type(PBC_CONTENT_TYPE).is_some());
/// ```
#[must_use]
pub fn codec_registry() -> PbCodecBundle {
    let mut codec = ProtobufCodec::new();
    codec
        .register::<RpbErrorResp>()
        .register::<RpbPingReq>()
        .register::<RpbPingResp>()
        .register::<RpbGetReq>()
        .register::<RpbGetResp>()
        .register::<RpbPutReq>()
        .register::<RpbPutResp>()
        .register::<RpbDelReq>()
        .register::<RpbServerInfoReq>()
        .register::<RpbGetServerInfoResp>()
        .register::<RpbListBucketsReq>()
        .register::<RpbListBucketsResp>()
        .register::<RpbListKeysReq>()
        .register::<RpbListKeysResp>()
        .register::<RpbGetBucketReq>()
        .register::<RpbGetBucketResp>()
        .register::<RpbSetBucketReq>()
        .register::<RpbSetBucketResp>()
        .register::<RpbIndexReq>()
        .register::<RpbIndexResp>();

    let mut registry = CodecRegistry::new();
    registry.register(codec);
    PbCodecBundle { registry }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::pb::messages::RpbGetReq;
    use dyn_encoding::WireValue;

    #[test]
    fn registry_has_protobuf_codec() {
        let bundle = codec_registry();
        let codec = bundle
            .registry()
            .for_content_type(PBC_CONTENT_TYPE)
            .expect("protobuf codec installed");
        assert_eq!(codec.content_type(), PBC_CONTENT_TYPE);
    }

    #[test]
    fn registry_round_trips_get_request() {
        let bundle = codec_registry();
        let codec = bundle
            .registry()
            .for_content_type(PBC_CONTENT_TYPE)
            .expect("codec");
        let req = RpbGetReq {
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            ..RpbGetReq::default()
        };
        let bytes = codec.encode(&req).expect("encode");
        let back = codec
            .decode(RpbGetReq::wire_type_id(), &bytes)
            .expect("decode");
        let back = back.as_any().downcast_ref::<RpbGetReq>().expect("downcast");
        assert_eq!(back, &req);
    }
}
