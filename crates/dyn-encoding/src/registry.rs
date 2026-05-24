//! Content-type-keyed registry of [`WireCodec`] implementations.

use std::collections::BTreeMap;

use crate::codec::{
    bebop::BebopCodec, bson::BsonCodec, capnp::CapnpCodec, cbor::CborCodec,
    flatbuffers::FlatbuffersCodec, json::JsonCodec, protobuf::ProtobufCodec,
};
use crate::value::WireCodec;

/// Bag of [`WireCodec`] implementations indexed by content-type.
///
/// The registry is populated at startup (typically through
/// [`Self::with_baseline`] plus per-message-type
/// [`JsonCodec::register`] / [`CborCodec::register`] /
/// [`ProtobufCodec::register`] calls) and then queried per-request via
/// [`Self::for_content_type`] to pick the codec the peer asked for.
pub struct CodecRegistry {
    by_content_type: BTreeMap<&'static str, Box<dyn WireCodec>>,
}

impl CodecRegistry {
    /// Build an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            by_content_type: BTreeMap::new(),
        }
    }

    /// Build a registry pre-populated with the seven baseline
    /// codecs: [`JsonCodec`], [`CborCodec`], [`ProtobufCodec`],
    /// [`FlatbuffersCodec`], [`CapnpCodec`], [`BebopCodec`], and
    /// [`BsonCodec`]. The codecs are returned with empty type
    /// tables; callers attach their message types through the
    /// codec-specific `register` entry points before installing the
    /// codecs into the registry, or use [`Self::register`] to
    /// install codecs that already have their type tables
    /// populated.
    #[must_use]
    pub fn with_baseline() -> Self {
        let mut r = Self::new();
        r.register(JsonCodec::new());
        r.register(CborCodec::new());
        r.register(ProtobufCodec::new());
        r.register(FlatbuffersCodec::new());
        r.register(CapnpCodec::new());
        r.register(BebopCodec::new());
        r.register(BsonCodec::new());
        r
    }

    /// Install a codec under its declared content-type. If a codec
    /// was already installed under the same content-type it is
    /// replaced.
    pub fn register<C: WireCodec + 'static>(&mut self, codec: C) {
        let ct = codec.content_type();
        self.by_content_type.insert(ct, Box::new(codec));
    }

    /// Look up the codec installed under `ct`, or `None` if no codec
    /// claims that content-type.
    #[must_use]
    pub fn for_content_type(&self, ct: &str) -> Option<&dyn WireCodec> {
        self.by_content_type
            .get(ct)
            .map(std::convert::AsRef::as_ref)
    }

    /// Iterate over the content-types currently registered. Useful
    /// for emitting an `Accept`-style negotiation header.
    pub fn content_types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.by_content_type.keys().copied()
    }
}

impl Default for CodecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::json::JsonCodec;
    use crate::value::{WireTypeId, WireValue};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Default, Deserialize, PartialEq, Serialize)]
    struct Probe {
        s: String,
    }

    impl WireValue for Probe {
        fn wire_type_id() -> WireTypeId {
            WireTypeId::new("test.Probe")
        }
    }

    #[test]
    fn register_and_lookup_by_content_type() {
        let mut codec = JsonCodec::new();
        codec.register::<Probe>();

        let mut registry = CodecRegistry::new();
        registry.register(codec);

        let codec = registry
            .for_content_type("application/json")
            .expect("json codec installed");
        assert_eq!(codec.content_type(), "application/json");
    }

    #[test]
    fn unknown_content_type_returns_none() {
        let registry = CodecRegistry::with_baseline();
        assert!(registry.for_content_type("application/x-mystery").is_none());
    }

    #[test]
    fn baseline_registers_all_seven_content_types() {
        let r = CodecRegistry::with_baseline();
        let cts: Vec<&'static str> = r.content_types().collect();
        assert!(cts.contains(&"application/json"));
        assert!(cts.contains(&"application/cbor"));
        assert!(cts.contains(&"application/x-protobuf"));
        assert!(cts.contains(&"application/octet-stream;schema=flatbuffers"));
        assert!(cts.contains(&"application/capnproto"));
        assert!(cts.contains(&"application/x-bebop"));
        assert!(cts.contains(&"application/bson"));
        assert_eq!(cts.len(), 7);
    }

    #[test]
    fn register_replaces_existing_codec_for_same_content_type() {
        // Two distinct JsonCodec instances; the second replaces the
        // first under the shared "application/json" key. We can
        // observe the replacement by encoding a probe value: the
        // first instance has no registrations and would error with
        // UnknownTypeId; the second instance registers Probe and
        // succeeds.
        let empty = JsonCodec::new();
        let mut populated = JsonCodec::new();
        populated.register::<Probe>();

        let mut r = CodecRegistry::new();
        r.register(empty);
        r.register(populated);

        let codec = r.for_content_type("application/json").expect("present");
        let probe = Probe { s: "hello".into() };
        let bytes = codec.encode(&probe).expect("populated codec encodes");
        assert!(!bytes.is_empty());
    }

    #[test]
    fn default_is_empty() {
        let r = CodecRegistry::default();
        assert_eq!(r.content_types().count(), 0);
        assert!(r.for_content_type("application/json").is_none());
    }
}
