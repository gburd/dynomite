# dyn-encoding

Wire-format codec abstraction for the Riak protocol layer of the
Dynomite Rust port.

The Riak protocol has historically pinned its on-the-wire encoding to
Google Protocol Buffers. Modern Riak deployments and the operator
brief that drives this workspace want to negotiate a richer set of
encodings on a per-request basis, so that clients that already speak
JSON, CBOR, or one of the schema-first contenders (FlatBuffers,
Cap'n Proto, Bebop) can talk to the same server without first
shoehorning their messages through protobuf.

This crate provides the abstraction. It does **not** implement the
Riak protocol itself; that lives in the (forthcoming) `dyn-riak`
crate and consumes `dyn-encoding` to negotiate the wire format on
each connection.

## Scope

Encoding negotiation applies **only** to the Riak protocol layer.
Redis RESP, Memcached ASCII, and the internal DNODE peer-plane
framing are unchanged.

## Trait surface

Two cooperating traits.

```rust
pub trait WireValue: Debug + Send + Sync + 'static {
    fn wire_type_id() -> WireTypeId where Self: Sized;
}

pub trait WireCodec: Send + Sync + 'static {
    fn content_type(&self) -> &'static str;
    fn encode(&self, value: &dyn ErasedWireValue) -> Result<Vec<u8>, CodecError>;
    fn decode(
        &self,
        type_id: WireTypeId,
        bytes: &[u8],
    ) -> Result<Box<dyn ErasedWireValue>, CodecError>;
}
```

`WireTypeId` is a newtype around `&'static str` keyed into the codec
registry. `ErasedWireValue` is the object-safe view over `WireValue`
that codec implementations consume; a blanket impl on every
`T: WireValue` makes the coercion implicit at call sites.

The `CodecRegistry` maps content-type headers (for example
`application/json`) to `&dyn WireCodec`:

```rust
let mut registry = CodecRegistry::with_baseline();
let codec = registry.for_content_type("application/cbor").unwrap();
```

## Baseline codecs

Seven codecs ship in version 0.0.1.

| Module        | Content-type                                  | Backend       |
|---------------|-----------------------------------------------|---------------|
| `json`        | `application/json`                            | `serde_json`  |
| `cbor`        | `application/cbor`                            | `ciborium`    |
| `protobuf`    | `application/x-protobuf`                      | `prost`       |
| `flatbuffers` | `application/octet-stream;schema=flatbuffers` | `flatbuffers` |
| `capnp`       | `application/capnproto`                       | `capnp`       |
| `bebop`       | `application/x-bebop`                         | `bebop`       |
| `bson`        | `application/bson`                            | `bson`        |

All seven follow the same registration pattern: types are attached
to a codec instance through `register::<T>()` before the codec is
installed in the registry. The bound on `T` differs per codec:

* `JsonCodec::register::<T>()`        -- `T: WireValue + Serialize + DeserializeOwned`
* `CborCodec::register::<T>()`        -- `T: WireValue + Serialize + DeserializeOwned`
* `BsonCodec::register::<T>()`        -- `T: WireValue + Serialize + DeserializeOwned`
* `ProtobufCodec::register::<T>()`    -- `T: WireValue + prost::Message + Default`
* `FlatbuffersCodec::register::<T>()` -- `T: FlatbuffersWire`
* `CapnpCodec::register::<T>()`       -- `T: CapnpWire`
* `BebopCodec::register::<T>()`       -- `T: BebopWire`

The protobuf codec deliberately avoids `prost-build`. The three
schema-first newcomers similarly avoid their respective code
generators (`flatc`, `capnpc`, `bebopc`); each defines a small
per-codec trait so the schema and conversion glue live in the
crate that owns the message types (typically `dyn-riak`), not in
this codec abstraction. For testing, hand-rolled fixtures live
inline in each codec module's `tests` block.

## Adding a codec

The trait surface is shaped to host new encodings without churn:

1. Add the upstream crate to `[workspace.dependencies]` in the
   root `Cargo.toml`.
2. Add a feature-flagged or unconditional dependency in
   `crates/dyn-encoding/Cargo.toml`.
3. Create `src/codec/<name>.rs` modeled on the JSON codec module.
   The bound on `register::<T>()` is the new format's native trait
   (`flatbuffers::Follow + Push`, `capnp::Owned`, ...), or a
   crate-defined trait if the upstream API does not fit.
4. Add `pub mod <name>;` to `src/codec/mod.rs` and the corresponding
   `pub use` in `src/lib.rs`.
5. Add the codec to `CodecRegistry::with_baseline` if it should be
   on by default.
6. Mirror the test suite from JSON/CBOR/protobuf: round-trip,
   idempotent encode, unknown-type-id (encode + decode),
   malformed-bytes.

The deferred-codec sketches and the on-the-ground deltas are in
`docs/journal/2026-05-24-dyn-encoding-scaffold.md` and
`docs/journal/2026-05-24-dyn-encoding-deferred-codecs.md`.

## Acknowledgements

The Dynomite Rust port descends from Netflix's Dynomite (C). The
encoding-negotiation idea is informed by the design notes at
<https://gist.github.com/MangaD/77dba2f4c7055b35637fb596c175ffb1>;
see that gist for the comparative analysis of the seven encodings.
