# 2026-05-24 dyn-encoding scaffold

## Task

Stand up a new `dyn-encoding` crate to host the per-request wire-format
codec abstraction the (forthcoming) `dyn-riak` crate will consume.
Ship the trait surface plus three baseline implementations: protobuf,
JSON, and CBOR. Defer FlatBuffers, Cap'n Proto, Bebop, and BSON.

## Design

### Trait surface

Two cooperating object-safe traits plus a registry:

* `WireValue: Debug + Send + Sync + 'static` -- per-message-type
  trait. Provides a stable `WireTypeId` (newtype over
  `&'static str`).
* `ErasedWireValue: Debug + Send + Sync + 'static` -- object-safe
  view with a blanket impl on every `T: WireValue`. Carries
  `type_id() -> WireTypeId` and `as_any() -> &dyn Any` so codec
  implementations can downcast.
* `WireCodec: Send + Sync + 'static` -- object-safe codec trait.
  Methods take/return `&dyn ErasedWireValue` /
  `Box<dyn ErasedWireValue>`. Errors flow through a single
  `CodecError` enum so the registry can return `&dyn WireCodec`.
* `CodecRegistry` -- BTreeMap keyed by content-type
  (`&'static str`) holding `Box<dyn WireCodec>`.

### Departure from the brief sketch

The brief sketches `WireCodec` with an associated `Error` type. That
is mutually inconsistent with the registry's
`for_content_type(...) -> Option<&dyn WireCodec>` return type, since
trait objects with associated types require a concrete type
parameter at the use site. The scaffold collapses all per-codec
errors into a single `CodecError` enum that wraps the underlying
serializer error as `Box<dyn std::error::Error + Send + Sync>`. The
brief explicitly licenses refining the sketch.

`WireValue` and `ErasedWireValue` were also tightened to require
`Debug` so error paths can use `Result::expect_err` and so that
diagnostics emitted from codec errors can include the value. This
is a reasonable bound for any structured wire-message type.

### Per-codec type registration

Schema-first codecs (protobuf today, FlatBuffers / Cap'n Proto /
Bebop tomorrow) need to know the concrete message type to reify
bytes back into a value. Schema-less codecs (JSON / CBOR / BSON)
could in principle dispatch through `erased_serde`, but that would
diverge from the schema-first path and add an extra dependency that
is not sanctioned by the brief.

Instead, every codec carries an internal per-`WireTypeId` registry
of encoder/decoder closures populated through a codec-specific
`register::<T>()` entry point with a per-codec bound:

* `JsonCodec::register::<T>()`     -- `T: WireValue + Serialize + DeserializeOwned`
* `CborCodec::register::<T>()`     -- `T: WireValue + Serialize + DeserializeOwned`
* `ProtobufCodec::register::<T>()` -- `T: WireValue + prost::Message + Default`

This single shape makes the four deferred codecs mechanical to add.

## Dependencies

Three new entries in `[workspace.dependencies]`:

| Crate         | Pin    | Lock       | Rationale |
|---------------|--------|------------|-----------|
| `serde_json`  | `1.0`  | already at `1.0.149` | Already in the workspace; reused, no version bump. |
| `ciborium`    | `0.2`  | already at `0.2.2` (transitive via tonic) | Picked over `serde_cbor` because the latter is unmaintained. The lock-file resolved version is unchanged. |
| `prost`       | `0.13` | already at `0.13.5` (transitive via opentelemetry-proto / tonic) | `prost` is the de-facto Rust protobuf implementation. The lock-file resolved version is unchanged. |

No new entries land in `Cargo.lock` because all three crates were
already pulled by the existing OTel/Tonic graph. The risk of
version-skew is therefore zero today; future bumps will drag the
direct and transitive consumers in lockstep.

## Layout

```
crates/dyn-encoding/
  Cargo.toml
  README.md
  src/
    lib.rs
    error.rs
    value.rs
    registry.rs
    codec/
      mod.rs
      json.rs
      cbor.rs
      protobuf.rs
```

## Tests

21 unit tests + 1 doctest, all passing under `cargo nextest run -p dyn-encoding`:

* Per-codec (JSON, CBOR, protobuf):
  * `round_trip_recovers_value`
  * `idempotent_encode_is_byte_equal`
  * `unregistered_type_returns_unknown_type_id_on_encode`
  * `unregistered_type_returns_unknown_type_id_on_decode` (JSON, CBOR)
  * `malformed_bytes_yield_decode_failure`
* Protobuf-specific:
  * `produced_bytes_match_prost_native_encoding` (asserts the codec's
    output equals `prost::Message::encode_to_vec` byte-for-byte)
  * `unregistered_type_returns_unknown_type_id`
* JSON-specific:
  * `registered_type_count_tracks_registrations`
* Registry:
  * `register_and_lookup_by_content_type`
  * `unknown_content_type_returns_none`
  * `baseline_registers_all_three_content_types`
  * `register_replaces_existing_codec_for_same_content_type`
  * `default_is_empty`
* Doctest: operator-facing example walking
  `JsonCodec::new -> register::<GetReq>() -> CodecRegistry::register
  -> for_content_type -> encode -> decode`.

## Verification

* `cargo build --workspace --all-targets --locked` -- clean.
* `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -- --check` -- clean.
* `cargo clippy --workspace --all-targets --all-features -- -D warnings` -- clean.
* `cargo nextest run -p dyn-encoding` -- 21/21 pass.
* `cargo test --doc -p dyn-encoding` -- 1/1 pass.
* `bash scripts/check_no_todos.sh` -- clean.
* `bash scripts/check_no_port_comments.sh` -- clean.
* `bash scripts/check_ascii.sh` -- clean.

## Deferred codecs

Four codec implementations are out of scope for this scaffold but
the trait surface is shaped to host them without churn. Sketches
follow.

### FlatBuffers (`application/x-flatbuffers`)

* Backend: `flatbuffers` crate (Apache-2.0).
* Generated code: `flatc` produces `*_generated.rs` files. The Riak
  crate will own the `.fbs` schemas and the build-script that
  invokes `flatc`; `dyn-encoding` itself only needs the runtime.
* Bound on `register::<T>()`: `T: WireValue` plus the per-table
  generated trait pair (`Follow<'_>` for decode,
  `WIPOffset`-producing builder for encode). The closure body
  allocates a `flatbuffers::FlatBufferBuilder`, calls the
  generated `pack`-style function, finishes the buffer, and
  returns the byte slice.
* Decode is zero-copy in flatbuffers; the codec will materialise
  into an owned struct (the codec returns
  `Box<dyn ErasedWireValue>`, which is necessarily owned) so the
  registration closure clones the verified flatbuffer view into a
  Rust struct.
* Verifier: `flatbuffers::root::<T>(bytes)` to detect malformed
  inputs cleanly.

### Cap'n Proto (`application/x-capnp`)

* Backend: `capnp` + `capnpc` (BSD-2-Clause).
* Generated code: `capnpc` produces `*_capnp.rs` from `.capnp`
  schemas. Same separation as FlatBuffers: the build-script lives
  in `dyn-riak`.
* Bound on `register::<T>()`: `T: WireValue` plus
  `capnp::traits::Owned` and a pair of converter functions
  (`from_capnp_reader: <reader> -> T`,
  `to_capnp_builder: &T -> <builder>`). The closure body
  serialises through `capnp::serialize::write_message` and
  deserialises through `capnp::serialize::read_message`.
* Cap'n Proto's typed Reader/Builder split makes this slightly
  more involved than the others; we will provide a small
  `capnp_helper!` declarative macro to reduce per-message
  boilerplate.

### Bebop (`application/x-bebop`)

* Backend: `bebop` crate (Apache-2.0). The Rust runtime is
  generated by `bebopc`; `bebop` provides the trait
  `bebop::SubRecord` (encode) and `bebop::Record` (decode).
* Bound on `register::<T>()`: `T: WireValue + bebop::Record<'_>`.
* Bebop is binary, schema-first, length-prefixed. Encoding and
  decoding are straight-line: `T::serialize_into(&mut Vec<u8>)`
  and `T::deserialize(&[u8]) -> Result<T, _>`.
* Status: actively maintained but smaller community; we expect
  to vendor a thin shim crate if upstream version churn becomes a
  problem.

### BSON (`application/bson`)

* Backend: `bson` crate (MIT).
* Bound on `register::<T>()`:
  `T: WireValue + Serialize + DeserializeOwned` (same shape as
  JSON / CBOR; BSON is also serde-driven).
* Implementation: thin clone of `cbor.rs` swapping
  `bson::to_vec` / `bson::from_slice` for
  `ciborium::into_writer` / `ciborium::from_reader`.
* Caveat: BSON is a document-oriented format with restrictions
  (top-level value must be a document). The codec will reject
  registration of `T` whose serde representation is not a struct
  or map; the test for this is straightforward.

## Open questions

* The codec trait is monomorphic in the value -- one
  `WireTypeId -> closure` mapping per type. For the Riak request
  set this is fine (a few dozen message types). If `dyn-riak` ever
  needs streaming or chunked encoding (multi-fragment responses),
  we will extend `WireCodec` with `encode_stream` /
  `decode_stream` methods rather than reshape the request/response
  path. No change is needed for the four deferred codecs to
  participate in such an extension.
* `CodecRegistry` is built once at startup. It is `Send + Sync`
  through its `Box<dyn WireCodec>` payload but is not thread-safely
  mutable; rebuilding requires `Arc::make_mut`. This is acceptable
  for a server that registers all codecs at boot; the journal
  entry pins the constraint.

## Status

Scaffold ready for review. `dyn-riak` worker can take a hard
dependency on `dyn-encoding` v0.0.1 immediately.
