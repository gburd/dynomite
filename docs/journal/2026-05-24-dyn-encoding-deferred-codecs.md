# 2026-05-24 dyn-encoding deferred codecs land

## Task

Land the four codecs deferred from the `dyn-encoding` scaffold:
FlatBuffers, Cap'n Proto, Bebop, and BSON. Wire them into
`CodecRegistry::with_baseline` so the registry now ships all seven
encodings in the brief.

## Files

New, sibling to the three existing codec modules:

* `crates/dyn-encoding/src/codec/flatbuffers.rs` -- `FlatbuffersCodec`
  + `FlatbuffersWire` trait. Backend: `flatbuffers` runtime crate.
* `crates/dyn-encoding/src/codec/capnp.rs` -- `CapnpCodec`
  + `CapnpWire` trait. Backend: `capnp` runtime crate.
* `crates/dyn-encoding/src/codec/bebop.rs` -- `BebopCodec`
  + `BebopWire` trait. Backend: `bebop` runtime crate.
* `crates/dyn-encoding/src/codec/bson.rs` -- `BsonCodec`. Backend:
  `bson` crate, in `serde` mode.

Touched (registry + module wiring + workspace dep table):

* `Cargo.toml` -- four new entries in `[workspace.dependencies]`.
* `crates/dyn-encoding/Cargo.toml` -- four new dependency entries.
* `crates/dyn-encoding/src/codec/mod.rs` -- `pub mod` lines.
* `crates/dyn-encoding/src/lib.rs` -- `pub use` lines, top-level
  doc rewrite. The doctest in the lib-level example is unchanged.
* `crates/dyn-encoding/src/registry.rs` -- `with_baseline` now
  registers seven codecs; the matching test now asserts seven
  content-types.

The three pre-existing codec files (`json.rs`, `cbor.rs`,
`protobuf.rs`) are untouched per the brief.

## Design

### Trait shape

The three new schema-first codecs each export a per-codec trait
that mirrors `prost::Message`'s role for the protobuf codec:

```rust
pub trait FlatbuffersWire: WireValue + Sized {
    fn flatbuffers_encode(&self) -> Result<Vec<u8>, CodecError>;
    fn flatbuffers_decode(bytes: &[u8]) -> Result<Self, CodecError>;
}
pub trait CapnpWire: WireValue + Sized {
    fn capnp_encode(&self) -> Result<Vec<u8>, CodecError>;
    fn capnp_decode(bytes: &[u8]) -> Result<Self, CodecError>;
}
pub trait BebopWire: WireValue + Sized {
    fn bebop_encode(&self) -> Result<Vec<u8>, CodecError>;
    fn bebop_decode(bytes: &[u8]) -> Result<Self, CodecError>;
}
```

This is a small departure from the deferred-codec sketch in
`docs/journal/2026-05-24-dyn-encoding-scaffold.md`. The sketch
proposed bounding `register::<T>()` directly on the upstream
crates' native traits (`flatbuffers::Follow + Push`,
`capnp::traits::Owned`, `bebop::OwnedRecord`, ...). On contact
with the actual crates this turns out not to fit the codec
contract, which works in terms of *owned* `Box<dyn ErasedWireValue>`
values:

* `flatbuffers::Follow::follow` is `unsafe fn` and returns a
  zero-copy view into `&'buf [u8]`. Wrapping that in an owned
  conversion needs caller-supplied glue anyway.
* `capnp::traits::Owned` is the schema-trait alias the `capnpc`
  output implements; it does not carry encode/decode methods.
* `bebop::Record` requires implementing
  `_serialize_chained_unaligned`, which is `unsafe fn` and would
  require lifting the crate-wide `forbid(unsafe_code)` for every
  consumer. The alternative -- composing the runtime's primitive
  `SubRecord` impls (`String`, `u32`, `Vec<u8>`, ...) -- is fully
  safe, and is exactly what `bebopc` emits for a Bebop `struct`.

The per-codec trait moves the burden of providing those wrappers
to each registered type, which is where the schema lives anyway.
The codec itself stays a 90-line dispatcher that mirrors
`ProtobufCodec` line for line.

The fourth codec, `BsonCodec`, is shaped exactly like
`JsonCodec` / `CborCodec`: bound is
`T: WireValue + Serialize + DeserializeOwned`, dispatch goes
through `bson::serialize_to_vec` /
`bson::deserialize_from_slice`. BSON is document-oriented (the
top-level value must be a document), so a non-struct/map type
will surface a `CodecError::Encode` from the underlying serializer
at runtime; this is consistent with the scaffold sketch's
caveat.

### Avoiding `unsafe` and codegen

The brief calls out two hard constraints: no `flatc` /
`capnpc` build dependency, and (via `AGENTS.md` plus the
crate-level `#![forbid(unsafe_code)]`) no unsafe blocks
without an allowance entry.

Three of the four runtimes have native APIs that touch unsafe.
We dodge each:

1. **FlatBuffers**: the encoder uses `FlatBufferBuilder`, which
   is fully safe. The decoder cannot use `flatbuffers::root_unchecked`
   (`unsafe fn`) or implement `Follow`
   (`unsafe fn follow`). We hand-write a safe parser that walks
   the documented FlatBuffers wire format
   (root uoffset -> table soffset -> vtable -> fields) using
   only `&[u8]::get(...)` and `u16::from_le_bytes` /
   `u32::from_le_bytes` / `i32::from_le_bytes`. This is ~60 lines
   in the test fixture and exercises the same wire format the
   `flatbuffers` runtime emits. The fixture comment captures the
   rationale.
2. **Cap'n Proto**: `capnp::serialize::write_message` and
   `capnp::serialize::read_message` are both safe. The fixture
   uses the runtime's `data_list` accessor, which packs the
   three fields as a heterogeneous list of `Data` blobs. No
   `capnpc` is required.
3. **Bebop**: the runtime's primitive `SubRecord` impls expose
   safe `_serialize_chained` / `_deserialize_chained` methods
   that we compose for each field. Implementing `Record` on the
   fixture itself is avoided because that path requires
   `unsafe fn _serialize_chained_unaligned`.

`BsonCodec` is fully safe by virtue of `bson`'s serde-driven API.

No `#[allow(unsafe_code)]` / `unsafe` blocks were added; no
new `docs/journal/allowances.md` entry is needed.

### Content types

The brief specifies the canonical MIME values; we use them
verbatim:

| Codec        | Content-type                                         |
|--------------|------------------------------------------------------|
| FlatBuffers  | `application/octet-stream;schema=flatbuffers`        |
| Cap'n Proto  | `application/capnproto`                              |
| Bebop        | `application/x-bebop`                                |
| BSON         | `application/bson`                                   |

The `application/octet-stream;schema=flatbuffers` value is the
IANA-friendly fallback FlatBuffers itself documents (FlatBuffers
has no IANA-registered MIME type; the Google maintainers
recommend the `;schema=...` parameter convention). The other
three are widely-cited de-facto values: `application/capnproto`
ships in Cap'n Proto's own `capnp serve` HTTP transport;
`application/x-bebop` is the value the upstream Bebop docs
suggest; `application/bson` is what MongoDB drivers and the
official `bson` spec page use.

## Dependencies

Four new entries in `[workspace.dependencies]`:

| Crate         | Pin    | Notes |
|---------------|--------|-------|
| `flatbuffers` | `25`   | Runtime-only; tracks the upstream FlatBuffers project's Rust crate. |
| `capnp`       | `0.21` | Stable, actively maintained. Capnp's pre-1.0 versioning means each minor bump is breaking; pinning at `0.21` matches the current crates.io recommended line. |
| `bebop`       | `3.2`  | Smaller community than the others but actively maintained on crates.io and on the Rainway/bebop repo. |
| `bson`        | `3`    | The MongoDB-shepherded crate. We enable the `serde` feature; the `chrono` and other ecosystem features stay off. |

`Cargo.lock` grew by 18 entries (counting transitive deps:
`bitvec`, `bitflags v1`, `funty`, `radium`, `tap`, `wyz`,
`uuid`, `serde_bytes`, `simdutf8`, `embedded-io`, `rand v0.9`
family, `rustc_version`, `semver`-bump). All sourced from
`crates.io`; no git deps.

## Tests

Per-codec test count mirrors the existing 5-test pattern from
`cbor.rs`:

* `round_trip_recovers_value`
* `idempotent_encode_is_byte_equal`
* `unregistered_type_returns_unknown_type_id_on_encode`
* `unregistered_type_returns_unknown_type_id_on_decode`
* `malformed_bytes_yield_decode_failure`

Plus the existing `baseline_registers_all_three_content_types`
test was widened to `baseline_registers_all_seven_content_types`,
which also pins the registry size at exactly 7.

Test counts:

| Suite              | Before | After |
|--------------------|--------|-------|
| nextest workspace  | 774    | 794   |
| nextest dyn-encoding | 21   | 41    |
| doctests workspace | 15     | 15    |

The +20 nextest delta is exactly 4 codecs * 5 tests. The widened
registry test does not add a new test (it replaced the old one).

## Verification

* `cargo build --workspace --all-targets --locked` -- clean.
* `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -p dyniak -- --check` -- clean.
* `cargo clippy --workspace --all-targets --all-features -- -D warnings` -- clean.
* `cargo nextest run --workspace` -- 794/794 pass, 4 skipped (unchanged set).
* `cargo test --doc --workspace` -- 15/15 pass.
* `bash scripts/check_no_todos.sh` -- clean.
* `bash scripts/check_no_port_comments.sh` -- clean.
* `bash scripts/check_ascii.sh` -- clean.

## Notes

* No upstream crate was unmaintained or unavailable; all four
  shipped from `crates.io` without surprises.
* The journal sketch in `2026-05-24-dyn-encoding-scaffold.md`
  pencilled in `bound: T: bebop::Record<'_>`. We chose the
  per-codec-trait shape instead. The deviation is captured in
  the design section above; the registration ergonomics are
  unchanged from a caller's perspective (`codec.register::<T>()`
  in both designs).
* Codec content-type strings are picked from the brief; if
  IANA later registers canonical values, the codec module is
  the single edit point.

## Status

Ready for review. Branch `stage/dyn-encoding-deferred-codecs`.
