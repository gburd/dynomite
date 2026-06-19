# 2026-06-18 -- PBC RpbContent on the wire

Branch: `stage/pbc-content` off `main` (fa606b0).

## Goal

Put Riak's nested `RpbContent` message on the PBC wire so PBC
puts/gets are 1:1 with Riak's published schema and PBC objects
carry links natively, not only via the shared `HttpObject`
storage envelope. This is the follow-up slice the object-links
slice (28c41c3) anticipated.

## What changed

### `crates/dyniak/src/proto/pb/messages.rs`

* Added `RpbLink { bucket, key, tag }` (all `optional bytes`,
  tags 1/2/3) matching Riak's schema.
* Added `RpbContent` with the full published field set:
  `value` (1), `content_type` (2), `charset` (3),
  `content_encoding` (4), `vtag` (5), `links` (6, repeated
  `RpbLink`), `last_mod` (7), `last_mod_usecs` (8), `usermeta`
  (9, repeated `RpbPair`), `indexes` (10, repeated `RpbPair`),
  `deleted` (11). dyniak populates `value`, `content_type`,
  `links`, `indexes`; the rest decode/encode as None/empty for
  byte-for-byte schema parity.
* `RpbPutReq`: replaced `value: Vec<u8>` (tag 4) with
  `content: Option<RpbContent>` (tag 4), matching Riak. Removed
  the temporary top-level `indexes` shim at tag 100 (folded
  into `content.indexes`).
* `RpbGetResp.content`: `Vec<Vec<u8>>` (repeated bytes) ->
  `Vec<RpbContent>` (repeated message, tag 1).
* `RpbPutResp.content`: same treatment.
* Dropped the "follow-up slice" / "v0.0.1 flat value"
  doc-comment language now that `RpbContent` is on the wire.
* New unit tests: `rpb_link_round_trips`,
  `rpb_content_round_trips_with_links_and_indexes`,
  `rpb_content_round_trips_empty_links`,
  `rpb_content_carries_full_field_set`,
  `legacy_flat_value_put_migration`. Updated
  `get_resp_round_trips`, `put_req_round_trips`,
  `put_resp_round_trips` to the new shapes.

### `crates/dyniak/src/proto/pb/mod.rs`

* Re-exported `RpbContent`, `RpbLink`.

### `crates/dyniak/src/server.rs`

* `pbc_content_from_storage` now returns an `RpbContent` built
  from the stored `HttpObject` (value, content-type, links via
  `http_link_to_rpb`, indexes).
* Added `http_link_to_rpb` and `rpb_link_to_http` mapping
  helpers.
* PUT: reads `RpbPutReq.content`, maps `RpbContent.links` into
  `HttpObject.links`, plus content-type and indexes, stores via
  the existing storage path. A PBC put with links now persists
  them.
* GET: builds an `RpbContent` from the stored `HttpObject` and
  returns it in `RpbGetResp.content`; a PBC get returns links.

### `crates/dyniak/src/proto/http/object.rs`

* Updated the module doc to describe links over both transports
  and the PBC<->envelope mapping; replaced the stale
  `RpbPutReq.indexes` / `RpbGetResp` references.

### Tests

* `tests/object_links.rs`: `pbc_get` now returns
  `Vec<RpbContent>`; added `pbc_put_content`; new tests
  `pbc_link_put_round_trips_over_pbc`,
  `pbc_link_put_is_visible_over_http`,
  `pbc_content_metadata_round_trips`,
  `mapreduce_link_phase_walks_pbc_persisted_links`. Updated
  `link_put_over_http_is_visible_over_pbc` to assert the link
  now appears in the PBC content.
* Migrated `RpbPutReq` / `RpbGetResp` call sites in
  `tests/bucket_props_routing.rs`, `tests/pbc_round_trip.rs`,
  `tests/noxu_pbc_round_trip.rs`, `tests/quic_pbc.rs` to the
  `content: Some(RpbContent { ... })` shape.

### Docs

* `docs/parity.md`: updated the D0 note (tag-100 shim removed)
  and rewrote D0b to record `RpbContent`/`RpbLink` on the wire,
  the tag-4 migration, the non-UTF-8 lossy mapping, and that
  the link phase walks PBC-persisted links.

## Decisions

* Full `RpbContent` field set included (cheap prost optionals)
  for schema parity over the meaningful-subset alternative.
* tag-4: faithful-to-Riak `content: RpbContent` replacing the
  flat value. The server is tolerant of an absent `content`
  (treats it as an empty value). A legacy flat-value put now
  mis-decodes: if the bytes are not a valid `RpbContent`
  submessage the put is rejected with a decode error; a
  tag-100 index shim is silently skipped. Documented in
  parity.md D0b + pinned by `legacy_flat_value_put_migration`.
* Non-UTF-8 `RpbLink` bytes <-> `HttpLink` String: lossy
  decode in / UTF-8 encode out, empty -> None, matching the
  existing 2i and link-header paths.

## Verification

* `cargo build -p dyniak --all-targets --features noxu --locked`: clean
* `cargo build -p dyniak --all-targets --features noxu,search --locked`: clean
* `cargo build -p dyniak --all-targets --features noxu,wasm,search --locked`: clean
* `cargo nextest run -p dyniak --features noxu`: 595 passed, 0 failed
* `cargo clippy -p dyniak --all-targets --features noxu -- -D warnings`: clean
* `cargo clippy -p dyniak --all-targets --features noxu,wasm,search -- -D warnings`: clean
* `cargo fmt -p dyniak -- --check`: clean
* `cargo test --doc -p dyniak --features noxu`: 51 passed

## Open questions

None. The XA datastore, search, hints, keyfun, and router files
were left untouched per the brief.
