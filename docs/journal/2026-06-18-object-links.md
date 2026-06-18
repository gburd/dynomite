# 2026-06-18 -- Object links end to end (storage + MapReduce link phase)

Stage: stage/object-links (off main @ a7874fc)
Author: Greg Burd <greg@burd.me>

## Summary

Implemented Riak object links end to end and wired the MapReduce
link phase that walks them. Two parts in one slice:

1. Persist links through the storage path (HTTP `Link:` headers, PBC
   round-trip via the shared `HttpObject` storage form).
2. The consumer: `Phase::Link` now walks links via `riak_get`,
   filters by `{bucket, tag}`, and emits targets to the next phase.

## Files touched

* `crates/dyniak/src/proto/http/object.rs`
  * New `HttpLink { bucket, key, tag }` prost message (tags 1/2/3),
    mirroring `HttpIndex`'s derive set and serde attrs.
  * Added `links: Vec<HttpLink>` to `HttpObject` at prost tag 4
    (`serde(default, skip_serializing_if = "Vec::is_empty")`). Tag 4
    was unused, so old objects decode with empty links (backward
    compatible). The prost round-trip carries the field automatically;
    `to_storage_bytes` / `from_storage_bytes` need no change.
  * Tests: links round-trip through storage; legacy tag-1/2/3 bytes
    decode with empty links and no error.
* `crates/dyniak/src/proto/http/routes.rs`
  * Parse `Link:` request headers on PUT into `HttpObject.links`
    (`collect_link_headers` + `split_link_values` + `parse_link_value`
    + `parse_link_resource` + `unquote` + `decode_path_segment` +
    `hex_digit`).
  * Emit stored links as `Link:` response headers on GET, plus the
    `</buckets/BUCKET>; rel="up"` bucket-up link for parity.
  * Unit tests for the parser (modern + legacy forms, rel=up skip,
    multi-header / multi-value).
* `crates/dyniak/src/server.rs` (PBC)
  * PBC put now wraps `req.value` into an `HttpObject` envelope and
    stores `to_storage_bytes()`; PBC get decodes the envelope via
    `pbc_content_from_storage` and returns its `value`. This keeps the
    PBC and HTTP storage forms compatible. Bytes that predate the
    shared form (or were stored raw) are returned verbatim on read.
* `crates/dyniak/src/mapreduce/executor.rs`
  * Threaded `Option<Arc<dyn Datastore>>` through `run_phase` and
    `run_phase_task` so the Link arm can fetch objects.
  * Replaced the `Phase::Link` blocker / `LinkNotImplemented` arm with
    a real walk extracted into `run_link_phase`: fetch via `riak_get`,
    decode `HttpObject`, filter `{bucket, tag}` (None = wildcard), emit
    matched `(bucket, key)` as the same datum shape Map emits, honour
    `keep`. Missing object -> no links, not an error.
  * `LinkNotImplemented` now only fires on the no-datastore in-memory
    path; its doc and error text were updated accordingly.
  * New `link_input_target` helper extracts `(bucket, key)` from a
    phase datum.
* Tests added:
  * `crates/dyniak/tests/object_links.rs` -- HTTP `Link:` header
    round-trip (two tags), cross-protocol (HTTP link put visible over
    PBC get; PBC value put fetchable over HTTP).
  * `crates/dyniak/tests/mapreduce_link_phase.rs` -- walk + tag filter
    (exactly {B, C}), wildcard tag includes D, bucket filter, missing
    object yields empty, multi-phase (Link -> Reduce) threads, `keep`
    honoured both ways.
  * Updated the existing in-tree literal `HttpObject` constructions in
    `tests/http_objects.rs` to add `links: Vec::new()`.

## Grammar accepted (HTTP Link:)

`Link: <RESOURCE>; riaktag="TAG"` where RESOURCE is
`/buckets/<bucket>/keys/<key>` or the legacy `/riak/<bucket>/<key>`.
Multiple `Link:` header lines and multiple comma-separated
link-values per line are both honoured. `tag=` is accepted as an
alias for `riaktag=`. Percent escapes in bucket/key segments are
decoded.

## Deliberately skipped

* `rel="up"` bucket-up links are NOT parsed on write: they name a
  bucket, not an object, carry no `riaktag`, and a link phase cannot
  walk them. They ARE re-synthesised on read for client parity.
* PBC `RpbPutReq` has no link field (the schema is a flat
  `value: bytes`, and `RpbContent` is not on the wire yet), so a PBC
  put never adds links. The capability returns when `RpbContent` with
  its nested `links` lands. Documented as parity entry D0b.
* PBC `/mapred` (`handle_mapreduce`) runs the no-datastore in-memory
  executor; a Link phase there returns `MrError::LinkNotImplemented`
  ("link phases require a datastore-backed job"). The HTTP `/mapred`
  path threads the datastore and walks links normally.

## Verify

```
cargo build -p dyniak --all-targets --features noxu --locked   # clean
cargo nextest run -p dyniak --features noxu                     # 568 passed
cargo test --doc -p dyniak --features noxu                      # 47 passed
cargo clippy -p dyniak --all-targets --features noxu -- -D warnings  # clean
cargo fmt -p dyniak -- --check                                  # clean
```

## Parity

`docs/parity.md` D0b: object links wire format, the shared
`HttpObject` storage form divergence (links on the envelope at tag 4
rather than a 1:1 `RpbContent.links` mirror), the accepted `Link:`
grammar, the rel=up skip, and the link-phase semantics.

## Open questions

None. When `RpbContent` lands, map `RpbContent.links` onto
`HttpObject.links` in the PBC put/get path and drop the wrap/unwrap
shim's reliance on the flat `value` field.
