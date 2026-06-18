# 2026-06-18 - MapReduce bucket inputs + link-phase scoping

Stage: `stage/mapred-link-bucket` (off `main` @ dd4050f).

## Summary

Wired the whole-bucket MapReduce input path (`Inputs::Bucket`) end to
end and scoped the Link phase honestly after confirming Riak links are
not persisted by the current object model.

## Gap A - Bucket inputs (DONE)

`Inputs::items()` stays synchronous and still returns `None` for
`Inputs::Bucket` (it has no inline item list). Bucket enumeration is
handled at the executor level by a new async resolver,
`resolve_inputs(&Inputs, Option<&Arc<dyn Datastore>>)`:

* `Pairs` / `KeyData` resolve inline via `items()`.
* `Bucket(name)` streams keys through
  `Datastore::list_keys_stream(name)`, emitting one routing-only
  `KeyDatum` (bucket + key, `value: null`) per key - exactly how Riak
  seeds a whole-bucket input.
* No datastore + `Bucket` -> `MrError::UnsupportedInputs("bucket scan")`
  (the no-store entry points keep their old behaviour).

New public entry points carry the datastore without breaking existing
callers:

* `run_job_full(job, registry, wasm, datastore)`
* `run_job_streaming_full(job, registry, wasm, datastore)`

`run_job`, `run_job_with_wasm`, `run_job_streaming`, and
`run_job_streaming_with_wasm` now delegate to the `_full` variants with
`datastore = None`, so their signatures and behaviour are unchanged.

The HTTP `POST /mapred` handler passes `ctx.datastore` through
`run_job_streaming_full`, so a bucket-scan job submitted over HTTP runs.

`NoxuDatastore` gained a `list_keys_stream` override (it previously used
the trait default that yields `Unsupported`). The override reuses the
existing `fold_primary` primary-record walk, filtering to the requested
bucket. This is a trait-method impl over the existing storage layout -
NOT a schema change.

## Gap B - Link phase (BLOCKED: links not persisted)

The brief allowed implementing real link-walking only if objects
persist their Riak links in a fetchable form. They do not:

* The persisted object model is
  `crate::proto::http::object::HttpObject { value: Vec<u8>,
  content_type: Option<String>, indexes: Vec<HttpIndex> }`, stored as
  its protobuf bytes and returned verbatim by `Datastore::riak_get`.
  There is no link field.
* The PBC content model (`RpbGetResp.content`, `RpbPutReq.value`) is a
  flat opaque `bytes` blob - no `RpbContent`, no `RpbLink`, no
  `RpbContent.links`. (Confirmed by grep: no `RpbLink`/`RpbContent`
  type exists in the crate.)
* The HTTP put path (`put_object_into_store`) accepts only the value,
  content-type, and `X-Riak-Index-*` headers; it never parses or stores
  a `Link:` header. The PBC put path stores `RpbPutReq.value` directly.

So there is genuinely nothing to walk. Implementing link-walking would
require an object-metadata-persistence prerequisite: add a links field
to `HttpObject` (and the PBC content model), parse `Link:` headers /
`RpbContent.links` on put, and round-trip them through the storage
layer. The brief explicitly forbids changing the noxu schema / object
persistence format in this slice and instructs to STOP and report.

The `Phase::Link` arm therefore continues to return
`MrError::LinkNotImplemented`, with its comment rewritten to state the
real blocker (no link list on the object) instead of the stale and
incorrect "Datastore::dispatch does not yet expose object content".
`MrError::LinkNotImplemented` remains reachable (the Link arm still
returns it), so it is not dead.

### Prerequisite for a future Link slice

1. Add `links: Vec<HttpLink { bucket, key, tag }>` to `HttpObject`
   (and the equivalent to the PBC content model).
2. Parse `Link:` headers on HTTP put and `RpbContent.links` on PBC put;
   persist them through `to_storage_bytes` / `riak_put`.
3. Then the Link arm fetches each input object via `riak_get`, decodes
   `HttpObject`, filters `links` by `{bucket, tag}`, and emits the
   matching `(bucket, key)` targets - feeding the next phase exactly as
   Map output does.

## MrError variants

Both `UnsupportedInputs` and `LinkNotImplemented` remain reachable:

* `UnsupportedInputs` - no-datastore bucket scan, or an unrunnable
  input spec.
* `LinkNotImplemented` - every `Phase::Link` (blocked above).

## Tests

* Executor unit tests (`MemoryDatastore`, no feature gate): bucket
  enumeration, bucket -> map -> reduce, empty bucket -> empty result,
  bucket via streaming entry point, no-datastore -> UnsupportedInputs.
* `tests/mapreduce_bucket_inputs.rs` (feature `noxu`): N objects put via
  `NoxuDatastore`, bucket MapReduce sees all N and reduces correctly;
  routing metadata + cross-bucket isolation; empty/nonexistent bucket is
  not an error.
* Existing built-in Map/Reduce, wasm `/mapred`, and PBC/HTTP mapred
  tests stay green (552 lib+integration tests, 46 doctests, 83
  mapred/mapreduce/wasm tests).

## Verify

* `cargo build -p dyniak --all-targets --features noxu --locked` - ok
* `cargo nextest run -p dyniak --features noxu` - 552 passed
* `cargo clippy -p dyniak --all-targets --features noxu -- -D warnings`
  - clean
* `cargo test --doc -p dyniak --features noxu` - 46 passed

`cargo fmt -p dyniak -- --check` reports 5 diffs, all on pre-existing
`OperationStatus` match arms in `noxu.rs` (lines 238/262/808/884/918)
that are identical on pristine `main` (toolchain rustfmt drift). None
are in the code this slice added; left untouched to keep the diff
minimal and avoid churning unrelated lines.
