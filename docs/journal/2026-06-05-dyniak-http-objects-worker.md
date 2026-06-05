# 2026-06-05 -- dyniak HTTP object endpoints wired to the noxu store

Stage: dyniak-http-objects
Branch: stage/dyniak-http-objects
Agent: worker (HTTP objects)

## Summary

Replaced the trampoline stubs in the Riak HTTP gateway's object
endpoints (`GET` / `PUT` / `DELETE /buckets/{bucket}/keys/{key}`)
with real storage calls against `NoxuDatastore`, encoding object
bodies through the negotiated codec. Content-type negotiation
(`select_codec` / `canonicalize`) was already complete and is reused
unchanged.

## Files touched

* `crates/dyniak/src/proto/http/object.rs` (new) -- the `HttpObject`
  envelope, its `HttpIndex` entries, the canonical storage form, and
  a process-wide codec registry (`object_codecs`) carrying json /
  cbor / protobuf codecs with `HttpObject` registered.
* `crates/dyniak/src/proto/http/mod.rs` -- exposes `pub mod object`;
  refreshed the datastore-semantics module docs.
* `crates/dyniak/src/proto/http/routes.rs` -- real `handle_get` /
  `handle_put` / `handle_delete`; added `object_store` probe (the
  GET/PUT/DELETE analogue of the existing `txn_store` probe),
  `get_object_from_store`, `put_object_into_store`,
  `collect_index_headers`, `storage_error_response`,
  `no_content_response`.
* `crates/dyniak/tests/http_objects.rs` (new) -- 10 end-to-end tests.
* `crates/dyniak-bench/src/config.rs` -- `HttpEncoding` enum +
  `DriverConfig.encoding`.
* `crates/dyniak-bench/src/main.rs` -- `--encoding json|cbor|protobuf`
  CLI flag plumbed into the driver config.
* `crates/dyniak-bench/src/driver/riak_http.rs` -- driver honours the
  encoding (sets Content-Type + Accept) and builds codec-encoded
  envelope bodies; hand-rolled json / cbor / protobuf encoders so the
  bench stays dependency-free; 6 encoder unit tests.
* `crates/dyniak-bench/examples/riak-http-encodings.toml` (new).
* `crates/dyniak-bench/src/driver/redis.rs`,
  `crates/dyniak-bench/tests/integration.rs` -- `DriverConfig` literal
  gains the new `encoding` field.

## Object shape (documented)

The HTTP object is a fixed-schema envelope so a single Rust type can
register with all three codecs uniformly:

```
HttpObject {
    value:        bytes,            // tag 1
    content_type: optional string,  // tag 2 (object metadata)
    indexes:      repeated HttpIndex { name: string, value: string }, // tag 3
}
```

JSON form: `{"value":[..bytes..],"content_type":"..","indexes":[{"name":"..","value":".."}]}`.
`content_type` and `indexes` are omitted when unset/empty.

The store holds the *decoded* object, not the raw request bytes: the
canonical persisted form is the protobuf serialisation of
`HttpObject`. A `GET` decodes that and re-encodes under the negotiated
`Accept` codec. This is what makes a value `PUT` as json fetchable as
cbor and protobuf (proven by
`put_json_then_get_cbor_and_protobuf_cross_encode`).

A dedicated envelope is used rather than `RpbGetResp` (whose `content`
is flat `repeated bytes` with no slot for per-object content-type or
structured index metadata).

## 2i / index-header handling

Indexes arrive two ways and are merged before storage:

* embedded in the envelope `indexes` field, and
* via `X-Riak-Index-<name>: v1, v2` request headers (Riak's HTTP 2i
  convention; comma-separated values become one entry each).

Both fan out into the NoxuDatastore 2i layer (verified with
`index_eq`) and are echoed back in the envelope on `GET`.

## Behavioural notes / deviations

* `DELETE` of an absent key returns `204 No Content`, matching the
  PBC del path (which replies `RpbDelResp` regardless) and Riak's
  documented behaviour. No `404` for delete.
* Non-object backends (`MemoryDatastore`) keep the documented
  fallback: the request still trampolines through `Datastore::dispatch`
  for accounting, then `GET` -> 404, `PUT`/`DELETE` -> 204. The
  existing `http_round_trip.rs` dispatch-count assertions are
  preserved.
* HTTP-written objects are stored as the protobuf envelope, so a PBC
  `riak_get` of an HTTP-written key returns the envelope bytes rather
  than the raw value. The two transports are not yet object-compatible
  for the same key; this is acceptable for the HTTP-scoped slice and
  is the natural seam for a future shared object representation.

## Tests

* `dyniak` lib (noxu): 545 nextest cases pass, including 5 new
  `object::tests` (storage round-trip, corrupt-decode, cross-encoding,
  index pairs, registry stability).
* `dyniak` `http_objects` integration: 10 tests pass (json round-trip,
  json->cbor/protobuf cross-encode, 404 miss, 415, 400 empty, 400
  undecodable, delete-then-get 404, X-Riak-Index fan-out, HEAD,
  MemoryDatastore fallback).
* `dyniak-bench` (http+riak): 6 new encoder unit tests pass.
* Manual smoke (scratch server + `dyniak-bench --encoding {json,cbor,
  protobuf}` against a live noxu-backed gateway): `err=0` for all three
  codecs across put/get/del; example TOML loads and runs.

## Verification

`cargo build --workspace --all-targets --features riak --locked`,
`cargo nextest run -p dyniak --features noxu`,
`cargo nextest run -p dyniak-bench`,
`cargo nextest run --workspace --features riak`,
`cargo clippy --workspace --all-targets --features riak -- -D warnings`
(plus per-crate clippy with `noxu` and `http`),
`cargo fmt --all -- --check`,
`cargo test -p dyniak --doc[ --features noxu]` -- all clean.

## Open questions

* Shared HTTP/PBC object representation (so the same key is readable
  on both transports) is left as a follow-up; flagged above.
