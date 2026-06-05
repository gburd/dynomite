# dyniak search bridge

Date: 2026-06-05
Branch: stage/dyniak-search

## Summary

Bridged dyniak to the dynomite search and vector engines: the Riak
HTTP gateway now exposes per-bucket text search (substring and
approximate-regex) and vector KNN, backed by the existing
`dynomite-search` `VectorRegistry`. The registry machinery is reused
verbatim; no fork, and the redis-side FT.* surface is untouched.

## Files touched

* `crates/dyniak/Cargo.toml` -- new optional `search` feature pulling
  `dynomite-search` (transitively `dynomite-vec` + `dynomite-text`).
* `crates/dyniak/src/proto/http/search.rs` (new) -- `SearchState`, the
  index-management and search route handlers, the request/response
  wire types (json/cbor/protobuf via `dyn_encoding`), feed-on-write
  (`index_object`), and the JSON-document field mapping.
* `crates/dyniak/src/proto/http/routes.rs` -- `RouteCtx` (datastore +
  optional search state), six new search Route variants + parsing,
  dispatch, and feed-on-write hook in `put_object_into_store`.
* `crates/dyniak/src/proto/http/mod.rs` -- `serve_http_with_search` /
  `serve_http_tls_with_search`, shared `serve_http_ctx` loops.
* `crates/dyniak/src/lib.rs` -- re-exports + public-surface doc.
* `crates/dyniak/tests/http_search.rs` (new) -- 6 end-to-end tests.
* `crates/dyniak-bench/src/driver/riak_http.rs` -- `search` workload
  ops (`index_put`, `search_text`, `search_regex`, `search_vector`).
* `crates/dyniak-bench/examples/riak-search.toml` (new) + an example
  parse test in `tests/integration.rs`.
* `crates/dynomited/{Cargo.toml,src/riak.rs,src/server.rs}` -- forward
  `dyniak?/search` and wire the shared vector registry into the
  `data_store: dyniak` HTTP gateway.

## Object -> index field mapping

The object PUT body decodes to an `HttpObject`; its `value` payload is
interpreted as a JSON document for indexing. A text index on field
`F` reads the document's top-level string key `F`. A vector index
reads a numeric array under the document field named at create time
(default `_vector`); the remaining top-level fields ride along as row
metadata so KNN queries can post-filter on them. Registry index names
are `text:{bucket}` and `vec:{bucket}`. Feeding is best-effort: a
parse miss or engine error never fails the (already durable) write.

## Verification

* `cargo build --workspace --all-targets --features riak,search --locked` -- ok
* `cargo nextest run -p dyniak --features noxu,search` -- 560 passed
* `cargo nextest run -p dyniak-bench` -- 50 passed (search ops + example
  parse under the `http` feature / conformance profile)
* `cargo nextest run --workspace --features riak` -- 2081 passed (two
  pre-existing load-correlated timing flakes, `pidfile::flock_retry`
  and `engine::token_bucket_paces`, pass in isolation and on a
  non-contended rerun)
* `cargo clippy --workspace --all-targets --features riak,search -- -D warnings` -- clean
* `cargo fmt --all -- --check` -- clean
* doctests clean with and without the `search` feature

## Open questions

* Delete-from-index on object DELETE is not wired (the registry text
  path overwrites on re-PUT but has no per-key text eviction by key
  exposed; vector rows are not evicted on delete). Documented as a
  follow-up; out of scope for this slice.
