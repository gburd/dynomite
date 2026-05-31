# 2026-05-24 -- dyniak HTTP gateway

Branch: `stage/dyniak-http`
Commit base: `main` (after `2026-05-24-dyniak-scaffold`)

## What landed

The HTTP sibling of the Riak PBC server already shipping in
`crates/dyniak/src/server.rs`. Operators can now front the same
[`dynomite::embed::Datastore`] with two transports without touching
the substrate or duplicating per-op handlers.

Files added:

* `crates/dyniak/src/proto/http/mod.rs` -- public surface plus
  `serve_http(listener, datastore)`. One tokio task per accepted
  connection; per-conn errors are logged at `tracing::warn!` so a
  misbehaving client cannot drop the listener.
* `crates/dyniak/src/proto/http/routes.rs` -- the route table,
  per-route handlers, and the request-body collector. The route
  parser is a pure function that takes `(method, path, query)` and
  returns a typed `Route` enum. Per-route handlers translate each
  recognised request into a `Datastore::dispatch(Msg)` call in the
  same shape `server::handle_conn` uses on the PBC side.
* `crates/dyniak/src/proto/http/content_type.rs` -- the
  `select_codec(accept, content_type)` negotiation routine.
  Implements the slice of RFC 7231 Section 5.3 the gateway needs:
  comma-split, `q=` weights, `*/*` wildcard with content-type
  fallback, case-insensitive matching, parameter stripping.
* `crates/dyniak/tests/http_round_trip.rs` -- end-to-end test
  spinning up `serve_http` over a real `tokio::net::TcpListener` and
  driving Ping / Put / Get / Delete / list-keys over a raw
  `tokio::net::TcpStream`. Asserts the expected Riak HTTP status
  codes (200, 204, 404, 204, 501) and that
  `MemoryDatastore::dispatch_count()` ticks exactly three times (one
  per K/V op).

Files appended (not rewritten):

* `crates/dyniak/src/lib.rs` -- adds `pub use crate::proto::http::serve_http`
  alongside the existing PBC re-exports. (rustfmt's
  `reorder_imports` reorders the three `pub use` lines
  alphabetically; the existing items are unchanged.)
* `crates/dyniak/src/proto/mod.rs` -- adds `pub mod http;`
  alongside `pub mod pb;`. Same alphabetical reordering applies.
* `crates/dyniak/Cargo.toml` -- adds direct deps on `hyper`,
  `hyper-util`, and `http-body-util`. No new entries in
  `workspace.dependencies`: every version is already pinned by
  another path in the workspace dep graph (OTel transport pulls
  hyper 1.9 and hyper-util 0.1 today), so the lockfile picks up
  these crates without a `cargo update`.

## Routes covered (v0.0.1)

| Method      | Path                                     | Status                  |
|-------------|------------------------------------------|-------------------------|
| GET, HEAD   | `/ping`                                  | 200                     |
| GET         | `/stats`                                 | 200, JSON body          |
| GET, HEAD   | `/buckets/{bucket}/keys/{key}`           | 404 (trampoline empty)  |
| PUT         | `/buckets/{bucket}/keys/{key}`           | 204 / 400 / 415 / 406   |
| POST        | `/buckets/{bucket}/keys/{key}`           | as PUT                  |
| DELETE      | `/buckets/{bucket}/keys/{key}`           | 204                     |
| GET         | `/buckets?buckets=true`                  | 501                     |
| GET         | `/buckets/{bucket}/keys?keys=true`       | 501                     |
| GET         | `/buckets/{bucket}/props`                | 200, JSON defaults      |
| PUT         | `/buckets/{bucket}/props`                | 204                     |

Anything else returns 404. The 415 is for unsupported request
`Content-Type`, the 406 is for an `Accept` header that lists no
supported codec, the 400 is for an empty `PUT` body. The 501 on
`list-buckets` / `list-keys` reflects that `MemoryDatastore` cannot
enumerate; once the streaming list path lands the routes will start
returning real bodies.

## Codecs negotiated

`SUPPORTED_CONTENT_TYPES` lists the three baseline codecs the
`dyn_encoding::CodecRegistry` baseline ships with:

* `application/json`
* `application/x-protobuf`
* `application/cbor`

The four "extra" codecs called out in the broader plan (flatbuffers,
capnp, bebop, bson) are NOT yet wired through
`dyn_encoding::CodecRegistry` as of this slice -- the parallel Item
2 worker has not merged before this branch. When that worker lands,
the only edit required here is appending those four MIME strings to
`SUPPORTED_CONTENT_TYPES`. The negotiator already handles arbitrary
canonical strings.

## HTTP library choice

Both `hyper` 1.9 and `axum` 0.7 are already in the workspace dep
graph (transitively via `opentelemetry-otlp` and friends). The
brief expressed a preference for `hyper`, so the gateway uses
`hyper::server::conn::http1::Builder::serve_connection` directly.
`hyper-util::rt::TokioIo` adapts the tokio `TcpStream` to hyper's
`rt::Read` / `rt::Write` traits; `http_body_util::Full<Bytes>` is
the response body type. No `tower` dep is needed because
`service_fn` produces a service from a closure.

## Sharing logic with the PBC server

The brief asked us to avoid duplicating operation logic. The PBC
server in `proto/pb/...` is owned by a parallel worker and is in the
do-not-touch set, so we cannot factor handlers into shared
per-op functions. Instead, the HTTP routes mirror the PBC server's
`Datastore::dispatch` call shape exactly: each K/V handler builds a
`Msg::new(0, MsgType::Unknown, true)` and awaits `datastore.dispatch`,
matching the PBC trampoline byte-for-byte. The substrate's accounting
ticks identically across the two transports. The richer Riak K/V
trait that replaces the trampoline will live somewhere both
transports import (likely `crate::datastore`), at which point both
handlers can share a single per-op function.

## Test deltas

* `cargo nextest run --workspace`: 774 -> 808 (+34).
  - 17 unit tests in `proto::http::content_type::tests`.
  - 16 unit tests in `proto::http::routes::tests`.
  - 1 integration test in `tests/http_round_trip.rs`.
* `cargo test --doc --workspace`: 608 -> 609 (+1 runnable
  doctest on `select_codec`; the `serve_http` doctest is a
  `no_run` block that still compiles).

## Verification

* `cargo build --workspace --all-targets` -- clean.
* `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -p dyniak -- --check` -- clean.
* `cargo clippy --workspace --all-targets --all-features -- -D warnings` -- clean.
* `cargo nextest run --workspace` -- 808 / 808.
* `cargo test --doc --workspace` -- 609 / 609.
* `bash scripts/check_no_todos.sh` -- clean.
* `bash scripts/check_no_port_comments.sh` -- clean.
* `bash scripts/check_ascii.sh` -- clean.

## Deferred (next slices, not this one)

* Streaming list responses (`?keys=stream`, chunked transfer).
* Search (Solr) endpoints (`/search/...`).
* Cluster admin endpoints (`/admin/...`).
* Real `RiakObject` body materialisation (the Get handler will go
  from 404 to 200 with content once the K/V trait lands).
* Persisted bucket-props (the `Set` handler will go from
  ack-and-discard to durable storage once the props store lands).
* TLS termination (gateway is HTTP/1.1 cleartext only today).
* HTTP/2 support (the `hyper::server::conn::http2` builder is one
  line away once the dep graph picks up the matching feature
  flags; the `hyper` features section already lists `http1`
  only).
* Wiring `dyniak`'s HTTP listener into `dynomited` behind the
  same `--features riak` switch the PBC listener uses.

## Notes

* `serve_http`'s accept loop intentionally mirrors `serve_pbc`'s
  shape so a future supervisor refactor that yields a single
  `accept_loop<Listener, Driver>` helper can absorb both.
* The route table is a single `match` over `(method, path-segments,
  query-flag)`. Adding a route is one new arm plus one new variant
  on `Route`. The order-by-specificity rule is not needed because
  the segment patterns are mutually exclusive.
* `MAX_BODY_LEN` is 16 MiB, identical to the PBC framer cap, so an
  operator's resource limits are uniform across both transports.
