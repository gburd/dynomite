# 2026-05-24 -- dyn-riak PBC ops slice (v0.0.2)

Branch: `stage/dyn-riak-pbc-ops-v1`
Commit base: `0b08494` (main: "merge: stage/dyn-riak-scaffold into main")

## What landed

The second usable slice of `crates/dyn-riak`. Six new Riak Protocol
Buffers operations join the four shipped in the v0.0.1 scaffold.

| Code | Direction | Operation |
|---|---|---|
| 7   | C->S | `RpbServerInfoReq` |
| 8   | S->C | `RpbGetServerInfoResp` |
| 15  | C->S | `RpbListBucketsReq` |
| 16  | S->C | `RpbListBucketsResp` |
| 17  | C->S | `RpbListKeysReq` |
| 18  | S->C | `RpbListKeysResp` |
| 19  | C->S | `RpbGetBucketReq` |
| 20  | S->C | `RpbGetBucketResp` |
| 21  | C->S | `RpbSetBucketReq` |
| 22  | S->C | `RpbSetBucketResp` |
| 25  | C->S | `RpbIndexReq` |
| 26  | S->C | `RpbIndexResp` |

Plus the supporting message types:

* `RpbBucketProps` -- per-bucket configuration. All scalar and
  `bytes` fields modelled at their published tags. Tags reserved by
  Riak for nested-message types not modelled in this slice
  (`precommit`, `postcommit`, `chash_keyfun`, `linkfun`, `repl`)
  are left as gaps; `prost` skips them on decode and the gap
  preserves wire-tag stability.
* `RpbPair` -- generic (key, value) tuple used inside
  `RpbIndexResp::results` for `return_terms` queries.

The `MessageCode` enum gains twelve new variants. The
`messages.rs::tests` module adds seven new round-trip unit tests
(server-info, list-buckets, list-keys, get-bucket, set-bucket,
index range, index equality). The integration test file gains
`pbc_new_ops_round_trip`, an end-to-end driver that exercises every
new op over a real `tokio::net::TcpListener`.

## Server-side semantics

The new request handlers split into three behavioural categories:

* **Direct reply** (server-info, get-bucket, set-bucket): the
  handler responds without touching the substrate. `dispatch_count`
  stays at zero. `RpbGetBucketResp` carries Riak's stock
  out-of-the-box defaults (`n_val = 3`, `allow_mult = false`,
  `last_write_wins = false`); per-bucket persistence is deferred
  to the K/V trait expansion. `RpbSetBucketReq` is acknowledged
  with the body-less `RpbSetBucketResp`; the supplied properties
  are not yet persisted.
* **Unsupported on this datastore** (list-buckets, list-keys,
  index): the handler decodes the request body for parse-error
  feedback, then responds with `RpbErrorResp` carrying a
  `"... not implemented for this datastore"` message and
  `errcode = 1`. The brief explicitly defers wiring these to
  `NoxuDatastore` until the richer Riak K/V trait lands.
* **K/V trampoline** (existing put, get, del paths): unchanged
  from v0.0.1.

The `RpbServerInfoReq` reply uses `env!("CARGO_PKG_VERSION")` so the
advertised version stays in sync with the crate's `Cargo.toml`.

## Streaming follow-up

`RpbListBucketsResp`, `RpbListKeysResp`, and `RpbIndexResp` all
carry an optional `done` flag in the schema so the server can split
results across multiple response frames. The brief asked for the
non-streaming variant: this slice ships a single response frame per
request with `done = Some(true)`. Streaming is recorded as a
follow-up here.

## File map

* `crates/dyn-riak/src/proto/pb/messages.rs` -- new struct
  definitions, expanded `MessageCode`, and seven new round-trip
  tests.
* `crates/dyn-riak/src/proto/pb/codec.rs` -- registers the new
  message types into the protobuf codec.
* `crates/dyn-riak/src/proto/pb/mod.rs` -- new `pub use` block
  appended below the existing one (no rearrangement).
* `crates/dyn-riak/src/server.rs` -- new per-op handler functions
  plus a `handle_unsupported<T>` helper. `process_frame` keeps a
  flat match.
* `crates/dyn-riak/tests/pbc_round_trip.rs` -- the original
  `pbc_ping_put_get_del_round_trip` test stays; a new
  `pbc_new_ops_round_trip` test drives the v0.0.2 surface. Both
  share a `Harness` helper.

## Verification

* `cargo build --workspace --all-targets --locked` -- clean.
* `cargo build --workspace --all-targets --all-features --locked`
  -- clean.
* `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -p dyn-riak -- --check` -- clean.
* `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  -- clean.
* `cargo nextest run --workspace`: 774 -> 782 (+8 tests). The
  dyn-riak crate's count went from 25 to 33.
* `cargo test --doc --workspace`: 608 doctests, all passing
  (no doctest count change; the doctest on `MessageCode` was
  updated in place to use an unused byte instead of byte 7,
  which is now `ServerInfoReq`).
* `bash scripts/check_no_todos.sh` -- clean.
* `bash scripts/check_no_port_comments.sh` -- clean.
* `bash scripts/check_ascii.sh` -- clean.

## Deferred (next slices, not this one)

* Streaming responses for `RpbListBucketsResp`,
  `RpbListKeysResp`, and `RpbIndexResp`. The single-frame variant
  ships now; the streaming variant requires a writer-side
  iterator API that lands with the K/V trait work.
* Wiring `RpbListBucketsReq`, `RpbListKeysReq`, and `RpbIndexReq`
  against the `NoxuDatastore`. The error-frame response is the
  contract for this slice.
* Per-bucket persistence: `RpbSetBucketReq` accepts properties
  but does not store them; `RpbGetBucketReq` returns stock
  defaults regardless of the bucket name. The richer Riak K/V
  trait covers this in the next slice.
* `RpbBucketProps` nested-message fields (`precommit`,
  `postcommit`, `chash_keyfun`, `linkfun`, `repl`). Tags
  reserved; types not yet modelled.

## Notes for the lead

* `MessageCode` is `#[non_exhaustive]`, so adding the twelve new
  variants does not break public-API compatibility for downstream
  callers that match on it.
* The new `pub use` block in `proto/pb/mod.rs` is appended below
  the existing one. Parallel branches that touch the existing
  block will merge cleanly with this one.
* No changes to `crates/dyn-riak/src/lib.rs` were needed.
