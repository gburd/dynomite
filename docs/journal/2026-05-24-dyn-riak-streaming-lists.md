# dyniak: streaming list-buckets / list-keys (PBC + HTTP)

Date: 2026-05-24
Branch: `stage/dyniak-streaming-lists`
Scope: `crates/dyniak`, `crates/dynomite/src/embed/hooks.rs`, root
`Cargo.toml`.

## Problem

The v0.0.2 PBC slice and the v0.0.1 HTTP gateway both replied to
`RpbListBucketsReq` / `RpbListKeysReq` (PBC) and `GET
/buckets?buckets=true` / `GET /buckets/{bucket}/keys?keys=true`
(HTTP) with a single buffered response. For a Riak-shaped workload
the bucket/key catalogue can be unbounded, so a single-frame reply
forces the server to materialise the whole list before anything
hits the wire. Riak's reference servers stream both lists in
chunks; this slice ports that behaviour.

## Result

- PBC: `RpbListBucketsReq` and `RpbListKeysReq` now produce an
  ordered sequence of `RpbListBucketsResp` / `RpbListKeysResp`
  frames, each carrying up to 256 entries with `done = false`,
  followed by a body-less `done = true` terminator frame. Datastore
  errors mid-stream surface as a single `RpbErrorResp` and stop the
  stream.
- HTTP: the same two routes now negotiate a streaming response
  body. For `application/json`, the body is a chunked JSON array
  (`Transfer-Encoding: chunked`, body `[entry1,entry2,...]`). For
  any other negotiated codec the body is a length-prefixed
  sequence of opaque entries (4-byte big-endian length per entry,
  zero-length terminator).
- `dynomite::embed::Datastore` gains two new methods
  (`list_buckets_stream`, `list_keys_stream`) returning a
  `DatastoreByteStream` (alias for `Pin<Box<dyn
  futures_core::Stream<Item = Result<Bytes, DatastoreError>> +
  Send>>`). Both come with default impls that yield a single
  `DatastoreError::Unsupported(MsgType::Unknown)` so existing
  embedders compile unchanged.
- `MemoryDatastore` overrides both methods to stream from an
  in-memory `BTreeMap<bucket -> BTreeSet<key>>` index. The index is
  attached out-of-band through a per-`Arc` registry keyed by the
  pointer identity of the existing `inner` field, so the public
  `MemoryDatastore` struct layout is unchanged. New helpers
  `MemoryDatastore::insert`, `list_buckets_snapshot`, and
  `list_keys_snapshot` make the listing index drive-able from
  tests.

## Chunk size

Both transports chunk at 256 entries per outbound frame. The cap is
chosen to:

- Stay comfortably below the PBC framer's 16 MiB per-frame limit
  for any practical key size.
- Match Riak's reference server's documented chunk size.
- Keep per-frame overhead (length prefix, code byte, prost
  envelope) below 1% of the body for typical 32 byte keys.

The HTTP and PBC code paths share the same constant in spirit but
declare it twice (`server::LIST_CHUNK_SIZE` and
`http::routes::HTTP_LIST_CHUNK_SIZE`) so a future tuning change to
either transport does not silently affect the other.

## Trait API addition

```rust
pub type DatastoreByteStream = Pin<Box<
    dyn futures_core::Stream<Item = Result<Bytes, DatastoreError>> + Send
>>;

pub trait Datastore: Send + Sync {
    // ... existing methods unchanged ...

    fn list_buckets_stream(&self) -> DatastoreByteStream {
        // default: one-shot Err(Unsupported)
    }

    fn list_keys_stream(&self, _bucket: &[u8]) -> DatastoreByteStream {
        // default: one-shot Err(Unsupported)
    }
}
```

The default body returns a one-item stream of
`Err(DatastoreError::Unsupported(MsgType::Unknown))`. Transports
detect that as a normal datastore error and translate it to an
`RpbErrorResp` (PBC) or close the JSON array immediately (HTTP).
Existing impls of `Datastore` (e.g. `RedisDatastore`,
`MemcacheDatastore`, the Noxu-backed datastore) compile unchanged.

## Streaming negotiation (client side)

PBC clients always get streaming: even an empty bucket produces a
single `RpbListBucketsResp { buckets: [], done: Some(true) }`
frame. A client that ignores the `done` flag and only reads the
first frame still works -- it just sees the first chunk, which for
any non-empty list is a partial result. This mirrors Riak's
behaviour with old clients and is the trade-off we record here for
operators auditing wire captures.

HTTP clients negotiate via the `Accept` header:

- `Accept: application/json` (the default codec) -> chunked JSON
  array. The body is a single well-formed JSON document; clients
  that buffer the body before parsing get a complete array.
  Clients that want true streaming parse the chunks as they
  arrive.
- `Accept: application/cbor`, `application/octet-stream`, or any
  other supported codec -> length-prefixed framing, terminated by
  a 4-byte big-endian zero. Clients that read the body byte by
  byte can pull entries one at a time.

`Transfer-Encoding: chunked` is set on every streaming response
because hyper switches to chunked encoding automatically when the
body is unbounded. The integration test asserts the header is
present.

## Backwards compatibility

- PBC clients that read exactly one frame still work; they get the
  first chunk (a partial list for non-empty inputs) and miss the
  `done` flag. Existing tests that asserted `RpbErrorResp` against
  the v0.0.2 unsupported-list behaviour are migrated to assert the
  streamed `RpbListBucketsResp` / `RpbListKeysResp` shape.
- HTTP clients that only read until end-of-body get the whole
  array in one go (chunked decoding is transparent to a
  reasonably-modern HTTP client). The unit test for the previous
  501 behaviour is replaced with a streaming-array test.
- The `Datastore` trait is extended only via default-bodied
  methods, so external impls keep compiling.

## Workspace dependencies added

Two crates added at the workspace level:

- `futures-core = "0.3"` -- consumed by `dynomite` for the
  `Stream` trait referenced in `DatastoreByteStream` and by
  `dyniak` to type-annotate the boxed stream returns.
- `futures-util = { version = "0.3", default-features = false,
  features = ["std"] }` -- consumed by `dyniak` for the
  `unfold` and `once` stream constructors and the `StreamExt::next`
  method. Both crates are already pulled in transitively (via
  `opentelemetry`, `http-body-util`, `tokio-util`); making the
  dependency explicit means the streaming list path does not rely
  on transitive resolution.

## Files touched

- `Cargo.toml` -- workspace dep additions.
- `crates/dynomite/Cargo.toml` -- + `futures-core`.
- `crates/dynomite/src/embed/hooks.rs` -- trait additions and
  `MemoryDatastore` listing index.
- `crates/dyniak/Cargo.toml` -- + `futures-core`,
  `futures-util`.
- `crates/dyniak/src/server.rs` -- `process_frame` returns a
  `FrameStream`; new `handle_list_buckets`, `handle_list_keys`,
  `buckets_to_frames`, `keys_to_frames`. Connection driver pumps
  the stream.
- `crates/dyniak/src/proto/http/routes.rs` -- `ResponseBody` is
  now `UnsyncBoxBody<Bytes, Infallible>`; new
  `list_buckets_response`, `list_keys_response`,
  `streaming_list_response`, `json_array_chunks`,
  `length_prefixed_chunks`. `not_implemented_response` removed
  (no callers).
- `crates/dyniak/src/proto/http/mod.rs` -- updated module-level
  docs.
- `crates/dyniak/tests/pbc_round_trip.rs` -- migrated
  `exchange_list_*_unsupported` to `exchange_list_*_empty`; added
  `pbc_list_keys_streams_multiple_frames`.
- `crates/dyniak/tests/http_round_trip.rs` -- listing branch
  now seeds the index, asserts chunked encoding, and parses the
  decoded array.

## Tests

New unit tests on `dyniak::server`:

- `list_buckets_empty_yields_one_terminator_frame`
- `list_keys_chunks_at_chunk_size` (1000 keys -> 5 frames)
- `list_buckets_streams_multiple_buckets` (512 -> 3 frames)
- `list_keys_against_unsupported_datastore_yields_error_frame`

New unit tests on `dyniak::proto::http::routes`:

- `list_keys_streams_chunked_json_array`
- `list_buckets_streams_chunked_json_array`
- `list_buckets_empty_streams_empty_json_array`

Migrated unit tests on `dyniak::proto::http::routes`:

- `list_keys_returns_501` -> deleted (route now streams).
- `list_buckets_returns_501` -> deleted.

New integration test in `pbc_round_trip.rs`:

- `pbc_list_keys_streams_multiple_frames` -- end-to-end PBC drive
  with 1000 keys, asserts 4 chunked frames of 256/256/256/232 plus
  an empty `done=true` terminator.

Migrated integration test in `pbc_round_trip.rs`:

- `pbc_new_ops_round_trip` -- list-buckets and list-keys arms now
  assert empty-result streams instead of `RpbErrorResp`.

Migrated integration test in `http_round_trip.rs`:

- `http_ping_put_get_delete_listkeys_round_trip` -- list-keys
  branch asserts 200 with chunked JSON array of seeded keys.

## Self-test results

```
cargo build --workspace --all-targets        # clean
cargo clippy --workspace --all-targets --all-features -- -D warnings  # clean
cargo nextest run --workspace                # 1012 passed, 4 skipped
cargo test --doc -p dynomite                 # all hooks doctests pass
cargo test --doc -p dyniak                 # 14 passed
```

Pre-change baseline (HEAD~1): nextest reported 1004 tests, all
passing. Post-change: 1012 tests, all passing. Delta: +8 tests
(new streaming list coverage).

## Notes / follow-ups

- The HTTP streaming path drops mid-stream datastore errors on the
  floor: once `200 OK` has been written there is no clean way to
  surface a body-level error to the client. Producers fold the
  buffered chunk into a final body frame and close the array; a
  future slice may emit a structured error sentinel. This is
  documented in the inline comment on `json_array_chunks`.
- The length-prefixed codec for non-JSON content-types is a v0.0.1
  shape; once the Riak HTTP gateway gains the multipart/mixed
  reply variant Riak uses for binary keys, the codec selection
  swaps to that shape.
- `MemoryDatastore::insert` is a test-only helper on the public
  surface. The follow-up Riak K/V slice replaces it with the
  full RiakObject store.
