# dyniak streaming: HTTP `/mapred` (multipart) and PBC `RpbIndexResp`

Date: 2026-05-24
Branch: `stage/dyniak-streaming-mr-2i`
Status: READY_FOR_REVIEW

## Scope

Two of the deferred streaming items from the v0.0.3 / 2i slices,
landed in one commit:

* **Item 6 -- HTTP `POST /mapred`**: convert the buffered JSON
  array response to a chunked-transfer-encoded `multipart/mixed`
  body where every kept phase yields its own body part as soon as
  the executor finishes the phase.
* **Item 7 -- PBC `RpbIndexResp`**: convert the single-frame 2i
  response to a multi-frame stream chunked at 256 keys per frame,
  matching the list-buckets / list-keys precedent.

## HTTP `/mapred` streaming (Item 6)

### Wire shape

The route now produces:

```text
HTTP/1.1 200 OK
Content-Type: multipart/mixed; boundary=dyniak-mr-<nanos16>-<counter16>
Transfer-Encoding: chunked
Server: dyniak

--{boundary}
Content-Type: application/json

[{"phase":0,"data":[...]}]
--{boundary}
Content-Type: application/json

[{"phase":1,"data":[...]}]
--{boundary}--
```

One body part per **kept** phase. The last phase always keeps
unconditionally (Riak parity). On a phase failure the final part
carries `Content-Type: text/plain` and the human-readable error
message; the closing delimiter follows immediately. Boundary
strings are unique per request.

### Boundary uniqueness

A monotonic process-local `AtomicU64` is combined with the current
system-time nanoseconds, both encoded as fixed-width hex. The
result is a 33-byte ASCII boundary like
`dyniak-mr-1865b7c92ab10000-0000000000000023`. Collision
probability inside a single request body is negligible. This avoids
adding a `uuid` workspace dependency.

### Executor changes

* New public type `mapreduce::executor::PhaseBatch { phase, data }`.
  One batch is emitted per kept phase.
* New entry points
  `run_job_streaming` / `run_job_streaming_with_wasm`. Both return
  a `tokio::sync::mpsc::Receiver<Result<PhaseBatch, MrError>>`. A
  background tokio task drives the pipeline; the receiver is
  drained by the HTTP body writer.
* Buffered `run_job` / `run_job_with_wasm` are unchanged. The PBC
  `handle_mapred` continues to emit a single `RpbMapRedResp`; that
  streaming variant lives in a follow-up slice.

### HTTP body stream

A `futures_util::stream::unfold`-driven state machine
(`MapRedMultipartState`) consumes the executor's mpsc receiver and
emits one `Bytes` chunk per body part: dash-boundary line, headers,
blank line, JSON / text body, trailing CRLF. The closing
`--{boundary}--\r\n` is the final chunk. The body is wrapped in
`http_body_util::StreamBody` and boxed into the existing
`ResponseBody = UnsyncBoxBody<Bytes, Infallible>` shape; hyper
handles chunked transfer-encoding automatically.

### Tests

* `routes::mapred_runs_simple_job` -- updated; parses multipart,
  finds one part, decodes the JSON array.
* `routes::mapred_streams_multiple_kept_phases` -- new; two kept
  phases yield two parts; verifies the closing delimiter is
  present.
* `routes::mapred_phase_failure_emits_text_part_and_closing_delimiter`
  -- new; references a missing function so the executor errors,
  asserts the final part is `text/plain` carrying the function
  name, and that the closing delimiter is present.
* `mapreduce::executor::streaming_*` -- four new unit tests
  exercising the streaming executor (kept-only batches, error path,
  empty phase list, intermediate-phase suppression).
* `tests/mapreduce_round_trip.rs::mapred_via_http` -- updated to
  decode chunked transfer encoding then walk multipart parts.
* `tests/mapreduce_round_trip.rs::mapred_via_http_phase_failure_emits_text_part`
  -- new wire-level integration test for the error path.

## PBC `RpbIndexResp` streaming (Item 7)

### Wire shape

`handle_index` now returns a `FrameStream` that yields up to
`LIST_CHUNK_SIZE = 256` keys per frame, finished with a body-less
terminator carrying `done = Some(true)`. Datastore errors and
`Unsupported` responses still produce a single `RpbErrorResp`
frame; an empty result set yields a single `done = true`
terminator (so an old client that reads exactly one frame still
sees a meaningful answer).

The chunking happens at the server boundary because the
`Datastore::riak_index_eq` / `riak_index_range` trait methods on
`crates/dynomite/src/embed/hooks.rs` already return
`Vec<Vec<u8>>`; that crate is owned by a parallel worker, so the
streaming layer is added on top of the buffered trait method
without touching the substrate.

### Backwards compatibility

A client written for the single-frame v0.0.2 shape continues to
parse. It sees `done = Some(false)` instead of `Some(true)` and
its first frame's keys are the first 256-entry chunk of the result
set; this is a strict superset of the old behaviour. The new test
`index_eq_first_frame_carries_partial_keys_for_old_clients`
asserts this contract.

### Tests

In `crates/dyniak/src/server.rs`:

* `index_eq_streams_chunks_of_chunk_size` -- 1000 mock keys
  produces 4 key chunks (3x256 + 232) plus a terminator.
* `index_eq_first_frame_carries_partial_keys_for_old_clients` --
  backwards-compat assertion.
* `index_empty_yields_single_terminator`.
* `index_eq_max_results_caps_total_streamed_keys` -- cap of 300
  still flows through chunking correctly.
* `index_unsupported_datastore_yields_error_frame`.

In `crates/dyniak/tests/pbc_round_trip.rs`:

* `pbc_index_streams_chunks_against_scripted_datastore` -- end-to-end
  through `serve_pbc` with a `ScriptedIndex` mock returning 1000
  keys; verifies 5 frames over the wire.
* `pbc_index_first_frame_decodes_for_old_clients` -- end-to-end
  backwards compat.

In `crates/dyniak/tests/noxu_pbc_round_trip.rs`:

* `index_eq` / `index_range` helpers updated to drain frames until
  `done = Some(true)`; existing `pbc_index_*` tests now exercise
  the streaming wire.

## Files touched

Modified:

* `crates/dyniak/src/mapreduce/executor.rs` (added `PhaseBatch`,
  `run_job_streaming`, `run_job_streaming_with_wasm`,
  `stream_job_inner`, four streaming unit tests).
* `crates/dyniak/src/mapreduce/mod.rs` (re-exported the
  streaming entry points).
* `crates/dyniak/src/proto/http/routes.rs` (rewrote
  `mapred_response`; added `mapred_boundary`,
  `mapred_multipart_body`, `mapred_phase_part_body`; added two new
  unit tests; updated the existing one).
* `crates/dyniak/src/server.rs` (rewrote `handle_index` to
  return a `FrameStream`; added `index_keys_to_frames`,
  `IndexChunkState`; added five streaming unit tests).
* `crates/dyniak/tests/mapreduce_round_trip.rs` (updated
  `mapred_via_http`, added a chunked-decode helper, added
  `mapred_via_http_phase_failure_emits_text_part`).
* `crates/dyniak/tests/pbc_round_trip.rs` (added
  `pbc_index_streams_chunks_against_scripted_datastore`,
  `pbc_index_first_frame_decodes_for_old_clients`; added
  `RpbIndexResp` to the imports).
* `crates/dyniak/tests/noxu_pbc_round_trip.rs` (drain helper +
  updated `index_eq` / `index_range`).
* `docs/journal/2026-05-24-dyniak-streaming-mr-2i.md` (this
  file).

No changes to `crates/dynomite/`, `crates/dynomited/`,
`crates/dyn-encoding/`, `crates/dyn-hash-tool/`,
`crates/dyn-admin/`, `crates/dyniak/src/datatypes/`,
`crates/dyniak/src/aae/`, `crates/dyniak/src/datastore/`, or
`scripts/`.

## Allowances

None. No new `#[allow]` annotations.

## Verification

```
cargo build --workspace --all-targets --locked          OK
cargo fmt -p dynomite -p dynomited -p dyn-hash-tool \
          -p dyn-encoding -p dyniak -p dyn-admin \
          -- --check                                    OK
cargo clippy --workspace --all-targets --all-features \
          -- -D warnings                                OK
cargo nextest run --workspace                           OK (1125 + 4 skipped)
cargo test --doc --workspace                            OK
scripts/check_no_todos.sh                               OK
scripts/check_no_port_comments.sh                       OK
scripts/check_ascii.sh                                  OK
```

Workspace nextest: 1111 -> 1125 (+14 new tests covering the
streaming executor, the multipart writer, the 2i chunker, and
two PBC integration tests; one existing HTTP integration test
was rewritten in place rather than duplicated, so the net delta
is 14 new and 1 reshape).

## Deferred

* PBC `RpbMapRedResp` streaming (one frame per phase + done=true
  terminator). The streaming executor entry point is in place;
  promoting `handle_mapred` to consume it is the obvious follow-up.
  The buffered shape continues to ship in this slice so PBC
  clients are not broken in the same release that lands the HTTP
  multipart change.
* `Inputs::Bucket` (all keys in a bucket) execution still returns
  `MrError::UnsupportedInputs("bucket scan")`.
* `Phase::Link` execution still returns `MrError::LinkNotImplemented`.
