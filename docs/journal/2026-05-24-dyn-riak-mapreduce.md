# dyn-riak MapReduce ("Pipes") -- v0.0.3 slice

Date: 2026-05-24
Branch: `stage/dyn-riak-mapreduce`
Status: READY_FOR_REVIEW

## Scope

A first slice of Riak-style MapReduce ("pipes") for the `dyn-riak`
crate. Delivers:

* `crate::mapreduce` -- the framework: job model, phase types,
  built-in registry, executor.
* `crate::proto::pb::mapreduce` -- `RpbMapRedReq` (PBC code 23) and
  `RpbMapRedResp` (PBC code 24).
* PBC dispatch: `crate::server::handle_mapred` decodes the request,
  runs the executor, and emits a single `RpbMapRedResp` carrying
  the captured outputs as JSON.
* HTTP dispatch: `POST /mapred` route in
  `crate::proto::http::routes` calls the same executor and returns
  `application/json`.
* End-to-end integration tests:
  `crates/dyn-riak/tests/mapreduce_round_trip.rs`.

The slice runs against `dynomite::embed::MemoryDatastore`; no
substrate changes were required.

## Design choices

### Named-builtin registry vs JS / Erlang interpreter

Riak shipped a JavaScript (Spidermonkey) and Erlang interpreter for
user-supplied phase functions. Both are out of scope for this port:

* Spidermonkey is a 1.5M-line C++ dependency with its own Cargo.toml
  story, security posture (sandboxing, syscall filtering), GC
  pauses, and FFI surface. Embedding it would dwarf the rest of
  the crate by an order of magnitude.
* Erlang would require a BEAM runtime; we are not shipping one.

The named-builtin alternative is a fixed registry of phase
functions written in Rust, identified by string name. Operators name
a function in their `Phase::Map { fn_name: "..." }`; the executor
resolves the name through a `PhaseRegistry`. The function set is
small but covers every reduction shape we have seen demand for:

* `map_object_value` -- extract the `value` field.
* `map_object_value_list` -- flatten a value array.
* `map_extract_field { field }` -- pluck one named field from a
  parsed JSON value.
* `map_identity` -- passthrough (assemblage convenience).
* `reduce_count`, `reduce_sum`, `reduce_sort`, `reduce_set_union`,
  `reduce_identity`.

This shape is fast (no interpreter overhead), safe (no sandbox
escape), and deterministic. Operators that need richer logic write
their own functions in Rust, register them through
`PhaseRegistry::register_map` / `register_reduce`, and ship a
binary. The ergonomics of "register your own Rust closure" are
documented in the module-level doctest.

### `WasmModule` reservation

Wasm fittings are the long-term answer to "user-supplied logic
without giving up safety or determinism". The framework reserves
the `Phase::WasmModule { module_id, fn_name, arg, keep }` enum
variant today so the JSON schema is forwards-compatible. The
executor matches the variant and returns
`MrError::WasmNotImplemented`. Adding the actual execution is
mechanical: drop a Wasm runtime (`wasmtime` is the obvious
candidate) into the registry alongside `MapFn` / `ReduceFn`, and
add one branch to `run_phase_task`.

This is not a stub: it is a typed unsupported branch. The schema
test asserts the variant round-trips through JSON.

### Streaming deferred

Riak's `/mapred` HTTP and `RpbMapRedResp` PBC are both natively
streaming: one frame per phase that has `keep: true`, plus a
body-less terminator with `done: true`. This slice ships the
single-response variant: the executor runs the entire job to
completion and emits one buffered response.

Reasons:

* The streaming wire is a strict superset of the buffered wire.
  Clients that handle streaming today read one final frame from
  this server tomorrow without a code change; the inverse is not
  true.
* The executor's pipe shape (mpsc per phase) is already streaming
  internally. Promoting it to streaming output is a tokio
  `mpsc::Receiver<RpbMapRedResp>` plumbed through the PBC writer
  and a hyper `BodyExt` adapter. Both are mechanical and contained.
* Tests for the buffered wire already exercise the executor's
  ordering guarantees end-to-end.

The follow-up slice will add a streaming sink and the wire
adapters; the public `run_job` becomes one of two entry points
(`run_job_buffered` / `run_job_streaming`) and the existing tests
continue to pass.

### `Link` phase semantics deferred

`Phase::Link` is preserved in the public enum and the JSON schema.
Execution returns `MrError::LinkNotImplemented`. The reason is the
same one that gates the buffered K/V trait: the substrate's
`Datastore::dispatch` does not yet expose object content, so there
is nothing for the link walker to follow. As soon as the K/V trait
lands, the link executor is ten lines of "fetch object, parse the
`Link` header, emit `(bucket, key)` per match" that drops in
without touching anything else.

### Pipe shape

The executor allocates two `tokio::sync::mpsc` channels per phase
(inbound + outbound). The previous phase's outbound is the next
phase's inbound; the final phase's outbound is collected into the
response envelope. Each phase runs as its own tokio task. The
shape mirrors Riak's `riak_pipe_*.erl` "pipe of fittings".

Determinism falls out of three properties:

1. `tokio::sync::mpsc` preserves FIFO.
2. Each phase task drains its inbound serially.
3. Built-in functions are pure Rust, no shared mutable state.

Test: `mapreduce::executor::tests::determinism_under_one_hundred_inputs_three_phases`
runs the same 3-phase, 100-input job twice and asserts byte-equal
results.

### `keep` flag

Riak phases carry a `keep: bool`. When true, the phase's outputs
are also captured into the final response. The last phase always
keeps its outputs unconditionally, mirroring Riak. Implemented as a
single `if phase.keep() || is_last { append_to_captured(...) }` in
the executor.

## Wire shape

### HTTP `POST /mapred`

Request body (JSON):

```json
{
  "inputs": [["bucket", "key"], ...],
  "query":  [{"map":    {"name": "map_object_value", "keep": false}},
             {"reduce": {"name": "reduce_sum",       "keep": true}}],
  "timeout": 60000
}
```

`inputs` accepts the three Riak shapes (pairs, inline KeyData
records, or a bucket name; the bucket-name variant returns
`MrError::UnsupportedInputs` since the streaming list-keys path
is not wired yet).

Response body (JSON, 200 OK):

```json
[{"phase": 0, "value": ...}, {"phase": 1, "value": ...}, ...]
```

### PBC `RpbMapRedReq` (23) / `RpbMapRedResp` (24)

```text
RpbMapRedReq  { request: bytes, content_type: bytes }
RpbMapRedResp { phase: uint32?, response: bytes?, done: bool? }
```

`content_type` is `application/json` in v0.0.3; other content
types surface as `RpbErrorResp`. The single-response variant emits
one frame with `phase: 0`, `response: <json>`, `done: true`.

## Tests

Per-area count delta:

| Area | New tests |
|---|---|
| `mapreduce::phase` | 4 |
| `mapreduce::job` | 5 |
| `mapreduce::registry` | 3 |
| `mapreduce::executor` | 9 |
| `mapreduce::builtins` | 19 |
| `proto::pb::mapreduce` | 4 |
| `proto::pb::messages` (MR codes) | 1 |
| `proto::http::routes` (HTTP) | 5 |
| `tests/mapreduce_round_trip.rs` | 4 |
| Total | 54+ |

Workspace-wide nextest count: 865 -> 925 (net +60).

## Files touched

New:
* `crates/dyn-riak/src/mapreduce/mod.rs`
* `crates/dyn-riak/src/mapreduce/job.rs`
* `crates/dyn-riak/src/mapreduce/phase.rs`
* `crates/dyn-riak/src/mapreduce/executor.rs`
* `crates/dyn-riak/src/mapreduce/registry.rs`
* `crates/dyn-riak/src/mapreduce/builtins.rs`
* `crates/dyn-riak/src/proto/pb/mapreduce.rs`
* `crates/dyn-riak/tests/mapreduce_round_trip.rs`

Modified (additive):
* `crates/dyn-riak/src/lib.rs` (appended `pub mod mapreduce;`)
* `crates/dyn-riak/src/proto/pb/mod.rs` (appended re-exports +
  `pub mod mapreduce;`)
* `crates/dyn-riak/src/proto/pb/messages.rs` (added `MapRedReq`
  and `MapRedResp` to `MessageCode`)
* `crates/dyn-riak/src/proto/http/routes.rs` (added `Route::MapRed`
  variant + parse arm + dispatch arm + handler)
* `crates/dyn-riak/src/server.rs` (added `MapRedReq` dispatch +
  imports)
* `docs/journal/allowances.md` (one new entry for the
  `cast_precision_loss` allowance on `int_to_float_lossy`)

## Allowances

One new entry in `docs/journal/allowances.md`:

* `crates/dyn-riak/src/mapreduce/builtins.rs` (`int_to_float_lossy`)
  -- `clippy::cast_precision_loss`. The helper is the single seam
  through which the integer reduce_sum accumulator joins the
  floating-point side; the conversion is the only way to mix the
  two in JSON-numeric arithmetic. Per-function.

## Verification

```
cargo build --workspace --all-targets --locked          OK
cargo fmt -p dynomite -p dynomited -p dyn-hash-tool \
          -p dyn-encoding -p dyn-riak -- --check        OK
cargo clippy --workspace --all-targets --all-features \
          -- -D warnings                                OK
cargo nextest run --workspace                           OK (921 + 4 skipped)
cargo nextest run --workspace --all-features            OK (929 + 4 skipped)
cargo test --doc --workspace                            OK
scripts/check_no_todos.sh                               OK
scripts/check_no_port_comments.sh                       OK
scripts/check_ascii.sh                                  OK
```

## Deferred

* Streaming responses for both HTTP `/mapred` and PBC
  `RpbMapRedResp` (one frame per phase plus a body-less
  terminator). The pipe shape is already streaming internally;
  promoting it to a streaming sink is the next slice.
* Wasm phase execution. The `Phase::WasmModule` variant exists in
  the enum and round-trips through JSON; the executor returns
  `MrError::WasmNotImplemented`.
* JavaScript / Erlang phase emulation. Out of scope per the
  named-builtin design; documented above.
* `Phase::Link` execution. Variant exists; executor returns
  `MrError::LinkNotImplemented`. Lands when the K/V trait surfaces
  object content.
* `Inputs::Bucket` (all keys in a bucket) execution. Returns
  `MrError::UnsupportedInputs("bucket scan")`. Lands with the
  streaming list-keys slice.
* `InputSource` trait for fetching values for `Inputs::Pairs`
  inputs without inline data. Today such inputs flow through with
  `value: null`; the substrate has no content-fetch path to wire
  through. Lands with the K/V trait.
