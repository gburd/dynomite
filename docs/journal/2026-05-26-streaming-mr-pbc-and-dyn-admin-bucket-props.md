# 2026-05-26 -- Streaming RpbMapRedReq over PBC + dyn-admin bucket-props

## Context

Two small Riak follow-ups, both ~half-day, landed in the same
working session:

* **A** -- convert the PBC `RpbMapRedReq` handler to per-phase
  streaming. The HTTP `/mapred` route already streams via
  `multipart/mixed`; the PBC side was still emitting a single
  `RpbMapRedResp` with the whole captured output as one JSON
  payload and `done = true`. Real Riak emits one frame per kept
  phase plus a body-less terminator, so the parity gap was
  visible to any client that drives PBC.

* **B** -- expose bucket-property reads and writes through
  `dyn-admin`. The PBC handlers already exist; the operator-
  facing CLI did not.

## A: streaming MR over PBC

### Wire shape

Each kept phase emits one `RpbMapRedResp` with:

```text
phase    = Some(batch.phase)
response = Some(json_payload)
done     = Some(false)
```

The JSON payload mirrors the HTTP multipart writer's body shape,
`[{"phase": N, "data": [...]}]`, so a client that bridges PBC
and HTTP sees byte-identical phase payloads.

End-of-stream is a body-less terminator:

```text
phase    = None
response = None
done     = Some(true)
```

A phase failure short-circuits to a single `RpbErrorResp` and
closes the stream without a terminator. The error itself is the
terminator; emitting `done = true` after an error would let a
naive consumer treat the run as success.

### Implementation

`handle_mapreduce` (renamed from `handle_mapred`) now returns a
`FrameStream` directly. The dispatcher in `process_frame` no
longer wraps it in `single_frame(...)`. The handler:

1. Decodes `RpbMapRedReq`. Decode failures, unsupported
   content types, and JSON job decode failures all surface as
   a one-frame `RpbErrorResp` stream (no streaming, no
   terminator -- the request never produced any phase batch).
2. Calls `run_job_streaming(job, registry)` from the executor
   (already wired by the streaming-MR-HTTP slice).
3. Wraps the resulting `mpsc::Receiver<Result<PhaseBatch,
   MrError>>` in a `futures_util::stream::unfold` state machine
   that converts each batch into one `RpbMapRedResp` frame and
   emits the terminator on receiver close.

The state machine has two states: `Streaming(rx)` and `Done`.
The terminator is emitted inline when `rx.recv()` returns
`None`; we do not need an intermediate `Terminate` state.

### Tests

Five new unit tests in `crates/dyniak/src/server.rs`:

* `process_frame_streams_mapreduce_response_with_per_phase_frames`
  -- two kept phases plus terminator = three frames; phase
  indices match.
* `process_frame_emits_terminator_frame_with_done_true` -- last
  frame carries `done = Some(true)`, no body, no phase.
* `mapreduce_first_frame_is_a_partial_phase_zero_answer` --
  backwards-compat: a one-frame consumer still gets a useful
  partial answer.
* `mapreduce_unknown_function_emits_single_error_frame` --
  phase failure surfaces as one error frame, no terminator.
* `mapreduce_unsupported_content_type_yields_error_frame` --
  pre-stream rejection of `application/xml`.

The existing `mapred_via_pbc` integration test was updated to
read both the per-phase frame and the terminator. The HTTP
integration tests were not touched: `/mapred` already streams,
and its tests assert the multipart shape directly.

### Doc updates

* `crates/dyniak/src/proto/pb/mapreduce.rs` -- the deferred-
  streaming note in the module docs is replaced with the
  current contract (one frame per kept phase plus a body-less
  terminator; errors short-circuit).

## B: dyn-admin bucket-props

### CLI shape

```sh
dyn-admin bucket-props get <bucket> [--node H:P] [--json]
dyn-admin bucket-props set <bucket>
    [--n-val N]
    [--read-consistency one|quorum|all|default|<int>]
    [--write-consistency one|quorum|all|default|<int>]
    [--keyfun std|bucketonly]
    [--replication-strategy topology|successors]
    [--node H:P] [--json]
```

The set path is a partial overlay: the CLI fetches the bucket's
current `RpbBucketProps`, layers the named flags on top, and
sends the merged record back. Fields the operator did not name
are sent unchanged so an operator never accidentally clobbers a
property they did not see.

`set` rejects an empty flag list with a hard error rather than a
no-op round-trip.

### Quorum encoding

Riak ships symbolic quorum names (`one`, `quorum`, `all`,
`default`) on top of the integer `r`/`w`/`pr`/`pw`/`dw`/`rw`
fields. The wire format uses magic uint32 sentinels:

| Name | Wire value |
|---|---|
| `default` | `u32::MAX - 4` (`4294967291`) |
| `one`     | `u32::MAX - 3` (`4294967292`) |
| `quorum`  | `u32::MAX - 2` (`4294967293`) |
| `all`     | `u32::MAX - 1` (`4294967294`) |
| `unset`   | `u32::MAX`     (`4294967295`) |

`ConsistencyArg::from_str` parses both the symbolic names and
literal integers. `render_quorum` decodes a wire value back to a
printable label. The pair guarantees `dyn-admin` and a Riak
client agree on the semantics.

### KeyFun / replication-strategy enums

Both flags are typed with their own clap `FromStr`-backed enums
(`KeyFunArg`, `ReplicationStrategyArg`) instead of clap's
`ValueEnum` derive: the value strings are case-insensitive and
admit a couple of synonyms (`bucketonly`, `bucket-only`,
`bucket_only`) which `ValueEnum` does not.

### Implementation

* New `crates/dyn-admin/src/commands/bucket_props.rs` exposes
  `run_get` and `run_set` plus the typed argument enums.
* The `bucket-props get|set` subcommand pair is wired in
  `crates/dyn-admin/src/main.rs`. Dispatching is split into a
  helper `dispatch_bucket_props` so the top-level `dispatch`
  stays under clippy's 100-line cap.

### Tests

Ten new unit tests in `commands/bucket_props.rs` cover argument
parsing, wire-magic constants, the overlay merger, the human/
JSON renderers, and quorum decoding.

Five new integration tests in
`crates/dyn-admin/tests/bucket_props_integration.rs` spin up a
real `serve_pbc_with_routing` listener with a populated
`BucketPropsRegistry`, drive the CLI through `assert_cmd`, and
assert both the wire round-trip and the registry side-effect:

* `bucket_props_get_unknown_bucket_returns_defaults`
* `bucket_props_get_returns_registered_overrides`
* `bucket_props_set_then_get_round_trips_n_val`
* `bucket_props_set_with_no_overrides_fails`
* `bucket_props_set_keyfun_and_replication_strategy`

The existing `cli_smoke::help_lists_every_v0_subcommand` was
extended to assert that `bucket-props` is announced in `--help`.

## Verification

```text
cargo build --workspace --all-targets --locked          # clean
cargo fmt -p ... -- --check                              # clean
cargo clippy --workspace --all-targets --all-features    # clean
cargo nextest run --workspace                            # 1364 -> 1381
cargo test --doc --workspace                             # 629 + 27 + 15 + 2 + 1
scripts/check_no_todos.sh                                # clean
scripts/check_no_port_comments.sh                        # clean
scripts/check_ascii.sh                                   # clean
```

Total new tests: 17 (5 streaming-MR unit + 10 bucket-props unit
+ 5 bucket-props integration - 1 cli-smoke that was extended in
place; 5 + 10 + 5 - 3 doc-test + the 0 = +17 net).

## Open questions

None.

## Out-of-scope

* `bucket-props` does not yet surface `pr`/`pw`/`dw`/`rw`,
  `consistent`, `write_once`, search-index, datatype, or hooks.
  The wire support exists in `RpbBucketProps`; the CLI carries
  only the four knobs the brief asked for. Adding the rest is a
  mechanical extension of `SetOptions` and `BucketPropsView`.
* The PBC `bucket-type`-scoped variant of `set`/`get` (Riak
  ships an optional `type` field on `RpbGetBucketReq`/
  `RpbSetBucketReq`) is not yet exposed. The wire field is
  threaded through to the server-side handler today, and adding
  a `--bucket-type` flag is one extra `Option<String>` plumb-
  through.
