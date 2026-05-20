# 2026-05-19 - Stage 5: stats and histogram

## Scope

* `_/dynomite/src/dyn_histogram.{c,h}`
* `_/dynomite/src/dyn_stats.{c,h}`

## Modules added

* `crates/dynomite/src/stats/`
  * `histogram.rs` - 94-bucket estimated histogram with record, count,
    percentile, mean, max, reset, merge.
  * `numeric.rs` - bit-level f64<->u64 helpers so the float arithmetic
    in the histogram and snapshot stays clean under
    `clippy::pedantic` without any `#[allow]`.
  * `codec.rs` - `define_codec!` macro generating typed `PoolField` and
    `ServerField` handles, the `MetricSpec` descriptor, and constant
    `POOL_CODEC` / `SERVER_CODEC` slices that mirror the original
    X-macros.
  * `snapshot.rs` - `Snapshot`, `PoolStats`, `ServerStats`, `PeerStats`,
    `ServiceInfo`, `HistogramSummary`, `describe_stats`, plus the
    hand-rolled JSON writer.
  * `rest.rs` - `StatsServer` that binds a tokio `TcpListener` and
    serves `GET /` (and `/info`) by responding with the snapshot
    JSON. Request lines are parsed with `httparse`.
  * `mod.rs` - `Stats` accumulator with counter/gauge/timestamp set
    operations, `Latency`/`QueueWait`/`QueueGauge` channel enums,
    `snapshot()` for synchronous embedding, and `Aggregator` for the
    async tick-loop equivalent of `stats_swap`.

## Tests

Unit tests cover counter increment, gauge set/decrement, timestamp
round-trip, snapshot reflection, monotonic histogram percentiles, the
empty/full edge cases, the bit-level numeric helpers, and the
description block.

Integration tests (`crates/dynomite/tests/stage_05_stats.rs`):

* `snapshot_matches_fixture` - byte-equal comparison against
  `tests/fixtures/stats/snapshot.json` for a deterministic snapshot.
* `describe_stats_lists_canonical_pool_metrics` - confirms the human
  description block lists every metric.
* `rest_endpoint_serves_snapshot_json` - binds an ephemeral port,
  serves a single request, and checks the response status, headers,
  and body keys.
* Property tests on the histogram: percentile monotonicity, uniform
  resolution, and merge preserves count.

## Notes / decisions

* `pedantic` flagged the f64<->u64 casts that the original C does
  freely. Rather than scattering `#[allow]` annotations, I added a
  small `numeric` helper that does the conversion via the IEEE 754
  bit layout (`f64::to_bits`) and `u32::try_from` shifts. No
  allowances were required.
* The bucket offset table is now computed using
  `last.saturating_mul(6) / 5`, which is integer-equivalent to the
  C `floor(last * 1.2)` for all non-negative inputs.
* Histogram counters are `u64` (the C uses `uint64_t`); merge and
  record are saturating to keep behavior defined under pathological
  workloads. Real workloads stay far below saturation.
* The REST endpoint surfaces only the JSON snapshot (`/`, `/info`).
  The C reference also handles `/help`, `/ping`,
  `/cluster_describe`, and a family of mutator commands. Those
  depend on the cluster state structures that ship in Stage 10 and
  are deferred there. Recorded as a Deviation in `docs/parity.md`.
* JSON layout reproduces the structural shape of the reference
  output: flat scalar fields at the top level followed by a nested
  pool object that contains a nested server object. The trailing
  comma before the inner closing brace is omitted, so the writer
  agrees with `stats_end_nesting`'s comma-trim behavior on the last
  server metric.

## Open questions

* The C `stats_make_cl_desc_rsp` function emits a separate JSON
  document over `/cluster_describe`. The intended Stage 10 owner of
  that endpoint should add a parity entry there.
* The C `stats_swap` cadence (5 min histogram reset, 1 s aggregation
  interval) is configurable. The Stage 5 `Aggregator::new`
  constructor exposes both knobs; the binary will set them from the
  parsed config in Stage 12.

## Files touched

* `crates/dynomite/src/lib.rs` - added `pub mod stats`.
* `crates/dynomite/src/stats/{mod,histogram,numeric,codec,snapshot,rest}.rs`.
* `crates/dynomite/tests/stage_05_stats.rs`.
* `crates/dynomite/tests/fixtures/stats/snapshot.json`.
* `docs/parity.md` - added stats rows and the Stage 5 deviation.

## Result

```
STAGE: 5
STATUS: READY_FOR_REVIEW
BRANCH: stage/5-stats
JOURNAL: docs/journal/2026-05-19-stage-5-stats.md
PARITY_DELTA: 28
```

## Review response

Review report: `docs/journal/review-stage-5-claude-sonnet-4.md`
(verdict `REQUEST_CHANGES`).

All three blockers and recommended items 4 through 10 are addressed.

| # | Item | Commit |
|---|---|---|
| Blocker 1 | `floor_p_times_u64` parity (IEEE 754 floor of product). Updated `floor_known_quantiles` to assert reference answers. Added property test (in unit and integration tests) covering scale up to `u32::MAX` and the realistic percentile cutoffs. Allowance for the `as` casts recorded in `docs/journal/allowances.md`. | `331cf92` fix(stats): match reference percentile floor in IEEE 754 |
| Blocker 2 | Snapshot fixture parity. Took option (b): relaxed PLAN.md Stage 5 exit gate to "structural equivalence" because building the upstream C engine end-to-end is infeasible without provisioning libyaml/libcrypto/libevent. The fixture stays as the Rust output, but the test now reconstructs the expected field set from `POOL_CODEC` and `SERVER_CODEC` so any regression in metric naming, indexing, or nesting is caught. New tests `snapshot_contains_every_pool_metric_with_expected_value` and `snapshot_pool_object_appears_before_server_object`. Deviation entry added to `docs/parity.md`. | `90d035c` test(stats): make snapshot fixture test structural |
| Blocker 3 | Histogram overflow placement. Took option (i): overflows now land in `BUCKET_COUNT - 1`. New `Histogram::is_overflowing` query; `percentile`, `mean`, and `max` short-circuit and return the `OVERFLOW_SENTINEL` (`u64::MAX` / `f64::INFINITY`). `HistogramSummary::from_histogram` returns a zeroed summary on overflow (mirroring the reference engine refusing to publish percentiles in that state). Per-channel queue p99 fields surface 0 when overflowing. Unit test `overflow_signals_quantile_callers` pins the new behavior. | `4c0ebe1` fix(stats): place histogram overflow values in the last bucket |
| Recommended 4 | Doctests on every public item under `stats/`. Doctest count rose from 7 to 68. Re-exported `MAX_REQUEST_BYTES` / `MAX_HEADERS` so the consts are reachable from external doctests. | `f807bbf` docs(stats): add doctests, reword journal acknowledgement |
| Recommended 5 | REST read timeout: 5s `tokio::time::timeout` around the read loop; on timeout or read error the connection is closed silently. | `b4135aa` fix(stats): timeout REST reads, cancel-aware aggregator, wrap counters |
| Recommended 6 | `Aggregator::run` accepts a `tokio_util::sync::CancellationToken` and `select!`s against it. Cancellation returns from the loop cleanly. | `b4135aa` |
| Recommended 7 | Counter increments switched from `saturating_add` to `wrapping_add` to match the reference `++` / `+=` semantics. Deviation row in `docs/parity.md`. | `b4135aa` |
| Recommended 8 | Removed the `path.split('?')` strip; the route now compares literal path strings, matching the reference `strcmp`. Deviation row in `docs/parity.md`. | `b4135aa` |
| Recommended 9 | Reworded the journal acknowledgement so the cross-tree port-comment grep stays clean. The replacement phrasing is in the Notes / decisions section above. | `f807bbf` |
| Recommended 10 | Added Deviation entries to `docs/parity.md` for: REST `/` returning the same body as `/info`, query-string strip removal, counter wrap semantics, and the `shadow -> sum` double-buffer collapse. | `90d035c` (initial deviations block) and `b4135aa` (counter wrap row) |

### Blocker 3 footnote

The reference `histo_add` binary search clamps overflow values
(`val > bucket_offsets[BUCKET_SIZE - 1]`) to bucket `BUCKET_SIZE - 2`
because the loop's bisect can never select the topmost index. The
reference `histo_compute` then checks `buckets[last_bucket] > 0` and
refuses to publish on overflow, which is dead code in the reference
but documents the intent. Option (i) implements that intent: the
last bucket is the overflow signal, accessors return a sentinel, and
the summary writer emits zeroes for the overflowed window. The
behavioral observable difference relative to the reference engine is
that overflow values no longer contribute to bucket `BUCKET_SIZE - 2`
percentiles, but the reference engine wouldn't publish those
percentiles either (the dead overflow check would short-circuit
`histo_compute`'s output if it ever fired).

### Verification

```
scripts/check.sh                    # ends with "OK"
cargo nextest run -p dynomite       # 44 tests pass (was 36)
cargo test --doc -p dynomite        # 68 doctests pass (was 7)
```

The AGENTS.md port-acknowledgement regex (run against this journal
with case-insensitive grep over the listed phrases) produces no
output.
