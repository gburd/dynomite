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
* JSON layout matches the C output structure: flat scalar fields at
  the top level followed by a nested pool object that contains a
  nested server object. The trailing comma before the inner closing
  brace is omitted, matching `stats_end_nesting`'s comma-trim
  behavior on the last server metric.

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
