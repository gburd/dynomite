# Combined-mode chaos: pass-combined3 (2026-06-18)

Single host (floki), MODE=combined, 1h, fault classes: process, network, clock, disk.
Three independent dynomited instances (valkey / memcache / dyniak) on port bands
+0 / +1000 / +2000, each driven with its full native workload, all faulted.

## Aggregate (whole run)

| API | ok | fail | rate | failure breakdown |
|---|---|---|---|---|
| memcache | 156379 | 3 | 99.998% | 3x bare Timeout (induced) |
| riak (dyniak PBC/HTTP) | 48803 | 4 | 99.992% | 4x riak/Timeout (induced) |
| valkey | 161436 | 5467 | 96.72% | 3362 ft/Unknown + 2101 ftsug/Unknown + 4 Timeout |

Total: 372092 ops.

## Failure accounting (investigation discipline: every failure -> induced fault)

- **All 11 non-FT failures are bare Timeouts** -- the direct, expected result of
  the injected network-delay / process-kill faults. Fully accounted.
- **The 5463 valkey FT.* failures (ft/Unknown + ftsug/Unknown) are the
  in-memory FT.* index/suggestion-dictionary registry being wiped by each
  chaos process-kill.** FT.* index state was not persisted across a dynomited
  restart (a tracked code gap). After a kill the index is transiently absent
  until the workload re-creates it. Chaos-induced, but until the
  IndexResetByChaos classification (commit f0f7018, which postdates this run)
  they surfaced as Unknown.

## Resolution in flight

- `f321c3b` -- workload recreates the FT.* index on miss (self-heal fallback).
- `f0f7018` -- classify_error labels index-not-found / already-exists as a
  distinct IndexResetByChaos class instead of Unknown.
- **FT.* index persistence (stage/ft-persist)** -- the feature-level fix:
  the VectorRegistry snapshots to disk and reloads on restart, so a
  process-kill no longer drops indexes. Once merged, a re-run should take
  valkey to ~99.99% alongside memcache and dyniak.

## Invariant violations

Zero. No data loss within quorum, no orphaned netem qdiscs / tokio tasks /
child processes / faketime overrides after the post-test sweep.
