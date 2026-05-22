# 2026-05-22 - Prometheus /metrics for dynomited

Stage: prometheus-metrics
Branch: feat/prometheus-metrics
Author: prometheus-metrics worker

## Summary

Adds a Prometheus 0.0.4 text-exposition endpoint to the existing
StatsServer. The same cached Snapshot is now served as both legacy
JSON (under `/`, `/info`, and the new `/stats` alias) and as
Prometheus text (under `/metrics`).

The work is split across four atomic commits:

  1. `build(deps): add prometheus crate` - adds `prometheus = "0.13"`
     at the bottom of `[workspace.dependencies]` and wires it into
     `crates/dynomite`.
  2. `feat(stats): Prometheus text exposition for Snapshot via
     render_prometheus` - new `crates/dynomite/src/stats/prometheus.rs`
     with the public `render_prometheus(&Snapshot) -> String` function
     and 5 unit tests.
  3. `feat(stats): expose /metrics on stats_listen alongside /stats`
     - routes `/metrics` (and the `/stats` alias) through the
     existing HTTP server with the
     `text/plain; version=0.0.4; charset=utf-8` Content-Type, plus
     2 integration tests covering both the new metrics path and the
     unchanged JSON body.
  4. `docs(stats): operations/metrics.md - Prometheus output
     reference` - new mdBook chapter and SUMMARY.md entry.

## Files touched

* `Cargo.toml` (added `prometheus = "0.13"` workspace dep)
* `Cargo.lock` (resolver update, 3 transitive crates)
* `crates/dynomite/Cargo.toml` (pulled the workspace dep)
* `crates/dynomite/src/stats/mod.rs` (registered the new module and
  re-exported `render_prometheus`)
* `crates/dynomite/src/stats/prometheus.rs` (new, 478 lines incl.
  tests and doctests)
* `crates/dynomite/src/stats/rest.rs` (added `/stats` alias and
  `/metrics` route + `write_metrics_response` helper)
* `crates/dynomite/tests/stage_05_stats.rs` (added
  `metrics_endpoint_returns_prometheus_text` and
  `stats_endpoint_unchanged`)
* `docs/book/src/SUMMARY.md` (linked the new chapter)
* `docs/book/src/operations/metrics.md` (new chapter)

## Test counts

Workspace before this branch: 619 tests.
Workspace after: 626 tests. All 626 pass under
`cargo nextest run --workspace --no-fail-fast`.

New tests:

  - `dynomite stats::prometheus::tests::render_prometheus_includes_help_and_type_lines`
  - `dynomite stats::prometheus::tests::render_prometheus_quotes_label_values`
  - `dynomite stats::prometheus::tests::render_prometheus_emits_build_info`
  - `dynomite stats::prometheus::tests::render_prometheus_includes_server_counters_and_uptime`
  - `dynomite stats::prometheus::tests::render_prometheus_emits_peer_state_for_server`
  - `dynomite::stage_05_stats::metrics_endpoint_returns_prometheus_text`
  - `dynomite::stage_05_stats::stats_endpoint_unchanged`

That is +7 tests over the 619 baseline (5 unit + 2 integration).

## Sample /metrics output

The first few lines of `render_prometheus` for a Snapshot with one
pool, one server, three live connections, and 100 reads:

```
# HELP dynomite_build_info Static identification of the running engine; value is always 1.
# TYPE dynomite_build_info gauge
dynomite_build_info{dc="dc-1",rack="rack-1",source="node-a",version="0.0.1"} 1
dynomite_peer_state{peer="redis_local",state="up"} 1
dynomite_pool_client_connections{pool="dyn_o_mite"} 3
dynomite_pool_client_read_requests_total{pool="dyn_o_mite"} 100
dynomite_server_read_requests_total{server="redis_local"} 100
dynomite_uptime_seconds 42
```

## Verification

  - `cargo build --workspace --all-targets`: clean (one pre-existing
    dead_code warning in `stage_11_entropy`, unrelated).
  - `cargo nextest run --workspace --no-fail-fast`: 626 passed,
    1 skipped.
  - `cargo test --doc --workspace`: all doctests pass, including the
    new `render_prometheus` doctest.
  - `rustfmt --check` on every file this branch touches: clean.
  - `cargo clippy --workspace --all-targets -- -D warnings`: clean
    for every file this branch touches. Pre-existing clippy errors
    in `crates/dynomite/src/cluster/failure_detector.rs`,
    `crates/dynomite/src/cluster/dispatch.rs`, and
    `crates/dynomite/src/net/proxy.rs` were verified to be present
    before this branch via `git stash` + clippy on the unstashed
    base commit; they belong to other workers' scopes per
    AGENTS.md and were not touched.

## Worker contract

```
STAGE: prometheus-metrics
STATUS: READY_FOR_REVIEW
BRANCH: feat/prometheus-metrics
JOURNAL: docs/journal/2026-05-22-prometheus-metrics.md
TESTS: 626 passed (was 619)
```
