# 2026-05-25: Failure-cause metrics for gossip + dispatch churn

## Scope

Instrument peer-state oscillation under chaos so an operator
(or the chaos team) can validate whether the pass-3 14% drop
in success rate is gossip phi-accrual settling or something
else.

This entry implements the brief in
`docs/post-chaos-queue.md` section P3-2.5 and follows the
revised pass-3 narrative in
`docs/journal/2026-05-25-token-coverage-validation.md`: ring
gaps are structurally impossible under Dynomite's wrap-around
vnode dispatch, so the dispatcher's `NoTargets` plan must be
the result of runtime liveness, not a static config gap.

## What landed

### `crates/dynomite/src/stats/failure.rs`

New module exposing:

* `FailureMetrics` - the live, mutable accumulator shared
  by the dispatcher and the gossip handler. Holds a single
  `parking_lot::Mutex` over a `FailureInner` struct of
  `HashMap`-backed counters and gauges.
* `FailureSnapshot` - an immutable, `Clone`-able snapshot
  embedded into the existing `Snapshot` type. The aggregator
  refreshes it once per second.
* Per-cause counters:
  * `record_no_targets(dc, rack, consistency)`
  * `record_peer_send_full(peer_idx, peer_dc)`
  * `record_peer_send_closed(peer_idx, peer_dc)`
  * `record_backend_send_full()`
  * `record_backend_send_closed()`
  * `record_response_timeout(consistency)`
* Per-peer state machinery:
  * `record_peer_state_transition(idx, dc, rack, from, to)`
  * `observe_peer_state(idx, dc, rack, state)`
  * `observe_phi(idx, dc, rack, phi)`

The phi gauge is rendered in thousandths (`_milli` suffix on
the Prometheus name) so the value survives the integer-gauge
wire format without losing precision around the default
suspicion threshold (`phi > 8.0` becomes
`gossip_phi_score_milli > 8000`).

### `crates/dynomite/src/cluster/dispatch.rs`

* `ClusterDispatcher::with_failure_metrics(Arc<FailureMetrics>)`
  builder. When `None` the dispatcher's behaviour is unchanged.
* Counter ticks at every error-producing branch:
  * `plan()` -> `dispatch_no_targets_total` for both the
    early-return branches and the post-`plan_with_consistency`
    fall-through.
  * `try_send` against the local backend
    (`DispatchPlan::LocalDatastore` arm of the dispatcher)
    splits `Full` and `Closed` into the two backend counters.
  * `fanout_send` (multi-target replicas) and
    `dispatch_replicas_direct` (single-target replicas) call
    the new `observe_send_error` helper which routes
    `Full`/`Closed` to either the per-peer or the backend
    counters, depending on `target.is_local`.
  * `coalesce_actor` records a `dispatch_response_timeout`
    when every per-target sender drops without producing a
    reply (the existing "no-reply, channel closed" path).

### `crates/dynomite/src/cluster/gossip.rs`

* `GossipHandler::with_failure_metrics(Arc<FailureMetrics>)`
  builder.
* `evaluate(now)` updates the `gossip_phi_score` gauge for
  every non-local peer on every tick and emits
  `peer_state_transitions_total` plus `peer_state_current`
  on every state flip.
* `record_heartbeat_pname`, `record_heartbeat_idx`, and
  `mark_down_pname` also emit transitions when they flip a
  peer's state (gossip's first-contact promotion path and the
  shutdown-time mark-down path are both covered).

### `crates/dynomite/src/stats/prometheus.rs`

New rendering helpers (`register_failure_metrics`,
`register_failure_no_targets`, ...) that emit one
`# HELP`/`# TYPE` block per metric family with the labels
listed above. The renderer is purely additive: existing
metrics still appear in the same order they did before.

### `crates/dynomited/src/server.rs`

* The binary creates a single `Arc<FailureMetrics>` at
  startup and plumbs it into both the dispatcher and the
  gossip handler.
* A new periodic task copies the latest `FailureMetrics`
  snapshot into the `Arc<Mutex<Snapshot>>` the
  `StatsServer` reads from. Cadence is 1s; the task is
  aborted on shutdown alongside the entropy task.

### Tests

Three new test bodies (eight total assertions, plus all the
unit tests inside `failure.rs`):

* `cluster::dispatch::tests::no_targets_records_failure_metric`
  drives a single-peer-Down dispatcher and asserts the
  per-`(dc, rack, consistency_level)` counter ticks by one.
* `cluster::dispatch::tests::closed_backend_channel_records_closed_metric`
  wires a closed mpsc channel as the dispatcher's backend,
  fires one request, and asserts
  `dispatch_backend_send_closed_total` is 1 and
  `dispatch_backend_send_full_total` is 0.
* `cluster::gossip::tests::handler_evaluate_records_normal_to_down_transition`
  drives the handler through 100 heartbeats, then advances 60
  seconds with no heartbeats, and asserts a single
  `(from=Normal, to=Down)` counter tick with peer_idx 1, the
  `peer_state_current` gauge reflecting `Down`, and the
  `gossip_phi_score` gauge populated.
* `cluster::dispatch::tests::two_peer_pool_with_one_down_records_per_key_no_targets`
  is the integration-style test the brief asked for: build a
  two-peer pool, kill one, drive 100 dispatches, and assert
  the counter total matches the planner's NoTargets count.
* The eight unit tests inside `stats::failure::tests` cover
  the accumulator's labelled-counter, gauge-update,
  empty-snapshot, and rounding paths.

## Files touched

* `crates/dynomite/src/stats/failure.rs` (new, 8 unit tests).
* `crates/dynomite/src/stats/mod.rs` (re-exports + Stats wiring).
* `crates/dynomite/src/stats/snapshot.rs` (Snapshot field).
* `crates/dynomite/src/stats/prometheus.rs` (rendering).
* `crates/dynomite/src/cluster/dispatch.rs` (counter ticks +
  3 new tests).
* `crates/dynomite/src/cluster/gossip.rs` (transition emit +
  1 new test).
* `crates/dynomited/src/server.rs` (binary wiring +
  refresher task).
* `docs/book/src/operations/metrics.md` (Failure-cause
  counters section).
* `dist/chaos-reports/v0.1.0/multi-host-pass-3-redis.md` (new,
  pass-3 retrospective).
* `docs/journal/2026-05-25-gossip-churn-metrics.md` (this
  entry).

## Test summary

* `cargo nextest run --workspace`: 1229 -> 1239 (+10 new).
* `cargo test --doc -p dynomite`: 612 doctests pass.
* `cargo clippy --workspace --all-targets --all-features --
  -D warnings`: clean.
* `bash scripts/check_no_todos.sh && check_no_port_comments.sh
  && check_ascii.sh`: clean.

## Pass-3 narrative link

The companion retrospective is at
`dist/chaos-reports/v0.1.0/multi-host-pass-3-redis.md`. The
short version is that the 14-point success-rate drop is most
plausibly attributed to gossip phi-accrual settling between a
peer being killed by the chaos injector and the local
dispatcher reclassifying it as Down. The metrics shipped here
will let pass-4 quantify that hypothesis directly:
`dispatch_no_targets_total` should track 1:1 against the
chaos-events ndjson `kill` lines plus an offset measured in
seconds (the settling window), and
`peer_state_transitions_total` should fire exactly twice per
kill cycle (Normal->Down on detection,
Down->Normal on first-heartbeat-after-restart).

## Default-behaviour invariant

When no `FailureMetrics` handle is wired, every method on the
dispatcher and gossip handler is a single `Option::is_some`
test that short-circuits to a noop. The counters initialise
to zero and only become observable once an operator queries
the `/stats` or `/metrics` endpoint. No existing tests had to
change.
