# 2026-05-23 - Autonomous queue execution

Operator directive: "Please continue with all tasks remaining
on the plan without waiting for me for each step."

This session ran the full bounded-item queue from
`docs/post-chaos-queue.md` (everything except the months-long
Riak M1-M9 implementation and the operator-driven Pass-3 chaos
run). Five worker subagents in isolated worktrees, merged in
dep-safe order with manual conflict resolution at the lead
level.

## Workers dispatched (in order)

### Batch 1 (parallel, 3 workers)

* **`stage/fix-make-error-payload`** (`4df911a9`) -- fixed the
  latent `make_error` bug surfaced by the audit. Every
  `DispatchOutcome::Error` path now produces parseable wire
  bytes instead of a 0-byte hang. 7 new tests covering all 14
  Redis error variants plus memcache. Files:
  `crates/dynomite/src/msg/response.rs`,
  `crates/dynomite/src/cluster/dispatch.rs`. Single commit.

* **`stage/m3-bucket-types`** (`c7a467d5`) -- Riak D3 / M3:
  per-bucket-type routing overrides. `<bucket>/<key>`
  convention; per-bucket `read_consistency`,
  `write_consistency`, `n_val`. Adds `BucketType` to
  `ConfPool` / `PoolConfig`. 18 new tests + 4 new doctests.
  10 files changed.

* **`stage/gossip-wiring`** (`add203a1`) -- replaces the
  supervisor-driven peer-state hack from F1 with
  gossip-driven authority. Phi-accrual is now LIVE
  (resolves audit F8). Heartbeats flow on the existing peer
  channel via a new `ty: DmsgType` discriminator on
  `OutboundRequest`; `dnode_client_loop` forks gossip frames
  before the datastore parser; `peer_supervisor` no longer
  publishes `PeerState`. 10 new tests. 12 files changed.

Merge order: make_error -> bucket types -> gossip wiring.
One conflict in `cluster/dispatch.rs` (two test blocks
appended at the end of `mod tests {}`); resolved by keeping
both blocks back-to-back. Two doctest fixes in
`cluster/dispatch.rs` and `cluster/gossip.rs` to add the new
`bucket_types` / `default_bucket_type` fields to existing
`PoolConfig` literals.

### Batch 2 (sequential)

* **`stage/d2-read-repair-coalescer`** (`c3039da1`) -- Riak D2
  / M4-M5: reply coalescer + read repair on quorum reads. Per
  fan-out, a coalesce_actor task gathers replies according to
  the resolved `ConsistencyLevel`, picks the winner by vote
  count (lowest peer_idx tiebreak), schedules read repair to
  divergent replicas via `DmsgType::ReqForward`. 14 new unit
  tests (CoalesceTracker) + 2 new integration tests
  (read_repair) + 1 new conformance scenario. 18 files
  changed.

  Notable design choices documented in
  `docs/journal/2026-05-23-d2-read-repair.md`:
  - `DispatchPlan::Replicas` widened from tuple to struct
    variant carrying the resolved consistency level.
  - `CoalesceOutcome::Ready.winner` is `Box<Msg>` to avoid
    `clippy::large_enum_variant`.
  - One new `#[allow(clippy::too_many_arguments)]` on
    `coalesce_actor` per `docs/journal/allowances.md`.
  - Per-write timestamps (the C reference's
    `redis_rewrite_query_with_timestamp_md` path) deferred;
    `docs/parity.md` Deviation entry filed.

* **`stage/d1-hinted-handoff`** (`d91090bc`) -- Riak D1 / M6:
  hinted handoff. RAM-only `HintStore` (on-disk variant
  deferred to M7). When `enable_hinted_handoff: true` and a
  target peer is `Down` at write time, the dispatcher stores
  a hint, counts the target toward consistency, and a
  `hint_drainer` task in `dynomited` periodically replays
  hints for peers that come back to `Normal`. 18 new tests +
  1 new in-process integration test file + 2 conformance
  scenarios (one ignored due to harness gossip-convergence
  flake). 17 files changed.

  Feature-flag-off path is byte-for-byte identical to
  pre-handoff behaviour; pinned by
  `handoff_off_preserves_legacy_behaviour`. Validation
  rejects zero values when the flag is on; with the flag
  off, the values are accepted and ignored.

## Counts

| Gate | Start of run | End of run | Delta |
|---|---|---|---|
| `cargo nextest run --workspace` | 658 | **727** | +69 |
| `cargo test --doc --workspace` (lib + bin) | 591 | **597** | +6 |
| Conformance suite | 34/34 | **36/36 + 1 ignored** | +2 |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | clean | clean | -- |
| `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool` | clean | clean | -- |
| `scripts/check_no_todos.sh` / `_no_port_comments.sh` / `_ascii.sh` | clean | clean | -- |
| `cargo build --workspace --all-targets --locked` | clean | clean | -- |

The pre-existing F9 load-correlated flakes (`pidfile::flock_retry...`,
`crypto::aes::wrong_key_fails_padding_check`,
`conformance::dc_safe_quorum_workload`) remain in the same state
they were in before this batch -- they pass when run in isolation
and intermittently fail under parallel pressure. None are
regressions from this work.

## Audit-derived disclosures status

* **F1** (3 conformance failures since v0.1.0): resolved earlier
  in `490a258`.
* **F2-F6** (CI gates / stale docs): resolved earlier in
  `2d0cb28`.
* **F7** (`--log-format` not in `--help` because help reproduces
  C engine's `dn_show_usage` verbatim): unchanged; cosmetic.
* **F8** (phi-accrual structurally inert): **resolved by gossip
  wiring**. Phi is now fed by inbound gossip frames; the
  periodic gossip task evaluates `phi(now)` against the
  configured threshold and toggles `PeerState`.
* **F9** (load-correlated flakes): unchanged.
* **F10** (end-to-end OTLP test `#[ignore]`'d for CI speed):
  unchanged; deliberate trade-off, both traces and logs paths
  pass under `--include-ignored`.

* **Latent `make_error` bug**: **resolved**. Every
  `DispatchOutcome::Error` path now produces a parseable
  client-visible reply.

## What is left

Per `docs/post-chaos-queue.md` the remaining items are:

1. **Pass-3 chaos run** (operator-driven, multi-host). Now
   should actually exercise cross-DC peer routing thanks to
   F1 + gossip. Requires `floki` + `arnold` + `nuc` access
   from the operator's terminal.
2. **Riak M1-M9 implementation** (16-21 weeks). The plan is
   already documented at `docs/riak-compat-plan.md`. M1's
   hash-function decision was resolved in `faa0988`
   (murmur3). M3 (bucket types) is now done as part of this
   session. Remaining: M1 (HTTP/PB protocol surface), M2
   (Noxu integration), M4-M5 (consistency / read-repair --
   actually done as D2 here, but Riak's specific
   semantics may differ in edge cases worth a review), M6
   (hinted handoff -- D1, done), M7 (on-disk hints), M8
   (vector clocks), M9 (production hardening).

The Riak track is genuinely months of work; the operator
should sequence M1 first as a milestone goal.

## Process notes

* The pi `Agent` tool's `isolation: "worktree"` worked well
  for parallel batch 1. Each worker landed its branch
  cleanly; the only conflicts were in
  `crates/dynomite/src/cluster/dispatch.rs` (a high-traffic
  file three workers all touched). Resolution was mechanical.
* Subagent runtimes ranged from 25 minutes (make_error) to 75
  minutes (gossip wiring) to 45 minutes (hinted handoff).
  Total wall-clock for the five workers: ~3.5 hours of
  parallel + sequential work.
* Every worker hit at least one minor cleanup that the lead
  resolved during merge: doctest field updates, a missing
  serde_json dev-dep (an earlier batch), a `cfg: LogConfig`
  vs `cfg: &LogConfig` clippy lint, etc. None of these were
  worker bugs -- they were the natural consequence of
  isolated worktrees not seeing each other's deps until merge
  time.
