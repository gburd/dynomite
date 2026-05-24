# 2026-05-23 - Hinted handoff (Riak D1)

## Stage

D1 (the last bounded item in `docs/riak-comparison.md` before the
months-long Riak M1-M9 rollout). Branch: `stage/d1-hinted-handoff`.

## Goal

Implement hinted handoff per the spec in `docs/riak-comparison.md`
section D1 and `docs/riak-compat-plan.md` section 4.3 / M7. A
write whose target peer is in `PeerState::Down` (or whose
outbound channel is closed / full) is recorded as a hint in a
node-local store and counted toward the consistency threshold; a
background drainer replays the hint to the peer once it returns
to `PeerState::Normal`.

## What landed

* **Config**: three new YAML keys on `ConfPool` plus their
  runtime mirrors on `cluster::pool::PoolConfig`:
  - `enable_hinted_handoff: bool` (default `false`).
  - `hint_ttl_seconds: u64` (default `86_400`).
  - `hint_store_max_bytes: u64` (default `64 * 1024 * 1024`).
  - `hint_drain_interval_ms: u64` (default `30_000`).
  Validation rejects zero values when `enable_hinted_handoff`
  is true.

* **Hint store** at `crates/dynomite/src/cluster/hints.rs`. In-
  memory FIFO per peer, byte-cap, TTL expiry. Public types:
  `HintStore`, `Hint`, `HintStoreError`, `HintStoreStats`.
  Nine unit tests cover the round-trip, max-bytes back-
  pressure, TTL expiry under both `take_for` and `expire_now`,
  zero-TTL / empty-payload rejection, mixed-peer independence,
  unbounded mode, and stats accounting.

* **Dispatch integration** in `crates/dynomite/src/cluster/dispatch.rs`:
  - Optional `Arc<HintStore>` carried by `ClusterDispatcher`,
    wired via a new `with_hint_store(...)` builder so embedders
    that do not want hinted handoff pay nothing.
  - `hinted_handoff_active()` predicate gates every change.
  - `collect_routable` now keeps `Down` peers when handoff is
    active AND the request is a write; reads still filter
    them out.
  - The fan-out loop checks each target's snapshot peer state
    and routes Down targets through `hint_target` instead of
    `try_send`. On `try_send` failure (channel closed / full)
    the dispatcher falls back to `hint_target` so a momentarily
    overwhelmed supervisor channel does not turn into a
    no-quorum error.
  - The coalescer is fed a synthetic `+OK\r\n` envelope for
    each hinted target so the consistency threshold is
    reached by the surviving real replies plus the hint
    count.
  - Single-target plans are handled symmetrically:
    `dispatch_replicas_direct` hints when the lone target is
    Down or when its `try_send` fails.

* **Delivery task** in `crates/dynomited/src/server.rs`:
  - `Server::build` constructs an `Arc<HintStore>` (sized from
    `hint_store_max_bytes`) when `enable_hinted_handoff: true`
    and wires it to the dispatcher.
  - `Server::run` spawns `hint_drainer_task` alongside
    `gossip_task`. Each tick: (a) `expire_now`, (b) for every
    non-local peer in `Normal` state, take that peer's hints
    and ship them via the same per-peer outbound channel the
    dispatcher uses, tagged `DmsgType::ReqForward` so the
    receiving peer's `dnode_client_loop` rewrites the parsed
    request's routing tag to `LocalNodeOnly` (no recursive
    fan-out at the destination).
  - On graceful shutdown the drainer is aborted alongside
    the gossip task.

* **Tests**:
  - 9 unit tests in `cluster::hints::tests`.
  - 4 in-process integration tests in
    `crates/dynomite/tests/hinted_handoff.rs` covering:
    1. `dc_quorum_set_with_one_down_replica_stores_and_drains_hint`
       (full happy-path: hint stored, drainer ships it,
       store empty afterwards).
    2. `handoff_off_preserves_legacy_behaviour` (regression
       gate: with `enable_hinted_handoff: false` the
       dispatcher behaviour is unchanged).
    3. `read_with_one_down_replica_does_not_hint`.
    4. `try_send_failure_falls_back_to_hint`.
  - 5 new conf YAML round-trip / validation tests in
    `crates/dynomite/src/conf/pool.rs::tests`.
  - 2 conformance tests in
    `crates/dynomited/tests/conformance/multi_dc.rs`:
    - `dc_quorum_hinted_handoff_enabled_cluster_smoke`
      (ACTIVE): bootstraps an 8-node multi-DC cluster with
      handoff enabled, drives a small workload, asserts no
      panic.
    - `dc_quorum_hinted_handoff_replay_after_restart`
      (`#[ignore]`): kill node 3 mid-workload, restart, and
      check node 3's redis caught up. Marked `#[ignore]`
      because the through-TCP path depends on gossip-driven
      state transitions converging within the test budget;
      see "Limitations" below.

* **Docs**:
  - `docs/book/src/configuration.md` adds a "Hinted handoff"
    section with the four knobs, operational notes, and the
    on-disk-variant deferral.
  - `docs/parity.md` adds a Deviation entry recording that
    the C reference does not implement hinted handoff at the
    proxy layer.

## Test counts

* `cargo nextest run --workspace`: 727 (was 709).
* `cargo test --doc --workspace`: 15 (unchanged).
* Conformance (`--features integration --profile conformance`):
  36 active + 1 ignored (was 35).

## Numbers / sizing

* `HintStore` carries `(peer_idx: u32, payload: Vec<u8>,
  deadline: Instant)`. The byte-cap accounting counts only
  the payload bytes; the surrounding metadata (~24 B per hint)
  is ignored because real-world request bodies dominate.
* The dispatcher's `intermediate_tx` channel is sized at
  `targets.len() + 1`. Hint synthesis pushes one envelope per
  hinted target, well under the cap.
* Drainer interval defaults to 30 s, matching the gossip
  cadence so peer-up transitions are noticed promptly.

## Limitations / deferred follow-ups

1. **On-disk hint store**: v1 is RAM-only. A node restart drops
   every pending hint. The natural follow-up is one
   memory-mapped append-only segment file per peer, replayed
   at startup; sequenced under M7 in `docs/riak-compat-plan.md`.
2. **Synthetic reply shape**: the coalescer is fed `+OK\r\n`
   for every hinted target. This is correct for `SET`-style
   writes and is the dominant case for handoff. Other write
   shapes (`DEL`, `INCR`) may surface the hinted target as
   "divergent" in the coalescer's accounting; the hint is still
   stored and replayed, and read-repair (which only fires for
   `GET`) is unaffected. A future revision can synthesise a
   per-request shape if needed.
3. **Conformance through TCP**: the
   `dc_quorum_hinted_handoff_replay_after_restart` scenario
   is marked `#[ignore]`. The drainer wiring is exercised
   strictly by the in-process integration test
   (`crates/dynomite/tests/hinted_handoff.rs`); the
   end-to-end variant is gated on the gossip + dnode
   secure-server interaction converging on every peer in the
   eight-node multi-DC harness within the test budget. In the
   current wiring only the cross-DC peer pair reliably
   transitions inside the harness window. Operators can run
   the ignored test manually with
   `cargo test --features integration -- --ignored`.
4. **Stats**: the hint store exposes `HintStoreStats` for
   programmatic introspection. Wiring those into the `/stats`
   HTTP endpoint is queued under the post-Stage-15
   observability backlog.

## Rationale notes

* The dispatcher predicate is `hinted_handoff_active() &&
  is_write`. Reads do not hint because there is no value to
  reproduce later. This matches the Riak / Cassandra model.
* `collect_routable` is the cleanest insertion point. The
  alternative (filter Down-peers out of the plan and re-run
  consistency checks at fan-out time) would have meant
  changing the planner's invariants in two places.
* The drainer ships hints using `DmsgType::ReqForward` (not
  `Req`) so the receiving peer's `dnode_client_loop` already
  rewrites the routing tag to `LocalNodeOnly`. This means a
  hinted SET does not re-fan-out at the destination, matching
  the read-repair scheduler.

## Files touched

* `crates/dynomite/src/cluster/dispatch.rs`
* `crates/dynomite/src/cluster/hints.rs` (new)
* `crates/dynomite/src/cluster/mod.rs`
* `crates/dynomite/src/cluster/gossip.rs`
* `crates/dynomite/src/cluster/pool.rs`
* `crates/dynomite/src/conf/pool.rs`
* `crates/dynomite/tests/distributed_tracing.rs`
* `crates/dynomite/tests/gossip_wiring.rs`
* `crates/dynomite/tests/hinted_handoff.rs` (new)
* `crates/dynomite/tests/read_repair.rs`
* `crates/dynomite/tests/stage_10_cluster.rs`
* `crates/dynomited/src/server.rs`
* `crates/dynomited/tests/conformance/mod.rs`
* `crates/dynomited/tests/conformance/multi_dc.rs`
* `docs/book/src/configuration.md`
* `docs/journal/2026-05-23-hinted-handoff.md` (new)
* `docs/parity.md`

## Status

`READY_FOR_REVIEW`.
