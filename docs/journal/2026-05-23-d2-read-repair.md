# 2026-05-23 - D2 reply coalescer + read repair

**Stage**: docs/riak-comparison.md item D2.
**Branch**: `stage/d2-read-repair-coalescer`.

## What

Implement the per-replica reply coalescer and the read-repair
scheduler.

* `crates/dynomite/src/proto/redis/coalesce.rs` grows a
  `CoalesceTracker` state machine. The tracker is keyed by the
  responding peer's index, compares replies via byte-equal
  payload, and reports `Ready { winner, divergent_targets }` /
  `Pending` / `Error(reason)` per the consistency level. 14 new
  unit tests cover every consistency level (`DC_ONE`,
  `DC_QUORUM`, `DC_SAFE_QUORUM`, `DC_EACH_SAFE_QUORUM`) and
  every divergence shape (all-agree, one-divergent, all-
  divergent, intra-DC divergent, inter-DC divergent).
* `crates/dynomite/src/cluster/dispatch.rs::ClusterDispatcher::dispatch`
  for multi-replica plans now spawns a `coalesce_actor` task on
  the ambient tokio runtime. The actor wraps the client-facing
  responder: per-target replies feed the tracker, and only the
  coalesced reply propagates to the client. Single-target plans
  short-circuit through a direct forward (no actor).
* On `CoalesceOutcome::Ready` with a non-empty
  `divergent_targets` set, the actor calls `schedule_read_repair`
  which builds a `SET key <winner-value>` (or `DEL key` for nil
  bulks) and ships the wire bytes through the divergent target's
  `peer_backends` channel. Repair traffic is tagged
  `DmsgType::ReqForward`; the receiving peer's
  `dnode_client_loop` rewrites the parsed request's routing tag
  to `MsgRouting::LocalNodeOnly`, preventing a recursive multi-
  replica fan-out at the divergent peer.
* New per-target identity plumbing: `OutboundRequest` grows
  `target_peer_idx: Option<u32>`; `OutboundEnvelope` grows
  `source_peer_idx: Option<u32>`. The local server / dnode-
  server drivers copy the field through.
* Integration test in
  `crates/dynomite/tests/read_repair.rs` exercises the in-
  process three-replica case (one divergent value, asserts the
  client sees the majority and replica 0 receives a
  `ReqForward SET key v2`).
* Conformance scenario added in
  `crates/dynomited/tests/conformance/multi_dc.rs::dc_quorum_read_repair_round_trip`:
  through-TCP `SET` + `GET` round-trip across the 8-node multi-
  DC topology under `DC_QUORUM`, asserting reply shape rather
  than per-replica state.

## Limitations (recorded under `docs/parity.md` as a Deviation)

* Winner selection is by vote count + lowest-`peer_idx`
  tiebreak, not per-write timestamp. The C reference's Lua-
  rewrite timestamp path remains a future deliverable.
* Repair scheduler handles only single-key Redis `GET` reads
  whose winning reply is a bulk string or nil bulk. Other shapes
  (integers, multibulks, errors) are skipped; entropy
  reconciliation handles them.
* The repair scheduler is fire-and-forget; replies are dropped.

## Tests

* `cargo nextest run --workspace`: 707 -> 709 pass (added 2
  read-repair scenarios). 14 new coalesce unit tests inside the
  709 figure.
* `cargo test --doc -p dynomite`: 584 pass.
* `cargo nextest run -p dynomited --features integration --test
  conformance --profile conformance`: 34 -> 35 pass (added
  through-TCP read-repair round-trip).
* `cargo clippy --workspace --all-targets --all-features --
  -D warnings`: clean.
* `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool --
  --check`: clean.
* `bash scripts/check_no_todos.sh`,
  `scripts/check_no_port_comments.sh`,
  `scripts/check_ascii.sh`: clean.

## Files touched

```
crates/dynomite/src/cluster/dispatch.rs
crates/dynomite/src/net/client.rs
crates/dynomite/src/net/dispatcher.rs
crates/dynomite/src/net/dnode_client.rs
crates/dynomite/src/net/dnode_server.rs
crates/dynomite/src/net/server.rs
crates/dynomite/src/proto/redis/coalesce.rs
crates/dynomite/src/proto/redis/mod.rs
crates/dynomite/src/embed/server.rs
crates/dynomite/tests/distributed_tracing.rs
crates/dynomite/tests/read_repair.rs (new)
crates/dynomite/tests/stage_09_net.rs
crates/dynomite/tests/stage_10_cluster.rs
crates/dynomited/src/server.rs
crates/dynomited/tests/conformance/multi_dc.rs
docs/parity.md
docs/journal/allowances.md
```

## Notes

* `DispatchPlan::Replicas` was widened from `Replicas(Vec<...>)`
  to `Replicas { targets, consistency }` so the dispatcher does
  not have to recompute the consistency level when wiring the
  coalescer. All tests / embed / server callers were updated.
* `CoalesceOutcome::Ready.winner` is `Box<Msg>` (not `Msg`) to
  keep the enum small (`Pending` and `Error` are <32 bytes).
  Clippy's `large_enum_variant` would otherwise fire.
* The coalesce task uses `mpsc` with capacity `targets.len() +
  1` so even a same-tick straggler reply has a slot.
* No unimplemented!/todo!/stubs introduced. No port-from-C
  comments. ASCII-only.
