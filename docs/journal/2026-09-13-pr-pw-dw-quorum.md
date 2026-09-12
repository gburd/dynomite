# 2026-09-13 -- PR / PW / DW quorum enforcement (gap/pr-quorum)

Closes item 8 from `2026-07-24-dyniak-riak-parity-audit.md`'s work queue:
PR, PW, and DW were accepted on the wire and echoed on `RpbGetBucketResp`
but never enforced, because the substrate had two real gaps: no
primary-vs-fallback distinction in the replica plan, and no durability
signal out of the datastore. Both gaps are closed; PR/PW/DW are now
enforced end to end.

## Part A -- sloppy-quorum substrate

`dynomite::cluster::ReplicaTarget` gained `pub is_fallback: bool`. Every
existing literal (`dyniak::replication::plan_successors`, one synthetic
empty-ring primary, `dynomite::cluster::dispatch::build_target`) sets it
`false`: the topology dispatch path and the no-liveness successors
planner have no fallback concept, so every target they name is a
primary owner (matches the parity audit's original framing -- PR would
otherwise alias R with no real distinction).

The real counting path lives in a new liveness-aware planner:
`dyniak::replication::{ReplicaLiveness, plan_replicas_with_liveness}`.
Given a liveness oracle, it walks the ring, drops a down primary-window
peer, and backfills the first live peer past the `n_val` window as a
fallback stand-in (`is_fallback = true`). `BucketRouter::with_liveness`
wires an oracle into the router's `try_route`; with no oracle attached
(the current production wiring -- no liveness source exists yet, same
constraint the audit called out), routing is unchanged. This is real
plumbing exercised by tests (`replication.rs` doctests +
`quorum_enforcement.rs`'s `spawn_with_liveness` fixture), not a
placeholder.

`RouteDecision::primary_replica_count()` reports the non-fallback count
in a replica list; the request handlers use it to size PR/PW's ceiling.

## Part B -- durability ack

`NoxuDatastore::commits_durably()` reports whether the environment's
sync policy makes a committed write durable (`Sync` / `WriteNoSync`;
only `NoSync` is not). `dyniak::datastore::write_is_durable()` probes a
`Datastore` via `as_any()` for a `NoxuDatastore` and defers to it;
absent that (a non-noxu backend, or a build without the `noxu`
feature), a successful `riak_put` is treated as durable by default --
the `Datastore::riak_put` contract already promises "applied or Err",
so "cannot inspect" is not evidence against durability.

The ack byte carried by a `PeerOp::RepairPut` reply is now
`router::ACK_STORED` (landed) or `router::ACK_STORED_DURABLE` (landed
and durable); `ReplicaApplier::apply_query` picks the byte via
`write_is_durable`.

## Part C -- enforcement

`bucket_props.rs::effective_dw` mirrors `effective_w` (DW defaults to
`quorum`, unlike PR/PW's `0`). `server.rs`:

* `handle_get` -> `fan_read_replicas` / `check_read_quorum`: counts
  total responses (R) and primary-only responses (PR) separately;
  fails with "primary read quorum not met" when PR is short, same
  availability fallback as R on a fire-and-forget transport.
* `handle_put` -> `resolve_and_store_put` / `fan_write_replicas` /
  `check_write_quorum`: counts total acks (W), primary acks (PW), and
  durable acks (DW) separately; fails on any of the three that is
  short.

Both handlers stayed under the 100-line budget by extracting the fan
and quorum-check helpers (`handle_get` 96 lines, `handle_put` 83 lines
after the refactor).

## DST model (AGENTS.md 6.5)

`model-tests::quorum_decision` gained `SubsetQuorumModel`: the shared
shape behind PR/PW/DW ("count total acks against `quorum`; separately
count SUBSET acks against `subset_quorum`; both must clear"). Negative
control `fallback_counts_rule` counts every ack (subset or not) toward
the subset quorum -- exactly "a fallback response counts toward PR" --
and is shown to violate sound-success.

## Tests

6 new integration tests in `quorum_enforcement.rs`: 2 DW (fails when an
ack is not durable; succeeds when all are), 2 PW, 2 PR (fails when
only a fallback acks/answers; succeeds once enough primaries do).
`dyniak` nextest: 891/891 (was 885; +6). `model-tests`: 53/53 (was 51;
+2 new subset-model tests). Doctests, clippy (`-p dyniak --features
noxu`, `-p dyniak --features noxu,wasm`, `-p dynomited --features
riak`, `-p dynomite-engine`), and `cargo fmt --all -- --check` all
clean.

## Scope note

`ReplicaTarget` lives in `crates/dynomite`, outside the
`crates/dyniak` + `crates/dynomited` boundary this task was scoped to.
The brief explicitly named the field addition and "fix every
`ReplicaTarget {}` literal", so this was treated as a deliberate,
minimal exception: only the one field and its two literal sites in
`dynomite::cluster::dispatch` were touched, nothing else in that
crate.
