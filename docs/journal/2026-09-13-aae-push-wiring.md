# AAE background push wired into dynomited

Date: 2026-09-13
Scope: `crates/dynomited/src/riak.rs`, `crates/dynomited/src/server.rs`,
`crates/model-tests/src/aae.rs`.

## Problem

`spawn_aae` ticked the configured sweep cadence and referenced a
`PeerChannelRepairSink`, but the tick body only logged: it never read
the local store, detected divergence, or dispatched a repair. The
parity ledger tracked this as item 9 (AAE exchange not wired). Two
deeper gaps blocked a full segmented-tree exchange:

1. `spawn_aae` was never handed the concrete `NoxuDatastore`, so it
   could not walk the local keyspace.
2. `PeerChannelRepairSink` shipped only the winner's vclock (not the
   value) as an `AAE:bucket/key:vclock` `ReqForward` payload, and no
   receiver applied those frames -- so even if delivered the peer
   could not reconstruct the object.

A full ROOT/TREE/KEY-SYNC pull exchange needs a bidirectional
request/response peer plane for arbitrary exchange frames, which the
codebase only partially supports (the object read path uses the
`apply_query` -> `Res` write-back seam).

## What was wired

Push-only anti-entropy over the ALREADY-PROVEN repair path. On each
sweep tick, when a local `NoxuDatastore` is present, the task walks a
BOUNDED slice (<= 256/tick) of the local primary keyspace via
`fold_primary` and pushes each object's canonical `SiblingSet` storage
to the tick's peer as a `PeerOp::RepairPut` -- the same op the
read-repair and write-fan paths use, which the receiving peer stores
verbatim and merges idempotently by causal frontier / element-wise max.

This proactively propagates local state a peer may have missed (a
dropped write fan, a lagging replica), converging replicas in the
background without a client read. It does NOT run the full segmented
tree diff (pull + minimal exchange); it pushes full state (bounded per
tick). Because the receiver merges idempotently, pushing more than
strictly diverged is safe -- never a lost or double-counted write, only
less efficient than a tree-diffed exchange.

Wiring: added `RiakHandles.noxu: Option<Arc<NoxuDatastore>>` (populated
from the shared noxu handle at the `build_handles` call site, where
`noxu_shared` is in scope); `spawn_aae` takes it and pushes over the
same per-peer outbound channels the gossip task and hint drainer use.
Without a datastore (a non-noxu build) the task only ticks the cadence.

The old vclock-only `PeerChannelRepairSink` remains as public API with
its doctest but is no longer on the wired path; the push uses the
full-state `RepairPut` op instead, which the receiver actually applies.

## Merge-gate evidence (AGENTS.md 6.5)

Distributed-behaviour change -> DST model AND Elle both required.

**DST (`crates/model-tests/src/aae.rs`):** added a `PushAll(dir)` action
to the existing two-replica AAE model. It offers the source's ENTIRE
keyset (matching the wired push), funnelled through the same
adopt-if-newer apply the pull path uses. Push offers a superset of the
pull diff, so it must converge whenever pull does; the idempotent
adopt-if-newer merge makes re-offering agreed keys a harmless no-op.
Properties held with `PushAll` in the action set:

* convergence (liveness): every terminal path reaches `a == b`;
* no spurious keys, diff-bounded, agreement/divergence reachable
  (safety + non-vacuity).

Negative control (`BrokenAae`) still has teeth with `PushAll` present:
the deterministic lowest-divergent-key skip leaves a divergent fixpoint
the checker catches (`broken_diff_fails_convergence` requires a
`convergence` counterexample to exist). 53 DST models pass.

**Elle (`scripts/consistency/check.sh`):** PASS. Checker self-test
confirms it catches lost-update + dirty-read; the recorded
list-append/register history shows no anomalies of the covered classes
(DUP, G1a, NONMONO, CYCLE). The push reuses the read-repair
`RepairPut` apply, already exercised by this harness.

## Tests

* 53 DST models pass (`cargo test -p model-tests`, `ulimit -v 8388608`).
* dynomited: 80 tests pass (includes `server::tests::build_runs_and_
  shuts_down`, which bootstraps the server with the AAE spawn).
* clippy clean: dynomited `--features riak,wasm`, model-tests.
* Elle consistency gate: PASS.

## Not done (honest scope)

* Full segmented-tree PULL exchange (ROOT/TREE/KEY-SYNC with a minimal
  diff) still needs a bidirectional exchange-frame peer plane. Push-only
  is correct and convergent but ships more bytes than a tree diff would.
* No delta throttling beyond the per-tick cap; a large keyspace
  converges over several ticks.

Ledger item 9 moves from "not wired" to "push wired; tree-diff pull
remains a follow-up".
