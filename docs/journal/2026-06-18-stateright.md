# 2026-06-18 stateright explicit-state model checks (`model-tests` crate)

Stage: `stage/stateright`
Worktree: `/home/gburd/ws/wt-stateright`
Author: Greg Burd <greg@burd.me>

## What landed

A new workspace member, `crates/model-tests`, holding stateright
0.31 explicit-state models of the core distributed protocols. Each
model is an *abstract* state machine that reproduces the production
decision logic and asserts the safety / liveness invariants the real
code must satisfy; the models do not link the production crates, so
the checker's reachable state space stays small and the bounded
BFS/DFS runs in under a second per check.

The crate mirrors the placement and gating intent of
`crates/loom-tests`: a separate, non-default-fast-pass member, run in
CI's slow lane via a dedicated script (`scripts/model.sh`).

## Models (which of the four, with mapping and sizes)

1. **XA two-phase commit** (`src/xa.rs`) -- the priority model.
   Abstracts `crates/dyniak/src/datastore/xa.rs`
   (`XaCoordinator::execute`) and the cross-node leg in
   `crates/dyniak/src/datastore/xa_net.rs`
   (`CrossNodeCoordinator`). Models: prepare -> vote(Ok|Abort),
   commit only on unanimous Ok, presumed abort on any abort vote or
   prepare-phase loss/timeout, durably-prepared branches surviving a
   crash, idempotent commit/rollback, forward commit (never roll back
   a prepared-Ok branch), and the in-doubt re-drive / cold-restart
   recovery (modelled as the coordinator re-sending the decided
   commit). The channel is lossy / reorderable / duplicating with a
   bounded fault budget so the channel becomes eventually reliable
   for the liveness check.
   - Properties asserted (stateright `always` / `eventually` /
     `sometimes`):
     - `atomicity` (always): never one participant committed while
       another rolled back the same transaction.
     - `no commit without unanimous prepare` (always).
     - `durability of prepared` (always): a coordinator commit
       decision implies no participant lost a durable prepared vote
       to a crash.
     - `resolved` (eventually): every terminal path is fully resolved
       (all committed or all rolled back) -- no permanent in-doubt.
     - reachability: `all committed`, `all aborted` (sometimes).
   - **Negative control**: `BrokenXa` commits on a *partial* vote.
     `broken_xa_violates_atomicity` asserts the checker FINDS the
     atomicity counterexample (`discovery("atomicity").is_some()`);
     `faithful_xa_at_control_shape_is_safe` asserts the faithful
     model at the identical shape finds none -- so the violation is
     the bug, not the harness.
   - State-space sizes: faithful `rms=2, faults=2` explores >100
     unique states (asserted); `rms=3, faults=1` larger; broken
     `rms=2, faults=1` for the control. All bounded to finish in a
     few seconds.

2. **Quorum decision** (`src/quorum.rs`). Abstracts
   `crates/dynomite/src/msg/response_mgr.rs` (`ResponseMgr`, quorum
   size `n / 2 + 1`) and the consistency fan-out in
   `crates/dynomite/src/cluster/dispatch.rs` / `pool.rs`. Models a
   request fanned out to `n` replicas, each ack/err in any order up
   to a tolerated failure bound.
   - `no false success` (always): accept only at >= threshold acks
     (`DC_ONE` -> 1, `DC_QUORUM` -> `n/2+1`).
   - `reject only when quorum unreachable` (always).
   - `can accept` (sometimes); reject reachability proven in a
     dedicated config where the failure budget can deny a majority
     (`n=5, max_failures=3`).
   - Configs: `n=3`/`5` DcQuorum, `n=3` DcOne; threshold-formula unit
     test pins `n/2+1` for `n in 1..=5`.

3. **Ring routing** (`src/ring.rs`). Abstracts
   `crates/dynomite/src/cluster/vnode.rs` (`dispatch`: smallest token
   >= key, wrap to first on overflow; preference list = primary +
   distinct successors). State machine applies single join/leave
   membership changes.
   - `coverage` (always): every key -> exactly one primary +
     distinct-owner preference list on a non-empty ring.
   - `determinism` (always): routing is a pure function of key+ring.
   - `bounded disruption` (always): a single join/leave only
     re-routes keys whose primary continuum point is the touched
     token's arc.
   - Plus unit tests: determinism + coverage exhaustive over the key
     domain, wraparound.

4. **Gossip convergence** (`src/gossip.rs`). Abstracts
   `crates/dynomite/src/cluster/gossip.rs`
   (`GossipState::add_or_update`, last-writer-wins on the per-token
   `ts_secs`). Fully-connected (non-partitioned) nodes push views;
   a node adopts a strictly newer version.
   - `convergence` (eventually): every quiescent terminal state is
     fully agreed.
   - monotonic max-version => stable fixpoint.
   - `agreement reachable` / `disagreement reachable` (sometimes).
   - Configs: 3 nodes / 2 changes, 4 nodes / 1 change.

All four models implemented.

## Verification

```
cargo build -p model-tests --locked          # clean
cargo test  -p model-tests --locked          # 16 tests pass; negative control finds its violation
cargo clippy -p model-tests --all-targets -- -D warnings   # clean (pedantic)
cargo fmt   -p model-tests -- --check         # clean
bash scripts/model.sh                         # 16 pass
```

## Footprint / flags for the lead

* **Added `crates/model-tests` to root `Cargo.toml` `members`** (one
  additive line, next to `crates/loom-tests`). Additive and safe;
  flagged for confirmation on merge.
* New files: `crates/model-tests/{Cargo.toml,src/lib.rs,src/xa.rs,
  src/quorum.rs,src/ring.rs,src/gossip.rs}`, `scripts/model.sh`, this
  journal.
* New dependency `stateright = "0.31"` -- in `model-tests` only;
  pulls no production crates into the model graph.
* **Gating recommendation (lead action)**: the loom crate is kept out
  of the default fast pass via `cfg(loom)` body-elision. `model-tests`
  cannot use that trick (its tests run unconditionally), so to keep
  `cargo nextest run --workspace` from picking it up the lead may add
  `package(model-tests)` to the `not (...)` `default-filter` in
  `.config/nextest.toml`. I left `.config/nextest.toml` untouched
  (out of my stated footprint); the dedicated `scripts/model.sh`
  runner names the package explicitly so the slow lane works either
  way. The full suite is ~0.7s, so running in `--workspace` is also
  harmless if the lead prefers no nextest change.

## Notes

* `ReadOnly` votes from the production prepare path are omitted from
  the XA model: a read-only branch is already resolved and never
  disagrees with a committed/aborted peer, so it adds no atomicity
  decision. Documented in `xa.rs` module docs.
* `Model::next_state` must return `Option`; the quorum/ring/gossip
  models return `None` to prune redundant transitions (no-op merges,
  exhausted budgets), which also keeps the state space tight.
* No `#[allow(...)]`, no `unsafe`, ASCII only, no port-acknowledgement
  comments; each module's docs frame the mapping as "models the
  protocol implemented in <path>".
