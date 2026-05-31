# 2026-05-24 -- dyn-admin cluster admin RPCs

## Summary

Shipped the four deferred `dyn-admin` cluster-mutation
subcommands (`cluster-join`, `cluster-leave`, `cluster-plan`,
`cluster-commit`) end-to-end and replaced the single-peer
`cluster-list` fallback with a real multi-peer discovery path.
The substrate gained a new `dynomite::cluster::admin_rpc`
module that exposes the mutation surface as the `ClusterAdmin`
trait; the PBC server learned five new request/response code
pairs (200-209) the trait hooks into.

Branch: `stage/dyn-admin-cluster-rpcs`.

## Substrate hooks (`crates/dynomite/src/cluster/admin_rpc.rs`)

Public surface added:

* `PeerSnapshot` -- `(idx, dc, rack, host, port, tokens, state,
  is_local)`. Returned by `list_peers`.
* `PeerSpec` -- the inverse of `PeerSnapshot`; describes a peer
  to add to the cluster.
* `ClusterChange { kind: ClusterChangeKind, peer_idx, peer }` --
  one staged mutation. `ClusterChangeKind` is `Add` or `Remove`.
* `JoinPlan { change }` -- thin wrapper returned by
  `cluster_join` and `cluster_leave`.
* `ClusterError` -- `PeerNotFound`, `CannotRemoveLocal`,
  `PeerAlreadyExists`, `Invalid`.
* `trait ClusterAdmin` -- `list_peers`, `cluster_join`,
  `cluster_leave`, `cluster_plan_pending`, `cluster_commit`.
  Object-safe; sent across the PBC server boundary as
  `Arc<dyn ClusterAdmin>`.
* `NoopClusterAdmin` -- always-empty / always-rejecting
  implementation for embeddings that do not opt in.
* `PoolClusterAdmin` -- the production implementation; holds an
  `Arc<ServerPool>` and a `parking_lot::Mutex<Vec<ClusterChange>>`
  staging list.

`PoolClusterAdmin::cluster_commit` takes the pool's `peers` and
`auto_eject` write-locks together, applies every staged change
in order, drops the locks, and calls `ServerPool::rebuild_ring`
once so the dispatcher routes against the post-mutation ring.
Mutations include:

* Add: append a fresh `Peer` with a freshly-allocated
  `peer_idx = peers.len()`, an `AutoEject` cloned from the
  pool config's defaults, and the inherited dc/rack from the
  local peer (or the pool config when no local peer is
  present).
* Remove: locate the position of the target peer by `idx()`
  (NOT vec position) and remove the peer plus its parallel
  `AutoEject` entry. Local peers are rejected up front.

Token allocation when the operator does not supply a value is
deterministic per `(host, port)` via FNV-1a. Token rebalancing
is left to a follow-up slice.

## PBC ops (`crates/dyniak/src/proto/pb/messages.rs`)

Five request/response pairs added under codes 200-209:

| Code | Type                       |
|-----:|----------------------------|
| 200  | `DynRpbListPeersReq`       |
| 201  | `DynRpbListPeersResp`      |
| 202  | `DynRpbClusterJoinReq`     |
| 203  | `DynRpbClusterJoinResp`    |
| 204  | `DynRpbClusterLeaveReq`    |
| 205  | `DynRpbClusterLeaveResp`   |
| 206  | `DynRpbClusterPlanReq`     |
| 207  | `DynRpbClusterPlanResp`    |
| 208  | `DynRpbClusterCommitReq`   |
| 209  | `DynRpbClusterCommitResp`  |

Two helper messages: `DynRpbPeerInfo` (the peer record, embedded
in list-peers responses and in the staged `peer` of an Add
change) and `DynRpbStagedChange` (the wire shape of a
`ClusterChange`). Append-only at the end of `messages.rs`. New
`MessageCode` variants appended at the end of the enum and to
the `from_u8` match. Both `proto/pb/mod.rs` and the crate root
`lib.rs` got append-only `pub use` re-exports.

## Server wiring (`crates/dyniak/src/server.rs`)

`process_frame` now takes `(frame, datastore, admin)`. New
public functions `serve_pbc_with_admin`,
`serve_pbc_tls_with_admin`, and `handle_conn_with_admin`
accept an `Arc<dyn ClusterAdmin>`; the existing `serve_pbc`,
`serve_pbc_tls`, and `handle_conn` keep their signatures and
internally use `Arc::new(NoopClusterAdmin)` so older callers
(including the `dynomited` binary's PBC bind path and the
existing ping integration test) continue to compile and run.

## dyn-admin (`crates/dyn-admin/`)

* `cluster_list.rs` rewritten to call the new
  `DynRpbListPeersReq` and render every returned peer.
* New modules `cluster_join.rs`, `cluster_leave.rs`,
  `cluster_plan.rs`, `cluster_commit.rs`. Each follows the
  existing one-module-per-subcommand pattern.
* `output.rs` got `PeerSpec`, `StagedChange`, and
  `render_change_human` so the four mutation subcommands share
  a single rendering helper for both human and JSON modes.
* `main.rs` extended with the four new clap subcommands; the
  existing `cluster-list` subcommand now uses `--seed` against
  a real PBC peer-list call rather than the previous
  server-info fallback.
* `README.md` updated: subcommand catalogue extended, the
  "Deferred subcommands" section replaced with a
  "Cluster mutation flow" cookbook.

## Tests

Diff vs main:

* nextest workspace: 1115 -> 1149 (+34).

New test surfaces:

* `crates/dynomite/src/cluster/admin_rpc.rs` -- 11 unit tests
  covering list/join/leave/plan/commit, duplicate detection,
  local-peer guard, mixed-batch commit, and the post-commit
  ring rebuild.
* `crates/dyniak/src/proto/pb/messages.rs` -- 7 prost
  round-trip tests for the new admin codes and structs.
* `crates/dyn-admin/src/output.rs` -- 2 tests for
  `render_change_human` (add / remove).
* `crates/dyn-admin/src/commands/{cluster_join,cluster_leave,
  cluster_plan,cluster_commit,cluster_list}.rs` -- per-module
  human / JSON render tests.
* `crates/dyn-admin/tests/cluster_admin_integration.rs` -- 6
  end-to-end tests that spin up `serve_pbc_with_admin` against
  a `PoolClusterAdmin` wired to a real `ServerPool` and drive
  every subcommand via `assert_cmd`. Includes the journey
  test (join -> plan -> commit -> list) and the negative
  paths (invalid target, unknown peer-idx, NoopClusterAdmin
  shows empty).

The pre-existing `cli_smoke.rs` was updated: the
`deferred_subcommands_are_absent_not_stubbed` test was replaced
with `cluster_mutating_subcommands_are_present`, and the help
test now asserts every cluster-* subcommand appears.

## Verification

```
cargo build --workspace --all-targets --locked          OK
cargo build -p dynomited --features riak --all-targets  OK
cargo fmt -p ... -- --check                             OK
cargo clippy --workspace --all-targets --all-features
    -- -D warnings                                       OK
cargo nextest run --workspace                            1149/1149
cargo test --doc --workspace                             OK
bash scripts/check_no_todos.sh                           OK
bash scripts/check_no_port_comments.sh                   OK
bash scripts/check_ascii.sh                              OK
```

## Constraints honoured

* No `unimplemented!()` / `todo!()` / stubs.
* No `#[allow]` suppressions added.
* ASCII-only.
* Owned files under `crates/dynomite/src/proto/`,
  `crates/dynomite/src/embed/`, `conf/`, `net/`, the
  `dynomited` binary, `dyn-encoding`, `dyn-hash-tool`, the
  `datatypes`/`aae`/`mapreduce`/`http`/`datastore`
  subdirectories of `dyniak`, and `scripts/` were not
  touched.
* `crates/dyniak/src/lib.rs`,
  `crates/dyniak/src/proto/pb/mod.rs`, and
  `crates/dyniak/src/proto/pb/messages.rs` were
  append-only: new variants and `pub use` items were added at
  the end of the enum / file; no existing line was modified.

## Follow-ups

* Token rebalancing on join. Today the new peer's token is
  derived from a stable hash of `(host, port)`; a real cluster
  wants the operator to be able to choose tokens or trigger an
  even redistribution.
* Gossip propagation of committed changes. The brief notes the
  commit "propagates via the existing gossip protocol"; the
  in-memory `ServerPool` mutation suffices for the v0 admin
  surface, but the next slice should drive a real
  `GOSSIP_SYN` round so peers other than the contacted seed
  observe the change without polling.
* `dynomited` does not yet wire `PoolClusterAdmin` into its
  PBC bind path. The follow-up that runs the binary as a real
  cluster supervisor will do so via
  `serve_pbc_with_admin(listener, ds, admin)`.
