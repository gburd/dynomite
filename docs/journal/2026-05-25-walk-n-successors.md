# 2026-05-25 -- walk-N-successors replication strategy

## Context

Operator follow-up on Riak-compat: ship the canonical Riak
replication strategy (primary partition + next `n_val - 1`
peers reached by walking the ring) as a per-bucket-property
opt-in alongside Dynomite's classic topology fan-out. The
default is mode-aware: non-Riak pools never see the knob and
keep `Topology`; Riak-mode pools default newly-created bucket
types to `Successors`.

The existing topology pipeline
(`dynomite::cluster::dispatch::ClusterDispatcher`) is
untouched. The new path is invoked by the Riak request handler
only when the bucket's `replication_strategy` resolves to
`Successors`; the dispatcher's existing
`collect_routable / plan_with_consistency` pipeline still owns
the `Topology` branch.

## Files touched

* `crates/dyn-riak/src/proto/pb/messages.rs` -- added
  `replication_strategy: Option<u32>` at tag 31 with
  `TOPOLOGY = 0` / `SUCCESSORS = 1` constants.
* `crates/dyn-riak/src/proto/pb/mod.rs` -- re-exported
  `REPLICATION_STRATEGY_*`.
* `crates/dyn-riak/src/replication.rs` -- new module with
  `ReplicationStrategy`, `RingPoint`, `RingView`,
  `ReplicationPlan`, and the
  `plan_replicas(distribution, key_hash, n_val, strategy,
  consistency)` entry point. Walk semantics: locate the
  primary slot, then iterate forward through the ordered ring
  points, deduplicating peer indices, capping at `n_val`.
* `crates/dyn-riak/src/bucket_props.rs` -- per-bucket
  registry caching effective `(KeyFun, ReplicationStrategy,
  n_val)`. Mode-aware defaults via
  `BucketPropsRegistry::new_riak_defaults()`.
* `crates/dyn-riak/src/router.rs` -- `BucketRouter` resolves
  `(bucket-type, bucket, key)` -> `RouteDecision { props,
  route_bytes, key_hash, plan }`. Handed an `Arc<BucketRouter>`
  plus an `Arc<dyn PeerOutbound>` it doubles as the
  successors-mode dispatch seam for the PBC server.
* `crates/dyn-riak/src/server.rs` -- new
  `serve_pbc_with_routing` entry point and a
  `handle_conn_with_hooks` driver. The existing
  `serve_pbc` / `serve_pbc_with_admin` / `serve_pbc_tls*`
  entry points trampoline through the new path with no
  hooks; behaviour for unrouted callers is unchanged.
* `crates/dyn-riak/src/lib.rs` -- re-exports for the new
  module surface.

## Test deltas

* Unit (`crates/dyn-riak/src/replication.rs::tests`): five-peer
  ring fixture at the prompt's prescribed token positions
  exercises the prompted `peer-0 / peer-3` cases, the
  `n_val=2` and `n_val=10` boundaries, dedup of duplicate peer
  slots, and the empty-ring synthetic primary.
* Unit (`crates/dyn-riak/src/bucket_props.rs::tests`):
  registry defaulting (Riak-mode vs non-Riak-mode), per-bucket
  override, normalised bucket-type fallback to `default`.
* Unit (`crates/dyn-riak/src/router.rs::tests`):
  `topology_strategy_yields_empty_replica_list`,
  `route_bytes_match_keyfun_shape`,
  `empty_bucket_type_normalises_to_default`.
* Integration (`crates/dyn-riak/tests/bucket_props_routing.rs`):
  `successors_strategy_fans_one_put_out_to_three_peers` --
  end-to-end PBC PUT through `serve_pbc_with_routing` to a
  5-peer ring with `Successors n_val=3`; asserts the put hits
  exactly three distinct per-peer outbound channels.
* Integration: `topology_strategy_does_not_fan_out_via_successors_path`
  -- asserts that with `Topology` strategy the new fan-out
  hook stays silent (the existing topology dispatcher remains
  the source of truth).
* Integration: `set_bucket_round_trips_through_get_bucket`
  -- asserts the registry-backed get-bucket reflects the
  Riak-mode `Successors` default.

## Parity delta

`docs/parity.md`: new Deviation entry recording that
`replication_strategy: Successors` is gated on Riak mode and
that non-Riak pools never expose the knob.

## Open questions

* Per-bucket-type override surfaced through `dyn-admin` --
  deferred. The request path already reads the registry; the
  CLI-side surface is a follow-up slice.
* Down-peer behaviour: planning returns Down peers as targets
  and lets the existing `is_routable()` filter exclude them
  at runtime. This matches the topology mode's semantics; if
  operators want strictly-up successors that is a follow-up
  toggle.
