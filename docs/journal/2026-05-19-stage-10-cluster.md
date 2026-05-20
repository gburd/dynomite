# Stage 10 - Cluster: pools, peers, gossip, snitch, vnode, dispatch

Branch: `stage/10-cluster`. Worktree:
`/tmp/pi-agent-40fcb92c-cf73-438-7e565da4`. Worker: cluster
Stage 10. Date: 2026-05-19.

## Summary

Stage 10 lands the cluster layer that sits between the Stage 9
per-connection FSMs and the Stage 8 protocol parsers. The
deliverables are:

* `crates/dynomite/src/cluster/`:
  * `peer.rs` - `Peer`, `PeerEndpoint`, `PeerState`.
  * `datacenter.rs` - `Datacenter`, `Rack`, `Continuum`.
  * `vnode.rs` - `dispatch` (the upper-bound binary search the
    reference engine uses) plus a `rebuild_continuums` helper
    decoupled from the live pool so the rebuild path can be
    unit-tested.
  * `snitch.rs` - env-var-driven address helpers
    (`broadcast_address`, `public_hostname`, `public_ip4`,
    `private_ip4`) plus the rack-distance / `pick_target_rack`
    helpers the brief asked for. The proximity helpers do not
    live in the reference engine's snitch unit (the reference's
    only DC/rack proximity decision is in
    `preselect_remote_rack_for_replication`); we add them on
    the Rust side so `ClusterDispatcher::plan` has a single
    point of truth, and document the deviation in
    `docs/parity.md`.
  * `pool.rs` - `ServerPool`, `PoolConfig`. Owns the peer table
    (peer index 0 is always the local node), the datacenter /
    rack tables, the per-peer `AutoEject` deciders, and the
    rebuild-on-demand ring. Also resolves Stage 7's deferred
    `init_response_mgr_all_dcs` as `init_response_mgrs`, which
    walks the cluster's DC table and produces one `ResponseMgr`
    per DC sized to the rack count.
  * `gossip.rs` - `GossipState`, `GossipNode`, `GossipStep`,
    `GossipConfig`, plus `parse_seed_node` / `parse_seed_blob`.
    The state machine reproduces the reference engine's
    add/replace/update logic (token + host equality decides
    which arm fires) and includes a `run_failure_detector`
    that mirrors `gossip_failure_detector`.
  * `dispatch.rs` - `ClusterDispatcher`, the cluster-aware
    `Dispatcher` impl. `plan` produces a `DispatchPlan`
    (`LocalDatastore`, `Replicas(Vec<ReplicaTarget>)`,
    `NoTargets`, `Drop`) and is independently unit-testable.
    `dispatch` (the trait method) bridges the Stage 9 seam and
    routes to the right outcome.
* `crates/dynomite/src/seeds/`:
  * `mod.rs` - `SeedsProvider` trait + `SeedsError`.
  * `simple.rs` - YAML-loaded list.
  * `dns.rs` - resolver-driven (the resolver is a trait so the
    test can inject a deterministic implementation; the Stage
    12 binary wires `tokio::net::lookup_host`).
  * `florida.rs` - hand-rolled HTTP/1.0 over
    `tokio::net::TcpStream` with `httparse` parsing the
    response header.
* `crates/dynomite/tests/stage_10_cluster.rs` - 15 integration
  tests covering all the brief's exit criteria.

## Test counts

| Run | Pre-stage | Post-stage |
|---|---|---|
| `cargo nextest run --workspace` | 471 | 534 |
| `cargo nextest run --workspace --all-features` | 475 | 538 |
| `cargo test --doc --workspace` | 466 | 510 |

All counts measured on this worktree. `cargo fmt --all --
--check` and `cargo clippy --workspace --all-targets
--all-features -- -D warnings` are clean.
`scripts/check_no_port_comments.sh`,
`scripts/check_no_todos.sh`, and `scripts/check_ascii.sh` all
pass.

## Brief-vs-source corrections

The Stage 10 brief noted up-front that prior briefs had
factual errors about the C source. Two such corrections came
up while reading for this stage:

1. **`dyn_node_snitch.{c,h}` does not own DC/rack proximity
   ordering.** The brief said "snitch.rs: DC/rack proximity
   ordering. `pick_target_rack` (prefer local rack),
   `rack_distance` for routing decisions. Mirrors
   `dyn_node_snitch.c`." The reference engine's snitch unit
   only contains the four env-var helpers
   (`get_broadcast_address`, `get_public_hostname`,
   `get_public_ip4`, `get_private_ip4`) plus
   `hostname_to_private_ip4` (which calls `gethostbyname`).
   The DC/rack proximity decision lives in
   `dyn_dnode_peer.c::preselect_remote_rack_for_replication`.
   Per AGENTS.md non-negotiable #6 we honor the C source: the
   Rust `cluster::snitch` ports the env-var helpers
   faithfully. The proximity helpers the brief asked for
   (`rack_distance`, `pick_target_rack`, `RackDistance`) are
   added as a Stage-10 Deviation and are recorded in
   `docs/parity.md`. They are pure functions consumed by
   `ClusterDispatcher::plan`.
2. **`vnode_dispatch` returns a peer index, not a continuum
   index.** The brief glossed over this; the C body assigns
   `c->index = i` where `i` walks the peer list. The Rust
   `cluster::vnode::dispatch` therefore returns `Option<u32>`
   (peer idx) and the test fixture asserts the returned idx
   is in `0..peers.len()`.

## Stage-9 deferrals resolved

* `cluster::dispatch::ClusterDispatcher` - cluster-aware
  `Dispatcher` impl that replaces `NoopDispatcher` for any
  test or binary wiring that wants the real plan. The
  `NoopDispatcher` stays in tree as the framing-only test
  helper.
* `init_response_mgr_all_dcs` - now `ServerPool::init_response_mgrs`,
  walks the live DC table and returns one `ResponseMgr` per
  DC.
* Auto-eject - `ServerPool` carries a `Vec<AutoEject>`
  parallel to its peer list so cluster-level metrics drive
  ejection. The data-shape glue is in place; the gossip task
  and dispatcher feed it via `record_failure` /
  `record_success` calls in Stage 12 binary wiring.

## Deferrals

Items intentionally left for later stages, all with parity-
table entries:

* `dnode_create_connection_pool` (consumes
  `local_peer_connections` / `remote_peer_connections` from
  YAML at startup): Stage 12 binary wiring.
* `gossip_loop` (the periodic tokio task that drives
  `GossipState`): Stage 12 binary wiring; the data shape is
  here.
* `dnode_req_send_next` per-conn token bucket: Stage 12
  binary wiring (the rate value lives on
  `crate::core::setting::msgs_per_sec`).
* `gossip_set_seeds_provider` (provider switch keyed on
  `dyn_seed_provider:`): Stage 12 binary wiring.
* `dn_resolve` for synthesised gossip nodes: Stage 12 binary
  wiring (the `dnode_peer_add_node` path that calls
  `dn_resolve` is a hot loop in the C engine and depends on
  the binary's resolver wiring).
* Entropy reconciliation: Stage 11.
* The `dynomited` binary that drives all of the above:
  Stage 12.
* Embedding API hooks for custom `SeedsProvider`
  implementations: Stage 13. The trait shape is locked here
  so Stage 13 only adds the `embed/` wrapper.

## Files touched

```
crates/dynomite/src/lib.rs                           (+2)
crates/dynomite/src/cluster/mod.rs                   (new)
crates/dynomite/src/cluster/peer.rs                  (new)
crates/dynomite/src/cluster/datacenter.rs            (new)
crates/dynomite/src/cluster/vnode.rs                 (new)
crates/dynomite/src/cluster/snitch.rs                (new)
crates/dynomite/src/cluster/pool.rs                  (new)
crates/dynomite/src/cluster/gossip.rs                (new)
crates/dynomite/src/cluster/dispatch.rs              (new)
crates/dynomite/src/seeds/mod.rs                     (new)
crates/dynomite/src/seeds/simple.rs                  (new)
crates/dynomite/src/seeds/dns.rs                     (new)
crates/dynomite/src/seeds/florida.rs                 (new)
crates/dynomite/tests/stage_10_cluster.rs            (new)
docs/parity.md                                       (+~150 rows)
```

## Open questions

None. The Stage 10 exit gate (every consistency level
produces the right fan-out, gossip converges within N rounds
on a scripted seed list, ring math matches the C reference
for a fixed key corpus) is met by the `tests/stage_10_cluster.rs`
suite.

## Final report

```
STAGE: 10
STATUS: READY_FOR_REVIEW
BRANCH: stage/10-cluster
JOURNAL: docs/journal/2026-05-19-stage-10-cluster.md
PARITY_DELTA: 150
```
