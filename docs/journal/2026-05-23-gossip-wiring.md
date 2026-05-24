# 2026-05-23 -- Gossip wiring milestone

Worker: gossip-wiring (parallel batch).

## Goal

Replace the supervisor-driven peer-state hack with gossip-driven
peer-state authority. The phi-accrual failure detector (already
attached to `Peer` in commit `979510f`) becomes the single source
of truth for `PeerState` transitions; the TCP-link supervisor
loop no longer publishes peer-state itself.

## Files touched

* `crates/dynomite/src/cluster/gossip.rs` -- new `GossipHandler`
  type. Owns peer-state transitions for the gossip plane:
  `record_heartbeat_pname` feeds the per-peer phi detector,
  `evaluate(now)` walks every non-local peer and reconciles
  `PeerState` with phi, `mark_down_pname` overrides the detector
  for `GossipShutdown`. New unit tests cover all four paths.
* `crates/dynomite/src/cluster/mod.rs` -- re-export `GossipHandler`.
* `crates/dynomite/src/net/server.rs` -- extend `OutboundRequest`
  with a `ty: DmsgType` field so the same per-peer outbound
  channel carries both data-plane (`Req`) and control-plane
  (`GossipSyn` / `GossipShutdown`) frames.
* `crates/dynomite/src/net/dnode_server.rs` -- read `req.ty` for
  the dnode header type and skip `pending` push-back for
  fire-and-forget gossip frames.
* `crates/dynomite/src/net/client.rs` -- optional `gossip:
  Option<Arc<GossipHandler>>` on `ClientHandler` plus
  `with_gossip()` builder.
* `crates/dynomite/src/net/dnode_client.rs` -- inbound dispatch
  fork: when the parsed `Dmsg` is gossip-class, route the payload
  (`pname` ASCII) into the gossip handler instead of the
  datastore parser.
* `crates/dynomite/src/cluster/dispatch.rs` -- thread `ty:
  DmsgType::Req` through the three `OutboundRequest` constructions.
* `crates/dynomited/src/server.rs` -- 
  - build a single `GossipHandler` per pool,
  - keep a vec of per-peer outbound channels for gossip TX,
  - drop the `set_peer_state(Normal/Down)` calls in
    `peer_supervisor` (gossip is the authority),
  - spawn a periodic `gossip_task` that sends `GossipSyn` to
    every peer and ticks `evaluate(now)` on the failure
    detector,
  - on graceful shutdown send `GossipShutdown` to every peer.
* `crates/dynomite/tests/gossip_wiring.rs` -- new integration
  test using `tokio::io::duplex` to verify gossip-wire frames
  drive `PeerState` transitions end-to-end.
* `crates/dynomite/tests/{stage_09_net,distributed_tracing}.rs`
  -- add `ty: DmsgType::Req` to existing `OutboundRequest`
  literals.

## Out of scope (per worker brief)

* `crates/dynomite/src/conf/pool.rs` is owned by a parallel
  worker. No new fields were added there. The runtime reads the
  existing `gos_interval: Option<i64>` field for the gossip
  cadence; the phi threshold uses
  `cluster::failure_detector::DEFAULT_THRESHOLD` (8.0). If the
  lead wants an operator-tunable threshold, the suggested fields
  for `ConfPool` are:
  ```
  pub gossip_phi_threshold: Option<f64>, // default 8.0
  ```

## Test deltas

* `cargo nextest run --workspace`: 658 -> 668 (+10).
  New cases: 6 unit tests in `cluster::gossip::tests::handler_*`
  plus 4 integration tests in `gossip_wiring`.
* Conformance: 34/34 (no change), with `each_node_accepts_workload`
  green under gossip-driven peer state.
* Doctests: 15 passed (no change).

## Behavioural notes

* The gossip handler's `record_heartbeat_*` immediately promotes
  a peer to `Normal` when the first heartbeat arrives; we do not
  wait for the next periodic `evaluate` tick. This makes the
  3-node conformance test green within `~1 * gossip_interval`
  even though the supervisor no longer publishes peer-state.
* The `evaluate(now)` rule is: `Normal` iff
  `last_heartbeat.is_some()` and `phi(now) <= threshold`,
  else `Down`. A peer we have never heard from stays `Down`.
* `GossipShutdown` is fire-and-forget and best-effort. The
  receiver short-circuits to `Down`; if the frame is lost the
  receiver still detects the sender via gossip silence within
  `~threshold * gossip_interval`.
