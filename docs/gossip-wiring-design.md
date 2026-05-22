# Gossip + phi-accrual wiring design

This document covers the next major piece of cluster machinery
needed in the binary: wiring the gossip task that drives the
phi-accrual failure detector now attached to each `Peer`
(commit `979510f`).

## Current state

* **Library**: `crates/dynomite/src/cluster/gossip.rs` (433 LOC)
  has `GossipState`, `GossipNode`, `GossipConfig`, `SeedRecord`,
  parsers (`parse_seed_node`, `parse_seed_blob`), and a stub
  `run_failure_detector` based on TS comparison.
* **Wire format**: `crates/dynomite/src/proto/dnode.rs` defines
  six gossip message types (`GossipSyn`, `GossipSynReply`,
  `GossipAck`, `GossipDigestSyn`, `GossipDigestAck`,
  `GossipDigestAck2`).
* **Binary**: `dynomited::server::Server::run` logs
  `enable_gossip is set but the gossip run loop is not yet
  wired (deferred)` and proceeds without spawning anything.
* **Phi-accrual**: `cluster::failure_detector::PhiAccrual`
  (commit `65ccb12`) plus `Peer::failure_detector{,_mut}()`
  (commit `979510f`). Standalone, ready to be fed.

## What "gossip wiring" means in the binary

Three new tokio tasks under `dynomited::server`:

1. **Gossip TX task** (one per pool): every `gos_interval_ms`
   pick a random non-local peer, build a `GossipDigestSyn`
   message containing this node's state, write it to that
   peer's outbound channel (the same channel the dispatcher
   uses for `Replicas` plans).

2. **Gossip RX handler** (no separate task; inside the existing
   `dnode_proxy` accept loop): inspect each accepted dnode
   message; if it's a gossip type, decode the payload and
   apply via `GossipState::add_or_update`. Reply with the
   matching ACK frame. Update the originating peer's
   `failure_detector_mut().record_heartbeat(now)`.

3. **Failure detector tick** (one per pool): every 1s, walk
   `pool.peers()` and for any non-local peer with
   `peer.failure_detector().is_suspect(now)`, transition
   `peer.set_state(PeerState::Down, now_secs)`. Conversely,
   if a peer is `Down` and the detector says no-longer-suspect
   (because a recent heartbeat reset phi), transition back to
   `Normal`.

The dispatcher's `Replicas` arm already filters out targets
whose state isn't `Normal` (or it should; if not, that's a
two-line addition in `cluster::dispatch::plan`).

## Sequencing

```
[done]   (1) PhiAccrual algorithm + tests              commit 65ccb12
[done]   (2) Wire detector field into Peer             commit 979510f
[next]   (3) Encode/decode gossip message bodies       ~1 day
[next]   (4) Gossip TX task + RX handler               ~1 day
[next]   (5) Failure-detector tick + state transitions ~0.5 day
         ----
         (3-5) total: ~2.5 days
```

## Where the gossip MESSAGE BODY format comes in

The 6 dnode gossip message types are defined as discriminators
but the body encoding is open. The C reference wraps a Cassandra-
style state digest:

```
GossipDigestSyn body =
    | uint32_t version
    | uint32_t num_endpoints
    | for each endpoint:
        | uint16_t addr_len  uint8_t[addr_len] addr
        | uint8_t  num_tokens   uint32_t[num_tokens] tokens
        | uint8_t  state
        | uint64_t generation_ts_secs
        | uint64_t heartbeat_count
```

Implementation order: encode first (tx), decode second (rx),
then the failure-detector tick.

## Risks / known unknowns

- The C reference uses gossip's own heartbeat counter, not phi-
  accrual; phi-accrual was layered on top by Cassandra/Akka/Riak
  separately. Our integration is a small departure: the gossip
  message carries the heartbeat count for compat (we still
  increment it) AND we use the *arrival time* of each message
  to drive phi-accrual on the receiving side.
- The `gos_interval` default (1s) and phi threshold (8.0)
  interact: at 1s gossip cadence, "phi=8" corresponds to
  approximately 8 * ln(10) = ~18 missed heartbeats, i.e. 18s
  silence. That matches Cassandra's operational defaults and is
  appropriate for a chaos-test SIGSTOP window of up to 15s.
- The current `gossip::run_failure_detector` (40-second timeout
  on `ts_secs`) becomes redundant once phi-accrual is wired.
  Either delete it or keep as a backstop.

## Acceptance

After step (5), the deferred warning at `server.rs:330` goes
away. `crate::entropy::receive` and `send` integrations follow
the same pattern but are independent (different message types,
different periodic task) and not blocking.

The chaos test then becomes the validation: when a SIGSTOP'd
peer's gossip messages stop arriving, the on-the-other-side
phi value should rise; when it crosses 8.0 the peer state
flips to `Down`; the dispatcher stops sending requests to it;
when SIGCONT'd the gossip resumes, phi resets, and the peer
returns to `Normal` with the dispatcher resuming traffic.
