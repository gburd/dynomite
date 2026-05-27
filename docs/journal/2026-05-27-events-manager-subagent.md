# Stage events-manager: structured cluster events channel

Author: subagent (committed as Greg Burd <greg@burd.me>)
Branch: stage/events-manager
Date: 2026-05-27

## Goal

Add an OTP `gen_event`-style structured channel for cluster events
that test harnesses, admin tools, and OTLP appenders can subscribe
to without parsing logs or re-deriving signal from Prometheus
counters.

## Files touched

* New: `crates/dynomite/src/events/mod.rs`
* New: `crates/dynomite/src/events/manager.rs`
* New: `crates/dynomite/src/events/subscriber.rs`
* New: `crates/dynomite/tests/events_manager.rs`
* Modified: `crates/dynomite/src/lib.rs` (register module)
* Modified: `crates/dynomite/src/embed/server.rs`
  * `ServerInner` carries an `Arc<EventManager>`.
  * `ServerHandle::events()` accessor exposes it to embedders.
  * `gossip_loop` publishes `GossipRoundComplete`, plus
    `PeerUp` + `RingChanged{tag="seed-discovery"}` whenever
    seed discovery adds peers.
* Modified: `crates/dynomite/src/cluster/gossip.rs`
  * `GossipHandler::with_events(Arc<EventManager>)` builder hook.
  * `record_heartbeat_pname`, `record_heartbeat_idx`, `evaluate`
    and `mark_down_pname` publish `ClusterEvent::PeerUp` /
    `ClusterEvent::PeerDown` (with the observed phi value) on
    every transition they apply.
* Modified: `crates/dyn-riak/src/aae/scheduler.rs`
  * `Scheduler::install_events`, `Scheduler::events`,
    `Scheduler::notify_exchange_started`,
    `Scheduler::notify_exchange_completed` so the AAE driver
    code can publish `AaeExchangeStarted` /
    `AaeExchangeCompleted` payloads when it has the full
    (peer, partition, repaired) tuple. The brief explicitly
    parks the actual driver wiring on the dyn-riak side; this
    commit only exposes the publish primitives.

## API surface

```rust
pub struct EventManager { /* broadcast::Sender<ClusterEvent> */ }
impl EventManager {
    pub fn new(buffer: usize) -> Self;
    pub fn publish(&self, event: ClusterEvent);
    pub fn subscribe(&self) -> Subscriber;
    pub fn subscriber_count(&self) -> usize;
}

pub struct Subscriber { /* broadcast::Receiver<ClusterEvent> */ }
impl Subscriber {
    pub async fn recv(&mut self) -> Result<ClusterEvent, SubscriberError>;
    pub fn try_recv(&mut self) -> Result<ClusterEvent, TryRecvError>;
}

pub enum SubscriberError { Closed, Lagged(u64) }
pub enum TryRecvError    { Empty, Closed, Lagged(u64) }

#[non_exhaustive]
pub enum ClusterEvent {
    PeerUp { peer_id, dc, ts },
    PeerDown { peer_id, dc, phi, ts },
    GossipRoundComplete { duration, peers_seen, ts },
    AaeExchangeStarted { with_peer, partition, ts },
    AaeExchangeCompleted { with_peer, partition, repaired, ts },
    RestartObserved { peer_id, ts },
    RingChanged { tag, ts },
}

pub struct TokenRange { /* DynToken start..end */ }
pub type PeerId = u32;
```

## Wired publishers

* `cluster::gossip::GossipHandler::evaluate` -> `PeerUp` / `PeerDown`
  (with phi at the moment of the threshold cross) on every
  applied transition.
* `cluster::gossip::GossipHandler::record_heartbeat_pname` /
  `record_heartbeat_idx` -> `PeerUp` on snappy first-contact
  promotion.
* `cluster::gossip::GossipHandler::mark_down_pname` -> `PeerDown`
  on the gossip-shutdown path.
* `embed::server::gossip_loop` -> `GossipRoundComplete` after
  every periodic tick, plus `PeerUp` and
  `RingChanged{tag="seed-discovery"}` on every newly discovered
  seed.
* `dyn-riak::aae::Scheduler::notify_exchange_*` -> publish
  primitives for `AaeExchangeStarted` / `AaeExchangeCompleted`.
  Driver-side call sites are deferred per the brief
  ("dynomite side just exposes the manager"); the manager is
  reachable from any `dyn-riak` code that holds a
  `ServerHandle` via `handle.events()`.

## Tests

`crates/dynomite/tests/events_manager.rs` (4 tests):

* `subscribe_then_publish_delivers_event`
* `multiple_subscribers_each_receive_the_event`
* `slow_subscriber_lags_and_surfaces_lagged_error`
* `event_after_drop_subscriber_does_not_block_publisher`

Plus 9 doctests on the new public items.

## Verification

* `cargo nextest run -p dynomite --test events_manager`
  -> 4 / 4 PASS.
* `cargo nextest run -p dynomite -p dyn-riak`
  -> 1225 / 1225 PASS (no regressions).
* `cargo test --doc -p dynomite events`
  -> 16 / 16 PASS.
* `cargo clippy --workspace --all-targets -- -D warnings`
  -> clean.
* `cargo fmt --all -- --check` -> clean.
* `scripts/check_ascii.sh` -> 0.
* `scripts/check_no_todos.sh` -> 0.
* `scripts/check_no_port_comments.sh` -> 0.

## Notes / open items

* `Cargo.lock` was inadvertently updated by cargo when the
  out-of-tree `noxu-db` workspace at `/tmp/lamdb/...` bumped
  from `2.0.0-rc1` to `2.0.0`. Reverted to keep this commit
  scoped to the events module; the lockfile drift is an
  unrelated infrastructure issue. Build still works without
  `--locked`.
* The dyn-riak driver loop that sequences AAE exchanges does
  not yet call `notify_exchange_started` /
  `notify_exchange_completed`; the brief explicitly parks that
  wiring. Tracked as a follow-up: thread the per-token
  exchange site in `Scheduler::exchange_per_token` plus the
  binary-side driver to invoke the publish helpers around each
  exchange.
* `ClusterEvent::RestartObserved` has no producer yet; the
  variant exists so when peer-incarnation tracking lands the
  publish point can be added without breaking the public API.

## Status

STATUS: READY_FOR_REVIEW
