# Pass 2 chaos test: cross-DC peer connections

## Status

Pass 1 (the run currently in-flight at the time of writing) tests:
* per-DC dynomited + redis pairs running independently across 3
  hosts spanning Linux x86_64 (floki, arnold) and FreeBSD amd64
  (nuc),
* the local-datastore wiring (`crates/dynomited/src/server.rs`'s
  `backend_supervisor` + `run_one_backend_conn`),
* process stability under SIGSTOP / SIGCONT pauses, SIGKILL +
  restart, and redis bounce.

It does **not** yet exercise cross-DC routing: client requests on
host A always route to host A's local dynomited / redis pair.
The dispatcher returns `DispatchPlan::Replicas` for keys that
hash into a different DC's token range; today that path falls
through to `DispatchOutcome::Pending` with no peer worker to
forward the request.

This document outlines what pass 2 needs.

## Gap

`crates/dynomited/src/server.rs::Server::Builder::build` does
not:

1. open outbound TCP connections to peer `dyn_listen` ports for
   each entry in `dyn_seeds`,
2. spawn a `DnodeServerConn` driver per peer connection,
3. expose the per-peer outbound channel to `ClusterDispatcher`
   so `Replicas` plans can be forwarded.

The library has the building blocks:

* `dynomite::net::server::OutboundRequest` (request envelope
  with bytes + `req_id` + per-client responder).
* `dynomite::net::dnode_server::DnodeServerConn` (outbound
  driver that wraps each request in a dnode header,
  optionally encrypts when secure_server_option is set, writes
  to the wire, and routes the parsed response back through the
  per-request responder channel).

## Plan

### `cluster::dispatch::ClusterDispatcher`

Add a per-peer outbound channel map alongside the existing local
backend channel:

```rust
pub struct ClusterDispatcher {
    pool: Arc<ServerPool>,
    backend: Option<Sender<OutboundRequest>>,
    peer_backends: HashMap<u32, Sender<OutboundRequest>>,
}

impl ClusterDispatcher {
    pub fn with_peer_backend(
        mut self,
        peer_idx: u32,
        sender: Sender<OutboundRequest>,
    ) -> Self {
        self.peer_backends.insert(peer_idx, sender);
        self
    }
}
```

In `dispatch()`, the `Replicas(targets)` arm should:

1. snapshot the wire bytes once (same as `LocalDatastore`),
2. for each target with `is_local == true`, forward via
   `self.backend` (if set),
3. for each remote target, look up `self.peer_backends[peer_idx]`
   and forward via `try_send`,
4. if any target lacks a wired backend, surface a
   `DynomiteNoQuorumAchieved` error response if the consistency
   level cannot be met with the available peers.

The per-request responder (`ServerSink`) is cloned for each
target. The first peer to reply wins (`DC_ONE`); waiting for
`DC_QUORUM` / `DC_EACH_SAFE_QUORUM` requires a coalescer, which
is a separate piece of work (see "Quorum coalescer" below).

### `dynomited::server::Server::Builder::build`

For each non-local peer in the constructed `peers` vector,
spawn a `peer_supervisor` task analogous to `backend_supervisor`:

```rust
for peer in &peers {
    if peer.is_local() { continue; }
    let (tx, rx) = mpsc::channel(peer_channel_capacity);
    let addr = peer.endpoint().tcp_addr()?;
    let handle = tokio::spawn(peer_supervisor(addr, rx, ...));
    peer_handles.push(handle);
    cluster_dispatcher = cluster_dispatcher.with_peer_backend(peer.idx(), tx);
}
```

`peer_supervisor` mirrors `backend_supervisor`:
* connect with bounded backoff,
* on each TCP connection, drive a `DnodeServerConn`,
* on disconnect, reconnect.

The `Conn` role is `ConnRole::DnodePeerServer` (we are the
client to the peer's `DnodePeerServer` listener).

### Quorum coalescer (separate stage)

`DC_QUORUM` and `DC_EACH_SAFE_QUORUM` require waiting for N of M
replicas to respond before deciding the client reply. The C
reference uses a coalescer that:

* tracks the responses per `req_id`,
* picks the first / quorum reply for the client,
* discards stragglers.

The Rust port has `cluster::coalesce` (in
`crates/dynomite/src/cluster/`) but the binary does not run it.
For pass 2 we can ship without coalescer support by:

* limiting the chaos test to `DC_ONE` consistency, and
* still fanning out writes to all DC replicas so the data lands
  everywhere even if the client only sees the first response.

A pass 3 run with coalescer-enabled `DC_EACH_SAFE_QUORUM` writes
exercises the harder consistency machinery.

## Test gates

Before launching pass 2:

1. `cargo nextest run --workspace` stays at 607 / 607 + 1 skip.
2. `cargo test --features integration -p dynomited --test
   integration` passes (single-DC end-to-end).
3. New unit test in `cluster::dispatch` covering the
   `Replicas`-with-wired-peer path.
4. New integration test in `crates/dynomited/tests/` that spawns
   two dynomited instances against a single redis backend per
   instance, drives a key that hashes to the remote DC, and
   verifies the response makes the round trip.
5. The stage_16_chaos in-process test stays green.

## Topology change for pass 2

In `scripts/chaos-multi-host/coordinator.sh`:

* Switch tokens back to per-DC distinct values
  (`floki=0, arnold=1431655765, nuc=2863311530`) so each DC owns
  a distinct slice of the token ring; that forces cross-DC
  routing.
* Keep `read_consistency: DC_ONE` for now (no coalescer).
* Set `write_consistency: DC_ONE` initially. When the coalescer
  lands (pass 3), bump writes to `DC_EACH_SAFE_QUORUM`.
* Re-enable the SSH tunnels through arnold for floki<->nuc
  reachability (already coded; just remove the "nothing uses
  these yet" simplification).

## Estimated effort

* Pass 2 wiring (no coalescer): ~2-3 hours.
* Pass 3 coalescer + DC-fanout writes: ~6-8 hours (plus careful
  invariant testing).
