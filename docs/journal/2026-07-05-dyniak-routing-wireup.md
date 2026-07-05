# 2026-07-05 -- Wire dyniak cross-node replica routing into dynomited

Branch: `stage/dyniak-routing` (off `main` @ `6add6f9`, which carries
the noxu 7.3.0 migration `44506a2`).

## Problem

The `dyniak` crate already had the full request-router machinery --
`BucketRouter`, `RoutingHooks`, `plan_replicas`, `PeerOutbound`, and a
`serve_pbc_with_routing` entry point whose PBC Put/Get/Del handlers
fan out to replica peers before the local store call. But the
`dynomited` binary never instantiated any of it: a `data_store: dyniak`
pool called plain `serve_pbc(listener, ds)`, so every object write
landed node-local only. A distributed EC2 test surfaced this as the
headline gap -- the replication code existed but was dead.

This change wires the existing machinery into the binary. No router or
replication design was reimagined; the existing `BucketRouter` /
`RoutingHooks` / `plan_replicas` are used as-is.

## What was wired

### 1. `DmsgType::RiakReplica = 22` (dnode wire type)

`crates/dynomite/src/proto/dnode.rs`:
- New `DmsgType::RiakReplica = 22` variant (next free discriminator
  after `XaAck = 21`).
- Added to `DmsgType::from_u8`.
- Added to `dmsg_process`'s bypass set alongside the XA variants, so
  the receive path routes it to the dyniak replica sink rather than
  the data-plane protocol stack.
- The dispatcher classification unit test asserts it is bypassed.

### 2. `PeerOp` wire format

`crates/dyniak/src/proto/replica_wire.rs` (new). Compact,
self-describing, std-only, big-endian:

```
op-kind(1)   0 = Put, 1 = Get, 2 = Del
bucket_type  (u32 be length prefix + bytes)
bucket       (u32 be length prefix + bytes)
key          (u32 be length prefix + bytes)
value        (u32 be length prefix + bytes)   -- Put only
```

`encode_peer_op` / `decode_peer_op`. Decoding is total: truncation,
unknown op-kind, and trailing bytes all surface a `ReplicaWireError`
rather than panicking or over-allocating. Unit tests cover round-trip
for each variant, empty value, truncation at every strict prefix,
empty buffer, unknown kind, trailing bytes, and an oversized length
prefix (no OOM).

### 3. Outbound `PeerOutbound` impl

`crates/dynomited/src/riak.rs` -- `PeerChannelOutbound`. Reuses the
dispatcher's per-peer `mpsc::Sender<OutboundRequest>` map (the same
`gossip_peer_txs` channels the RESP data plane, gossip task, and hint
drainer use). Each replica op is `encode_peer_op`'d into an
`OutboundRequest { ty: DmsgType::RiakReplica, target_peer_idx, .. }`;
`DnodeServerConn` frames it with a dnode header carrying that type.

Fire-and-forget contract: delivery is a bounded `try_send`. A missing
peer, closed channel, or full channel logs at `debug` and drops the
op -- it never blocks or panics. The `PeerOutbound::dispatch` future
returns `Ready` immediately so the PBC request handler never awaits a
peer. The responder channel is drained on a throwaway task because
`RiakReplica` is not a data-plane request type (no reply frame is
produced). The write still lands locally and on every reachable
replica; anti-entropy / read-repair reconcile a peer that missed it,
matching Riak's eventual-consistency model.

### 4. Receive path (apply to local noxu, no re-forward)

- `crates/dynomite/src/net/client.rs`: new `ReplicaApplySink` trait
  (engine-owned; the engine does not interpret the payload) plus a
  `ClientHandler::with_replica_sink` builder and `replica_sink`
  accessor.
- `crates/dynomite/src/net/dnode_client.rs`: `drive_dnode_parser`
  forks `DmsgType::RiakReplica` frames right after the gossip fork.
  It hands the payload to the attached sink and `continue`s -- the
  frame never reaches the datastore parser and is never re-forwarded.
  A node with no sink wired (non-dyniak pool) drops the frame.
- `crates/dyniak/src/replica_apply.rs` (new): `ReplicaApplier`
  implements `ReplicaApplySink`. It `decode_peer_op`s the payload and
  applies `Put` via `riak_put` (canonical `HttpObject` storage form,
  matching the local PBC put path) and `Del` via `riak_delete`,
  directly against the local `Datastore`. It holds no outbound
  channel, so re-forwarding is impossible by construction. `Get`
  replication (read-repair / handoff read) is a documented no-op:
  write replication does not need it, and it can be wired later
  without a wire-format change.

### 5. Router + hooks construction in dynomited

- `crates/dynomite/src/cluster/dispatch.rs`: `map_hash` (conf
  `HashType` -> hashkit `HashType`) made `pub` and re-exported from
  `cluster::mod`, so the dispatcher, reaper, and dyniak router share
  one conversion seam.
- `crates/dynomited/src/riak.rs` -- `build_routing_hooks(pool, hash,
  peer_txs)`: builds a `RingView` with one `RingPoint` per
  `(peer, token)` from `ServerPool::peers()` (each `DynToken`
  projected to `u64` via `get_int()`, matching the reaper's
  `ring_token_to_dyntoken` u32 continuum), a Riak-defaults
  `BucketPropsRegistry`, and a `BucketRouter`. Assembles the
  `RoutingHooks` with a `PeerChannelOutbound`.
- `RiakHandles` gained a `hooks: Option<RoutingHooks>` field.
  `spawn_listeners`'s PBC path serves `serve_pbc_with_routing` when
  hooks are present, else plain `serve_pbc` (or `serve_pbc_tls`).
- `crates/dynomited/src/server.rs`: after `build_handles`, a dyniak
  pool with at least one non-local peer gets `hooks` populated. The
  `Server` gained a `replica_sink` built from the local noxu store;
  the dnode peer-receive `ClientHandler` factory attaches it via
  `with_replica_sink` so inbound `RiakReplica` frames apply locally.

## Fire-and-forget replica contract (summary)

- Primary + n_val-1 replicas via the ring (`plan_replicas`,
  `ReplicationStrategy::Successors`).
- Write is locally durable, then best-effort to each replica peer
  (fire-and-forget; unreachable/full/closed peer is logged and
  dropped).
- No cross-node atomicity for plain puts -- that is XA's job.
- Applying a forwarded replica op is terminal / local-only: it never
  fans out again.

## Scoped as follow-up

- **HTTP-gateway replica routing.** The PBC path replicates end to
  end; the HTTP object/transaction handlers still write node-local.
  Wiring a `serve_http_with_routing` mirror is the next slice. The
  `RiakHandles::hooks` doc comment records this.
- **`Get` replica forwarding** (read-repair / handoff read) is a
  no-op in `ReplicaApplier`; write replication does not need it.

## Tests

- Unit (`replica_wire`): 8 tests -- round-trip per variant, empty
  value, truncation at every prefix, empty buffer, unknown kind,
  trailing bytes, oversized length prefix.
- Unit (`replica_apply`): undecodable payload dropped without panic.
- Integration (`crates/dyniak/tests/replica_routing.rs`, feature
  `noxu`, /scratch tempdirs):
  - `router_fan_out_matches_plan_replicas` (deterministic): 64 keys,
    router replica set asserted peer-for-peer equal to an independent
    `plan_replicas` computation over the same ring/n_val/strategy.
  - `receive_apply_put_lands_in_local_noxu`: encoded `PeerOp::Put`
    fed to `ReplicaApplier` over a real transactional Noxu env; value
    read back locally. No re-forward is structural (the applier holds
    no outbound channel).
  - `receive_apply_del_removes_from_local_noxu`.
  - `outbound_receive_pairing_delivers_write_to_node_b` (mock
    transport): node A routes; the exact `encode_peer_op` bytes
    `PeerChannelOutbound` would ship are decoded + applied by node
    B's `ReplicaApplier`, landing the write on node B's store. Mock
    transport, not two live dnode loops -- the two-node dnode
    delivery is validated by the multi-node conformance rig.
- Existing `bucket_props_routing.rs` already exercises the full
  `serve_pbc_with_routing` PBC fan-out through the `RecordingOutbound`
  fixture (one dispatch per replica per write) and continues to pass.
- Engine `dmsg_process` classification test extended for
  `RiakReplica`.

## Verification (all green)

```
cargo build -p dyniak --features noxu --locked
cargo build -p dynomited --features riak --locked
cargo build -p dynomited --all-targets --features riak,search,wasm --locked
cargo nextest run -p dyniak --features noxu           # 724 passed
cargo nextest run -p dynomite-engine                  # 1318 passed
cargo clippy -p dyniak -p dynomited --all-targets --features noxu -- -D warnings
cargo clippy -p dynomited --all-targets --features riak,search,wasm -- -D warnings
cargo clippy -p dyniak --all-targets --features noxu,wasm,search -- -D warnings
cargo clippy -p dynomite-engine --all-targets -- -D warnings
cargo fmt -p dyniak -p dynomited -- --check
cargo test --doc -p dyniak --features noxu replica_wire
cargo test --doc -p dynomite-engine dnode
```

No new dependencies. `forbid(unsafe_code)` preserved. ASCII only. No
version bumps. No port comments.

## DST + Elle merge gate (AGENTS.md Section 6.5)

This change was authored before the DST + Elle gate was codified
(`2edfd65`). Cross-node replication routing is squarely a
distributed-behaviour change, so on rebase it was brought under the
gate rather than grandfathered.

### DST model (`crates/model-tests/src/replication.rs`, in `scripts/model.sh`)

A stateright model of the fan-out abstracts the exact decision the code
runs: a coordinator applies a client write locally and forwards one
`PeerOp` per other replica on the preference list; each replica applies
to its LOCAL store and does NOT re-forward. Forwards may arrive in any
order and (in the lossy variant) be dropped. Invariants:

* **Bounded fan-out** (safety, `always`): the total apply count for one
  client write never exceeds `n_val`. This is exactly the "terminal,
  local-only" contract in `replica_apply.rs`: because an applied op is
  never routed back onto the ring, a replica write fans out at most once
  per preference-list peer -- no fan-out storm, no infinite loop. The
  invariant holds under reordering AND under loss (the lossy variant
  asserts it too).
* **Convergence** (liveness, `eventually`, reliable delivery): once every
  forward is delivered, every replica on the preference list holds the
  written value.

Negative control `re_forwarding_applier_violates_bounded_fan_out`: the
broken applier re-forwards each applied op to the other replicas (the
behaviour the "does NOT re-forward" contract forbids). The apply count
blows past `n_val` and the checker reports the bounded-fan-out
counterexample; the test asserts the discovery is present, so a
toothless model fails. The broken variant caps its own re-forwarding at
`2 * n_val` so the (otherwise unbounded) re-forward loop stays a finite,
CI-safe search -- the safety violation is already reached well before
the cap.

### Elle / consistency harness

Dyniak replica writes are fire-and-forget best-effort eventual
consistency (the Riak model): a replica write is neither linearizable
nor transactional, and unreachable/full peers are reconciled later by
anti-entropy and read-repair. The list-append Elle-style checker in
`scripts/consistency/` detects transactional anomalies (aborted reads,
dependency cycles) over a totally-orderable register/list history; there
is no transaction and no total order to check for a best-effort replica
fan-out, so its anomaly classes do not apply. The meaningful consistency
property here is monotonic convergence (no committed write lost; every
reachable replica ends up with the value), which the DST convergence
model asserts and which the end-to-end integration tests in
`crates/dyniak/tests/replica_routing.rs`
(`outbound_receive_pairing_delivers_write_to_node_b`) exercise against a
real local noxu store. Per Section 6.5's framing (as documented for the
delta-CRDT, SWIM, and HLC prototypes for their non-list-append
semantics), the DST model plus the real-code integration tests carry the
consistency gate for this change. The transactional consistency gate
lives on the RAMP / XA paths, which do drive the Elle harness.
