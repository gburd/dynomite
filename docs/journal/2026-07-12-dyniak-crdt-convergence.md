# Dyniak CRDT convergence: served data-type path + convergent replication

Date: 2026-07-12
Author: chaos/CRDT feature work

## Problem

Dyniak advertises a Riak-like experience including CRDTs, but the CRDT
guarantee is not reachable in practice:

1. The PBC server (`serve_pbc`) has no `DtUpdateReq`/`DtFetchReq`
   handler. `MessageCode::from_u8` maps codes through 26 and 200-221;
   the data-type codes 80-83 fall to `Err`, so the server closes the
   connection on any CRDT request. The CRDT types (`PnCounter`,
   `OrSet`, ...) exist and are unit-tested for in-memory merge and PBC
   codec round-trips, but no served endpoint applies them.

2. Replica apply (`ReplicaApplier::apply_op`) stores the forwarded
   `Put` value verbatim (`riak_put(bucket, key, value)`). Concurrent
   writes to the same key on partitioned replicas resolve
   last-write-wins, not by CRDT merge -- two partitioned `+1`
   increments overwrite instead of summing. Even AAE repair resolves
   concurrent siblings as "lex-largest value".

So "single-key updates always accepted, converging to CRDT-resolved
values" is not what the code does.

## Design

### Storage form

A CRDT-typed key stores the *serialized CRDT state* (not a scalar
value) under `(bucket, key)` via the existing
`Datastore::riak_get`/`riak_put`. State-based (CvRDT) merge is the
convergence mechanism: it is commutative, associative, and idempotent,
so applying operations in any order, with duplicates, on any replica,
converges to the same value. This is why single-key CRDT writes need no
quorum and stay available under partition.

Serialization: each CRDT type gains `to_bytes`/`from_bytes` (a small,
explicit, versioned binary form -- length-prefixed actor ids and
per-actor counts for PnCounter; tagged (actor, element) entries plus
the tombstone set for OrSet). A one-byte type tag precedes the payload
so a fetch can project the right value and a merge can reject a
type-mismatched blob.

### Client-facing DtUpdate (PBC codes 82/80)

Add `DtUpdateReq`/`DtFetchReq`/`DtUpdateResp`/`DtFetchResp` to
`MessageCode` and dispatch them in `serve_pbc` to new handlers:

* `handle_dt_update`: decode the op, route via `hooks.router`, fan a
  `PeerOp::DtUpdate { type, bucket, key, op_bytes }` to every replica
  (the OP, not the merged value -- each replica merges locally), then
  apply the op to the *local* stored state and persist. The op carries
  the coordinating node's `ActorId` so each replica's contribution is
  attributed to a distinct actor (required for G-Counter-per-actor
  max-merge to sum correctly).
* `handle_dt_fetch`: read local state, project the value into
  `DtFetchResp.DtValue` (counter_value / set_value).

Apply is read-current -> merge-op -> write-back against the local
store. A per-key async mutex serialises apply on one node so two
concurrent updates to the same key do not lost-update each other
locally (cross-node concurrency is handled by the CRDT merge itself).

### Convergent replica apply

`ReplicaApplier::apply_op` gains a `PeerOp::DtUpdate` arm that does the
same read -> merge -> write-back against the local store. Because the
op is attributed to the originating node's actor, and merge is a
join-semilattice operation, every replica that receives the op (once,
more than once, or out of order) converges to the same state.

`PeerOp::Put`/`Del` keep their existing last-write-wins behaviour for
opaque (non-CRDT) objects -- CRDT convergence is opt-in per bucket type
(the `counters` / `sets` typed buckets), matching Riak, where only
data-type bucket types get convergent merge and the default bucket type
is opaque.

### Actor identity

Each node derives a stable `ActorId` from its datacenter + node name
(already available in the routing context). A counter incremented +1 on
nodes A, B, C under partition holds pos={A:1,B:1,C:1}; merge takes
element-wise max per actor, so the value converges to 3 -- the total,
not 1.

## Invariants (to be modelled in DST and proven at scale)

* **Convergence (safety):** for any interleaving/duplication/reordering
  of a set of DtUpdate ops, all replicas that have seen the same set of
  ops project the same value, equal to the arithmetic result (counter:
  sum of increments; set: union of adds, minus removes that saw the
  add).
* **Availability (liveness):** every single-key DtUpdate is accepted
  and applied locally without waiting on a quorum or a remote replica,
  so it succeeds during a partition or ring change.
* **Negative control:** an LWW apply (store-verbatim, the pre-feature
  behaviour) loses increments under concurrent partitioned updates --
  the DST model must catch this.

## At-scale proof

Multi-region EC2 Dyniak cluster on local NVMe + separate per-region
load generators driving counter/set DtUpdate traffic through net
splits and node churn; a coordinator reconstructs the expected value
per key from the recorded op history and compares against a
post-quiescence fetch from every surviving replica. Availability =
fraction of accepted updates (must stay ~100% through faults); p99 must
stay steady; every counter must converge to its expected sum.
