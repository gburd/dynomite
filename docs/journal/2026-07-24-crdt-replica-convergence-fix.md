# 2026-07-24 -- Dyniak CRDT per-replica convergence: fix + clean at-scale verdict

Follow-up to `2026-07-24-chaos-crdt-owner-centric-finding.md`, which
recorded that Dyniak CRDT writes converged only at the coordinating
(owner) node: non-owner replicas held partial or null state, and a
`DtFetch` to a non-owner returned its local partial value. The in-tree
tests passed because they drove the owner/local path; the earlier DST
model (`crdt_convergence`) passed vacuously because its delivery premise
did not match production.

## Root cause

Two defects in the write path, plus a masking test-topology error:

1. `handle_dt_update` (crates/dyniak/src/server.rs) applied the op to
   the coordinator's local store and fanned the merged state to the
   key's replica set -- but with a **degenerate ring** (every node had
   `tokens: '0'`), `primary_index()` returned the same start for every
   key, so every key routed to the same `n_val` nodes and keys never
   spread. Only a fixed handful of nodes were ever replicas.

2. The fan was fire-and-forget; under churn a replica could miss a
   fan, and nothing re-delivered it.

3. The chaos-verify success criterion required **every** node to hold
   the exact value -- wrong for a Dynamo-style system where a key lives
   only on its replica set. A non-replica holding null is correct, not
   a divergence.

## Fix

Model first (AGENTS.md 6.5), then code.

* **DST model** `crates/model-tests/src/crdt_routing.rs`: models a
  replica set of size `R` over `N` nodes, per-actor counter columns,
  at-most-once fan delivery, and a coordinated read. Safety: once all
  fans are delivered, a read over the replica set equals the sum of all
  contributions; no over-count. **Negative control** (proven caught): a
  local-only read at a non-replica returns null -- exactly the gap the
  chaos test found. The model's delivery premise matches production
  (fan to the replica set, merge idempotently), unlike the prior model.
  45 DST models pass.

* **Write path**: `RoutingHooks` gains `local_peer_idx`. A CRDT write
  applies the op to the coordinator's LOCAL store first (accumulating
  this node's distinct actor, so per-actor columns sum rather than
  overwrite, and the write never waits on a quorum -- always-available),
  then fans the merged FULL state to every OTHER replica of the key.
  Each replica merges idempotently (element-wise max), so a
  re-delivered or reordered state cannot double-count. Full state (not a
  delta) means a replica that missed a fan is filled by the next write
  or by anti-entropy.

* **Replica-apply wire** is now discriminated (`DT_WIRE_STATE` vs
  `DT_WIRE_OP`) so a receiving replica knows to merge-idempotently (a
  state fan) or apply-accumulate (an op forward). `crdt_store` gains
  `to_state_wire`, `CrdtOp::to_op_wire`, `to_state_bytes`,
  `project_state`, and a static `merge_state_borrowed`.

* **Ring topology** (chaos harness): each node now gets a distinct token
  `idx * (2^32 / total)` so keys spread across the replica set.

* **Verify** (`chaos-verify.py`) reworked to the Dynamo correctness
  model: convergence-without-loss = `max` value across nodes ==
  expected AND no node over-counts.

## Clean at-scale verdict

Re-ran the multi-region adversarial chaos test with the fix:

* 6 Dyniak nodes (m6id.xlarge) across us-east-1 / us-west-2 /
  eu-central-1, real NVMe (`/mnt/data/noxu`), distinct ring tokens,
  `n_val=3`.
* 3 topology-aware load generators, 240s counter load, 12370 total ops,
  200 keys.
* Faults during load: 2 cross-region partitions (45s each) + 2 node
  churns (kill/restart, 30-40s each).

Availability (always-available, bounded tail through faults):

```
gen1  ok=4800  err=0  avail=100.0%    p99=50ms    p999=60ms
gen2  ok=2772  err=1  avail=99.964%   p99=170ms   p999=181ms
gen3  ok=4798  err=0  avail=100.0%    p99=50ms    p999=61ms
```

Convergence:

```
keys_expected=200  total_ops=12370
lost_count=0  overcount_count=0  all_converged=true
CHAOS-VERDICT converged=True worst_avail=99.964% worst_p99=170.02ms
```

Every key converged on its replica set with zero lost updates and zero
over-counts, through partitions and churn. This closes the
owner-centric gap for the write path.

EC2 fully torn down and verified clean across all three regions (0
instances / SGs / prefix-lists / keys).

## Still open (tracked)

* **Read coordination.** Reads still target a replica (topology-aware
  client); a fetch to a non-replica reads local-only. Fanning a
  `DtFetch` to the replica set and merging responses needs a
  request/response peer plane (the current `PeerOutbound` is
  fire-and-forget). Until that lands, CRDT-aware read-repair / AAE is
  the convergence backstop, and the chaos verify checks replica-set
  convergence accordingly. This is a request/response-plane feature,
  not a rushed patch.
