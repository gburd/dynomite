# Chaos finding: Dyniak CRDT convergence is owner-centric, not per-replica

Date: 2026-07-24
Run: chaos-20260723-222519 (clean 6-node / 3-region cluster on local
NVMe, 3 regional load generators, 2 net splits + 2 node churns during a
240s counter workload).

## What the scale test proved

**Availability + steady p99: PASS.** Through two cross-region net splits
and two node churns during live load, every load generator stayed
essentially fully available with bounded p99:

  gen1 (us-east-1): 100.0%   p99 50.1ms   (partition + churn)
  gen2 (us-west-2): 99.964%  p99 160.4ms  (churn)
  gen3 (eu-central-1): 100.0% p99 54.4ms  (partition)

Single-key CRDT updates were accepted throughout, including during
partitions and ring changes, when the client is topology-aware (fails
over to another node). This is the always-available, steady-p99
property, and it holds at scale.

## What the scale test surfaced (a real gap)

**Per-replica CRDT convergence: FAIL.** After quiescence, fetching every
key from every node showed:

  * The token OWNER for the shared keyspace (both us-east-1 nodes)
    converged to the exact arithmetic total: k0 = 58 = sum of all 3
    generators' increments (15 + 17 + 26). 0 mismatches on 200 keys.
  * Non-owner nodes in the other regions held only PARTIAL values (k0 =
    25 on us-west-2, 26 on eu-central-1) -- roughly their own region's
    contribution plus some -- and did NOT converge to 58.
  * The two racks that received no client load held NO value for the
    keys (null).

A `DtFetch` sent directly to a non-owner node returns that node's local
partial state; it does not route the read to the owner, nor does it
merge across replicas.

## Interpretation

Dyniak's CRDT convergence today is OWNER-CENTRIC:

  * A write is applied locally on the receiving node and the merged
    state is forwarded to the key's replicas. In practice the writes
    converge onto the token owner (the owner accumulates everyone's
    contributions and holds the correct total), so a read routed to the
    owner is correct and always-available.
  * But the reverse -- the owner's merged state propagating back to
    every non-owner replica so that ALL replicas converge to the total
    -- does not complete. Non-owner replicas lag with partial values,
    and a fetch to a non-owner is answered from its local partial state
    rather than routed to the owner or merged.

So the guarantee that HOLDS is "read from the owner converges to the
CRDT-correct value, always-available." The guarantee that does NOT yet
hold is "every replica converges to the expected value" -- the strong
form the Dyniak promise implies.

Why the in-tree tests pass while this fails at scale: the unit and
integration tests (crdt_pbc_round_trip, the crdt_store tests) exercise
the owner/local-apply path and a direct replica state-merge, both of
which are correct. They do not exercise the full multi-region topology
where a fetch can land on a non-owner replica that only saw a subset of
the writes. The DST convergence model asserts that a replica which has
MERGED a given set of ops converges -- which is true -- but the
production replication does not actually deliver every owner-merged
state to every replica, so the model's premise ("all replicas see the
same set of ops") is not met in practice.

## Required follow-up (per AGENTS.md 6.5, before claiming per-replica convergence)

1. Decide the intended model:
   (a) Riak-style: a DtFetch is routed/coordinated to the key's replica
       set and the responses are merged (R replicas), so a read
       converges regardless of which node the client hit; or
   (b) full background convergence: anti-entropy ships owner state to
       every replica so all replicas hold the total.
   Riak does (a) at read time (the coordinator merges R sibling values)
   AND (b) via read-repair + AAE. Dyniak needs at least (a) for a fetch
   to any node to be correct.
2. Extend the DST model so its premise matches production: model the
   actual delivery (owner receives forwarded ops; non-owners receive
   owner state only via AAE/read-repair), and assert either read-time
   merge (a) or eventual full propagation (b). The current model passes
   because it assumes delivery that the code does not perform -- that
   is the model-vs-code mismatch AGENTS.md 6.5 warns about, and it is
   the reason this gap reached a scale test instead of a unit test.
3. Implement read-coordination (fan a DtFetch to the replica set, merge
   the responses) and/or CRDT-aware read-repair + AAE for data-type
   keys, then re-run this chaos test to a clean per-replica-converged
   verdict.

## Honest status

The 1.5.0 CRDT feature delivers always-available single-key updates that
converge AT THE OWNER -- real and validated at scale. It does NOT yet
deliver "every replica converges," and the marketing/claim language for
Dyniak CRDTs must say "converges at the coordinating replica; full
per-replica convergence via read-coordination/AAE is in progress" until
the follow-up lands. This gap was found by the scale chaos test doing
exactly its job.
