# Design Decisions (Roads Not Taken)

<div class="dyn-hero">
Every design is defined as much by what it rejects as by what it ships.
This chapter collects the alternatives Dynomite considered and
deliberately did not take, and why. Other chapters link here whenever a
choice matters.
</div>

Dynomite is opinionated, and its opinions are inherited from the
[Amazon Dynamo paper](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
and refined by two decades of production experience with Netflix
Dynomite and Basho Riak. This page is the honest ledger of the
trade-offs. None of these choices is universally correct; each is correct
*for the problem Dynomite solves* -- adding availability and cross-
datacenter replication to storage engines that lack it.

## Distribution as a layer, not a rewrite

**Chosen:** wrap an unmodified single-node store (Valkey, Memcached,
Noxu) with a separate distribution layer.

**Not chosen:** fork the storage engine and build replication into it.

A store that knows nothing about the ring stays simple, stays fast, and
stays independently upgradable. The distribution logic lives in one place
and works the same regardless of backend. The cost is a network hop for
non-local keys and the fact that the layer cannot exploit engine
internals (it cannot, say, replicate at the storage-page level). For the
goal -- portable HA across heterogeneous backends -- the separation wins.

## Tunable quorum, not consensus

**Chosen:** the Dynamo model -- eventual consistency with per-request
tunable quorums (`DC_ONE`, `DC_QUORUM`, and friends), plus read repair
and anti-entropy to converge divergent replicas.

**Not chosen:** a consensus protocol (Raft, Paxos, or multi-Paxos) giving
linearizable single-key writes.

```admonish note title="What this costs you"
Dynomite does not offer linearizability for ordinary reads and writes. Two
clients writing the same key concurrently under <code>DC_ONE</code> can
both succeed and produce divergent replicas that are reconciled later.
This is the deliberate Dynamo trade: availability and low, predictable
tail latency during partitions, in exchange for eventual (not immediate)
consistency. If you need linearizable writes, you need a consensus system,
and Dynomite is the wrong tool.
```

Consensus buys linearizability at the price of availability under
partition (a minority partition cannot make progress) and of a latency
floor set by the slowest quorum member on every write. Dynomite's users
chose availability. See [Replication and
Consistency](./../architecture/consistency.md).

## Cross-node transactions: 2PC, not Paxos-commit

**Chosen (Dyniak):** two-phase commit (XA) over Noxu's transactional
engine for cross-node multi-key atomic updates, with a RAMP-style path
for read-atomic multi-key reads.

**Not chosen:** a Paxos-based atomic commit or a full distributed SQL
transaction manager.

2PC is simple, well understood, and sufficient for the bounded,
short-lived multi-key updates Dyniak targets. Its classic weakness -- a
coordinator crash can block participants -- is bounded here by timeouts
and the fact that participants are Dynomite peers already under failure
detection. A Paxos-commit would remove the blocking window at the cost of
substantially more machinery and latency; for the workload it was not
worth it. See [Distributed Transactions](./../dyniak/transactions.md).

## Conflict resolution: CRDTs and vector clocks, not last-write-wins-only

**Chosen (Dyniak):** convergent replicated data types (counters, sets,
maps, registers) for conflict-free merges, plus causal context for
sibling-aware writes.

**Not chosen:** last-write-wins on a wall-clock timestamp as the only
resolution strategy.

Last-write-wins is available and is the right default for opaque values,
but it silently discards concurrent updates. CRDTs let a counter that was
incremented on two partitioned nodes converge to the correct sum instead
of losing one increment. The cost is that CRDT values carry more
metadata and the client must use the typed API. See [Convergent Data
Types](./../dyniak/crdts.md).

## Gossip membership, not a central coordinator

**Chosen:** decentralized gossip for membership, topology discovery, and
failure detection.

**Not chosen:** a central coordinator or an external service (ZooKeeper,
etcd) holding the cluster's membership.

A coordinator is a single point of failure and an operational dependency.
Gossip has no such center: any node can join by contacting a seed, and
state propagates epidemically. The trade-off is that membership is
eventually consistent -- a just-joined node is not instantly visible
everywhere -- and gossip uses steady background bandwidth. See
[Membership and Gossip](./../architecture/gossip.md).

## phi-accrual failure detection, not a fixed timeout

**Chosen:** the phi-accrual detector, which outputs a continuous
suspicion level adapting to observed network variance.

**Not chosen:** a fixed heartbeat timeout ("declare dead after N missed
beats").

A fixed timeout forces a bad choice: short enough to detect failures
quickly means false positives on a jittery link; long enough to avoid
false positives means slow detection. phi-accrual adapts its threshold to
the link's actual behavior. See [Failure Handling](./../architecture/failure.md).

## Consistent hashing, with pluggable distribution

**Chosen:** a token ring as the default, with alternative distribution
modes (including random slicing) selectable per pool.

**Not chosen:** hard-coding one hashing scheme.

Consistent hashing minimizes the keys that move when the ring changes,
which is the property that matters most for a cache or store front. But it
can leave ownership uneven with few nodes; random slicing rebalances more
evenly at the cost of strict monotonicity. Rather than pick one, Dynomite
makes the strategy a configuration choice and ships the
[`random_slicing`](./../examples/random_slicing.md) example so you can
measure the difference. See [Distribution Modes](./../operations/distribution.md).

## Property, DST, and Elle testing, not unit tests alone

**Chosen:** for anything on the distributed path, a deterministic
simulation model (with a negative control that has teeth) plus an
Elle-style consistency check on recorded histories, in addition to unit,
property, and fuzz tests.

**Not chosen:** treating a green unit suite as sufficient for distributed
correctness.

Several real distributed defects in this port passed the entire unit
suite and were only caught at scale or by a model. The project codifies
model-first reproduction of any distributed failure as a merge gate. This
is a testing philosophy, but it is also a design constraint: a change
that cannot be expressed in a deterministic model is, by that standard,
not ready. See [Chaos Test](./../operations/chaos.md) and the
conformance and coverage chapters.

## Rust, `forbid(unsafe_code)`, not a C fork

**Chosen:** a from-scratch Rust reimplementation with `unsafe` forbidden
by default.

**Not chosen:** maintaining a fork of the original C.

Memory safety without a garbage collector, a strong type system for the
protocol and state machines, and a modern async runtime were judged worth
the cost of a full rewrite and the discipline of maintaining parity with
the C reference. Parity is tracked symbol by symbol in
[`docs/parity.md`](DYN_SRC_BASE/docs/parity.md).
