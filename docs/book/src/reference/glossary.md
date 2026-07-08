# Glossary

Terms used throughout this manual. Where a term has a chapter of its own,
the definition links to it.

**Active anti-entropy (AAE).** A background process that compares
replicas -- using Merkle trees so the comparison is cheap -- and repairs
any divergence it finds. See [Anti-Entropy and Repair](../dyniak/aae.md).

**Backend.** The single-node storage engine a Dynomite node fronts:
Valkey, Memcached, or the embedded Noxu store. The backend knows nothing
about the ring.

**Consistency level.** The per-request quorum policy: `DC_ONE`,
`DC_QUORUM`, `DC_SAFE_QUORUM`, `DC_EACH_SAFE_QUORUM`. It decides how many
replicas must respond before a request succeeds and how reads pick a
replica. See [Replication and Consistency](../architecture/consistency.md).

**Coordinator.** For a given request, the node that received it from the
client and is responsible for routing it to replicas and coalescing the
responses. Any node can coordinate any request.

**CRDT (Convergent Replicated Data Type).** A data type -- counter, set,
map, register -- whose replicas merge deterministically without conflict.
Dyniak feature. See [Convergent Data Types](../dyniak/crdts.md).

**Datacenter (DC).** The top level of the topology. Replication and the
consistency levels are defined across and within datacenters.

**DNODE.** The peer plane: the wire protocol Dynomite nodes use to talk
to each other, distinct from the client-facing protocol. See
[DNODE](../protocols/dnode.md).

**Dyniak.** The Riak-compatible layer built on the engine and backed by
Noxu. See [Introduction to Dyniak](../dyniak/index.md).

**Gossip.** The decentralized protocol nodes use to discover each other,
share topology, and detect failures. See [Membership and
Gossip](../architecture/gossip.md).

**Hinted handoff.** When a write's target peer is temporarily
unavailable, a coordinator stores a durable "hint" and replays it when
the peer returns, so the write is not lost. See [Failure
Handling](../architecture/failure.md).

**Node.** One `dynomited` process (or one embedded `Server`), owning one
or more tokens and fronting one backend.

**Noxu.** The transactional Rust storage engine that backs Dyniak; a
re-implementation of Berkeley DB Java Edition. Provides the XA support
Dyniak's transactions use.

**phi-accrual.** The adaptive failure detector Dynomite uses: it outputs
a continuous suspicion value rather than a binary alive/dead, adapting to
observed network variance. See [Failure Handling](../architecture/failure.md).

**Quorum.** A majority of the relevant replicas. For a local-DC quorum of
`n` replicas the count is `n/2 + 1`.

**Rack.** A subdivision of a datacenter. In Dynomite's replication model a
rack holds a full copy of the ring, so the number of racks in a DC is the
replication factor within that DC.

**Read repair.** When a read observes divergent replicas, the coordinator
writes the reconciled value back to the stale ones. See [Failure
Handling](../architecture/failure.md).

**Replica.** A node (in Dynomite's model, a rack) that holds a copy of a
given key.

**Ring / token ring.** The circular token space over which keys are
partitioned by consistent hashing. See [The Ring and the Token
Space](../architecture/ring.md).

**Token.** A point on the ring. A key hashes to a token; the node owning
the next token clockwise owns the key.

**Vnode (virtual node).** A token owned by a physical node; a physical
node may own several, spreading its ownership around the ring.

**XA / 2PC.** Two-phase commit, the protocol Dyniak uses for cross-node
multi-key atomic transactions. See [Distributed
Transactions](../dyniak/transactions.md).
