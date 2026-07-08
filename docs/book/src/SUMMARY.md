# Summary

[Dynomite](./intro.md)

# Getting Started

- [Why Dynomite?](./getting-started/why.md)
- [Concepts in Ten Minutes](./getting-started/concepts.md)
- [Your First Cluster](./getting-started/first-cluster.md)
- [Your First Embedded Engine](./getting-started/first-embed.md)
- [Tutorial: Vector, Text, and Regex Search](./tutorial-search.md)

# The Dynomite Engine

- [Architecture](./architecture.md)
  - [The Ring and the Token Space](./architecture/ring.md)
  - [Replication and Consistency](./architecture/consistency.md)
  - [Membership and Gossip](./architecture/gossip.md)
  - [Failure Handling](./architecture/failure.md)
- [Configuration](./configuration.md)
- [Protocols](./protocols/index.md)
  - [Redis / Valkey](./protocols/redis.md)
  - [Memcache](./protocols/memcache.md)
  - [DNODE (Peer Plane)](./protocols/dnode.md)
- [Transports](./transports/index.md)
  - [TCP](./transports/tcp.md)
  - [QUIC](./transports/quic.md)
- [Security](./security/crypto.md)

# The Embedding API

- [Overview](./embedding/index.md)
- [Server Lifecycle](./embedding/server.md)
- [Hooks and Traits](./embedding/hooks.md)
- [Cookbook](./embedding/cookbook.md)

# Dyniak: The Riak-Compatible Layer

- [Introduction to Dyniak](./dyniak/index.md)
- [Getting Started with Dyniak](./dyniak/getting-started.md)
- [Buckets, Keys, and Objects](./dyniak/objects.md)
- [Convergent Data Types (CRDTs)](./dyniak/crdts.md)
- [Distributed Transactions (XA / 2PC)](./dyniak/transactions.md)
- [Links and Link Walking](./dyniak/links.md)
- [Secondary Indexes and MapReduce](./dyniak/mapreduce.md)
- [Full-Text, Vector, and Regex Search](./dyniak/search.md)
- [Anti-Entropy and Repair](./dyniak/aae.md)
- [The Dyniak Wire Protocols](./protocols/dyniak.md)

# Operations

- [Recommendations](./operations/recommendations.md)
- [Running dynomited](./operations/running.md)
- [Admin CLI (dyn-admin)](./operations/admin.md)
- [Metrics](./operations/metrics.md)
- [Distributed Tracing and OTLP Logs](./operations/tracing.md)
- [Distribution Modes](./operations/distribution.md)
- [Riak Mode](./operations/riak.md)
- [Dyniak Features](./operations/dyniak-features.md)
- [Benchmarks](./operations/benchmarks.md)
- [Conformance Suite](./operations/conformance.md)
- [Coverage Gate](./operations/coverage.md)
- [Chaos Test](./operations/chaos.md)
- [Release Process](./operations/release.md)

# Examples

- [Reading the Examples](./examples/index.md)
- [embedded_minimal](./examples/embedded_minimal.md)
- [embedded_single_node](./examples/embedded_single_node.md)
- [embedded_cluster3](./examples/embedded_cluster3.md)
- [random_slicing](./examples/random_slicing.md)
- [embedded_custom_transport_sketch](./examples/custom_transport.md)
- [Search: demo_vector_text](./examples/demo_vector_text.md)
- [Vector: quickstart](./examples/vec_quickstart.md)

# Reference

- [Internals](./internals/index.md)
- [Design Decisions (Roads Not Taken)](./reference/roads-not-taken.md)
- [Glossary](./reference/glossary.md)
- [Manual Pages](./reference/man-pages.md)

---

[Contributing and the Parity Discipline](./reference/contributing.md)
