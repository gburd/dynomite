# dyniak

`dyniak` is the Riak-compatible protocol layer for dynomite. It speaks
the wire formats Riak KV exposes (Protocol Buffers over TCP, JSON over
HTTP) and bridges them to dynomite's distributed substrate via
`dynomite::embed::Datastore`.

## Acknowledgements: Basho and the Riak project

dyniak exists because of two decades of work by the engineers at
**[Basho](https://en.wikipedia.org/wiki/Basho_Technologies)** and the
broader **[Riak](https://github.com/basho/riak)** open-source
community. Riak was the canonical real-world implementation of the
Amazon Dynamo paper (DeCandia et al., SOSP 2007): a masterless,
ring-distributed, eventually-consistent key-value store with
configurable per-request quorums, vnode-based partitioning,
hinted handoff, read repair, active anti-entropy, sloppy quorums,
last-write-wins / vector-clock conflict resolution, secondary indexes,
CRDTs (the riak_dt family), MapReduce, search via Solr, and a
production-tested operations story.

Riak was years ahead of its time. Many of the patterns now standard
in distributed databases (chash rings with vnodes, quorum tuples
exposed at the API level, sibling-aware writes) were first widely
deployed in Riak. The Erlang/OTP code in `riak_core`, `riak_kv`,
`riak_pipe`, `riak_search`, and `riak_dt` is the reference
implementation for an entire category of systems.

We owe a debt of gratitude to the Basho team -- Andy Gross, Justin
Sheehy, Rusty Klophaus, Sean Cribbs, Joseph Wayne Norton, Russell
Brown, and many others -- who built Riak in the open, wrote about
the design choices, and answered questions on the riak-users mailing
list for over a decade. dyniak is downstream of their thinking; the
explicit goal of this crate is to make it possible to drop
`dyniak`-fronted dynomite into a slot a Riak cluster used to
occupy and have client applications keep working.

If you operated a Riak cluster in production and want to keep
the API contract while migrating off the EOL'd Erlang stack:
that is the audience for dyniak.

## How dyniak is similar to Riak

* **Wire-protocol compatibility**: dyniak speaks Riak's PBC (Protocol
  Buffers binary) protocol on its TCP listener. The same `riak-erlang-client`,
  `riak-python-client`, `riak-go-client`, `riak-java-client`, and
  `Riak-Client.NET` libraries that talk to a Riak cluster talk to
  dyniak.
* **HTTP REST surface**: dyniak ships an HTTP gateway with the
  `/buckets/<bucket>/keys/<key>` shape Riak's HTTP API uses, plus
  `/types/<bucket-type>/buckets/<bucket>/keys/<key>` for typed
  buckets, the `/mapred` MapReduce endpoint, and `/buckets/<bucket>/index/<idx>/...`
  for secondary index queries.
* **CRDTs**: convergent data types on the Riak model. Counter and Set
  are served over the wire today; Register, Flag, Map, and HyperLogLog
  are implemented in-crate and tracked to be wired next. See
  `crates/dyniak/src/datatypes/`.
* **Per-request quorums**: `R`, `W`, `PR`, `PW`, `DW`, `RW` are accepted
  for API compatibility. Only `n_val` (the replica count) is enforced
  today; the quorum knobs are not yet applied on the read/write path.
* **Conflict resolution**: writes that race are detected via the causal
  context and resolved to a single deterministic value. Sibling sets
  are not yet surfaced to clients (no `300 Multiple Choices`), and
  `allow_mult` does not yet change read behavior; for concurrent-write
  correctness use a CRDT. Causality is tracked with an interval tree
  clock (ITC), not a Riak DVV (a documented deviation).
* **Active anti-entropy**: dyniak's AAE exchange protocol is
  modelled on Riak's `riak_kv_index_hashtree` (the TicTac-style
  segmented merkle tree at `crates/hashtree/`).
* **Hinted handoff**: same shape as `riak_kv_handoff`.
* **MapReduce**: a JSON pipeline driven by the same
  `inputs / query / timeout` envelope; built-in phases match the
  Erlang module names where possible (`riak_kv_mapreduce`).
* **Bucket types**: declared, not auto-created. `n_val` is honored;
  the remaining properties (quorum knobs, `allow_mult`,
  `last_write_wins`) are accepted but not yet enforced.

## How dyniak differs from Riak

* **Language and runtime**: Rust + Tokio, not Erlang + BEAM. No
  native distribution, no `gen_server` mailboxes; we mirror the
  *patterns* via `gen-fsm` (a state-functions FSM driver in
  `crates/gen-fsm/`) and `sup` (an OTP-style supervisor tree in
  `crates/sup/`). Same shape, different substrate.
* **Storage backend**: pluggable, default Noxu (an embedded
  transactional B+tree engine published on crates.io). Riak
  shipped Bitcask, eLevelDB, and the LevelEd backends; dyniak's
  storage trait lets operators plug in any KV engine that
  implements `dyniak::Datastore`.
* **Integrated text + vector search**: dyniak inherits dynomite's
  `dyntext` (trigram + bloom + TRE-backed approximate-regex) and
  `dynvec` (HNSW + turbovec quantisation) crates. Riak's
  search story leaned on Solr (riak_search 1.x) or yokozuna
  (riak_search 2.x via Apache Solr); dyniak does it in-process.
* **Strong-consistency mode**: out of scope. Riak had `riak_ensemble`
  (Raft-per-bucket); we do not. If you need strong consistency
  use a different store. dyniak is honest about being eventually
  consistent.
* **Cross-DC replication (MDC)**: out of scope for v1. Riak shipped
  `riak_repl` (realtime + fullsync). The substrate (gossip, vnode
  ownership, AAE) is in place; cross-DC realtime queues are a
  follow-up.
* **`riak admin` CLI**: replaced by `dyn-admin` (a separate crate)
  which speaks PBC management ops the same way `riak admin cluster`
  / `riak admin bucket-type` did, but with the verbs Rust developers
  expect.
* **Configuration**: YAML, not advanced.config / app.config. The
  `dyniak::Config` type is the structured surface; operators who
  want a config file load it via `serde_yaml`.
* **Observability**: OpenTelemetry traces + Prometheus metrics
  out of the box. Riak had `folsom` and a custom HTTP `/stats`
  endpoint; dyniak emits OTLP and exposes `/metrics` in Prometheus
  exposition format directly.
* **Hot code reload**: Erlang has it; Rust does not. Updating
  dyniak is a process restart. The cluster substrate handles a
  rolling restart cleanly (gossip + vnode handoff are stable
  across rolling upgrades; the `cluster::capability` module
  provides version-aware capability negotiation).

## Where it sits in dynomite

dynomite is the cluster substrate: hashing, gossip, vnodes,
quorum, hinted handoff, read repair, AAE. dyniak is one of three
protocol layers operators can put in front of it:

* `dynomite::proto::redis` (default): Redis Stack RESP, including
  RediSearch FT.* commands for vector and text search.
* `dynomite::proto::memcache` (default): memcache binary + ASCII.
* `dyniak`: Riak PBC + HTTP + CRDTs + MapReduce + AAE.

The crate is **embedded** by default: `dynomited` links `dyniak`
behind the `--features riak` switch and instantiates the Riak
listener in-process alongside the existing Redis/Memcached
listeners. Operators who want process isolation can run separate
`dynomited` processes per protocol; the substrate is the same.

## What's in v0.0.1

* Riak PBC framing (4-byte big-endian length, 1-byte message
  code, protobuf body).
* Operation codes 1-26 + the CRDT data-type ops 80-83 + the
  Dynomite-extension cluster-admin ops 200-209 + 220-221 for AAE
  status.
* `serve_pbc` connection driver: reads framed PBC requests from
  a `tokio::net::TcpListener`, dispatches them through any
  `dynomite::embed::Datastore`, writes framed responses.
* HTTP gateway (axum-based) for the `/buckets/...`, `/types/...`,
  `/mapred`, and `/buckets/<bucket>/index/...` paths.
* CRDT types: Counter and Set served over the wire; Register, Flag,
  Map, and HyperLogLog implemented in-crate, tracked to be wired next.
* MapReduce pipeline: 9 built-in phases + Wasm-hosted user phases
  (gated under `--features wasm`).
* Tictac-style AAE (segmented merkle tree, persisted across
  restart, per-token exchange).
* Hinted handoff with explicit FSM and chunked transfer with
  throttling and per-state timeouts.
* TTL-driven sibling/tombstone reaper (`riak_kv_reaper` shape).
* `NoxuDatastore`: bridges to the in-process Noxu DB storage
  engine via the `noxu` umbrella crate (gated behind the
  `noxu` Cargo feature).

## What's not yet in v0.1

* Strong-consistency / `riak_ensemble` equivalent (out of scope;
  see "How dyniak differs" above).
* Cross-DC realtime replication (riak_repl realtime queue + fullsync).
* `riak admin handoff` style operator commands beyond what
  `dyn-admin handoff` exposes.
* Object encoding fidelity at the byte level for causality-context
  + sibling serialization. The semantics are correct, the
  bytes-on-the-wire are not always identical to Riak 2.9; clients
  that crack the per-object causality blob (Itc-encoded; the
  blob travels in the same Riak header slot but with the dyniak
  Itc shape) need a switched decoder. Clients that round-trip
  the bytes opaquely keep working unchanged.
  The shape is documented under `docs/dyniak/wire-compat.md`.
