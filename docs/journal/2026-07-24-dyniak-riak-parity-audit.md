# 2026-07-24 -- Dyniak vs Riak: parity audit and gap ledger

Three independent read-only audits compared Dyniak (the Riak-compatible
crate) against Apache Riak KV on (1) read repair / anti-entropy, (2)
functional behavior, and (3) performance / scale evidence. This entry
records the findings and the resulting work queue. The bar the user set:
Dyniak should be nearly identical to Riak on functional/consistency
dimensions and similar-or-better on benchmarked dimensions.

## 1. Read repair and anti-entropy

### What Riak does
On a GET the coordinator reads N replicas, waits for R, reconciles by
vector-clock causality (siblings if concurrent), and asynchronously
writes the winner back to stale/missing replicas (read repair). For data
types it merges replica states on read and repairs with the merged
state. AAE (Merkle/hash trees per vnode) detects and repairs divergence
in the background.

### What Dyniak does (post this session)
* **CRDT read coordination: NOW PRESENT.** `handle_dt_fetch` fans a
  `DtFetch` to the key's replica set, merges the returned states, and
  answers with the converged value; it also writes the merged state back
  locally (read repair for the coordinator). This landed this session
  (`feat(dyniak): CRDT read coordination`) over a new request/response
  peer seam (`ReplicaApplySink::apply_query` + `PeerOutbound::request` +
  a dnode `Res` write-back). On a transport without request/response it
  falls back to the local value with AAE as the backstop.
* **CRDT write convergence: PRESENT** (fixed earlier this session):
  writes apply locally and fan full state to all replicas; each merges
  idempotently. Validated at scale: 0 lost, 0 over-count over 200 keys /
  12370 ops through partitions + churn.
* **AAE: PRESENT.** `aae/tictac.rs` (Merkle tree), `aae/exchange_fsm.rs`
  (ROOT/TREE/KEY sync), `aae/repair.rs` (divergence resolution +
  dispatch), persisted trees.

### Remaining read-repair gaps (tracked)
* **KV object read coordination / quorum R,PR: PARTIAL/MISSING.** The PBC
  `handle_get` fans `PeerOp::Get` but returns the LOCAL value and never
  waits or merges; read repair for opaque objects is wired at the
  Redis/dispatch layer, not the PBC layer. Needs R/PR read semantics on
  the PBC path (effort M, uses the new request/response seam).
* **Vector-clock causality on the PBC read path: PARTIAL.** ITC exists
  for CRDT actor identity; opaque-object reads pick winners by checksum,
  not by causal domination, and siblings are not surfaced on PBC.
* **CRDT-aware AAE repair payload: refine (S).** AAE ships the
  lexicographically-largest bytes; for CRDT keys it should ship the
  merged state.

## 2. Functional behavior differences

Dimension scoreboard (audit 2):

| Dim | Area | Status |
| --- | --- | --- |
| A | Data model (buckets/types/keys/objects/links/2i) | FULL except server-assigned keys (S) and 2i term enumeration (deferred) |
| B | Vector clocks / causal context | ITC (not DVV) -- documented; siblings detected but not surfaced |
| C | Conflict resolution | PARTIAL: siblings not surfaced to clients (see below) |
| D | Quorum tunables (N/R/W/PR/PW/DW) | PARTIAL: N present; R/W/PR/PW/DW not on the read path; hinted-handoff persistence missing |
| E | CRDTs | Counter/Set/Register/Flag present; Map/HLL code exists but not exported/handled |
| F | Query (MapReduce/2i/search) | FULL; Yokozuna/Solr is a documented non-goal |
| G | Bucket props | PARTIAL: TTL field ignored; pre/postcommit hooks not executed |
| H | APIs (PBC/HTTP) | Near-complete; server-assigned keys (POST unnamed) missing |
| I | Storage backends | noxu (design choice); TTL/expiry missing |
| J | Strong consistency (riak_ensemble) + riak_repl | Documented non-goals |
| K | Cluster ops (ring/gossip/handoff) | Handoff FSM present; durable handoff queue missing |

### Silent gaps a Riak user would hit (ranked)
1. **Siblings never surfaced.** Concurrent writes are detected (ITC) but
   the client always sees a single value; `RpbGetResp.contents` never
   carries a sibling array. This diverges from `allow_mult=true`
   semantics. Effort M.
2. **TTL / object expiry ignored.** `RpbBucketProps.ttl` is dropped on
   decode; no reaper-driven expiry. Silent unbounded growth. Effort M.
3. **Hinted handoff not persisted.** Writes to a downed peer are dropped
   (fire-and-forget) rather than durably queued for replay. Effort L.
   (Deferred, but not clearly flagged in code.)
4. **Server-assigned keys rejected.** `POST /buckets/{b}/keys` returns an
   error instead of `201 + Location`. Effort S. **DONE 2026-07-24**:
   `POST /buckets/{b}/keys` (no key) now stores under a generated
   base36 key and replies `201 Created` + `Location`.
5. **Map + HLL CRDTs not reachable.** Modules exist but are not exported
   and have no handlers. Effort M.
6. **Pre/postcommit hooks not executed.** Wire fields skipped on decode.
   Effort L (needs a hook execution model).

### Documented deliberate non-goals (not gaps)
riak_ensemble (strong consistency), riak_repl (MDC replication), and
Yokozuna/Solr search are called out in `README.md` (Status) and
`docs/riak-comparison.md`. Per-node RediSearch-shaped search stands in
for Riak Search.

## 3. Performance / scale evidence

### What exists
* `dyniak-bench` (a basho_bench-equivalent load driver: RESP, riak_pbc,
  riak_http, riak_quic drivers; TOML workloads; basho-shaped CSV/SVG
  output).
* criterion micro-benches (parsers, hashkit, mbuf, tokens, dnode,
  crypto, quorum) with a 10%-regression gate.
* Multi-host chaos harness with p50/p99/p999 and failure injection.

### What is measured
* Single-node loopback: ~76k ops/sec, p99 ~3ms (Valkey-backed, no disk).
* Multi-host chaos: 4.1M ops / 2h across 4 hosts, ~92% success under
  continuous fault injection.
* Multi-region Dyniak CRDT chaos (this session): 100% / 99.96% / 100%
  availability, p99 50-170ms through partitions + churn.

### Gaps to a credible "similar or better than Riak" claim
1. **No head-to-head Dyniak-vs-Riak benchmark.** The 76k/p99-3ms number
   is single-node-on-loopback backed by Valkey (no disk); Riak is
   disk-backed. Not apples-to-apples. Effort M; needs real Riak deployed
   side-by-side (AWS burner, no cost concern).
2. **No scale-out linearity curve** (1/2/4/8 nodes under quorum). M.
3. **No tail latency under distributed quorum** (p99.9/p99.99 at w=2 on
   a 3-node cluster with netem delay). M.
4. **criterion baselines are empty placeholders** -- the regression gate
   is inert until populated. S.
5. **Workload coverage** in dyniak-bench lacks 2i-range and MapReduce
   op classes. M.

Honest claim until (1) lands: "performance comparable to Riak on
equivalent hardware, with Rust latency-tail gains observed single-node",
NOT "better than Riak".

## Work queue (ranked)

Now done this session:
* [x] CRDT read coordination (the #1 read-repair gating item).
* [x] Server-assigned keys: `POST /buckets/{b}/keys` -> 201 + Location.

Near-term correctness parity (highest surprise for a Riak user):
1. KV object quorum read + read repair on the PBC path (R/PR). M.
2. Surface siblings on PBC/HTTP for `allow_mult` buckets. M.
3. TTL / object expiry via the reaper. M.
4. Server-assigned keys (POST unnamed). S. **DONE.**
5. Map + HLL CRDT handlers (export + wire). M.
6. Durable hinted-handoff queue. L.
7. Pre/postcommit hooks. L.

Benchmark credibility:
8. Populate criterion baselines; activate the regression gate. S.
9. Head-to-head Dyniak-vs-Riak single-node bench on EC2. M.
10. Scale-out linearity + distributed-quorum tail latency. M.

None of these are code-blocking for the CRDT convergence work already
shipped; they are the roadmap to "nearly identical to Riak" on function
and "demonstrably similar-or-better" on performance.
