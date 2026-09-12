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
* **Vector-clock causality on the PBC read path: PARTIAL (improving).**
  Opaque objects now carry a per-object ITC causal context: a PUT
  advances the context and returns it (`RpbPutResp.vclock` /
  `X-Riak-Vclock`), a GET returns it, and the write path detects a
  concurrent write (client context diverges from stored). Sibling
  RETENTION on a concurrent write (storing both values) and
  cross-replica causal read-repair remain follow-up slices.
* **CRDT-aware AAE repair payload: refine (S).** AAE ships the
  lexicographically-largest bytes; for CRDT keys it should ship the
  merged state.

## 2. Functional behavior differences

Dimension scoreboard (audit 2). NOTE 2026-09-12: several rows below
were the original audit snapshot; the parenthetical **[now: ...]**
annotations record what has since landed (see the Work queue at the end
of this file for the authoritative current status).

| Dim | Area | Status |
| --- | --- | --- |
| A | Data model (buckets/types/keys/objects/links/2i) | FULL except 2i term enumeration (deferred). **[now: server-assigned keys DONE]** |
| B | Vector clocks / causal context | Per-object VERSION VECTOR (`vclock.rs`, not ITC/DVV) flows on get/put; concurrent writes detected. **[now: sibling retention DONE]** |
| C | Conflict resolution | **[now: DONE -- siblings retained under allow_mult; PBC multi-content read + HTTP 300]** |
| D | Quorum tunables (N/R/W/PR/PW/DW) | N/R/W enforced on the read/write path. **[now: R/W enforcement DONE; PR/PW/DW accepted+echoed but not applied]** |
| E | CRDTs | **[now: DONE -- all six (Counter/Set/Register/Flag/Map/HLL) served over the wire]** |
| F | Query (MapReduce/2i/search) | FULL; Yokozuna/Solr is a documented non-goal |
| G | Bucket props | **[now: `ttl` applied by runtime reaper; precommit hooks DONE; postcommit not implemented]** |
| H | APIs (PBC/HTTP) | **[now: server-assigned keys DONE]** |
| I | Storage backends | noxu (design choice). **[now: object TTL/expiry DONE via reaper]** |
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
* [x] All six CRDTs wired over the wire: Counter, Set, Register, Flag,
  Map (recursive), and HyperLogLog.
* [x] Object TTL expiry in the reaper FSM
  (`ReaperConfig::object_ttl_seconds`) + the `ttl` bucket property
  (settable/readable over PBC); the runtime reaper orchestrator that
  applies the per-bucket ttl remains.

Near-term correctness parity (highest surprise for a Riak user):
1. KV object quorum read + read repair on the PBC path (R/PR). M.
   **Read coordination + read-repair DONE: a GET fans to the key's
   replica set (`PeerOp::Get` via the request/response seam), merges
   the returned sibling sets by causal frontier, returns the converged
   value(s), and read-repairs replicas that were behind
   (`PeerOp::RepairPut` ships the resolved SiblingSet storage verbatim).
   The write fan also ships the resolved storage so replicas hold
   byte-identical, causally-correct copies. **Quorum tunables DONE too:**
   `r`/`w`/`pr`/`pw`/`dw` are bucket properties with per-request
   overrides (`crate::quorum::resolve`, symbolic one/quorum/all/default);
   a GET enforces R (counts responses, fails below quorum) and a PUT
   enforces W (counts local + acked replica writes, fails below quorum),
   with availability fallback on a fire-and-forget transport. DST model
   `quorum_decision` (sound-success + available-above-quorum + a
   one-wins negative control) and integration tests
   (`quorum_enforcement`) cover it.**
2. Surface siblings on PBC/HTTP for `allow_mult` buckets. M. **DONE:
   per-object version-vector context; concurrent writes retained as
   siblings under allow_mult; PBC multi-content read + HTTP 300 Multiple
   Choices; cross-replica causal read coordination + read-repair (see
   item 1).**
3. TTL / object expiry via the reaper. M. **DONE: reaper FSM
   (`object_ttl_seconds`), `ttl` bucket property (PBC settable/readable),
   object write-timestamp (`HttpObject.written_at_unix`), and a runtime
   `ReaperOrchestrator` spawned in dynomited that sweeps the primary
   key space on an interval and deletes objects past their bucket TTL.**
4. Server-assigned keys (POST unnamed). S. **DONE.**
5. Map + HLL CRDT handlers (export + wire). M. **DONE (all six CRDTs
   are now wire-reachable).**
6. Durable hinted-handoff queue. L. **ALREADY DONE (audit finding was
   about the RAM-only default): `HintStore::open` gives a durable,
   write-through, replay-on-restart backend under `<hint_dir>/peer-<n>
   .hints`, wired in dynomited when `hint_dir` is configured
   (`enable_hinted_handoff` + `hint_dir`). Tested by
   `durable_round_trip_survives_reopen` and the replay suite. The
   default remains RAM-only (durability is opt-in via `hint_dir`).**
7. Pre/postcommit hooks. L. **Precommit DONE: a WASM precommit hook
   (`crate::precommit::PrecommitHooks`, same linear-memory ABI as
   keyfuns / MapReduce) named per bucket via the `precommit_module`
   property runs on every object write; it may transform the value or
   VETO the write (rejected -> error frame, nothing stored). Wired
   through `RoutingHooks.precommit` (a feature-agnostic
   `PrecommitRunner` trait) and spawned in dynomited from the pool's
   WASM store. Postcommit (fire-and-forget notification after a
   successful write) remains -- it needs an async side-effect plane and
   does not gate the write.**

Benchmark credibility:
8. Populate criterion baselines; activate the regression gate. S.
9. Head-to-head Dyniak-vs-Riak single-node bench on EC2. M.
10. Scale-out linearity + distributed-quorum tail latency. M.

None of these are code-blocking for the CRDT convergence work already
shipped; they are the roadmap to "nearly identical to Riak" on function
and "demonstrably similar-or-better" on performance.
