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
* **AAE: PRESENT + PUSH WIRED.** `aae/tictac.rs` (Merkle tree),
  `aae/exchange_fsm.rs` (ROOT/TREE/KEY sync), `aae/repair.rs`
  (divergence resolution + dispatch), persisted trees. As of `49bac1b`
  `dynomited::spawn_aae` drives a background full-state PUSH per tick
  (bounded, via `RepairPut`); the tree-diffed PULL exchange over a
  bidirectional peer plane remains a follow-up.

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
| D | Quorum tunables (N/R/W/PR/PW/DW) | N/R/W enforced on the read/write path. **[now: R/W enforcement DONE; PR/PW/DW counting logic DONE + DST-gated -- enforced wherever the substrate supplies primary/fallback + durable acks; production wires no liveness source yet, so PR still aliases R there]** |
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
   WASM store. Postcommit DONE (`49bac1b` era): a fire-and-forget
   `PostcommitRunner` runs the committed value through a per-bucket
   `postcommit_module` after the write quorum is satisfied and before
   the response; it returns nothing and cannot change the reply, so a
   failed/quorum-short write never triggers it. Wired through
   `RoutingHooks.postcommit`, mirroring precommit.**

8. PR / PW / DW quorum enforcement. M. **DONE (counting path real,
   test-proven with an oracle; production has no liveness source wired
   yet). `crate::quorum::resolve` maps the symbolic magic values;
   `handle_get`/`handle_put` enforce PR/PW/DW alongside R/W by counting
   primary vs fallback acks and durable vs buffered acks. A
   `ReplicaLiveness` trait + `plan_replicas_with_liveness` +
   `ReplicaTarget.is_fallback` carry the primary/fallback distinction,
   and an `ACK_STORED` / `ACK_STORED_DURABLE` ack (inspecting noxu
   COMMIT_SYNC) carries durability. HONEST CAVEAT: production wires no
   liveness source, so every replica is still a primary owner and PR
   aliases R there -- but the counting logic is real and unit-tested
   against an oracle that DOES supply fallback/durable acks, and a DST
   model with a negative control gates it. Enforcement is no longer
   faked; it is exercised end-to-end wherever the substrate supplies
   the signals.**

9. AAE exchange/repair wired into the running binary. L. **PUSH DONE
   (`49bac1b`): `dynomited::spawn_aae` now takes the local
   `NoxuDatastore` and, on each sweep tick, walks a bounded slice
   (<=256/tick) of the local primary keyspace and pushes each object's
   canonical `SiblingSet` storage to the tick's peer as a
   `PeerOp::RepairPut` -- the op read-repair and write-fan already use,
   applied verbatim + merged idempotently by the receiver. This is
   push-only anti-entropy: it propagates state a peer missed (dropped
   fan, lagging replica) in the background, convergent and safe (never
   a lost/double-counted write because the merge is adopt-if-newer).
   Object read-repair on the read path was already wired. REMAINING:
   the full segmented-tree PULL exchange (ROOT/TREE/KEY-SYNC minimal
   diff) still needs a bidirectional exchange-frame peer plane; push
   ships more bytes than a tree diff would. Gated by a DST PushAll
   model + the Elle consistency check (AGENTS.md 6.5).**

11. Storage key drops `bucket_type` (Riak-parity gap; DISCOVERED, NOT
    FIXED). M. **The `Datastore::riak_get`/`riak_put`/`riak_delete`
    trait methods (`crates/dynomite/src/embed/hooks.rs`) key storage by
    `(bucket, key)` only; `bucket_type` participates in routing and
    props resolution (`try_route`, `registry().resolve`) but NOT in the
    storage identity. Every call site in `server.rs` passes only
    `&req.bucket` + `key` (e.g. `riak_put(&req.bucket, key, ...)` in
    `resolve_and_store_put`). Real Riak's object/CRDT identity is
    `(bucket_type, bucket, key)`, so two DIFFERENT CRDT types under the
    same `(bucket, key)` but distinct `bucket_type` (the shape
    `examples/riak-crdts.toml` uses: counter/set/map all under
    `bucket="crdts"`) collide on one noxu slot -- the second write's
    type tag mismatches the stored blob and
    `CrdtSerialError::TagMismatch` (`datatypes/serial.rs`) fires. This
    is a genuine defect, not a benchmark artifact: it was surfaced
    while running the head-to-head bench (which worked around it by
    giving each CRDT type its own bucket, a fair real-client
    workaround) and confirmed against the code. FIX: fold `bucket_type`
    into the storage key in the datastore layer (a `default` sentinel
    when unset for backward compatibility), and thread it through the
    `riak_get/put/delete` trait signatures + every call site. Deferred
    (touches the public `Datastore` trait -- a SemVer surface change --
    and every stored-object migration path); tracked here so it is not
    lost.**

12. Map CRDT wire schema diverges from upstream `riak_dt.proto`
    (Riak-parity gap; DISCOVERED, NOT FIXED). M. **Surfaced while
    validating the bench workloads against real Riak 2.2.3 and
    confirmed against `crates/dyniak/src/proto/pb/datatypes.rs`. Two
    divergences: (1) dyniak's `MapOp` is `updates=tag1 / removes=tag2`,
    but upstream is `removes=tag1 / updates=tag2` (swapped); (2)
    dyniak's `MapUpdate` is `{ field=tag1, op=ScalarOp@tag2 }`, wrapping
    the per-type op in a `ScalarOp` message that does NOT exist in the
    published protocol -- upstream `MapUpdate` is flat: `field=1,
    counter_op=2, set_op=3, register_op=4 (raw bytes), flag_op=5,
    map_op=6`. Consequence: dyniak's Map / nested-map path is
    self-consistent but NOT wire-compatible with any stock Riak PBC
    client. The Counter/Set/HyperLogLog ops and the `DtOp` envelope
    (`counter_op=1, set_op=2, map_op=3, hll_op=4, gset_op=5`),
    `CounterOp.increment=1`, and `SetOp.adds=1/removes=2` DO match
    upstream, so those types interoperate. FIX: renumber `MapOp` fields
    and flatten `MapUpdate` to the upstream layout (drop the `ScalarOp`
    wrapper); a wire-format change needing an on-wire/stored-object
    migration story. Deferred, tracked. The README over-claim ("all six
    served over the wire" implying Riak-client compat for Map) was
    corrected in the same commit.**

13. QUIC listener forces TLS onto the plain TCP PBC port (design sharp
    edge; DISCOVERED, NOT FIXED). S. **QUIC mandates TLS, so
    `quic_listen` requires `tls_cert`/`tls_key`. But that same cert
    pair drives the shared `RiakHandles.tls` acceptor, which
    `serve_pbc_full` applies to EVERY accepted TCP socket -- so a
    config with both `quic_listen` and `pbc_listen` serves TLS on the
    "plain" TCP port too, and a plain-TCP PBC client gets a TLS
    handshake instead of a PBC response (observed during the
    2026-09-13 bench: a raw `ping` came back starting `15 03 03`, a TLS
    alert). Defensible (setting certs asks for TLS) but the
    QUIC-forces-TLS-everywhere coupling is a trap. FIX: a separate
    `quic_tls_*` cert knob, or let the TCP listener stay plain when
    only QUIC needs the cert. Deferred (config-surface change). The
    bench worked around it by running two dyniak configs.**

14. QUIC bench driver flaky at low worker counts (bench-tooling bug;
    DISCOVERED, NOT FIXED). S. **`dyniak-bench`'s `riak_quic.rs`
    multi-worker path served a clean c=32 run (1.36M ops, 0 errors) but
    a c=4 run failed immediately with "quic driver shut down" / "early
    eof". A robustness bug in the bench QUIC driver's shared-runtime /
    connection handling, NOT a dyniak server defect (the server served
    the c=32 run cleanly). Flagged for the bench tooling.**

Benchmark credibility:
8. Populate criterion baselines; activate the regression gate. S.
   **DONE (`acd0f3b`): all seven micro benches captured (161 criterion
   cases), manifests populated. Caveat: captured on a loaded host, so a
   clean re-baseline on a quiescent runner is advised before trusting
   tight (10%) regressions.**
9. Head-to-head Dyniak-vs-Riak single-node bench on EC2. M. **DONE
   (`d4a027c`): two-node A/B on EC2 (lava). Surfaced a REAL latency bug
   (not a storage ceiling): PBC `write_frame` did three separate
   `write_all`s and the accept loop never set `TCP_NODELAY`, giving a
   ~50ms Nagle/delayed-ACK floor per op -- now fixed. The pre-fix
   report states plainly that "similar or better than Riak" is NOT yet
   substantiated; re-run after the fix for a true comparison. Also
   fixed a wire bug in the bench's own CRDT driver (DtOp at field 5,
   not 4). Script: `scripts/ec2-dist/dyniak-vs-riak-bench.sh`; report
   under `dist/bench-reports/`.**
10. Scale-out linearity + distributed-quorum tail latency. M. **Not
    done. The re-run (item 9) showed the single-node TCP throughput is
    bounded by the BLOCKING bench TCP driver (~2,800 ops/s flat from
    c=4 to c=32; server serves 47K serial ops/s on one connection), so
    a meaningful scale-out / tail-latency study first needs a
    non-blocking TCP load driver in `dyniak-bench`. Tracked.**

Re-run result (2026-09-13, `dist/bench-reports/dyniak-tcp-vs-quic-2026-09-13.md`):
the NODELAY fix removed the 50ms floor (mixed 610 -> 2,798 ops/s, p50
50 -> 11ms; a raw serial ping measured ~21us/op server-side). QUIC
reached 15,056 ops/s p50 2ms (beats Riak's TCP throughput), though
part of that ratio is the async-vs-blocking bench driver, not just the
transport. "Similar or better than Riak" is now defensible at the
SERVER level; a clean client-side apples-to-apples still needs a
non-blocking TCP driver. Two new defects (items 13, 14) surfaced.

None of these are code-blocking for the CRDT convergence work already
shipped; they are the roadmap to "nearly identical to Riak" on function
and "demonstrably similar-or-better" on performance.
