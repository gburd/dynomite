# Riak vs Dynomite: feature comparison and adoption recommendations

Survey of `basho/riak` (the meta-repo, plus `riak_kv`, `riak_core`,
`riak_repl`, `riak_auth_mods` deps) against this Rust
implementation. Conducted while the pass-2 multi-host chaos run
was in flight (run id `pass2-20260522-032705Z`).

## TL;DR

Riak is a **complete distributed database**; Dynomite is a
**proxy in front of an existing single-node store** (redis or
memcache). They share the Dynamo lineage (consistent hashing,
quorum reads/writes, gossip, vnodes) but operate at different
layers of the stack. **About a third of Riak's architectural
inventory has no equivalent in Dynomite by design**  --  anything
that touches the storage engine itself (CRDTs, MapReduce,
secondary indexes, search, multi-backend) is the responsibility
of the underlying redis/memcache.

The interesting overlap is in **cluster / consistency / repair
machinery** at the proxy layer: hinted handoff, read repair,
vector clocks, bucket-type configs, real-time replication
queues. Of those, three look like genuine wins for Dynomite to
adopt; the rest are out of scope or already approximated.

## Architectural categorisation

### Category A: Storage-engine features (out of scope - delegated to redis)

| Riak feature | Why N/A |
|---|---|
| **Pluggable backends** (Bitcask / LevelDB / Leveled / Memory / Multi-backend / Yes-sir) | Dynomite delegates storage to the configured backend. Operators pick their backend by choosing redis vs memcache, not via Dynomite config. |
| **CRDTs** (gcounter, pncounter, hll, set, map, register, riak_dt) | Redis already has its own counter / set / hash / sorted-set primitives. Operators get conflict-free counter / set semantics by hashing to a single replica via DC_ONE; CRDT-style merge happens at the redis level. |
| **MapReduce** (riak_kv_mapreduce, mrc_pipe, w_reduce) | Compute jobs against the underlying store live in the application layer, not the proxy. Redis has `LUA EVAL` for one-shot, no-data-locality compute; clients do parallel `MGET` for the rest. |
| **Secondary indexes (2i)** (riak_kv_index_fsm, pb_index) | Redis has its own indexing primitives (sorted sets, hashes, scripted indexes). Dynomite passes them through. |
| **MapReduce pipes** (riak_kv_mrc_*) | Same as MapReduce. |
| **Search/Solr integration** (yokozuna, separate repo) | Out of scope; redis modules like RediSearch live below us. |
| **Bitcask / LevelDB / Leveled tuning** (~80 schema knobs) | All backend-internal. |

### Category B: Cluster/topology features (Dynomite already covers)

| Riak feature | Dynomite equivalent |
|---|---|
| **Vnode-based ring** (riak_core_vnode, ring management) | `crates/dynomite/src/cluster/vnode.rs` + `DynToken`. We use 32-bit tokens; Riak uses 160-bit. |
| **Gossip** (riak_core_gossip) | `crates/dynomite/src/cluster/gossip.rs` (library code; binary task is "deferred"). |
| **Hash partitioning** (Dynamo-style) | `crates/dynomite/src/hashkit/` with 5 hash functions; Murmur3 default. |
| **Coverage queries** (read-all-vnodes streaming) | Approximated by client-side `MGET` and redis `SCAN` against each peer. |
| **Sloppy quorum** (use the next available node when primary down) | `auto_eject` makes a similar trade: skip ejected peers. The Riak version is more nuanced (write to a different vnode and "hint" the original to handoff later). See Category D #1. |
| **Read repair on quorum read** | Approximated by `crate::entropy` anti-entropy; on-the-fly read repair is Category D #2. |
| **Coordinator forwarding** (PUT to local-DC primary) | `cluster::dispatch::ClusterDispatcher` does the equivalent: hash, look up replica, dispatch. |
| **Auto-eject hosts** | `crates/dynomite/src/net/auto_eject.rs`. |
| **Phi-accrual failure detector** | Just landed at `crates/dynomite/src/cluster/failure_detector.rs` (commit `65ccb12`). Same algorithm as Riak's. |

### Category C: Operations / observability (Dynomite has lighter equivalents)

| Riak feature | Dynomite equivalent |
|---|---|
| **Stats / Exometer** | `crates/dynomite/src/stats/` HTTP /stats. Less granular than Riak's per-vnode metrics. |
| **TLS / authentication** (riak_auth_mods, peer SSL) | Stage-3 TLS via QUIC for peer plane; client AUTH (`redis_requirepass`, just landed at `71046c0`); TCP+TLS for peer plane is queued (`docs/post-chaos-queue.md` item 7's deferred sibling). |
| **Cluster-state console / `riak admin`** | `dyn-hash-tool` for offline; runtime introspection via `/stats`. Less feature-rich. |
| **Hot config reload (cuttlefish)** | `crate::core::log::reopen_on_sighup` for log reopen. Full conf reload not wired. |
| **Logging / log rotation** | `tracing` + standard SIGHUP logrotate pattern. |

### Category D: Riak features that COULD genuinely improve Dynomite

This is the actionable list. Each item is sized in the
recommendation table at the bottom.

#### D1. Hinted handoff (high value, medium effort)

**Riak**: when a write targets a vnode whose owner is down, the
write is sent to the next available vnode along the ring AND a
"hint" is recorded so when the original owner comes back, the
hinted vnode replays the writes to it. This is what makes
sloppy-quorum actually durable: a write succeeds even when
quorum's intended owners are down, because a fallback node
captured the data and will hand it off.

**Dynomite today**: when the dispatcher computes
`DispatchPlan::Replicas` and a target peer's channel is full or
the peer is auto-ejected, that target is silently skipped. If
zero targets accept, the dispatcher returns
`DynomiteNoQuorumAchieved` to the client. Lost write availability
during peer outages is the cost.

**Why it matters**: Pass-1 of the chaos test demonstrated a
9-minute period where one DC was down. With per-DC distinct
tokens (pass-2 in flight), keys hashing to that DC's range
return errors during the outage. With hinted handoff the writes
would land on a fallback peer in another DC and replay to the
original peer when it recovers. **Same operational model
operators already understand from Cassandra/Riak.**

**Implementation sketch**:
- Per-peer "hint queue" persisted on disk (similar to
  `crate::core::buffer::CBuffer` but file-backed).
- When a request can't reach its target peer, route to the
  nearest available peer AND enqueue the request bytes + target
  peer id on the destination peer's hint queue file.
- A periodic task replays the hint queue to the original target
  once its `PeerState` returns to `Normal` (phi-accrual back
  below threshold).

**Estimated effort**: 3-5 days. Significant; queues need to be
durable across crashes, ordering matters for some commands, and
testing requires multi-host chaos with controlled outages.

#### D2. Read repair (medium value, low effort)

**Riak**: when a read returns conflicting values from N replicas
(different vector clocks), the FSM picks the latest and writes
it back to the stale replicas. This cleans up divergence on the
read path without waiting for AAE.

**Dynomite today**: the `DC_QUORUM` and `DC_EACH_SAFE_QUORUM`
read paths fan out and accept the first reply (in the planned
design  --  currently the reply coalescer isn't wired so all reads
are effectively `DC_ONE`). No comparison of replies, no
write-back of stale values.

**Why it matters**: smaller divergence windows, less work for
the scheduled entropy job, less reliance on per-query TTL.

**Implementation sketch**: in the response coalescer (Phase 3
work, queued under `docs/chaos-pass2-design.md`), if the first
quorum reply differs from later replies, re-issue the write
with the chosen value to the stale targets. Only applies to
reads; writes already replicate.

**Estimated effort**: ~1 day on top of the coalescer (which is
~1 week of work itself). Would land naturally as part of the
quorum/coalescer milestone.

#### D3. Bucket-type configuration (medium value, medium effort)

**Riak**: a "bucket type" is a named config bundle that applies
to keys with a given prefix: `n_val`, `r`, `w`, `allow_mult`,
TTL defaults, hooks, etc. Operators can have hot keys at higher
durability and cold keys at lower cost without changing client
code.

**Dynomite today**: pool-level config only. Every key in a pool
gets the same `read_consistency`, `write_consistency`,
`auto_eject_hosts`, etc. Per-key-class differentiation requires
running multiple pools and routing client traffic to the right
pool externally.

**Why it matters**: real production deployments have key
classes with very different SLAs. A cache-style key can use
`DC_ONE` everything; a transactional key needs
`DC_EACH_SAFE_QUORUM`. Per-bucket config makes both cohabit
cleanly.

**Implementation sketch**:
- Add `bucket_types: HashMap<Prefix, BucketTypeConfig>` to
  `ConfPool`.
- Dispatcher walks the key, finds the longest-matching prefix,
  applies that type's overrides over the pool defaults.
- Operator surface: YAML stanza. No protocol change.

**Estimated effort**: 2-3 days. Cleanly factored; tests would
parameterise the existing dispatch test matrix over bucket
types.

#### D4. Real-time replication queue (low value, high effort)  --  **defer**

**Riak**: `riak_repl` ships writes to a remote cluster
asynchronously via a persistent queue. This is for
**inter-cluster** replication (DR / geo-redundancy), distinct
from the gossip-based same-cluster replication. The default
mechanism in 3.x is `replrtq` with `tictacaae` for catching
missed updates.

**Dynomite today**: `DC_EACH_SAFE_QUORUM` writes synchronously
fan out to every DC. Strong consistency-flavored cross-DC, but
high tail latency and capacity sensitivity.

**Why it doesn't make the cut**: Dynomite's design assumption
is that all DCs are part of one cluster; an async cross-cluster
queue is a different shape of system. Operators who need it can
run two Dynomite clusters and use external tooling
(redis-shake, mirror-maker-style) to bridge them. Adding
in-tree async cluster replication is a new product, not a
feature.

#### D5. Strong consistency mode (riak_ensemble)  --  **defer**

**Riak**: optional Paxos-based strongly consistent mode for
specific buckets. Available since 2.x; rarely used in
production due to the operational complexity.

**Dynomite today**: eventually consistent only.

**Why it doesn't make the cut**: a Paxos implementation is a
multi-month project, the use cases that need it usually want a
real consensus database (etcd, ZooKeeper, FoundationDB)
instead, and Riak's own production guidance has been to skip
this for most workloads. Out of scope.

#### D6. Vector clocks at the proxy (low value, high invasiveness)  --  **defer**

**Riak**: every object carries a vector clock; replicas can
detect concurrent writes and either return siblings (when
`allow_mult=true`) or auto-resolve via configured strategy.

**Dynomite today**: redis `SET` is timestamped server-side;
last-write-wins is the only model. Two concurrent writes with
the same key can both succeed and one silently overwrites the
other.

**Why it doesn't make the cut**: vector clocks at the proxy
require either (a) a metadata sidecar protocol layered on top
of redis (every value becomes `<vclock_blob><user_value>`,
costly and breaks redis-cli compatibility), or (b) using a
redis module that natively versions values (RedisJSON-style).
Either is a major architectural commitment that fights
Dynomite's "transparent proxy" promise.

If a deployment really needs causal consistency, operators can
use redis modules with built-in versioning behind Dynomite and
configure conflict resolution there. Worth documenting; not
worth implementing in Dynomite proper.

## Recommendation table

| Item | Adopt? | Default or opt-in | Effort | When |
|---|---|---|---|---|
| **D1. Hinted handoff** | **yes** | configurable (`hinted_handoff: on`), default OFF until tested | 3-5 days | After pass-3 (post-coalescer) |
| **D2. Read repair on quorum read** | **yes** | default ON when `read_consistency` >= DC_QUORUM (lands "free" with the coalescer) | ~1 day on top of coalescer | With the coalescer |
| **D3. Bucket-type config** | **yes** | opt-in (define types in YAML; default pool config still applies) | 2-3 days | Independent, after pass-3 |
| **D4. Async cross-cluster replication** | no |  --  | weeks | Out of scope |
| **D5. Strong consistency mode** | no |  --  | months | Out of scope |
| **D6. Vector clocks at proxy** | no |  --  | weeks + protocol break | Out of scope |
| Phi-accrual (already shipped) | yes | default on, threshold 8 | 0 (done) | landed in `65ccb12` |
| Already-covered Riak features (Category B) | n/a | already present | n/a | n/a |
| Storage-engine features (Category A) | no | delegated to redis | n/a | by design |

## Notes on implementation order

If we land all three "yes" items, the natural sequence is:

1. **Coalescer + read repair** (D2)  --  the coalescer is the biggest blocker for `DC_QUORUM`/`DC_EACH_SAFE_QUORUM` readiness anyway; read repair adds <10% to that work.
2. **Bucket types** (D3)  --  independent, can land in parallel with the coalescer.
3. **Hinted handoff** (D1)  --  needs the coalescer's reply-tracking machinery to know which targets succeeded vs failed, so it sequences after D2.

After all three, Dynomite would have feature parity with Riak's Dynamo lineage minus the storage-engine stack we deliberately don't own.

## Out-of-scope but worth documenting in `docs/`

- "Why Dynomite doesn't have CRDTs" - delegated to redis primitives, with a pointer at how operators get conflict-free counter / set semantics.
- "Why Dynomite doesn't have MapReduce" - parallel client `MGET` plus redis `LUA EVAL` is the architectural answer.
- "Choosing between Dynomite and Riak" - decision matrix: Dynomite when you already run redis at scale; Riak when you want one product to handle storage + replication + consistency + observability.
