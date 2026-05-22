# Riak-compatible API + Noxu storage backend: design plan

This document is the standing design for grafting the Riak K/V client
API and the Noxu DB storage engine onto Dynomite without disturbing
the existing Redis or Memcached transparent-proxy paths. Implementation
is sequenced behind it; the only code change carried in this branch
is the workspace-level Cargo plumbing (Section 6 below).

The plan is a *multi-month* program of work. It is written so the
follow-on stages can be parallelised across worker agents without each
agent having to rederive the architecture.

---

## 1. Architecture overview

### Vision

Today, Dynomite is a transparent shard-and-replicate proxy in front of
a single-node store (`redis-server` or `memcached`). The Dynamo
substrate (consistent hashing, vnodes, gossip, quorum, auto-eject,
phi-accrual failure detection, anti-entropy, hinted handoff,
read-repair, bucket types) lives inside `crate::dynomite`; the actual
storage lives in a separate process behind `backend_supervisor`.

The user's vision is to generalise this into:

```text
+-----------------------------------------------------------+
|  Client wire protocols (pluggable)                        |
|                                                           |
|   Redis RESP   Memcached ASCII   Riak HTTP   Riak PBC     |
|     (today)       (today)         (new)        (new)      |
+-----------------------------------------------------------+
|  proto::* parsers + per-protocol fragment/coalesce        |
+-----------------------------------------------------------+
|  Dynamo substrate (existing - reused untouched)           |
|                                                           |
|   net::client | cluster::dispatch | cluster::peer         |
|   cluster::gossip | cluster::failure_detector             |
|   net::auto_eject | crypto | proto::dnode | hashkit       |
|   conf | stats | embed::Server                            |
+-----------------------------------------------------------+
|  Per-node storage backends (pluggable Datastore impls)    |
|                                                           |
|   RedisDatastore    MemcacheDatastore    NoxuDatastore    |
|     (today)            (today)              (new)         |
+-----------------------------------------------------------+
|  Storage engines                                          |
|                                                           |
|   external `redis-server` / `memcached`  |  in-process    |
|   over TCP (existing)                    |  Noxu DB       |
|                                          |  (new, ACID,   |
|                                          |  log-structured|
|                                          |  B+tree)       |
+-----------------------------------------------------------+
```

The substrate (the middle two layers) is unchanged. The public
contract that lets us add layers above and below it is already in
place at `crates/dynomite/src/embed/hooks.rs`:

* `trait Datastore` (Send + Sync, dyn-compatible) is the storage
  seam. Today it has three impls: `MemoryDatastore`, `RedisDatastore`,
  `MemcacheDatastore`. We add a fourth: `NoxuDatastore`.
* `proto::redis` and `proto::memcache` are sibling modules under
  `proto::*`; we add `proto::riak::http` and `proto::riak::pb`.
* `net::client` reads bytes off the wire and dispatches them through
  the parser of the configured protocol; the per-listener `Protocol`
  enum already covers Redis, Memcache and `Custom` and is the place
  the Riak listener slots in.

### What is reused versus what is new

| Concern | Reused | New |
|---|---|---|
| Listener accept loop | `net::proxy`, `net::dnode_proxy` | one new `RiakHttpListener`, one new `RiakPbListener` |
| Per-conn FSM | `net::client` (parameterised on protocol) | new `RiakHttpHandler`, `RiakPbHandler` driving an `httparse`/protobuf parser |
| Wire codec | `proto::dnode`, `proto::redis`, `proto::memcache` | `proto::riak::http`, `proto::riak::pb` |
| Hashing + ring | `hashkit::*`, `cluster::vnode` | the Riak HTTP API exposes 160-bit token formatting; we add a 160-bit `RingToken` newtype that wraps the existing `DynToken` for display only |
| Routing + quorum | `cluster::dispatch` | per-bucket-type N/R/W parameters override the pool defaults via the existing `RoutingOverride` |
| Gossip + failure detection | `cluster::gossip`, `cluster::failure_detector` | unchanged |
| Anti-entropy | `crate::entropy` | extended with Tictac AAE Merkle-tree mode |
| Hinted handoff | post-chaos queue D1 (planned) | unchanged plan, now with disk-backed queue files served from Noxu |
| Read repair | post-chaos queue D2 (planned) | unchanged plan |
| Bucket types | post-chaos queue D3 (planned) | extended to carry the Riak superset of knobs |
| Storage | external redis/memcache process | in-process Noxu DB Environment with one Database per vnode |
| Object encoding | redis transparent | new: `RiakObject` carrying a vector clock, sibling list, content-type, last-modified, user metadata, and indexes |
| Admin tooling | `dyn-hash-tool` | new `dyn-admin` binary providing the riak-admin equivalent commands |

### Backwards compatibility

The Redis and Memcache paths remain the default. The Riak protocol
listeners and the Noxu datastore are off unless explicitly configured
in the YAML and built with the `riak-storage` Cargo feature (declared
in this branch, body deferred to M1+). Existing tests and existing
deployments are unaffected.

The substrate makes one pre-existing assumption that we honour: the
Datastore trait's `dispatch` method takes a `Msg` and returns a `Msg`.
For the Riak path, the inbound HTTP/PB request is parsed into a
synthesised `Msg` whose `MsgType` lives in a new namespace (see
Section 2) and whose payload carries a typed `RiakOp`. The Noxu
datastore reads that payload and produces a response `Msg`. The
dispatcher does not need to know it is talking to a Riak frontend;
all routing, quorum, repair, and AAE machinery sees only `Msg`.


---

## 2. Protocol layer (Riak API surface)

Riak exposes two parallel client APIs: a JSON/REST API over HTTP and a
Protocol Buffers API over a length-prefixed binary protocol on TCP.
Both surfaces talk to the same `riak_kv_*_fsm` request handlers
inside `riak_kv`. We need not implement them simultaneously; PB is the
production path for high-throughput clients, HTTP is the path for
operators, scripts, and ad-hoc curl. We sequence PB first, HTTP a
half-stage behind, with the parsers sharing a typed `RiakOp` enum.

### 2.1 Endpoint catalogue (Riak KV 2.2.3)

The table groups Riak's user-facing operations into the categories from
the comparison doc and notes which are in-scope for v0.

| Category | Riak HTTP route | Riak PB message | Scope | Plumbing |
|---|---|---|---|---|
| Object | `GET /buckets/B/keys/K` | `RpbGetReq` (9) / `RpbGetResp` (10) | **v0 (M2)** | new `RiakOp::Get`, dispatched as `MsgType::ReqRiakGet`, served by `NoxuDatastore` |
| Object | `PUT /buckets/B/keys/K` | `RpbPutReq` (11) / `RpbPutResp` (12) | **v0 (M2)** | new `RiakOp::Put` carrying value + content-type + indexes + vclock |
| Object | `POST /buckets/B/keys` | (server-assigned key) | **v0 (M2)** | same as `Put`, with key allocator |
| Object | `DELETE /buckets/B/keys/K` | `RpbDelReq` (13) / `RpbDelResp` (14) | **v0 (M2)** | tombstones with `delete_mode` honoured by `NoxuDatastore` |
| Object | `HEAD /buckets/B/keys/K` | (server emits headers only, body omitted) | **v0 (M2)** | `RiakOp::Get { head_only: true }` |
| Bucket | `GET /buckets?buckets=true` (stream) | `RpbListBucketsReq` (15) / `RpbListBucketsResp` (16) | **v1 (M5)** | streaming response, requires coverage plan |
| Bucket | `GET /buckets/B/keys?keys=true` | `RpbListKeysReq` (17) / `RpbListKeysResp` (18) | **v1 (M5)** | as above |
| Bucket | `GET /buckets/B/props` | `RpbGetBucketReq` (19) / `RpbGetBucketResp` (20) | **v0 (M2)** | reads from new `BucketRegistry` |
| Bucket | `PUT /buckets/B/props` | `RpbSetBucketReq` (21) / `RpbSetBucketResp` (22) | **v0 (M2)** | writes into `BucketRegistry`, gossiped to peers |
| Bucket | `RESET` props | `RpbResetBucketReq` (29) | **v1 (M5)** | trivially derived from the SetBucketReq path |
| Bucket type | `GET /types/T/props` | `RpbGetBucketTypeReq` (31) | **v0 (M2)** | post-chaos queue D3 wired through |
| Bucket type | `PUT /types/T/props` | `RpbSetBucketTypeReq` (32) | **v0 (M2)** | same |
| 2i | `GET /buckets/B/index/I/V` | `RpbIndexReq` (25) / `RpbIndexResp` (26) | **v1 (M5)** | exact and range queries, term return, `$bucket`/`$key` meta-queries |
| 2i | `GET /buckets/B/index/I/V1/V2` | RpbIndexReq with `range_min`/`range_max` | **v1 (M5)** | range with continuation pagination |
| MapReduce | `POST /mapred` | `RpbMapRedReq` (23) / `RpbMapRedResp` (24) | **v2 (M8)** | new `pipe::*` module; biggest single port |
| Coverage | `GET /coverage` | `RpbCoverageReq` (70) / `RpbCoverageResp` (71) | **v1 (M5)** | exposes the existing ring as a coverage plan |
| Server info | `GET /stats` | `RpbGetServerInfoReq` (7) / `RpbGetServerInfoResp` (8) | **v0 (M3)** | trivial wrapper around existing `stats::Snapshot` |
| Server info | `GET /` (ping) | `RpbPingReq` (1) / `RpbPingResp` (2) | **v0 (M3)** | trivial |
| Auth | `GET /auth` (basic) | `RpbAuthReq` (253) / `RpbAuthResp` (254) | **v1 (M6)** | passes through to a configured `AuthProvider` trait |
| TLS | `STARTTLS` | `RpbStartTls` (255) | **v1 (M6)** | upgrades the socket via `tokio-rustls` |
| CRDT | `GET /types/T/buckets/B/datatypes/K` | `DtFetchReq` (80) / `DtFetchResp` (81) | **v2 (M7)** | counter, set, map, register, hll |
| CRDT | `POST /types/T/buckets/B/datatypes/K` | `DtUpdateReq` (82) / `DtUpdateResp` (83) | **v2 (M7)** | per-type op encoders |
| Search (Yokozuna) | `GET /search/query/idx` | `RpbSearchQueryReq` (27) / `RpbSearchQueryResp` (28) | **OUT** | per user instruction |
| Riak TS | `Ts*Req` (90-101) | various | **OUT** | per user instruction |
| Riak CS | n/a | n/a | **OUT** | per user instruction |
| Link walking | `GET /buckets/B/keys/K/.../...` | n/a | **v2 (M8)** | layered on MapReduce pipes |

### 2.2 Wire-format work

**Protocol Buffers**: Riak ships its `.proto` files at
`riak_pb/src/riak_kv.proto`, `riak.proto`, and `riak_dt.proto`. We
pick a stable subset (the messages flagged v0/v1/v2 above) and run
`prost-build` from `build.rs` in a new `crates/riak-pb/` crate. We do
*not* consume the Erlang-side schema files at runtime; the codegen
output is committed and reviewed. PB framing is `<u32 length> <u8
msg_code> <protobuf body>`; `httparse` is unsuitable, so the listener
has its own bounded-length framer in `proto::riak::pb::framer`.

**HTTP**: we hand-roll a tiny HTTP/1.1 parser using `httparse`
(already in `[workspace.dependencies]`) with a minimum surface:
method, path, headers (case-insensitive lookup), body. No HTTP/2 in
v0; HTTP/3 is naturally available later via the existing QUIC
transport but not in scope for the first release. Routing is a
match-statement on `(method, path-prefix)` that emits `RiakOp::*`; the
body is parsed lazily by the op handler (some ops want JSON, some want
raw bytes, some want multipart for siblings).

**JSON encoding** of object metadata follows Riak's documented schema:
`X-Riak-Vclock`, `X-Riak-Meta-*`, `X-Riak-Index-*-bin`,
`X-Riak-Index-*-int`, `Last-Modified`, `Content-Type`. We use `serde`
+ `serde_json` (already a dev-dep; promoted to a runtime dep behind
the `riak-storage` feature gate in M1).

### 2.3 Mapping into existing plumbing

* **`MsgType` extensions**: Section 7 of `dyn_message.h` declares
  `MSG_TYPE_CODEC` as the discriminator. We extend it with
  `ReqRiakGet`, `ReqRiakPut`, `ReqRiakDel`, `ReqRiakHead`,
  `ReqRiakIndex`, `ReqRiakListKeys`, `ReqRiakListBuckets`,
  `ReqRiakBucketProps`, `ReqRiakSetBucketProps`,
  `ReqRiakDtFetch`, `ReqRiakDtUpdate`, `ReqRiakMapRed`,
  `ReqRiakCoverage`, `ReqRiakServerInfo`, `ReqRiakAuth`, plus their
  `Rsp*` siblings. Existing parity tests (`tests/parity_msg_codec.rs`
  if present, otherwise the discriminated-union exhaustive match) get
  updated.
* **`net::client`**: takes a new `Protocol::Riak { transport: Http |
  Pb }` variant (today the embed `Protocol` enum has `Redis`,
  `Memcache`, `Custom`). The handler trait `ConnHandler` already
  decouples wire parsing from dispatch.
* **`cluster::dispatch::ClusterDispatcher::plan`**: works key-first,
  protocol-agnostic. Riak's "bucket+key" identifier becomes a single
  hashed key under the existing ring math: `hash_input = bucket || 0
  || key` (this is how Riak's `riak_core_util:chash_key` builds its
  hash input; we reproduce it bit-for-bit so tokens agree if a
  cluster is migrated). The hash function is **murmur3 32-bit** by
  default (Dynomite's default; Riak uses SHA-1 over a 160-bit ring -
  see Section 8 open question).
* **Quorum N/R/W**: we add `n_val`, `r`, `w`, `pr`, `pw`, `dw`,
  `notfound_ok`, `basic_quorum` to `BucketTypeConfig` (post-chaos
  queue D3); the dispatcher consults the bucket-type config before
  the pool defaults to compute the replica count and the success
  threshold.
* **Vector clocks**: never inspected by the substrate. Carried opaque
  through `Msg::payload` so the dispatcher can ship them across
  peers. The Noxu datastore is responsible for `vclock_inc`,
  sibling materialisation, and `allow_mult` resolution (see
  Section 3).


---

## 3. Storage layer (Noxu integration)

### 3.1 Noxu surface we use

From `crates/noxu-db/src/lib.rs` (`/home/gburd/ws/lamdb`):

* `Environment::open(EnvironmentConfig)` - the per-data-directory ACID
  transactional environment.
* `Environment::open_database(name, DatabaseConfig, txn?)` - opens or
  creates a named B+tree.
* `Environment::begin_transaction(...)` - `Transaction` with explicit
  `commit()`, `commit_with_durability(Durability)`, `abort()`.
* `Database::{get, get_with_options, put, put_with_options,
  put_no_overwrite, delete}` - point ops, transactional or
  auto-commit.
* `Database::open_cursor(...)` - returns a `Cursor` with rich seek
  semantics (`Get::{Search, SearchGte, SearchRange, First, Last, Next,
  Prev, Current, ...}`).
* `Database::open_sequence(...)` - durable monotonic counter (used for
  the per-vnode object version field).
* `SecondaryDatabase::open(primary, secondary_db, SecondaryConfig)` -
  index that maintains its own B+tree and rebuilds on demand. The
  `SecondaryKeyCreator` and `SecondaryMultiKeyCreator` traits are
  exactly the shape Riak 2i needs: given primary key + value, emit
  zero, one, or many index keys.
* `DatabaseEntry` - byte-bag I/O struct, comfortable with `Bytes` /
  `Vec<u8>` / partial reads.
* `Durability::{SyncPolicy, ReplicaAckPolicy}` - tunable per
  transaction. Riak's `dw` ("durable write") concept maps cleanly.

### 3.2 Mapping Riak's data model onto Noxu

**One Environment per node**, rooted at the configured data
directory (default `/var/lib/dynomite`). Multiple environments are
not needed; Noxu's transactions span databases.

**One Database per (vnode, kind)** pair. The vnode set is determined
by the ring topology from `cluster::vnode`; for a 64-vnode default we
get 64 primary databases per node. Database names follow a stable
scheme so a restarting process re-opens the right ones:

| Database name | Stores | Format |
|---|---|---|
| `vnode_<id>_objects` | primary K/V data | key = `bucket_type \| 0xFF \| bucket \| 0xFF \| key`; value = bincode-encoded `RiakObject` |
| `vnode_<id>_aae` | Tictac AAE tree segment hashes | key = `(level, segment_id)`; value = hash digest |
| `vnode_<id>_handoff` | hinted-handoff queue | key = monotonic `u64` from a Sequence; value = serialized request |
| `vnode_<id>_idx_<index_name>` | 2i secondary index, one per declared index | maintained via `SecondaryDatabase` against the objects DB |
| `cluster_state` | gossip state, ring snapshot, bucket types | small, mostly-static; one DB per node |
| `bucket_types` | bucket-type registry | name -> `BucketTypeConfig` |

The `0xFF` separator is a byte that cannot occur inside a UTF-8 key.
Riak permits binary keys; we use the same trick Riak does internally
(length-prefixed components). Switching to length-prefixing is purely
mechanical and is captured as an open question in Section 8 (we want
to default to length-prefixing to avoid the 0xFF caveat, but it
breaks human-friendly debugging via `cursor.scan_all_kv()`).

**`RiakObject` blob format** (bincode, version-tagged):

```text
struct RiakObject {
    version: u8,                    // schema version, 1 today
    vclock: VClock,                 // dotted version vector
    siblings: Vec<Sibling>,         // 1..N entries; len > 1 only when allow_mult
    deleted: bool,                  // tombstone marker
    last_modified: u64,             // micros since epoch
}
struct Sibling {
    content_type: SmallString,
    user_meta: Vec<(SmallString, SmallVec<u8>)>,
    indexes: Vec<IndexEntry>,       // 2i tag + value
    value: Bytes,
}
enum IndexEntry {
    Bin { name: SmallString, value: Bytes },
    Int { name: SmallString, value: i64 },
}
struct VClock {
    entries: Vec<(ActorId, u64, u64)>,  // (actor, counter, timestamp)
}
```

bincode is chosen over protobuf for the on-disk format because (a) the
Riak protobuf schema is over-the-wire only, (b) bincode is faster to
encode/decode in Rust, and (c) we control the schema version
explicitly. The Riak PB schema is consumed by the protocol layer;
bincode is consumed by Noxu's value blobs.

### 3.3 2i, the Noxu way

Each declared index becomes one `SecondaryDatabase` over the primary
objects DB. The `SecondaryMultiKeyCreator` impl reads the encoded
`RiakObject`, walks `siblings[*].indexes`, and emits one secondary
key per index entry of the matching name.

Range queries map onto `Cursor::get(Get::SearchRange)` followed by
`Get::Next` until the upper bound. The `term_regex` filter (a Riak 2i
feature) runs on materialised values at the end of the pipeline; we
do not push it down because Noxu does not have a regex predicate.

### 3.4 Tictac AAE

Riak's Tictac AAE replaced the original Riak 1.x AAE in 3.0. It is a
two-level Merkle tree of segment-id hashes maintained per vnode in a
small KV (Riak uses `aae_keystore`; we use `vnode_<id>_aae`). The
per-segment hash is the XOR of the per-key hashes within the segment.
The exchange protocol is: pick a peer, compare top-level digests,
recurse into mismatched segments, exchange the diverging key set,
repair via read-then-write.

Plan: **layer this over `crate::entropy`**. The existing entropy
module already implements peer-to-peer reconciliation for redis RDB
streams; we add a generic `EntropyTree` trait with one impl
(`TictacTree`) backed by Noxu, and one impl (`RdbStream`) backed by
the existing redis path. The `crate::entropy::send` and
`crate::entropy::receive` workers become parameterised.

### 3.5 Bitcask / eleveldb tunings

Bitcask's "expiry secs" maps to Noxu's `Durability::SyncPolicy` plus a
per-key TTL field added to `RiakObject`. eleveldb's `block_cache_size`
maps to Noxu's `EnvironmentConfig::cache_size`. Compaction is
internal to Noxu's log-structured B+tree and is not user-tunable in
the same shape; we expose it as `compaction_throttle_mb_per_sec` and
delegate to whatever Noxu surface emerges. **Open question (Section 8
#3)**: does Noxu currently expose a compaction-throttle knob; if not,
file a Noxu issue.

### 3.6 Cargo plumbing (already landed in this branch)

The path deps are wired so cargo can resolve them today. See Section 6.


---

## 4. Riak features (what to enable, in what order)

Each row classifies a Riak feature against the Dynomite substrate:
"reuse" if our existing code already covers it, "extend" if existing
code grows but does not change shape, "new" if we need a green-field
module. Effort is engineering-time (not calendar) for a single
focused worker agent.

### 4.1 Anti-entropy (AAE)

* **Substrate**: extend `crate::entropy` with an `EntropyTree` trait
  + `TictacTree` impl reading from the Noxu `vnode_<id>_aae` DB.
* **Protocol**: none (peer-only; runs over DNODE, encrypted by the
  existing `crypto::*` stack).
* **Effort**: 5-7 days. The Tictac algorithm itself is well-specified
  in `riak_kv_tictac_aae.erl`; the trick is the streaming exchange
  protocol.
* **Tests**: existing entropy tests parameterised over the new trait.

### 4.2 Read repair

* **Substrate**: D2 in `docs/riak-comparison.md`; lands with the
  per-DC response coalescer that is queued behind quorum readiness.
  Nothing Riak-specific.
* **Protocol**: none.
* **Effort**: ~1 day on top of the coalescer.
* **Tests**: extend the existing dispatch tests.

### 4.3 Hinted handoff

* **Substrate**: D1 in `docs/riak-comparison.md`. The Noxu backend
  gives us a durable disk queue for free: `vnode_<id>_handoff` is a
  Noxu Database keyed by a monotonic Sequence; the replay worker
  walks the cursor in order and `delete()`s entries on successful
  ack.
* **Protocol**: none externally. New peer message
  `DMSG_HINT_REPLAY` carries an opaque blob.
* **Effort**: 4-5 days (down from 3-5 in the comparison doc, because
  Noxu eliminates the file-format work).
* **Tests**: chaos-style multi-host integration test where one DC is
  paused, then recovers.

### 4.4 Bucket types

* **Substrate**: D3 in `docs/riak-comparison.md`. We extend
  `BucketTypeConfig` with the Riak superset: `n_val`, `r`, `w`, `pr`,
  `pw`, `dw`, `notfound_ok`, `basic_quorum`, `allow_mult`,
  `last_write_wins`, `precommit`/`postcommit` hook lists, `chash_keyfun`
  override, `linkfun`, `young_vclock`, `old_vclock`, `small_vclock`,
  `big_vclock`, `dvv_enabled`, `datatype` (for CRDT types).
* **Protocol**: `RpbGetBucketTypeReq`/`RpbSetBucketTypeReq` messages
  exposed; the registry is gossiped.
* **Effort**: 3-4 days.
* **Tests**: fixture YAML covering every knob; differential test
  asserting that two different prefixes get different routing.

### 4.5 Pipes (MapReduce)

* **Substrate**: **new**. `crate::pipe` module. The shape of
  `riak_pipe_*.erl` is a directed compute graph of stages; each stage
  has a fitting (`fit_proc`) and a dispatcher (`fit_disp`). Stages
  exchange `riak_pipe_vnode_queue_*` messages.
* **Implementation**: a tokio-based stage graph. Each fitting is a
  long-lived task with an mpsc input; the dispatcher splits inputs
  by partition and shards to the matching vnode worker. Built-in
  fittings: `map`, `reduce`, `link`, `kv_get`, `kv_listkeys`. The JS
  / Erlang scripted-fitting feature is **OUT** for v0; we accept
  Rust closures via the embedding API and a small fixed set of named
  fittings selectable by JSON config.
* **Effort**: 14-21 days. The single largest item.
* **Tests**: a curated corpus of MapReduce queries from the Riak
  documentation; round-trip via the HTTP and PB endpoints; assert
  output equivalence on a fixed dataset.

### 4.6 Secondary indexes (2i)

* **Substrate**: extend `cluster::dispatch` with an "index query"
  routing mode that fans out to all vnodes (a coverage query) and
  streams results.
* **Protocol**: `RpbIndexReq`/`RpbIndexResp` (PB), `GET
  /buckets/B/index/I/V` (HTTP).
* **Storage**: `SecondaryDatabase` per index, populated by the
  `RiakObject`'s `IndexEntry` list.
* **Effort**: 5-7 days; the dispatcher work is the bulk.
* **Tests**: index correctness under concurrent writes (property
  test); range pagination correctness; `$bucket` and `$key` meta-queries.

### 4.7 CRDTs (counter, set, map, register, hll)

* **Substrate**: untouched. Each CRDT op is a `Msg` whose payload
  carries a typed `RiakOp::DtUpdate { datatype, op }` payload. The
  dispatcher routes it like any write.
* **Datastore**: the merge logic lives entirely inside
  `NoxuDatastore::dispatch`. The CRDT crate `riak_dt_*.erl` types
  are well-specified state-based + operation-based hybrids. We
  introduce `crate::crdt::{counter, set, map, register, hll}` modules
  with one `Crdt` trait: `apply(state, op) -> state`,
  `merge(a, b) -> state`, `value(state) -> Value`.
* **Effort**: 7-10 days for all five types together; can be split.
* **Tests**: property tests against the CRDT laws (associativity,
  commutativity, idempotence) per type; cross-impl tests against
  reference outputs from the Erlang `riak_dt_*` test corpora when
  obtainable.

### 4.8 Coverage queries / streaming list_keys / list_buckets

* **Substrate**: extend `cluster::dispatch` with a `Coverage` plan
  variant that visits the smallest set of vnodes covering the keyspace.
  The plan is precomputed from the ring; the response is streamed
  back to the client via the existing per-conn writer.
* **Protocol**: `RpbListBucketsReq`/`Resp`, `RpbListKeysReq`/`Resp`,
  `RpbCoverageReq`/`Resp`.
* **Storage**: `Database::open_cursor` + `Cursor::get(Get::Next)` to
  iterate.
* **Effort**: 5-7 days.
* **Tests**: assert that for any ring topology, the coverage plan
  visits every key exactly once (property test on the substrate);
  end-to-end streaming tests with cancellation.

### 4.9 Authentication

* **HTTP basic + cert auth**: `tokio-rustls` for TLS termination
  (already in lamdb's dep graph as `rustls`; we promote it to
  workspace dep behind the `riak-storage` feature). HTTP basic is a
  trivial header parse. Cert auth uses the rustls peer cert chain
  passed into a configured `AuthProvider` trait.
* **Substrate**: extend `embed::hooks` with `AuthProvider`; default
  impl is `AllowAll`, alternative is `BasicAuth { users:
  HashMap<String, BcryptHash> }`.
* **Effort**: 3-4 days.
* **Tests**: golden-vector basic-auth headers; rustls peer-cert
  injection in the test harness.

### 4.10 Out of scope

Per user instruction:
* **Yokozuna / Search**: not implemented.
* **Riak TS**: not implemented.
* **Riak CS**: not implemented.

### 4.11 Summary table

| Feature | Substrate | Protocol layer | Effort |
|---|---|---|---|
| AAE (Tictac) | extend `crate::entropy` | none | 5-7d |
| Read repair | reuse coalescer (queued) | none | 1d on top |
| Hinted handoff | extend, new replay worker | new DMSG type | 4-5d |
| Bucket types | extend `BucketTypeConfig` | new PB+HTTP routes | 3-4d |
| MapReduce pipes | new `crate::pipe` | new PB+HTTP routes | 14-21d |
| 2i | extend dispatch (coverage) | new PB+HTTP routes | 5-7d |
| CRDTs | reuse | new PB+HTTP routes; `crate::crdt` | 7-10d |
| Coverage / list ops | extend dispatch | new PB+HTTP routes | 5-7d |
| Auth + TLS | new `AuthProvider` hook | new HTTP/PB plumbing | 3-4d |


---

## 5. riak-admin equivalent

Riak ships a single `riak-admin` CLI for cluster operations. We
introduce a new binary, `dyn-admin`, modelled on
`crates/dyn-hash-tool/` (a small, focused, single-binary crate that
calls into `crate::dynomite` library functions). It links the
embedding API rather than running a separate daemon. Each subcommand
is a `clap` derive variant.

### 5.1 Command catalogue

The Riak KV 2.2.3 docs define the following operator commands; we map
each to a `dyn-admin` subcommand and indicate which we ship in v0.

| `riak-admin` command | `dyn-admin` subcommand | v0? | Backing function |
|---|---|---|---|
| `cluster status` | `cluster status` | yes | reads gossip view from a running peer over DNODE; falls back to local YAML |
| `cluster join NODE` | `cluster join NODE` | yes | injects a seed peer; subsequent gossip handles the rest |
| `cluster leave [NODE]` | `cluster leave [NODE]` | yes | sends `GOSSIP_SHUTDOWN` |
| `cluster plan` | `cluster plan` | yes | computes pending vnode transfers from the staged ring |
| `cluster commit` | `cluster commit` | yes | applies the staged ring (writes to `cluster_state` Noxu DB) |
| `cluster clear` | `cluster clear` | yes | discards staged changes |
| `cluster force-remove NODE` | `cluster force-remove NODE` | v1 | DANGEROUS; behind `--yes-i-really-mean-it` flag |
| `cluster partitions` | `cluster partitions` | yes | shows vnode -> peer mapping |
| `cluster partition NODE` | `cluster partition NODE` | yes | filters above by node |
| `cluster partition-count` | `cluster partition-count` | yes | trivial summary |
| `ring-status` | `ring status` | yes | shows ring convergence state |
| `member-status` / `member_status` | `member status` | yes | per-node up/down/joining/leaving |
| `transfers` | `transfers` | v1 | hinted-handoff progress |
| `transfer-limit [N]` | `transfers limit [N]` | v1 | rate limit |
| `handoff status` | `handoff status` | v1 | per-vnode hint queue depth |
| `handoff enable` / `handoff disable` | `handoff enable|disable` | v1 | toggles the replay worker |
| `repair PARTITION` | `repair PARTITION` | v1 | triggers AAE exchange for a single vnode |
| `repair-2i` | `repair index INDEX` | v1 | rebuilds a 2i secondary database |
| `bucket-type create T '<json>'` | `bucket-type create T --props <file>` | yes | calls into `BucketRegistry` |
| `bucket-type activate T` | `bucket-type activate T` | yes | gossips activation |
| `bucket-type list` | `bucket-type list` | yes | lists registered types |
| `bucket-type status T` | `bucket-type status T` | yes | yields the activated config |
| `bucket-type update T '<json>'` | `bucket-type update T --props <file>` | yes | merges new props |
| `security enable` / `disable` | `security enable|disable` | v1 | toggles the AuthProvider |
| `security status` | `security status` | v1 | reports the current AuthProvider |
| `security add-user U` | `security add-user U` | v1 | writes to the BasicAuth provider |
| `security del-user U` | `security del-user U` | v1 | removes from BasicAuth |
| `security passwd U` | `security passwd U` | v1 | reissues bcrypt |
| `security add-source U H S O` | `security add-source ...` | v2 | per-CIDR auth source |
| `security grant P ON S TO U` | `security grant ...` | v2 | per-bucket ACL |
| `security revoke P ON S FROM U` | `security revoke ...` | v2 | inverse |
| `security ciphers` | `security ciphers` | v1 | rustls cipher suite list |
| `services` | `services` | yes | reports listener bind addresses + status |
| `wait-for-service S NODE` | `wait-for-service S` | yes | polls until the named service is listening |
| `ringready` | `ring ready` | yes | fast-path of `ring status` returning yes/no |
| `down NODE` | `down NODE` | yes | marks a node down; routing avoids it |
| `reip OLD NEW` | `reip OLD NEW` | v1 | rewrites the ring's address for a node |
| `reformat-indexes` | `reformat indexes` | v2 | one-shot 2i reformat for upgrades |
| `top` | `top` | v1 | tokio-console-style live process view; lower priority |
| `version` | `version` | yes | prints the binary version |
| `test` | `test` | yes | end-to-end PUT/GET roundtrip against the local node |
| `aae-status` | `aae status` | v1 | reports last exchange + tree build times |
| `diag` | `diag` | v1 | sanity-check report (file descriptors, disk space, log warnings) |

(`OUT-OF-SCOPE`) commands not listed: anything under `search-cmd`,
`riak-ts-*`, `riak-cs-*`, all per the user's instruction.

### 5.2 Mechanism

* `dyn-admin` either runs locally and reads/writes Noxu DBs directly
  (`bucket-type list`, `aae-status` from a running node) or talks to
  a peer over DNODE using a new admin-only message family
  `DMSG_ADMIN_*`. The DNODE-mediated commands are encrypted by the
  existing `crypto::*` stack so no new TLS plumbing is required.
* Output is human text by default; `--json` emits machine-readable
  output for scripting. Both shapes are versioned in
  `docs/book/src/operations/dyn-admin.md`.
* Tests: `assert_cmd` smoke tests for every subcommand, plus
  integration tests that spin up a 3-node embedded cluster and drive
  every v0 command end-to-end.

### 5.3 Crate layout

```text
crates/dyn-admin/
  Cargo.toml
  src/
    main.rs              # clap derive root
    cluster.rs           # cluster-* subcommands
    bucket_type.rs       # bucket-type-* subcommands
    handoff.rs           # handoff/transfers/repair
    security.rs          # security-* subcommands
    services.rs
    output.rs            # human + JSON renderers
  tests/
    smoke.rs
```

Lives behind the same `riak-storage` feature gate as the protocol
layer so a Redis-only build does not pull it in.


---

## 6. Cargo plumbing (landed in this branch)

### 6.1 What changed

Two files:

**Workspace `Cargo.toml`**: added 13 path entries under
`[workspace.dependencies]`, all pointing at
`../lamdb/crates/noxu-*`. The complete set is the closure required
to compile `noxu-db`:

```toml
noxu-db = { path = "../lamdb/crates/noxu-db" }
noxu-cleaner = { path = "../lamdb/crates/noxu-cleaner" }
noxu-config = { path = "../lamdb/crates/noxu-config" }
noxu-dbi = { path = "../lamdb/crates/noxu-dbi" }
noxu-engine = { path = "../lamdb/crates/noxu-engine" }
noxu-evictor = { path = "../lamdb/crates/noxu-evictor" }
noxu-latch = { path = "../lamdb/crates/noxu-latch" }
noxu-log = { path = "../lamdb/crates/noxu-log" }
noxu-recovery = { path = "../lamdb/crates/noxu-recovery" }
noxu-sync = { path = "../lamdb/crates/noxu-sync" }
noxu-tree = { path = "../lamdb/crates/noxu-tree" }
noxu-txn = { path = "../lamdb/crates/noxu-txn" }
noxu-util = { path = "../lamdb/crates/noxu-util" }
```

The user clarified that Noxu lives at `../lamdb/crates/noxu-*`, not at
`../noxu/`; the actual lamdb checkout was inspected to determine the
crate set. `noxu-bind`, `noxu-collections`, `noxu-persist`,
`noxu-rep`, `noxu-xa`, and `noxu-observe` are intentionally
**excluded**: they are higher-level conveniences that we do not need
for the Riak-compat path and pulling them would drag in `quinn`,
`rustls`, `tracing`, and `metrics` versions that may conflict with
our pins. We re-evaluate inclusion of `noxu-rep` (the Quoracle
quorum library) when D2 read-repair work begins.

**`crates/dynomite/Cargo.toml`**: added `noxu-db = { workspace =
true, optional = true }` and a new feature `riak-storage =
["dep:noxu-db"]`. The feature is **off by default**. Source-level
`use noxu_db::*` is forbidden in this branch; the feature exists
only so that cargo resolves the full Noxu dep graph against our
existing version pins, today, before any implementation work
starts.

### 6.2 Verified

* `cargo build --workspace --all-targets --locked` - clean.
* `cargo build --workspace --all-targets --all-features --locked` -
  clean. With `riak-storage` enabled, cargo compiled all 13 noxu
  crates plus their transitive deps without errors.
* `cargo nextest run --workspace --no-fail-fast` - 619 / 619 pass.
* `cargo test --doc --workspace` - clean.

### 6.3 Version-graph notes

Cargo resolved the following extra dependencies on top of the existing
Dynomite graph when `riak-storage` is on:

| Crate | Version | Source | Notes |
|---|---|---|---|
| `thiserror` | 2.0.18 | crates.io | Coexists with our pin at `1.0`. Cargo picks separate versions per consumer; no conflict. |
| `hashbrown` | 0.15.5 | crates.io | New runtime dep. Already implicitly pulled in by other crates we use indirectly. |
| `foldhash` | 0.1.5 | crates.io | hashbrown 0.15 default hasher. |
| `allocator-api2` | 0.2.21 | crates.io | hashbrown allocator helpers. |
| `byteorder` | 1.5.0 | crates.io | Standard. |
| `memmap2` | 0.9.10 | crates.io | Used by Noxu's mmap path; would need a `forbid(unsafe_code)` deviation if we ever surface it in `crate::dynomite`. Today we do not. |
| `fs2` | 0.4.3 | crates.io | flock helpers. |
| `lru` | 0.12.5 | crates.io | Standard cache. |

No version of `bytes`, `parking_lot`, `serde`, `tokio`, `tracing`, or
`log` was reshuffled. The `bytes 1` and `parking_lot 0.12` pins are
shared. Lamdb's `tracing-subscriber` and `metrics` deps are not
pulled because we do not enable `noxu-db`'s `observability` feature.

The branch's commit
`682e4a9 build(deps): add Noxu workspace deps for Riak-compat backend (path-style)`
contains the minimal diff and the verification log.

### 6.4 Risks

* The lamdb checkout pins `rust-toolchain.toml = 1.95`; our pin is
  `1.90`. The noxu-* crates use edition 2024, which is fine on
  rustc 1.85+. This is **not** a build break today (1.90 > 1.85),
  but the lamdb tree may add 1.95-only language features at any
  time. Mitigation: track the upstream pin and bump our toolchain
  to match when M1 starts (open question 8.5).
* Path deps mean a `cargo publish` of the dynomite crate would
  require simultaneously publishing every noxu-* crate. We are
  pre-0.1, no publishing yet. When we approach release, flip the
  path deps to versioned crates.io deps.


---

## 7. Sequencing and effort

The plan splits into nine milestones. Effort is single-worker focused
days; with two parallel workers some milestones overlap (noted).

### M1 - Storage substrate scaffolding (5-7 days)

* Create `crate::storage` module with the public types we need:
  `NoxuStore`, `RiakObject`, `VClock`, `Sibling`, `IndexEntry`.
* `NoxuStore::open(path)` opens an Environment, creates the
  `cluster_state` and `bucket_types` databases, scans for existing
  `vnode_<id>_objects` databases, and rebuilds the in-memory vnode
  table. No Riak protocol work yet.
* Implement `Datastore` trait for `NoxuStore` for a tiny subset
  (Get/Put/Delete on a single test bucket type) wiring through the
  existing dispatcher with `Protocol::Custom`.
* **Exit gate**: a `dynomite::embed::Server` configured with
  `NoxuStore` round-trips a hand-crafted `Msg` carrying `RiakOp::Put`
  through the substrate.
* **Blocks**: M2.
* **Parallelisable with**: M3 (admin), M5 (auth) if there are spare
  hands.

### M2 - Riak PB protocol layer, basic ops (10-14 days)

* `crates/riak-pb` crate generated from a vendored Riak `.proto`
  subset.
* `crate::proto::riak::pb` parser + writer driven by `tokio_util::codec`.
* `RiakOp` enum + `MsgType::ReqRiak*` extensions.
* Listener for the PB transport; per-conn handler.
* End-to-end: `riak-erlang-client` connects, performs Get/Put/Delete
  on a single bucket type, succeeds.
* **Exit gate**: differential test against a Riak 2.2.3 single-node
  cluster on a fixed corpus of objects.
* **Blocks**: M4, M5, M6.

### M3 - dyn-admin v0 commands (5-7 days)

* `crates/dyn-admin/` crate.
* Subcommands: `cluster status|join|leave|plan|commit|clear|partitions`,
  `ring status|ready`, `member status`, `services`, `version`, `test`,
  `bucket-type create|activate|list|status|update`.
* **Parallelisable with**: M2.

### M4 - HTTP protocol layer (7-9 days)

* `crate::proto::riak::http` mirroring M2's PB scope; reuses the
  same `RiakOp` enum.
* Hand-rolled HTTP/1.1 parser using `httparse`.
* JSON metadata encoding; multipart for siblings.
* **Exit gate**: `curl -X PUT /buckets/.../keys/...` and matching GET
  succeed.
* **Parallelisable with**: M3.

### M5 - Bucket types, 2i, coverage (10-14 days)

* Extend `BucketTypeConfig`, wire the registry into the gossip
  payload.
* `SecondaryDatabase`-backed 2i, with `RpbIndexReq`/`RpbIndexResp`.
* Coverage planner + `RpbListBucketsReq`, `RpbListKeysReq`,
  `RpbCoverageReq`.
* `dyn-admin` v1 commands for handoff/transfers/repair (depend on M7).

### M6 - Auth + TLS (4-5 days)

* `AuthProvider` hook trait, `AllowAll` and `BasicAuth` defaults.
* `tokio-rustls` integration for the HTTP and PB listeners.
* `STARTTLS` PB message.
* **Parallelisable with**: M5.

### M7 - Hinted handoff + read repair + AAE (10-14 days)

* `vnode_<id>_handoff` Noxu database + replay worker.
* Read-repair-on-quorum landing with the per-DC coalescer (queued
  separately).
* Tictac AAE Merkle tree backed by `vnode_<id>_aae`, layered onto
  `crate::entropy`.
* **Blocks**: M9 chaos.

### M8 - CRDTs + MapReduce (21-28 days)

* `crate::crdt::{counter, set, map, register, hll}` modules with the
  five datatypes.
* `DtFetchReq`/`DtUpdateReq` PB and HTTP routes.
* `crate::pipe` with a tokio-based stage graph; built-in fittings;
  Rust-closure stages via the embed API.
* `MapRedReq`/`MapRedResp`.
* Link walking layered on top.
* **The single biggest milestone**. Subdivide along
  `pipe` / `crdt::counter` / `crdt::set` / `crdt::map+register+hll`.

### M9 - Differential + chaos integration (5-7 days)

* Run the existing Stage 16 chaos harness against a
  Riak-compat-configured cluster.
* Add a Riak-flavoured workload to the chaos driver.
* Update `dist/chaos-reports/` template.

### Total

Single-worker total: 77-105 working days, i.e. ~16-21 weeks. With two
parallel workers and the parallelism noted above: ~10-14 weeks.

### Cross-team coordination items

These are Noxu features Dynomite needs that may not exist yet; each
needs an issue filed against the lamdb tracker:

1. **Compaction throttle knob** (Section 3.5 / Section 8 #3).
2. **Per-database open / close from a long-lived process under
   contention** - Riak's ring topology changes mean we open/close
   vnode databases live; Noxu's `Environment::open_database` and
   `remove_database` need to be safe under concurrent reads on
   sibling databases.
3. **Sequence durability semantics under crash** - the handoff queue
   relies on the Sequence being monotonic across restarts even when
   the prior process aborted mid-write.
4. **Cursor pagination across transaction commit boundaries** -
   long-running 2i scans may outlive a single transaction; we need
   either snapshot isolation that survives commit, or a
   resume-from-key idiom.
5. **Memory-budget controls** - per-environment cache sizes, plus a
   stat to introspect them, so the metrics sink can report them.

Filing these is part of M1's exit gate.


---

## 8. Open questions

Honest list of things this plan does **not** answer. Each carries a
confidence label and the smallest decision unit needed to resolve it.

### 8.1 Hash function for the Riak-compat ring

* Riak hashes with SHA-1 over a 160-bit ring partitioned into
  `ring_size` (default 64) vnodes.
* Dynomite hashes with murmur3 over a 32-bit ring; `ring_size` is
  determined by the `tokens:` list, default also 64.
* **Question**: does Riak compatibility mean *bug-for-bug* hash
  compatibility (so a key hashes to the same vnode in a Riak cluster
  and a Dynomite-with-Noxu cluster), or only *behavioural*
  compatibility (so the substrate's invariants hold but tokens
  differ)?
* **Confidence**: low. The user has not stated whether wire-level
  cluster migration from a real Riak deployment is in scope.
* **Recommendation if undecided**: ship behavioural compatibility
  with murmur3 (matches every other Dynomite hash), expose
  `chash_keyfun: sha1` as a per-bucket-type option for users who
  want to interoperate with a real Riak.

### 8.2 Vector clock implementation

* Riak ships *both* classic vector clocks and DVV (Dotted Version
  Vectors). DVV is the modern path; classic is legacy but still
  supported by some clients.
* **Question**: do we ship DVV only, or both?
* **Confidence**: medium. DVV-only is correct and simpler; the cost
  is older Riak Java/Erlang clients may complain.
* **Recommendation**: DVV only; document the deviation; if a real
  client breaks, add classic vclocks behind a config flag.

### 8.3 Noxu compaction throttle

* Section 3.5 references a `compaction_throttle_mb_per_sec` knob.
  We have not confirmed Noxu exposes one.
* **Confidence**: medium. The lamdb engine appears log-structured
  and self-managing; the knob may not exist.
* **Recommendation**: M1 worker files a Noxu issue and either
  surfaces the existing knob or requests one. Until then, document
  as "auto" and accept whatever Noxu does.

### 8.4 MapReduce scripted fittings

* Real Riak supports JS and Erlang stages in user-supplied MapReduce
  jobs. We do not run a JS or Erlang VM.
* **Question**: do we ship a Lua or WASM fitting host, or only Rust
  closures + named built-ins?
* **Confidence**: medium-high. Rust closures + named built-ins covers
  every operator workflow we have seen; user-supplied scripts are a
  multi-month sandbox project on their own.
* **Recommendation**: Rust closures + named built-ins for v0;
  WASM-fitting-host as a deferred follow-on (out of scope here).

### 8.5 Toolchain alignment with lamdb

* lamdb pins `1.95`, dynomite pins `1.90`. Today both build under
  `1.90` because nothing in lamdb actually requires `1.95`. Tomorrow
  they might.
* **Confidence**: high (the risk is real; the timing is unknown).
* **Recommendation**: when M1 starts, bump dynomite's
  `rust-toolchain.toml` to match lamdb's. AGENTS.md gets an entry
  noting the rationale.

### 8.6 Dual-protocol ports

* Real Riak listens on three ports per node (HTTP, PB, internal). We
  already listen on three (client proxy, dnode peer proxy, stats).
  Adding HTTP and PB makes five.
* **Question**: do we keep the existing Redis/Memcache `proxy` port
  and add HTTP/PB on dedicated ports, or multiplex?
* **Confidence**: high.
* **Recommendation**: dedicated ports, mirroring Riak's
  `riak_api_pb_listener` (8087) and `riak_api_http_listener` (8098)
  ports, both configurable. The existing proxy port stays for
  Redis/Memcache traffic.

### 8.7 Single-cluster vs separate-cluster deployment

* Can a single Dynomite cluster serve both Redis-protocol traffic to
  bucket type `redis_compat` and Riak-protocol traffic to bucket
  type `riak_default` at the same time, using the same Noxu
  storage?
* **Confidence**: low - this is a *vision* question. Architecturally
  the substrate allows it (the Datastore trait can dispatch based on
  the request's MsgType); operationally it is a foot-gun (one client
  protocol's bucket type semantics leaking into the other).
* **Recommendation**: support per-pool protocol selection (one pool
  is one protocol), not per-key-class. Document that mixing protocols
  in one pool is unsupported. Revisit after M2.

### 8.8 Backup / restore

* Riak ships `riak-admin backup` / `restore` as logical dumps. Noxu
  presumably has hot-backup primitives, but we have not surveyed
  them.
* **Confidence**: low.
* **Recommendation**: surface this in M1 alongside the Noxu
  cross-team items.

### 8.9 Metrics manifest

* Riak emits a *very* detailed metrics manifest via Exometer; we
  have a much smaller set in `crate::stats`. The Riak admin tools
  expect specific metric names.
* **Question**: do we map the Riak metric set onto our smaller
  manifest (and accept that some commands like `aae-status` show
  only a subset of fields), or extend `stats::*` to mirror Riak's
  manifest?
* **Confidence**: medium.
* **Recommendation**: extend incrementally as commands need them;
  accept that v0 emits a strict subset. Document gaps in the
  parity table.

### 8.10 Object size limits

* Riak default bucket-type cap is 50 MB per object. Noxu can store
  larger blobs but the wire protocols and the AAE Merkle hash are
  sensitive to object size.
* **Confidence**: high. We adopt Riak's 50 MB cap in
  `BucketTypeConfig` as `max_object_size_bytes`, configurable.


---

## Appendix A: ASCII-only check

This document contains no non-ASCII characters by construction; the
project lint `scripts/check_ascii.sh` will verify on commit.

## Appendix B: Cross-references

* `AGENTS.md` - operating rules; this document obeys section 5
  (style), section 11 (documentation), section 13 (embedding API
  discipline).
* `PLAN.md` Section 0 inventory (storage backend not in the original
  C surface) - this plan extends scope beyond the strict C-port
  inventory; the user explicitly authorised this.
* `docs/riak-comparison.md` - the source of truth for which Riak
  features are in scope; this plan is downstream of it.
* `docs/post-chaos-queue.md` D1, D2, D3 - hinted handoff, read repair,
  bucket types are queued there; this plan reuses their effort
  estimates and adds the Riak protocol layer on top.
* `docs/upstream-fork-review.md` - notes the Redis AUTH fork; the
  Riak path uses a separate AuthProvider trait, but both flow
  through the same `embed::hooks` extension point.
* `crates/dynomite/src/embed/hooks.rs` - the Datastore trait that
  the new `NoxuDatastore` will implement.
* `crates/dynomite/src/proto/redis/` and
  `crates/dynomite/src/proto/memcache/` - reference implementations
  for the new `proto::riak::http` and `proto::riak::pb` modules.
