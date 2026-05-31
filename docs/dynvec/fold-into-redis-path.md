# dynvecdb fold into the dynomite Redis path

## Decision

dynvecdb-as-its-own-server (HTTP API on port 21900) is the wrong
shape. The right shape is **dynvecdb as a vector subsystem inside
dynomite, exposed via Redis Stack's RediSearch FT.* commands over
the existing Redis listener**.

Outcome:
- Drop the standalone `dynvecdb` HTTP server.
- Extract the vector storage + index code into a backend module.
- Wire FT.CREATE, FT.SEARCH, FT.INFO, FT.ADD, FT.DROPINDEX, etc.
  into the existing Redis parser + dispatcher.
- Keep the HTTP API as a secondary inspection surface (debug
  only; not the primary protocol).
- Drop the CQL stretch goal until ScyllaDB vector adoption
  warrants it. RediSearch covers the Redis ecosystem; that is
  90% of vector-DB clients.

## Why RediSearch

The user asked: is there a standard network API for vector DBs?
Survey:

| Protocol | API style | Adoption | Compat fit for dynomite |
|---|---|---|---|
| pgvector | SQL over PG wire | Largest | Would need a PG wire impl. 4 weeks. |
| **RediSearch** | **RESP via FT.* commands** | **Large** | **Native fit; we already speak Redis** |
| Cassandra 5 / Scylla | CQL VECTOR<> | Smaller | Would need CQL parser + binary protocol. 8 weeks. |
| Pinecone | REST | Cloud-only | Proprietary; can't drop-in |
| Qdrant | gRPC + REST | Smaller | Proprietary |
| Weaviate | REST + GraphQL | Smaller | Proprietary |
| Milvus | gRPC | Smaller | Proprietary |
| Chroma | REST | Niche | Proprietary |

RediSearch is the only "real" standard reachable from the
existing dynomite Redis path. Every Redis client library already
ships RediSearch helpers (`redis-py`, `node-redis`, `Lettuce`,
`StackExchange.Redis`, `go-redis`, etc.). Migrating from
Redis-Stack-with-RediSearch to dynomite-with-vectors becomes a
one-line config change for downstream apps.

## Wire-protocol mapping

Today's RediSearch commands -> dynomite implementation:

```text
FT.CREATE idx
    ON HASH
    PREFIX 1 docs:
    SCHEMA
        title TEXT
        vec VECTOR HNSW 6 TYPE FLOAT32 DIM 1024 DISTANCE_METRIC COSINE
```
=> Creates a `dynvecdb::Table` with HNSW index, dim=1024,
   distance=cosine, encoding inferred from TYPE clause.

```text
HSET docs:1 title "first" vec <16384 raw bytes>
```
=> Inserts (key=`docs:1`, vector=parsed_from_bytes,
   metadata=`{"title": "first"}`) into the table.

```text
FT.SEARCH idx "*=>[KNN 5 @vec $blob]"
    PARAMS 2 blob <16384 raw bytes>
    RETURN 2 __vec_score title
    SORTBY __vec_score
    LIMIT 0 5
```
=> Top-5 k-NN search via the table's HNSW index.

```text
FT.INFO idx
```
=> Returns table stats: dim, distance, num_docs, index_size,
   etc.

```text
FT.DROPINDEX idx
    DD     # also delete underlying docs
```
=> Drops the table (and the underlying HSET data if `DD` is
   given).

## Storage backend

Noxu (via `data_store: noxu` in Riak mode, or a future
`data_store: noxu_redis` for the Redis path):
- Each Vector row stored as a Noxu key (the doc id) plus a
  CompressedVector blob (Int8Quantized | Fp16 | PQ).
- Noxu's B-tree gives O(log n) point reads.
- Noxu's MVCC means reads don't block concurrent writes.
- Noxu's eviction layer keeps hot vectors in memory.
- HNSW graph nodes stored in a sibling Noxu sub-DB so the index
  is durable across restarts.

For mostly-read workloads, Noxu's read-amp profile is good:
- Bloom filters on each LSM segment
- Lazy compaction (writes batched; reads see stable snapshot)
- mmap-friendly format

## Compression strategy

Already shipped:
- Int8Quantized (per-vector min+scale, 4x compression, ~1% recall loss)
- Fp16 (2x compression, ~0.05% recall loss)

To add (this fold's follow-up):
- **Product Quantization (PQ)**: split each vector into M
  subvectors, quantize each subvector to one of 256 centroids.
  16x-32x compression. ~5% recall loss. The technique used by
  FAISS / ScaNN / Annoy.
- **Binary Quantization (BQ)**: 1 bit per dim. 32x compression,
  ~10-20% recall loss. Only useful for very-low-precision
  scenarios (rerank required).

Inspectability stays the same: each compressed vector carries
a header with its codec id, dim, and codec parameters
(centroids id for PQ, scale+offset for Int8Q, etc.) so
`dyn-admin vector-inspect <key>` can show "vector dim=512,
codec=PQ-16-256, centroids@0xabc...".

## Distribution

Reuse what dynomite already has. No new code:
- Hash(key) -> vnode -> primary peer (existing
  cluster::vnode)
- Inserts: route to primary peer; replicate to N successors
  (existing dispatch::dispatch)
- k-NN queries: BROADCAST to all primary peers covering the
  table's partition set, gather top-K from each, merge
  (the gen-fsm coordinator we already have in
  dyniak::handoff::fsm pattern, but new in
  dynomite::vector::query_fsm)
- AAE: vector tables participate in AAE just like any other
  data; the existing dyniak AAE handles them.

## What to delete vs keep in the existing crates/dynvecdb/

**Keep**:
- `encoding/` (Int8Quantized + Fp16 + future PQ)
- `distance/` (Euclidean + Cosine + DotProduct)
- `index/` (HNSW)
- `storage/` (Noxu Vector row layout)
- `tests/{encoding,distance,index,storage}.rs`
- `benches/throughput.rs`

**Re-shape**:
- `api.rs` (HTTP server) -> drop OR keep as a dev/debug surface
  only. Document it clearly as "not the primary protocol".
- `cluster_query.rs` -> move to
  `crates/dynomite/src/vector/query_fsm.rs` since it leverages
  the cluster machinery.

**Add**:
- `crates/dynomite/src/proto/redis/ft_*.rs` for FT.CREATE,
  FT.SEARCH, FT.INFO, FT.DROPINDEX, FT.ALTER, FT.AGGREGATE,
  FT.CONFIG, FT.LIST. Parsed at the same layer as today's GET /
  SET / HSET.
- `crates/dynomite/src/vector/` for the index registry, schema
  store, and the FT.* command handlers. Imports from dynvecdb
  (encoding, index, storage).

## Migration path

1. Phase A (smallest first): rename `crates/dynvecdb/` to
   `crates/dynvec/` (it's the engine, not the database). Keep
   the contents.
2. Phase B: add `crates/dynomite/src/vector/` that uses
   `dynvec` for storage + index. Wire it into the dispatcher.
3. Phase C: extend the Redis parser to recognize FT.* and
   route the right commands.
4. Phase D: make the standalone `dynvec` HTTP server an
   optional "tools" feature, not on by default.
5. Phase E: ship docs/dynvec/redisearch-mapping.md showing
   exactly which RediSearch commands are supported and which
   are TODO.

Estimated effort:
- Phase A: 30 min (rename + Cargo.toml updates)
- Phase B: 4-6 hours (registry + dispatcher wiring)
- Phase C: 8-12 hours (FT.* command parsing; RediSearch syntax
  is non-trivial)
- Phase D: 30 min
- Phase E: 1 hour

Total: about 2 days of focused work.

## Decision matrix

| Approach | Speed to ship | Adoption story | Storage win | Complexity |
|---|---|---|---|---|
| Status quo (HTTP API only) | shipped | weak | as-is | low |
| Standalone HTTP + CQL | 8 weeks | medium | as-is | high |
| **Fold into dynomite Redis path with RediSearch** | **2 days** | **strongest** | **same code** | **medium** |
| Fold into dynomite + add PQ compression | 3 days | strongest | better | medium |

The fold is the right call. Schedule:
- This week: Phase A + B + C (rename, integrate, FT.*).
- Next week: Phase D + E + PQ compression.
- Following week: real-Redis RediSearch corpus replay test
  (point a Redis benchmark like `redis-benchmark` with the FT.*
  workload at dynomite and compare to a real Redis Stack
  cluster).

## Note on Noxu crate publication

`noxu-*` is on crates.io but at `0.0.0` placeholder versions
(name squats from the noxu maintainer). Only
`noxu-persist-derive` has a real published version (3.0.0).
Real Noxu is at v2.4.2 in the noxu workspace; we use it via
path-deps from `~/ws/noxu/crates/noxu-*`. CI clones noxu
sibling-checkout to make path-deps work.

If/when noxu-* gets real crates.io releases, switching from
path-deps to versioned deps is a one-line `Cargo.toml` change.
The CI noxu-clone step can then be removed.

For now: status quo on Noxu. The vector fold doesn't depend
on Noxu being on crates.io.
