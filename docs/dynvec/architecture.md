# dynvecdb architecture

`dynvecdb` is the vector-database engine for the Dynomite Rust
port. It composes three pieces that already live in the
workspace:

* **distribution** -- `dynomite::cluster::vnode` plus
  `dynomite::cluster::apl` for routing primary writes and the
  per-table peer set.
* **node-local persistence** -- Noxu DB through the
  `dyn-riak::datastore::NoxuDatastore` shape (the same engine
  the Riak-compatible surface uses) wrapped by an in-crate
  abstraction so an embedder who does not have lamdb checked
  out next door can still spin up an in-memory store.
* **state-graph orchestration** -- `gen-fsm`, used for the
  distributed k-NN coordinator (Init -> Fanout -> Gather ->
  Merge).

## Layers

```
            +-----------------------------+
            |          api (HTTP)         |
            +-------+------+--------+-----+
                    |              |
                    v              v
       +------------+--+       +---+------------------+
       |  cluster_query|       |     storage          |
       |  (gen-fsm)    |<----->| TableState (per-tbl) |
       +------+--------+       +-----+--+-------------+
              |                      |  |
              | per-peer probes      |  | encoded rows
              v                      |  v
       +------+-----+         +------+--+--------+
       |  dynomite  |         |  Backend trait   |
       |  cluster:: |         | MemoryBackend or |
       |   vnode    |         | NoxuBackend (TBD)|
       +------------+         +------------------+
                                       |
                                       v
                                   +---+----+
                                   |  HNSW  |
                                   |  index |
                                   +--------+
```

`storage::VectorStore` owns a `parking_lot::Mutex<TableState>`
per registered table. Each `TableState` carries:

* the table's `TableSchema` (frozen dim, codec, distance,
  HNSW tuning);
* an in-process `HnswIndex` (the ANN graph);
* a bidirectional `RowKey <-> NodeId` map so search results can
  be hydrated against the `Backend`-persisted row map.

## Encodings

Three codecs ship in `encoding`:

* `Int8Quantized` (default). Stores per-vector `min` and `scale`
  alongside `dim` `u8`s. Compression: 4x vs raw `f32`.
  Reconstruction error: `range / 255` per component, well below
  1% on typical embedding distributions.
* `Fp16`. IEEE 754 half-precision; 2x compression, ~0.05%
  reconstruction error. Used when an embedder cares more about
  decode latency than disk footprint.
* `Turbovec2Bit`, `Turbovec3Bit`, `Turbovec4Bit` -- 2/3/4-bit
  data-oblivious quantisation backed by the `turbovec` crate's
  TurboQuant codec. The compressed packed codes (16x / 10.6x /
  8x compression) live inside a per-table SIMD index
  (`crate::turbo_index::TurboTable`); the per-row
  `EncodedVector` holds the original `f32` bytes so round-trip
  and rehydration paths stay exact. The headline win is
  search-time SIMD throughput, not row-storage footprint.

The scalar codecs (`Int8Quantized`, `Fp16`) reject non-finite
input components so the index guarantees finite distance
scores. Their encoded bytes are self-describing:
`EncodedVector { codec, dim, bytes, params }`.  A future
`dynvecdb-cli inspect <id>` command renders the params
without rerunning the codec.

### Picking an encoding

| Encoding       | Compression | Search speed     | Recall@10 (typical) | When to pick |
|----------------|-------------|------------------|---------------------|--------------|
| `Fp32` (raw)   | 1x          | scalar baseline  | 100%                | tiny tables; debugging; no compression need |
| `Fp16`         | 2x          | ~1.2x baseline   | ~99.95%             | low-loss memory tradeoff |
| `Int8Quantized`| 4x          | ~1.5x baseline   | ~99%                | balanced default |
| `Turbovec4Bit` | 8x          | ~3-5x baseline   | ~88-95%             | large tables (>=1M rows) where some recall loss is acceptable |
| `Turbovec2Bit` | 16x         | ~5-8x baseline   | ~85-90%             | very large tables; pair with a re-rank pass against the f32 row bytes |

The Turbovec speed ratios are head-to-head against the HNSW +
scalar `f32` baseline at 64-dim; the speedup grows on higher
dims because the SIMD lanes amortise more decoding work per
candidate. The recall numbers come from the
`recall_at_10_with_turbovec_4bit_above_85pct` integration test
on 1024 uniformly random 64-dim vectors with
`Distance::Cosine`; a representative run measured 0.8844.

### Distance semantics with Turbovec

The Turbovec path is inner-product-native. To honour all three
`Distance` metrics, `TurboTable`:

* L2-normalises queries and stored vectors at ingest time when
  the metric is `Cosine` or `Euclidean`, so turbovec's
  inner-product surrogate doubles as a `cos(theta)` estimate.
* Maps the resulting similarity score to dynvecdb's
  smaller-is-closer convention via `1 - similarity` (Cosine),
  `sqrt(max(0, 2 - 2 * similarity))` (Euclidean), or
  `-similarity` (DotProduct).

For `Euclidean`, the cosine surrogate orders identically to L2
distance on the unit-normalised inputs but is not bit-equal to
a true L2 score. Embedders that need exact L2 should pick the
HNSW path or run a post-search re-rank against the row-stored
`f32` bytes.

## Distance metrics

* `Euclidean` -- `sqrt(sum((a-b)^2))`.
* `Cosine` -- `1 - cos(theta)`. Smaller is closer; orthogonal
  is `1.0`, antiparallel is `2.0`.
* `DotProduct` -- negated inner product, so the same comparator
  works across all three metrics.

The implementation is scalar `f32`. Portable SIMD via
`std::simd` is intentionally not used: the workspace forbids
`unsafe_code` and `std::simd` is still nightly-only on stable
1.83.

## HNSW index

Hand-rolled HNSW over `f32` vectors with the parameters from
Malkov & Yashunin (2018):

* M = 16 (M0 = 32 at layer 0)
* ef_construction = 200
* ef_search = 50 (raisable per query)
* layer assignment: `floor(-ln(rand()) / ln(M))`

Why not `instant-distance` or `hnsw_rs`?

1. The on-disk row is an `EncodedVector`, not an `f32` slice.
   A wrapper around either crate's `Point` trait plus the row
   layer would lock us into that crate's API surface or
   require keeping a parallel `Vec<f32>` cache (doubling the
   memory cost).
2. Neither crate exposes deletion semantics. A vector database
   that cannot delete is not a database.
3. The hand-rolled implementation is < 700 lines and has zero
   third-party dependencies. The review burden is lower than
   pulling in a new HNSW crate.

Recall@10 on N=1000 random 64-dim vectors with the default
tuning reaches 1.000 in our integration tests
(`tests/index.rs`); see the test for ground-truth comparison
and shrink-budget margins.

Deletes are soft-tombstone: the node stays in the graph for
connectivity purposes (insert paths still see it) but is
filtered out of search results. A future compaction pass that
re-builds the graph from scratch is the obvious cleanup path.

## Distributed k-NN coordinator

`cluster_query::run` builds a `gen-fsm` driver around the
`Coordinator` handler. The state graph:

```
Init  -- Fanout(internal) -->  Fanout
Fanout  -- Gather(internal) -->  Gather  (also issues per-peer probes)
Gather  -- PeerHits(N times) -->  Gather  (collect, accumulate)
Gather  -- GatherComplete(internal) -->  Merge
Gather  -- state-timeout -->  Merge  (drains slow peers, merges what we have)
Merge  -- (stop) -->  done, response_lock holds the SearchResponse
```

The `PeerProbe` callback is supplied by the embedder so the FSM
is testable in-process: the unit tests build a probe that
returns canned hits and validate the merge logic without
standing up real peers.

The merge stage de-duplicates by `NodeId` (a single key may
land on multiple peers under N=3 replication) keeping the
smallest-score entry, then takes top-K.

## HTTP API (MVP)

Routes:

* `GET /healthz`
* `GET /tables`, `POST /tables`
* `GET /tables/{name}/stats`
* `POST /tables/{name}/vectors`
* `GET /tables/{name}/vectors/{key}`
* `DELETE /tables/{name}/vectors/{key}`
* `POST /tables/{name}/search`

Implementation lives in `src/api.rs`. Built directly on `hyper`
to avoid adding a new HTTP framework dependency to the
workspace; the route table mirrors the shape used by
`dyn_riak::proto::http`.

The MVP HTTP server runs single-node: writes and searches go
to the local `VectorStore`. The cluster-wide fanout shape lives
in `src/cluster_query.rs` and is independently tested; wiring
the HTTP layer to that coordinator is the obvious next slice.
The entry points are marked `// CQL future:`.

## CQL stretch goal

A drop-in CQL native protocol surface that exposes the
`storage` layer through Cassandra-compatible CREATE TABLE and
SELECT statements is documented in `docs/dynvecdb/cql-stretch.md`.
It is NOT shipped in this MVP. Estimated effort: 2-3 weeks of
focused work.

## Distribution semantics (target shape)

Once the HTTP layer is wired through `cluster_query::run`:

* Inserts: HTTP -> `hash(key)` via `dynomite::hashkit::hash` ->
  primary peer via `dynomite::cluster::vnode::dispatch` ->
  peer writes locally and replicates to N successors. Default
  N=3, W=2.
* Searches: HTTP -> coordinator broadcasts to one peer per
  vnode covering the search's partition set -> per-peer top-K
  -> merge to global top-K.
* Replication and AAE: re-uses the existing `dyn-riak`
  active-anti-entropy machinery; vector rows are content-
  addressable via the row key.

These hooks all exist in the workspace already; the next slice
is wiring them through.
