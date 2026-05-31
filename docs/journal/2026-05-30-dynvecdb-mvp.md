# 2026-05-30 -- dynvecdb MVP

Stage: dynvecdb-mvp
Branch: stage/dynvecdb-mvp
Worktree: /home/gburd/ws/wt-dynvecdb-mvp

## Mission

Build a vector database engine that uses dynomite for
distribution and Noxu for node-local storage, shaped to be a
drop-in conceptual replacement for ScyllaDB Vector DB.

The user's brief asked for "all the features" of ScyllaDB
Vector DB; the realistic 3-hour scope is the MVP engine plus a
clear migration story to CQL (the wire protocol Scylla
exposes). The MVP ships a working core with HTTP transport;
CQL is documented as a 2-3 week stretch goal.

## What landed

* New crate at `crates/dynvecdb/` (workspace member, builds
  under default and `--all-features`).
* Storage layer (`src/storage.rs`):
  * `Backend` trait with an in-memory implementation
    (`MemoryBackend`).
  * `VectorStore` front: per-table `Mutex<TableState>` carrying
    a schema, an HNSW index, and the row key <-> NodeId map.
  * Rehydration on open: walks every persisted row, decodes
    via the table's codec, and rebuilds the HNSW from scratch.
* Encoding layer (`src/encoding.rs`):
  * `Int8Quantized` (per-vector min + scale, 4x compression).
  * `Fp16` (2x compression via the `half` crate).
  * Both reject non-finite components; both surface a
    self-describing `EncodedVector { codec, dim, bytes,
    params }` so a future inspect tool renders without
    re-decoding.
* Distance layer (`src/distance.rs`):
  * `Euclidean`, `Cosine`, `DotProduct`. Scalar `f32`; SIMD
    deferred (workspace forbids `unsafe_code` and `std::simd`
    is nightly-only on stable 1.83).
* HNSW index (`src/index.rs`):
  * Hand-rolled per Malkov & Yashunin (2018).
  * M=16, M0=32, ef_construction=200, ef_search=50.
  * Soft-delete tombstones with "include in graph traversal,
    filter at search boundary" semantics so the graph stays
    connected through deletions.
  * Recall@10 = 1.000 on N=1000 random 64-dim vectors,
    well above the 0.85 target.
* Cluster query coordinator (`src/cluster_query.rs`):
  * `gen-fsm` driven state graph: Init -> Fanout -> Gather ->
    Merge.
  * `PeerProbe` trait so the FSM is testable in-process
    without a real cluster.
  * Merge stage de-duplicates by NodeId (multi-replica
    correctness) and takes top-K.
* HTTP API (`src/api.rs`, gated on `http` feature):
  * Routes: tables, vectors, search, stats, healthz.
  * Built directly on `hyper`, mirroring the dyniak shape
    so the workspace does not pick up a new HTTP framework.
* Quickstart example (`examples/quickstart.rs`).
* Throughput bench (`benches/throughput.rs`).
* Docs:
  * `docs/dynvecdb/architecture.md`
  * `docs/dynvecdb/scyllav-mapping.md`
  * `docs/dynvecdb/cql-stretch.md`

## HNSW choice

I evaluated `instant-distance` and `hnsw_rs` and rejected both:

1. The on-disk row is an `EncodedVector`, not an `f32` slice.
   Wrapping either crate's `Point` trait would require either
   a parallel `Vec<f32>` cache (doubles memory) or a wrapper
   that locks us into the upstream API.
2. Neither crate exposes deletion semantics. A vector
   database without deletes is not a database.
3. The hand-rolled implementation is < 700 lines, has zero
   third-party dependencies, and is fully under the project's
   test/lint/format gates.

The hand-rolled implementation reaches recall@10 = 1.000 with
default tuning on the integration test workload, which is
plenty for the MVP. The follow-up tune knobs are documented in
the architecture doc.

## New dependencies

`half = "2.4"` was added to the dynvecdb crate Cargo.toml. It
is already in the workspace lockfile (transitively via the OTel
stack). No new third-party crate is added at the workspace
level.

`hyper`, `hyper-util`, `http-body-util` are referenced via the
`http` feature; all already used by `dyniak`.

## Tests

* Lib tests (`cargo test --lib`): 27 passing.
* `tests/encoding.rs`: 7 tests (round-trip, error budget,
  malformed-payload detection).
* `tests/distance.rs`: 10 tests (canonical values per metric,
  edge cases).
* `tests/index.rs`: 3 tests (recall@10 for euclidean and
  cosine on N=1000 64-dim, soft-delete behaviour).
* `tests/storage.rs`: 7 tests (round-trips, rehydration,
  metric routing).
* `tests/api_http.rs`: 4 tests (full round-trip, conflict,
  dim mismatch, 404).
* Doc tests: 2 passing.
* Total: 60 tests, all passing.

Recall@10 measured: 1.000 (euclidean), 1.000 (cosine).

## Verification

```
cargo build -p dynvecdb --all-features         # OK
cargo build --workspace                        # OK
cargo build --workspace --features dynomite/riak-storage   # OK
cargo test -p dynvecdb --all-features          # 60 tests pass
cargo test --doc -p dynvecdb                   # 2 doctests pass
cargo clippy -p dynvecdb --all-targets --all-features -- -D warnings   # clean
cargo fmt --all -- --check                     # clean
cargo run -p dynvecdb --example quickstart --features http   # binds + serves OK
```

## Bugs found and fixed during development

1. HNSW soft-delete connectivity bug: the original
   implementation passed `include_deleted=false` to
   `search_layer` from the insert path, which meant a freshly
   tombstoned node's slot was unreachable for new inserts.
   Fix: insert and search both run the layer search with
   `include_deleted=true` (so graph traversal sees every
   node), and the public `search()` filters tombstoned nodes
   at the result boundary. Caught by
   `storage::tests::upsert_replaces_metadata_and_vector`
   after the upsert path was changed to soft-delete the prior
   node and insert a fresh one.

2. Codec serde tag mismatch: `Codec` was missing
   `#[serde(rename_all = "snake_case")]` so HTTP requests with
   `"codec":"int8_quantized"` returned 400. Caught by the
   HTTP integration test.

3. Test fixture array size mismatch in
   `storage::tests::search_returns_nearest_first` (heterogeneous
   `b"unit_x"` vs `b"diag"` byte literals). Fixed by adding
   explicit `&[u8]` slices.

## CQL stretch

Documented in `docs/dynvecdb/cql-stretch.md`. The decision
points are tagged `// CQL future:` in `src/api.rs`. The
follow-up worker has hooks; a 2-3 week budget delivers the
full Scylla wire-compatible surface.

## Open questions / follow-ups

* HNSW index persistence (write the graph to disk, mmap on
  open) is not yet implemented; the MVP rebuilds from rows.
  For a 10M-row table that's an O(N log N) reopen cost.
  Acceptable for the MVP; a follow-up slice writes the graph
  out via `bincode` to a Noxu sub-DB.
* Full distributed wire-up: the HTTP layer routes to the
  local store; the `cluster_query::run` coordinator is
  independently tested but not yet plugged into the HTTP
  search path. The `// CQL future:` annotations mark the two
  call sites that need to switch from local to coordinator.
* No Noxu-backed `Backend` impl yet; the `noxu` feature is
  declared but only enables the `noxu` feature on the
  `dyniak` dependency. The full impl needs a small wrapper
  around `NoxuDatastore::put_object` / `get_object` that
  serialises a `VectorRow` via bincode. Out of MVP scope.

## Status

```
STATUS: READY_FOR_REVIEW
BRANCH: stage/dynvecdb-mvp
NEW_CRATE: crates/dynvecdb
HNSW_LIB: hand-rolled (rationale: avoids parallel vector cache,
          supports deletion, no new third-party dep, ~700 LOC)
ENCODINGS: Int8Quantized, Fp16
DISTANCE_METRICS: euclidean, cosine, dot
TESTS: 60 (27 lib + 7 storage + 10 distance + 7 encoding +
       3 index + 4 api + 2 doc)
RECALL_AT_10: 1.000 (both euclidean and cosine on N=1000 64-dim)
DOCS: docs/dynvecdb/{architecture,scyllav-mapping,cql-stretch}.md
NOTES: HTTP layer not yet wired through cluster_query coordinator
       (noted in architecture.md and tagged in code). Noxu
       backend declared as a feature, full impl deferred.
```
