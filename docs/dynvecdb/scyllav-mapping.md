# ScyllaDB Vector DB feature mapping

This table tracks every feature that ScyllaDB Vector DB exposes
through CQL against the `dynvecdb` MVP. Status values:

* **shipped** -- supported in this MVP, parity with the
  Scylla shape on the Rust API and the HTTP transport.
* **mvp-shipped** -- supported in this MVP through a different
  surface (HTTP) than ScyllaDB exposes (CQL); the engine
  primitive exists, the wire-protocol bridge is the gap.
* **not-yet** -- not implemented; covered by the stretch goal
  doc.

## Storage and types

| Feature                                  | Status        | Notes |
|------------------------------------------|---------------|-------|
| `VECTOR<FLOAT, N>` column type           | mvp-shipped   | dim is per-table, frozen at create. f32 components. |
| Per-row metadata columns                 | mvp-shipped   | Stored as `HashMap<String, serde_json::Value>` on `VectorRow`. |
| Tunable HNSW M                           | shipped       | `TableSchema::hnsw.m`. |
| Tunable HNSW efConstruction              | shipped       | `TableSchema::hnsw.ef_construction`. |
| Tunable HNSW efSearch                    | shipped       | Override per query via `Option<usize>` argument. |
| Vector compression: scalar quantisation  | shipped       | `Codec::Int8Quantized`, default. |
| Vector compression: half precision       | shipped       | `Codec::Fp16`. |
| Product Quantization (PQ)                | not-yet       | Future codec; scalar quant covers the 4x compression bracket. |
| Disk-resident index                      | not-yet       | Index is in-memory; rebuilt on startup from persisted rows. Persistence across restart works; the rebuild pays an O(N log N) cost. |
| Index persistence                        | mvp-shipped   | Rebuilt from row store; native serialisation is the obvious follow-up. |

## Distance metrics

| Feature                                  | Status        | Notes |
|------------------------------------------|---------------|-------|
| Euclidean / L2                           | shipped       | `Distance::Euclidean`. |
| Cosine                                   | shipped       | `Distance::Cosine`. Returns `1 - cos(theta)`. |
| Dot product / inner product              | shipped       | `Distance::DotProduct`. Negated for monotonic ordering. |
| Manhattan / L1                           | not-yet       | Trivial to add (one match arm in `distance.rs`). |

## Query surface

| Feature                                  | Status        | Notes |
|------------------------------------------|---------------|-------|
| `SELECT ... ANN OF [...] LIMIT K`        | not-yet       | Stretch: CQL parser. The HTTP `POST /search` endpoint covers the same operation. |
| Hybrid filter + ANN                      | not-yet       | Engine supports filter via metadata; query API does not yet expose it. |
| Pagination on ANN results                | not-yet       | Single-shot top-K only. |
| Multiple vectors per row                 | not-yet       | One vector column per table. |

## Distribution and replication

| Feature                                  | Status        | Notes |
|------------------------------------------|---------------|-------|
| Token-ring routing                       | mvp-shipped   | `dynomite::cluster::vnode` exists; HTTP layer not wired through it yet. |
| Quorum reads / writes                    | mvp-shipped   | Engine supports W of N; the search coordinator handles fanout. |
| Cross-DC replication                     | mvp-shipped   | Inherited from `dynomite::cluster`. |
| Active anti-entropy                      | mvp-shipped   | `dyniak` AAE works for KV; vector AAE plugs in via the same machinery. |
| Hinted handoff                           | mvp-shipped   | `dynomite::cluster::hints`. |

## Operational surface

| Feature                                  | Status        | Notes |
|------------------------------------------|---------------|-------|
| HTTP admin API                           | shipped       | `GET /tables`, `GET /tables/{n}/stats`. |
| Per-table stats (live rows, dim, codec)  | shipped       | `GET /tables/{n}/stats`. |
| Per-row inspection                       | shipped       | `GET /tables/{n}/vectors/{key}` returns decoded vector + L2 norm + codec. |
| `nodetool` parity                        | not-yet       | Use `dynomite::admin` channels. |
| Prometheus metrics                       | not-yet       | Hooks via `dynomite::stats`; not yet exposed on dynvecdb. |
| Distributed tracing                      | not-yet       | `tracing` events emitted; OTLP wire-up via `dynomited`. |

## Wire protocols

| Feature                                  | Status        | Notes |
|------------------------------------------|---------------|-------|
| HTTP/JSON                                | shipped       | `src/api.rs`. |
| CQL native protocol                      | not-yet       | Stretch goal; see `docs/dynvecdb/cql-stretch.md`. |
| gRPC                                     | not-yet       | Possible future surface. |
