# CQL native-protocol stretch goal

This doc captures the design for a future drop-in CQL-native
wire protocol surface on top of `dynvecdb`. It is **not**
implemented in this MVP. Estimated effort: 2-3 weeks of
focused work.

## Why it is not in the MVP

The brief originally asked for a "drop-in wire-protocol
compatible replacement for ScyllaDB Vector DB". The realistic
breakdown of that work:

* Cassandra binary protocol v5 frame format: ~3 days.
* CQL parser with vector extensions: ~5 days.
* Prepared statement registry and binding: ~3 days.
* Pagination state, metadata flags, batches: ~3 days.
* CQL-shaped query planner that maps `ORDER BY x ANN OF [...]`
  to `VectorStore::search`: ~3 days.
* Authentication, TLS, connection lifecycle: ~3 days.

Total: ~3 weeks. The MVP picks the HTTP API as the wire
surface so the engine architecture is end-to-end testable
within a 3-hour budget; CQL is added over the same engine
primitives in the next slice.

## Wire protocol

Cassandra's binary protocol v5 (CASSANDRA-9362) shapes:

```
+----+------+--------+--------+----+--------+
| ver| flgs | stream | opcode |len | body...|
+----+------+--------+--------+----+--------+
   1     1      2        1     4    len bytes
```

Opcodes used:

* `0x01` STARTUP -> `0x02` READY
* `0x05` OPTIONS -> `0x06` SUPPORTED
* `0x07` QUERY -> `0x08` RESULT
* `0x09` PREPARE -> `0x08` RESULT(prepared)
* `0x0A` EXECUTE -> `0x08` RESULT
* `0x00` ERROR

Mapping:

* `STARTUP` carries a string-multimap of options. Implement
  `CQL_VERSION`, `COMPRESSION`, `DRIVER_NAME` parsing; reply
  with `READY` (no AUTHENTICATE in the MVP).
* `OPTIONS` -> static `SUPPORTED` map naming
  `CQL_VERSION=3.4.0`, `COMPRESSION=lz4` (or none).

## CREATE TABLE with vectors

```sql
CREATE TABLE embeddings (
    id text PRIMARY KEY,
    title text,
    embedding VECTOR<FLOAT, 384>
) WITH vector_options = {
    'distance': 'cosine',
    'codec': 'int8_quantized',
    'index': 'hnsw',
    'index_options': {
        'm': 16,
        'ef_construction': 200
    }
};
```

Maps to:

```rust
let schema = TableSchema {
    name: "embeddings".to_string(),
    dim: 384,
    codec: Codec::Int8Quantized,
    distance: Distance::Cosine,
    hnsw: HnswParams { m: 16, ef_construction: 200, ..Default::default() },
};
store.create_table(schema)?;
```

Non-vector columns (`id`, `title`) need a column-store layer
that the MVP does not yet expose. The first slice would either
require every table to have exactly one vector column plus a
key, or layer a thin K/V-with-metadata adapter over
`VectorRow::metadata`.

## INSERT

```sql
INSERT INTO embeddings (id, title, embedding)
VALUES ('doc1', 'hello world', [0.1, 0.2, ...]);
```

Maps to:

```rust
let mut md = HashMap::new();
md.insert("title".to_string(), json!("hello world"));
store.upsert("embeddings", b"doc1".to_vec(), &vector, md)?;
```

Vector literals: parse a CQL list of FLOAT into `Vec<f32>`;
length must equal the schema dim.

## ANN SELECT

```sql
SELECT id, title FROM embeddings
ORDER BY embedding ANN OF [0.1, 0.2, ...]
LIMIT 10;
```

Maps to:

```rust
let hits = store.search("embeddings", &qvec, 10, None)?;
```

The CQL planner extracts:

1. The table name from the FROM clause.
2. The vector column from the ORDER BY expression.
3. The query vector from `ANN OF [...]`.
4. The K from `LIMIT`.
5. Result columns from the SELECT projection list.

Result rows are formatted per the `RESULT` opcode's `Rows`
flavour with the `id` and `title` columns drawn from the row
key + metadata.

## Prepared statements

`PREPARE` returns an opaque `prepared_id` and a metadata
descriptor. Implementation: hash the normalised statement,
store the parsed AST in a `HashMap<u64, PreparedStatement>`,
hand the hash back as the id. `EXECUTE` looks the AST up and
binds positional parameters before driving the engine.

## Pagination

Each `RESULT.Rows` carries an optional paging-state byte
string. Implementation: encode `(table, last_score, last_key)`
as a tuple. The next page filters the search to scores
strictly greater than `last_score` (or equal-and-key-after).
For ANN this is best-effort; full Scylla parity here would
need the index to expose a "search after this score" API.

## Authentication, TLS

Out of scope for the MVP-plus-one slice. Reuse the
`tokio-rustls` setup that `dyn-riak::proto::http` already
plumbs.

## Packaging

The CQL surface lives in `dynvecdb::cql` (or a sibling crate
`dyn-cql` if the dep graph gets heavy) and reuses
`dynvecdb::storage::VectorStore` directly. Existing tests stay
valid; new conformance tests drive the engine through a real
CQL driver (use the `cassandra-cpp-driver` crate or the
official ScyllaDB Rust driver in dev-dependencies) and assert
parity against the HTTP path's results.

## Decision points marked `// CQL future:`

Search the codebase for `CQL future:` to find every place a
CQL bridge would need to integrate. Current count:

* `crates/dynvecdb/src/api.rs` upsert -- routing decision.
* `crates/dynvecdb/src/api.rs` search -- coordinator dispatch.
