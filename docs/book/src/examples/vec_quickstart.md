# `quickstart` (dynvec)

<div class="dyn-hero">
The vector store on its own: bring up a single-node <code>dynvec</code>
HTTP server, populate it with a few vectors, and run a similarity search.
</div>

<p class="dyn-srclink">Source:
<code>crates/dynomite-vec/examples/quickstart.rs</code> --
run with
<code>cargo run -p dynvec --example quickstart --features http</code></p>

## What it demonstrates

`dynvec`, the vector engine that backs Dynomite's `VECTOR` search fields,
used directly and standalone. It creates an in-memory store with one
table, seeds a handful of labeled vectors, and serves an HTTP API you can
query.

```rust,no_run
use dynvec::api::serve;
use dynvec::distance::Distance;
use dynvec::encoding::Codec;
use dynvec::index::HnswParams;
use dynvec::storage::{IndexAlgorithm, TableSchema, VectorStore};

let store = Arc::new(VectorStore::in_memory());
store.create_table(TableSchema {
    name: "demo".to_string(),
    dim: 3,
    codec: Codec::Int8Quantized,
    distance: Distance::Cosine,
    hnsw: HnswParams::default(),
    algorithm: IndexAlgorithm::Hnsw,
})?;
```

It listens on `127.0.0.1:21900` by default (override with
`DYNVEC_LISTEN`).

## Design decisions and trade-offs

<dl class="dyn-facts">
<dt>In-memory store</dt>
<dd><code>VectorStore::in_memory()</code> keeps the example dependency-free.
The same API backs a persistent store when you want durability.</dd>
<dt>Int8 quantized codec</dt>
<dd>The table uses <code>Codec::Int8Quantized</code>, which shrinks each
vector roughly four-fold versus raw <code>f32</code> at a small recall
cost. Choosing the codec at table-creation time is a deliberate
space/accuracy trade-off exposed to the caller.</dd>
<dt>Cosine distance + HNSW</dt>
<dd>Cosine similarity over an HNSW index -- the standard combination for
semantic-similarity search. The <code>Distance</code> and
<code>IndexAlgorithm</code> are per-table so different tables can make
different choices.</dd>
<dt>HTTP surface</dt>
<dd>The <code>http</code> feature exposes a REST API, which is why the run
command passes <code>--features http</code>. Without it, the store is a
library only.</dd>
</dl>

```admonish note title="Road not taken: one global vector configuration"
Distance metric, codec, and index algorithm are chosen per table rather
than globally. A store commonly holds vectors with different
dimensionalities and accuracy needs; forcing one setting on all of them
would push the trade-off onto the wrong layer. The cost is a slightly
larger table schema; the benefit is that each table is tuned to its data.
```

## When to use this pattern

When you want the vector engine without the rest of Dynomite -- to
prototype an embedding search, to benchmark a codec/distance combination,
or to understand what the `VECTOR` fields in
[`demo_vector_text`](./demo_vector_text.md) sit on top of.

## Where to go next

* [`demo_vector_text`](./demo_vector_text.md) shows the same vector engine
  reached through the `FT.*` search surface.
* [Full-Text, Vector, and Regex Search](../dyniak/search.md) is the
  reference for search across Dynomite.
