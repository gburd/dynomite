# `demo_vector_text`

<div class="dyn-hero">
An end-to-end tour of Dynomite's search: an HNSW vector index, a trigram
text index, and combined queries, all driven through the in-process
<code>FT.*</code> registry.
</div>

<p class="dyn-srclink">Source:
<code>crates/dynomite-search/examples/demo_vector_text.rs</code> --
run with <code>cargo run -p dynomite --example demo_vector_text</code></p>

## What it demonstrates

The library side of the RediSearch-compatible `FT.*` surface: creating an
index, inserting documents, and running vector-similarity, text, and
combined searches. It renders each request and its RESP reply so you can
see the wire shapes even though the calls go through the registry rather
than a socket.

```rust,no_run
use dynomite_search::ft::{self, FtOutcome, InfoValue};
use dynomite_search::registry::VectorRegistry;
use dyntext::index::TextIndex;

// FT.CREATE myidx ON HASH PREFIX 1 docs: SCHEMA ...
//   a 4-dimensional cosine-similarity HNSW vector field, plus text fields
// FT.ADD / HSET the documents
// FT.SEARCH with a KNN vector query, a text query, and a combination
```

Vectors are converted to little-endian bytes -- the exact format Redis
Stack clients send for `VECTOR` fields -- so the example doubles as
documentation of that encoding:

```rust,no_run
fn f32_to_le_bytes(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values { out.extend_from_slice(&v.to_le_bytes()); }
    out
}
```

## Design decisions and trade-offs

<dl class="dyn-facts">
<dt>Library, not wire</dt>
<dd>The example calls the <code>FT.*</code> registry directly. This is
the same API <code>dynomited</code>'s dispatcher uses once a request is
parsed, so it exercises the real search engine while staying independent
of the network stack.</dd>
<dt>HNSW for vectors</dt>
<dd>The vector field uses an HNSW index with cosine similarity. HNSW
trades a little index-build cost and memory for fast approximate
nearest-neighbor search at query time.</dd>
<dt>Trigram text index</dt>
<dd>Text search is trigram-based, which supports substring and fuzzy
matching without a full inverted-index-per-term structure.</dd>
</dl>

```admonish note title="Road not taken: a separate search service"
Dynomite embeds search in the same node that stores the data, rather
than shipping documents to an external search service (the way Riak used
Solr). Co-locating the index with the data keeps queries on the node that
already owns the key and avoids a second system to operate; the trade-off
is that the index shares the node's resources. See
<a href="../dyniak/search.md">Full-Text, Vector, and Regex Search</a>.
```

## When to use this pattern

When you want to understand or test the `FT.*` semantics -- index
creation, the vector byte encoding, query construction -- without a
running server. It is the fastest way to see what a given `FT.SEARCH`
returns.

## Where to go next

* [Tutorial: Vector, Text, and Regex Search](../tutorial-search.md) does
  the same thing over `valkey-cli` against a running `dynomited`.
* [Full-Text, Vector, and Regex Search](../dyniak/search.md) is the
  reference for the `FT.*` surface.
* [`quickstart`](./vec_quickstart.md) drops to the vector store alone.
