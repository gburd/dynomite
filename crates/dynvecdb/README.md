# dynvecdb

Distributed vector database engine for the Dynomite Rust port.

* Node-local persistence via Noxu DB (or an in-memory backend for
  tests / embedded use).
* Two compression codecs: per-vector int8 quantisation (default,
  4x compression, ~1% reconstruction error) and IEEE 754
  half-precision floats (2x compression, ~0.05% error).
* Three distance metrics: Euclidean, cosine, and (negated) dot
  product.
* HNSW approximate-nearest-neighbour index, hand-rolled in this
  crate -- see `docs/dynvecdb/architecture.md` for the rationale.
* Distributed k-NN search via a `gen-fsm`-driven coordinator.
* HTTP API (gated on the `http` feature) for clients.

The MVP runs single-node; the pieces for cluster-wide fanout
(routing, broadcast, merge) are wired up but not exposed through
the HTTP API yet. See `docs/dynvecdb/architecture.md` for the
full picture and `docs/dynvecdb/cql-stretch.md` for the CQL
native-protocol stretch goal.

## Example

```bash
cargo run -p dynvecdb --example quickstart --features http
```

In another terminal:

```bash
curl -s -X POST localhost:21900/tables \
  -H 'content-type: application/json' \
  -d '{"name":"demo","dim":3,"codec":"int8_quantized","distance":"cosine"}'

curl -s -X POST localhost:21900/tables/demo/vectors \
  -H 'content-type: application/json' \
  -d '{"key":"hello","vector":[1.0,0.0,0.0],"metadata":{"label":"x"}}'

curl -s -X POST localhost:21900/tables/demo/search \
  -H 'content-type: application/json' \
  -d '{"vector":[0.95,0.05,0.0],"k":3}'
```
