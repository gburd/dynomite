# dynvec

Vector storage + HNSW ANN index engine for the Dynomite Rust
port.

* Node-local persistence via Noxu DB (or an in-memory backend for
  tests / embedded use).
* Two compression codecs: per-vector int8 quantisation (default,
  4x compression, ~1% reconstruction error) and IEEE 754
  half-precision floats (2x compression, ~0.05% error).
* Three distance metrics: Euclidean, cosine, and (negated) dot
  product.
* HNSW approximate-nearest-neighbour index, hand-rolled in this
  crate -- see `docs/dynvecdb/architecture.md` for the rationale.
* Per-table `Engine` handle for embedders.

This crate is the engine layer. The primary protocol surface
lives in `dynomite::vector`, which exposes the engine through
the existing Redis listener using Redis-Stack-style RediSearch
FT.* commands. See `docs/dynvec/fold-into-redis-path.md` for the
architecture.

The standalone HTTP API is retained as a debug / inspection
facility and is gated behind the `http` feature; it is not the
primary protocol.

## Example

```bash
cargo run -p dynvec --example quickstart --features http
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
