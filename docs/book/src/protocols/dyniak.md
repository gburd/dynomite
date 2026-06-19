# Dyniak (Riak PBC / HTTP)

The `dyniak` data store is the built-in, Riak-compatible backend. It
is gated behind the `riak` Cargo feature and serves two client wire
surfaces against the same in-process, transactional Noxu environment:

* **Riak Protocol Buffers Client (PBC)** -- the binary protocol, with
  `[4-byte BE length][1-byte msg-code][prost body]` framing.
* **HTTP gateway** -- the same operations over `GET` / `PUT` / `POST`
  / `DELETE`, negotiating `application/x-protobuf`, `application/json`,
  or `application/cbor` per request.

The implementation lives in the
[`dyniak`](https://crates.io/crates/dyniak) crate. For the operator
walk-through (building, listeners, AAE, 2i storage layout, bucket
properties) see [Riak mode](../operations/riak.md). This page is the
protocol-surface summary.

## PBC message surface

Ping, ServerInfo, Get, Put, Del, GetBucket, SetBucket, ListBuckets,
ListKeys, Index (2i), and MapRed, plus the Dynomite cluster-admin
extensions (`DynListPeers`, `DynClusterJoin` / `Leave` / `Plan` /
`Commit`, `DynAaeStatus`). ListBuckets and ListKeys are chunked into a
multi-frame stream.

## HTTP routes

| Method | Path | Operation |
| --- | --- | --- |
| `GET`/`HEAD` | `/ping` | Liveness probe. |
| `GET` | `/stats` | Server stats. |
| `GET` | `/buckets?buckets=true` | List buckets. |
| `GET`/`HEAD` | `/buckets/{b}/keys/{k}` | Fetch an object. |
| `PUT` | `/buckets/{b}/keys/{k}` | Store an object. |
| `POST` | `/buckets/{b}/keys/{k}` | Store an object. |
| `DELETE` | `/buckets/{b}/keys/{k}` | Delete an object. |
| `GET` | `/buckets/{b}/keys?keys=true` | List keys. |
| `GET` | `/buckets/{b}/props` | Get bucket properties. |
| `PUT` | `/buckets/{b}/props` | Set bucket properties. |
| `POST` | `/mapred` | Submit a MapReduce job. |
| `POST` | `/transactions` | Cluster-wide multi-key transaction. |
| `POST` | `/buckets/{b}/transactions` | Bucket-scoped transaction. |

The `search` feature adds index-management and search routes under
`/buckets/{b}/index/...` and `/buckets/{b}/search/...`; without the
feature (or without a registry wired in) those routes reply `501 Not
Implemented`.

## Object links

An object carries typed `links`: pointers to other objects, each a
`(bucket, key, tag)` triple. Over HTTP they ride in the `Link` header;
over PBC they are `RpbLink` entries inside `RpbContent`. Links are not
walked by a dedicated route -- they are traversed by a MapReduce link
phase (see below).

## Secondary indexes (2i)

Objects can carry secondary-index entries (integer `*_int` and binary
`*_bin`). The PBC `RpbIndexReq` handler answers equality and range
queries. The storage layout and a client example are documented in
[Riak mode](../operations/riak.md#secondary-indexes-2i).

## MapReduce

A MapReduce job is a pipeline of phases submitted over `POST /mapred`
(HTTP) or `RpbMapRedReq` (PBC). Phase kinds:

* `Map` / `Reduce` -- named functions resolved through the phase
  registry.
* `Link` -- follows object links, optionally filtered by `bucket` and
  `tag`; this is how link-walking is expressed.
* `WasmModule` -- invokes a registered Wasm module as a map or reduce
  phase. Available only when the binary is built with the `wasm`
  feature and the module is registered (via `riak.wasm_modules:` or at
  runtime); without it, a `WasmModule` phase returns a
  `WasmNotImplemented` error.

## FT.* search

When built with the `search` feature, the dyniak HTTP gateway exposes
per-bucket text (substring + approximate-regex) and vector-KNN index
management and search. The same `FT.*` surface is available on the
RESP plane; see [Valkey (RESP)](./redis.md#search-extension-ft) and
the [search tutorial](../tutorial-search.md).

## Transactions and causality

`dyniak` extends Riak's per-key model with atomic multi-key
transactions and tracks per-key causality with an Interval Tree Clock.
See [Dyniak features](../operations/dyniak-features.md) for cross-node
XA transactions and the custom Wasm keyfun, and
[Riak mode](../operations/riak.md#causality-tracking) for the ITC
context blob.
