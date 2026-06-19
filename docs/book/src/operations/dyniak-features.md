# Dyniak features

The built-in `dyniak` store (the `riak` Cargo feature) ships a few
capabilities beyond a stock Riak deployment. This page documents the
ones that need operator attention. The protocol surface is summarised
in [Dyniak (Riak PBC / HTTP)](../protocols/dyniak.md); the listener
and storage operations are in [Riak mode](./riak.md).

## Cross-node XA transactions

`dyniak` extends Riak's per-key, eventually-consistent model with
atomic multi-key transactions: a client groups several put and delete
operations into one batch over `POST /transactions` (cluster-wide) or
`POST /buckets/{bucket}/transactions` (bucket-scoped), and the backend
applies all of them or none of them.

Two layers stack:

* **Single-environment:** every op in the batch routes to one node's
  storage engine and commits in a single engine transaction.
* **Cross-node:** when the batch touches keys owned by different
  primary nodes, the operations are coordinated with X/Open XA
  two-phase commit. Each node prepares its branch; the coordinator
  commits all branches only once every prepare has voted to commit. A
  branch that performed no writes votes read-only and skips the second
  phase. Cross-node coordination travels over the DNODE peer plane.

The cross-node coordinator handles the network failure modes that a
single-process commit never sees: presumed-abort on prepare,
commit-in-doubt forward recovery, and a durable in-doubt log that a
cold restart re-drives so a branch that voted to commit is never left
dangling.

A transaction whose `force_abort` flag is set rolls back every
prepared branch regardless of votes.

## Custom Wasm keyfun (`chash_keyfun: CUSTOM`)

By default a key is routed by hashing `<bucket>/<key>` (`STD`), and a
bucket property can switch a bucket to hash `<bucket>` only
(`BUCKETONLY`); see
[bucket properties](./riak.md#bucket-properties). A third option,
`CUSTOM`, routes through an operator-supplied Wasm module instead of a
fixed rule.

**Scoped limitation:** `CUSTOM` is only usable once a Wasm keyfun
module is registered. A bucket whose `chash_keyfun` is `CUSTOM` but
which has no module registered cannot be routed, and the engine
returns a typed error rather than guessing. The module must export a
`keyfun_route` entry point. Registration reuses the same Wasm module
store, compilation cache, and resource limits as the MapReduce
executor.

## Link-walking

Objects carry typed `links` -- `(bucket, key, tag)` pointers to other
objects. There is no dedicated link-walk route; links are traversed by
a MapReduce `Link` phase, optionally filtered by `bucket` and `tag`.
Chain a `Link` phase into a MapReduce pipeline (`POST /mapred` or
`RpbMapRedReq`) to follow links and feed the resolved objects into the
next phase.

## Wasm MapReduce phases

A MapReduce pipeline can include a `WasmModule` phase that runs a
registered Wasm module as a map or reduce step. This requires the
binary to be built with the `wasm` feature, and the module to be
registered -- either at startup via `riak.wasm_modules:` (a list of
`{id, path}` entries pointing at `.wasm` or `.wat` files) or at
runtime. Without the `wasm` feature the phase is parsed and validated
but a submission returns a `WasmNotImplemented` error.
