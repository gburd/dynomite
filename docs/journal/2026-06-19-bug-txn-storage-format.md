# BUG: transaction-written values are stored unwrapped and unreadable via HTTP GET

Found 2026-06-19 by the consistency-verification harness (workstream
W1) on its first run against a healthy single-node dyniak instance,
no faults injected.

## Symptom

A value written via `POST /buckets/<b>/transactions` (or
`POST /transactions`) cannot be read back via
`GET /buckets/<b>/keys/<k>`: the GET returns

    HTTP 500: stored object is corrupt: failed to decode Protobuf
    message: buffer underflow

The W1 history workload, which does a client-side read-modify-write
(GET each key, append, PUT the batch atomically), saw ~99.96% of its
transactions land in `:info` because every GET read 500'd.

## Root cause

dyniak has three write paths that disagree on the on-disk value
format:

- HTTP object PUT (`put_object_into_store`, routes.rs): wraps the
  value in an `HttpObject` protobuf envelope via
  `HttpObject::to_storage_bytes()` before storing. (object.rs)
- PBC `RpbPut` (server.rs): same -- persists the canonical
  `HttpObject` storage form.
- HTTP transaction PUT (`TxnOp::Put` -> `NoxuDatastore::put_object`
  -> `put_object_in`, noxu.rs): stores the **raw value bytes**, with
  NO `HttpObject` wrapping.

Both READ paths expect the envelope:

- HTTP GET (`get_object_from_store`, routes.rs:788):
  `HttpObject::from_storage_bytes(&stored)` -- on raw bytes this
  fails with "buffer underflow" and returns 500. No fallback.
- PBC GET (`pbc_content_from_storage`, server.rs:620): also decodes
  `HttpObject`, but FALLS BACK to treating undecodable bytes as a raw
  value, so PBC GET of a transaction-written value returns the raw
  bytes (no crash, but the 2i / content-type / links metadata the
  envelope carries is absent).

So:
- txn PUT  -> HTTP GET  : 500 (crash).
- txn PUT  -> PBC GET   : returns raw bytes (lossy: no envelope
  metadata, but does not crash).
- HTTP PUT -> txn read inside a batch (`tx.get_object`): returns the
  envelope protobuf bytes, not the value -- the inverse mismatch.

The transaction path is the odd one out: it is the only writer that
does not wrap, and HTTP GET is the only reader with no raw fallback.

## Impact

Transactionally-written objects are not interoperable with the
object API. A customer using `POST /transactions` for multi-key
atomic writes and `GET /buckets/.../keys/...` (or any HTTP read) to
read them gets a hard 500. This is a correctness/interop defect
between two advertised storage methods, present in the published
1.0.0 / 1.1.0 line.

## Fix direction

The transaction write path should wrap values in the same
`HttpObject` envelope the HTTP and PBC put paths use, so all three
writers and both readers agree on one storage format. The fix lands
in the transaction lowering (where `HttpTxnOp::Put` becomes a
`TxnOp::Put`, or in `put_object_in`'s caller for the txn path) -- NOT
in `put_object_in` itself, because the auto-commit `riak_put` path
already wraps upstream in server.rs / routes.rs before calling
`put_object`. Care: the auto-commit `put_object` is called with
already-wrapped bytes from the HTTP/PBC put handlers, so the wrapping
must happen at the txn lowering layer, mirroring exactly what the
non-txn put handlers do (parse the op value, build an `HttpObject`,
`to_storage_bytes()`), and the txn-internal `tx.get_object` used for
read-modify-write must `from_storage_bytes()` symmetrically.

A regression test must cover: txn PUT then HTTP GET round-trips the
value; txn PUT then PBC GET round-trips; HTTP PUT then txn read
inside a batch sees the value; and the W1 history workload runs at a
~100% commit rate against a healthy node.

## Provenance

This is exactly the class of bug the consistency-verification
initiative exists to catch. The chaos rig never caught it because it
drives valkey/memcache/PBC workloads, and PBC GET's raw fallback
masks the mismatch; the HTTP transaction + HTTP GET combination was
not exercised end to end until W1.
