# dyn-riak

`dyn-riak` is the Riak-compatible protocol layer for the Dynomite Rust
port. It implements the wire formats Riak KV exposes (Protocol Buffers
on TCP and, in a follow-up slice, JSON over HTTP) and bridges them to
the Dynomite engine through `dynomite::embed::Datastore`.

## Where it sits

Dynomite is the cluster substrate: hashing, gossip, vnodes, quorum,
hinted handoff, read repair. `dyn-riak` is one of the protocol layers
operators can put in front of it. Today the substrate already ships
the Redis and Memcached transparent-proxy paths; this crate adds a
third surface that speaks the Riak client APIs.

The crate is **embedded** by default: `dynomited` links `dyn-riak`
behind a (future) `--features riak` switch and instantiates the Riak
listener in-process, alongside the existing Redis/Memcached listeners.
Operators who want process isolation can also reach the same surface
through the workspace's `backend_supervisor` shape, but the v1 path is
in-process for one fewer socket hop and a simpler operational story.

## What's in v0.0.1

This is the first usable slice. The crate ships:

* The Riak Protocol Buffers framing (4-byte big-endian length, 1-byte
  message code, protobuf body).
* Four PBC operations: `RpbPing`, `RpbGetReq` / `RpbGetResp`,
  `RpbPutReq` / `RpbPutResp`, `RpbDelReq`.
* A `serve_pbc` connection driver that reads framed PBC requests from
  a `tokio::net::TcpListener`, dispatches them through any
  `dynomite::embed::Datastore`, and writes framed responses.
* A `NoxuDatastore` impl, gated behind the `noxu` Cargo feature, that
  bridges to the in-process Noxu DB storage engine via the
  `noxu-db` crate.
* End-to-end PBC round-trip integration tests.

## What's deferred

The full Riak surface is a multi-month track. The following are
deliberately out of this slice and tracked in `docs/riak-compat-plan.md`:

* HTTP gateway with content-type negotiation through `dyn-encoding`
  (next slice). PBC v0.0.1 is hard-coded to `application/x-protobuf`.
* The remainder of the Riak PBC operation set (list-keys,
  list-buckets, bucket-props, 2i, MapReduce, Search, CRDT data types,
  STARTTLS, auth).
* CRDT semantics (counters, sets, maps, registers, HLL).
* MapReduce pipes.
* Tictac AAE.
* Wiring `dyn-riak` into `dynomited` behind a `riak` feature flag.

## Cargo features

| Feature | Default | Purpose |
|---|---|---|
| (none)  | n/a     | Pure protocol crate; no storage backend. |
| `noxu`  | off     | Enables `NoxuDatastore`; pulls in `noxu-db` from `../lamdb`. |

## Acknowledgement

The Riak wire format and message-code numbering come from Basho's
Riak KV (`riak_pb`). The codebase here is original Rust; only the
external wire shape is shared.
