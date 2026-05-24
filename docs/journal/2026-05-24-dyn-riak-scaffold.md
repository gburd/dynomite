# 2026-05-24 -- dyn-riak crate scaffold (v0.0.1)

Branch: `stage/dyn-riak-scaffold`
Commit base: `97e9c9d` (main: "merge: stage/encoding-scaffold into main")

## What landed

The first usable slice of `crates/dyn-riak`. The crate is the home for
the Riak-compatible protocol surface called out in
`docs/riak-compat-plan.md` and the architectural follow-up in
`docs/riak-comparison.md`. Operator direction (recorded in the brief
that opened this stage) moved the protocol code and `NoxuDatastore`
out of `crate::dynomite` and into a dedicated workspace member so the
dynomite engine remains a pure substrate.

The slice ships:

* `crates/dyn-riak/Cargo.toml` -- workspace member, version 0.0.1.
  Depends on `dynomite`, `dyn-encoding`, `prost`, `bytes`,
  `serde`, `serde_json`, `thiserror`, `tracing`, and `tokio`. The
  `noxu` feature (off by default) pulls in `noxu-db`.
* `src/lib.rs`, `README.md` -- public-surface map and crate-role doc.
* `src/error.rs` -- `RiakError` covering framing, decode, encode,
  unknown-message-code, and downstream `DatastoreError` paths.
* `src/proto/pb/framer.rs` -- 4-byte-length + 1-byte-code framer
  with a `MAX_FRAME_LEN = 16 MiB` cap (matches Riak's reference
  framer). Generic over `AsyncRead` / `AsyncWrite`; tests drive it
  with `tokio::io::duplex`.
* `src/proto/pb/messages.rs` -- hand-derived `prost::Message` structs
  for `RpbErrorResp`, `RpbPingReq` / `RpbPingResp`,
  `RpbGetReq` / `RpbGetResp`, `RpbPutReq` / `RpbPutResp`, and
  `RpbDelReq`, plus a `MessageCode` enum that round-trips with the
  raw byte. Field tags match Riak KV 2.2.3's published schema
  byte-for-byte.
* `src/proto/pb/codec.rs` -- `codec_registry()` builds a
  `dyn_encoding::CodecRegistry` populated with a `ProtobufCodec`
  registered for every v0.0.1 message type. The PBC content-type is
  hard-coded to `"application/x-protobuf"`; the registry shape is
  reused by the upcoming HTTP gateway for content-type negotiation.
* `src/server.rs` -- `serve_pbc(listener, datastore)` accept loop
  plus a generic `handle_conn(stream, datastore)` per-connection
  driver. Dispatches to `dynomite::embed::Datastore::dispatch`.
* `src/datastore/noxu.rs` -- `NoxuDatastore` that opens a Noxu
  environment in a directory and exposes `get` / `put` / `delete`.
  Implements `dynomite::embed::Datastore` so it can be installed
  into a `dynomite::embed::Server`. Gated behind the `noxu` feature.
* `tests/pbc_round_trip.rs` -- end-to-end Ping / Put / Get / Del
  round-trip over a real `tokio::net::TcpListener` against an
  in-memory `MemoryDatastore`.

## Design choices

### Embedded vs sidecar

The default deployment posture is **in-process**: `dynomited` will
(in a follow-up slice) link `dyn-riak` behind a `--features riak`
switch and instantiate the Riak listener alongside the existing
Redis/Memcached listeners. Trade-offs considered:

* In-process (chosen): one fewer socket hop; one fewer process the
  operator has to start, monitor, and supervise; one fewer point at
  which a serde-mismatch can corrupt the wire.
* Sidecar: process isolation; independently restartable; matches
  the existing `backend_supervisor` shape used by the Redis path.
  Reachable via the existing supervisor without further crate work,
  so sidecar-mode operators are not blocked by this choice.

The embed path is the v0.0.1 default; sidecar mode is documented
support that requires no additional code in this crate.

### Where Riak code lives

Per operator direction, the brief explicitly moves the Riak protocol
modules out of `crate::dynomite` and into `crates/dyn-riak/`. The
`dynomite` engine stays a pure substrate that knows nothing about
Riak. Cross-cutting hooks already exposed by `dynomite::embed`
(`Datastore`, `Protocol::Custom`, `Server`, `ServerBuilder`) are
sufficient for this slice; no `crate::dynomite` changes were
required.

### Datastore trait reuse vs Riak-specific trait

The brief asked the PBC server to dispatch to
`dynomite::embed::Datastore`. That trait is shaped around the
substrate's internal `Msg` flow rather than Riak-style
(bucket, key, value) operations. For the v0.0.1 slice the server
trampolines K/V requests through `Datastore::dispatch` -- enough
to fire the substrate's accounting -- and replies with
empty Riak responses. The `NoxuDatastore` exposes its real
`get` / `put` / `delete` semantics through inherent methods that
the next slice will invoke directly via a richer Riak K/V trait.
This keeps the slice useful (full wire-protocol round-trip,
encode/decode tested end-to-end) without forcing the
`Datastore` trait to grow Riak-specific methods that would
constrain the abstraction.

### Operations in v0.0.1

The four operations cover the minimum useful Riak API:

| Code | Direction | Purpose |
|---|---|---|
| 1   | C->S | `RpbPingReq` |
| 2   | S->C | `RpbPingResp` |
| 9   | C->S | `RpbGetReq` |
| 10  | S->C | `RpbGetResp` |
| 11  | C->S | `RpbPutReq` |
| 12  | S->C | `RpbPutResp` |
| 13  | C->S | `RpbDelReq` |
| 14  | S->C | `RpbDelResp` |
| 0   | S->C | `RpbErrorResp` (returned when an inbound frame uses a response code) |

### Encoding plumbing

The PBC path goes through `dyn_encoding::CodecRegistry`. A single
`ProtobufCodec` is built once via `codec_registry()` and stocked with
every v0.0.1 message type. The HTTP gateway (next slice) registers
JSON and CBOR codecs alongside it under the same registry; the per-
connection content-type negotiation flow is intentionally identical
across PBC (which always uses `application/x-protobuf`) and HTTP
(which negotiates).

### Noxu feature gate

`noxu` is off by default. The crate compiles cleanly without the
`lamdb` checkout next door. Enabling the feature pulls in `noxu-db`
through the workspace dep entries the previous Cargo plumbing stage
added (`docs/riak-compat-plan.md` Section 6). The `NoxuDatastore`
unit tests exercise an in-environment `tempfile::TempDir`, so they
run against a real Noxu environment but leave nothing on disk.

## Verification

* `cargo build --workspace --all-targets --locked` -- clean.
* `cargo build --workspace --features dyn-riak/noxu --all-targets --locked` -- clean.
* `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -p dyn-riak -- --check` -- clean.
* `cargo clippy --workspace --all-targets --all-features -- -D warnings` -- clean.
* `cargo nextest run --workspace` (no-fail-fast): 749 -> 774
  (+25 tests). With `--all-features`: 782 (+33 over baseline).
* `cargo test --doc --workspace`: 602 -> 608 (+6 doctests on
  `Frame`, `MessageCode`, `RpbPingReq`, `codec_registry`,
  `serve_pbc`, `NoxuDatastore::open`).
* `bash scripts/check_no_todos.sh` -- clean.
* `bash scripts/check_no_port_comments.sh` -- clean.
* `bash scripts/check_ascii.sh` -- clean.

## Deferred (next slices, not this one)

* HTTP gateway with content-type negotiation through `dyn-encoding`.
* The remainder of the Riak PBC operation surface (list-keys,
  list-buckets, bucket-props, 2i, MapReduce, CRDT data types,
  STARTTLS, auth, server-info, coverage).
* CRDT semantics (counters, sets, maps, registers, HLL).
* MapReduce pipes.
* Tictac AAE.
* Wiring `dyn-riak` into `dynomited` behind a `--features riak`
  switch. The crate is intentionally not yet a default workspace
  dep of `dynomited`; operators opt in.
* Full `RiakObject` schema (vector clocks, siblings, content-type,
  user metadata, indexes). The v0.0.1 `RpbGetResp.content` /
  `RpbPutResp.content` fields are opaque bytes pending that work.
* A Riak-aware K/V trait that the protocol server dispatches into
  end-to-end (replacing the current `Datastore::dispatch`
  trampoline).

## Open follow-ups for the lead

* `docs/riak-compat-plan.md` Section 1 still lists the protocol
  modules as living inside `crate::dynomite`. Section 14 (a small
  pointer note) was added in this commit; the older sections should
  be reviewed and updated when the next dyn-riak slice lands.
* No `dynomite::embed::Datastore` extension was made. If a future
  slice wants Riak K/V methods on the trait itself, that requires a
  journal entry under `api-change:` per AGENTS.md Section 13.
