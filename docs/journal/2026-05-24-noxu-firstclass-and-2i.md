# 2026-05-24 - NoxuDatastore as first-class backend + 2i lookup

## Summary

Two related slices land together:

1. **NoxuDatastore as a first-class `Datastore` choice** for
   operators. Selecting `data_store: noxu` (or `data_store: 2`)
   in YAML, with a `noxu_path:` directive, instructs
   `dynomited` to open an in-process Noxu DB environment at
   that path and route both the Redis-front proxy traffic and
   any configured Riak PBC / HTTP listeners through the same
   environment.
2. **2i (secondary index) lookup support** on the
   `RpbIndexReq` PBC handler when the backing datastore is
   `NoxuDatastore`. Equality and range queries both resolve to
   the keys whose stored objects carry a matching index entry.

The `dyniak` crate's `MemoryDatastore` keeps its existing
behaviour (returns a "not implemented" error frame for 2i and
list ops); the upgrade is local to the `noxu` feature path.

## Motivation

Up to this slice, `NoxuDatastore` was reachable only through
the embedding API (`ServerBuilder::datastore(...)`). Operators
running `dynomited` could not pick Noxu via YAML, so the
production path for an embedded Riak-compatible datastore did
not exist. The brief specifies both a config-level wiring and
a working 2i query against that wiring, so a complete operator
journey from `data_store: noxu` to `RpbIndexReq` was
required.

## Configuration shape

* `data_store:` accepts integer (`0`, `1`, `2`) and string
  (`redis`, `memcache`, `noxu`) forms; both round-trip through
  YAML cleanly.
* `noxu_path:` is required when `data_store: noxu` is selected
  and is ignored otherwise.
* Without `--features riak`, the configuration validator
  rejects `data_store: noxu` with
  `noxu data_store requires dynomited built with --features
  riak`. The check is implemented as a process-wide
  `set_noxu_supported(bool)` flag that `main.rs` flips on at
  startup under the `riak` feature.
* The legacy `servers:` list still applies (validation
  requires exactly one entry); under Noxu the listed address
  is a placeholder and is never contacted.

## Backend supervisor wiring

The Redis-front proxy parses an inbound `*3 SET k v` style
request and the cluster dispatcher forwards the bytes to the
local-datastore mpsc channel exactly as it does for a TCP
backend. A new `dynomited::noxu_backend::noxu_backend_supervisor`
task drains that channel, parses each request as RESP,
executes the corresponding op against the shared
`Arc<NoxuDatastore>`, encodes a RESP response, and ships it
back via the request's responder channel. Today the supported
ops are `GET`, `SET`, `DEL`, and `PING`; unknown commands
respond with a `-ERR ...\r\n` simple-error so the client gets
a structured failure rather than a hung request.

The Noxu environment is opened exactly once during
`Server::build` and shared via `Arc<NoxuDatastore>` between
the backend supervisor and any configured Riak listeners.
This avoids the `Environment locked` failure mode that arose
when both subsystems tried to open the same directory
independently.

## 2i storage shape

Three record kinds share the Noxu keyspace, distinguished by
a 2-byte tag prefix:

| Prefix | Layout                                             | Value    |
| ------ | -------------------------------------------------- | -------- |
| `K\0`  | `K\0{bucket}\0{key}`                               | object   |
| `I\0`  | `I\0{bucket}\0{name}\0<u32-be vlen>{value}{key}`   | empty    |
| `R\0`  | `R\0{bucket}\0{key}`                               | rev-list |

The fixed-width length prefix on the index value (rather than
a separator byte) is the key design decision: it keeps prefix
scans unambiguous when binary index values contain the byte we
would otherwise use as a structural separator.

* Equality query: scan with prefix
  `I\0{bucket}\0{name}\0<vlen>{value}` and read everything
  after the prefix as the original object key.
* Range query: scan with prefix `I\0{bucket}\0{name}\0`, read
  the leading `<u32-be vlen>{value}` bytes, filter against the
  caller's `[min, max]`, then read the rest as the key.

The reverse map at `R\0{bucket}\0{key}` is a length-prefixed
list of `(name, encoded_value)` pairs. On delete or
overwrite, the supervisor reads the reverse record, deletes
every forward entry it references, then deletes the reverse
record itself.

`_int` indexes encode their values as 8-byte big-endian
unsigned integers so range scans iterate in numeric order;
the encoding accepts ASCII decimal input (typical Riak client
shape) and parses it before storing. `_bin` indexes store the
value bytes verbatim.

## Wire-shape note

The upstream Riak schema nests indexes inside an `RpbContent`
message at tag 4 of `RpbPutReq`. The v0.0.1 dyniak slice
modelled the put body as a flat `value: bytes` at tag 4 (a
deviation already documented in `docs/parity.md`); to keep the
2i path usable without a wire-incompatible refactor, the
indexes list is exposed at a top-level tag (100) on
`RpbPutReq`. The follow-up slice that finally introduces
`RpbContent` will migrate this to `RpbContent.indexes` and
remove the top-level tag.

## Test coverage

* Unit tests in `crates/dyniak/src/datastore/noxu.rs` cover
  primary K/V round-trips, equality and range 2i queries,
  reverse-mapping cleanup on delete, index replacement on
  update, malformed `_int` rejection, and the bucket / index
  name validation path.
* Integration tests in `crates/dyniak/tests/noxu_pbc_round_trip.rs`
  drive PBC frames over a real TCP loopback connection
  against a Noxu environment in a tempdir, exercising both
  put-with-indexes and the resulting equality / range queries.
* The conformance suite in `crates/dynomited/tests/riak.rs`
  adds `riak_pbc_2i_against_noxu_round_trip`, gated on the
  `riak` feature, which boots a full `Server` with
  `data_store: noxu`, drives an `RpbPutReq` carrying two
  index entries, then queries via `RpbIndexReq` and asserts
  the right key comes back.
* `crates/dynomite/src/conf/pool.rs` adds tests for YAML
  round-trip of both integer and string forms, validation
  rejection without the riak feature, validation rejection
  when `noxu_path` is missing, and rejection of unknown
  string forms.

## Test totals

* Default `cargo nextest run --workspace` was 1106 before this
  slice; after, it is 1111 (added: the four pool conf tests,
  one `enums::data_store_round_trip` upgrade, and the
  workspace-default builds did not pick up dyniak's noxu
  tests). Workspace + `--features riak` runs 1167 (riak
  bucket adds 4 tests in dynomited::riak plus the 4 in
  noxu_backend); workspace + dyniak/noxu runs 302 in
  dyniak alone (240 lib + 62 integration).

## Follow-ups deliberately not landed

* `$key` reserved internal index for primary-key range
  queries.
* Streaming `RpbIndexResp` (one frame per chunk plus a
  body-less terminator) - the current handler emits a single
  frame with `done = Some(true)`.
* Per-write conditional-on-index 2i constraint enforcement -
  requires lifting Riak's `dt_only` and `if_not_modified`
  semantics through the K/V trait first.
* Full `RpbContent` message refactor on `RpbPutReq` (see
  wire-shape note).

## Files touched

* `crates/dynomite/src/conf/enums.rs`,
  `crates/dynomite/src/conf/error.rs`,
  `crates/dynomite/src/conf/mod.rs`,
  `crates/dynomite/src/conf/pool.rs`: `DataStore::Noxu`,
  `noxu_path`, validation.
* `crates/dynomite/src/cluster/pool.rs`,
  `crates/dynomite/src/net/{client,server,dnode_client}.rs`:
  pattern-matching updates for the new variant.
* `crates/dynomite/src/embed/hooks.rs`: 2i extension methods
  on the `Datastore` trait, default impl returns Unsupported.
* `crates/dyniak/src/datastore/noxu.rs`: full 2i
  implementation, `Datastore` impl wires the new trait
  methods.
* `crates/dyniak/src/proto/pb/messages.rs`: indexes field on
  `RpbPutReq` at tag 100.
* `crates/dyniak/src/server.rs`: `handle_get`, `handle_put`,
  `handle_del`, `handle_index` against the new K/V + 2i trait
  surface; legacy `handle_unsupported` retired.
* `crates/dyniak/tests/noxu_pbc_round_trip.rs`: PBC-level 2i
  integration tests.
* `crates/dynomited/Cargo.toml`: `riak` feature now also
  enables `dyniak/noxu`.
* `crates/dynomited/src/lib.rs`,
  `crates/dynomited/src/main.rs`,
  `crates/dynomited/src/server.rs`,
  `crates/dynomited/src/noxu_backend.rs`: backend supervisor
  branch, `set_noxu_supported(true)` gate flip, shared Noxu
  environment.
* `crates/dynomited/tests/riak.rs`: end-to-end Server-level 2i
  conformance test.
* `docs/book/src/operations/riak.md`: operator-facing config
  example and 2i usage.
* `docs/parity.md`: deviation entry for the 2i storage shape.
