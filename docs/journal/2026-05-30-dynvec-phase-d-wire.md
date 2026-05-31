# 2026-05-30: dynvec Phase D - wire FT.* into the live Redis dispatcher

Stage: `stage/dynvec-phase-d-wire`

## Goal

Phase C (`stage/dynvec-redis-ft-parser`) landed the FT.* parser
and executor against the in-process `VectorRegistry`, but the
plumbing was library-only: nothing in the live RESP dispatcher
recognised the keywords. Phase D wires the FT.* surface into
the dispatcher so a `redis-cli` session against `dynomited`
can drive vector indexes over the wire.

## Files touched

- `crates/dynomite/src/proto/redis/commands.rs`: lookup +
  classify rules for the FT.* surface.
  - HSET classification bumped from `Arg2` to `ArgN` so
    multi-pair `HSET key f1 v1 f2 v2` requests parse. Single-
    pair HSETs continue to parse byte-equivalently to the old
    behaviour; the change is strictly additive.
  - Added a prefix match for any `FT.*` keyword that is not
    one of the typed variants (`FT.CREATE` / `FT.SEARCH` /
    `FT.INFO` / `FT.LIST` / `FT._LIST` / `FT.DROPINDEX`):
    those stamp a generic `MsgType::ReqRedisFtUnknown` and
    flow into the dispatcher's intercept rather than failing
    the parser.
  - `classify(MsgType::ReqRedisFtUnknown)` returns `ArgN` so
    every token reaches the dispatcher.
- `crates/dynomite/src/msg/msg_type.rs`: added the
  `ReqRedisFtUnknown` variant in front of the `EndIdx`
  sentinel.
- `crates/dynomite/src/cluster/dispatch.rs`:
  - New `vector_registry: Option<Arc<VectorRegistry>>` field
    on `ClusterDispatcher`, plus `with_vector_registry()`
    setter and `vector_registry()` accessor.
  - `Dispatcher::dispatch` now runs an
    `intercept_vector_command` helper before computing the
    routing plan. The helper:
    1. Returns `DispatchOutcome::Inline` for FT.* keywords
       (calls `ft::dispatch` and wraps the RESP bytes in a
       synthesised reply Msg).
    2. For `ReqRedisFtUnknown`, recovers the original
       keyword from the request's mbuf chain and synthesises
       `-ERR not supported in this build: <keyword>\r\n`.
    3. For HSET on indexed prefixes, calls
       `ft::maybe_index_hset` and returns
       `DispatchOutcome::Error` only if the registry surfaced
       an error (vector missing / malformed); on success the
       helper returns `None` so the standard backend dispatch
       path forwards the HSET to redis.
  - Added `synthetic_redis_reply` and `first_bulk_token`
    helpers used by the intercept.
- `crates/dynomite/src/vector/registry.rs`: added a manual
  `Debug` impl on `VectorRegistry` so the dispatcher can keep
  `#[derive(Debug)]`.
- `crates/dynomite/src/embed/server.rs`:
  - `Server` and `ServerInner` carry the registry; the
    dispatcher is constructed with `with_vector_registry`.
  - `ServerHandle::vector_registry()` exposes the registry
    so embedders can introspect / mutate the in-process
    catalog without a wire round-trip.
- `crates/dynomite/src/embed/builder.rs`:
  - New `with_vector_registry(Arc<VectorRegistry>)` setter
    and `vector_registry()` accessor on `ServerBuilder`.
  - `build()` allocates a fresh `Arc<VectorRegistry>` when
    the embedder did not supply one, threading it into
    `Server::from_pool`.
- `crates/dynomited/src/server.rs`: the binary's
  `Server::build` allocates a fresh registry and attaches it
  to the dispatcher; the new `Server::vector_registry()`
  accessor exposes it. The destructure in `Server::run` was
  updated to acknowledge the new field.
- `crates/dynomited/tests/ft_wire.rs` (new): six wire tests
  driving real RESP traffic through `dynomited` against a
  spawned `redis-server`.

## Architecture decisions

1. **Where to intercept.** The brief left this open between
   `ClientHandler` (per-connection driver) and
   `ClusterDispatcher`. Putting it in the dispatcher lets the
   `embed::Server` path (which uses
   `ServerHandle::inject_request`) exercise the same intercept
   for free, keeps the FT.* concerns outside the byte-pumping
   FSM, and reuses the existing `DispatchOutcome::Inline`
   contract for synthesised replies.
2. **HSET as ArgN.** The parser's `Arg2` shape rejected
   real-world multi-pair HSETs (e.g.
   `HSET docs:1 title "x" vec <bytes>`). Bumping HSET to
   `ArgN` (which already covers the rest of the variadic
   commands) is strictly additive: every previously-valid
   HSET parse still produces the same Msg shape, and
   previously-rejected variadic HSETs now land where the
   dispatcher's intercept can see all field/value pairs.
3. **`ReqRedisFtUnknown`.** Without it, `FT.AGGREGATE` and
   friends would either trip the parser's "unknown command"
   error path (closing the connection without a reply) or
   reach the backend (which would return its own opaque
   `-ERR unknown command` message). The new variant routes
   them through the dispatcher's intercept so the brief's
   `-ERR not supported in this build` contract holds.
4. **Two-layer `vector_registry` plumbing.** `embed::Server`
   owns its registry; `dynomited::server::Server` owns its
   own. Embedders that want to share one across multiple
   embed servers do so via `ServerBuilder::with_vector_registry`;
   the binary always allocates one per process.

## Tests

- `crates/dynomite/tests/ft_redis.rs` (existing 17 tests):
  unchanged, all green.
- `crates/dynomited/tests/ft_wire.rs` (new, 6 tests):
  - `ft_create_via_redis_cli_binary_returns_ok`
  - `hset_then_ft_search_round_trip_via_wire`
  - `ft_info_via_wire_returns_array`
  - `ft_dropindex_via_wire_removes_index`
  - `ft_unsupported_command_via_wire_returns_err`
  - `ft_list_via_wire_returns_array_alias`

  Gated on the `integration` Cargo feature like the existing
  `tests/integration.rs` (the tests need `redis-server` on
  PATH). Tests use a hand-rolled minimal RESP encoder/decoder
  to avoid pulling in the heavyweight `redis` crate as a
  dev-dep.

Full workspace test count: `1839 tests run: 1839 passed, 6 skipped`
under `cargo nextest run --workspace --all-features`.

## Verification

```
cargo build -p dynomite -p dynomited --all-targets --features riak       OK
cargo nextest run -p dynomite --test ft_redis                            OK (17/17)
cargo nextest run -p dynomited --test ft_wire --features integration     OK (6/6)
cargo nextest run --workspace --all-features                             OK (1839/1839)
cargo clippy --workspace --all-targets --all-features -- -D warnings     OK
cargo fmt -p dynomite -p dynomited -- --check                            OK
cargo test --doc -p dynomite                                             OK (668/668)
```

`cargo build --locked` fails with a pre-existing Cargo.lock
drift coming from an external path-dep
(`/home/gburd/ws/lamdb/crates/noxu-db` versions bumped); the
same failure reproduces on `main` before this branch.

## New deps

None. The wire test deliberately hand-rolls a minimal RESP
codec rather than pulling in the `redis` crate (which would
have been a one-line `dev-dep` add but is heavy for what we
need - 6 commands round-tripped, no pipelining or pub-sub).

## Open questions / follow-ups

- The HSET wire reply is whatever redis-server emits (an
  integer count of newly-added fields). Real RediSearch on
  Redis-Stack also returns this reply, so the Phase D
  contract matches. If a future deviation surfaces, the
  dispatcher's intercept can synthesise its own reply
  instead of falling through.
- Multi-pair HSETs that hit a backend without the matching
  hash key still work, but the parser's previous Arg2
  classification used to hard-reject them at the wire. This
  is a behaviour change for clients that depended on the
  rejection (none known); flagged here so a future review
  notices.
- The wire tests gate on `feature = "integration"` plus a
  runtime PATH check for `redis-server`, mirroring
  `tests/integration.rs`. This means `cargo test` without
  the feature compiles them out; the conformance / CI
  pipelines that already exercise the integration feature
  will pick them up.
