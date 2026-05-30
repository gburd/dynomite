# 2026-05-30 dynvec Phase C: Redis FT.* parser

## Stage

dynvec Phase C: extend the Redis-protocol surface so that
RediSearch `FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.LIST`, and
`FT.DROPINDEX` (plus HSET interception against indexed
prefixes) drive the in-process `VectorRegistry` landed in
Phase B.

## Files touched

* `crates/dynomite/Cargo.toml` -- promoted `serde_json` from
  dev-deps to main deps; the FT.* HSET interception path needs
  `serde_json::Value` to construct the per-row metadata that
  `dynvec::Engine::upsert` requires. Already in the workspace
  graph (transitive via `dynvec`); the change is a manifest
  edit, not a new external crate.
* `crates/dynomite/src/msg/msg_type.rs` -- added five new Rust-
  side `MsgType` variants (`ReqRedisFtCreate`,
  `ReqRedisFtSearch`, `ReqRedisFtInfo`, `ReqRedisFtList`,
  `ReqRedisFtDropindex`) right before the `EndIdx` sentinel.
  No `REQ_*` parity index shifts; the existing `Sentinel`
  marker keeps the parity-locked variants in their original
  positions.
* `crates/dynomite/src/proto/redis/commands.rs` -- added the
  five `ft.*` keyword lookup entries plus their classifier
  arms (`FT.CREATE` / `FT.SEARCH` / `FT.DROPINDEX` ->
  `ArgN`, `FT.INFO` -> `Arg0`, `FT.LIST` -> `Argz`). Also
  accepted `ft._list` as an alias to track real RediSearch.
* `crates/dynomite/src/proto/redis/mod.rs` -- registered the
  new `ft` submodule.
* `crates/dynomite/src/proto/redis/ft.rs` (new) -- 950ish
  lines: `FtError` typed-error tree, `FtCommand` parsed
  representation, `parse_command` /  `execute` / `dispatch`
  pipeline, `maybe_index_hset` HSET interception helper, RESP2
  encoder for `FtOutcome` / `FtError`, and a small unit-test
  module covering the parsers and the LE-f32 decoder.
* `crates/dynomite/src/vector/schema.rs` -- added a
  `prefixes: Vec<Vec<u8>>` field to `VectorSchema` so the
  FT.CREATE `PREFIX` clause is captured at the protocol layer.
* `crates/dynomite/src/vector/registry.rs` -- extended
  `VectorTable` with a `record_indexed_key` /
  `indexed_keys` mailbox plus a registry-level
  `drop_with_dd` helper that returns the keys the caller
  should also remove from the underlying datastore on
  `FT.DROPINDEX <name> DD`.
* `crates/dynomite/tests/vector_registry.rs` -- updated the
  helper to populate the new `prefixes` field.
* `crates/dynomite/tests/ft_redis.rs` (new) -- 17 integration
  tests covering the brief's checklist plus a few negative
  paths (unknown index, unmatched HSET prefix, missing
  vector field, unknown FT.* keyword).

## Subset implemented

Per the brief:

* `FT.CREATE <idx> ON HASH PREFIX <n> <prefix>... SCHEMA
  ( <field> TEXT | <field> VECTOR HNSW <m> TYPE FLOAT32
    DIM <d> DISTANCE_METRIC COSINE|EUCLIDEAN|DOTPRODUCT )+`
  Other doc types and other algorithms are rejected with
  `-ERR not supported in this build`.
* `FT.SEARCH <idx> "*=>[KNN <k> @<field> $<param>]"
    PARAMS 2 <param> <bytes>` -- KNN-only, no prefilter, no
  filter expressions, no `SORTBY` (rejected explicitly), no
  `LIMIT` clause (tolerated and ignored). `RETURN` is
  parsed-and-ignored (the response always includes the doc
  id and the `__vec_score` plus every metadata field stored
  on the row).
* `FT.INFO <idx>`, `FT.LIST` (alias `FT._LIST`),
  `FT.DROPINDEX <idx> [DD]`.
* HSET interception via `maybe_index_hset`: scans the
  registry, matches the key against any registered prefix,
  decodes the vector field bytes as a stream of LE f32s,
  records every other field in the per-row metadata
  `HashMap<String, serde_json::Value>`.

## Out of scope (future phases)

Tracked in the brief: `FT.AGGREGATE`, `FT.ALTER`, `FT.CONFIG`,
`FT.EXPLAIN`, filter expressions, range queries, `SORTBY`,
multi-prefix indexes, the dispatcher integration that wires
`ft::dispatch` into the production Redis path (today the
parser recognises FT.* commands and the dispatcher's
follow-up worker will route them through `ft::dispatch`),
prepared statements, and the cluster-distributed broadcast
that the `query_fsm` coordinator already implements.

## Tests

```
cargo nextest run -p dynomite --test ft_redis
   17 passed
cargo nextest run --workspace --features dynomited/riak
 1812 passed (6 skipped)
cargo test --doc -p dynomite
   666 passed
cargo clippy --workspace --all-targets --features dynomited/riak -- -D warnings
   clean
cargo fmt --all -- --check
   clean
```

Verification scripts:

```
bash scripts/check_no_port_comments.sh   ok
bash scripts/check_ascii.sh              ok
bash scripts/check_no_todos.sh           ok
```

## Open questions

* The brief asked to "Wire the registry from
  `crate::vector::registry::VectorRegistry` into the
  dispatcher's request context. Probably an
  `Arc<VectorRegistry>` field on the dispatcher state." but
  also constrained the patch to
  `crates/dynomite/src/proto/redis/` and
  `crates/dynomite/src/vector/`. The dispatcher state lives
  in `crate::cluster::dispatch` / `crate::net::client`, both
  out of those directories. The compromise: this patch lands
  the pure FT.* surface (`ft::dispatch`,
  `ft::maybe_index_hset`) plus the Redis parser's recognition
  of `FT.*` keywords, leaving the actual `VectorRegistry`
  threading through the dispatcher to the next worker. The
  brief acknowledges this kind of cross-cut: "broadcast comes
  via the gen-fsm coordinator from Phase B's query_fsm but we
  keep it for the next worker."

* `serde_json` was already a dev-dep on `dynomite` and a
  transitive runtime dep via `dynvec`. Promoting it to a main
  dep is a manifest-only change, not a new third-party
  crate. Recorded here because the brief set "no new
  third-party deps" as a hard constraint and a reviewer
  would otherwise have to chase the diff to confirm the
  promotion is internal.

## Status

`READY_FOR_REVIEW` -- 17/17 FT.* tests green, 1812/1812
workspace tests green, clippy/fmt clean across the workspace.
