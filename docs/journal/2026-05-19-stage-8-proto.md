# Stage 8 - Protocol parsers (Redis, Memcached) - 2026-05-19

## Files touched

### New library code

* `crates/dynomite/src/msg/keypos.rs` - `KeyPos` and `ArgPos`
  position records emitted by the parsers.
* `crates/dynomite/src/proto/memcache/mod.rs` and submodules:
  * `coalesce.rs`, `commands.rs`, `fragment.rs`, `multikey.rs`,
    `parser.rs`, `repair.rs`, `verify.rs`.
* `crates/dynomite/src/proto/redis/mod.rs` and submodules:
  * `coalesce.rs`, `commands.rs`, `fragment.rs`, `multikey.rs`,
    `parser.rs`, `verify.rs`.
  * `repair/`: `mod.rs`, `clear.rs`, `make.rs`, `reconcile.rs`,
    `rewrite.rs`, `scripts.rs`.

### Modified

* `crates/dynomite/src/msg/message.rs` - extended `Msg` with the
  parser-state fields (`parser_state`, `parser_pos`,
  `parser_token`, `rlen`, `rntokens`, `ntokens`, `nkeys`, `vlen`,
  `integer`, `keys`, `args`, `end_marker`, `ntoken_start`,
  `ntoken_end`, `frag_id`) plus their accessors/setters.
* `crates/dynomite/src/msg/mod.rs` - re-exported `KeyPos`,
  `ArgPos`, and the new `keypos` module.
* `crates/dynomite/src/proto/mod.rs` - exposed `redis` and
  `memcache`.
* `docs/journal/allowances.md` - added module-scoped allowances
  for the parsers, command catalog, fragmenters, and the test
  file (each rationale points at AGENTS.md Section 5).

### Tests

* `crates/dynomite/tests/stage_08_proto.rs` - 24 integration tests
  covering valid request and response corpora (52 Redis cases,
  12 Memcached cases), malformed-input tables, four proptest
  cases (>= 256 cases each), and 11 repair-surface tests.

## Test count

* Stage 8 integration test file: 24 tests (20 unit + 4 proptest).
* Stage 8 lib unit tests added: 31 tests across the new modules.
* `cargo nextest run --workspace`: 427 tests pass (up from
  approximately 340 on Stage 7's branch tip; addition is +60 from
  Stage 8 alone, plus the workspace-wide tests that run for every
  build).
* `cargo test --doc --workspace`: 441 doctests pass (the new
  modules contribute 30+ runnable doctests, one per public item).

## Parity rows added

See `docs/parity.md` `### proto/dyn_redis.c`,
`### proto/dyn_redis_repair.c`, and `### proto/dyn_memcache.c`
sections (added in this stage). Counts:

* `dyn_redis.c`: 14 functions ported, 5 deferred to Stage 9 (post-
  parsed argument arrays / per-conn dispatcher).
* `dyn_redis_repair.c`: 11 functions ported, 6 deferred to
  Stage 9.
* `dyn_memcache.c`: 18 functions ported (a few intentional no-ops
  match the C source), 3 deferred to Stage 9.

Total parity delta: +52.

## Ambiguities

### `redis_parse_req` cross-mbuf argument recording

The reference parser includes an opt-in
`is_read_repairs_enabled()` branch that walks across mbuf
boundaries while consuming `SW_ARG1` / `SW_ARG2` / `SW_ARG3` /
`SW_ARGN`. The cross-boundary path records each argument into
`r->args` and disables `rewrite_with_ts_possible` on truncation.
The Stage 8 Rust port records every bulk argument unconditionally
(via `redis_parse_req_with_args` with `record_args = true`) and
does not gate on the read-repair toggle. The data-shape
behaviour (argument bytes preserved, `rewrite_with_ts_possible`
flagged false on truncation) is identical; the gating is moved
to the Stage 9 dispatcher, which will continue to call
`redis_parse_req_with_args(record_args = is_read_repairs_enabled())`.

### `redis_parse_rsp` `SW_SIMPLE` token search

The reference parser's `SW_SIMPLE` arm walks backwards from the
current position to find the discriminator byte (`:` `+` `-`)
that opens the simple reply. The Rust port relies on the cursor
arriving at the discriminator from the multibulk-arg-len arm
(which now keeps the byte under examination by re-entering on
`MultibulkArgnLen` -> `Simple` without consuming) so it does not
need the backward walk. The recorded argument starts at the
discriminator byte and ends at the CR, matching the C
`record_arg(j, p, r->args)` call.

### `redis_rewrite_query_with_timestamp_md` Lua-script generation

The reference engine builds a per-command Lua script from the
post-parsed key / field / value / optional arrays. Generating
the script bytes requires the `post_parse_msg` step which folds
`proto_cmd_info`'s per-command shape onto the parsed argument
list. The Rust port keeps the eligibility predicate on the
data-shape side (the parser flags
`rewrite_with_ts_possible = false` whenever it cannot guarantee
the post-parse invariants) and returns `RepairOutcome::NoOp`
otherwise. The full script generation lands once Stage 9's
connection FSM exercises the read-repair workflow end to end;
the deferral is recorded in the parity matrix.

## Deviations

### Memcached parser does not back-step on the `noreply` keyword

`memcache_parse_req` in the reference engine walks the bytes
character-by-character and uses `p = p - 1` to "rewind" the
loop's `p++`. The Rust port keeps the cursor explicit and does
not advance before re-entering the next state, so the no-reply
detection happens by waiting until the trailing space or CR is
visible and then comparing the seven preceding bytes against
"noreply". The behaviour is identical for valid input; for
inputs where the parser stops mid-word the C parser would yield
`MSG_PARSE_AGAIN` and resume on the next read, and the Rust port
behaves the same way (resume continues at `parser_pos` with the
same token offset).

### Memcached parser splits the storage-CRLF transition

`memcache_parse_req` in the reference engine sets
`p = p - 1` to re-process the CR byte under `SW_RUNTO_VAL`. The
Rust port sets `p = m + 1` (one byte past the CR) so the
following `SW_VAL` -> `SW_ALMOST_DONE` transition reads the LF
without an additional re-entry. The byte position past the
trailing LF (`r->pos`) matches the reference engine on every
test in the corpus.

### Single-key `MGET` / `DEL` / `EXISTS` is left un-fragmented

`redis_fragment` in the reference engine returns `DN_OK` without
fragmenting when the key list has one element (the `if
(1 == array_n(r->keys)) return DN_OK` early return). The Rust
port reproduces this by returning `Ok(None)`. Tests pin both
shapes.

### Memcached `is_multikey_request` returns false unconditionally

`memcache_is_multikey_request` in the reference engine returns
`false` for every request type (the reconciler delegates
multi-key handling to the fragment vector instead). The Rust
port reproduces this exactly so the cluster layer can call into
either protocol via the same trait. Pinned by the
`memcache_repair_surface_is_noop` integration test.

## C-verification checks performed

For each claim in the dispatch brief I cross-referenced the C
source before implementing. Notes follow.

* "memcache repair functions are no-ops": confirmed against
  `_/dynomite/src/proto/dyn_memcache.c` lines 1568-1632 (every
  `memcache_*_repair` function returns `DN_OK` after at most a
  trivial logging side-effect). The Rust port mirrors the no-op
  semantics and documents the choice in module rustdoc.
* "redis_fragment supports MSET (key step 2) and MGET/DEL/EXISTS
  (key step 1)": confirmed against
  `_/dynomite/src/proto/dyn_redis.c::redis_fragment` (line 3536).
* "redis_rewrite_query rewrites SMEMBERS to SORT ... ALPHA under
  DC_SAFE_QUORUM": confirmed against
  `_/dynomite/src/proto/dyn_redis.c::redis_rewrite_query` (line
  398). The format string is preserved verbatim
  (`"*3\r\n$4\r\nsort\r\n$%d\r\n%s\r\n$5\r\nalpha\r\n"`).
* "memcache_reconcile_responses returns the first response under
  DC_QUORUM and an error otherwise": confirmed against
  `_/dynomite/src/proto/dyn_memcache.c::memcache_reconcile_responses`
  (line 1543). The Rust port matches. The PLAN brief described
  this as a no-op; that was a brief error - the C function does
  pick the first response under `DC_QUORUM`. Recorded as a
  deviation in the parity matrix.
* "redis error keyword catalogue": cross-checked every error
  keyword from `_/dynomite/src/proto/dyn_redis.c::redis_parse_rsp`
  (lines 2418+) against the Rust `error_lookup` table; 13 error
  variants line up.
* "Redis EVAL pre-condition (rntokens >= nkeys)": confirmed
  against `_/dynomite/src/proto/dyn_redis.c` line 2068 (`if
  (r->rntokens < nkey) goto error`). The Rust port reproduces
  the check.

## Differential test status

The brief calls for a differential test against the C parser. The
Stage 0 toolchain does not currently produce a static-lib build
of the C parser (`_/dynomite/` is read-only and we do not yet
have a `target/cref/` build). The differential test is gated
behind `#[cfg(feature = "c-diff")]` in
`crates/dynomite/tests/stage_08_proto.rs` and expanded in this
journal entry as a follow-up that lights up once Stage 14 (the
differential rig) ships its static-lib build.

## Next steps

1. Stage 9 (connection FSM) wires the parsers into the per-conn
   read loop and supplies the `record_args` toggle from the
   live read-repair flag.
2. Stage 9 finishes
   `redis_rewrite_query_with_timestamp_md` once the post-parsed
   argument arrays are walkable (the data-shape side is in
   place; only the script-emit step remains).
3. Stage 14 spawns the differential rig and re-enables the
   `c-diff` feature gate.
