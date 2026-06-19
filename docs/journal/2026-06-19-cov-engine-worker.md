# Stage cov-engine: raise protocol + cluster + io core coverage

Date: 2026-06-19
Branch: `stage/cov-engine`
Agent: coverage worker

## Goal

Raise measured line/region coverage of the engine protocol core
(`proto::redis`, `proto::memcache`, `proto::dnode`) plus the
`stats` and `util` support modules toward the 95% production bar,
and add `hegel` property tests where they close real invariant
gaps. Tests only; no production logic was changed.

## Before / after (cargo llvm-cov, line% and region%)

| File | Before line | After line | After region |
|---|---|---|---|
| proto/redis/parser.rs | 54.91% | 89.19% | 90.71% |
| proto/redis/commands.rs | 51.01% | 100.00% | 100.00% |
| proto/redis/coalesce.rs | 88.57% | 91.84% | 93.98% |
| proto/redis/fragment.rs | 89.72% | 95.83% | 95.76% |
| proto/redis/repair/make.rs | 25.00% | 100.00% | 100.00% |
| proto/redis/repair/reconcile.rs | 55.00% | 100.00% | 100.00% |
| proto/memcache/parser.rs | 63.29% | 87.51% | 92.23% |
| proto/dnode.rs | 85.43% | 95.59% | 96.48% |
| stats/mod.rs | 65.81% | 98.97% | 98.81% |
| util/rbtree.rs | 81.36% | 100.00% | 100.00% |
| util/dict.rs | 88.37% | 100.00% | 100.00% |
| util/sockinfo.rs | 79.31% | 90.80% (all lines covered) | 97.96% |

Coverage was measured with
`cargo llvm-cov -p dynomite-engine --summary-only
--ignore-filename-regex 'tests?/|fuzz/|benches/'`. The engine
crate (`dynomite-engine`) has no `riak` feature; that feature
lives on `dynomited`/`dyniak-bench`. The brief's `--features
riak` is a workspace-level invocation; for the engine crate the
flag is omitted (and `cargo build -p dynomite-engine --features
riak` errors with "does not contain this feature").

## Tests added (new files, no existing test reordered)

- `tests/proto_redis_commands_coverage.rs` (15 tests): every
  catalog keyword resolves and classifies; case-insensitivity;
  every CommandClass arm; routing overrides; FT.* unknown
  fallthrough; length-bound rejection; error_lookup table.
- `tests/proto_redis_parser_coverage.rs` (47 tests): every
  command-class arg shape (Arg0/1/2/3/N/X/KvX/Upto1/Eval/Argz),
  EVAL/EVALSHA layouts, SCRIPT subcommands, hash-tag carving,
  inline PING, per-state framing rejections (req+rsp), null/
  nested multibulk, integer/bulk/status/error responses.
- `tests/proto_redis_parser_deep.rs` (47 tests): deep arg-state
  framing errors, EVAL multi-key, two-chunk and
  resume-at-every-boundary sweeps (drive the ReqState/RspState
  from_u32 restore tables for the deep states), arity-mismatch
  arms, reachable response error arms.
- `tests/proto_memcache_coverage.rs` (64 tests): every command
  type (storage/arithmetic/touch/delete/retrieval/quit), noreply,
  CAS, multi-key GET, hash-tag, per-field framing errors
  (including mid-number bad bytes), every response shape, resume
  sweeps.
- `tests/proto_dnode_coverage.rs` (30 tests): per-field header
  framing errors, data payload (incl. split), parse_req/parse_rsp
  Msg wrappers (Ok/Again/Error), flatten_chain, Dmsg flag
  accessors, Handshake decode error branches, XA variant headers.
- `tests/proto_redis_repair_coverage.rs` (27 tests): MSET/DEL/
  EXISTS fragment encode paths, EmptyKeys, bucket resize, pre/post
  coalesce guards, accumulate_fragment_integer guards, reconcile
  decisions, the read-repair-enabled make path.
- `tests/proto_redis_coalesce_tracker.rs` (5 tests): tracker
  accessors, empty-local-DC fallback, plurality tiebreak,
  already-decided Pending, DC_ONE first-wins.
- `tests/proto_property.rs` (14 hegel/std tests).
- `tests/util_coverage.rs` (12 tests), `tests/stats_coverage.rs`
  (12 tests).

Total: ~273 new tests. Full suite: 1176 nextest + 678 doctests
pass; clippy `-D warnings` clean; `cargo fmt --check` clean.

## Property tests (hegel, default 256 cases unless noted)

- `redis_req_parser_is_total` / `redis_rsp_parser_is_total` (512
  cases each) plus structured-garbage variants biased toward RESP
  framing bytes: the parser never panics and an `Ok` result
  always carries a concrete (non-Unknown) type.
- `memcache_req_parser_is_total` / `..rsp..` (512 each) plus a
  verb-biased structured-garbage variant.
- `dnode_parser_is_total` (512) plus a header-alphabet variant:
  the streaming parser never panics and never consumes past the
  input.
- `classify_is_deterministic_over_catalog`,
  `classify_is_total_over_arbitrary_keyword_bytes`: classify is a
  pure total function and lookup is case-insensitive.
- `mget_fragment_round_trip_preserves_keys`: fragmenting a
  multi-key MGET across 1..=4 shards and re-parsing every fragment
  reconstructs exactly the input key set (no key lost/duplicated).
- `dnode_header_round_trips_every_type`: header encode/decode is
  the identity across all 20 DmsgType variants including the five
  XA variants (XaPrepare/XaVote/XaCommit/XaRollback/XaAck).

## Bugs found

None. No production divergence surfaced; all assertions matched
the existing parser/coalescer/fragmenter behaviour. Two initial
test assertions were wrong about parser behaviour (zero-key MGET
returns Again not Ok; a memcache VALUE+END reply ends as RspMcEnd
not RspMcValue) and were corrected in the tests, not the engine.

## Proposed coverage Deviations (for docs/parity.md)

The following residual uncovered lines are unreachable-in-practice
defensive or resume-only code. They should be recorded as
coverage Deviations rather than chased with contorted tests.

### proto/redis/parser.rs (89.19% line / 90.71% region)

1. **Resume-only `from_u32` arms** (line 106: `ReqState::InlinePing`
   restore; several `RspState::from_u32` arms 139-155). The inline
   PING fast path never returns `Again` at the `InlinePing` state,
   and the response parser's `Start` state transitions directly to
   `IntegerStart`/`RuntoCrlf` (never saving the `Status`/`Integer`
   intermediate states), so those restore arms and the
   `RspState::Status`/`RspState::Integer` resume-entry no-ops
   (1306-1311) are not reachable through the public byte feed.
2. **Dead `BulkLenStep::Eof` guard and arms** (1068, plus the
   `BulkLenStep::Eof => break` arms at 627/744/901/975).
   `read_bulk_len` is only ever called inside `while p <
   input.len()`, so its `if *p >= input.len()` guard and the Eof
   break can never fire.
3. **64-bit-dead `checked_add` overflow arms** (1111, 1457-1467,
   1696-1706). The bulk length is a `u32` (<= 4 GiB), so
   `p.checked_add(rlen as usize)` cannot overflow a 64-bit
   `usize`; the `None =>` overflow arms are reachable only on a
   32-bit target.
4. **Unreachable class/state wildcard and arity arms** (610-612,
   672-715, 834-886, 934-960, 1023-1025). Each `Arg*Lf` arity
   check (`if rntokens != N`) and `_ =>` wildcard is gated by the
   preceding state's own arity check, so the combination that
   would reach them is rejected earlier. Example: an Arg2 command
   can only enter `Arg1Lf` after `KeyLf` already required
   `rntokens == 2`, and `Arg1Len` decrements by exactly one, so
   `rntokens != 1` at `Arg1Lf` is impossible.
5. **Resume-into-Unknown finishers** (1137-1138, 1783-1784):
   `finish_req_ok`/`finish_rsp_ok` reject `ty == Unknown`; reached
   only when a caller resumes from a hand-set parser state with no
   type, which the engine never does.
6. **Resume-only response marker re-entries** (1273-1289 Error,
   1378-1388 Bulk, 1518-1528 Multibulk `ch != marker`): the
   response `Start` state sets the marker state and re-enters on
   the marker byte, so the `ch != marker` arm is only reachable on
   a manual resume.

### proto/memcache/parser.rs (87.51% line / 92.23% region)

Same two categories: resume-only `from_u32` restore arms
(121-185, 240 -- Crlf/AfterNoreply/AlmostDone/End states the
parser does not save mid-parse in a resumable way) and deeply
nested defensive `finish_error`/`finish_error_rsp` returns whose
preceding-state guards make the residual combination
unreachable. The streaming model advances the cursor to the
buffer end inside the value-body state, so a body-state resume
cannot recover the value token; the deep body resume arms are
therefore not exercisable through the public feed.

### proto/redis/coalesce.rs (91.84% line / 93.98% region)

Defensive dc_one error returns (`dc_one: no recorded reply`,
`dc_one: reply already consumed`): the reply slot is inserted and
its `msg` is `Some` immediately before `evaluate_dc_one` reads it,
so both `None` arms are dead. The `winner key has no surviving
msg` arm is similarly defensive. The remaining uncovered lines in
the in-crate `replica_coalesce_tests` module are `panic!`
assertion arms that only fire on test failure.

### proto/redis/fragment.rs (95.83%, at target)

`FragmentError::UnsupportedType` (line 213) is dead:
`encode_fragment` is only invoked on fragments the fragmenter
itself created with the parent's verified multikey type.

### util/sockinfo.rs

The `--show-missing-lines` report lists no uncovered source lines
(the 90.80% line figure is a region/line counting artifact for
the `to_socket_addrs` IO-error path, which depends on a DNS
failure and is exercised structurally by the invalid-port test).

## Open questions

None.
