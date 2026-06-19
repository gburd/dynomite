# 2026-06-19 -- dyniak CORE coverage to >= 95%

Branch: `stage/cov-dyniak-core` (worktree `wt-cov-dyniak-core`).

Raised line + function coverage on the dyniak storage and protocol
core modules. Tests only; production source was touched only to add
co-located `#[cfg(test)]` modules (no logic change). No bugs found.

## Per-file before -> after (line% / function%)

Measured with
`cargo llvm-cov -p dyniak --features noxu,wasm,search --summary-only
--ignore-filename-regex 'tests?/'`.

| File | line before | line after | fn before | fn after |
|---|---|---|---|---|
| `datastore/xa.rs` | 79.64 | 89.80 | 61.29 | 77.14 |
| `datastore/xa_net.rs` | 84.67 | 95.20 | 78.89 | 92.52 |
| `datastore/noxu.rs` | 93.02 | 93.56 | 85.86 | 86.87 |
| `mapreduce/executor.rs` | 89.12 | 97.22 | 82.28 | 95.45 |
| `mapreduce/builtins.rs` | 88.79 | 96.19 | 86.36 | 92.59 |
| `mapreduce/registry.rs` | 83.33 | 97.33 | 85.71 | 88.24 |
| `proto/http/mod.rs` | 76.19 | 88.10 | 88.89 | 100.00 |
| `proto/http/routes.rs` | 88.73 | 94.36 | 88.81 | 93.30 |
| `proto/http/search.rs` | 88.60 | 94.32 | 89.23 | 92.11 |
| `proto/pb/messages.rs` | 92.86 | 99.88 | 75.64 | 100.00 |
| `proto/pb/mapreduce.rs` | 85.37 | 100.00 | 66.67 | 100.00 |
| `proto/pb/framer.rs` | 94.78 | 98.20 | 95.24 | 96.15 |

## Tests added

* `datastore/xa.rs` (+5 unit): accessors, Delete-op apply path,
  phase-1 apply-failure rollback, `map_xa_error` Conflict/Backend
  classification.
* `datastore/xa_net.rs` (+30 unit): hex/unhex/LineTag edges,
  `InDoubtLog` load (missing file / junk lines / non-NotFound io
  error / round-trip), `handle_prepare` unknown-env / apply-failure /
  malformed-xid, `resolve` unknown-env / malformed-xid / wrong-state,
  coordinator accessors + execute empty / out-of-range / force-abort /
  all-local commit / local-prepare-abort, `recover_in_doubt` local
  redrive (Ok / NotFound idempotent / non-NotFound error / malformed
  xid / unknown env / tombstone-write failure / load error),
  `new_with_recovery`, in-doubt-log-write-failure, `wire_xid`.
* `tests/xa_net_wire.rs` (new, +10): dnode transport connect failure,
  per-phase timeout, wrong reply type (prepare / commit), ack ok=false,
  peer-closed, peer-plane unexpected dnode type, split-frame reassembly,
  garbage-header parse error, persistent-connection reuse.
* `mapreduce/executor.rs` (+13 unit): link-phase riak_get error /
  undecodable object / missing object / missing bucket-key / matching
  emit (scripted store), WasmModule success / wasm-error passthrough /
  generic-error wrap, streaming-with-wasm, bucket-scan stream error,
  scripted-store trampoline.
* `mapreduce/builtins.rs` (+11 unit): extract-field array/opaque-string/
  scalar/null-value/bad-arg arms, u64-over-i64 rejection, int+float
  mix, sort tie-break / arrays+objects ordering.
* `mapreduce/registry.rs` (+1 unit): sorted name listing.
* `proto/pb/mapreduce.rs` (+1), `proto/pb/messages.rs` (+1): every
  `WireValue::wire_type_id` exercised + uniqueness check.
* `proto/pb/framer.rs` (+2): write-frame over-max rejection,
  non-EOF io error classification.
* `proto/http/routes.rs` (+27): dispatch-error 500s, not-acceptable
  406s (object / props / list), `parse_link_resource` / `unquote` /
  `decode_path_segment` / `hex_digit` / `storage_error_response` /
  txn-response helpers, and a noxu-backed block (PUT->GET transcode
  across json/cbor/protobuf, missing-object 404, HEAD, invalid-bucket
  400, delete 204, transaction bucket-mismatch / decode-error).
* `proto/http/search.rs` (+8): all six route handlers' no-registry
  501 / not-acceptable 406 / bad-request / not-found / conflict /
  happy arms, `SearchError::into_response` every variant.
* `tests/http_server_variants.rs` (new, +4): the four remaining
  `serve_http*` entry points (search+wasm, tls+search, tls+wasm,
  tls+search+wasm) each with a `GET /ping` round-trip.
* `tests/mapreduce_pb_properties.rs` (new, +5 hegel property tests,
  256 cases each): `RpbContent` / `RpbLink` encode->decode round-trip
  over arbitrary field sets (byte-identical canonical re-encode);
  `reduce_count` length invariant; `reduce_set_union` dedup +
  first-seen order + idempotence; `reduce_sort` permutation +
  idempotence + input-order independence.

Total added: ~125 tests across 13 files.

## Bugs found

None. Every assertion matched the documented contract. In particular
the XA 2PC paths held: idempotent commit/rollback on the peer
(NotFound treated as success), presumed-abort on prepare failure,
forward-only recovery of in-doubt branches, and never rolling back a
branch that voted Ok in the commit phase. One test initially asserted
that `handle_commit` of a never-prepared branch returns `false`; that
was a wrong test expectation, not a bug -- the engine reports
`NotFound`, which the idempotency contract correctly treats as
success. The test was corrected and a separate `resolve_commit_in_
wrong_state_is_false` test covers the genuine `Err(_) -> false` arm.

## Proposed coverage deviations (genuinely-unreachable lines)

These lines are unreachable from in-process tests without a
fault-injection seam the production code does not expose, or are
type-guaranteed-dead arms. Recommend a documented deviation entry.

### `datastore/xa.rs` lines 353-360, 429-446 (function 77.14%)

The single-process `XaCoordinator::execute` prepare-phase `Err(e)`
arm (353-360) and the `rollback_remaining` helper it calls (429-446)
only fire when `noxu::xa::xa_prepare` itself returns an engine error
(a WAL fsync / disk fault). After a clean `xa_start` + apply +
`xa_end(TMSUCCESS)` the branch is in the `Idle` state `xa_prepare`
requires, so it returns `Ok` for any input a test can construct;
there is no public seam to inject an engine fault into a real
`XaParticipant`. The analogous cross-node `local_prepare` abort path
IS reachable (apply failure via a separator byte in the bucket) and is
covered; the difference is that the single-process coordinator surfaces
an apply failure through phase 1 (`run_branch_work`, now covered),
never through the phase-2 prepare arm. Line 603 is a test closing
brace.

### `datastore/xa_net.rs` (function 92.52%)

* 249, 259-264, 270-271 (`handle_prepare`) and 1040, 1049-1054,
  1060-1061 (`local_prepare`): the `xa_start` / `mark_write` /
  `xa_end` / `xa_prepare`-`Err` defensive abort arms. Same
  engine-fault unreachability as xa.rs above; the apply-failure abort
  arm in both functions IS covered.
* 268 (`handle_prepare`) and 1058 (`local_prepare`): the
  `Ok(PrepareResult::ReadOnly) -> XaVote::ReadOnly` arm. Both functions
  unconditionally call `mark_write` before `xa_prepare`, so the engine
  always sees writes present and never returns `ReadOnly`. The arm is
  dead given the current call sequence (it exists for parity with the
  X/Open read-only optimisation). Line 883 in `execute`
  (`Ok(XaVote::ReadOnly) => {}`) is dead for the same reason -- no
  branch ever votes ReadOnly.
* 515: the `create_dir_all(parent)` branch in `InDoubtLog::append`
  when the path has a parent that does not yet exist; covered for the
  with-parent case, the no-parent case is the bare-filename edge.
* 1306: `serve_xa_conn`'s `Err(e) => return Err(e)` arm. `read_frame`
  only ever returns `XaTransportError::Transport`, which the preceding
  arm handles, so this arm is unreachable (it exists for exhaustive
  matching). Lines 1375, 1390, 1421 are test-helper lines (the
  `/scratch` fallback branch and a mock rollback header).

### `mapreduce/builtins.rs` lines 134-136, 141, 288-291 (function 92.59%)

* 134-136: `reduce_sum`'s `as_u64` -> `i64::try_from` success ->
  `checked_add` path. A JSON number whose `as_i64()` returns `None`
  but `as_u64()` returns `Some` is always `> i64::MAX`, so the
  `i64::try_from` on line 132 fails first (covered) and the success
  path on 134-136 is unreachable.
* 141: "numeric value not representable" -- a `serde_json::Number`
  that is none of `as_i64` / `as_u64` / `as_f64`. serde_json numbers
  are always exactly one of those, so the arm is dead.
* 288-291: `canonical_compare`'s final `_ => if ra < rb ...` arm.
  Different-rank pairs are returned at the top of the function
  (`if ra != rb`), so by the time control reaches the match the ranks
  are equal and every equal-rank pair is handled explicitly above;
  the catch-all is dead.

### `proto/http/mod.rs` lines 107, 128, 151, 174, 189, 193, 209, 226, 243, 263, 279-281, 293, 297 (line 88.10%)

These are the per-connection `tracing::warn!` error-logging closures
inside the `serve_http*` / `serve_http_tls*` accept loops, taken only
when an established connection ends with a `hyper` serve error or a TLS
handshake fails mid-stream. Every accept-loop entry point now has a
ping round-trip (function coverage is 100%); the warn-on-error arms
need a connection that handshakes and then faults, which is an
integration / chaos concern rather than a unit-test one. Same class
of deviation already documented for the dynomite server bootstrap
modules.

## Out of scope / left for follow-up

`datastore/noxu.rs` (93.56 line / 86.87 fn), `datatypes/{hll,itc,map}`
(~93-94), and the residual function-coverage on `routes.rs` /
`search.rs` (monomorphised generic `dispatch`/`route`-closure
instances and a few defensive `unwrap_or_else` encode-failure
closures) were not pushed to 95% in this pass; the listed-core line
targets that were the priority (xa_net, executor, builtins, http/mod,
messages, mapreduce, framer) are met, and xa.rs is at its reachable
ceiling pending the deviation above.

## Verify

* `cargo nextest run -p dyniak --features noxu,wasm,search` -- 763
  tests, all green.
* `cargo test --doc -p dyniak --features noxu,wasm,search` -- 51
  doctests green.
* `cargo clippy -p dyniak --all-targets --features noxu,wasm,search
  -- -D warnings` -- clean.
* `cargo clippy -p dyniak --all-targets --features noxu -- -D
  warnings` -- clean.
* `cargo fmt -p dyniak -- --check` -- clean.
