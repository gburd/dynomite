# P3-3.9 Differential chaos rig: phases 3 + 4 (workload fan-out + reply comparison)

Status: phases 3+4 implemented. Phase 5 (chaos applied to
both proxies in lockstep) still queued.
Stage branch: `stage/p3-3.9-differential-phase3-4`.
Builds on: phase 1+2 substrate (commit `05bb33d`,
`docs/journal/2026-05-26-differential-chaos-substrate.md`).

## What landed

### Phase 3: workload-driver dual fan-out

`scripts/chaos-multi-host/workload-driver.py` learns a
`differential` mode. The CLI grows four flags:

* `--rust-host` (defaults to `--host`)
* `--rust-port` (defaults to `--port`)
* `--c-host` (defaults to `--rust-host`)
* `--c-port` (defaults to `--rust-port + 100`, matching the
  phase-2 port shift)

The new `DualConn` class wraps two `RespConn` instances and
dispatches every `call(*parts)` to both proxies in parallel
via short-lived `threading.Thread` pairs. The Rust side stays
the source of truth for the existing retry layer
(`run_with_retry`) -- on Rust failure `DualConn.call` re-raises
so the per-class budget applies. The C-side outcome (reply or
exception) lives on a per-call snapshot
(`DualConn.snapshot()`) that the driver loop reads after
`run_with_retry` returns.

### Phase 4: reply comparison + allowlist

A new module `scripts/chaos-multi-host/differential_allowlist.py`
provides:

* `ALLOWLIST` -- list of `(op-class, rule-name)` tuples.
* `lookup_rule(op_class)` -- case-insensitive match.
* `compare_replies(rust_reply, c_reply, rust_exc, c_exc, op_class)`
  -- returns `(bucket, detail)` where bucket is `agreed`,
  `divergent`, `one_side_failed`, or `both_failed` (folded
  into `divergent` for accounting).
* `classify_error(exc)` -- coarse `(type-name, head)`
  classification used to compare error replies by class
  rather than text.

Allowlist entries (8 total):

| op | rule | rationale |
|---|---|---|
| `INFO`, `TIME` | `ignore_timing_fields` | uptime, pid, version, byte counts; node identity differs per cluster (`server_version`, `node`, `build_id` in dynomited's INFO blob) |
| `CLIENT` | `ignore_connection_ids` | per-cluster id/fd/addr bookkeeping (`id`, `addr`, `laddr`, `fd`, `age`, `idle`, ...) |
| `KEYS`, `SCAN`, `SMEMBERS`, `HKEYS`, `HVALS`, `HGETALL` | `sort_array_response` | unordered set/hash semantics |

Error classification is by prefix: the first token of the
message before `:` or whitespace is uppercased and used as
the class head. So `-ERR no such key` and `-ERR key not
found` both classify to `("RespError", "ERR")` and the
comparator says `agreed`. `-ERR` vs `-WRONGTYPE` classifies
differently and lands in `both_failed` (folded into
`divergent`).

The driver's NDJSON output gains three new buckets per row,
emitted only in differential mode:

* `agreed`: `{op: count}`
* `divergent`: `{op/reason: count}` plus a capped
  `divergent_samples` list of `{op, reason, snippet_rust,
  snippet_c}`.
* `one_side_failed`: `{op/which/error_class: count}`.

Existing `counts` / `failures` / `retries` are unchanged: they
continue to reflect the Rust side as the source of truth (a
Rust-side success increments `counts` even if C timed out;
the asymmetry surfaces as a `one_side_failed/SET/c/timeout`
entry on the same row).

### Coordinator integration

`scripts/chaos-multi-host/coordinator.sh`:

* Defines `CLIENT_LISTEN_PORT_C=$((CLIENT_LISTEN_PORT + 100))`
  alongside the existing port table.
* `start_workload` selects mode flags as:
  ```
  if MODE=riak:          --mode riak --riak-pbc-port ...
  if MODE=differential:  --mode differential --rust-port $CLIENT_LISTEN_PORT --c-port $CLIENT_LISTEN_PORT_C
  else:                  --mode $MODE
  ```
  The C-side host defaults to `127.0.0.1` via the existing
  `--host` flag (both proxies live on the same chaos host
  per the phase-2 layout).

### Smoke test extension

`scripts/chaos-multi-host/smoke-differential.sh` already
brought up both proxies and asserted each port answered SET
/ GET. Phase 3+4 adds a step 4: run `workload-driver.py
--mode differential` for 30 seconds at 50 QPS and assert the
emitted NDJSON contains a non-zero `agreed` count. Any
non-zero `divergent` count surfaces as a `WARN` line so the
next reviewer extends the allowlist before merging; it does
not fail the smoke (the allowlist is intended to be
operator-extensible as new divergences are observed).

## Tests

* `scripts/chaos-multi-host/test_differential_allowlist.py`
  (new, 21 cases):
  * INFO ignores timing fields (drop list + stable-key
    survival).
  * Unsorted array responses get sorted before compare
    (KEYS, SMEMBERS, HKEYS), with the negative case (MGET is
    not on the allowlist so a reorder remains divergent).
  * Error messages match by class not text (`ERR no such key`
    vs `ERR key not found`, both agree).
  * Byte-exact path for SET / GET / INCR.
  * One-side-failed shape including error_class detail.
  * CLIENT-LIST strip with real diff vs id/addr noise.

* `scripts/chaos-multi-host/test_workload_driver.py`
  (existing file, +9 cases):
  * `_ScriptedRespConn` + `_build_dual` helpers wire a
    DualConn whose sub-conns are scripted fakes.
  * `DifferentialModeDualFanoutReturnsAgreedWhenRepliesMatch`
    (2 cases): SET OK on both sides, GET same bytes.
  * `DifferentialModeRecordsDivergentWhenByteDiffOutsideAllowlist`
    (2 cases): GET byte-diff, INCR int-diff.
  * `DifferentialModeKeysCommandSortsBeforeCompare` (2 cases):
    KEYS in two orderings agree; KEYS with real diff
    diverges after sort.
  * `DifferentialModeOneSideFailedClassification` (3 cases):
    C timeout while Rust succeeds; Rust DYNOMITE error while
    C succeeds (re-raised); both sides DYNOMITE with
    different message text agree.

Total new tests: 30.

Existing self-tests (`workload-driver.py --self-test`,
`test_workload_driver.py`) pass unchanged: 35 + 11 + 9 = 55
on the workload driver, 21 on the allowlist.

## Verification

```
$ python3 -m unittest test_workload_driver
Ran 20 tests in 0.034s
OK

$ python3 -m unittest test_differential_allowlist
Ran 21 tests in 0.001s
OK

$ python3 scripts/chaos-multi-host/workload-driver.py --self-test
Ran 35 tests in 0.006s
OK

$ bash -n scripts/chaos-multi-host/coordinator.sh
$ bash -n scripts/chaos-multi-host/smoke-differential.sh
$ bash scripts/check_no_todos.sh
$ bash scripts/check_no_port_comments.sh
$ bash scripts/check_ascii.sh
```

The end-to-end smoke
(`bash scripts/chaos-multi-host/smoke-differential.sh`) was
not run in this session because the host is currently
holding live chaos run state under
`/scratch/dynomite-chaos/run/`. The script's port-override
env knobs avoid the proxy-port collision but the pidfile
layout is still shared. Operators run the smoke on an
otherwise-idle floki between chaos passes.

## Future-divergence-discovery process

The allowlist is intentionally narrow at first: 8 entries
covering known semantic differences. As phase 5 lands and
the rig actually drives traffic at scale, new divergences
will surface. The discovery loop is:

1. The smoke or a real chaos pass logs a `divergent`
   bucket entry with a `divergent_samples` snippet.
2. The operator inspects the snippet:
   * If the divergence is a real Rust-vs-C bug, file an
     issue and fix it; do NOT add to the allowlist.
   * If the divergence is a known semantic difference (new
     INFO field, a command we missed adding to the
     unsorted-array list, a server-error wording skew),
     extend `ALLOWLIST` with a new tuple AND extend the
     relevant strip helper (`_strip_info_blob`,
     `_strip_client_list`, ...) if the rule needs new
     logic.
3. Add a regression test in
   `test_differential_allowlist.py` that pins the new
   entry's behaviour.
4. Re-run the smoke; the divergent bucket should drain.

The allowlist deliberately uses a list of tuples (not a
dict) so future entries that need rule-ordering control
(e.g. a more specific rule that should win over a
catch-all) can be inserted without rewriting consumers.

## What's next: phase 5

Phase 5 (chaos faults applied to both proxies in lockstep)
remains queued. The chaos-injector currently kills only
the Rust dynomited; phase 5 makes it kill BOTH proxies on
the same schedule so they see identical fault conditions.
Network/clock/disk faults are host-level and already affect
both processes; only process-level faults
(`SIGSTOP/SIGKILL`, redis-bounce) need a parallel pidfile
lookup. Effort estimate: ~3 days. Design lives in
`docs/journal/2026-05-26-differential-chaos-substrate.md`
under "Phase 5".

## Hard-constraint compliance

* ASCII only (no smart quotes / em-dashes / arrows).
* No new third-party Python deps; stdlib only.
* Default behaviour unchanged: when `--mode differential`
  is unset, workload-driver behaves identically to before
  (verified by re-running the existing 35 + 11 self-tests).
* No touches to `crates/` or `dist/`.
* Single conventional commit with `Greg Burd <greg@burd.me>`
  authorship and sign-off.

## Files touched

* `scripts/chaos-multi-host/differential_allowlist.py` (new)
* `scripts/chaos-multi-host/workload-driver.py`
  (DualConn, --mode differential, --rust-host/port,
  --c-host/port, ndjson buckets agreed/divergent/
  one_side_failed plus divergent_samples)
* `scripts/chaos-multi-host/coordinator.sh`
  (CLIENT_LISTEN_PORT_C, start_workload mode_flags branch
  for differential)
* `scripts/chaos-multi-host/smoke-differential.sh`
  (step 4: 30s differential workload + agreed-count assertion)
* `scripts/chaos-multi-host/test_workload_driver.py`
  (+9 cases for DualConn + differential bookkeeping)
* `scripts/chaos-multi-host/test_differential_allowlist.py`
  (new, 21 cases)
* `docs/operations/chaos.md` (Differential mode section
  extended with phase 3+4 description)
* `docs/post-chaos-queue.md` (P3-3.9 phases 3+4 marked done;
  phase 5 still queued)
* `docs/journal/2026-05-26-differential-phases-3-4.md`
  (this file)
