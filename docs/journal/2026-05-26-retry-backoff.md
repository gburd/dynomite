# 2026-05-26 - Workload-driver retry backoff with jitter

## Summary

Pass-5 (2026-05-26 redis mode, see
`docs/journal/2026-05-25-workload-retry-semantics.md` for the
retry-policy framework) shipped with `RETRY_POLICY=...,Closed:2`
and rescued ~503k errors that pass-4 had counted as failures
(success rate +2.7pt to 93.78%). The retry layer worked. But
the implementation had no backoff between attempts: a recoverable
error closed the connection and the loop immediately re-issued
the request. Under chaos that produced a thundering-herd
re-saturation pattern that this commit fixes.

## Problem

The chaos rig runs four workload drivers concurrently, one per
host. The chaos injector cycles through faults that include
SIGKILL of the local dynomited; each kill window restarts the
listener about a second later. When the listener comes back,
all four drivers have queued up recoverable errors at exactly
the same instant (their connections all dropped together) and
all four immediately retry. The freshly-bound listener absorbs
4x the steady-state connection-establishment load in one
syscall window. Under heavier injector cadences this caused:

* the listener to refuse the burst (ECONNREFUSED for some
  drivers), turning a recovered cluster back into a Closed-
  reporting one,
* visible "echo" failures in the success-rate timeline 200-400
  ms after every restart, and
* prolonged recovery windows compared to a smoothed retry
  pattern.

The fix is the textbook one: per-class exponential backoff with
a uniform jitter band, capped at a per-class ceiling so a long
recovery window does not let one op block forever.

## Design

### Syntax

The `--retry-on` flag's per-entry syntax extends from
`<class>:<count>` to:

```
<class>[:<count>[:<base_ms>[:<max_ms>]]]
```

Two new optional fields. When omitted, both default to
`50:200` (the documented universal default; see
`RETRY_DEFAULT_BASE_MS` / `RETRY_DEFAULT_MAX_MS` in
`workload-driver.py`). The short form `Closed:2` continues to
parse, so existing tooling and operator runbooks keep working.

The new default coordinator policy is:

```
NoTargets:1:50:200,Timeout:0,Closed:2:100:1000
```

* `NoTargets` keeps the modest 50/200ms window. Gossip churn
  clears in under a second and a tight retry window is fine.
* `Timeout:0` is unchanged: timeouts mean genuine unavailability
  and we never retry.
* `Closed` gets a wider 100/1000ms window because it is the
  dominant chaos-cycle failure class (>99.9% in pass-4) and
  because waiting longer for a freshly-restarted listener is
  cheap compared to flooding it.

### Sleep formula

Between attempt `n` and attempt `n+1` for a given recoverable
class, the loop computes:

```
window_ms = min(base_ms * 2**n, max_ms)
sleep_ms  = window_ms * uniform(0.5, 1.5)
```

i.e. exponential growth capped at `max_ms`, with a uniform
jitter factor in `[0.5, 1.5]`. So a 100ms base sleeps anywhere
in `[50, 150]`ms on the first retry, anywhere in `[100, 300]`ms
on the second, plateauing once the doubling clears the cap.

The jitter band is symmetric around 1.0 so the long-run
average of N attempts equals the un-jittered curve. We picked
`[0.5, 1.5]` rather than the also-common `[0, 1.0)` "full
jitter" because the symmetric band has lower variance for the
same expected wait, and pass-5 traces show recoveries are
typically uniform-distributed over the recovery window rather
than front-loaded.

### Wallclock deadline

A misconfigured policy with a high count and a high max can
make a single op block for tens of seconds before the budget
runs out. To prevent that, every op carries a
`retry_deadline_ms` (default 5000, exposed as
`--retry-deadline-ms`). If the next backoff would push
cumulative time-in-retry past that deadline, the loop gives
up immediately and surfaces the failure. Budget that would
not have actually fired stays accounted-for: we do not
consume `retries` for a sleep we never performed.

The deadline is wallclock-only (sum of sleep durations). We
do not include actual workload execution time, because that
varies wildly with op size and the deadline is meant to bound
the time the driver itself spends waiting, not the time the
backend spends working.

### NDJSON

The existing `retries` field (per-`<cls>/<class>` retry counter)
is unchanged. A new `retry_sleep_ms` field, keyed identically,
records the cumulative wallclock cost (in milliseconds) of all
backoff sleeps in the window:

```json
{
  "retries":        { "strings/Closed": 12, "strings/NoTargets": 3 },
  "retry_sleep_ms": { "strings/Closed": 1843, "strings/NoTargets": 78 }
}
```

Operators can sum the values to get a single "time spent in
backoff" gauge per window, or watch per-class for unexpected
ratios (e.g. a 3-retry class with 30s of sleep means the per-
attempt window is too high or every retry is hitting the cap).

The `generate-report.py` consumer uses `.get("retries", {})`
defensively today; adding `retry_sleep_ms` is additive and
does not break the report generator. A future commit can wire
the new gauge into the report output.

## Implementation notes

* `parse_retry_policy` returns `dict[str, tuple[int, int, int]]`
  where the tuple is `(count, base_ms, max_ms)`. The shape
  change ripples into the existing inline tests in
  `workload-driver.py` and the assertion in
  `test_workload_driver.py::ClosedRetryTests`; both are
  updated.
* `run_with_retry` grew two keyword-only parameters
  (`retry_sleep_ms` and `retry_deadline_ms`) with documented
  defaults so the call site in `main()` is the only place
  that needs to pass them. Other callers (the inline test
  suite) get the new behaviour for free with the deadline
  default.
* Sleep is performed via `time.sleep` and jitter is sampled
  from `random.random()`; both are module-level so tests
  patch them directly with `unittest.mock.patch.object`.
* The deadline check happens BEFORE the sleep, not after, so
  we never sleep into a deadline overrun. If the upcoming
  sleep would exceed the deadline, we surface the failure
  with the just-classified `err_class` and consume neither
  budget nor retry counter.

## Tests

Five new cases in `test_workload_driver.py` cover the parser
and runtime layers:

* `parse_retry_policy_accepts_backoff_suffixes` -- pins
  `Closed:2:100:1000` -> `(2, 100, 1000)`.
* `parse_retry_policy_uses_default_backoff_when_suffixes_omitted`
  -- pins `Closed:2` -> `(2, 50, 200)` so the short form keeps
  parsing.
* `parse_retry_policy_full_default_policy` -- pins the new
  coordinator default end-to-end.
* `parse_retry_policy_rejects_max_below_base` -- rejects
  obviously broken policies.
* `parse_retry_policy_rejects_too_many_segments` -- five-or-
  more colons is an error.
* `run_with_retry_sleeps_with_jitter_between_attempts` --
  patches `time.sleep` and pins `random.random` to 0.5 (jitter
  factor 1.0); confirms two sleeps of 100ms and 200ms (matching
  the exponential window) and confirms `retry_sleep_ms`
  accumulates 300ms.
* `run_with_retry_sleeps_within_jitter_band` -- pins
  `random.random` to both extremes (0.0 and 0.999) and
  confirms the resulting sleep stays in the documented
  `[0.5, 1.5]` band of the window.
* `run_with_retry_respects_retry_deadline_ms` -- with a 10ms
  deadline and a 100ms base, the very first backoff would
  overrun; we should give up before sleeping or consuming
  budget.
* `run_with_retry_deadline_allows_partial_progress` -- with
  a 150ms deadline and base=100ms, exactly one retry fits;
  the second is refused and we surface the failure with one
  retry counted.
* `run_with_retry_without_sleep_dict_still_works` -- the
  `retry_sleep_ms` parameter is optional.

Inline tests in `workload-driver.py` (under `--self-test`)
update their parser-shape expectations to the new tuple form.
The retry-loop tests there now sleep for real (~500ms total
across the suite) but stay well inside the default deadline.

## Verification

```
python3 scripts/chaos-multi-host/test_workload_driver.py     # 21 tests pass
python3 scripts/chaos-multi-host/workload-driver.py --self-test  # 35 tests pass
bash -n scripts/chaos-multi-host/coordinator.sh
scripts/check_no_todos.sh
scripts/check_no_port_comments.sh
scripts/check_ascii.sh
```

## Files touched

* `scripts/chaos-multi-host/workload-driver.py` -- parser
  shape change, new sleep-with-jitter step, new
  `--retry-deadline-ms` flag, new `retry_sleep_ms` NDJSON
  field, inline test updates.
* `scripts/chaos-multi-host/test_workload_driver.py` -- five
  new parser cases plus four new runtime cases; existing
  default-policy assertion updated to tuple shape.
* `scripts/chaos-multi-host/coordinator.sh` -- default
  `RETRY_POLICY` updated to ship the new per-class backoff
  windows.
* `docs/operations/chaos.md` -- new "Backoff" subsection
  documenting the syntax extension, the deadline knob, and
  the new NDJSON field.

## Followups

* `generate-report.py` does not yet surface `retry_sleep_ms`
  in the rendered report. Once pass-6 generates real backoff
  data we can decide whether to add it as a per-class column
  in the failures-vs-retries breakdown or as a single
  rolled-up gauge.
* If pass-6 still shows post-restart "echo" failures, we may
  need to push `Closed` `max_ms` higher (to 2000 or 3000) or
  add a small floor on the first backoff so the first attempt
  is never sub-50ms even with the unlucky end of the jitter
  band.
