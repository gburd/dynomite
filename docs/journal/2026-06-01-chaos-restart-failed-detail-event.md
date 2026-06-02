# 2026-06-01 - chaos: structured `restart_failed_detail` event + classifier

Stage: post-chaos queue P3-1.3 (residual-failure diagnosis).
Branch: `stage/chaos-infra`.

## Motivation

The 2026-05-25 P3-1.3 work added a `tail` field to the existing
`restart_failed` event (see
`docs/journal/2026-05-25-chaos-restart-failed-detail.md`),
capturing the last 50 lines of the start-host.sh restart-log
inline. Pass-3 then ran with that change and the chaos lead
still had to read every per-host event stream by eye to
classify the residual 7-11% `restart_failed` rate by cause.

Three things were missing:

1. The captured tail came from `start-host.sh`'s own
   stdout/stderr (a progress trace), not from `dynomited`'s
   stderr or log. The actionable diagnostic - the panic
   message, the `Address in use` line - lives in dynomited's
   own files.
2. The tail was JSON-string-escaped, so any ASCII-control
   bytes that dynomited dumped to stderr corrupted the
   resulting NDJSON line.
3. Nothing classified the captured tails. Operators triaged
   them with grep by hand.

## Change

### Injector side

`scripts/chaos-multi-host/chaos-injector.sh`:

* New helper `base64_file <path>` that takes the last 50 lines
  of a file and emits a single-line base64 blob. Picks GNU
  `base64 -w0` or BSD `base64 | tr -d '\n'` based on what the
  binary supports. Missing file -> empty string (graceful).
* New emit function `emit_restart_failed_detail <rc>` writes a
  single ndjson line to `$EVENTS` with the spec'd shape:

  ```json
  {"event":"restart_failed_detail",
   "kind":"restart_failed_detail",
   "host":"<dc-name>",
   "rc":<int>,
   "stderr_tail":"<base64>",
   "log_tail":"<base64>",
   "timestamp":"<RFC3339>",
   "ts":"<RFC3339>"}
  ```

  The duplicated `kind`/`ts` aliases preserve the existing
  ndjson consumers (`live-status.sh`, `parse_chaos_events`)
  while also satisfying the literal queue-spec field names
  (`event`/`timestamp`).

* `restart_dynomited` now calls `emit_restart_failed_detail`
  alongside the legacy `restart_failed` event when the rc is
  non-zero. The detail event's tails come from
  `$LOGS/dynomited-$DC_NAME.stderr` and
  `$LOGS/dynomited-$DC_NAME.log` rather than the start-host
  restart-log; that is where dynomited's own panic / bind /
  init-error bytes land.

### Reporter side

`scripts/chaos-multi-host/generate-report.py`:

* New `chaos_restart_failed_class(stderr_tail, log_tail)`
  classifier returns one of `port-collision`, `backend-down`,
  `crash-mid-startup`, `unknown`. Implementation is a fixed
  list of ordered regex matchers (`RESTART_FAILED_CLASS_PATTERNS`)
  applied with `re.IGNORECASE | re.MULTILINE` over the
  concatenated tails. The first match wins; the regex set is
  documented inline so adding a category is a one-line patch.
* New `extract_restart_failed_classes(events)` walks the
  per-host event list, filters to events whose `event` or
  `kind` is `restart_failed_detail`, base64-decodes the tails
  with a permissive fallback, and returns a Counter keyed on
  the four class labels.
* `render_report` emits a new "Restart-failed class breakdown
  (P3-1.3)" subsection under the per-host stability indicators
  ONLY when at least one classified event is present in the
  run. The section renders a per-host table with one column
  per class plus an aggregate row. When no detail events were
  recorded the section is omitted entirely so legacy run-dirs
  produce the same report they always did.

## Tests

`scripts/chaos-multi-host/test_generate_report.py` grew two
new test classes:

* `RestartFailedClassifierTests` (8 tests): exercises every
  class label, verifies the matcher-priority ordering
  (port-collision wins over crash-mid-startup when both
  patterns are present), confirms unknown is the residual
  bucket for both empty and noisy inputs, and asserts the
  base64 extractor decodes valid blobs while routing corrupt
  ones to `unknown` rather than raising.
* `RestartFailedClassReportTests` (2 tests): synthesises a
  full run-dir with multiple `restart_failed_detail` events
  and asserts the rendered markdown carries the new
  subsection with the right per-host counts; a separate test
  asserts the section is omitted when no detail events are
  present.

The original 16 tests stay green. New count: 26.

## Verification

```
$ python3 scripts/chaos-multi-host/test_generate_report.py
...
Ran 26 tests in 0.119s
OK
$ bash scripts/chaos-multi-host/test_fault_smoke.sh
PASS=5 SKIP=5 FAIL=0
$ bash -n scripts/chaos-multi-host/chaos-injector.sh
$ shellcheck scripts/chaos-multi-host/chaos-injector.sh
(no output)
```

A round-trip smoke (driver script kept under `/tmp` and not
committed) writes a synthetic stderr/log pair, sources the
injector in library mode, calls
`emit_restart_failed_detail 7`, and asserts the resulting
line round-trips through `extract_restart_failed_classes`
into the expected `port-collision` bucket.

## Files touched

* `scripts/chaos-multi-host/chaos-injector.sh`
  (base64_file helper, emit_restart_failed_detail emitter,
  call from restart_dynomited)
* `scripts/chaos-multi-host/generate-report.py`
  (RESTART_FAILED_CLASS_PATTERNS, classifier, extractor,
  rendered subsection)
* `scripts/chaos-multi-host/test_generate_report.py`
  (RestartFailedClassifierTests + RestartFailedClassReportTests)
