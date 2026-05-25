# 2026-05-25 chaos report generator (P3-2.6)

## Context

The chaos lead has been hand-pasting per-pass markdown reports
under `dist/chaos-reports/v0.1.0/multi-host-pass-N-<mode>.md` after
every multi-host chaos run. The numbers in those reports
(workload totals, chaos-event histograms, top failure reasons)
are mechanically derivable from the per-host
`workload-dc-<host>.ndjson` and `chaos-events-dc-<host>.ndjson`
files the coordinator already collects. P3-2.6 in
`docs/post-chaos-queue.md` queues automating that derivation.

## Work this session

* Replaced `scripts/chaos-multi-host/generate-report.py`
  with a CLI-driven mode-aware generator. The new layout is a
  pure function `render_report(run_dir: Path) -> str` plus a
  thin `main()` that handles argument parsing, run-dir
  discovery, and writing the markdown to disk. The generator
  never modifies the run-dir.
* CLI surface:
    * `--run-id <id>` selects a run-dir; defaults to the latest
      `pass*-...Z` or `prod-...Z` directory under
      `target/chaos-multi-host/`.
    * `--out <path>` overrides the output path; the default is
      `dist/chaos-reports/v0.1.0/multi-host-pass-N-<mode>.md`
      when the run-id encodes both, falling back to
      `multi-host-pass-N.md` or `multi-host-<run-id>.md`.
    * `--all-runs` regenerates every run-dir found.
    * `--runs-dir <dir>` overrides the search root (used by the
      tests).
* Sections produced (eight as required by the brief):
    1. Run summary (run-id, mode, hosts, planned vs actual
       duration, ISO start/end timestamps, coordinator log
       span).
    2. Workload totals (per-host ok/fail/total/rate plus
       aggregate).
    3. Top failure reasons (top 10 across all hosts).
    4. Chaos events by kind (per-host histogram + aggregate).
    5. Per-host stability indicators (restart_failed /
       recovery_restart / redis_bounce / kill / restart).
    6. Failure-cause metrics snapshot (parsed from
       `metrics-*.json` if present; section omitted silently
       otherwise).
    7. Notable timeline events (first three `restart_failed`
       events with a `tail` payload, rendered verbatim).
    8. Provenance (dynomited git sha if recorded; otherwise a
       clear "not recorded" note plus run-dir path and
       generation timestamp).
* Added `scripts/chaos-multi-host/test_generate_report.py`
  with 16 tests covering: run-id parsing, default output paths,
  three-host synthetic data, divide-by-zero on empty workload,
  zero-chaos rendering, metrics snapshot present/absent,
  `restart_failed` tail rendering, git-sha pickup, CLI plumbing,
  latest-run-dir discovery, and a regression test against the
  hand-curated pass-1 numbers
  (3,344,844 ok / 182,339 fail / 94.83%).

## Verification

```
$ python3 scripts/chaos-multi-host/test_generate_report.py
Ran 16 tests in 0.185s
OK

$ python3 -m unittest scripts.chaos-multi-host.test_generate_report
Ran 16 tests in 0.217s
OK

$ python3 scripts/chaos-multi-host/generate-report.py \
    --runs-dir /home/gburd/ws/dynomite/target/chaos-multi-host \
    --run-id pass3-redis-20260525-034149Z \
    --out /tmp/regenerated-pass3-redis.md
wrote /tmp/regenerated-pass3-redis.md

$ bash scripts/check_no_todos.sh && bash scripts/check_ascii.sh
(both exit 0)
```

The pass-3 redis regenerated report's headline numbers match the
hand-curated copy exactly:

* aggregate: 3,097,369 ok / 533,356 fail / 85.31% success
* per-host (arnold/floki/nuc):
  965,654 / 1,040,185 / 1,091,530 ok and
  168,703 / 175,403 / 189,250 fail.

## Smoke diff vs hand-curated pass-3 redis

```
$ diff /tmp/regenerated-pass3-redis.md \
       dist/chaos-reports/v0.1.0/multi-host-pass-3-redis.md
```

Differences (all expected and acceptable):

* The hand-curated header reads "Pass-3 Multi-Host Chaos Report"
  while the generator emits "Multi-host chaos report: pass-3".
* The hand-curated copy starts with several paragraphs of
  pass-3-specific narrative ("**Run ID**: ...", "**Window**: ...",
  "**Mode**: ...", "## Headline numbers" preface) and ends with
  multi-page operator analysis sections ("Original framing",
  "Revised root-cause analysis", "What pass-3 did NOT
  validate", "What pass-4 will validate", "References"). The
  generator does not attempt to reproduce that prose; it
  produces the deterministic "facts" half of the report and
  leaves the analysis half to the operator.
* The hand-curated tables use approximate counts (e.g. "~30",
  "1,061k") and a fixed column order (floki/arnold/nuc); the
  generator emits exact integers and sorts hosts alphabetically.
* The generator's per-host breakdown table has `kill` and
  `restart` columns that the hand-curated copy omits. These
  are useful for any future operator review and the
  per-pass-3 prose explicitly references them.

The brief explicitly noted these structural differences as
acceptable.

## Notes for the operator

* When future runs ship a `git-sha` file at the top of the
  run-dir (or a `start-args` file containing a `GIT_SHA=...`
  line), the Provenance section will pick it up automatically.
  Until that wiring lands, the section says "not recorded".
* When P3-2.5 metrics are scraped into `metrics-*.json` files
  in the run-dir, the Failure-cause metrics snapshot section
  will render the latest snapshot's counters. Until then it is
  silently absent, matching the brief.

## Status

READY_FOR_REVIEW.
