# Stage 15 criterion baselines

This directory holds a per-bench baseline manifest that the regression
gate consumes. Each `<bench>.json` records:

* `criterion_baseline`: the criterion baseline name to load when
  comparing a new run (passed to `cargo bench` via
  `--baseline <name>`).
* `captured`: ISO-8601 timestamp of the original capture, or `null`
  when no baseline has been recorded on this checkout yet.
* `git_sha`: the commit the baseline was captured against, or
  `null`.
* `regression_budget_pct`: the per-case regression budget the gate
  enforces (the Stage 15 default is 10).
* `notes`: free-form text describing the capture conditions.

## Recording a baseline

```
cargo bench --bench <name> -p dynomite -- --save-baseline stage-15
```

then update the matching JSON manifest with the new `captured`,
`git_sha`, and (if applicable) host details. Commit the change.

## Gating a new run

```
cargo bench --bench <name> -p dynomite -- --baseline stage-15
```

criterion writes a `change/` report under `target/criterion/<bench>/`
for every measurement; the Stage 15 macro harness consumes that
report and exits non-zero on any case whose median time regressed by
more than `regression_budget_pct` against the baseline. The
mechanics are documented in
`docs/book/src/operations/benchmarks.md`.

## Status on this checkout

The Stage 15 commit lands the bench harness with empty manifests so
the gate has a stable file layout to load. Capturing the actual
baselines requires a quiescent host (preferably a CI runner with
`isolcpus`), so that step is left for the operator to run before the
release-gate window. CI treats a missing baseline as a non-blocking
warning so the bench harness still compiles and runs in `--test`
mode on every PR.
