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
cargo bench --bench <name> -p dynomite-engine -- --save-baseline stage-15
```

then update the matching JSON manifest with the new `captured`,
`git_sha`, and (if applicable) host details. Commit the change.

## Gating a new run

```
cargo bench --bench <name> -p dynomite-engine -- --baseline stage-15
```

criterion writes a `change/` report under `target/criterion/<bench>/`
for every measurement; the Stage 15 macro harness consumes that
report and exits non-zero on any case whose median time regressed by
more than `regression_budget_pct` against the baseline. The
mechanics are documented in
`docs/book/src/operations/benchmarks.md`.

## Status on this checkout

All seven micro benches (`crypto`, `dnode`, `hashkit`, `mbuf`,
`parsers`, `quorum`, `tokens`) have a populated manifest: `captured`
and `git_sha` are set, and the criterion `stage-15` baseline data
itself lives under `target/criterion/<case>/stage-15/` on whichever
machine ran the capture. That directory is gitignored (see the
workspace `.gitignore`, `/target`), so `--save-baseline` must be
re-run locally before `--baseline stage-15` has anything to compare
against on a fresh checkout; the manifest's `captured`/`git_sha`
fields are provenance only, not a substitute for the actual
criterion data. The `macro_throughput` bench (gated behind the
`bench-macro` feature; needs a live 3-node cluster and
`CAP_NET_ADMIN`) and `random_slicing` (not part of the Stage 15
micro-bench list in `AGENTS.md` Section 7.1) are out of scope for
this manifest set and have no baseline file here.

The capture host was not quiescent (background load average in the
10-25 range on an 8-thread machine); several cases show 5-25%
outlier rates in the raw criterion output. This is expected on a
developer workstation and is why the gate's `regression_budget_pct`
(10%) compares a *new* run's median against *this host's own*
previous median rather than asserting an absolute number: as long as
re-baselining and gating both happen on hosts with comparable noise
characteristics, the relative comparison stays meaningful. For a
tighter gate, re-capture on a quiescent CI runner with `isolcpus` and
update `captured`/`git_sha` accordingly.

See `dist/bench-reports/` for the full measured numbers from the
capture run that produced the current manifests.
