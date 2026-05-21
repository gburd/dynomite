# Coverage gate

Stage 15 lands a workspace-wide coverage gate at >= 95% line,
branch, and function coverage. The gate is enforced by
`scripts/coverage_gate.sh`; CI runs it as part of `scripts/check.sh`.

## Running locally

```
scripts/coverage_gate.sh             # enforce 95% threshold
scripts/coverage_gate.sh --report    # report-only; do not fail
```

The script writes:

* `target/coverage/summary.json` - the raw `cargo-llvm-cov`
  summary (`--json --summary-only`).
* `target/coverage/report.txt` - human-readable percentages,
  documented deviations (warnings only), and undocumented
  per-file deviations (errors).

## How the gate decides

1. The script invokes:

   ```
   cargo llvm-cov --workspace --all-features --summary-only --json \
       --output-path target/coverage/summary.json
   ```

2. It extracts the workspace-wide line, branch, and function
   percentages from the `data[0].totals` block.
3. It compares each axis to the 95% threshold.
4. It walks every per-file entry and classifies files below the
   threshold as either:
   * **Documented deviations**: listed in
     `docs/coverage-deviations.md`. The gate logs a warning but
     does not fail.
   * **Undocumented deviations**: any file under threshold that
     is not in the deviations list. The gate fails.
5. The gate exits non-zero if either:
   * Any of the three workspace axes is below 95%, OR
   * There is at least one undocumented per-file deviation.

## Tracking deviations

`docs/coverage-deviations.md` lists each module that is allowed
below the threshold along with its line / branch / function
percentages and the reason. Listed examples:

* Modules whose primary exerciser is the Stage 14 conformance
  suite (network listeners, peer-protocol clients).
* Modules whose primary exerciser is the Stage 16 chaos test
  (kill-restart paths, gossip recovery, partition handling).

When you add a deviation:

1. Run `scripts/coverage_gate.sh --report` to capture the
   percentages.
2. Add a row to `docs/coverage-deviations.md` with the file
   path, the three percentages, and a one-line technical
   reason.
3. Re-run `scripts/coverage_gate.sh` to confirm the gate now
   classifies the module as a documented deviation (warning
   only).

When you reduce a deviation:

1. Add tests that lift the module above 95%.
2. Run `scripts/coverage_gate.sh --report` to confirm.
3. Remove the row from `docs/coverage-deviations.md`.

## Soak coverage

The soak job runs property tests at 1M cases each
(`make soak`) and re-runs `scripts/coverage_gate.sh` with the
same thresholds. The expectation is that soak coverage matches
or exceeds the per-PR coverage; any regression is a soak
finding that blocks the next release tag.
