# Coverage gate

The workspace ships a tiered, blocking coverage gate. Core
components must reach 95% line and function coverage; supporting
and tool crates must reach 75%. The gate is enforced by
`scripts/coverage_gate.sh`; CI runs it as part of
`scripts/check.sh` (without `|| true`, so it can fail the build).

The tiers are:

* **Core** (95%): the engine `proto` / `cluster` / `io` /
  `hashkit` / `crypto` / `msg` / `core` / `net` layers and the
  dyniak `datastore` / `proto` / `datatypes` / `mapreduce`
  layers -- the code a customer's data integrity depends on.
* **Supporting** (75%): the remaining library crates
  (`dynomite-search`, `gen-fsm`, `dyn-sup`, `dyn-encoding`,
  `dynomite-text`, `dynomite-vec`, `dyn-hashtree`,
  `throttle-core`).
* **Tool** (75%): `dyniak-bench`, `dyn-hash-tool`, `dyn-admin`,
  and the test-harness crates `loom-tests` / `model-tests`.

## Running locally

```
scripts/coverage_gate.sh             # enforce the tiered policy
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
   cargo llvm-cov --workspace --features riak --summary-only --json \
       --output-path target/coverage/summary.json
   ```

2. It walks every per-file source entry under `crates/`
   (skipping `tests/`, `benches/`, and the fuzz crate), assigns
   each file its tier threshold, and compares the file's line
   and function coverage to that threshold.
3. A file below its tier is classified as either:
   * **Documented deviation**: listed in
     `docs/coverage-deviations.md`. The gate logs a warning but
     does not fail.
   * **Undocumented deviation**: any file under its tier that is
     not in the deviations list. The gate fails.
4. The workspace-wide line, region, and function percentages are
   printed for trend tracking but are not themselves gated; the
   per-file tier policy is the enforcement axis.

## Tracking deviations

`docs/coverage-deviations.md` lists each module allowed below its
tier along with its line / region / function percentages and a
concrete reason. Every entry is reachable only by an
out-of-process suite (the conformance harness or the chaos rig),
is a re-export facade, is process bootstrap, is rendering output,
or has only unreachable defensive arms left -- none is an
untested unit of pure logic. Regenerate the table with:

```
scripts/coverage_gate.sh --report
python3 scripts/regen_coverage_deviations.py
```

When you reduce a deviation:

1. Add tests that lift the module to its tier threshold.
2. Run `scripts/coverage_gate.sh --report` and regenerate the
   deviations table; the file drops out automatically.

## Soak coverage

The soak job runs property tests at 1M cases each
(`make soak`) and re-runs `scripts/coverage_gate.sh` with the
same tiered thresholds. The expectation is that soak coverage
matches or exceeds the per-PR coverage; any regression is a soak
finding that blocks the next release tag.
