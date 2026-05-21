#!/usr/bin/env python3
"""Coverage-gate post-processor.

Reads `target/coverage/summary.json` (produced by
`cargo llvm-cov --workspace --all-features --summary-only --json
--output-path target/coverage/summary.json`) and enforces the
Stage 15 95% threshold workspace-wide. Per-file modules listed in
`docs/coverage-deviations.md` are downgraded from errors to
warnings.

Three axes are checked:

* `lines`     - LLVM line coverage (raw line %).
* `regions`   - region coverage; the closest stable proxy for
                branch coverage. The unstable
                `-Zcoverage-options=branch` flag is required to
                populate the JSON `branches` block, so on stable
                we fall back to regions.
* `functions` - function coverage.
"""

import json
import os
import sys

THRESHOLD = float(os.environ.get("DYNOMITE_COV_THRESHOLD", "95"))
MODE = os.environ.get("DYNOMITE_COV_MODE", "enforce")
SUMMARY = "target/coverage/summary.json"
DEV_PATH = "docs/coverage-deviations.md"
REPORT = "target/coverage/report.txt"


def pct(section: dict) -> float:
    if "percent" in section:
        return float(section["percent"])
    count = max(int(section.get("count", 1)), 1)
    return float(section.get("covered", 0)) / count * 100.0


def main() -> int:
    with open(SUMMARY) as fh:
        data = json.load(fh)

    totals = data["data"][0]["totals"]
    line_pct = pct(totals["lines"])
    branch_pct = pct(totals["regions"])
    func_pct = pct(totals["functions"])

    with open(REPORT, "w") as fh:
        fh.write("workspace coverage (cargo-llvm-cov --all-features):\n")
        fh.write(
            f"  line     (lines):     {line_pct:6.2f}% (threshold {THRESHOLD}%)\n"
        )
        fh.write(
            f"  branch   (regions):   {branch_pct:6.2f}% (threshold {THRESHOLD}%)\n"
        )
        fh.write(
            f"  function (functions): {func_pct:6.2f}% (threshold {THRESHOLD}%)\n"
        )

    print(f"line     (lines):     {line_pct:6.2f}% (threshold {THRESHOLD}%)")
    print(f"branch   (regions):   {branch_pct:6.2f}% (threshold {THRESHOLD}%)")
    print(f"function (functions): {func_pct:6.2f}% (threshold {THRESHOLD}%)")

    deviations = set()
    if os.path.exists(DEV_PATH):
        with open(DEV_PATH) as fh:
            for raw in fh:
                line = raw.strip()
                if line.startswith("|") and ".rs" in line:
                    cells = [c.strip() for c in line.strip("|").split("|")]
                    if cells and cells[0]:
                        deviations.add(cells[0].strip("`"))

    documented = []
    undocumented = []
    cwd = os.getcwd()
    for entry in data["data"][0].get("files", []):
        fname = entry.get("filename", "")
        rel = os.path.relpath(fname, cwd)
        if not rel.startswith("crates/"):
            continue
        summary = entry.get("summary", {})
        fl = summary.get("lines", {}).get("percent", 100.0)
        fb = summary.get("regions", {}).get("percent", 100.0)
        ff = summary.get("functions", {}).get("percent", 100.0)
        if min(fl, fb, ff) < THRESHOLD:
            listed = rel in deviations or any(rel.endswith(d) for d in deviations)
            row = (rel, fl, fb, ff)
            (documented if listed else undocumented).append(row)

    with open(REPORT, "a") as fh:
        if documented:
            fh.write("\nDocumented deviations below threshold (warnings only):\n")
            for rel, fl, fb, ff in sorted(documented):
                fh.write(
                    f"  {rel}: line {fl:.2f}% branch {fb:.2f}% function {ff:.2f}%\n"
                )
        if undocumented:
            fh.write("\nUNDOCUMENTED modules below threshold:\n")
            for rel, fl, fb, ff in sorted(undocumented):
                fh.write(
                    f"  {rel}: line {fl:.2f}% branch {fb:.2f}% function {ff:.2f}%\n"
                )

    if documented:
        print()
        print(
            f"warning: {len(documented)} documented deviation(s) below threshold"
        )
        for rel, fl, fb, ff in sorted(documented):
            print(
                f"  WARN {rel}: line {fl:.2f}% branch {fb:.2f}% function {ff:.2f}%"
            )

    if undocumented:
        print()
        print(
            f"error: {len(undocumented)} undocumented module(s) below threshold:"
        )
        for rel, fl, fb, ff in sorted(undocumented):
            print(
                f"  FAIL {rel}: line {fl:.2f}% branch {fb:.2f}% function {ff:.2f}%"
            )

    workspace_pass = (
        line_pct >= THRESHOLD
        and branch_pct >= THRESHOLD
        and func_pct >= THRESHOLD
    )

    if MODE == "report":
        print(
            f"\ncoverage_gate: report-only (mode=report); "
            f"workspace_pass={workspace_pass}"
        )
        return 0
    if not workspace_pass:
        print(f"\ncoverage_gate: workspace below {THRESHOLD}% threshold")
        return 1
    if undocumented:
        print("\ncoverage_gate: undocumented per-file deviations")
        return 1
    print(
        f"\ncoverage_gate: PASS (line {line_pct:.2f}%, "
        f"branch {branch_pct:.2f}%, function {func_pct:.2f}%)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
