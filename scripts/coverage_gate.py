#!/usr/bin/env python3
"""Tiered coverage gate.

Reads `target/coverage/summary.json` (produced by
`cargo llvm-cov --workspace --features riak --summary-only --json
--output-path target/coverage/summary.json`) and enforces a tiered
per-file coverage policy:

* Core components       >= 95% (line and function).
* Supporting components >= 75%.
* Tool / process-entry  >= 75%, with documented deviations for the
  code that is only reachable by spawning the binary or driving a
  live external server (process bootstrap, network drivers, CLI
  main, plotting), which co-located unit tests cannot exercise.

A file below its tier threshold is an ERROR unless it is listed in
`docs/coverage-deviations.md`, in which case it is a WARNING. A
documented deviation must carry a concrete reason; the deviation
list is the audit trail.

Two axes are checked per file:

* `lines`     - LLVM line coverage.
* `functions` - function coverage.

Region/branch coverage is reported for context but not gated
per-file (region counts are noisy at the file level on stable;
the workspace region total is printed for trend tracking).

Usage:
  DYNOMITE_COV_MODE=enforce scripts/coverage_gate.py   # gate
  DYNOMITE_COV_MODE=report  scripts/coverage_gate.py   # report only
"""

import json
import os
import sys

MODE = os.environ.get("DYNOMITE_COV_MODE", "enforce")
SUMMARY = "target/coverage/summary.json"
DEV_PATH = "docs/coverage-deviations.md"
REPORT = "target/coverage/report.txt"

# Tier thresholds (line and function coverage).
CORE_THRESHOLD = 95.0
SUPPORTING_THRESHOLD = 75.0
TOOL_THRESHOLD = 75.0

# Core components: the engine substrate and the dyniak storage /
# protocol layer a customer's data integrity depends on.
CORE_PREFIXES = (
    "crates/dynomite/src/proto/",
    "crates/dynomite/src/cluster/",
    "crates/dynomite/src/io/",
    "crates/dynomite/src/hashkit/",
    "crates/dynomite/src/crypto/",
    "crates/dynomite/src/msg/",
    "crates/dynomite/src/core/",
    "crates/dynomite/src/net/",
    "crates/dyniak/src/datastore/",
    "crates/dyniak/src/proto/",
    "crates/dyniak/src/datatypes/",
    "crates/dyniak/src/mapreduce/",
)

# Tool / benchmark / process-entry crates: held to the supporting
# tier, with deviations for the un-unit-testable bootstrap.
TOOL_PREFIXES = (
    "crates/dyniak-bench/",
    "crates/dyn-hash-tool/",
    "crates/dyn-admin/",
    "crates/loom-tests/",
    "crates/model-tests/",
)


def tier_threshold(rel: str) -> float:
    if any(rel.startswith(p) for p in CORE_PREFIXES):
        return CORE_THRESHOLD
    if any(rel.startswith(p) for p in TOOL_PREFIXES):
        return TOOL_THRESHOLD
    return SUPPORTING_THRESHOLD


def tier_name(rel: str) -> str:
    if any(rel.startswith(p) for p in CORE_PREFIXES):
        return "core"
    if any(rel.startswith(p) for p in TOOL_PREFIXES):
        return "tool"
    return "supporting"


def pct(section: dict) -> float:
    if "percent" in section:
        return float(section["percent"])
    count = max(int(section.get("count", 1)), 1)
    return float(section.get("covered", 0)) / count * 100.0


def load_deviations() -> set:
    deviations = set()
    if os.path.exists(DEV_PATH):
        with open(DEV_PATH) as fh:
            for raw in fh:
                line = raw.strip()
                if line.startswith("|") and ".rs" in line:
                    cells = [c.strip() for c in line.strip("|").split("|")]
                    if cells and cells[0]:
                        deviations.add(cells[0].strip("`"))
    return deviations


def main() -> int:
    with open(SUMMARY) as fh:
        data = json.load(fh)

    totals = data["data"][0]["totals"]
    line_pct = pct(totals["lines"])
    region_pct = pct(totals["regions"])
    func_pct = pct(totals["functions"])

    deviations = load_deviations()
    cwd = os.getcwd()

    documented = []
    undocumented = []
    for entry in data["data"][0].get("files", []):
        rel = os.path.relpath(entry.get("filename", ""), cwd)
        if not rel.startswith("crates/"):
            continue
        # Test, fuzz, and bench-harness fixtures are not gated.
        if "/tests/" in rel or "/benches/" in rel or rel.startswith(
            "crates/fuzz/"
        ):
            continue
        summary = entry.get("summary", {})
        fl = summary.get("lines", {}).get("percent", 100.0)
        ff = summary.get("functions", {}).get("percent", 100.0)
        threshold = tier_threshold(rel)
        if min(fl, ff) < threshold:
            listed = rel in deviations or any(
                rel.endswith(d) for d in deviations
            )
            row = (rel, tier_name(rel), threshold, fl, ff)
            (documented if listed else undocumented).append(row)

    with open(REPORT, "w") as fh:
        fh.write("workspace coverage (cargo-llvm-cov --features riak):\n")
        fh.write(f"  line:     {line_pct:6.2f}%\n")
        fh.write(f"  region:   {region_pct:6.2f}%\n")
        fh.write(f"  function: {func_pct:6.2f}%\n")
        fh.write(
            "\nTiers: core >= 95%, supporting >= 75%, tool >= 75%.\n"
        )
        if documented:
            fh.write("\nDocumented deviations below tier (warnings):\n")
            for rel, tier, thr, fl, ff in sorted(documented):
                fh.write(
                    f"  {rel} [{tier} {thr:.0f}%]: "
                    f"line {fl:.2f}% function {ff:.2f}%\n"
                )
        if undocumented:
            fh.write("\nUNDOCUMENTED modules below tier:\n")
            for rel, tier, thr, fl, ff in sorted(undocumented):
                fh.write(
                    f"  {rel} [{tier} {thr:.0f}%]: "
                    f"line {fl:.2f}% function {ff:.2f}%\n"
                )

    print(f"line:     {line_pct:6.2f}%")
    print(f"region:   {region_pct:6.2f}%")
    print(f"function: {func_pct:6.2f}%")
    print("tiers: core >= 95%, supporting >= 75%, tool >= 75%")

    if documented:
        print(
            f"\nwarning: {len(documented)} documented deviation(s) below tier"
        )
        for rel, tier, thr, fl, ff in sorted(documented):
            print(
                f"  WARN {rel} [{tier} {thr:.0f}%]: "
                f"line {fl:.2f}% function {ff:.2f}%"
            )

    if undocumented:
        print(
            f"\nerror: {len(undocumented)} module(s) below tier and"
            " undocumented:"
        )
        for rel, tier, thr, fl, ff in sorted(undocumented):
            print(
                f"  FAIL {rel} [{tier} {thr:.0f}%]: "
                f"line {fl:.2f}% function {ff:.2f}%"
            )

    if MODE == "report":
        print("\ncoverage_gate: report-only (mode=report)")
        return 0
    if undocumented:
        print(
            "\ncoverage_gate: FAIL -- modules below their tier threshold"
            " are neither covered nor documented as deviations"
        )
        return 1
    print(
        f"\ncoverage_gate: PASS (line {line_pct:.2f}%, "
        f"function {func_pct:.2f}%; all below-tier files are documented"
        " deviations)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
