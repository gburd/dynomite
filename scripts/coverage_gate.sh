#!/usr/bin/env bash
# Stage 15 coverage gate.
#
# Runs `cargo llvm-cov --workspace --features riak --json --summary-only`
# and applies a tiered per-file threshold (core 95%, supporting/tool
# 75%) and fails when any module below its tier that is not explicitly listed in
# `docs/coverage-deviations.md`.
#
# Output:
#   target/coverage/summary.json   - the raw cargo-llvm-cov summary
#   target/coverage/report.txt     - human-readable summary
#
# Usage:
#   scripts/coverage_gate.sh             # run + enforce 95% threshold
#   scripts/coverage_gate.sh --report    # run, write report, do not enforce
#
# When `cargo-llvm-cov` is missing (e.g. fresh dev shell), the script
# logs a SKIP and exits 0 so the surrounding gate stays green; CI
# always installs the tool via the Nix flake.
set -euo pipefail

THRESHOLD=95
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mode="enforce"
if [ "${1:-}" = "--report" ]; then
  mode="report"
fi

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  echo "coverage_gate: cargo-llvm-cov not on PATH; skipping coverage gate."
  exit 0
fi

mkdir -p target/coverage

echo "==> cargo llvm-cov --workspace --features riak --summary-only --json"
# Test output goes to a log rather than /dev/null: when a test fails
# under instrumentation the gate must say which one.
if ! cargo llvm-cov --workspace --features riak --summary-only --json \
    --output-path target/coverage/summary.json \
    >target/coverage/test-output.log 2>&1; then
    echo "coverage_gate: the instrumented test run failed:" >&2
    grep -aE '\.\.\. FAILED|panicked at|test failed|^failures:|^    [a-z_:]+$' \
        target/coverage/test-output.log | head -40 >&2
    exit 1
fi

DYNOMITE_COV_THRESHOLD="$THRESHOLD" \
DYNOMITE_COV_MODE="$mode" \
exec python3 "$ROOT/scripts/coverage_gate.py"
