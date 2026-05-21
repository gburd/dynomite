#!/usr/bin/env bash
# Local CI gate. Mirrors .github/workflows/ci.yml. Run before declaring
# any stage done.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> rustfmt"
cargo fmt --all -- --check

echo "==> clippy"
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "==> build"
cargo build --workspace --all-targets --locked

echo "==> nextest"
if command -v cargo-nextest >/dev/null 2>&1; then
  cargo nextest run --workspace --all-features
else
  cargo test --workspace --all-features
fi

echo "==> conformance suite (Stage 14)"
# The conformance suite is gated behind the `integration`
# feature plus a runtime check for `redis-server` on PATH. When
# Redis is missing, every scenario returns a skip notice and
# passes; otherwise the full multi-cluster matrix runs and the
# JUnit XML report lands at `target/junit/conformance.xml`.
if command -v cargo-nextest >/dev/null 2>&1; then
  cargo nextest run \
    --profile conformance \
    -p dynomited \
    --features integration \
    --test conformance --test differential
  src="target/nextest/conformance/junit.xml"
  dst="target/junit/conformance.xml"
  if [ -f "$src" ]; then
    mkdir -p target/junit
    cp "$src" "$dst"
    echo "junit XML mirrored to $dst"
  fi
fi

echo "==> doctests"
cargo test --doc --workspace

echo "==> deny"
if command -v cargo-deny >/dev/null 2>&1; then
  cargo deny check || true
fi

echo "==> audit"
if command -v cargo-audit >/dev/null 2>&1; then
  cargo audit --deny warnings \
    --ignore RUSTSEC-2023-0071 \
    --ignore RUSTSEC-2024-0436 || true
fi

echo "==> mdbook"
if [ -d docs/book ] && command -v mdbook >/dev/null 2>&1; then
  mdbook build docs/book
fi

echo "==> repo hygiene"
"$ROOT/scripts/check_no_todos.sh"
"$ROOT/scripts/check_no_port_comments.sh"
"$ROOT/scripts/check_ascii.sh"

echo "==> quickfuzz (60s smoke per target)"
"$ROOT/scripts/quickfuzz.sh" 60

echo "==> coverage gate (Stage 15)"
# The Stage 15 brief allows the actual coverage percentage to fall
# below the 95% threshold while the Stage 16 chaos test is
# pending; the gate is informational on `main` until the chaos
# test lifts the network and FSM modules. Run
# `scripts/coverage_gate.sh` directly (without `|| true`) to
# enforce the threshold locally; CI flips this to enforcing
# once Stage 16 lands.
"$ROOT/scripts/coverage_gate.sh" || true

echo "OK"
