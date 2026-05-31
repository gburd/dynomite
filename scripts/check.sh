#!/usr/bin/env bash
# Local CI gate. Mirrors .github/workflows/ci.yml. Run before declaring
# any stage done.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> rustfmt"
# Restrict to our crates so the noxu path-deps pulled in by the
# `riak-storage` feature do not leak into the format check.
cargo fmt \
    -p dynomite -p dynomited -p dyn-hash-tool \
    -p dyn-encoding -p dyniak -p dyn-admin \
    -p dyntext -p dynvec -p gen-fsm -p hashtree \
    -p loom-tests -p sup -p throttle-core \
    -- --check

echo "==> clippy"
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "==> build"
cargo build --workspace --all-targets --locked

echo "==> build (--features riak)"
# Build verification only: confirm the optional Riak protocol
# surface compiles cleanly. The full Riak integration test
# matrix runs under the regular nextest pass below when
# `--features riak` is requested explicitly.
cargo build -p dynomited --features riak --all-targets --locked

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
#
# The `conformance`, `differential`, and `integration` binaries
# spawn real `dynomited` and `redis-server` processes and are
# excluded from the default profile (see `.config/nextest.toml`)
# so the parallel default run does not race them. They run here
# under the `conformance` profile (test-threads=1).
#
# The differential rig at crates/dynomited/tests/differential.rs
# additionally compares the Rust dynomited against the C
# dynomite reference when the latter is available. The C
# binary is NOT built by default: it is opt-in via the
# `DYNOMITE_DIFFERENTIAL` environment variable. When set to a
# non-empty value we invoke `scripts/build_cref.sh` first,
# which materialises the binary under `target/cref/build/src/`
# and writes its absolute path to `target/cref/path` for the
# test rig to pick up. Without that flag the rig still runs
# but skips the C-side comparison.
if [ -n "${DYNOMITE_DIFFERENTIAL:-}" ]; then
  echo "   DYNOMITE_DIFFERENTIAL=$DYNOMITE_DIFFERENTIAL set; building C reference"
  "$ROOT/scripts/build_cref.sh" >/dev/null
fi
if command -v cargo-nextest >/dev/null 2>&1; then
  cargo nextest run \
    --profile conformance \
    -p dynomited \
    --features integration \
    --test conformance --test differential --test integration
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

echo "==> conformance suite (Stage 14, --features integration)"
# AGENTS.md Section 14b lists the conformance suite as one of
# the gates both runners exercise. The `dynomited` integration
# feature spawns a real `redis-server` from PATH; when the
# binary is missing the tests skip themselves rather than
# fail. We invoke nextest only when both `cargo-nextest` and
# `redis-server` are available; otherwise we surface a notice
# and continue.
#
# This pass uses the `conformance` profile so the
# process-spawning binaries (excluded from the default profile)
# actually execute, and so the `test-threads=1` setting prevents
# the ephemeral-port races that otherwise made these tests
# load-correlated flakes (F9 in
# `docs/journal/2026-05-23-audit.md`).
if command -v cargo-nextest >/dev/null 2>&1 \
   && command -v redis-server >/dev/null 2>&1; then
    cargo nextest run --profile conformance --workspace --features integration
else
    echo "   (skipped: cargo-nextest and/or redis-server not on PATH)"
fi

echo "==> cleanup-sweep (Stage 16)"
"$ROOT/scripts/check_clean.sh"

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

echo "==> note: slow-tests are in scripts/slow_tests.sh; run weekly via slow-tests.yml workflow"

echo "OK"
