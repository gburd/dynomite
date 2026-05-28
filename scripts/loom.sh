#!/usr/bin/env bash
# Run loom-based concurrency model tests. Loom shadows std atomics
# with model-recording variants and runs the program-under-test
# under all legal interleavings; expensive but exhaustive.
#
# Run via:
#
#     bash scripts/loom.sh
#
# Pinned to release mode because loom builds are slow.
set -euo pipefail

cd "$(dirname "$0")/.."

# Increase the default per-test interleaving budget so we explore
# more of the model space. The default (LOOM_MAX_BRANCHES=1000) is
# enough for the small primitives in `crates/loom-tests`; raise it
# only when adding richer models.
: "${LOOM_MAX_BRANCHES:=1000}"
export LOOM_MAX_BRANCHES

# `RUSTDOCFLAGS` gets the same `--cfg loom` so the doctest
# binaries see the gate. The `throttle-core` doctest examples
# wrap their bodies in `#[cfg(not(loom))]` so that under loom
# they compile to a no-op `fn main() {}` rather than driving
# loom-instrumented atomics from outside `loom::model`.
RUSTFLAGS='--cfg loom' RUSTDOCFLAGS='--cfg loom' \
    cargo test -p loom-tests -p throttle-core --release "$@"
