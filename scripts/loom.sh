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

# Pass `--cfg loom` to both the compiler and rustdoc so that
# doctests that gate themselves on `cfg(loom)` see the flag and
# compile to the no-op branch under loom (throttle-core +
# hashtree both gate their doctest bodies on `cfg(not(loom))`
# so they don't drive loom-shadowed atomics from outside a
# `loom::model` closure).
RUSTFLAGS='--cfg loom' RUSTDOCFLAGS='--cfg loom' \
    cargo test -p loom-tests -p throttle-core -p hashtree --release "$@"
