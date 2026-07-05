#!/usr/bin/env bash
# Run stateright explicit-state model checks for the distributed
# protocols (XA two-phase commit, quorum decision, ring routing,
# gossip convergence, SWIM + Lifeguard membership). The models are
# abstract state machines that reproduce the production decision
# logic and assert its safety and liveness invariants; see
# crates/model-tests for the mapping from each model to the source
# file it abstracts. The SWIM model additionally carries the
# completeness / accuracy-low-false-positive / dissemination
# invariants, a comparative assertion against a naive fixed-timeout
# detector, and a negative control that reproduces a false permanent
# death when incarnation refutation is disabled.
#
# Run via:
#
#     bash scripts/model.sh
#
# These checks are gated out of the default fast test pass (the
# crate is a non-default workspace member and is not built by a bare
# `cargo nextest run --workspace` unless named). They run in CI's
# slow-tests lane.
#
# CI / quick gate (the default): each model is checked to a bounded
# state-space depth that finishes in a few seconds. The bounds are
# baked into the #[test]s (small participant / replica / node counts
# and a tight fault budget), so a plain test run is the CI gate.
#
# Soak lane: to exhaustively explore deeper, raise the bounds by
# editing the model parameters (e.g. `Xa { rms: 3, faults: 3, .. }`)
# or run the stateright explorer for one model interactively. For a
# longer deterministic pass, run the suite repeatedly under release
# mode:
#
#     MODEL_SOAK=1 bash scripts/model.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${MODEL_SOAK:-0}" == "1" ]]; then
    # Soak: release mode (faster BFS) and a repeated run to surface
    # any nondeterminism in the checker's parallel exploration.
    for _ in 1 2 3; do
        cargo test -p model-tests --release "$@"
    done
else
    cargo test -p model-tests "$@"
fi
