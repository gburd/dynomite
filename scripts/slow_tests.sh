#!/usr/bin/env bash
# Slow-tests local runner. Mirrors what `.github/workflows/slow-tests.yml`
# and `.forgejo/workflows/slow-tests.yml` do. Run weekly in CI; run
# locally any time you want to exercise every `#[ignore]`'d test
# (notably the OTLP bytes-flow proof at
# `crates/dynomited/tests/observability.rs::otlp_grpc_bytes_reach_mock_listener`).
#
# Pass extra args to forward them to nextest (e.g. `-- --no-capture`).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
cargo nextest run --workspace --run-ignored only "$@"
cargo nextest run -p dynomited --features integration --test observability --run-ignored only "$@"
