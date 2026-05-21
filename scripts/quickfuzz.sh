#!/usr/bin/env bash
# Run every cargo-fuzz target for a short duration and exit non-zero
# on the first finding. Wired into scripts/check.sh so CI exercises
# every parser on every push.
#
# Usage:
#   scripts/quickfuzz.sh                # default 60 seconds per target
#   scripts/quickfuzz.sh 600            # 10 minutes per target
#   scripts/quickfuzz.sh 60 dnode_parse # one target only
#
# When `cargo-fuzz` is not installed, the script logs a SKIP and
# exits 0 so the surrounding gate stays green on developer machines
# without nightly. CI installs cargo-fuzz and exercises the full
# matrix.
set -euo pipefail

DURATION="${1:-60}"
shift || true

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FUZZ_DIR="$ROOT/crates/fuzz"
SEEDS_ROOT="$FUZZ_DIR/seeds"

ALL_TARGETS=(
  "proto_redis_parse"
  "proto_redis_parse_rsp"
  "proto_memcache_parse"
  "proto_memcache_parse_rsp"
  "dnode_parse"
  "conf_parse"
  "crypto_aes_decrypt"
)

if [ "$#" -gt 0 ]; then
  TARGETS=("$@")
else
  TARGETS=("${ALL_TARGETS[@]}")
fi

if ! command -v cargo-fuzz >/dev/null 2>&1; then
  echo "quickfuzz: cargo-fuzz not on PATH; skipping fuzz smoke (CI installs it)."
  exit 0
fi

cd "$ROOT"

failed=0
for target in "${TARGETS[@]}"; do
  echo "==> quickfuzz $target ($DURATION s)"
  seeds="$SEEDS_ROOT/$target"
  if [ ! -d "$seeds" ]; then
    echo "quickfuzz: missing seed directory $seeds" >&2
    failed=1
    break
  fi
  if ! cargo +nightly fuzz run --fuzz-dir "$FUZZ_DIR" "$target" "$seeds" \
      -- -max_total_time="$DURATION" -timeout=10 2>&1 | tee "target/quickfuzz-$target.log"
  then
    echo "quickfuzz: $target FAILED" >&2
    failed=1
    break
  fi
done

if [ "$failed" -ne 0 ]; then
  echo "quickfuzz: at least one target reported a finding" >&2
  exit 1
fi

echo "quickfuzz: all targets clean"
