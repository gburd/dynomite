#!/usr/bin/env bash
# Reject comments that advertise this code as a port of the C source.
# Acknowledgement of the C origin lives only in README.md / NOTICE / LICENSE.
# Exempt by design: docs/transliteration/ (the C-to-Rust ledger) and
# crates/c-oracle/ (the test-only crate that compiles and calls the C), whose
# purpose is to name C files and functions.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -d crates ]; then
  exit 0
fi

PATTERN='ported from|port of|matches dyn_|matches the C|originally in dyn_|from twemproxy|[Mm]irrors `?dyn_|[Mm]irrors the `?dyn_|[Mm]irror[s]? .* dyn_|C reference engine|dyn_[a-z_]+\.c\b'
HITS=$(grep -RInE -i "$PATTERN" \
  --include='*.rs' \
  --include='*.md' \
  --exclude-dir=target \
  --exclude-dir=_ \
  --exclude='README.md' \
  --exclude='AGENTS.md' \
  --exclude='PLAN.md' \
  --exclude='CHANGELOG.md' \
  --exclude='parity.md' \
  --exclude='NOTICE' \
  --exclude='LICENSE' \
  --exclude-dir='journal' \
  --exclude-dir='transliteration' \
  --exclude-dir='c-oracle' \
  crates docs 2>/dev/null || true)

if [ -n "$HITS" ]; then
  echo "Port-acknowledgement comments found (only README/NOTICE/LICENSE may say this):" >&2
  echo "$HITS" >&2
  exit 1
fi
