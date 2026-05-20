#!/usr/bin/env bash
# Reject TODO/FIXME/unimplemented!/todo! markers in production Rust code.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [ ! -d crates ]; then
  exit 0
fi

PATTERN='\b(TODO|FIXME|XXX|unimplemented!|todo!\(|panic!\("TODO)'
HITS=$(grep -RInE "$PATTERN" \
  --include='*.rs' \
  --exclude-dir=target \
  crates 2>/dev/null || true)

if [ -n "$HITS" ]; then
  echo "Forbidden markers found in Rust sources:" >&2
  echo "$HITS" >&2
  exit 1
fi
