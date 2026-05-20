#!/usr/bin/env bash
# Reject non-ASCII characters in source code, docs, and tests.
# Binary fixtures under tests/fixtures/ are exempt.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BAD=0
while IFS= read -r f; do
  if LC_ALL=C grep -nP '[^\x00-\x7F]' "$f" >/dev/null 2>&1; then
    echo "non-ASCII bytes in: $f" >&2
    LC_ALL=C grep -nP '[^\x00-\x7F]' "$f" >&2 || true
    BAD=1
  fi
done < <(
  find crates docs scripts \
    -type f \
    \( -name '*.rs' -o -name '*.md' -o -name '*.sh' -o -name '*.toml' -o -name '*.nix' \) \
    -not -path '*/target/*' \
    -not -path '*/fixtures/*' \
    -not -path '*/_/*' \
    2>/dev/null
)
[ -f README.md ]    && LC_ALL=C grep -nP '[^\x00-\x7F]' README.md    >&2 && BAD=1 || true
[ -f AGENTS.md ]    && LC_ALL=C grep -nP '[^\x00-\x7F]' AGENTS.md    >&2 && BAD=1 || true
[ -f PLAN.md ]      && LC_ALL=C grep -nP '[^\x00-\x7F]' PLAN.md      >&2 && BAD=1 || true

exit $BAD
