#!/usr/bin/env bash
# Parity gate for the C -> Rust transliteration.
#
#   scripts/check_parity.sh             validate docs/transliteration/ledger.tsv
#   scripts/check_parity.sh --sync      regenerate the C inventory and merge it
#   scripts/check_parity.sh --summary   per-C-file progress table
#   scripts/check_parity.sh --file F    list the ledger rows for C file F
#
# With the C reference present ($DYNOMITE_C_REF, default ~/src/dynomite)
# the check also regenerates the inventory and fails when the committed
# ledger has drifted from it (a C symbol with no row, or a row for a symbol
# that no longer exists). Without the C tree it validates the committed
# ledger alone.
#
# Files listed in docs/transliteration/closed-files.txt must have zero
# `todo` rows; add a C file there when its stage closes.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
LEDGER_PY="$HERE/transliteration/ledger.py"
INVENTORY="$HERE/transliteration/c_inventory.sh"
CLOSED="$ROOT/docs/transliteration/closed-files.txt"
C_REF="${DYNOMITE_C_REF:-$HOME/src/dynomite}"
TMP="${TMPDIR:-/tmp}/check_parity.$$"
trap 'rm -f "$TMP" "$TMP.ledger"' EXIT

have_c_ref() { [ -d "$C_REF/src" ]; }

case "${1:-}" in
  --sync)
    have_c_ref || { echo "check_parity: --sync needs the C tree at $C_REF" >&2; exit 2; }
    DYNOMITE_C_REF="$C_REF" "$INVENTORY" > "$TMP"
    python3 "$LEDGER_PY" sync "$TMP"
    exit 0
    ;;
  --summary)
    python3 "$LEDGER_PY" summary
    exit 0
    ;;
  --file)
    python3 "$LEDGER_PY" file "${2:?--file needs a C path like src/dyn_mbuf.c}"
    exit 0
    ;;
  "") ;;
  *) echo "usage: $0 [--sync|--summary|--file C_FILE]" >&2; exit 2 ;;
esac

if have_c_ref; then
  # Drift check: syncing a scratch copy of the ledger against a fresh
  # inventory must not change it.
  DYNOMITE_C_REF="$C_REF" "$INVENTORY" > "$TMP"
  cp "$ROOT/docs/transliteration/ledger.tsv" "$TMP.ledger"
  LEDGER_PATH="$TMP.ledger" python3 "$LEDGER_PY" sync "$TMP" > /dev/null
  if ! cmp -s "$ROOT/docs/transliteration/ledger.tsv" "$TMP.ledger"; then
    echo "check_parity: ledger is out of date with the C inventory; run scripts/check_parity.sh --sync and commit" >&2
    diff "$ROOT/docs/transliteration/ledger.tsv" "$TMP.ledger" | head -20 >&2
    exit 1
  fi
else
  echo "check_parity: C tree not found at $C_REF; validating the committed ledger only"
fi

closed=()
if [ -f "$CLOSED" ]; then
  while IFS= read -r f; do
    case "$f" in ''|'#'*) ;; *) closed+=("$f") ;; esac
  done < "$CLOSED"
fi
python3 "$LEDGER_PY" check "${closed[@]}"
