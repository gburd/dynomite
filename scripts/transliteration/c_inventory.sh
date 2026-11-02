#!/usr/bin/env bash
# Inventory every symbol in the C Dynomite reference tree.
#
# Prints TSV: kind <TAB> c_symbol <TAB> c_file <TAB> c_line
# for functions, macros, structs, enums, enumerators, typedefs, unions,
# and file-scope variables under $DYNOMITE_C_REF/src. Generated
# Makefiles, autotools output, and contrib/ are excluded. The output is
# sorted and deterministic so the ledger can be diffed against it.
#
# Requires universal-ctags. When the binary on PATH is not
# universal-ctags (for example Emacs etags/ctags), the script re-runs
# itself under `nix shell nixpkgs#universal-ctags`.
set -euo pipefail

C_REF="${DYNOMITE_C_REF:-$HOME/src/dynomite}"
[ -d "$C_REF/src" ] || { echo "c_inventory: no C tree at $C_REF/src (set DYNOMITE_C_REF)" >&2; exit 2; }

if ! ctags --version 2>/dev/null | grep -q 'Universal Ctags'; then
  if [ -z "${C_INVENTORY_REEXEC:-}" ] && command -v nix >/dev/null 2>&1; then
    C_INVENTORY_REEXEC=1 exec nix shell nixpkgs#universal-ctags -c "$0" "$@"
  fi
  echo "c_inventory: universal-ctags is required" >&2
  exit 2
fi

cd "$C_REF"
find src -type f \( -name '*.c' -o -name '*.h' \) | LC_ALL=C sort | \
  xargs ctags -x --c-kinds=fdsgetuv --_xformat='%K	%N	%F	%n' 2>/dev/null | \
  awk -F'\t' '
    $1 == "function"   { k = "fn" }
    $1 == "macro"      { k = "macro" }
    $1 == "struct"     { k = "struct" }
    $1 == "enum"       { k = "enum" }
    $1 == "enumerator" { k = "enumerator" }
    $1 == "typedef"    { k = "typedef" }
    $1 == "union"      { k = "union" }
    $1 == "variable"   { k = "var" }
    k != "" { print k "\t" $2 "\t" $3 "\t" $4; k = "" }
  ' | LC_ALL=C sort -t'	' -k3,3 -k4,4n -k2,2 | LC_ALL=C sort -u
