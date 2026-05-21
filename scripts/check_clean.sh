#!/usr/bin/env bash
# scripts/check_clean.sh - Stage 16 cleanup-sweep gate.
#
# Asserted invariants (AGENTS.md Section 14c):
#
#   1. No tracked file lives under a path matched by .gitignore
#      (committed-into-gitignored is the most common drift).
#   2. target/chaos/<run-id>/ is never committed; the curated
#      report lives under dist/chaos-reports/ instead.
#   3. crates/fuzz/corpus/ and crates/fuzz/artifacts/ are never
#      committed; only crates/fuzz/seeds/ ships in the tree.
#   4. The pi-tool's .pi/ scratch directory has no tracked
#      contents.
#   5. No tracked file matches the agent-cruft glob list
#      (target/, result, .direnv, .cargo/registry, .cargo/git,
#      *.swp, *.bak, *.orig, .aider*, .history,
#      docs/journal/scratch/).
#
# Runs read-only; suitable as the final gate inside
# scripts/check.sh and the `pre-tag` workflow on both the
# GitHub and Forgejo runners.
#
# Exit codes:
#   0 - clean
#   1 - violations found (printed to stderr)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

violations=0

emit() {
    printf 'check_clean: %s\n' "$1" >&2
    violations=$((violations + 1))
}

# --- 1. tracked files under gitignored paths ---------------------
#
# `git ls-files -ci --exclude-standard` lists every tracked
# (`-c`) entry that is also ignored (`-i`) by the standard
# exclude rules. Negation patterns (`!foo`) un-ignore the
# entry, so this lookup is the right shape: it surfaces the
# committed-into-gitignored case while leaving deliberately
# whitelisted artefacts (e.g. the bundled C reference's
# `_/dynomite/contrib/yaml-0.1.4.tar.gz`, kept via `!`) clean.
tracked_under_ignored=$(git ls-files -ci --exclude-standard 2>/dev/null || true)
if [ -n "$tracked_under_ignored" ]; then
    emit "tracked files matched by .gitignore:"
    printf '%s\n' "$tracked_under_ignored" >&2
fi

# --- 2. target/chaos/ never committed ----------------------------
chaos_committed=$(git ls-files 'target/chaos/*' || true)
if [ -n "$chaos_committed" ]; then
    emit "target/chaos/ contents committed (must live under dist/chaos-reports/ instead):"
    printf '%s\n' "$chaos_committed" >&2
fi

# --- 3. fuzz corpus / artifacts never committed ------------------
fuzz_corpus=$(git ls-files 'crates/fuzz/corpus/*' 'crates/fuzz/artifacts/*' || true)
if [ -n "$fuzz_corpus" ]; then
    emit "crates/fuzz/{corpus,artifacts} contents committed (only seeds/ may ship):"
    printf '%s\n' "$fuzz_corpus" >&2
fi

# --- 4. .pi/ scratch never committed -----------------------------
pi_committed=$(git ls-files '.pi/*' || true)
if [ -n "$pi_committed" ]; then
    emit ".pi/ scratch directory has tracked contents:"
    printf '%s\n' "$pi_committed" >&2
fi

# --- 5. agent-cruft glob list ------------------------------------
#
# These globs cover the agent-side scratch detritus that
# .gitignore catches but a stray `git add -f` could still slip
# in. We enumerate them explicitly so the gate fails closed
# even if a future .gitignore edit drops one of the entries.
cruft_globs=(
    '*.swp'
    '*.bak'
    '*.orig'
    '*.rs.bk'
    'Cargo.lock.bak'
    'result'
    'result-*'
    '.aider*'
    '.direnv/*'
    '.history/*'
    'docs/journal/scratch/*'
    '.cargo/registry/*'
    '.cargo/git/*'
)
cruft_committed=$(git ls-files -- "${cruft_globs[@]}" 2>/dev/null || true)
if [ -n "$cruft_committed" ]; then
    emit "agent-cruft files committed:"
    printf '%s\n' "$cruft_committed" >&2
fi

# --- 6. target/ tree never committed -----------------------------
#
# .gitignore catches `target/` at the repo root; we check the
# nested form too (e.g. `crates/dynomite/target/`) which the
# repo-root rule would not match.
target_committed=$(git ls-files '*/target/*' 'target/*' 2>/dev/null || true)
if [ -n "$target_committed" ]; then
    emit "target/ tree files committed:"
    printf '%s\n' "$target_committed" >&2
fi

if [ "$violations" -ne 0 ]; then
    printf 'check_clean: %d violation(s)\n' "$violations" >&2
    exit 1
fi

printf 'check_clean: ok\n'
exit 0
