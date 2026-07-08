#!/usr/bin/env bash
# Render the mdBook with forge-correct repository links.
#
# The book source uses a `DYN_SRC_BASE/<path>` sentinel for every link
# into the repository tree. GitHub and Codeberg spell source-file URLs
# differently:
#
#   Codeberg: https://codeberg.org/gregburd/dynomite/src/branch/main/<path>
#   GitHub:   https://github.com/gburd/dynomite/blob/main/<path>
#
# The Codeberg auto-mirror keeps the two trees identical, so a reader on
# either Pages site should follow source links to the forge they are
# already on. This script substitutes the sentinel (and book.toml's
# repository / edit URLs) for the requested forge into a throwaway copy
# of the book, builds it, and leaves the checked-in source untouched.
#
# Usage:
#   scripts/render-book.sh <forge> [build-dir]
#     forge      = codeberg | github   (default: codeberg)
#     build-dir  = output dir override  (default: book.toml's build-dir)
#
# Both CI docs workflows call this; `scripts/check.sh` calls it with the
# default (codeberg) so a local/CI gate build resolves the sentinel too.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FORGE="${1:-codeberg}"

case "$FORGE" in
  codeberg)
    SRC_BASE="https://codeberg.org/gregburd/dynomite/src/branch/main"
    REPO_URL="https://codeberg.org/gregburd/dynomite"
    EDIT_URL="https://codeberg.org/gregburd/dynomite/_edit/main/docs/book/{path}"
    ;;
  github)
    SRC_BASE="https://github.com/gburd/dynomite/blob/main"
    REPO_URL="https://github.com/gburd/dynomite"
    EDIT_URL="https://github.com/gburd/dynomite/edit/main/docs/book/{path}"
    ;;
  *)
    echo "render-book: unknown forge '$FORGE' (want codeberg|github)" >&2
    exit 2
    ;;
esac

SRCBOOK="$ROOT/docs/book"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp -a "$SRCBOOK/." "$WORK/"

# Substitute the source-link sentinel throughout the book source.
# The sentinel is `DYN_SRC_BASE/<path>` -> `<SRC_BASE>/<path>`.
grep -rlF 'DYN_SRC_BASE/' "$WORK/src" 2>/dev/null | while IFS= read -r f; do
  sed -i "s|DYN_SRC_BASE/|${SRC_BASE}/|g" "$f"
done

# Point book.toml's repository icon link and edit-this-page link at the
# same forge.
sed -i \
  -e "s|^git-repository-url = .*|git-repository-url = \"${REPO_URL}\"|" \
  -e "s|^edit-url-template = .*|edit-url-template = \"${EDIT_URL}\"|" \
  "$WORK/book.toml"

# Build. Honour an explicit output dir if given; otherwise the book.toml
# build-dir (relative to the book root) is used, resolved against the
# real book dir so the output lands where callers expect.
if [ -n "${2:-}" ]; then
  MDBOOK_BUILD__BUILD_DIR="$2" mdbook build "$WORK"
else
  # book.toml build-dir is `../../target/book` relative to docs/book;
  # from the temp dir that would point elsewhere, so pin it to the repo.
  MDBOOK_BUILD__BUILD_DIR="$ROOT/target/book" mdbook build "$WORK"
fi

echo "render-book: built $FORGE book -> ${2:-$ROOT/target/book}"
