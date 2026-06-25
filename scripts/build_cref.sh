#!/usr/bin/env bash
# Build the Netflix dynomite C reference for the Stage 14
# differential rig.
#
# This script is opt-in: the default workspace build does not
# require it. Run it explicitly when reviving the differential
# corpus, or trigger it via DYNOMITE_DIFFERENTIAL=1 in
# scripts/check.sh.
#
# Behaviour
# ---------
# * Locates the upstream Netflix dynomite C source via the
#   `DYNOMITE_C_REF` environment variable (a local checkout of
#   the upstream repository). The C tree is no longer vendored
#   in this repository (it was removed in commit `2561d13`); see
#   AGENTS.md Section 2.
# * Mirrors the source tree into `target/cref/build/` so the
#   checkout itself is never modified.
# * Runs `autoreconf -fvi` and `./configure` inside the mirror,
#   then a partial `make` that builds the supporting static
#   archives and finally the `dynomite` binary. The C code
#   pre-dates GCC 10's `-fno-common` default and clang 18's
#   stricter implicit-declaration handling, so we relax CFLAGS
#   accordingly. We do not patch any source.
# * Prints the absolute path of the produced binary on stdout
#   and writes the same path to `target/cref/path` so the
#   differential test rig can pick it up without a follow-up
#   `eval` step.
#
# Build deps (provisioned by flake.nix):
#   autoconf, automake, libtool, pkg-config, openssl
#   gcc (the GCC wrapper from nixpkgs supplies the linker too).
#
# Exit codes
#   0  success; binary path on stdout.
#   1  DYNOMITE_C_REF unset or does not point at a C source tree.
#   2  configure step failed.
#   3  make step failed.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

C_REF="${DYNOMITE_C_REF:-}"
TARGET_DIR="$ROOT/target/cref"
BUILD_DIR="$TARGET_DIR/build"
PATH_FILE="$TARGET_DIR/path"
BIN_REL="src/dynomite"
LOG_DIR="$TARGET_DIR/logs"

note() { echo "[build_cref] $*" >&2; }

if [ -z "$C_REF" ]; then
  echo "build_cref: DYNOMITE_C_REF is unset. The C reference is no longer" >&2
  echo "  vendored in this repository; point DYNOMITE_C_REF at a local" >&2
  echo "  checkout of the upstream Netflix dynomite repository." >&2
  exit 1
fi
if [ ! -f "$C_REF/configure.ac" ]; then
  echo "build_cref: $C_REF does not look like a dynomite C source tree" >&2
  echo "  (no configure.ac). Set DYNOMITE_C_REF to the repository root." >&2
  exit 1
fi

mkdir -p "$TARGET_DIR" "$LOG_DIR"

# Re-mirror the source when:
#   * the build dir does not exist, or
#   * the source HEAD differs from the cached marker, or
#   * the cached binary is missing.
SHA="$(git -C "$C_REF" rev-parse HEAD 2>/dev/null || echo unknown)"
MARKER="$TARGET_DIR/source.sha"
NEED_REMIRROR=0
if [ ! -d "$BUILD_DIR" ]; then
  NEED_REMIRROR=1
elif [ ! -f "$MARKER" ] || [ "$(cat "$MARKER")" != "$SHA" ]; then
  NEED_REMIRROR=1
elif [ ! -x "$BUILD_DIR/$BIN_REL" ]; then
  NEED_REMIRROR=1
fi

if [ "$NEED_REMIRROR" -eq 1 ]; then
  note "mirroring source from $C_REF into $BUILD_DIR (sha $SHA)"
  if [ -d "$BUILD_DIR" ]; then
    chmod -R u+w "$BUILD_DIR" 2>/dev/null || true
    find "$BUILD_DIR" -mindepth 1 -delete
  else
    mkdir -p "$BUILD_DIR"
  fi
  # Copy the source contents (preserve attributes); then delete
  # the .git pointer so autotools cannot touch the source repo.
  (cd "$C_REF" && tar cf - .) | (cd "$BUILD_DIR" && tar xf -)
  rm -rf "$BUILD_DIR/.git"
fi

CFLAGS_LAX=(
  -g
  -O2
  -D_GNU_SOURCE
  # GCC 10+ defaults to -fno-common; the C tree relies on the
  # legacy "common" symbol semantics for the C2G_*/G2C_* ring
  # queues declared without `extern` in dyn_ring_queue.h.
  -fcommon
  # The C tree pre-dates the modern warning -> error promotion
  # for these patterns. We are not modifying the submodule, so
  # we relax the front-end here. The Rust port is the source of
  # truth; this build only produces a reference oracle.
  -Wno-error
  -Wno-int-conversion
  -Wno-incompatible-pointer-types
  -Wno-implicit-function-declaration
)

cd "$BUILD_DIR"

if [ ! -f "$BIN_REL" ] || [ "$NEED_REMIRROR" -eq 1 ]; then
  note "running autoreconf -fvi"
  if ! autoreconf -fvi >"$LOG_DIR/autoreconf.log" 2>&1; then
    echo "build_cref: autoreconf failed; see $LOG_DIR/autoreconf.log" >&2
    exit 2
  fi
  note "running ./configure --enable-debug=full"
  if ! CC="${CC:-gcc}" CFLAGS="${CFLAGS_LAX[*]}" \
       ./configure --enable-debug=full >"$LOG_DIR/configure.log" 2>&1; then
    echo "build_cref: configure failed; see $LOG_DIR/configure.log" >&2
    exit 2
  fi
fi

# Build supporting static archives, then the dynomite binary.
# We avoid the top-level `make all` because contrib/yaml-0.1.4's
# test suite chokes on Nix's hardened gcc wrapper (it injects
# `-Wformat-security` without `-Wformat`), and src/tools has a
# header-only mismatch with modern gcc that does not affect the
# main binary. The fine-grained sequence below produces the
# same `src/dynomite` artefact as `build.sh` would on a non-Nix
# host.
note "building yaml lib"
if ! make -C contrib/yaml-0.1.4/src \
       CFLAGS="${CFLAGS_LAX[*]}" >"$LOG_DIR/yaml.log" 2>&1; then
  echo "build_cref: yaml lib failed; see $LOG_DIR/yaml.log" >&2
  exit 3
fi
for d in src/hashkit src/proto src/event src/entropy src/seedsprovider; do
  note "building $d"
  if ! make -C "$d" \
         CFLAGS="${CFLAGS_LAX[*]}" >"$LOG_DIR/$(basename "$d").log" 2>&1; then
    echo "build_cref: $d failed; see $LOG_DIR/$(basename "$d").log" >&2
    exit 3
  fi
done
note "linking src/dynomite"
if ! make -C src dynomite \
       CFLAGS="${CFLAGS_LAX[*]}" >"$LOG_DIR/dynomite.log" 2>&1; then
  echo "build_cref: link failed; see $LOG_DIR/dynomite.log" >&2
  exit 3
fi

ABS_BIN="$BUILD_DIR/$BIN_REL"
if [ ! -x "$ABS_BIN" ]; then
  echo "build_cref: expected binary at $ABS_BIN but none found" >&2
  exit 3
fi

echo "$SHA" >"$MARKER"
echo "$ABS_BIN" >"$PATH_FILE"
note "binary at $ABS_BIN"
note "wrote path to $PATH_FILE"
echo "$ABS_BIN"
