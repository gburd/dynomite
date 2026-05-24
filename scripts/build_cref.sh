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
# * Initialises the `_/dynomite` git submodule (idempotent).
# * Mirrors the source tree into `target/cref/build/` so the
#   submodule itself is never modified (the submodule is
#   informational only; AGENTS.md forbids in-tree edits).
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
#   1  submodule init failed.
#   2  configure step failed.
#   3  make step failed.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SUBMODULE="_/dynomite"
TARGET_DIR="$ROOT/target/cref"
BUILD_DIR="$TARGET_DIR/build"
PATH_FILE="$TARGET_DIR/path"
BIN_REL="src/dynomite"
LOG_DIR="$TARGET_DIR/logs"

note() { echo "[build_cref] $*" >&2; }

note "ensuring submodule $SUBMODULE is initialised"
if ! git submodule update --init --recursive "$SUBMODULE" >&2; then
  echo "build_cref: submodule update failed" >&2
  exit 1
fi

if [ ! -f "$SUBMODULE/configure.ac" ]; then
  echo "build_cref: $SUBMODULE missing configure.ac after submodule init" >&2
  exit 1
fi

mkdir -p "$TARGET_DIR" "$LOG_DIR"

# Re-mirror the submodule when:
#   * the build dir does not exist, or
#   * the submodule's HEAD differs from the cached marker, or
#   * the cached binary is missing.
SHA="$(git -C "$SUBMODULE" rev-parse HEAD)"
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
  note "mirroring source into $BUILD_DIR (sha $SHA)"
  if [ -d "$BUILD_DIR" ]; then
    chmod -R u+w "$BUILD_DIR" 2>/dev/null || true
    find "$BUILD_DIR" -mindepth 1 -delete
  else
    mkdir -p "$BUILD_DIR"
  fi
  # Copy the submodule contents (preserve attributes); then
  # delete the .git pointer file so autotools cannot try to
  # touch the parent repo.
  (cd "$SUBMODULE" && tar cf - .) | (cd "$BUILD_DIR" && tar xf -)
  rm -f "$BUILD_DIR/.git"
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
