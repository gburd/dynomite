#!/usr/bin/env bash
#
# Build the C dynomite reference binary on a remote chaos host.
#
# Phase 1 of P3-3.9: each chaos host needs a working C
# `dynomite` binary so the differential rig can drive both the
# Rust and C clusters with the same workload. The local lead
# host already has `scripts/build_cref.sh`; this script does
# the equivalent on each remote, and is idempotent against the
# submodule's git commit hash.
#
# Usage:
#   bash scripts/chaos-multi-host/build_cref_remote.sh [--clean] <host>
#
# <host> is one of:
#   floki | local           -- build locally, no SSH
#   arnold                  -- ssh arnold
#   nuc                     -- ssh gburd@nuc -o ProxyJump=arnold
#   meh                     -- ssh meh
#   <other>                 -- passed to ssh as-is
#
# Flags:
#   --clean   wipe /scratch/dynomite-chaos/cref-{src,build} first
#
# Environment:
#   SSH_KEY              override default SSH key
#   ROOT_REMOTE          override default remote install root
#                        (default /scratch/dynomite-chaos)
#   CFLAGS_EXTRA         appended to the relaxed CFLAGS used
#                        by autotools' configure step.
#
# Layout on the remote:
#   $ROOT_REMOTE/cref-src/    rsynced copy of _/dynomite
#   $ROOT_REMOTE/cref-build/  produced binary + build state
#                             dynomite          -> the binary
#                             .source-sha       -> submodule HEAD
#                             logs/*.log        -> per-step logs
#
# Exit codes:
#   0  binary built (or skipped because cache was current)
#   1  prerequisite missing on remote, or rsync failed
#   2  configure step failed
#   3  make step failed
#   4  invalid arguments

set -euo pipefail

usage() {
    cat <<'USAGE' >&2
usage: build_cref_remote.sh [--clean] <host>

  <host> := floki | local | arnold | nuc | meh | <ssh-spec>
  --clean wipes the remote build tree before rebuilding.

USAGE
}

CLEAN=0
HOST=""
while [ $# -gt 0 ]; do
    case "$1" in
        --clean) CLEAN=1; shift ;;
        -h|--help) usage; exit 0 ;;
        --) shift; break ;;
        -*) echo "unknown flag: $1" >&2; usage; exit 4 ;;
        *)
            if [ -n "$HOST" ]; then
                echo "extra argument: $1" >&2
                usage
                exit 4
            fi
            HOST="$1"
            shift
            ;;
    esac
done

if [ -z "$HOST" ]; then
    usage
    exit 4
fi

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
SUBMODULE="$REPO/_/dynomite"
ROOT_REMOTE="${ROOT_REMOTE:-/scratch/dynomite-chaos}"
SRC_DIR_REMOTE="$ROOT_REMOTE/cref-src"
BUILD_DIR_REMOTE="$ROOT_REMOTE/cref-build"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"

note() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

# Resolve <host> token to:
#   IS_LOCAL  -- 1 if the build runs on this host directly
#   SSH_CMD   -- ssh argv array (empty when local)
#   RSYNC_E   -- rsync -e string (unused when local)
#   RSYNC_DST -- rsync destination prefix (unused when local)
SSH_BASE_OPTS=(-o IdentitiesOnly=yes -i "$SSH_KEY"
               -o ControlMaster=no -o ControlPath=none
               -o StrictHostKeyChecking=accept-new
               -o ServerAliveInterval=30)
IS_LOCAL=0
SSH_CMD=()
RSYNC_E=""
RSYNC_DST=""
case "$HOST" in
    floki|local|localhost)
        IS_LOCAL=1
        ;;
    arnold)
        SSH_CMD=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" arnold)
        RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
        RSYNC_DST="arnold"
        ;;
    nuc)
        SSH_CMD=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" -o ProxyJump=arnold gburd@nuc)
        RSYNC_E="ssh ${SSH_BASE_OPTS[*]} -o ProxyJump=arnold"
        RSYNC_DST="gburd@nuc"
        ;;
    meh)
        SSH_CMD=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" meh)
        RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
        RSYNC_DST="meh"
        ;;
    *)
        # Free-form ssh spec: e.g. user@host or just a name in
        # ~/.ssh/config. Pass through verbatim.
        SSH_CMD=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" "$HOST")
        RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
        RSYNC_DST="$HOST"
        ;;
esac

# Ensure the local submodule is initialised. The remote build
# uses the rsynced copy of this tree, so a missing submodule is
# a hard error.
if [ ! -f "$SUBMODULE/configure.ac" ]; then
    note "initialising submodule _/dynomite"
    if ! git -C "$REPO" submodule update --init --recursive _/dynomite >&2; then
        echo "build_cref_remote: submodule init failed" >&2
        exit 1
    fi
fi

LOCAL_SHA="$(git -C "$SUBMODULE" rev-parse HEAD)"
note "local submodule HEAD: $LOCAL_SHA"

# Remote (or local) prerequisite probe. The probe runs the
# same shell snippet on either path; the snippet writes a
# single line of OS info and exits non-zero if any required
# tool is missing.
PREREQ_SNIPPET=$(cat <<'PREREQ'
set -u
need() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "MISSING $1" >&2
        return 1
    fi
}
miss=0
for t in autoreconf automake autoconf libtool pkg-config make rsync sha256sum; do
    case "$t" in
        sha256sum)
            command -v sha256sum >/dev/null 2>&1 \
                || command -v sha256 >/dev/null 2>&1 \
                || command -v shasum >/dev/null 2>&1 \
                || { echo "MISSING sha256sum-or-equivalent" >&2; miss=1; }
            ;;
        *)
            need "$t" || miss=1
            ;;
    esac
done
if ! command -v cc >/dev/null 2>&1 && ! command -v gcc >/dev/null 2>&1 && ! command -v clang >/dev/null 2>&1; then
    echo "MISSING C compiler (cc/gcc/clang)" >&2
    miss=1
fi
# OpenSSL headers: prefer pkg-config, fall back to header probe.
if pkg-config --exists openssl 2>/dev/null; then
    :
else
    found=0
    for d in /usr/include /usr/local/include /opt/homebrew/include /nix/var/nix/profiles/default/include; do
        if [ -f "$d/openssl/ssl.h" ]; then
            found=1
            break
        fi
    done
    if [ "$found" -ne 1 ]; then
        echo "MISSING openssl development headers (libssl-dev / openssl-devel / pkgsrc openssl)" >&2
        miss=1
    fi
fi
uname -srm
exit "$miss"
PREREQ
)

note "checking prerequisites on $HOST"
if [ "$IS_LOCAL" -eq 1 ]; then
    if ! bash -s <<<"$PREREQ_SNIPPET"; then
        echo "build_cref_remote: $HOST is missing required build tools" >&2
        echo "build_cref_remote: install autoconf automake libtool pkg-config and openssl headers" >&2
        exit 1
    fi
else
    if ! "${SSH_CMD[@]}" bash -s <<<"$PREREQ_SNIPPET"; then
        echo "build_cref_remote: $HOST is missing required build tools" >&2
        echo "build_cref_remote: install autoconf automake libtool pkg-config and openssl headers" >&2
        exit 1
    fi
fi

# Cache check: if --clean is unset, ask the remote for the
# cached source-sha. If it matches LOCAL_SHA AND the binary
# exists, we can short-circuit.
SKIP_BUILD=0
if [ "$CLEAN" -eq 0 ]; then
    CACHE_SNIPPET=$(cat <<CACHE
set -u
sha_file='$BUILD_DIR_REMOTE/.source-sha'
bin_file='$BUILD_DIR_REMOTE/dynomite'
if [ -f "\$sha_file" ] && [ -x "\$bin_file" ]; then
    cat "\$sha_file"
else
    echo "no-cache"
fi
CACHE
)
    if [ "$IS_LOCAL" -eq 1 ]; then
        REMOTE_SHA="$(bash -s <<<"$CACHE_SNIPPET" 2>/dev/null || echo "no-cache")"
    else
        REMOTE_SHA="$("${SSH_CMD[@]}" bash -s <<<"$CACHE_SNIPPET" 2>/dev/null || echo "no-cache")"
    fi
    REMOTE_SHA="$(printf '%s' "$REMOTE_SHA" | tr -d '\r\n ')"
    if [ "$REMOTE_SHA" = "$LOCAL_SHA" ]; then
        note "cache hit on $HOST (sha=$LOCAL_SHA); skipping build"
        SKIP_BUILD=1
    else
        note "cache miss on $HOST (have='$REMOTE_SHA', want='$LOCAL_SHA')"
    fi
fi

# Wipe build tree on --clean.
if [ "$CLEAN" -eq 1 ]; then
    note "--clean: wiping $SRC_DIR_REMOTE and $BUILD_DIR_REMOTE on $HOST"
    WIPE_SNIPPET=$(cat <<WIPE
rm -rf '$SRC_DIR_REMOTE' '$BUILD_DIR_REMOTE'
mkdir -p '$SRC_DIR_REMOTE' '$BUILD_DIR_REMOTE/logs'
WIPE
)
    if [ "$IS_LOCAL" -eq 1 ]; then
        bash -s <<<"$WIPE_SNIPPET"
    else
        "${SSH_CMD[@]}" bash -s <<<"$WIPE_SNIPPET"
    fi
fi

if [ "$SKIP_BUILD" -eq 0 ]; then
    # Make sure target dirs exist before rsync. rsync is happy
    # to create the leaf directory but not parents.
    MK_SNIPPET=$(cat <<MK
mkdir -p '$SRC_DIR_REMOTE' '$BUILD_DIR_REMOTE/logs'
MK
)
    if [ "$IS_LOCAL" -eq 1 ]; then
        bash -s <<<"$MK_SNIPPET"
    else
        "${SSH_CMD[@]}" bash -s <<<"$MK_SNIPPET"
    fi

    note "syncing source tree to $HOST:$SRC_DIR_REMOTE"
    if [ "$IS_LOCAL" -eq 1 ]; then
        # Local mirror; preserve attributes, drop the .git
        # pointer that autotools dislike.
        rsync -a --delete \
            --exclude '.git' \
            "$SUBMODULE/" "$SRC_DIR_REMOTE/"
    else
        rsync -a --delete \
            --exclude '.git' \
            -e "$RSYNC_E" \
            "$SUBMODULE/" "$RSYNC_DST:$SRC_DIR_REMOTE/"
    fi

    # Build snippet. Mirrors scripts/build_cref.sh's relaxation
    # of CFLAGS so the C tree compiles under modern compilers
    # without modifying the submodule. Captures step logs to
    # $BUILD_DIR_REMOTE/logs/ on the remote so failures can be
    # post-mortem'd without re-running.
    BUILD_SNIPPET=$(cat <<BUILD
set -u
SRC='$SRC_DIR_REMOTE'
BUILD='$BUILD_DIR_REMOTE'
LOGS="\$BUILD/logs"
mkdir -p "\$LOGS"
cd "\$SRC"

# Pick a compiler. CC honoured if exported.
if [ -z "\${CC:-}" ]; then
    if command -v gcc >/dev/null 2>&1; then
        CC=gcc
    elif command -v clang >/dev/null 2>&1; then
        CC=clang
    elif command -v cc >/dev/null 2>&1; then
        CC=cc
    else
        echo "build_cref_remote(remote): no C compiler" >&2
        exit 1
    fi
fi
export CC

# Relaxed CFLAGS so older C tree builds under GCC 10+ / clang
# 14+ / FreeBSD's clang. We do not edit the submodule.
CFLAGS_LAX="-g -O2 -D_GNU_SOURCE -fcommon -Wno-error -Wno-int-conversion -Wno-incompatible-pointer-types -Wno-implicit-function-declaration ${CFLAGS_EXTRA:-}"

OS="\$(uname -s)"
EXTRA_CONFIGURE=""
case "\$OS" in
    FreeBSD)
        # Base-system openssl headers live in /usr/include and
        # the libs in /usr/lib; configure usually finds them,
        # but be explicit when pkg-config is happier.
        if [ -d /usr/local/include/openssl ] && [ -d /usr/local/lib ]; then
            EXTRA_CONFIGURE="CPPFLAGS=-I/usr/local/include LDFLAGS=-L/usr/local/lib"
        fi
        ;;
esac

echo "[remote] autoreconf -fvi" >&2
if ! autoreconf -fvi >"\$LOGS/autoreconf.log" 2>&1; then
    echo "build_cref_remote(remote): autoreconf failed; tail of log:" >&2
    tail -40 "\$LOGS/autoreconf.log" >&2 || true
    exit 2
fi

echo "[remote] ./configure --enable-debug=full" >&2
if ! eval CC="\$CC" CFLAGS="'\$CFLAGS_LAX'" \$EXTRA_CONFIGURE \
        ./configure --enable-debug=full >"\$LOGS/configure.log" 2>&1; then
    echo "build_cref_remote(remote): configure failed; tail of log:" >&2
    tail -60 "\$LOGS/configure.log" >&2 || true
    exit 2
fi

echo "[remote] make" >&2
# Try the simple recursive build first. If it fails, fall back
# to the per-subdir approach used by scripts/build_cref.sh
# (skips the yaml test suite and src/tools, neither of which
# affects the produced 'src/dynomite' binary). Either way we
# only need src/dynomite at the end.
if ! make CFLAGS="\$CFLAGS_LAX" >"\$LOGS/make.log" 2>&1; then
    echo "[remote] top-level make failed; falling back to per-dir build" >&2
    if ! make -C contrib/yaml-0.1.4/src CFLAGS="\$CFLAGS_LAX" >"\$LOGS/yaml.log" 2>&1; then
        echo "build_cref_remote(remote): yaml lib failed" >&2
        tail -40 "\$LOGS/yaml.log" >&2 || true
        exit 3
    fi
    for d in src/hashkit src/proto src/event src/entropy src/seedsprovider; do
        if ! make -C "\$d" CFLAGS="\$CFLAGS_LAX" >"\$LOGS/\$(basename \$d).log" 2>&1; then
            echo "build_cref_remote(remote): \$d failed" >&2
            tail -40 "\$LOGS/\$(basename \$d).log" >&2 || true
            exit 3
        fi
    done
    if ! make -C src dynomite CFLAGS="\$CFLAGS_LAX" >"\$LOGS/dynomite.log" 2>&1; then
        echo "build_cref_remote(remote): link failed" >&2
        tail -40 "\$LOGS/dynomite.log" >&2 || true
        exit 3
    fi
fi

if [ ! -x src/dynomite ]; then
    echo "build_cref_remote(remote): expected src/dynomite missing" >&2
    exit 3
fi

# Stage the produced binary into the build dir + record sha.
cp -f src/dynomite "\$BUILD/dynomite"
chmod 0755 "\$BUILD/dynomite"
echo '$LOCAL_SHA' > "\$BUILD/.source-sha"

# Print binary sha256 (and use the right tool for the OS).
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "\$BUILD/dynomite"
elif command -v sha256 >/dev/null 2>&1; then
    sha256 "\$BUILD/dynomite"
elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "\$BUILD/dynomite"
fi

ls -l "\$BUILD/dynomite"
BUILD
)
    note "building C dynomite on $HOST"
    if [ "$IS_LOCAL" -eq 1 ]; then
        bash -s <<<"$BUILD_SNIPPET" || {
            rc=$?
            echo "build_cref_remote: build failed on $HOST (rc=$rc)" >&2
            exit "$rc"
        }
    else
        "${SSH_CMD[@]}" bash -s <<<"$BUILD_SNIPPET" || {
            rc=$?
            echo "build_cref_remote: build failed on $HOST (rc=$rc)" >&2
            exit "$rc"
        }
    fi
fi

# Final verification: binary present + sha file matches.
VERIFY_SNIPPET=$(cat <<VERIFY
set -u
bin='$BUILD_DIR_REMOTE/dynomite'
sha='$BUILD_DIR_REMOTE/.source-sha'
if [ ! -x "\$bin" ]; then
    echo "build_cref_remote(verify): missing binary at \$bin" >&2
    exit 3
fi
if [ ! -f "\$sha" ]; then
    echo "build_cref_remote(verify): missing sha marker at \$sha" >&2
    exit 3
fi
got="\$(cat "\$sha" | tr -d ' \r\n')"
if [ "\$got" != '$LOCAL_SHA' ]; then
    echo "build_cref_remote(verify): sha mismatch: have '\$got' want '$LOCAL_SHA'" >&2
    exit 3
fi
if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "\$bin"
elif command -v sha256 >/dev/null 2>&1; then
    sha256 "\$bin"
elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "\$bin"
fi
VERIFY
)
note "verifying binary on $HOST"
if [ "$IS_LOCAL" -eq 1 ]; then
    bash -s <<<"$VERIFY_SNIPPET"
else
    "${SSH_CMD[@]}" bash -s <<<"$VERIFY_SNIPPET"
fi

note "done: C dynomite ready on $HOST at $BUILD_DIR_REMOTE/dynomite"
