#!/usr/bin/env bash
# scripts/netem/clock_skew.sh - launch the supplied command
# under faketime with the configured offset. Used by the
# chaos harness to introduce a 0..30 s skew on one peer at
# the 30-minute mark.
#
# Usage:
#   clock_skew.sh <offset> -- <cmd> [args...]
#
# `offset` is a faketime spec, e.g. `+15s`, `-3m`, etc. The
# script execs the command in-place so SIGTERM from the chaos
# harness reaches the dynomited child.

set -euo pipefail

if [ $# -lt 3 ] || [ "$2" != "--" ]; then
    echo "usage: $0 <offset> -- <cmd> [args...]" >&2
    exit 2
fi
OFFSET="$1"
shift 2

if ! command -v faketime >/dev/null 2>&1; then
    printf '{"status":"skip","reason":"faketime-not-on-PATH"}\n' >&2
    # Fall back to running the command without skew so the
    # chaos harness still has a live peer; the harness logs
    # the skip notice and decrements its expected coverage.
    exec "$@"
fi

printf '{"status":"installed","offset":"%s","argv0":"%s"}\n' "$OFFSET" "$1" >&2
exec faketime "$OFFSET" "$@"
