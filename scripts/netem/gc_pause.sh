#!/usr/bin/env bash
# scripts/netem/gc_pause.sh - simulate a GC-style stop-the-
# world pause by SIGSTOP'ing a child process for the configured
# duration, then SIGCONT'ing it.
#
# Usage:
#   gc_pause.sh <pid> [duration_secs]
#
# The chaos harness fires this against one peer every 7
# minutes. We do NOT use netem here: the goal is to model an
# in-process pause, not a network pause.

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "usage: $0 <pid> [duration_secs]" >&2
    exit 2
fi
PID="$1"
DURATION="${2:-5}"

if ! kill -0 "$PID" 2>/dev/null; then
    printf '{"status":"skip","reason":"pid-not-found","pid":%d}\n' "$PID"
    exit 0
fi

# We only need permission to SIGSTOP the target. The chaos
# harness owns every dynomited PID so this is normally fine
# without elevation.
if ! kill -STOP "$PID" 2>/dev/null; then
    printf '{"status":"skip","reason":"sigstop-failed","pid":%d}\n' "$PID"
    exit 0
fi

trap 'kill -CONT "$PID" 2>/dev/null || true' INT TERM EXIT

printf '{"status":"stopped","pid":%d,"duration_secs":%d}\n' "$PID" "$DURATION"
sleep "$DURATION"

kill -CONT "$PID" 2>/dev/null || true
trap - INT TERM EXIT

printf '{"status":"resumed","pid":%d}\n' "$PID"
