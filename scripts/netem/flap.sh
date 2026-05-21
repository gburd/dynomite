#!/usr/bin/env bash
# scripts/netem/flap.sh - 1-second connectivity flaps against
# a single port. Alternates 1 s of 100% loss with 1 s of clean
# traffic for the configured duration.
#
# Usage:
#   flap.sh <port> [duration_secs] [period_secs]
#
# The chaos harness runs this against one randomly-chosen
# peer per 5-minute window.

set -euo pipefail

# shellcheck source=./_lib.sh
. "$(dirname "$0")/_lib.sh"

if [ $# -lt 1 ]; then
    echo "usage: $0 <port> [duration_secs] [period_secs]" >&2
    exit 2
fi
PORT="$1"
DURATION="${2:-30}"
PERIOD="${3:-1}"

cap_check

DEV="$NETEM_DEV"
trap 'clear_qdisc "$DEV"' INT TERM EXIT

elapsed=0
state=down

while [ "$elapsed" -lt "$DURATION" ]; do
    if [ "$state" = "down" ]; then
        clear_qdisc "$DEV"
        tc qdisc add dev "$DEV" root handle 1: prio
        tc qdisc add dev "$DEV" parent 1:3 handle 30: netem loss 100%
        tc filter add dev "$DEV" protocol ip parent 1: prio 1 \
            u32 match ip dport "$PORT" 0xffff flowid 1:3
        printf '{"status":"flap-down","port":%d,"elapsed":%d}\n' "$PORT" "$elapsed"
        state=up
    else
        clear_qdisc "$DEV"
        printf '{"status":"flap-up","port":%d,"elapsed":%d}\n' "$PORT" "$elapsed"
        state=down
    fi
    sleep "$PERIOD"
    elapsed=$((elapsed + PERIOD))
done

clear_qdisc "$DEV"
trap - INT TERM EXIT

printf '{"status":"cleared","port":%d}\n' "$PORT"
