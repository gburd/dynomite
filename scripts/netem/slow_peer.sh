#!/usr/bin/env bash
# scripts/netem/slow_peer.sh - apply a 200 ms one-way delay
# to traffic destined for the supplied local port for the
# configured duration, then clear.
#
# Usage:
#   slow_peer.sh <port> [delay_ms] [duration_secs]
#
# The chaos harness rotates this every minute over the peer
# set so each peer takes a turn being the slow one.

set -euo pipefail

# shellcheck source=./_lib.sh
. "$(dirname "$0")/_lib.sh"

if [ $# -lt 1 ]; then
    echo "usage: $0 <port> [delay_ms] [duration_secs]" >&2
    exit 2
fi
PORT="$1"
DELAY_MS="${2:-200}"
DURATION="${3:-60}"

cap_check

DEV="$NETEM_DEV"
trap 'clear_qdisc "$DEV"' INT TERM EXIT

clear_qdisc "$DEV"
tc qdisc add dev "$DEV" root handle 1: prio
tc qdisc add dev "$DEV" parent 1:3 handle 30: netem delay "${DELAY_MS}ms"
tc filter add dev "$DEV" protocol ip parent 1: prio 1 \
    u32 match ip dport "$PORT" 0xffff flowid 1:3

printf '{"status":"installed","port":%d,"delay_ms":%d,"duration_secs":%d}\n' \
    "$PORT" "$DELAY_MS" "$DURATION"

sleep "$DURATION"

clear_qdisc "$DEV"
trap - INT TERM EXIT

printf '{"status":"cleared","port":%d}\n' "$PORT"
