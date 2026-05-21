#!/usr/bin/env bash
# scripts/netem/partition_dc.sh - inject a one-way packet
# drop between two cross-DC port ranges for the configured
# duration, then clear.
#
# Usage:
#   partition_dc.sh <local_port_a> <local_port_b> [duration_secs]
#
# The chaos harness uses this to simulate a 30-second
# cross-DC partition. The qdisc is clamped to the loopback
# device because the entire chaos test runs on lo; netem on
# lo with `dst port` filters is the cheapest way to drop only
# the relevant traffic.
#
# Emits a JSON status object on stdout for the harness to
# parse. Exits 0 on graceful skip; non-zero on a hard
# failure.

set -euo pipefail

# shellcheck source=./_lib.sh
. "$(dirname "$0")/_lib.sh"

if [ $# -lt 2 ]; then
    echo "usage: $0 <port_a> <port_b> [duration_secs]" >&2
    exit 2
fi
PORT_A="$1"
PORT_B="$2"
DURATION="${3:-30}"

cap_check

# Build a prio qdisc with a netem leaf that drops 100% of
# traffic to either port. We use a u32 filter on dst port.
DEV="$NETEM_DEV"
trap 'clear_qdisc "$DEV"' INT TERM EXIT

clear_qdisc "$DEV"
tc qdisc add dev "$DEV" root handle 1: prio
tc qdisc add dev "$DEV" parent 1:3 handle 30: netem loss 100%
tc filter add dev "$DEV" protocol ip parent 1: prio 1 \
    u32 match ip dport "$PORT_A" 0xffff flowid 1:3
tc filter add dev "$DEV" protocol ip parent 1: prio 1 \
    u32 match ip dport "$PORT_B" 0xffff flowid 1:3

printf '{"status":"installed","ports":[%d,%d],"duration_secs":%d}\n' \
    "$PORT_A" "$PORT_B" "$DURATION"

sleep "$DURATION"

clear_qdisc "$DEV"
trap - INT TERM EXIT

printf '{"status":"cleared","ports":[%d,%d]}\n' "$PORT_A" "$PORT_B"
