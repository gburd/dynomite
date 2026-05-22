#!/usr/bin/env bash
#
# Periodic status logger for the live chaos run. Writes a
# snapshot every $INTERVAL seconds to a single growing log file,
# until the coordinator process exits.
#
# Usage: ./watch-status.sh [RUN_ID] [INTERVAL_SECS]

set -euo pipefail

RUN_ID="${1:-prod-20260522-010136Z}"
INTERVAL="${2:-600}"   # 10 minutes default
COORD_PID_FILE="${COORD_PID_FILE:-/tmp/chaos-prod.pid}"
OUT="/home/gburd/ws/dynomite/target/chaos-multi-host/$RUN_ID/watch.log"
mkdir -p "$(dirname "$OUT")"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

while true; do
    if [ -f "$COORD_PID_FILE" ]; then
        pid=$(cat "$COORD_PID_FILE")
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "$(date -u +%H:%M:%SZ): coordinator pid $pid is gone; stopping watch" >> "$OUT"
            exit 0
        fi
    fi
    {
        echo
        echo "================================================================"
        echo "WATCH @ $(date -u +%Y-%m-%dT%H:%M:%SZ)"
        echo "================================================================"
        bash "$SCRIPT_DIR/live-status.sh" "$RUN_ID" 2>&1
    } >> "$OUT"
    sleep "$INTERVAL"
done
