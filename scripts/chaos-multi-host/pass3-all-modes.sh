#!/usr/bin/env bash
#
# Sequential pass-3 chaos: Redis -> Memcache -> Riak.
#
# Each mode runs CHAOS_DURATION_SECS seconds (default 2h).  Total
# wall-clock with the 2h default is ~6.5h including teardown gaps.
#
# Each mode is launched via launch-detached.sh (own setsid
# session) and this wrapper waits for the coordinator pid to exit
# before kicking off the next mode.
#
# Run with:
#
#   bash scripts/chaos-multi-host/pass3-all-modes.sh
#
# The wrapper itself can be backgrounded with `nohup`:
#
#   nohup bash scripts/chaos-multi-host/pass3-all-modes.sh \
#       > /tmp/pass3-all-modes.log 2>&1 &
#   disown
#
# Each mode's run id is `pass3-<mode>-<utc-stamp>` so the logs do
# not collide.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
DURATION="${CHAOS_DURATION_SECS:-7200}"
SCRIPT_DIR="$REPO/scripts/chaos-multi-host"

start_one_mode() {
    local mode="$1"
    local stamp; stamp=$(date -u +%Y%m%d-%H%M%SZ)
    local run_id="pass3-${mode}-${stamp}"
    local logs="$REPO/target/chaos-multi-host/$run_id"
    mkdir -p "$logs"
    local log_file="$logs/launcher.log"
    local pid_file="$logs/coordinator.pid"

    echo "==> [pass3] launching mode=$mode run_id=$run_id duration=${DURATION}s"
    echo "    log:  $log_file"
    echo "    pid:  $pid_file"

    RUN_ID="$run_id" \
    CHAOS_DURATION_SECS="$DURATION" \
    MODE="$mode" \
    bash "$SCRIPT_DIR/launch-detached.sh" "$log_file" "$pid_file"

    if [ ! -f "$pid_file" ]; then
        echo "!!! [pass3] launcher did not write pid file for mode=$mode" >&2
        return 1
    fi

    local coord_pid; coord_pid=$(cat "$pid_file")
    echo "==> [pass3] coordinator pid=$coord_pid; waiting for it to exit"

    while kill -0 "$coord_pid" 2>/dev/null; do
        sleep 60
    done

    echo "==> [pass3] mode=$mode finished (coordinator pid $coord_pid exited)"
    echo "    artifacts: $logs"
    echo
}

echo "==> [pass3] starting all-modes sequence at $(date -u)"
echo

for mode in redis memcache riak; do
    start_one_mode "$mode"
done

echo "==> [pass3] all three modes complete at $(date -u)"
