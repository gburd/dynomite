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
#
# Issue C (Pass-7 cross-mode log contamination): the coordinator's
# own teardown rsync runs inside a 60s `timeout --signal=KILL`.
# On a 2h workload that produces multi-hundred-megabyte ndjsons
# the rsync routinely times out, leaving the local per-RUN_ID
# destination empty or partial. Then the next mode's start-host
# truncates the same ndjson on the remote, so each mode after
# the first observes the latest mode's data on subsequent local
# inspection. We add a belt-and-suspenders "post-mode rsync"
# step here that runs AFTER the coordinator pid exits and
# BEFORE the next mode starts. It targets a dedicated
# `<run-id>/post-mode-rsync/<host>-logs/` subdir that the
# coordinator's teardown does not touch, with a generous 600s
# budget. Even if the coordinator's own rsync timed out, this
# pass captures the real data before the next mode wipes it.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
DURATION="${CHAOS_DURATION_SECS:-7200}"
SCRIPT_DIR="$REPO/scripts/chaos-multi-host"

# Post-mode rsync settings. POST_RSYNC_TIMEOUT_SECS bounds each
# host's rsync; the operator can lower it for smoke runs.
POST_RSYNC_TIMEOUT_SECS="${POST_RSYNC_TIMEOUT_SECS:-600}"
SSH_KEY="$HOME/.ssh/id_ed25519"
SSH_BASE_OPTS=(-o IdentitiesOnly=yes -i "$SSH_KEY"
               -o ControlMaster=no -o ControlPath=none
               -o StrictHostKeyChecking=accept-new
               -o ServerAliveInterval=30)
ARNOLD_RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
NUC_RSYNC_E="ssh ${SSH_BASE_OPTS[*]} -o ProxyJump=arnold"
MEH_RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"

# Belt-and-suspenders post-mode rsync.
#
# Captures /scratch/dynomite-chaos/logs/ from each remote into
# $logs/post-mode-rsync/<host>-logs/. The coordinator's own
# teardown writes to $logs/<host>-logs/ (no "post-mode-rsync"
# segment), so the two paths can never collide. Failures here
# are warnings; a Pass-7-style stall in the coordinator's rsync
# is exactly what this step exists to recover.
#
# Respects HOSTS_OVERRIDE: when set, only listed hosts are
# rsynced. The smoke test (test_per_mode_rsync.sh) sets
# HOSTS_OVERRIDE=floki to avoid blocking on remote SSH.
host_in_override() {
    local h="$1"
    local override="${HOSTS_OVERRIDE:-}"
    if [ -z "$override" ]; then
        return 0
    fi
    case ",$override," in
        *",$h,"*) return 0 ;;
        *)        return 1 ;;
    esac
}

post_mode_rsync() {
    local logs="$1"
    local mode="$2"
    local dest="$logs/post-mode-rsync"
    mkdir -p "$dest"
    echo "==> [pass3] post-mode rsync (mode=$mode, dest=$dest, timeout=${POST_RSYNC_TIMEOUT_SECS}s)"

    if host_in_override arnold; then
        if timeout --signal=KILL "${POST_RSYNC_TIMEOUT_SECS}s" \
                rsync -az -e "$ARNOLD_RSYNC_E" \
                    arnold:/scratch/dynomite-chaos/logs/ "$dest/arnold-logs/"; then
            echo "    arnold: ok"
        else
            echo "    arnold: WARN post-mode rsync failed (rc=$?); continuing"
        fi
    fi

    # nuc: ProxyJump first (matches coord). On failure leave a
    # marker so the operator can see the difference between
    # "didn't try" and "tried and failed".
    if host_in_override nuc; then
        if timeout --signal=KILL "${POST_RSYNC_TIMEOUT_SECS}s" \
                rsync -az -e "$NUC_RSYNC_E" \
                    gburd@nuc:/scratch/dynomite-chaos/logs/ "$dest/nuc-logs/"; then
            echo "    nuc: ok"
        else
            echo "    nuc: WARN post-mode rsync failed (rc=$?); continuing"
        fi
    fi

    if host_in_override meh; then
        if timeout --signal=KILL "${POST_RSYNC_TIMEOUT_SECS}s" \
                rsync -az -e "$MEH_RSYNC_E" \
                    meh:/scratch/dynomite-chaos/logs/ "$dest/meh-logs/"; then
            echo "    meh: ok"
        else
            echo "    meh: WARN post-mode rsync failed (rc=$?); continuing"
        fi
    fi

    # floki is local; the coordinator's teardown already cp -r
    # into $logs/floki-logs/ but that may have raced; a second
    # cp is cheap.
    if host_in_override floki; then
        if [ -d /scratch/dynomite-chaos/logs ]; then
            cp -r /scratch/dynomite-chaos/logs "$dest/floki-logs" 2>/dev/null \
                && echo "    floki: ok" \
                || echo "    floki: WARN cp failed; continuing"
        fi
    fi
}

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

    # Issue C (Pass-7): independent post-mode rsync between
    # modes, regardless of whether the coordinator's own
    # rsync completed. This MUST run before the next call to
    # start_one_mode whose start-host.sh truncates the
    # workload ndjson on the remote.
    post_mode_rsync "$logs" "$mode" || true
    echo
}

echo "==> [pass3] starting all-modes sequence at $(date -u)"
echo

for mode in redis memcache riak; do
    start_one_mode "$mode"
done

echo "==> [pass3] all three modes complete at $(date -u)"
