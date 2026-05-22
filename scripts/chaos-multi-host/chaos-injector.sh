#!/usr/bin/env bash
#
# Per-host chaos injector. Runs on each host in parallel with the
# workload driver. Inflicts process-level damage on the local
# dynomited and redis processes:
#
#   * SIGSTOP / SIGCONT pause for 5-15s every 60-180s ("gc pause")
#   * SIGKILL + restart of dynomited every 8-15min
#   * Periodic redis-server bounce every 20-30min
#
# Designed to be SIGTERM-able; on exit, leaves dynomited running
# so the coordinator's teardown can collect its logs.

set -uo pipefail

DC_NAME="${1:?DC name required}"
ROOT="/scratch/dynomite-chaos"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
EVENTS="$LOGS/chaos-events-$DC_NAME.ndjson"

# Pull the start args saved by the coordinator so we can restart
# the same dynomited config on a kill.
START_ARGS_FILE="$RUN/start-args"
if [ ! -f "$START_ARGS_FILE" ]; then
    echo "missing $START_ARGS_FILE; coordinator must have written it before launching the injector" >&2
    exit 1
fi

# shellcheck disable=SC1090
. "$START_ARGS_FILE"

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }
event() {
    local kind="$1"; shift
    printf '{"ts":"%s","host":"%s","kind":"%s","detail":%s}\n' \
        "$(stamp)" "$DC_NAME" "$kind" "$1" \
        >> "$EVENTS"
}

dyn_pid() {
    if [ -f "$RUN/dynomited.pid" ]; then
        local pid; pid=$(cat "$RUN/dynomited.pid" 2>/dev/null)
        # On Linux, /proc/<pid> exists when the process is alive.
        # On FreeBSD, the same shape works via procfs if mounted;
        # otherwise fall back to `kill -0`.
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            printf '%s' "$pid"; return 0
        fi
    fi
    return 1
}

redis_pid() {
    if [ -f "$RUN/redis.pid" ]; then
        local pid; pid=$(cat "$RUN/redis.pid" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            printf '%s' "$pid"; return 0
        fi
    fi
    return 1
}

restart_dynomited() {
    event restart "{\"reason\":\"sigkill\"}"
    bash "$ROOT/src/scripts/chaos-multi-host/start-host.sh" \
        "$DC_NAME" "$TOKENS" "$SEEDS" \
        "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" \
        >> "$LOGS/restart-$DC_NAME.log" 2>&1 \
        || event restart_failed "{\"reason\":\"start-host.sh-nonzero\"}"
}

trap 'event injector_exit "{}"; exit 0' TERM INT

event injector_start "{\"datacenter\":\"$DC_NAME\"}"

# Schedule windows.
NEXT_PAUSE=$(( $(date +%s) + (RANDOM % 60 + 60) ))   # 1-2 min
NEXT_KILL=$(( $(date +%s) + (RANDOM % 240 + 480) ))  # 8-12 min
NEXT_REDIS_BOUNCE=$(( $(date +%s) + (RANDOM % 600 + 1200) ))  # 20-30 min

while true; do
    NOW=$(date +%s)

    if [ "$NOW" -ge "$NEXT_PAUSE" ]; then
        if pid=$(dyn_pid); then
            DUR=$(( RANDOM % 11 + 5 ))
            event pause_start "{\"pid\":$pid,\"duration\":$DUR}"
            kill -STOP "$pid" 2>/dev/null || true
            sleep "$DUR"
            kill -CONT "$pid" 2>/dev/null || true
            event pause_end "{\"pid\":$pid,\"duration\":$DUR}"
        else
            event pause_skipped "{\"reason\":\"no-dynomited\"}"
        fi
        NEXT_PAUSE=$(( $(date +%s) + (RANDOM % 60 + 60) ))
    fi

    if [ "$NOW" -ge "$NEXT_KILL" ]; then
        if pid=$(dyn_pid); then
            event kill "{\"pid\":$pid}"
            kill -KILL "$pid" 2>/dev/null || true
            sleep 2
        fi
        # Wait for the spawn pid to settle, then restart.
        sleep 1
        restart_dynomited
        NEXT_KILL=$(( $(date +%s) + (RANDOM % 240 + 480) ))
    fi

    if [ "$NOW" -ge "$NEXT_REDIS_BOUNCE" ]; then
        if pid=$(redis_pid); then
            event redis_bounce "{\"pid\":$pid}"
            kill -KILL "$pid" 2>/dev/null || true
            sleep 1
            # restart redis with the same args
            REDIS=$(command -v redis-server || true)
            if [ -n "$REDIS" ]; then
                nohup "$REDIS" \
                    --port "$DATASTORE_PORT" \
                    --bind 127.0.0.1 \
                    --daemonize no \
                    --appendonly no \
                    --save "" \
                    --dir "$RUN" \
                    --logfile "$LOGS/redis-$DC_NAME.log" \
                    > /dev/null 2>&1 &
                echo $! > "$RUN/redis.pid"
            fi
        fi
        NEXT_REDIS_BOUNCE=$(( $(date +%s) + (RANDOM % 600 + 1200) ))
    fi

    sleep 5
done
