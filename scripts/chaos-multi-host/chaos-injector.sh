#!/usr/bin/env bash
#
# Per-host chaos injector. Runs on each host in parallel with the
# workload driver. Inflicts process-level damage on the local
# dynomited and datastore (redis-server or memcached) processes:
#
#   * SIGSTOP / SIGCONT pause for 5-15s every 60-180s ("gc pause")
#   * SIGKILL + restart of dynomited every 8-15min
#   * Periodic backend bounce every 20-30min
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

# Honour the saved MODE; default to redis for back-compat with
# pre-multi-mode start-args files.
MODE="${MODE:-redis}"

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }
event() {
    local kind="$1"; shift
    printf '{"ts":"%s","host":"%s","kind":"%s","detail":%s}\n' \
        "$(stamp)" "$DC_NAME" "$kind" "$1" \
        >> "$EVENTS"
}

# JSON-escape a multi-line string for embedding in a single JSON
# string field. Backslashes, double quotes and tabs are escaped
# inline; newlines are passed through a vertical-tab placeholder
# (a byte that is forbidden in our log payloads) and then
# rewritten to the literal two-character sequence backslash-n so
# the resulting blob is safe to splice between double quotes in
# the ndjson detail object.
#
# Usage: out=$(json_string_escape "$multiline")
json_string_escape() {
    printf '%s' "$1" \
        | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g' \
        | tr '\n' '\v' \
        | sed 's/\v/\\n/g'
}

# Compute a SHA-256 fingerprint of a file using whichever tool
# is available on this host. Linux ships sha256sum; FreeBSD
# ships sha256(1) and shasum(1). Returns the literal string
# "unknown" if no hasher is found, so the event payload stays
# parseable.
file_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" 2>/dev/null | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" 2>/dev/null | awk '{print $1}'
    elif command -v sha256 >/dev/null 2>&1; then
        sha256 -q "$1" 2>/dev/null
    else
        printf 'unknown'
    fi
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
        local raw; raw=$(cat "$RUN/redis.pid" 2>/dev/null)
        case "$raw" in
            container:*)
                # Container variant: report the container name as a
                # synthetic identifier the injector can bounce via
                # the container runtime.
                printf '%s' "$raw"; return 0
                ;;
            *)
                if [ -n "$raw" ] && kill -0 "$raw" 2>/dev/null; then
                    printf '%s' "$raw"; return 0
                fi
                ;;
        esac
    fi
    return 1
}

# restart_dynomited [failure_event_name]
#
# Re-launch dynomited via start-host.sh, killing any stale
# pre-existing process first. On failure, emit an ndjson event
# that includes the start-host.sh exit code and the last 50
# lines of its combined stdout+stderr log so a post-mortem can
# attribute the failure without per-host log scraping.
#
# The first argument selects the failure-event kind so callers
# can distinguish a scheduled-kill restart from a recovery
# restart in the event stream. Defaults to restart_failed.
restart_dynomited() {
    local fail_event="${1:-restart_failed}"
    local restart_log="$LOGS/restart-$DC_NAME.log"
    event restart "{\"reason\":\"sigkill\"}"
    kill_stale_dynomited
    MODE="$MODE" bash "$ROOT/src/scripts/chaos-multi-host/start-host.sh" \
        "$DC_NAME" "$TOKENS" "$SEEDS" \
        "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" \
        >> "$restart_log" 2>&1
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        local tail_blob
        tail_blob=$(json_string_escape "$(tail -n 50 "$restart_log" 2>/dev/null || true)")
        event "$fail_event" \
            "{\"reason\":\"start-host.sh-nonzero\",\"rc\":$rc,\"tail\":\"$tail_blob\"}"
    fi
}

# Kill any dynomited process bound to this DC's config file. The
# pidfile-tracked process is the common case, but a previous
# start-host.sh that crashed mid-flight (between the binary spawn
# and the pidfile write) can leave an untracked dynomited holding
# the listen port; the next start-host.sh would then fail with
# "Address already in use" and we'd loop indefinitely. Resolve by
# pgrep'ing for any process whose argv contains our config file
# path and SIGKILL'ing all matches before launching a new one.
kill_stale_dynomited() {
    local conf="$RUN/dynomite.yml"
    # Match the binary path AND the conf file so we never touch a
    # neighbour DC running on the same host (the per-DC conf path
    # makes that match unique).
    local pids
    pids=$(pgrep -f "dynomited.*$conf" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        event kill_stale "{\"pids\":\"$(echo "$pids" | tr '\n' ',' | sed 's/,$//')\"}"
        # SIGKILL every match. The pidfile-tracked process and any
        # untracked siblings die together. Wait briefly for the
        # kernel to reap them so the new dynomited's listen-bind
        # and pidfile-flock do not race the dying process.
        for pid in $pids; do
            kill -KILL "$pid" 2>/dev/null || true
        done
        for _ in $(seq 1 50); do
            local still
            still=$(pgrep -f "dynomited.*$conf" 2>/dev/null || true)
            [ -z "$still" ] && break
            sleep 0.1
        done
    fi
    # Drop a stale pidfile so start-host.sh's flock acquires
    # cleanly. We just SIGKILL'd whoever held it; the file lock is
    # released on close-on-exit, but the pidfile contents are still
    # the dead pid - rather than rely on the new dynomited's
    # pidfile-stale handling we remove it explicitly.
    rm -f "$RUN/dynomited.pid" 2>/dev/null || true
}

trap 'event injector_exit "{}"; exit 0' TERM INT

event injector_start "{\"datacenter\":\"$DC_NAME\"}"

# Fingerprint the start-args file once at boot. When the same DC
# produces different failure signatures across a multi-mode
# pass-3 run we can correlate the event stream tails with the
# exact argument set that produced them.
START_ARGS_SHA=$(file_sha256 "$START_ARGS_FILE")
event start_args_fingerprint \
    "{\"file\":\"$START_ARGS_FILE\",\"sha256\":\"$START_ARGS_SHA\",\"mode\":\"$MODE\"}"

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
            # Wait for the kernel to reap the killed process before
            # restarting. The new dynomited's flock(2) on the
            # pidfile would otherwise race the still-being-reaped
            # holder and EAGAIN. Bound the wait so a stuck
            # process does not stall the injector forever.
            for _ in $(seq 1 50); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.1
            done
        fi
        sleep 1
        restart_dynomited
        NEXT_KILL=$(( $(date +%s) + (RANDOM % 240 + 480) ))
    fi

    if [ "$NOW" -ge "$NEXT_REDIS_BOUNCE" ]; then
        if id=$(redis_pid); then
            event redis_bounce "{\"id\":\"$id\",\"mode\":\"$MODE\"}"
            case "$id" in
                container:*)
                    name="${id#container:}"
                    if command -v podman >/dev/null 2>&1; then
                        podman rm -f "$name" >/dev/null 2>&1 || true
                    elif command -v docker >/dev/null 2>&1; then
                        docker rm -f "$name" >/dev/null 2>&1 || true
                    fi
                    sleep 1
                    # Restart via start-host.sh's container path.
                    bounce_log="$LOGS/restart-backend-$DC_NAME.log"
                    MODE="$MODE" bash "$ROOT/src/scripts/chaos-multi-host/start-host.sh" \
                        "$DC_NAME" "$TOKENS" "$SEEDS" \
                        "$DATASTORE_PORT" "$DYN_LISTEN_PORT" \
                        "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" \
                        >> "$bounce_log" 2>&1
                    bounce_rc=$?
                    if [ "$bounce_rc" -ne 0 ]; then
                        bounce_tail=$(json_string_escape "$(tail -n 50 "$bounce_log" 2>/dev/null || true)")
                        event redis_bounce_failed \
                            "{\"reason\":\"start-host.sh-nonzero\",\"rc\":$bounce_rc,\"tail\":\"$bounce_tail\"}"
                    fi
                    ;;
                *)
                    kill -KILL "$id" 2>/dev/null || true
                    sleep 1
                    case "$MODE" in
                        memcache)
                            MEMCACHED=$(command -v memcached || true)
                            if [ -n "$MEMCACHED" ]; then
                                nohup "$MEMCACHED" \
                                    -l 127.0.0.1 \
                                    -p "$DATASTORE_PORT" \
                                    -U 0 \
                                    -m 64 \
                                    -v \
                                    > "$LOGS/memcached-$DC_NAME.log" 2>&1 &
                                echo $! > "$RUN/redis.pid"
                            fi
                            ;;
                        riak|redis|*)
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
                            ;;
                    esac
                    ;;
            esac
        fi
        NEXT_REDIS_BOUNCE=$(( $(date +%s) + (RANDOM % 600 + 1200) ))
    fi

    # Independent recovery: if dynomited is missing for any
    # reason (failed restart, crash we did not cause, OS-level
    # OOM kill), bring it back without waiting for the next
    # scheduled kill. This catches the case where
    # restart_dynomited above returned nonzero and we would
    # otherwise have to wait 8-12 minutes for the next kill
    # cycle to retry. With the pidfile flock retry in place this
    # branch should rarely fire, but it makes the injector
    # robust to any future failure mode that leaves dynomited
    # missing.
    #
    # Hysteresis: only fire a recovery restart once dynomited
    # has been missing for two consecutive 5s checks. start-host.sh
    # commonly takes 5-15s to bring dynomited fully up; without
    # this debounce we'd fire overlapping restarts that compete
    # for the pidfile flock and produce a thrashing loop.
    if ! dyn_pid >/dev/null; then
        if [ "${MISSING_STREAK:-0}" -ge 1 ]; then
            event recovery_restart "{\"streak\":$MISSING_STREAK}"
            restart_dynomited recovery_restart_failed
            MISSING_STREAK=0
        else
            MISSING_STREAK=$(( ${MISSING_STREAK:-0} + 1 ))
        fi
    else
        MISSING_STREAK=0
    fi

    sleep 5
done
