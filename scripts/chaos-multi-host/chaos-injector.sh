#!/usr/bin/env bash
#
# Per-host chaos injector. Runs on each host in parallel with the
# workload driver. Inflicts failures on the local dynomited
# instance and (optionally) on the host's network/clock/disk
# state. The set of fault classes the injector will exercise is
# selected at start-up via the MODE_FAULTS environment variable:
#
#   process   SIGSTOP/SIGCONT pauses, SIGKILL+restart cycles,
#             redis/memcached bounce. (Default; matches the
#             pre-2026-05-26 behaviour byte-for-byte when
#             MODE_FAULTS is unset.)
#   network   tc-qdisc-driven partition / delay / loss / bandcap
#             on a single device (default lo).
#   clock     faketime-driven wall-clock skew applied to a fresh
#             dynomited launch via start-host.sh.
#   disk      tmpfs ballast (squeeze + full) and cgroups-v2
#             io.max latency injection on the redis backend.
#
# Multiple classes can be combined as a comma-separated list:
#
#     MODE_FAULTS=process,network,clock,disk
#
# When MODE_FAULTS is unset (the default), the injector runs the
# legacy three-timer process-only schedule unchanged. When it is
# set explicitly (even to MODE_FAULTS=process) the unified
# scheduler is used: every 60-180 s the injector picks one fault
# uniformly across the enabled classes, runs it, and goes back
# to sleep.
#
# Required-tool detection happens at start-up. Classes whose
# prerequisites are missing are dropped from the runnable set
# and an injector_classes event records both the configured and
# the runnable list. A class with no runnable sub-faults is
# silently skipped by the scheduler.
#
# Cleanup is mandatory and idempotent: the SIGTERM trap calls
# cleanup_all which removes any tc qdisc, ballast file, cgroup
# io.max limit, or clock-skew marker file the injector may have
# left behind. cleanup_all is also called at injector start to
# scrub state from a previous run that may have died mid-fault
# without running its trap.

set -uo pipefail

# ---------- bootstrap ----------

# Detect whether we are being sourced (for the smoke-test
# harness in test_fault_smoke.sh) or executed directly. When
# sourced, the caller is responsible for populating the
# globals (DC_NAME, ROOT, RUN, LOGS, EVENTS, MODE, etc.) and
# we do NOT read start-args or invoke main.
if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    __CHAOS_SOURCED=0
else
    __CHAOS_SOURCED=1
fi

if [ "$__CHAOS_SOURCED" = "0" ]; then
    DC_NAME="${1:?DC name required}"
    ROOT="${ROOT:-/scratch/dynomite-chaos}"
    RUN="$ROOT/run"
    LOGS="$ROOT/logs"
    EVENTS="$LOGS/chaos-events-$DC_NAME.ndjson"

    START_ARGS_FILE="$RUN/start-args"
    if [ ! -f "$START_ARGS_FILE" ]; then
        echo "missing $START_ARGS_FILE; coordinator must have written it before launching the injector" >&2
        exit 1
    fi

    # shellcheck disable=SC1090
    . "$START_ARGS_FILE"

    # Honour the saved MODE; default to redis for back-compat
    # with pre-multi-mode start-args files.
    MODE="${MODE:-redis}"

    # Distinguish "unset" from "explicitly set" for
    # MODE_FAULTS so the default unset case can route to the
    # legacy scheduler.
    MODE_FAULTS_EXPLICIT=0
    if [ -n "${MODE_FAULTS+set}" ]; then
        MODE_FAULTS_EXPLICIT=1
    fi
    MODE_FAULTS="${MODE_FAULTS:-process}"

    # Network device for tc-based faults. Defaults to
    # loopback so the smoke tests and single-host runs work
    # unprivileged-ish; operators target eth0 / ts0 / etc.
    # via the env knob.
    CHAOS_NETEM_DEV="${CHAOS_NETEM_DEV:-lo}"

    # Cgroup-v2 path used by fault_disk_iolat. We keep the
    # cgroup alive across faults to avoid the move-process-
    # back-to-root dance; cleanup just resets io.max to
    # "max".
    CHAOS_CGROUP="${CHAOS_CGROUP:-/sys/fs/cgroup/chaos-iolat-$DC_NAME}"

    # State files shared across the fault scheduler and the
    # trap.
    CLOCK_SKEW_MARKER="$RUN/clock-skew-active"
    BALLAST_FILE="$RUN/chaos-ballast"
    BALLAST_FULL_FILE="$RUN/chaos-ballast-full"
fi

# Runnable-class sets are populated by check_prereqs once the
# event stream is open.
RUNNABLE_PROCESS=0
RUNNABLE_NETWORK=0
RUNNABLE_CLOCK=0
RUNNABLE_DISK=0

# ---------- helpers ----------

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }

event() {
    local kind="$1"; shift
    printf '{"ts":"%s","host":"%s","kind":"%s","detail":%s}\n' \
        "$(stamp)" "$DC_NAME" "$kind" "$1" \
        >> "$EVENTS"
}

# JSON-escape a multi-line string for embedding in a single JSON
# string field. Backslashes, double quotes and tabs are escaped
# inline; newlines pass through a vertical-tab placeholder
# (forbidden in our log payloads) and are then rewritten to the
# literal two-character backslash-n so the resulting blob is
# safe to splice between double quotes.
json_string_escape() {
    printf '%s' "$1" \
        | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g' \
        | tr '\n' '\v' \
        | sed 's/\v/\\n/g'
}

# Base64-encode the contents of a file (or empty string when the
# file is missing). Uses the portable `base64` binary in single-
# line mode. The output is ASCII-only so it can be spliced
# between double quotes in JSON without further escaping; the
# decode side is a one-liner in any language.
base64_file() {
    local path="$1"
    if [ ! -f "$path" ]; then
        printf ''
        return 0
    fi
    if base64 --help 2>&1 | grep -q -- '-w'; then
        # GNU coreutils: -w0 disables wrapping.
        tail -n 50 "$path" 2>/dev/null | base64 -w0 2>/dev/null || printf ''
    else
        # BSD/macOS base64: no -w flag; output is one line by
        # default for stdin <= 76 bytes, multi-line otherwise.
        # Re-flatten via `tr` to be safe.
        tail -n 50 "$path" 2>/dev/null | base64 2>/dev/null | tr -d '\n' || printf ''
    fi
}

# Emit a `restart_failed_detail` event to the chaos-events
# stream. Carries the dynomited stderr and log tails (last 50
# lines each) base64-encoded so embedded ASCII-control bytes
# (a real failure mode: dynomited's stderr can include the raw
# bytes a panicked socket dumped) cannot break JSON parsing on
# the report side.
#
# The shape matches the post-chaos queue P3-1.3 spec exactly:
#   {"event":"restart_failed_detail",
#    "host":"<dc-name>",
#    "rc":<int>,
#    "stderr_tail":"<base64>",
#    "log_tail":"<base64>",
#    "timestamp":"<RFC3339>"}
#
# The line ALSO carries `kind` and `ts` aliases so the existing
# event consumers (live-status.sh, generate-report.py's
# parse_chaos_events) keep working without a schema split.
emit_restart_failed_detail() {
    local rc="$1"
    local stderr_b64 log_b64 ts
    stderr_b64=$(base64_file "$LOGS/dynomited-$DC_NAME.stderr")
    log_b64=$(base64_file "$LOGS/dynomited-$DC_NAME.log")
    ts=$(stamp)
    printf '{"event":"restart_failed_detail","kind":"restart_failed_detail","host":"%s","rc":%d,"stderr_tail":"%s","log_tail":"%s","timestamp":"%s","ts":"%s"}\n' \
        "$DC_NAME" "$rc" "$stderr_b64" "$log_b64" "$ts" "$ts" \
        >> "$EVENTS"
}

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
            container:*) printf '%s' "$raw"; return 0 ;;
            *)
                if [ -n "$raw" ] && kill -0 "$raw" 2>/dev/null; then
                    printf '%s' "$raw"; return 0
                fi
                ;;
        esac
    fi
    return 1
}

# Parse "process,network,..." into the `IS_CLASS_<n>` shell
# vars. Empty list disables every class (the operator wanted to
# observe with no chaos applied; we honour it).
IS_CLASS_PROCESS=0
IS_CLASS_NETWORK=0
IS_CLASS_CLOCK=0
IS_CLASS_DISK=0
parse_classes() {
    local raw="${MODE_FAULTS//,/ }"
    local c
    for c in $raw; do
        case "$c" in
            process) IS_CLASS_PROCESS=1 ;;
            network) IS_CLASS_NETWORK=1 ;;
            clock)   IS_CLASS_CLOCK=1 ;;
            disk)    IS_CLASS_DISK=1 ;;
            "")      : ;;
            *)
                event mode_faults_unknown_class "{\"class\":\"$c\"}"
                ;;
        esac
    done
}

# ---------- prerequisite checks ----------

check_prereq_process() {
    # Always available; we rely only on signals.
    RUNNABLE_PROCESS=1
}

check_prereq_network() {
    local dev="$CHAOS_NETEM_DEV"
    if ! command -v tc >/dev/null 2>&1; then
        event prereq_skip "{\"class\":\"network\",\"reason\":\"tc-not-on-PATH\"}"
        RUNNABLE_NETWORK=0
        return 0
    fi
    # Probe: try a no-op pfifo on a high handle, then delete.
    # If we lack CAP_NET_ADMIN both calls fail and we skip.
    if tc qdisc add dev "$dev" handle 999: ingress 2>/dev/null; then
        tc qdisc del dev "$dev" handle 999: ingress 2>/dev/null || true
        RUNNABLE_NETWORK=1
        return 0
    fi
    # ingress probe failed; try the path the faults actually use.
    if tc qdisc add dev "$dev" root handle 999: pfifo 2>/dev/null; then
        tc qdisc del dev "$dev" root 2>/dev/null || true
        RUNNABLE_NETWORK=1
        return 0
    fi
    event prereq_skip "{\"class\":\"network\",\"reason\":\"tc-add-denied\",\"dev\":\"$dev\"}"
    RUNNABLE_NETWORK=0
}

check_prereq_clock() {
    if command -v faketime >/dev/null 2>&1; then
        RUNNABLE_CLOCK=1
        return 0
    fi
    event prereq_skip "{\"class\":\"clock\",\"reason\":\"faketime-not-on-PATH\"}"
    RUNNABLE_CLOCK=0
}

check_prereq_disk() {
    if [ ! -f /sys/fs/cgroup/cgroup.controllers ]; then
        event prereq_skip "{\"class\":\"disk\",\"reason\":\"no-cgroups-v2\"}"
        RUNNABLE_DISK=0
        return 0
    fi
    if ! grep -q '\bio\b' /sys/fs/cgroup/cgroup.controllers 2>/dev/null; then
        event prereq_skip "{\"class\":\"disk\",\"reason\":\"io-controller-not-available\"}"
        RUNNABLE_DISK=0
        return 0
    fi
    # The squeeze and full sub-faults only need write access to
    # ROOT; the iolat sub-fault degrades to skipped if cgroup
    # creation fails. Disk class is runnable as long as we can
    # at least do squeeze/full.
    if [ ! -w "$RUN" ]; then
        event prereq_skip "{\"class\":\"disk\",\"reason\":\"run-not-writable\"}"
        RUNNABLE_DISK=0
        return 0
    fi
    RUNNABLE_DISK=1
}

check_prereqs() {
    [ "$IS_CLASS_PROCESS" = 1 ] && check_prereq_process || RUNNABLE_PROCESS=0
    [ "$IS_CLASS_NETWORK" = 1 ] && check_prereq_network || RUNNABLE_NETWORK=0
    [ "$IS_CLASS_CLOCK"   = 1 ] && check_prereq_clock   || RUNNABLE_CLOCK=0
    [ "$IS_CLASS_DISK"    = 1 ] && check_prereq_disk    || RUNNABLE_DISK=0
}

# ---------- restart helpers (used by process and clock faults) ----------

# Kill any dynomited bound to this DC's config file. The
# pidfile-tracked process is the common case, but a previous
# start-host.sh that crashed mid-flight (between binary spawn
# and pidfile write) can leave an untracked dynomited holding
# the listen port; the next start-host.sh would then fail with
# "Address already in use" and we'd loop indefinitely. Resolve
# by pgrep'ing for any process whose argv contains our config
# file path and SIGKILL'ing all matches before launching a new
# one.
kill_stale_dynomited() {
    local conf="$RUN/dynomite.yml"
    local pids
    pids=$(pgrep -f "dynomited.*$conf" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        event kill_stale "{\"pids\":\"$(echo "$pids" | tr '\n' ',' | sed 's/,$//')\"}"
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
    rm -f "$RUN/dynomited.pid" 2>/dev/null || true
}

# restart_dynomited [failure_event_name]
#
# Re-launch dynomited via start-host.sh, killing any stale
# pre-existing process first. Caller may set FAKETIME in the
# environment (passed through verbatim) so start-host.sh can
# launch the binary under faketime for clock-skew faults.
restart_dynomited() {
    local fail_event="${1:-restart_failed}"
    local restart_log="$LOGS/restart-$DC_NAME.log"
    event restart "{\"reason\":\"sigkill\",\"faketime\":\"${FAKETIME:-}\"}"
    kill_stale_dynomited
    FAKETIME="${FAKETIME:-}" MODE="$MODE" \
        bash "$ROOT/src/scripts/chaos-multi-host/start-host.sh" \
            "$DC_NAME" "$TOKENS" "$SEEDS" \
            "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" \
            >> "$restart_log" 2>&1
    local rc=$?
    if [ "$rc" -ne 0 ]; then
        local tail_blob
        tail_blob=$(json_string_escape "$(tail -n 50 "$restart_log" 2>/dev/null || true)")
        event "$fail_event" \
            "{\"reason\":\"start-host.sh-nonzero\",\"rc\":$rc,\"tail\":\"$tail_blob\"}"
        # P3-1.3: emit the structured `restart_failed_detail`
        # event alongside the legacy `restart_failed` payload
        # so the report generator can classify the residual
        # failures (port-collision / backend-down /
        # crash-mid-startup / unknown). The detail event
        # carries the dynomited stderr+log tails, not the
        # start-host.sh restart-log, because start-host.sh's
        # output is mostly its own progress trace; the
        # actionable diagnostic is what dynomited itself
        # printed before exiting.
        emit_restart_failed_detail "$rc"
    fi
}

# ---------- process faults ----------

fault_process_pause() {
    if pid=$(dyn_pid); then
        local dur=$(( RANDOM % 11 + 5 ))
        event fault_process_pause "{\"pid\":$pid,\"duration\":$dur}"
        kill -STOP "$pid" 2>/dev/null || true
        sleep "$dur"
        kill -CONT "$pid" 2>/dev/null || true
        event fault_process_pause_end "{\"pid\":$pid,\"duration\":$dur}"
    else
        event fault_process_pause_skipped "{\"reason\":\"no-dynomited\"}"
    fi
}

fault_process_kill() {
    if pid=$(dyn_pid); then
        event fault_process_kill "{\"pid\":$pid}"
        kill -KILL "$pid" 2>/dev/null || true
        # Wait for the kernel to reap before restart.
        for _ in $(seq 1 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
    else
        event fault_process_kill_skipped "{\"reason\":\"no-dynomited\"}"
    fi
    sleep 1
    restart_dynomited
    event fault_process_kill_end "{}"
}

fault_process_redis_bounce() {
    if id=$(redis_pid); then
        event fault_process_redis_bounce "{\"id\":\"$id\",\"mode\":\"$MODE\"}"
        case "$id" in
            container:*)
                local name="${id#container:}"
                if command -v podman >/dev/null 2>&1; then
                    podman rm -f "$name" >/dev/null 2>&1 || true
                elif command -v docker >/dev/null 2>&1; then
                    docker rm -f "$name" >/dev/null 2>&1 || true
                fi
                sleep 1
                local bounce_log="$LOGS/restart-backend-$DC_NAME.log"
                MODE="$MODE" bash "$ROOT/src/scripts/chaos-multi-host/start-host.sh" \
                    "$DC_NAME" "$TOKENS" "$SEEDS" \
                    "$DATASTORE_PORT" "$DYN_LISTEN_PORT" \
                    "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" \
                    >> "$bounce_log" 2>&1
                local bounce_rc=$?
                if [ "$bounce_rc" -ne 0 ]; then
                    local bounce_tail
                    bounce_tail=$(json_string_escape "$(tail -n 50 "$bounce_log" 2>/dev/null || true)")
                    event fault_process_redis_bounce_failed \
                        "{\"reason\":\"start-host.sh-nonzero\",\"rc\":$bounce_rc,\"tail\":\"$bounce_tail\"}"
                fi
                ;;
            *)
                kill -KILL "$id" 2>/dev/null || true
                sleep 1
                case "$MODE" in
                    memcache)
                        local memcached
                        memcached=$(command -v memcached || true)
                        if [ -n "$memcached" ]; then
                            nohup "$memcached" \
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
                        local redis
                        redis=$(command -v redis-server || true)
                        if [ -n "$redis" ]; then
                            nohup "$redis" \
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
        event fault_process_redis_bounce_end "{}"
    else
        event fault_process_redis_bounce_skipped "{\"reason\":\"no-redis\"}"
    fi
}

# ---------- network faults ----------

# All network faults clear-and-replace the root qdisc on the
# configured device. Cleanup is therefore the same single
# `tc qdisc del dev <dev> root` call regardless of which
# sub-fault is active. Idempotent.
cleanup_network() {
    tc qdisc del dev "$CHAOS_NETEM_DEV" root 2>/dev/null || true
}

fault_network_partition() {
    local dev="$CHAOS_NETEM_DEV"
    local dur=$(( RANDOM % 26 + 5 ))            # 5-30s
    local target_port="${DYN_LISTEN_PORT:-18101}"
    cleanup_network
    if ! tc qdisc add dev "$dev" root handle 1: prio 2>/dev/null; then
        event fault_network_partition_skipped "{\"reason\":\"tc-prio-add-failed\"}"
        return 0
    fi
    tc qdisc add dev "$dev" parent 1:3 handle 30: netem loss 100% 2>/dev/null || true
    tc filter add dev "$dev" protocol ip parent 1: prio 1 \
        u32 match ip dport "$target_port" 0xffff flowid 1:3 2>/dev/null || true
    event fault_network_partition \
        "{\"dev\":\"$dev\",\"port\":$target_port,\"duration\":$dur}"
    sleep "$dur"
    cleanup_network
    event fault_network_partition_end "{\"dev\":\"$dev\"}"
}

fault_network_delay() {
    local dev="$CHAOS_NETEM_DEV"
    local ms=$(( RANDOM % 151 + 50 ))           # 50-200ms
    local dur=$(( RANDOM % 61 + 30 ))           # 30-90s
    cleanup_network
    if ! tc qdisc add dev "$dev" root netem delay "${ms}ms" 2>/dev/null; then
        event fault_network_delay_skipped "{\"reason\":\"tc-netem-add-failed\"}"
        return 0
    fi
    event fault_network_delay \
        "{\"dev\":\"$dev\",\"delay_ms\":$ms,\"duration\":$dur}"
    sleep "$dur"
    cleanup_network
    event fault_network_delay_end "{\"dev\":\"$dev\"}"
}

fault_network_loss() {
    local dev="$CHAOS_NETEM_DEV"
    local pct=$(( RANDOM % 5 + 1 ))             # 1-5%
    local dur=$(( RANDOM % 31 + 30 ))           # 30-60s
    cleanup_network
    if ! tc qdisc add dev "$dev" root netem loss "${pct}%" 2>/dev/null; then
        event fault_network_loss_skipped "{\"reason\":\"tc-netem-add-failed\"}"
        return 0
    fi
    event fault_network_loss \
        "{\"dev\":\"$dev\",\"pct\":$pct,\"duration\":$dur}"
    sleep "$dur"
    cleanup_network
    event fault_network_loss_end "{\"dev\":\"$dev\"}"
}

fault_network_bandcap() {
    local dev="$CHAOS_NETEM_DEV"
    local dur=30
    cleanup_network
    if ! tc qdisc add dev "$dev" root handle 1: tbf rate 1mbit burst 32kbit latency 100ms 2>/dev/null; then
        event fault_network_bandcap_skipped "{\"reason\":\"tc-tbf-add-failed\"}"
        return 0
    fi
    event fault_network_bandcap \
        "{\"dev\":\"$dev\",\"rate\":\"1mbit\",\"duration\":$dur}"
    sleep "$dur"
    cleanup_network
    event fault_network_bandcap_end "{\"dev\":\"$dev\"}"
}

# ---------- clock faults ----------

# Clock skew is applied by re-launching dynomited under
# faketime. We can't apply faketime to a running process. The
# CLOCK_SKEW_MARKER file is a hint for start-host.sh on the
# rare path where a non-restart_dynomited launch happens; the
# primary path is just FAKETIME=<offset> restart_dynomited.
cleanup_clock() {
    rm -f "$CLOCK_SKEW_MARKER" 2>/dev/null || true
}

fault_clock_skew() {
    # Pick offset: 50% positive (+30..120s), 50% negative (-10s).
    local offset
    if [ $(( RANDOM % 2 )) -eq 0 ]; then
        offset="+$(( RANDOM % 91 + 30 ))s"
        local sub="positive_skew"
    else
        offset="-10s"
        local sub="negative_skew"
    fi
    local dur=$(( RANDOM % 61 + 60 ))            # 60-120s
    event fault_clock_skew "{\"offset\":\"$offset\",\"sub\":\"$sub\",\"duration\":$dur}"
    echo "$offset" > "$CLOCK_SKEW_MARKER"
    # Kill and restart with FAKETIME set; start-host.sh wraps
    # the dynomited launch with the faketime prefix.
    if pid=$(dyn_pid); then
        kill -KILL "$pid" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
    fi
    sleep 1
    FAKETIME="$offset" restart_dynomited clock_skew_restart_failed
    sleep "$dur"
    # Restore real clock by killing the skewed dynomited and
    # restarting normally.
    cleanup_clock
    if pid=$(dyn_pid); then
        kill -KILL "$pid" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
    fi
    sleep 1
    restart_dynomited clock_skew_revert_failed
    event fault_clock_skew_end "{\"offset\":\"$offset\"}"
}

# ---------- disk faults ----------

cleanup_disk() {
    rm -f "$BALLAST_FILE" "$BALLAST_FULL_FILE" 2>/dev/null || true
    if [ -d "$CHAOS_CGROUP" ] && [ -f "$CHAOS_CGROUP/io.max" ]; then
        # Reset every device entry to "max". Reading io.max
        # returns one line per limited device; if no limits are
        # set the file is empty and the loop is a no-op.
        local line dev_field
        while IFS= read -r line; do
            dev_field=$(awk '{print $1}' <<<"$line")
            if [ -n "$dev_field" ]; then
                echo "$dev_field rbps=max wbps=max riops=max wiops=max" \
                    > "$CHAOS_CGROUP/io.max" 2>/dev/null || true
            fi
        done < "$CHAOS_CGROUP/io.max"
    fi
}

# Compute the size of a fill-to-target-percent ballast file,
# capped at 10 GiB so a giant /scratch doesn't trigger a
# multi-minute dd.
compute_ballast_kb() {
    local target_pct="$1"
    local mount_point="$ROOT"
    local total_kb avail_kb used_kb want_used_kb pad_kb
    if ! read -r total_kb avail_kb < <(df -P "$mount_point" 2>/dev/null \
            | awk 'NR==2 {print $2, $4}'); then
        printf '0'; return
    fi
    used_kb=$(( total_kb - avail_kb ))
    want_used_kb=$(( total_kb * target_pct / 100 ))
    pad_kb=$(( want_used_kb - used_kb ))
    if [ "$pad_kb" -le 0 ]; then
        printf '0'; return
    fi
    if [ "$pad_kb" -gt 10485760 ]; then
        pad_kb=10485760
    fi
    printf '%d' "$pad_kb"
}

fault_disk_squeeze() {
    local dur=30
    local pad_kb
    pad_kb=$(compute_ballast_kb 95)
    if [ "$pad_kb" -eq 0 ]; then
        event fault_disk_squeeze_skipped "{\"reason\":\"already-95-full-or-df-failed\"}"
        return 0
    fi
    local mb=$(( pad_kb / 1024 ))
    if [ "$mb" -lt 1 ]; then mb=1; fi
    if ! dd if=/dev/zero of="$BALLAST_FILE" bs=1M count="$mb" status=none 2>/dev/null; then
        rm -f "$BALLAST_FILE" 2>/dev/null || true
        event fault_disk_squeeze_skipped "{\"reason\":\"dd-failed\",\"mb\":$mb}"
        return 0
    fi
    event fault_disk_squeeze "{\"mb\":$mb,\"duration\":$dur}"
    sleep "$dur"
    rm -f "$BALLAST_FILE"
    event fault_disk_squeeze_end "{}"
}

fault_disk_full() {
    local dur=5
    # dd until ENOSPC; rc!=0 here is the goal, not a failure.
    dd if=/dev/zero of="$BALLAST_FULL_FILE" bs=1M status=none 2>/dev/null || true
    if [ ! -f "$BALLAST_FULL_FILE" ]; then
        event fault_disk_full_skipped "{\"reason\":\"dd-create-failed\"}"
        return 0
    fi
    local sz_kb
    sz_kb=$(stat -c '%s' "$BALLAST_FULL_FILE" 2>/dev/null || echo 0)
    sz_kb=$(( sz_kb / 1024 ))
    event fault_disk_full "{\"kb\":$sz_kb,\"duration\":$dur}"
    sleep "$dur"
    rm -f "$BALLAST_FULL_FILE"
    event fault_disk_full_end "{}"
}

fault_disk_iolat() {
    local dur=30
    local maj_min=""
    if command -v findmnt >/dev/null 2>&1; then
        maj_min=$(findmnt -no MAJ:MIN --target "$ROOT" 2>/dev/null | head -1)
    fi
    if [ -z "$maj_min" ]; then
        event fault_disk_iolat_skipped "{\"reason\":\"no-majmin-for-root\"}"
        return 0
    fi
    if [ ! -d "$CHAOS_CGROUP" ]; then
        if ! mkdir "$CHAOS_CGROUP" 2>/dev/null; then
            event fault_disk_iolat_skipped \
                "{\"reason\":\"cgroup-mkdir-failed\",\"path\":\"$CHAOS_CGROUP\"}"
            return 0
        fi
    fi
    local id rpid
    if id=$(redis_pid); then
        case "$id" in
            container:*)
                event fault_disk_iolat_skipped "{\"reason\":\"container-redis-not-supported\"}"
                return 0
                ;;
        esac
        rpid="$id"
    else
        event fault_disk_iolat_skipped "{\"reason\":\"no-redis-pid\"}"
        return 0
    fi
    if ! echo "$rpid" > "$CHAOS_CGROUP/cgroup.procs" 2>/dev/null; then
        event fault_disk_iolat_skipped \
            "{\"reason\":\"cgroup-procs-write-failed\",\"pid\":$rpid}"
        return 0
    fi
    # 1 MiB/s read+write throughput cap. The injection target is
    # latency, not bandwidth; cgroups v2 only exposes throughput
    # caps via io.max so we approximate the desired latency
    # increase by capping throughput to a level that produces
    # 5ms+/op when the process is doing typical 4 KiB reads.
    if ! echo "$maj_min rbps=1048576 wbps=1048576" > "$CHAOS_CGROUP/io.max" 2>/dev/null; then
        event fault_disk_iolat_skipped \
            "{\"reason\":\"io.max-write-failed\",\"majmin\":\"$maj_min\"}"
        return 0
    fi
    event fault_disk_iolat \
        "{\"cgroup\":\"$CHAOS_CGROUP\",\"majmin\":\"$maj_min\",\"pid\":$rpid,\"duration\":$dur}"
    sleep "$dur"
    echo "$maj_min rbps=max wbps=max riops=max wiops=max" \
        > "$CHAOS_CGROUP/io.max" 2>/dev/null || true
    event fault_disk_iolat_end "{}"
}

# ---------- aggregate cleanup ----------

cleanup_all() {
    cleanup_network
    cleanup_clock
    cleanup_disk
}

# ---------- scheduler ----------

# Pick one fault sub-routine uniformly at random across the
# enabled-and-runnable classes, weighted equally per class as
# specified. Echoes the function name; caller invokes it.
pick_fault() {
    local active=()
    [ "$IS_CLASS_PROCESS" = 1 ] && [ "$RUNNABLE_PROCESS" = 1 ] && active+=(process)
    [ "$IS_CLASS_NETWORK" = 1 ] && [ "$RUNNABLE_NETWORK" = 1 ] && active+=(network)
    [ "$IS_CLASS_CLOCK"   = 1 ] && [ "$RUNNABLE_CLOCK"   = 1 ] && active+=(clock)
    [ "$IS_CLASS_DISK"    = 1 ] && [ "$RUNNABLE_DISK"    = 1 ] && active+=(disk)
    if [ "${#active[@]}" -eq 0 ]; then
        printf 'noop'; return
    fi
    local class="${active[$(( RANDOM % ${#active[@]} ))]}"
    case "$class" in
        process)
            local subs=(fault_process_pause fault_process_kill fault_process_redis_bounce)
            printf '%s' "${subs[$(( RANDOM % 3 ))]}"
            ;;
        network)
            local subs=(fault_network_partition fault_network_delay fault_network_loss fault_network_bandcap)
            printf '%s' "${subs[$(( RANDOM % 4 ))]}"
            ;;
        clock)
            printf 'fault_clock_skew'
            ;;
        disk)
            local subs=(fault_disk_squeeze fault_disk_full fault_disk_iolat)
            printf '%s' "${subs[$(( RANDOM % 3 ))]}"
            ;;
    esac
}

scheduler_unified() {
    local f
    while true; do
        local nap=$(( RANDOM % 121 + 60 ))   # 60-180s
        sleep "$nap"
        f=$(pick_fault)
        if [ "$f" = "noop" ]; then
            event scheduler_noop "{\"reason\":\"no-runnable-classes\"}"
            continue
        fi
        event scheduler_fire "{\"fault\":\"$f\"}"
        "$f"
        # Check dynomited is still up; if the fault left it
        # down, recover.
        if ! dyn_pid >/dev/null; then
            event scheduler_recovery "{}"
            restart_dynomited scheduler_recovery_failed
        fi
    done
}

scheduler_legacy() {
    # Three independent timers, identical to the pre-2026-05-26
    # behaviour. Default-when-MODE_FAULTS-unset path.
    local NEXT_PAUSE NEXT_KILL NEXT_REDIS_BOUNCE NOW MISSING_STREAK
    NEXT_PAUSE=$(( $(date +%s) + (RANDOM % 60 + 60) ))
    NEXT_KILL=$(( $(date +%s) + (RANDOM % 240 + 480) ))
    NEXT_REDIS_BOUNCE=$(( $(date +%s) + (RANDOM % 600 + 1200) ))
    MISSING_STREAK=0
    while true; do
        NOW=$(date +%s)
        if [ "$NOW" -ge "$NEXT_PAUSE" ]; then
            if pid=$(dyn_pid); then
                local DUR=$(( RANDOM % 11 + 5 ))
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
            fault_process_redis_bounce
            NEXT_REDIS_BOUNCE=$(( $(date +%s) + (RANDOM % 600 + 1200) ))
        fi
        # Independent recovery: hysteresis-debounced restart of
        # a missing dynomited.
        if ! dyn_pid >/dev/null; then
            if [ "$MISSING_STREAK" -ge 1 ]; then
                event recovery_restart "{\"streak\":$MISSING_STREAK}"
                restart_dynomited recovery_restart_failed
                MISSING_STREAK=0
            else
                MISSING_STREAK=$(( MISSING_STREAK + 1 ))
            fi
        else
            MISSING_STREAK=0
        fi
        sleep 5
    done
}

# ---------- main ----------

main() {
    trap 'cleanup_all; event injector_exit "{}"; exit 0' TERM INT

    event injector_start "{\"datacenter\":\"$DC_NAME\"}"

    local START_ARGS_SHA
    START_ARGS_SHA=$(file_sha256 "$START_ARGS_FILE")
    event start_args_fingerprint \
        "{\"file\":\"$START_ARGS_FILE\",\"sha256\":\"$START_ARGS_SHA\",\"mode\":\"$MODE\"}"

    parse_classes

    # Boot-time idempotent cleanup: scrub state from a previous
    # run that may have died mid-fault.
    cleanup_all

    if [ "$MODE_FAULTS_EXPLICIT" = "0" ]; then
        # Default: legacy process-only path. Don't bother with
        # the prereq pass; process is always runnable.
        RUNNABLE_PROCESS=1
        event injector_classes \
            "{\"configured\":\"<unset:legacy-process>\",\"runnable\":\"process\"}"
        scheduler_legacy
        return
    fi

    check_prereqs
    local conf_list=()
    local run_list=()
    [ "$IS_CLASS_PROCESS" = 1 ] && conf_list+=(process)
    [ "$IS_CLASS_NETWORK" = 1 ] && conf_list+=(network)
    [ "$IS_CLASS_CLOCK"   = 1 ] && conf_list+=(clock)
    [ "$IS_CLASS_DISK"    = 1 ] && conf_list+=(disk)
    [ "$RUNNABLE_PROCESS" = 1 ] && run_list+=(process)
    [ "$RUNNABLE_NETWORK" = 1 ] && run_list+=(network)
    [ "$RUNNABLE_CLOCK"   = 1 ] && run_list+=(clock)
    [ "$RUNNABLE_DISK"    = 1 ] && run_list+=(disk)
    local conf_csv run_csv
    conf_csv=$(IFS=,; echo "${conf_list[*]:-}")
    run_csv=$(IFS=,; echo "${run_list[*]:-}")
    event injector_classes \
        "{\"configured\":\"$conf_csv\",\"runnable\":\"$run_csv\"}"
    scheduler_unified
}

# Sourceability for the fault-smoke test harness. When the
# file is sourced (BASH_SOURCE != $0) we expose the fault
# routines without entering main; the test sets up the
# minimum global state it needs and calls the routines
# directly.
if [ "$__CHAOS_SOURCED" = "0" ]; then
    main "$@"
fi
