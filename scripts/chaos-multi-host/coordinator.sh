#!/usr/bin/env bash
#
# Multi-host chaos coordinator (pass 1).
#
# Drives a 3-DC dynomite cluster across:
#
#   floki  (this host) - DC1, Linux x86_64, has the source
#   arnold (Tailscale) - DC2, Fedora 44 x86_64
#   nuc    (LAN, via arnold ProxyJump) - DC3, FreeBSD 15 amd64
#
# Each host runs:
#   * 1 redis (native on floki/nuc, podman container on arnold)
#   * 1 dynomited bound on 0.0.0.0 with peer/client/stats ports
#   * 1 workload-driver.py issuing every Redis feature class to
#     127.0.0.1:CLIENT_LISTEN_PORT (the local dynomited)
#   * 1 chaos-injector.sh that SIGSTOP/SIGKILLs dynomited and
#     periodically bounces redis
#
# Pass 1 NOTE: the dynomited binary does not yet wire outbound
# peer connections, so traffic does NOT cross between DCs. Each
# DC operates independently. With identical tokens on every node
# and DC_ONE consistency this is consistent: every key has a
# local replica. The chaos test exercises:
#   - process stability under SIGSTOP / SIGKILL
#   - backend reconnect after redis bounce
#   - cross-platform behavior (Linux + FreeBSD)
#   - pidfile / signal handling
#   - 2-hour soak
#
# When outbound peer connections land, switch to per-DC tokens
# and DC_EACH_SAFE_QUORUM writes to exercise cross-DC routing.

set -euo pipefail

# ---- configuration ----

RUN_ID="${RUN_ID:-$(date -u +%Y%m%d-%H%M%SZ)}"
DURATION="${CHAOS_DURATION_SECS:-7200}"   # 2 hours

REPO="/home/gburd/ws/dynomite"
LOCAL_LOGS="$REPO/target/chaos-multi-host/$RUN_ID"
mkdir -p "$LOCAL_LOGS"

DATASTORE_PORT=17100
DYN_LISTEN_PORT=18101
CLIENT_LISTEN_PORT=18102
STATS_LISTEN_PORT=22222
RIAK_PBC_PORT=21800

MODE="${MODE:-redis}"
export MODE

# Per-DC distinct tokens (pass-2). Distinct token slices on the
# ring force keys to hash into a specific DC, exercising outbound
# peer connections from the dispatcher's `Replicas` plan.
# Choose roughly equally spaced 32-bit tokens. With
# `DC_QUORUM` consistency, the dispatcher will fan out to every
# replica in the local DC; with each DC owning a distinct token
# range and only one node per DC, that's still LocalDatastore
# for keys hashing into the local range and Replicas (cross-DC)
# for keys that don't. Pass-1 used identical tokens on every
# node so cross-DC routing was never triggered.
TOKENS_FLOKI="0"
TOKENS_ARNOLD="1431655765"
TOKENS_NUC="2863311530"

FLOKI_TS_IP="100.104.16.13"
ARNOLD_TS_IP="100.117.233.104"
ARNOLD_LAN_IP="192.168.1.37"
NUC_LAN_IP="192.168.1.61"

SSH_KEY="$HOME/.ssh/id_ed25519"
SSH_BASE_OPTS=(-o IdentitiesOnly=yes -i "$SSH_KEY"
               -o ControlMaster=no -o ControlPath=none
               -o StrictHostKeyChecking=accept-new
               -o ServerAliveInterval=30)

ARNOLD_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" arnold)
NUC_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" -o ProxyJump=arnold gburd@nuc)

ARNOLD_RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
NUC_RSYNC_E="ssh ${SSH_BASE_OPTS[*]} -o ProxyJump=arnold"

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOCAL_LOGS/coordinator.log" ; }

# Each host's view of the cluster: floki sees arnold via
# Tailscale and nuc as effectively unreachable (no cross-host
# routing yet); arnold can reach both floki (Tailscale) and nuc
# (LAN); nuc reaches arnold via LAN, floki effectively
# unreachable. The seed lists below reflect the topology so
# `dyn_seeds` is parsed correctly even though the binary does
# not yet open outbound peer connections.

floki_seeds() {
    cat <<SEEDS
    - $ARNOLD_TS_IP:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
SEEDS
}

arnold_seeds() {
    cat <<SEEDS
    - $FLOKI_TS_IP:$DYN_LISTEN_PORT:rack-1:dc-floki:$TOKENS_FLOKI
    - $NUC_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-nuc:$TOKENS_NUC
SEEDS
}

nuc_seeds() {
    cat <<SEEDS
    - $ARNOLD_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
SEEDS
}

# ---- per-host start ----

start_host() {
    local label="$1"; shift
    local tokens="$1"; shift
    local seeds_str="$1"; shift
    local runner=("$@")
    log "starting $label tokens=$tokens"
    "${runner[@]}" "mkdir -p /scratch/dynomite-chaos/run /scratch/dynomite-chaos/logs"
    # Persist start-args so the chaos injector can restart
    # dynomited with the same arguments after a SIGKILL.
    #
    # Build seeds.yml + start-args via stdin to avoid double
    # heredoc expansion: the previous version used
    # ssh "...cat > start-args <<EOF..." which command-substituted
    # `$(cat seeds.yml)` on the REMOTE side at write time, baking
    # the seeds into start-args without quoting and breaking the
    # later `. start-args` in chaos-injector.sh. Pipe through
    # stdin so the remote `cat` writes the bytes verbatim.
    "${runner[@]}" "cat > /scratch/dynomite-chaos/run/seeds.yml" <<EOF
$seeds_str
EOF
    "${runner[@]}" "cat > /scratch/dynomite-chaos/run/start-args" <<EOF
MODE='$MODE'
TOKENS='$tokens'
SEEDS=\$(cat /scratch/dynomite-chaos/run/seeds.yml)
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
RIAK_PBC_PORT=$RIAK_PBC_PORT
EOF
    # FreeBSD's /bin/sh is a different shell than bash; pick
    # bash explicitly for the start-host script.
    local bash_path=/bin/bash
    case "$label" in
        dc-nuc) bash_path=/usr/local/bin/bash ;;
    esac
    "${runner[@]}" "MODE='$MODE' $bash_path /scratch/dynomite-chaos/src/scripts/chaos-multi-host/start-host.sh \
        $label '$tokens' '$seeds_str' \
        $DATASTORE_PORT $DYN_LISTEN_PORT $CLIENT_LISTEN_PORT $STATS_LISTEN_PORT $RIAK_PBC_PORT" \
        >> "$LOCAL_LOGS/$label-start.log" 2>&1
    log "  $label dynomited up"
}

start_floki() {
    log "preparing floki tokens=$TOKENS_FLOKI"
    mkdir -p /scratch/dynomite-chaos/run /scratch/dynomite-chaos/logs /scratch/dynomite-chaos/build/release
    cp -f "$REPO/target/release/dynomited" /scratch/dynomite-chaos/build/release/dynomited
    # rsync source so the injector can find scripts via the same
    # /scratch/dynomite-chaos/src layout used on the remotes.
    mkdir -p /scratch/dynomite-chaos/src
    rsync -a --delete --exclude target/ --exclude .git/ --exclude _/dynomite/.git/ \
        "$REPO"/ /scratch/dynomite-chaos/src/
    SEEDS_STR=$(floki_seeds)
    cat > /scratch/dynomite-chaos/run/seeds.yml <<EOF
$SEEDS_STR
EOF
    cat > /scratch/dynomite-chaos/run/start-args <<EOF
MODE='$MODE'
TOKENS='$TOKENS_FLOKI'
SEEDS=\$(cat /scratch/dynomite-chaos/run/seeds.yml)
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
RIAK_PBC_PORT=$RIAK_PBC_PORT
EOF
    bash "$REPO/scripts/chaos-multi-host/start-host.sh" \
        dc-floki "$TOKENS_FLOKI" "$SEEDS_STR" \
        "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" "$RIAK_PBC_PORT" \
        >> "$LOCAL_LOGS/dc-floki-start.log" 2>&1
    log "  dc-floki dynomited up"
}

# ---- workload + injector ----

start_workload() {
    local label="$1"; shift
    local bash_path="$1"; shift
    local runner=("$@")
    log "starting workload-driver on $label (mode=$MODE)"
    # In riak mode the driver dials the PBC listener instead of
    # the engine's client_listen, so swap the --port wiring for
    # --riak-pbc-port. The redis/memcache modes keep using
    # --port $CLIENT_LISTEN_PORT.
    local mode_flags
    if [ "$MODE" = "riak" ]; then
        mode_flags="--mode riak --riak-pbc-port $RIAK_PBC_PORT"
    else
        mode_flags="--mode $MODE"
    fi
    "${runner[@]}" "nohup python3 /scratch/dynomite-chaos/src/scripts/chaos-multi-host/workload-driver.py \
        --host 127.0.0.1 --port $CLIENT_LISTEN_PORT \
        $mode_flags \
        --label $label \
        --out /scratch/dynomite-chaos/logs/workload-$label.ndjson \
        --duration $DURATION \
        --qps 200 \
        > /scratch/dynomite-chaos/logs/workload-$label.stderr 2>&1 < /dev/null &
    echo \$! > /scratch/dynomite-chaos/run/workload.pid"
}

start_injector() {
    local label="$1"; shift
    local bash_path="$1"; shift
    local runner=("$@")
    log "starting chaos-injector on $label"
    "${runner[@]}" "nohup $bash_path /scratch/dynomite-chaos/src/scripts/chaos-multi-host/chaos-injector.sh $label \
        > /scratch/dynomite-chaos/logs/injector-$label.stderr 2>&1 < /dev/null &
    echo \$! > /scratch/dynomite-chaos/run/injector.pid"
}

# ---- teardown ----

teardown() {
    log "==> TEARDOWN"
    for spec in \
        "dc-floki:bash:bash -lc" \
        "dc-arnold:bash:${ARNOLD_SSH[*]}" \
        "dc-nuc:bash:${NUC_SSH[*]}"; do
        IFS=: read -r label _ runner <<<"$spec"
        log "  teardown $label"
        $runner "for f in /scratch/dynomite-chaos/run/workload.pid /scratch/dynomite-chaos/run/injector.pid; do \
            [ -f \$f ] && pid=\$(cat \$f) && kill -TERM \$pid 2>/dev/null; \
        done; sleep 2; \
        for f in /scratch/dynomite-chaos/run/workload.pid /scratch/dynomite-chaos/run/injector.pid /scratch/dynomite-chaos/run/dynomited.pid; do \
            [ -f \$f ] && pid=\$(cat \$f) && kill -KILL \$pid 2>/dev/null; \
        done; \
        if [ -f /scratch/dynomite-chaos/run/redis.pid ]; then \
            id=\$(cat /scratch/dynomite-chaos/run/redis.pid); \
            case \$id in \
                container:*) (command -v podman >/dev/null && podman rm -f \${id#container:}) || (command -v docker >/dev/null && docker rm -f \${id#container:}); ;; \
                *) kill -KILL \$id 2>/dev/null; ;; \
            esac; \
        fi; \
        true" >> "$LOCAL_LOGS/$label-teardown.log" 2>&1 || true
    done

    log "  rsync arnold logs"
    rsync -az -e "$ARNOLD_RSYNC_E" arnold:/scratch/dynomite-chaos/logs/ "$LOCAL_LOGS/arnold-logs/" || true
    log "  rsync nuc logs"
    rsync -az -e "$NUC_RSYNC_E" gburd@nuc:/scratch/dynomite-chaos/logs/ "$LOCAL_LOGS/nuc-logs/" || true
    log "  copy floki logs"
    cp -r /scratch/dynomite-chaos/logs "$LOCAL_LOGS/floki-logs" 2>/dev/null || true

    log "  done; logs at $LOCAL_LOGS"
}

# ---- main ----

trap teardown EXIT INT TERM

log "================================================================"
log "multi-host chaos coordinator starting"
log "  run id:   $RUN_ID"
log "  duration: $DURATION s"
log "  mode:     $MODE"
log "  hosts:    floki arnold nuc meh"
log "  logs:     $LOCAL_LOGS"
log "================================================================"

"${ARNOLD_SSH[@]}" "[ -d /scratch/dynomite-chaos/src ]" || { log "arnold:src missing"; exit 1; }
"${NUC_SSH[@]}"    "[ -d /scratch/dynomite-chaos/src ]" || { log "nuc:src missing"; exit 1; }
"${MEH_SSH[@]}"    "[ -d /scratch/dynomite-chaos/src ]" || { log "meh:src missing"; exit 1; }

start_floki
start_host dc-arnold "$TOKENS_ARNOLD" "$(arnold_seeds)" "${ARNOLD_SSH[@]}"
start_host dc-nuc    "$TOKENS_NUC"    "$(nuc_seeds)"    "${NUC_SSH[@]}"
start_host dc-meh    "$TOKENS_MEH"    "$(meh_seeds)"    "${MEH_SSH[@]}"

# Brief settle so any deferred state is in place.
sleep 5

start_workload dc-floki  /bin/bash             bash -lc
start_workload dc-arnold /bin/bash             "${ARNOLD_SSH[@]}"
start_workload dc-nuc    /bin/bash             "${NUC_SSH[@]}"
start_workload dc-meh    /bin/bash             "${MEH_SSH[@]}"

start_injector dc-floki  /bin/bash             bash -lc
start_injector dc-arnold /bin/bash             "${ARNOLD_SSH[@]}"
start_injector dc-nuc    /usr/local/bin/bash   "${NUC_SSH[@]}"
start_injector dc-meh    /bin/bash             "${MEH_SSH[@]}"

log "==> all components up; sleeping for $DURATION seconds"
sleep "$DURATION"

log "==> duration elapsed"
trap - EXIT INT TERM
teardown
log "==> coordinator done"
