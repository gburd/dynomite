#!/usr/bin/env bash
#
# Multi-host chaos coordinator.
#
# Drives a 3-DC dynomite cluster across:
#
#   floki  (this host) - DC1, Linux x86_64, has the source
#   arnold (Tailscale) - DC2, Fedora 44 x86_64
#   nuc    (LAN, via arnold ProxyJump) - DC3, FreeBSD 15 amd64
#
# Each host runs:
#   * 1 redis-server on 127.0.0.1:17100 (datastore)
#   * 1 dynomited bound on 0.0.0.0 with peer/client/stats ports
#   * 1 workload-driver.py that issues every Redis feature class
#   * 1 chaos-injector.sh that SIGSTOP/SIGKILLs dynomited periodically
#
# Inter-host networking:
#   floki <-> arnold  : direct via Tailscale 100.x.x.x
#   arnold <-> nuc    : direct via LAN 192.168.1.x
#   floki <-> nuc     : via SSH local-forward through arnold
#
# After CHAOS_DURATION_SECS the coordinator tears everything down,
# rsyncs all logs back to floki, and runs the report script.

set -euo pipefail

# ---- configuration ----

RUN_ID="${RUN_ID:-$(date -u +%Y%m%d-%H%M%SZ)}"
DURATION="${CHAOS_DURATION_SECS:-7200}"   # 2 hours

REPO="/home/gburd/ws/dynomite"
LOCAL_LOGS="$REPO/target/chaos-multi-host/$RUN_ID"
mkdir -p "$LOCAL_LOGS"

# Peer/client/stats ports (the same on every host).
DATASTORE_PORT=17100
DYN_LISTEN_PORT=18101
CLIENT_LISTEN_PORT=18102
STATS_LISTEN_PORT=22222

# SSH-tunnel-only ports. Used to reach nuc:18101 from floki, and
# floki:18101 from nuc, both via arnold.
NUC_TUNNEL_PORT_FLOKI=19501       # floki uses this to reach nuc
FLOKI_TUNNEL_PORT_NUC=19501       # nuc uses this to reach floki

# Per-DC tokens picked to land on different ring positions.
# Token ring is 32-bit big-int; these are evenly spaced.
TOKENS_FLOKI="0"
TOKENS_ARNOLD="1431655765"
TOKENS_NUC="2863311530"

# Tailscale + LAN IPs.
FLOKI_TS_IP="100.104.16.13"
ARNOLD_TS_IP="100.117.233.104"
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

# ---- start the SSH tunnels ----

start_tunnels() {
    log "starting SSH tunnels via arnold"
    # floki -> nuc:DYN_LISTEN_PORT
    env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" \
        -fN -L "${NUC_TUNNEL_PORT_FLOKI}:${NUC_LAN_IP}:${DYN_LISTEN_PORT}" \
        arnold &
    TUN_PID_FLOKI_TO_NUC=$!
    log "  floki -> nuc tunnel pid=$TUN_PID_FLOKI_TO_NUC (local:$NUC_TUNNEL_PORT_FLOKI -> $NUC_LAN_IP:$DYN_LISTEN_PORT)"

    # The nuc -> floki tunnel runs ON nuc.
    "${NUC_SSH[@]}" "nohup ssh -o IdentitiesOnly=yes -i \$HOME/.ssh/id_ed25519 \
        -o ControlMaster=no -o ControlPath=none \
        -o StrictHostKeyChecking=accept-new \
        -fN -L ${FLOKI_TUNNEL_PORT_NUC}:${FLOKI_TS_IP}:${DYN_LISTEN_PORT} \
        gburd@arnold > /scratch/dynomite-chaos/logs/nuc-tunnel.log 2>&1 < /dev/null"

    sleep 2
    # Verify the floki -> nuc tunnel is up.
    if (echo; sleep 0.1) | nc -q 1 127.0.0.1 "$NUC_TUNNEL_PORT_FLOKI" 2>/dev/null; then
        log "  floki -> nuc tunnel: connection acceptable"
    else
        log "  floki -> nuc tunnel: NOT yet usable (will retry once dynomited is up on nuc)"
    fi
}

# ---- per-host setup ----

# Each host's seed list is host-specific because the address each
# host uses to reach a given peer depends on the host's network
# position.

write_floki_config() {
    cat <<SEEDS
    - $ARNOLD_TS_IP:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
    - 127.0.0.1:$NUC_TUNNEL_PORT_FLOKI:rack-1:dc-nuc:$TOKENS_NUC
SEEDS
}

write_arnold_config() {
    cat <<SEEDS
    - $FLOKI_TS_IP:$DYN_LISTEN_PORT:rack-1:dc-floki:$TOKENS_FLOKI
    - $NUC_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-nuc:$TOKENS_NUC
SEEDS
}

write_nuc_config() {
    cat <<SEEDS
    - 127.0.0.1:$FLOKI_TUNNEL_PORT_NUC:rack-1:dc-floki:$TOKENS_FLOKI
    - $NUC_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
SEEDS
}
# Actually arnold's IP from nuc is the LAN address.
write_nuc_config() {
    cat <<SEEDS
    - 127.0.0.1:$FLOKI_TUNNEL_PORT_NUC:rack-1:dc-floki:$TOKENS_FLOKI
    - 192.168.1.37:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
SEEDS
}

# ---- bring up each host ----

write_start_args_local() {
    cat > "$REPO/.start-args.floki" <<EOF
TOKENS=$TOKENS_FLOKI
SEEDS=$(write_floki_config | base64 -w0)
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
EOF
}

start_floki() {
    log "starting floki (DC=dc-floki)"
    mkdir -p /scratch/dynomite-chaos/run /scratch/dynomite-chaos/logs
    # On floki we use the in-tree binary at target/release.
    if [ ! -x "$REPO/target/release/dynomited" ]; then
        log "  building dynomited on floki..."
        (cd "$REPO" && cargo build --release --locked --bin dynomited >> "$LOCAL_LOGS/floki-build.log" 2>&1)
    fi
    ln -sf "$REPO/target/release/dynomited" /scratch/dynomite-chaos/build/release/dynomited 2>/dev/null || true
    mkdir -p /scratch/dynomite-chaos/build/release
    cp -f "$REPO/target/release/dynomited" /scratch/dynomite-chaos/build/release/dynomited
    # Save start-args so the injector can restart with the same shape.
    SEEDS_TMP="$LOCAL_LOGS/floki-seeds.yml"
    write_floki_config > "$SEEDS_TMP"
    cat > /scratch/dynomite-chaos/run/start-args <<EOF
TOKENS='$TOKENS_FLOKI'
SEEDS=\$(cat $SEEDS_TMP)
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
EOF
    bash "$REPO/scripts/chaos-multi-host/start-host.sh" \
        dc-floki "$TOKENS_FLOKI" "$(write_floki_config)" \
        "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" \
        >> "$LOCAL_LOGS/floki-start.log" 2>&1
    log "  floki dynomited up"
}

start_arnold() {
    log "starting arnold (DC=dc-arnold)"
    "${ARNOLD_SSH[@]}" "mkdir -p /scratch/dynomite-chaos/run /scratch/dynomite-chaos/logs"
    SEEDS_STR=$(write_arnold_config)
    "${ARNOLD_SSH[@]}" "cat > /scratch/dynomite-chaos/run/seeds <<'EOF'
$SEEDS_STR
EOF"
    "${ARNOLD_SSH[@]}" "cat > /scratch/dynomite-chaos/run/start-args <<EOF
TOKENS='$TOKENS_ARNOLD'
SEEDS=\\\$(cat /scratch/dynomite-chaos/run/seeds)
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
EOF"
    "${ARNOLD_SSH[@]}" "bash /scratch/dynomite-chaos/src/scripts/chaos-multi-host/start-host.sh \
        dc-arnold '$TOKENS_ARNOLD' '$SEEDS_STR' \
        $DATASTORE_PORT $DYN_LISTEN_PORT $CLIENT_LISTEN_PORT $STATS_LISTEN_PORT" \
        >> "$LOCAL_LOGS/arnold-start.log" 2>&1
    log "  arnold dynomited up"
}

start_nuc() {
    log "starting nuc (DC=dc-nuc)"
    "${NUC_SSH[@]}" "mkdir -p /scratch/dynomite-chaos/run /scratch/dynomite-chaos/logs"
    SEEDS_STR=$(write_nuc_config)
    "${NUC_SSH[@]}" "cat > /scratch/dynomite-chaos/run/seeds <<'EOF'
$SEEDS_STR
EOF"
    "${NUC_SSH[@]}" "cat > /scratch/dynomite-chaos/run/start-args <<EOF
TOKENS='$TOKENS_NUC'
SEEDS=\\\$(cat /scratch/dynomite-chaos/run/seeds)
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
EOF"
    # nuc is FreeBSD; bash exists but may live at /usr/local/bin/bash.
    "${NUC_SSH[@]}" "/usr/local/bin/bash /scratch/dynomite-chaos/src/scripts/chaos-multi-host/start-host.sh \
        dc-nuc '$TOKENS_NUC' '$SEEDS_STR' \
        $DATASTORE_PORT $DYN_LISTEN_PORT $CLIENT_LISTEN_PORT $STATS_LISTEN_PORT" \
        >> "$LOCAL_LOGS/nuc-start.log" 2>&1
    log "  nuc dynomited up"
}

# ---- workload + injector ----

start_workload() {
    local label="$1" host_runner=("${@:2}")
    log "starting workload-driver on $label"
    "${host_runner[@]}" "nohup python3 /scratch/dynomite-chaos/src/scripts/chaos-multi-host/workload-driver.py \
        --host 127.0.0.1 --port $CLIENT_LISTEN_PORT \
        --label $label \
        --out /scratch/dynomite-chaos/logs/workload-$label.ndjson \
        --duration $DURATION \
        --qps 200 \
        > /scratch/dynomite-chaos/logs/workload-$label.stderr 2>&1 < /dev/null &
    echo \$! > /scratch/dynomite-chaos/run/workload.pid"
}

start_injector() {
    local label="$1" host_runner=("${@:2}")
    log "starting chaos-injector on $label"
    "${host_runner[@]}" "nohup bash /scratch/dynomite-chaos/src/scripts/chaos-multi-host/chaos-injector.sh $label \
        > /scratch/dynomite-chaos/logs/injector-$label.stderr 2>&1 < /dev/null &
    echo \$! > /scratch/dynomite-chaos/run/injector.pid"
}

# ---- teardown ----

teardown() {
    log "==> TEARDOWN"
    # Kill local processes on every host.
    for spec in "floki:bash -lc" \
                "arnold:${ARNOLD_SSH[*]}" \
                "nuc:${NUC_SSH[*]}"; do
        label="${spec%%:*}"
        runner="${spec#*:}"
        log "  teardown $label"
        $runner "for f in /scratch/dynomite-chaos/run/workload.pid /scratch/dynomite-chaos/run/injector.pid /scratch/dynomite-chaos/run/dynomited.pid /scratch/dynomite-chaos/run/redis.pid; do \
            [ -f \$f ] && pid=\$(cat \$f) && kill -TERM \$pid 2>/dev/null; \
        done; sleep 2; \
        for f in /scratch/dynomite-chaos/run/workload.pid /scratch/dynomite-chaos/run/injector.pid /scratch/dynomite-chaos/run/dynomited.pid /scratch/dynomite-chaos/run/redis.pid; do \
            [ -f \$f ] && pid=\$(cat \$f) && kill -KILL \$pid 2>/dev/null; \
        done; \
        true" >> "$LOCAL_LOGS/teardown-$label.log" 2>&1 || true
    done

    # Pull logs back.
    log "  rsync arnold logs"
    rsync -az -e "$ARNOLD_RSYNC_E" arnold:/scratch/dynomite-chaos/logs/ "$LOCAL_LOGS/arnold-logs/" || true
    log "  rsync nuc logs"
    rsync -az -e "$NUC_RSYNC_E" gburd@nuc:/scratch/dynomite-chaos/logs/ "$LOCAL_LOGS/nuc-logs/" || true
    log "  copy floki logs"
    cp -r /scratch/dynomite-chaos/logs "$LOCAL_LOGS/floki-logs" 2>/dev/null || true

    # Tear down tunnels.
    if [ -n "${TUN_PID_FLOKI_TO_NUC:-}" ]; then
        kill -TERM "$TUN_PID_FLOKI_TO_NUC" 2>/dev/null || true
    fi
    "${NUC_SSH[@]}" 'pkill -f "ssh.*-fN.*-L 19501" || true' || true

    log "  done"
}

# ---- main ----

trap teardown EXIT INT TERM

log "================================================================"
log "multi-host chaos coordinator starting; run id=$RUN_ID, duration=$DURATION s"
log "================================================================"

# Sanity: source must already be on arnold and nuc.
"${ARNOLD_SSH[@]}" "[ -d /scratch/dynomite-chaos/src ]" || { log "arnold:src missing"; exit 1; }
"${NUC_SSH[@]}" "[ -d /scratch/dynomite-chaos/src ]" || { log "nuc:src missing"; exit 1; }

start_tunnels

# Bring up the cluster.
start_floki
start_arnold
start_nuc

# Wait for gossip convergence (peers should see each other within 30s).
sleep 30

start_workload dc-floki bash -lc
start_workload dc-arnold "${ARNOLD_SSH[@]}"
start_workload dc-nuc "${NUC_SSH[@]}"

start_injector dc-floki bash -lc
start_injector dc-arnold "${ARNOLD_SSH[@]}"
start_injector dc-nuc "${NUC_SSH[@]}"

log "==> all components up; sleeping for $DURATION seconds"
sleep "$DURATION"

log "==> duration elapsed"
trap - EXIT INT TERM
teardown
log "==> coordinator done"
