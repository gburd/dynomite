#!/usr/bin/env bash
#
# Per-host startup script for the multi-host chaos test.
# Runs on every cluster host. Receives $DC_NAME and $TOKENS and
# the seed list as args, starts a local datastore (redis-server
# or memcached, controlled by $MODE) and a single dynomited
# instance, both writing logs into /scratch/dynomite-chaos.
#
# Environment knobs:
#   MODE=redis    (default) - run redis-server, data_store=0
#   MODE=memcache           - run memcached, data_store=1
#   MODE=riak               - run redis-server (used as the
#                             dispatcher's backing store, even
#                             though the workload driver dials
#                             dyniak's PBC listener) AND
#                             configure dynomited's riak.pbc_listen
#                             so the workload driver can drive
#                             Riak PBC traffic. Requires a
#                             dynomited binary built with
#                             --features riak; aborts with a
#                             clear error if the binary does not
#                             expose the --riak-pbc-listen flag.
#   MODE=differential       - run the existing Rust dynomited
#                             on its configured ports AND a C
#                             `dynomite` reference on shifted
#                             ports (+100 on client/dyn/stats).
#                             Both proxies front the same redis
#                             backend so phase 4 can compare
#                             responses byte-for-byte. Requires
#                             $ROOT/cref-build/dynomite to be
#                             present (built by
#                             scripts/chaos-multi-host/build_cref_remote.sh);
#                             aborts with a clear error if the
#                             binary is missing.
#   MODE=unified            - run a single dynomited with
#                             data_store=noxu (the in-process
#                             Noxu datastore) plus a riak block
#                             exposing BOTH a PBC listener and an
#                             HTTP gateway. One shared
#                             Arc<NoxuDatastore> backs the Redis
#                             (RESP / RediSearch FT.*) client
#                             proxy AND the Riak PBC/HTTP
#                             surface, so a key SET via RESP and
#                             a GET via Riak PBC hit the same
#                             store. No external redis/memcached
#                             backend is started (noxu is
#                             in-process). Requires a dynomited
#                             built with --features riak (and
#                             --features search for the FT.*
#                             surface); aborts with a clear
#                             error if the binary does not
#                             expose --riak-pbc-listen.
#   ROOT=/scratch/dynomite-chaos (default install root)

set -euo pipefail

DC_NAME="${1:?DC name required}"
TOKENS="${2:?token list required}"
# Allow an empty seeds string (single-host smoke); only the
# unset case is an error.
SEEDS="${3?seeds string required}"
DATASTORE_PORT="${4:-17100}"
DYN_LISTEN_PORT="${5:-18101}"
CLIENT_LISTEN_PORT="${6:-18102}"
STATS_LISTEN_PORT="${7:-22222}"
RIAK_PBC_PORT="${8:-21800}"
# Riak HTTP gateway port. Defaults to one above the PBC port
# (e.g. PBC 21800 -> HTTP 21801) so a single positional arg
# pins both listeners; operators may override via the env.
RIAK_HTTP_PORT="${RIAK_HTTP_PORT:-$((RIAK_PBC_PORT + 1))}"
MODE="${MODE:-redis}"

# Whether the in-process Noxu datastore backs the pool. Only
# MODE=unified turns this on; every other mode leaves it 0 so
# the external-backend bring-up runs unchanged.
ENABLE_NOXU=0

case "$MODE" in
    redis)
        EFFECTIVE_MODE=redis
        DATA_STORE_VAL=0
        ENABLE_RIAK_PBC=0
        ;;
    memcache)
        EFFECTIVE_MODE=memcache
        DATA_STORE_VAL=1
        ENABLE_RIAK_PBC=0
        ;;
    riak)
        # The Riak PBC listener inside dynomited speaks to the
        # in-process MemoryDatastore by default (no noxu_path
        # required), so we do NOT need a separate Riak server.
        # We still bring up redis as the dispatcher's data_store
        # so the engine's Redis-front pipeline is healthy and
        # the chaos injector's redis-bounce step has something
        # to bounce; the workload driver will only hit
        # 127.0.0.1:$RIAK_PBC_PORT.
        EFFECTIVE_MODE=redis
        DATA_STORE_VAL=0
        ENABLE_RIAK_PBC=1
        ;;
    differential)
        # Both proxies (Rust + C) speak Redis to a shared
        # backend on $DATASTORE_PORT. The C cluster shadows
        # the Rust one on shifted ports.
        EFFECTIVE_MODE=redis
        DATA_STORE_VAL=0
        ENABLE_RIAK_PBC=0
        ;;
    unified)
        # One dynomited, one shared in-process Noxu datastore
        # (data_store=2). The Redis-front proxy and the Riak
        # PBC/HTTP listeners both run against that single
        # Arc<NoxuDatastore>, so a value written over RESP is
        # readable over Riak PBC and vice versa. No external
        # redis/memcached backend is started.
        EFFECTIVE_MODE=unified
        DATA_STORE_VAL=2
        ENABLE_RIAK_PBC=1
        ENABLE_NOXU=1
        ;;
    *)
        echo "unknown MODE=$MODE (expected redis|memcache|riak|differential|unified)" >&2
        exit 1
        ;;
esac

# Differential-mode port shifts. The C cluster mirrors the
# Rust ports +100 so a single host can run both proxies
# without a port collision.
C_CLIENT_LISTEN_PORT=$((CLIENT_LISTEN_PORT + 100))
C_DYN_LISTEN_PORT=$((DYN_LISTEN_PORT + 100))
C_STATS_LISTEN_PORT=$((STATS_LISTEN_PORT + 100))

ROOT="${ROOT:-/scratch/dynomite-chaos}"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
mkdir -p "$RUN" "$LOGS"

# Per-host directory the in-process Noxu environment opens at
# when MODE=unified. One dir per DC so a single host running
# multiple DCs (smoke / HOSTS_OVERRIDE) never shares a Noxu
# environment lock across pools.
NOXU_PATH="$RUN/noxu-$DC_NAME"

# Discover dynomited binary.
if [ -x "$ROOT/build/release/dynomited" ]; then
    DYNOMITED="$ROOT/build/release/dynomited"
elif [ -x "$ROOT/src/target/release/dynomited" ]; then
    DYNOMITED="$ROOT/src/target/release/dynomited"
elif [ -n "${DYNOMITED:-}" ] && [ -x "$DYNOMITED" ]; then
    : # use whatever the caller exported
else
    echo "no dynomited binary found in $ROOT/build/release or $ROOT/src/target/release" >&2
    exit 1
fi

# When the Riak PBC listener is enabled (MODE=riak or
# MODE=unified), verify the binary was built with the `riak`
# feature. The CLI only registers --riak-pbc-listen behind
# `#[cfg(feature = "riak")]`, so a `--help` probe is the
# cheapest reliable check.
if [ "$ENABLE_RIAK_PBC" = "1" ]; then
    if ! "$DYNOMITED" --help 2>&1 | grep -q -- '--riak-pbc-listen'; then
        echo "==> ERROR: MODE=$MODE requires a dynomited built with" \
             "--features riak" >&2
        echo "           binary at $DYNOMITED does not expose" \
             "--riak-pbc-listen; rebuild with" >&2
        echo "           'cargo build --release -p dynomited --features riak'" >&2
        exit 1
    fi
fi

# Backend bring-up. MODE=unified backs the pool with the
# in-process Noxu datastore, so there is no external
# redis/memcached process to start or probe; we only create
# the per-host Noxu directory and skip straight to the
# dynomited launch. Every other mode resolves, starts, and
# protocol-probes an external backend as before.
if [ "$ENABLE_NOXU" = "1" ]; then
    mkdir -p "$NOXU_PATH"
    echo "==> MODE=unified: in-process Noxu datastore at $NOXU_PATH (no external backend)"
else
# Resolve the backend binary based on EFFECTIVE_MODE. Redis and
# memcached can both fall back to a container runtime, so the
# probe is identical.
if [ "$EFFECTIVE_MODE" = "redis" ]; then
    BACKEND_BIN=$(command -v redis-server || true)
    BACKEND_LABEL=redis
    BACKEND_CONTAINER_IMAGE="docker.io/library/redis:7-alpine"
else
    BACKEND_BIN=$(command -v memcached || true)
    BACKEND_LABEL=memcached
    BACKEND_CONTAINER_IMAGE="docker.io/library/memcached:1.6-alpine"
fi
BACKEND_CONTAINER_TOOL=""
if [ -z "$BACKEND_BIN" ]; then
    if command -v podman >/dev/null 2>&1; then
        BACKEND_CONTAINER_TOOL=podman
    elif command -v docker >/dev/null 2>&1; then
        BACKEND_CONTAINER_TOOL=docker
    else
        echo "$BACKEND_LABEL not on PATH and no podman/docker available" >&2
        exit 1
    fi
fi

# Start the datastore in the background.
echo "==> starting $BACKEND_LABEL on $DATASTORE_PORT (mode=$EFFECTIVE_MODE)"

# Bug C fix (port-side): kill any process or container bound
# to $DATASTORE_PORT before starting the new backend. The
# chaos coordinator's all-modes wrapper rotates redis ->
# memcache -> riak; if a prior mode's teardown timed out,
# the prior container or native backend survives. Without
# this step, the new backend fails to bind silently and
# dynomited talks the wrong protocol to whatever's still
# there.
#
# We kill in two passes:
#   1. Containers via the container tool, both the
#      same-label name (which the start path also rms below
#      but earlier here for the native-backend path which
#      doesn't) and the sibling-label name (e.g., we are
#      bringing up memcached so kill any leftover redis).
#   2. Native processes bound to the port via fuser or lsof.
for stale_label in redis memcached; do
    stale_name="dyn-chaos-$stale_label-$DC_NAME"
    if command -v podman >/dev/null 2>&1; then
        podman rm -f "$stale_name" >/dev/null 2>&1 || true
    fi
    if command -v docker >/dev/null 2>&1; then
        docker rm -f "$stale_name" >/dev/null 2>&1 || true
    fi
done
if command -v fuser >/dev/null 2>&1; then
    fuser -k -TERM "$DATASTORE_PORT/tcp" >/dev/null 2>&1 || true
    sleep 0.5
    fuser -k -KILL "$DATASTORE_PORT/tcp" >/dev/null 2>&1 || true
elif command -v lsof >/dev/null 2>&1; then
    pids=$(lsof -ti ":$DATASTORE_PORT" -sTCP:LISTEN 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "$pids" | xargs -r kill -TERM 2>/dev/null || true
        sleep 0.5
        echo "$pids" | xargs -r kill -KILL 2>/dev/null || true
    fi
fi

if [ -n "$BACKEND_BIN" ]; then
    if [ "$EFFECTIVE_MODE" = "redis" ]; then
        nohup "$BACKEND_BIN" \
            --port "$DATASTORE_PORT" \
            --bind 127.0.0.1 \
            --daemonize no \
            --appendonly no \
            --save "" \
            --dir "$RUN" \
            --logfile "$LOGS/redis-$DC_NAME.log" \
            > /dev/null 2>&1 &
    else
        # memcached: -l 127.0.0.1, -p $DATASTORE_PORT, no UDP,
        # 64 MB cache (-m 64), foreground (-u may be required
        # when running as root; harmless when not).
        nohup "$BACKEND_BIN" \
            -l 127.0.0.1 \
            -p "$DATASTORE_PORT" \
            -U 0 \
            -m 64 \
            -v \
            > "$LOGS/memcached-$DC_NAME.log" 2>&1 &
    fi
    BACKEND_PID=$!
    echo "$BACKEND_PID" > "$RUN/redis.pid"
else
    CONTAINER_NAME="dyn-chaos-$BACKEND_LABEL-$DC_NAME"
    # Container with our exact name was already killed by
    # the prefix sweep above; this rm -f is belt-and-braces
    # to handle the case where the sweep raced a fresh
    # podman/docker create.
    "$BACKEND_CONTAINER_TOOL" rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    if [ "$EFFECTIVE_MODE" = "redis" ]; then
        "$BACKEND_CONTAINER_TOOL" run -d \
            --name "$CONTAINER_NAME" \
            --network=host \
            --rm \
            "$BACKEND_CONTAINER_IMAGE" \
            redis-server \
                --port "$DATASTORE_PORT" \
                --bind 127.0.0.1 \
                --appendonly no \
                --save "" \
            > "$LOGS/$BACKEND_LABEL-$DC_NAME-container.id" 2>&1
    else
        "$BACKEND_CONTAINER_TOOL" run -d \
            --name "$CONTAINER_NAME" \
            --network=host \
            --rm \
            "$BACKEND_CONTAINER_IMAGE" \
            memcached \
                -l 127.0.0.1 \
                -p "$DATASTORE_PORT" \
                -U 0 \
                -m 64 \
                -v \
            > "$LOGS/$BACKEND_LABEL-$DC_NAME-container.id" 2>&1
    fi
    # The container's PID isn't directly tracked; record the
    # container name in redis.pid (legacy filename, now
    # mode-agnostic) so the injector knows what to bounce.
    echo "container:$CONTAINER_NAME" > "$RUN/redis.pid"
fi

# Bug C fix companion: wait for the backend to accept
# connections AND speak the expected protocol. The probe
# is the load-bearing check: without it, a stale backend on
# $DATASTORE_PORT (wrong protocol) would happily accept TCP
# but reply garbage to every probe; before this fix
# dynomited would then enter a tight reconnect loop
# (Bug A's parent symptom).
PROBE_OK=0
for i in $(seq 1 30); do
    if [ "$EFFECTIVE_MODE" = "redis" ]; then
        if printf 'PING\r\n' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$DATASTORE_PORT && cat >&9 && head -c5 <&9" 2>/dev/null | grep -q PONG; then
            PROBE_OK=1
            break
        fi
    else
        if printf 'version\r\n' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$DATASTORE_PORT && cat >&9 && head -c8 <&9" 2>/dev/null | grep -q VERSION; then
            PROBE_OK=1
            break
        fi
    fi
    sleep 0.2
done
if [ "$PROBE_OK" -ne 1 ]; then
    echo "==> ERROR: backend on $DATASTORE_PORT did not respond to $EFFECTIVE_MODE protocol probe within 6 seconds" >&2
    echo "==>        the port may be bound by a stale process from a prior mode; refusing to start dynomited" >&2
    exit 1
fi
fi

# Write the dynomited config.
CONF="$RUN/dynomite.yml"
cat > "$CONF" <<EOF
dyn_o_mite:
  listen: 0.0.0.0:$CLIENT_LISTEN_PORT
  dyn_listen: 0.0.0.0:$DYN_LISTEN_PORT
  stats_listen: 127.0.0.1:$STATS_LISTEN_PORT
  servers:
    - 127.0.0.1:$DATASTORE_PORT:1
  tokens: '$TOKENS'
  datacenter: $DC_NAME
  rack: rack-1
  data_store: $DATA_STORE_VAL
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  enable_gossip: true
  gos_interval: 1000
  timeout: 5000
  auto_eject_hosts: true
  server_failure_limit: 2
  server_retry_timeout: 5000
  preconnect: false
  client_connections: 1000
  datastore_connections: 8
  local_peer_connections: 4
  remote_peer_connections: 4
  dyn_read_timeout: 1000
  dyn_write_timeout: 1000
EOF

# Append the noxu_path directive when the in-process Noxu
# datastore backs the pool (MODE=unified). data_store=2 above
# selects Noxu; noxu_path tells dynomited where to open the
# environment. The servers: line stays as a syntactic
# placeholder; the Noxu backend supervisor ignores it.
if [ "$ENABLE_NOXU" = "1" ]; then
    cat >> "$CONF" <<EOF
  noxu_path: $NOXU_PATH
EOF
fi

# Append the seeds block only when SEEDS is non-empty. The C
# `simple_provider` and the Rust seeds parser both treat an
# empty seeds list as "no peers" (single-host smoke); writing
# `dyn_seeds:` followed by a blank line is a YAML parse error
# in the C config loader.
if [ -n "$(printf '%s' "$SEEDS" | tr -d ' \t\n\r')" ]; then
    cat >> "$CONF" <<EOF
  dyn_seeds:
$SEEDS
EOF
fi

# Append the riak block when the Riak PBC listener is enabled
# (MODE=riak or MODE=unified). The block is a YAML sibling of
# dyn_o_mite (under the same top-level pool key) read by the
# binary's --features riak code path. The driver will dial
# 127.0.0.1:$RIAK_PBC_PORT. In MODE=unified we additionally
# emit http_listen so the Riak HTTP gateway comes up against
# the same shared Noxu store the PBC listener serves.
if [ "$ENABLE_RIAK_PBC" = "1" ]; then
    cat >> "$CONF" <<EOF
  riak:
    pbc_listen: 0.0.0.0:$RIAK_PBC_PORT
EOF
    if [ "$ENABLE_NOXU" = "1" ]; then
        cat >> "$CONF" <<EOF
    http_listen: 0.0.0.0:$RIAK_HTTP_PORT
EOF
    fi
fi

# Start dynomited. The chaos injector's clock-skew fault
# may set the FAKETIME environment variable (or write the
# offset to $RUN/clock-skew-active) to launch the binary under
# faketime; both forms are honoured here. Without faketime
# available the env knob is silently ignored so the launch
# still succeeds with a real clock.
FAKETIME_PREFIX=()
if [ -z "${FAKETIME:-}" ] && [ -f "$RUN/clock-skew-active" ]; then
    FAKETIME="$(cat "$RUN/clock-skew-active" 2>/dev/null || true)"
fi
if [ -n "${FAKETIME:-}" ] && command -v faketime >/dev/null 2>&1; then
    FAKETIME_PREFIX=(faketime "$FAKETIME")
    echo "==> starting dynomited under faketime offset=$FAKETIME (DC=$DC_NAME, tokens=$TOKENS)"
else
    echo "==> starting dynomited (DC=$DC_NAME, tokens=$TOKENS)"
fi
nohup "${FAKETIME_PREFIX[@]}" "$DYNOMITED" \
    -c "$CONF" \
    -p "$RUN/dynomited.pid" \
    -o "$LOGS/dynomited-$DC_NAME.log" \
    -v 6 \
    > "$LOGS/dynomited-$DC_NAME.stderr" 2>&1 &
DYN_PID=$!
# `-p` writes its own pid; if the binary exits early, capture the
# spawn pid as a fallback.
echo "$DYN_PID" > "$RUN/dynomited.spawn-pid"

# Wait for dynomited's stats endpoint to come up so the
# caller can move on. Use curl when present, else /dev/tcp.
# In MODE=differential we do not exit on the Rust side; the
# C proxy still needs to come up below.
#
# Issue A (Pass-7 arnold redis-mode failure): the prior budget
# was 60 attempts at 0.5s = 30s wall-clock. arnold has more
# contended I/O than floki/meh (Tailscale relay + podman +
# co-tenant workloads) and dynomited's stats listener takes
# longer than 30s to bind on a hot host. We now budget the
# poll loop in seconds and tick at 0.2s for finer-grained
# detection of readiness; the default of 30s preserves the
# previous wall-clock while operators can extend it via
# START_HOST_STATS_TIMEOUT_SECS for known-slow hosts.
# 30s / 0.2s = 150 attempts is the default.
STATS_TIMEOUT_SECS="${START_HOST_STATS_TIMEOUT_SECS:-30}"
STATS_TICK_SECS="0.2"
# bash arithmetic is integer-only; compute attempts assuming
# the tick is 200ms.
STATS_MAX_ATTEMPTS=$(( STATS_TIMEOUT_SECS * 5 ))
if [ "$STATS_MAX_ATTEMPTS" -lt 1 ]; then
    STATS_MAX_ATTEMPTS=1
fi

rust_ready=0
for i in $(seq 1 "$STATS_MAX_ATTEMPTS"); do
    if command -v curl >/dev/null 2>&1; then
        if curl -s --max-time 1 "http://127.0.0.1:$STATS_LISTEN_PORT/" 2>/dev/null | grep -q '"service"'; then
            rust_ready=1
            break
        fi
    else
        if printf 'GET / HTTP/1.0\r\n\r\n' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$STATS_LISTEN_PORT && cat >&9 && cat <&9" 2>/dev/null | grep -q '"service"'; then
            rust_ready=1
            break
        fi
    fi
    sleep "$STATS_TICK_SECS"
done

if [ "$rust_ready" -ne 1 ]; then
    # Distinguish three failure modes so the operator can tell
    # them apart in the per-host start log:
    #   1. dynomited never started (spawn pid is gone, no
    #      pidfile from -p);
    #   2. dynomited started but never bound the stats port
    #      (process is alive, port is silent -- the contended-
    #      I/O case Pass-7 hit on arnold);
    #   3. dynomited crashed mid-startup (process gone, log
    #      shows fatal error).
    echo "==> dynomited never listened on stats:$STATS_LISTEN_PORT after ${STATS_TIMEOUT_SECS}s (${STATS_MAX_ATTEMPTS} attempts at ${STATS_TICK_SECS}s)" >&2
    dyn_alive=0
    dyn_pid=""
    if [ -f "$RUN/dynomited.pid" ]; then
        dyn_pid=$(cat "$RUN/dynomited.pid" 2>/dev/null || true)
    fi
    if [ -z "$dyn_pid" ] && [ -f "$RUN/dynomited.spawn-pid" ]; then
        dyn_pid=$(cat "$RUN/dynomited.spawn-pid" 2>/dev/null || true)
    fi
    if [ -n "$dyn_pid" ] && kill -0 "$dyn_pid" 2>/dev/null; then
        dyn_alive=1
    fi
    if [ "$dyn_alive" = "1" ]; then
        echo "==> failure mode: dynomited (pid=$dyn_pid) is RUNNING but did not bind stats; likely contended I/O during startup" >&2
        echo "==> raise START_HOST_STATS_TIMEOUT_SECS (currently ${STATS_TIMEOUT_SECS}s) for this host" >&2
    elif [ -n "$dyn_pid" ]; then
        echo "==> failure mode: dynomited (pid=$dyn_pid) CRASHED mid-startup; see stderr/log tail below" >&2
    else
        echo "==> failure mode: dynomited never produced a pid; spawn likely failed before exec" >&2
    fi
    echo "==> dynomited stderr tail (last 50 lines):" >&2
    tail -50 "$LOGS/dynomited-$DC_NAME.stderr" 2>/dev/null >&2 || echo "    (no stderr log)" >&2
    echo "==> dynomited log tail (last 50 lines):" >&2
    tail -50 "$LOGS/dynomited-$DC_NAME.log" 2>/dev/null >&2 || echo "    (no log file)" >&2
    exit 1
fi
echo "==> dynomited up on $DC_NAME (stats:$STATS_LISTEN_PORT bound after $i tick(s) of ${STATS_TICK_SECS}s)"

# MODE=unified readiness: in addition to the stats listener
# (proven above), confirm the Riak PBC listener is accepting
# connections. The PBC and the Redis-front proxy both run
# against the same in-process Noxu store, so a bare TCP
# connect to the PBC port is enough to prove the unified
# surface is live before the workload drivers attach.
if [ "$ENABLE_NOXU" = "1" ]; then
    pbc_ready=0
    for i in $(seq 1 "$STATS_MAX_ATTEMPTS"); do
        if printf '' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$RIAK_PBC_PORT" 2>/dev/null; then
            pbc_ready=1
            break
        fi
        sleep "$STATS_TICK_SECS"
    done
    if [ "$pbc_ready" -ne 1 ]; then
        echo "==> ERROR: Riak PBC listener on $RIAK_PBC_PORT never accepted a TCP connection after ${STATS_TIMEOUT_SECS}s" >&2
        echo "==> dynomited stderr tail (last 50 lines):" >&2
        tail -50 "$LOGS/dynomited-$DC_NAME.stderr" 2>/dev/null >&2 || echo "    (no stderr log)" >&2
        echo "==> dynomited log tail (last 50 lines):" >&2
        tail -50 "$LOGS/dynomited-$DC_NAME.log" 2>/dev/null >&2 || echo "    (no log file)" >&2
        exit 1
    fi
    echo "==> Riak PBC listener up on $DC_NAME (pbc:$RIAK_PBC_PORT, http:$RIAK_HTTP_PORT, noxu shared)"
fi

if [ "$MODE" != "differential" ]; then
    exit 0
fi

# Differential mode: bring up the C dynomite reference next
# to the Rust dynomited. Both share the same backend redis;
# the C proxy listens on shifted ports (+100). Reaching this
# point implies the Rust stats endpoint is already healthy.
start_c_dynomite() {
    local cref_bin="$ROOT/cref-build/dynomite"
    if [ ! -x "$cref_bin" ]; then
        echo "==> ERROR: MODE=differential requires $cref_bin" >&2
        echo "           run scripts/chaos-multi-host/build_cref_remote.sh on this host first" >&2
        return 1
    fi

    # Translate the Rust seed list to the C-port view by
    # rewriting the dyn_listen port in each seed line. Each
    # SEEDS entry has shape "<ip>:<dyn_port>:<rack>:<dc>:<token>".
    local c_seeds
    c_seeds="$(printf '%s\n' "$SEEDS" | sed -E "s/:$DYN_LISTEN_PORT:/:$C_DYN_LISTEN_PORT:/g")"

    # The C engine waits for at least one peer ack via gossip
    # before promoting itself out of JOINING; with no seeds the
    # node would stay in JOINING forever and reject every
    # client write. dynomite.c short-circuits to NORMAL when
    # `enable_gossip` is false. We honour that contract here:
    # populated seed list -> gossip enabled (production multi-
    # host differential mode); empty seed list -> gossip
    # disabled (single-host smoke).
    local enable_gossip_c=true
    if [ -z "$(printf '%s' "$c_seeds" | tr -d ' \t\n\r')" ]; then
        enable_gossip_c=false
    fi

    local c_conf="$RUN/dynomite-c.yml"
    cat > "$c_conf" <<CCONF
dyn_o_mite:
  listen: 0.0.0.0:$C_CLIENT_LISTEN_PORT
  dyn_listen: 0.0.0.0:$C_DYN_LISTEN_PORT
  stats_listen: 127.0.0.1:$C_STATS_LISTEN_PORT
  servers:
    - 127.0.0.1:$DATASTORE_PORT:1
  tokens: '$TOKENS'
  datacenter: $DC_NAME
  rack: rack-1
  data_store: $DATA_STORE_VAL
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  enable_gossip: $enable_gossip_c
  gos_interval: 1000
  timeout: 5000
  auto_eject_hosts: true
  server_failure_limit: 2
  server_retry_timeout: 5000
  preconnect: false
  client_connections: 1000
  datastore_connections: 8
  local_peer_connections: 4
  remote_peer_connections: 4
  dyn_read_timeout: 1000
  dyn_write_timeout: 1000
CCONF
    if [ -n "$(printf '%s' "$c_seeds" | tr -d ' \t\n\r')" ]; then
        cat >> "$c_conf" <<CCONF
  dyn_seeds:
$c_seeds
CCONF
    fi

    echo "==> starting C dynomite (DC=$DC_NAME, ports client=$C_CLIENT_LISTEN_PORT dyn=$C_DYN_LISTEN_PORT stats=$C_STATS_LISTEN_PORT)"
    nohup "$cref_bin" \
        -c "$c_conf" \
        -p "$RUN/dynomite-c.pid" \
        -o "$LOGS/dynomite-c-$DC_NAME.log" \
        -v 6 \
        > "$LOGS/dynomite-c-$DC_NAME.stderr" 2>&1 &
    local c_pid=$!
    echo "$c_pid" > "$RUN/dynomite-c.spawn-pid"

    # A bare TCP connect to the client listener is enough to
    # prove the C proxy bound and listened. The C binary's
    # stats page is plain-text rather than JSON, so we don't
    # require a `"service"` substring.
    #
    # Same Issue-A budget knob as the Rust path: 0.2s ticks,
    # default 30s, override via START_HOST_STATS_TIMEOUT_SECS.
    local i
    for i in $(seq 1 "$STATS_MAX_ATTEMPTS"); do
        if printf '' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$C_CLIENT_LISTEN_PORT" 2>/dev/null; then
            echo "==> C dynomite up on $DC_NAME (client port $C_CLIENT_LISTEN_PORT)"
            return 0
        fi
        sleep "$STATS_TICK_SECS"
    done

    echo "==> C dynomite never listened on client:$C_CLIENT_LISTEN_PORT after ${STATS_TIMEOUT_SECS}s" >&2
    local c_pid=""
    if [ -f "$RUN/dynomite-c.pid" ]; then
        c_pid=$(cat "$RUN/dynomite-c.pid" 2>/dev/null || true)
    fi
    if [ -z "$c_pid" ] && [ -f "$RUN/dynomite-c.spawn-pid" ]; then
        c_pid=$(cat "$RUN/dynomite-c.spawn-pid" 2>/dev/null || true)
    fi
    if [ -n "$c_pid" ] && kill -0 "$c_pid" 2>/dev/null; then
        echo "==> failure mode: C dynomite (pid=$c_pid) is RUNNING but did not bind; raise START_HOST_STATS_TIMEOUT_SECS" >&2
    elif [ -n "$c_pid" ]; then
        echo "==> failure mode: C dynomite (pid=$c_pid) CRASHED mid-startup" >&2
    else
        echo "==> failure mode: C dynomite never produced a pid" >&2
    fi
    echo "==> C dynomite stderr tail (last 50 lines):" >&2
    tail -50 "$LOGS/dynomite-c-$DC_NAME.stderr" 2>/dev/null >&2 || echo "    (no stderr log)" >&2
    echo "==> C dynomite log tail (last 50 lines):" >&2
    tail -50 "$LOGS/dynomite-c-$DC_NAME.log" 2>/dev/null >&2 || echo "    (no log file)" >&2
    return 1
}

if ! start_c_dynomite; then
    exit 1
fi
exit 0
