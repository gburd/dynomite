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
#   MODE=combined           - run THREE independent dynomited
#                             instances on this host, one per
#                             backend, selected by the INSTANCE
#                             env (redis|memcache|riak). Each
#                             instance is a full Netflix-style
#                             pool (one data_store + one client
#                             port) on its own port band so the
#                             three never collide:
#                               INSTANCE=redis    data_store=0,
#                                 real redis-server, base ports.
#                               INSTANCE=memcache data_store=1,
#                                 real memcached, ports +1000.
#                               INSTANCE=riak     data_store=noxu,
#                                 in-process Noxu + a riak block
#                                 (PBC + HTTP), ports +2000.
#                             Each instance writes its pidfile,
#                             config, and backend under a
#                             per-instance run subdir
#                             ($ROOT/run/<instance>/) and tags
#                             its logs with -<instance>. The
#                             coordinator launches start-host.sh
#                             once per INSTANCE; the chaos
#                             injector faults all three. The
#                             riak instance requires a dynomited
#                             built with --features riak (and
#                             --features search for the FT.*
#                             surface driven against the redis
#                             instance); aborts with a clear
#                             error if the binary does not
#                             expose --riak-pbc-listen.
#   INSTANCE=redis|memcache|riak  (only with MODE=combined)
#                             selects which of the three pools
#                             this invocation starts.
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

# In MODE=combined this selects which of the three co-located
# pools this invocation starts (redis|memcache|riak). Ignored by
# every other mode.
INSTANCE="${INSTANCE:-}"

# Port-band offset applied to every port (client/dyn/stats/
# backend/PBC) so the three combined instances never collide. Set
# per-INSTANCE in the MODE=combined arm below; 0 otherwise.
BAND_OFFSET=0

# Whether the in-process Noxu datastore backs the pool. Only the
# MODE=combined riak instance turns this on; every other
# mode/instance leaves it 0 so the external-backend bring-up runs
# unchanged.
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
    combined)
        # MODE=combined runs three independent pools per host,
        # one per backend, selected by INSTANCE. Each picks its
        # own data_store, backend, and port band so all three
        # co-exist. The redis instance keeps the base ports; the
        # memcache instance is +1000; the riak (noxu) instance is
        # +2000.
        case "${INSTANCE:-}" in
            redis)
                EFFECTIVE_MODE=redis
                DATA_STORE_VAL=0
                ENABLE_RIAK_PBC=0
                BAND_OFFSET=0
                ;;
            memcache)
                EFFECTIVE_MODE=memcache
                DATA_STORE_VAL=1
                ENABLE_RIAK_PBC=0
                BAND_OFFSET=1000
                ;;
            riak)
                # data_store=noxu (in-process), a riak block
                # exposing the PBC + HTTP listeners, and no
                # external backend. The workload driver dials the
                # band-shifted PBC port. EFFECTIVE_MODE is unused
                # when ENABLE_NOXU=1 (no external backend probe).
                EFFECTIVE_MODE=redis
                DATA_STORE_VAL=2
                ENABLE_RIAK_PBC=1
                ENABLE_NOXU=1
                BAND_OFFSET=2000
                ;;
            *)
                echo "MODE=combined requires INSTANCE=redis|memcache|riak (got '${INSTANCE:-}')" >&2
                exit 1
                ;;
        esac
        ;;
    *)
        echo "unknown MODE=$MODE (expected redis|memcache|riak|differential|combined)" >&2
        exit 1
        ;;
esac

# MODE=combined port-band shift. Apply the per-instance offset to
# every port and rewrite the seed dyn ports to the same band so
# this instance gossips only with the matching-kind instances on
# the other hosts (which all use the same band). Done before the
# C-port and RUN computations below so they see the shifted
# values.
if [ "$BAND_OFFSET" -ne 0 ]; then
    BASE_DYN_LISTEN_PORT="$DYN_LISTEN_PORT"
    DATASTORE_PORT=$((DATASTORE_PORT + BAND_OFFSET))
    DYN_LISTEN_PORT=$((DYN_LISTEN_PORT + BAND_OFFSET))
    CLIENT_LISTEN_PORT=$((CLIENT_LISTEN_PORT + BAND_OFFSET))
    STATS_LISTEN_PORT=$((STATS_LISTEN_PORT + BAND_OFFSET))
    RIAK_PBC_PORT=$((RIAK_PBC_PORT + BAND_OFFSET))
    RIAK_HTTP_PORT=$((RIAK_PBC_PORT + 1))
    if [ -n "$(printf '%s' "$SEEDS" | tr -d ' \t\n\r')" ]; then
        SEEDS="$(printf '%s\n' "$SEEDS" \
            | sed -E "s/:${BASE_DYN_LISTEN_PORT}:/:${DYN_LISTEN_PORT}:/g")"
    fi
fi

# Differential-mode port shifts. The C cluster mirrors the
# Rust ports +100 so a single host can run both proxies
# without a port collision.
C_CLIENT_LISTEN_PORT=$((CLIENT_LISTEN_PORT + 100))
C_DYN_LISTEN_PORT=$((DYN_LISTEN_PORT + 100))
C_STATS_LISTEN_PORT=$((STATS_LISTEN_PORT + 100))

ROOT="${ROOT:-/scratch/dynomite-chaos}"
# In MODE=combined each instance gets its own run subdir so its
# config, pidfiles, and backend never collide with the other two
# pools on this host. Logs stay in the shared $LOGS dir but carry
# a per-instance tag.
if [ -n "$INSTANCE" ]; then
    RUN="$ROOT/run/$INSTANCE"
    LOG_TAG="$DC_NAME-$INSTANCE"
else
    RUN="$ROOT/run"
    LOG_TAG="$DC_NAME"
fi
LOGS="$ROOT/logs"
mkdir -p "$RUN" "$LOGS"

# Per-host directory the in-process Noxu environment opens at
# when the MODE=combined riak instance is selected. One dir per
# DC (under the per-instance run subdir) so a single host running
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

# When the Riak PBC listener is enabled (MODE=riak or the
# MODE=combined riak instance), verify the binary was built with
# the `riak` feature. The CLI only registers --riak-pbc-listen
# behind `#[cfg(feature = "riak")]`, so a `--help` probe is the
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

# Backend bring-up. The MODE=combined riak instance backs the
# pool with the in-process Noxu datastore, so there is no
# external redis/memcached process to start or probe; we only
# create the per-host Noxu directory and skip straight to the
# dynomited launch. Every other mode/instance resolves, starts,
# and protocol-probes an external backend as before.
if [ "$ENABLE_NOXU" = "1" ]; then
    mkdir -p "$NOXU_PATH"
    # Noxu guards the environment with an advisory flock(2) on
    # `noxu.lck` (`fs2::FileExt::try_lock_exclusive`), which the
    # kernel releases automatically when the holding process
    # dies -- including a `SIGKILL` from the chaos injector. We
    # therefore do NOT touch the lock file: a fresh open after a
    # hard kill reacquires it cleanly. Deleting it would be
    # actively harmful -- unlinking the inode another live
    # process still holds an flock on lets a second opener flock
    # a brand-new inode and open the same environment
    # concurrently (two writers). The coordinator's pre-kill of
    # stale processes is what guarantees single ownership across
    # restarts; noxu's flock enforces it.
    echo "==> ENABLE_NOXU: in-process Noxu datastore at $NOXU_PATH (no external backend)"
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
    stale_name="dyn-chaos-$stale_label-$LOG_TAG"
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
            --logfile "$LOGS/redis-$LOG_TAG.log" \
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
            > "$LOGS/memcached-$LOG_TAG.log" 2>&1 &
    fi
    BACKEND_PID=$!
    echo "$BACKEND_PID" > "$RUN/redis.pid"
else
    CONTAINER_NAME="dyn-chaos-$BACKEND_LABEL-$LOG_TAG"
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
            > "$LOGS/$BACKEND_LABEL-$LOG_TAG-container.id" 2>&1
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
            > "$LOGS/$BACKEND_LABEL-$LOG_TAG-container.id" 2>&1
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
# datastore backs the pool (the MODE=combined riak instance).
# data_store=2 above selects Noxu; noxu_path tells dynomited
# where to open the environment. The servers: line stays as a
# syntactic placeholder; the Noxu backend supervisor ignores it.
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
# (MODE=riak or the MODE=combined riak instance). The block is a
# YAML sibling of dyn_o_mite (under the same top-level pool key)
# read by the binary's --features riak code path. The driver
# will dial 127.0.0.1:$RIAK_PBC_PORT. When the Noxu datastore
# backs the pool we additionally emit http_listen so the Riak
# HTTP gateway comes up against the same Noxu store the PBC
# listener serves.
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
    -o "$LOGS/dynomited-$LOG_TAG.log" \
    -v 6 \
    > "$LOGS/dynomited-$LOG_TAG.stderr" 2>&1 &
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
    tail -50 "$LOGS/dynomited-$LOG_TAG.stderr" 2>/dev/null >&2 || echo "    (no stderr log)" >&2
    echo "==> dynomited log tail (last 50 lines):" >&2
    tail -50 "$LOGS/dynomited-$LOG_TAG.log" 2>/dev/null >&2 || echo "    (no log file)" >&2
    exit 1
fi
echo "==> dynomited up on $LOG_TAG (stats:$STATS_LISTEN_PORT bound after $i tick(s) of ${STATS_TICK_SECS}s)"

# ENABLE_NOXU readiness: in addition to the stats listener
# (proven above), confirm the Riak PBC listener is accepting
# connections. A bare TCP connect to the PBC port is enough to
# prove the surface is live before the workload drivers attach.
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
        tail -50 "$LOGS/dynomited-$LOG_TAG.stderr" 2>/dev/null >&2 || echo "    (no stderr log)" >&2
        echo "==> dynomited log tail (last 50 lines):" >&2
        tail -50 "$LOGS/dynomited-$LOG_TAG.log" 2>/dev/null >&2 || echo "    (no log file)" >&2
        exit 1
    fi
    echo "==> Riak PBC listener up on $LOG_TAG (pbc:$RIAK_PBC_PORT, http:$RIAK_HTTP_PORT, noxu)"
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
