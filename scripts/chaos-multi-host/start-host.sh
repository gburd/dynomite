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
#                             dyn-riak's PBC listener) AND
#                             configure dynomited's riak.pbc_listen
#                             so the workload driver can drive
#                             Riak PBC traffic. Requires a
#                             dynomited binary built with
#                             --features riak; aborts with a
#                             clear error if the binary does not
#                             expose the --riak-pbc-listen flag.
#   ROOT=/scratch/dynomite-chaos (default install root)

set -euo pipefail

DC_NAME="${1:?DC name required}"
TOKENS="${2:?token list required}"
SEEDS="${3:?seeds string required}"
DATASTORE_PORT="${4:-17100}"
DYN_LISTEN_PORT="${5:-18101}"
CLIENT_LISTEN_PORT="${6:-18102}"
STATS_LISTEN_PORT="${7:-22222}"
RIAK_PBC_PORT="${8:-21800}"
MODE="${MODE:-redis}"

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
    *)
        echo "unknown MODE=$MODE (expected redis|memcache|riak)" >&2
        exit 1
        ;;
esac

ROOT="${ROOT:-/scratch/dynomite-chaos}"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
mkdir -p "$RUN" "$LOGS"

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

# When MODE=riak, verify the binary was built with the `riak`
# feature. The CLI only registers --riak-pbc-listen behind
# `#[cfg(feature = "riak")]`, so a `--help` probe is the
# cheapest reliable check.
if [ "$ENABLE_RIAK_PBC" = "1" ]; then
    if ! "$DYNOMITED" --help 2>&1 | grep -q -- '--riak-pbc-listen'; then
        echo "==> ERROR: MODE=riak requires a dynomited built with" \
             "--features riak" >&2
        echo "           binary at $DYNOMITED does not expose" \
             "--riak-pbc-listen; rebuild with" >&2
        echo "           'cargo build --release -p dynomited --features riak'" >&2
        exit 1
    fi
fi

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

# Wait for the backend to accept connections.
for i in $(seq 1 30); do
    if [ "$EFFECTIVE_MODE" = "redis" ]; then
        if printf 'PING\r\n' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$DATASTORE_PORT && cat >&9 && head -c4 <&9" 2>/dev/null | grep -q PONG; then
            break
        fi
    else
        if printf 'version\r\n' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$DATASTORE_PORT && cat >&9 && head -c8 <&9" 2>/dev/null | grep -q VERSION; then
            break
        fi
    fi
    sleep 0.2
done

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
  dyn_seeds:
$SEEDS
EOF

# Append the riak block when MODE=riak. The block is a YAML
# sibling of dyn_o_mite (under the same top-level pool key)
# read by the binary's --features riak code path. The driver
# will dial 127.0.0.1:$RIAK_PBC_PORT.
if [ "$ENABLE_RIAK_PBC" = "1" ]; then
    cat >> "$CONF" <<EOF
  riak:
    pbc_listen: 0.0.0.0:$RIAK_PBC_PORT
EOF
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

# Wait for dynomited's stats endpoint to come up so the coordinator
# can move on. Use curl when present, else /dev/tcp.
for i in $(seq 1 60); do
    if command -v curl >/dev/null 2>&1; then
        if curl -s --max-time 1 "http://127.0.0.1:$STATS_LISTEN_PORT/" 2>/dev/null | grep -q '"service"'; then
            echo "==> dynomited up on $DC_NAME"
            exit 0
        fi
    else
        if printf 'GET / HTTP/1.0\r\n\r\n' | timeout 1 bash -c "exec 9<>/dev/tcp/127.0.0.1/$STATS_LISTEN_PORT && cat >&9 && cat <&9" 2>/dev/null | grep -q '"service"'; then
            echo "==> dynomited up on $DC_NAME"
            exit 0
        fi
    fi
    sleep 0.5
done

echo "==> dynomited never listened on stats:$STATS_LISTEN_PORT" >&2
tail -50 "$LOGS/dynomited-$DC_NAME.stderr" "$LOGS/dynomited-$DC_NAME.log" 2>/dev/null >&2 || true
exit 1
