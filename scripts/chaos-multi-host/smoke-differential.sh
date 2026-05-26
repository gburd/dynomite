#!/usr/bin/env bash
#
# Single-host smoke for the differential substrate (P3-3.9
# phase 1+2). Boots a Rust dynomited and a C dynomite on the
# lead host (floki), backed by the same redis, and asserts both
# proxy ports answer a SET / GET round-trip.
#
# Run from the repo root inside `nix develop`:
#
#   bash scripts/chaos-multi-host/smoke-differential.sh
#
# Expected runtime: ~90 seconds. Not exercised by CI; the
# operator runs it before merging differential-substrate
# changes. Requires `_/dynomite` and the autotools toolchain.

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
RUN_ID="diff-smoke-$(date -u +%Y%m%d-%H%M%SZ)"
LOG_DIR="$REPO/target/chaos-multi-host/$RUN_ID"
mkdir -p "$LOG_DIR"

# Ports default to the standard chaos rig values. Override via
# env when an operator already has a live chaos rig holding
# them (e.g. running this smoke from a worktree alongside a
# production coordinator run).
DATASTORE_PORT="${DATASTORE_PORT:-17100}"
DYN_LISTEN_PORT="${DYN_LISTEN_PORT:-18101}"
CLIENT_LISTEN_PORT="${CLIENT_LISTEN_PORT:-18102}"
STATS_LISTEN_PORT="${STATS_LISTEN_PORT:-22222}"
RIAK_PBC_PORT="${RIAK_PBC_PORT:-21800}"
C_CLIENT_PORT=$((CLIENT_LISTEN_PORT + 100))
echo "==> smoke-differential run=$RUN_ID logs=$LOG_DIR"
echo "  Rust client=$CLIENT_LISTEN_PORT  C client=$C_CLIENT_PORT"

cleanup() {
    rc=${1:-1}
    for f in /scratch/dynomite-chaos/run/dynomited.pid /scratch/dynomite-chaos/run/dynomite-c.pid; do
        [ -f "$f" ] && kill -KILL "$(cat "$f")" 2>/dev/null || true
    done
    [ -f /scratch/dynomite-chaos/run/redis.pid ] \
        && kill -KILL "$(cat /scratch/dynomite-chaos/run/redis.pid)" 2>/dev/null || true
    exit "$rc"
}
trap 'cleanup 1' INT TERM ERR

echo "==> step 1: build C dynomite (idempotent)"
bash "$REPO/scripts/chaos-multi-host/build_cref_remote.sh" floki \
    > "$LOG_DIR/build_cref.log" 2>&1 \
    || { echo "FAIL: build_cref_remote.sh; see $LOG_DIR/build_cref.log"; cleanup 1; }

echo "==> step 2: bring up Rust + C clusters"
MODE=differential bash "$REPO/scripts/chaos-multi-host/start-host.sh" \
    dc-floki "0" "" "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" "$RIAK_PBC_PORT" \
    > "$LOG_DIR/start-host.log" 2>&1 \
    || { echo "FAIL: start-host.sh; see $LOG_DIR/start-host.log"; \
         tail -40 "$LOG_DIR/start-host.log" || true; cleanup 1; }

probe_redis() {
    local port="$1" label="$2" key="diff-smoke-$2-$$" val="ok-$2-$RANDOM"
    local set_resp get_resp
    # SET response is exactly 5 bytes: "+OK\r\n".
    # shellcheck disable=SC2016
    set_resp=$(printf '*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' \
        "${#key}" "$key" "${#val}" "$val" \
        | timeout 5 bash -c "exec 9<>/dev/tcp/127.0.0.1/$port; cat >&9; head -c 5 <&9" 2>/dev/null || true)
    [[ "$set_resp" == *"+OK"* ]] || { echo "  $label SET($port) bad: $(printf %q "$set_resp")"; return 1; }
    # GET response shape: "$<len>\r\n<value>\r\n". Compute the
    # exact byte count so head -c does not block waiting for
    # data the proxy will not send.
    local vlen="${#val}"
    local rlen=$(( ${#vlen} + 2 + vlen + 2 + 1 ))
    # shellcheck disable=SC2016
    get_resp=$(printf '*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n' "${#key}" "$key" \
        | timeout 5 bash -c "exec 9<>/dev/tcp/127.0.0.1/$port; cat >&9; head -c $rlen <&9" 2>/dev/null || true)
    [[ "$get_resp" == *"$val"* ]] || { echo "  $label GET($port) bad: $(printf %q "$get_resp")"; return 1; }
    echo "  OK: $label port $port SET/GET"
    return 0
}

echo "==> step 3: probe both proxy ports (60s window)"
deadline=$(( $(date +%s) + 60 ))
fail=1
while [ "$(date +%s)" -lt "$deadline" ]; do
    if probe_redis "$CLIENT_LISTEN_PORT" rust && probe_redis "$C_CLIENT_PORT" c; then
        fail=0; break
    fi
    sleep 5
done

if [ "$fail" -ne 0 ]; then
    echo "FAIL: differential smoke could not get both ports answering"
    tail -20 /scratch/dynomite-chaos/logs/dynomited-dc-floki.log 2>/dev/null || true
    tail -20 /scratch/dynomite-chaos/logs/dynomite-c-dc-floki.log 2>/dev/null || true
    cleanup 1
fi

echo "==> smoke PASSED"
trap - INT TERM ERR
cleanup 0
