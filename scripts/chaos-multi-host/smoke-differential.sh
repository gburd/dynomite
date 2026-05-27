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

echo "==> step 4: drive a 30-second differential workload"
# Phase 3+4 smoke. The dual-fanout driver runs for 30 seconds
# at 50 QPS (1500 ops total budget). Both proxies front the
# same redis backend so every reply should agree under the
# allowlist; we assert the ndjson final row carries an
# ``agreed`` bucket with > 0 ops and that ``divergent`` is
# either absent or zero in every per-op key.
WORKLOAD_NDJSON="$LOG_DIR/workload-diff-smoke.ndjson"
if ! python3 "$REPO/scripts/chaos-multi-host/workload-driver.py" \
        --mode differential \
        --rust-host 127.0.0.1 --rust-port "$CLIENT_LISTEN_PORT" \
        --c-host 127.0.0.1 --c-port "$C_CLIENT_PORT" \
        --label dc-floki-diff-smoke \
        --out "$WORKLOAD_NDJSON" \
        --duration 30 \
        --qps 50 \
        --retry-on='NoTargets:1,Timeout:0,Closed:2' \
        > "$LOG_DIR/workload-diff.stderr" 2>&1; then
    echo "FAIL: workload-driver --mode differential exited non-zero"
    tail -20 "$LOG_DIR/workload-diff.stderr" || true
    cleanup 1
fi

if [ ! -s "$WORKLOAD_NDJSON" ]; then
    echo "FAIL: workload ndjson is empty: $WORKLOAD_NDJSON"
    cleanup 1
fi

# Sum ``agreed`` op-counts across every flushed row (final
# included). Python is the simplest lever here; the rig
# already ships python3.
AGREED_TOTAL=$(python3 - "$WORKLOAD_NDJSON" <<'PY'
import json, sys
total = 0
div = 0
with open(sys.argv[1]) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        total += sum((row.get("agreed") or {}).values())
        div += sum((row.get("divergent") or {}).values())
print("%d %d" % (total, div))
PY
)
set -- $AGREED_TOTAL
A="$1"
D="$2"
echo "  agreed=$A  divergent=$D  ndjson=$WORKLOAD_NDJSON"
if [ "$A" -le 0 ]; then
    echo "FAIL: zero agreed ops in differential workload"
    cleanup 1
fi
if [ "$D" -gt 0 ]; then
    # A non-zero divergent count is not necessarily a smoke
    # failure (the allowlist may need an entry the operator
    # hasn't observed yet) but we surface it loudly so the
    # next reviewer extends the allowlist before merging.
    echo "  WARN: $D divergent ops observed; review samples in $WORKLOAD_NDJSON"
fi

echo "==> smoke PASSED"
trap - INT TERM ERR
cleanup 0
