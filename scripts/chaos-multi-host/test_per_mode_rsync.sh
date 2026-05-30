#!/usr/bin/env bash
#
# Test per-mode rsync isolation (Issue C).
#
# Asserts that pass3-all-modes.sh's post-mode rsync captures
# distinct data across modes. The strategy is to run a
# 60-second-per-mode pass3 against a single host (HOSTS_OVERRIDE
# narrows it to floki only, which is local to the coordinator),
# then verify that the redis-mode and memcache-mode log dirs
# under the post-mode-rsync subdir contain DIFFERENT data.
#
# We do NOT run riak in this smoke -- riak's bring-up requires
# more substrate -- so the assertion is only between redis and
# memcache.
#
# Skips when:
#   * dynomited release binary is missing;
#   * redis-server / memcached are not on PATH (the smoke
#     can't actually exercise either mode without them);
#   * the operator already has chaos resources allocated
#     (we refuse to clobber a real run by detecting an
#     existing /scratch/dynomite-chaos/run/dynomited.pid).
#
# Run with:
#   bash scripts/chaos-multi-host/test_per_mode_rsync.sh
#
# Exit codes:
#   0  redis and memcache log dirs contain distinct data
#   1  data is identical (regression)
#   77 environment cannot run the test (treated as skip)

set -euo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$REPO/scripts/chaos-multi-host"

# Mode duration. Short enough that the overall test runs in
# 3-4 minutes; long enough that the workload driver writes a
# meaningful number of ndjson rows. The driver flushes once
# per second so 60s gives ~60 rows per mode.
PER_MODE_DURATION="${PER_MODE_DURATION:-60}"

# Environment validation.
if [ ! -x "$REPO/target/release/dynomited" ]; then
    echo "SKIP: $REPO/target/release/dynomited missing; build with 'cargo build --release -p dynomited' first"
    exit 77
fi
if ! command -v redis-server >/dev/null 2>&1; then
    echo "SKIP: redis-server not on PATH"
    exit 77
fi
if ! command -v memcached >/dev/null 2>&1; then
    echo "SKIP: memcached not on PATH"
    exit 77
fi
if [ -f /scratch/dynomite-chaos/run/dynomited.pid ] \
        && kill -0 "$(cat /scratch/dynomite-chaos/run/dynomited.pid 2>/dev/null)" 2>/dev/null; then
    echo "SKIP: an active dynomited is running under /scratch/dynomite-chaos; refusing to interfere"
    exit 77
fi

# Constrain pass3 to the floki (local) host so we don't depend
# on remote SSH/Tailscale during the smoke. floki's logs are
# captured via cp -r in post_mode_rsync, exercising the same
# pre-next-mode capture timing.
export HOSTS_OVERRIDE="floki"
export CHAOS_DURATION_SECS="$PER_MODE_DURATION"
export POST_RSYNC_TIMEOUT_SECS="60"

# Run a stripped-down pass3 that does only redis -> memcache.
# We don't reuse pass3-all-modes.sh literally because it
# always iterates all three modes; copying its core loop
# inline is small and keeps the test focused.
TMP_LOG=$(mktemp)
trap 'rm -f "$TMP_LOG"' EXIT

echo "==> running redis-mode (duration=${PER_MODE_DURATION}s, hosts=$HOSTS_OVERRIDE)"
REDIS_RUN_ID="test-permode-redis-$(date -u +%Y%m%d-%H%M%SZ)"
REDIS_LOGS="$REPO/target/chaos-multi-host/$REDIS_RUN_ID"
mkdir -p "$REDIS_LOGS"
RUN_ID="$REDIS_RUN_ID" CHAOS_DURATION_SECS="$PER_MODE_DURATION" MODE=redis \
    bash "$SCRIPT_DIR/launch-detached.sh" "$REDIS_LOGS/launcher.log" "$REDIS_LOGS/coordinator.pid" \
    >> "$TMP_LOG" 2>&1
REDIS_PID=$(cat "$REDIS_LOGS/coordinator.pid")
echo "    coordinator pid=$REDIS_PID; waiting"
while kill -0 "$REDIS_PID" 2>/dev/null; do sleep 5; done

# Run the same post-mode rsync logic that pass3 now does. We
# can't source pass3-all-modes.sh because its top-level loop
# fires immediately; instead we extract post_mode_rsync via
# awk.
EXTRACTED=$(mktemp)
awk '/^post_mode_rsync\(\) \{/{found=1} found{print} found && /^}$/{exit}' \
    "$SCRIPT_DIR/pass3-all-modes.sh" > "$EXTRACTED"
SSH_KEY="$HOME/.ssh/id_ed25519"
SSH_BASE_OPTS=(-o IdentitiesOnly=yes -i "$SSH_KEY"
               -o ControlMaster=no -o ControlPath=none
               -o StrictHostKeyChecking=accept-new
               -o ServerAliveInterval=30)
ARNOLD_RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
NUC_RSYNC_E="ssh ${SSH_BASE_OPTS[*]} -o ProxyJump=arnold"
MEH_RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
# shellcheck disable=SC1090
. "$EXTRACTED"

post_mode_rsync "$REDIS_LOGS" redis >> "$TMP_LOG" 2>&1 || true

echo "==> running memcache-mode (duration=${PER_MODE_DURATION}s)"
MEM_RUN_ID="test-permode-memcache-$(date -u +%Y%m%d-%H%M%SZ)"
MEM_LOGS="$REPO/target/chaos-multi-host/$MEM_RUN_ID"
mkdir -p "$MEM_LOGS"
RUN_ID="$MEM_RUN_ID" CHAOS_DURATION_SECS="$PER_MODE_DURATION" MODE=memcache \
    bash "$SCRIPT_DIR/launch-detached.sh" "$MEM_LOGS/launcher.log" "$MEM_LOGS/coordinator.pid" \
    >> "$TMP_LOG" 2>&1
MEM_PID=$(cat "$MEM_LOGS/coordinator.pid")
echo "    coordinator pid=$MEM_PID; waiting"
while kill -0 "$MEM_PID" 2>/dev/null; do sleep 5; done
post_mode_rsync "$MEM_LOGS" memcache >> "$TMP_LOG" 2>&1 || true

# Assertion: the floki workload ndjsons in the two post-mode
# rsync subdirs must differ. Identical bytes mean the second
# mode's start-host wiped the first mode's data on the remote
# before our rsync ran -- the original Pass-7 symptom.
REDIS_NDJSON="$REDIS_LOGS/post-mode-rsync/floki-logs/workload-dc-floki.ndjson"
MEM_NDJSON="$MEM_LOGS/post-mode-rsync/floki-logs/workload-dc-floki.ndjson"

if [ ! -s "$REDIS_NDJSON" ]; then
    echo "FAIL: redis ndjson missing or empty: $REDIS_NDJSON"
    echo "      (full launcher log at $TMP_LOG)"
    exit 1
fi
if [ ! -s "$MEM_NDJSON" ]; then
    echo "FAIL: memcache ndjson missing or empty: $MEM_NDJSON"
    echo "      (full launcher log at $TMP_LOG)"
    exit 1
fi

# Mode-tagged rows are the cleanest signal. The driver
# embeds the mode name in each emitted row's "mode" field.
redis_has_redis=$(grep -c '"mode":"redis"' "$REDIS_NDJSON" || true)
mem_has_memcache=$(grep -c '"mode":"memcache"' "$MEM_NDJSON" || true)

if [ "${redis_has_redis:-0}" -lt 1 ]; then
    echo "FAIL: redis ndjson contains no redis-mode rows ($REDIS_NDJSON)"
    exit 1
fi
if [ "${mem_has_memcache:-0}" -lt 1 ]; then
    echo "FAIL: memcache ndjson contains no memcache-mode rows ($MEM_NDJSON)"
    exit 1
fi

# Bytes-equal would be a strong negative signal even before
# we look at the mode column; assert it explicitly.
if cmp -s "$REDIS_NDJSON" "$MEM_NDJSON"; then
    echo "FAIL: redis and memcache ndjsons are byte-identical"
    echo "      $REDIS_NDJSON"
    echo "      $MEM_NDJSON"
    exit 1
fi

# And the negative cross-check: the redis ndjson should NOT
# contain memcache-mode rows (and vice versa), because each
# mode truncates its own ndjson on start-up.
redis_has_memcache=$(grep -c '"mode":"memcache"' "$REDIS_NDJSON" || true)
mem_has_redis=$(grep -c '"mode":"redis"' "$MEM_NDJSON" || true)
if [ "${redis_has_memcache:-0}" -gt 0 ]; then
    echo "FAIL: redis ndjson is contaminated with memcache-mode rows"
    exit 1
fi
if [ "${mem_has_redis:-0}" -gt 0 ]; then
    echo "FAIL: memcache ndjson is contaminated with redis-mode rows"
    exit 1
fi

echo "PASS: redis and memcache ndjsons contain distinct, mode-correct data"
echo "      redis: $redis_has_redis redis-mode rows"
echo "      memcache: $mem_has_memcache memcache-mode rows"
exit 0
