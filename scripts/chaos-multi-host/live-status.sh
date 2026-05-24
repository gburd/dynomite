#!/usr/bin/env bash
#
# Pull a live snapshot of the running multi-host chaos test:
#   - elapsed / remaining time
#   - per-host process counts
#   - per-host workload throughput (last 30s window)
#   - per-host injector events so far
#   - any ERROR / panic lines in dynomited logs
#
# Usage: ./live-status.sh [RUN_ID]

set -euo pipefail

RUN_ID="${1:-prod-20260522-010136Z}"
REPO="/home/gburd/ws/dynomite"
LOCAL="$REPO/target/chaos-multi-host/$RUN_ID"

SSH_KEY="$HOME/.ssh/id_ed25519"
SSH_BASE_OPTS=(-o IdentitiesOnly=yes -i "$SSH_KEY"
               -o ControlMaster=no -o ControlPath=none
               -o StrictHostKeyChecking=accept-new
               -o ServerAliveInterval=30
               -o ConnectTimeout=10)

ARNOLD_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" arnold)
NUC_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" -o ProxyJump=arnold gburd@nuc)
MEH_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" meh)

printf '%s\n' "================================================================"
printf '  multi-host chaos status @ %s\n' "$(date -u +%H:%M:%SZ)"
printf '%s\n' "================================================================"
printf '  run id: %s\n' "$RUN_ID"

# Coordinator alive?
COORD_PID=$(cat /tmp/chaos-prod.pid 2>/dev/null || echo "")
if [ -n "$COORD_PID" ] && kill -0 "$COORD_PID" 2>/dev/null; then
    printf '  coordinator: alive (pid %s)\n' "$COORD_PID"
else
    printf '  coordinator: NOT RUNNING\n'
fi

# Time budget
if [ -f "$LOCAL/coordinator.log" ]; then
    started=$(stat -c %Y "$LOCAL/coordinator.log")
    now=$(date +%s)
    elapsed=$((now - started))
    remaining=$((7200 - elapsed))
    printf '  elapsed: %d s (%d min)   remaining: %d s (%d min)\n' \
        "$elapsed" "$((elapsed/60))" "$remaining" "$((remaining/60))"
fi

snapshot_host() {
    local label="$1"; shift
    local runner=("$@")
    printf '\n--- %s ---\n' "$label"
    "${runner[@]}" '
        # Process count + names
        ps_count=$(pgrep -af "dynomited|workload-driver.py|chaos-injector.sh" 2>/dev/null | grep -v "bash -c" | grep -v "pgrep" | wc -l)
        echo "  processes: $ps_count"
        # Latest workload window
        if [ -f /scratch/dynomite-chaos/logs/workload-'"$label"'.ndjson ]; then
            python3 -c "
import json, sys
last = None
with open(\"/scratch/dynomite-chaos/logs/workload-'"$label"'.ndjson\") as f:
    for line in f:
        if line.strip(): last = line
if last:
    d = json.loads(last)
    counts = d.get(\"counts\", {})
    fails = d.get(\"failures\", {})
    elapsed = d.get(\"elapsed\", 0)
    total_ok = sum(counts.values())
    total_fail = sum(fails.values())
    print(f\"  workload: window @ {elapsed:.0f}s, ok={total_ok} fail={total_fail}\")
    if fails:
        print(f\"  failures: {dict(fails)}\")
" 2>&1
        else
            echo "  workload: no ndjson yet"
        fi
        # Injector events so far
        if [ -f /scratch/dynomite-chaos/logs/chaos-events-'"$label"'.ndjson ]; then
            python3 -c "
import json, collections, sys
kinds = collections.Counter()
with open(\"/scratch/dynomite-chaos/logs/chaos-events-'"$label"'.ndjson\") as f:
    for line in f:
        if line.strip():
            d = json.loads(line)
            kinds[d[\"kind\"]] += 1
print(\"  injector events:\", dict(kinds))
" 2>&1
        else
            echo "  injector events: no log yet"
        fi
        # ERROR / WARN in dynomited log (excluding the standard deferred warnings)
        if [ -f /scratch/dynomite-chaos/logs/dynomited-'"$label"'.log ]; then
            errs=$(grep -c "ERROR" /scratch/dynomite-chaos/logs/dynomited-'"$label"'.log 2>/dev/null | head -1); errs=${errs:-0}
            warns=$(grep -c "WARN" /scratch/dynomite-chaos/logs/dynomited-'"$label"'.log 2>/dev/null | head -1); warns=${warns:-0}
            echo "  dynomited log: ERROR=$errs WARN=$warns"
            if [ "$errs" -gt "0" ]; then
                echo "  --- recent ERRORs ---"
                grep "ERROR" /scratch/dynomite-chaos/logs/dynomited-'"$label"'.log 2>/dev/null | tail -3
            fi
        fi
    '
}

# Floki snapshot inline
printf '\n--- dc-floki ---\n'
ps_count=$(pgrep -af "dynomited|workload-driver|chaos-injector" 2>/dev/null | grep -v "bash -c\|pgrep" | wc -l)
printf '  processes: %d\n' "$ps_count"

if [ -f /scratch/dynomite-chaos/logs/workload-dc-floki.ndjson ]; then
    python3 -c "
import json
last = None
with open('/scratch/dynomite-chaos/logs/workload-dc-floki.ndjson') as f:
    for line in f:
        if line.strip(): last = line
if last:
    d = json.loads(last)
    counts = d.get('counts', {})
    fails = d.get('failures', {})
    print(f'  workload: window @ {d.get(\"elapsed\",0):.0f}s, ok={sum(counts.values())} fail={sum(fails.values())}')
    if fails: print(f'  failures: {dict(fails)}')
"
fi

if [ -f /scratch/dynomite-chaos/logs/chaos-events-dc-floki.ndjson ]; then
    python3 -c "
import json, collections
kinds = collections.Counter()
with open('/scratch/dynomite-chaos/logs/chaos-events-dc-floki.ndjson') as f:
    for line in f:
        if line.strip():
            d = json.loads(line)
            kinds[d['kind']] += 1
print('  injector events:', dict(kinds))
"
fi

snapshot_host dc-arnold "${ARNOLD_SSH[@]}"
snapshot_host dc-nuc "${NUC_SSH[@]}"
snapshot_host dc-meh "${MEH_SSH[@]}"

printf '\n%s\n' "================================================================"
