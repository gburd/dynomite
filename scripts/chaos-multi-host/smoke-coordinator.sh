#!/usr/bin/env bash
#
# Operator-only smoke test for the multi-host chaos coordinator.
#
# Runs a 30-second mock chaos cycle in redis mode across all
# four hosts (floki, arnold, nuc, meh) and asserts that every
# host produced a workload.ndjson with at least 100 ops.
#
# This is the post-refactor sanity check that the bash-stdin
# runner pattern in coordinator.sh works against meh's fish
# login shell. CI does NOT run this; the chaos hosts are not
# available in CI. Run manually from floki:
#
#   bash scripts/chaos-multi-host/smoke-coordinator.sh
#
# Or to exercise just one host (useful while iterating on the
# runner pattern):
#
#   HOSTS_OVERRIDE=meh bash scripts/chaos-multi-host/smoke-coordinator.sh
#
# Cleanup: the coordinator's teardown trap kills workload +
# injector + dynomited + redis on every enabled host. This
# script does not leave residue beyond the run-id log
# directory under target/chaos-multi-host/.

set -euo pipefail

REPO="/home/gburd/ws/dynomite"
RUN_ID="${RUN_ID:-smoke-$(date -u +%Y%m%d-%H%M%SZ)}"
HOSTS_OVERRIDE="${HOSTS_OVERRIDE:-floki,arnold,nuc,meh}"
CHAOS_DURATION_SECS="${CHAOS_DURATION_SECS:-30}"
MODE="${MODE:-redis}"
MIN_OPS="${MIN_OPS:-100}"

export RUN_ID HOSTS_OVERRIDE CHAOS_DURATION_SECS MODE

LOGS="$REPO/target/chaos-multi-host/$RUN_ID"

echo "==> smoke-coordinator"
echo "  run id:    $RUN_ID"
echo "  hosts:     $HOSTS_OVERRIDE"
echo "  duration:  ${CHAOS_DURATION_SECS}s"
echo "  mode:      $MODE"
echo "  min ops:   $MIN_OPS"
echo "  logs:      $LOGS"

bash "$REPO/scripts/chaos-multi-host/coordinator.sh"

echo "==> coordinator done; verifying workload outputs"

fail=0
IFS=',' read -ra HOSTS <<< "$HOSTS_OVERRIDE"
for h in "${HOSTS[@]}"; do
    case "$h" in
        floki)  ndjson="$LOGS/floki-logs/workload-dc-floki.ndjson"   ;;
        arnold) ndjson="$LOGS/arnold-logs/workload-dc-arnold.ndjson" ;;
        nuc)    ndjson="$LOGS/nuc-logs/workload-dc-nuc.ndjson"       ;;
        meh)    ndjson="$LOGS/meh-logs/workload-dc-meh.ndjson"       ;;
        *)
            echo "  FAIL: unknown host '$h' in HOSTS_OVERRIDE"
            fail=1
            continue
            ;;
    esac
    if [ ! -s "$ndjson" ]; then
        echo "  FAIL: $h workload.ndjson missing or empty: $ndjson"
        fail=1
        continue
    fi
    n=$(wc -l < "$ndjson" | tr -d ' ')
    if [ "$n" -lt "$MIN_OPS" ]; then
        echo "  FAIL: $h workload.ndjson has $n ops (< $MIN_OPS): $ndjson"
        fail=1
    else
        echo "  OK:   $h $n ops"
    fi
done

if [ "$fail" -eq 1 ]; then
    echo "==> smoke FAILED (logs under $LOGS)"
    exit 1
fi

echo "==> smoke PASSED"
