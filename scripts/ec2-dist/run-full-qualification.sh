#!/usr/bin/env bash
# Unattended full-qualification orchestrator.
#
# Runs the entire multi-region, mixed-architecture qualification to
# completion without operator interaction, then tears everything down.
# Intended to run detached on a controller/bastion (setsid ... &) so a
# disconnect does not interrupt it. Every phase checkpoints to
# $STATE_DIR/phase so a re-invocation resumes from the last completed
# phase.
#
# Pipeline:
#   1. provision   -- 5 regions x 9 nodes (3 racks x 3); 3 Intel(x86)
#                     regions + 2 Graviton(arm) regions; local NVMe.
#   2. build       -- Rust dynomited + C dynomite for BOTH archs on one
#                     node of each arch.
#   3. distribute  -- push the right-arch binaries + driver to all 45.
#   4. mount        -- mount the NVMe instance store, point noxu at it.
#   5. matrix       -- C-vs-Rust differential across entry nodes x
#                     consistency levels (DC_ONE, DC_QUORUM), both archs.
#   6. migrate      -- under a constant 60/40 read/write load, drain the
#                     3 Intel regions and grow to 3 Graviton regions
#                     (add a 3rd Graviton region, hand off, remove Intel).
#   7. jepsen       -- run the Jepsen consistency suite against the final
#                     3-region Graviton cluster.
#   8. teardown     -- terminate all instances, delete SGs + keypairs.
#
# Progress + results stream to $STATE_DIR/orchestrator.log and per-phase
# result files. On any phase failure the orchestrator records the
# failure, still runs teardown (unless KEEP_ON_FAIL=1), and exits nonzero.
#
# Usage (on the controller):
#   RUN_ID=dyn-qual-$(date -u +%Y%m%d-%H%M%S) \
#     nohup bash run-full-qualification.sh > /tmp/$RUN_ID.orch 2>&1 &
#
# Requires: aws cli (profile numa), the ec2-dist scripts alongside this
# one, the dynomite source tree at $SRC_DIR (default: this repo).
set -uo pipefail

# ----- configuration -----
PROFILE="${PROFILE:-numa}"
RUN_ID="${RUN_ID:-dyn-qual-$(date -u +%Y%m%d-%H%M%S)}"
SRC_DIR="${SRC_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
HERE="$(cd "$(dirname "$0")" && pwd)"
STATE_DIR="${STATE_DIR:-/tmp/${RUN_ID}}"
KEEP_ON_FAIL="${KEEP_ON_FAIL:-0}"
TAG="dyn-run"

# SSH identity used to reach nodes (per-region keys live in STATE_DIR).
SSH_OPTS="-o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=20 -o ServerAliveInterval=15"

mkdir -p "$STATE_DIR"
echo "RUN_ID=$RUN_ID" > /tmp/dyn-ec2-runid
mkdir -p "$STATE_DIR"
echo "RUN_ID=$RUN_ID" > /tmp/dyn-ec2-runid
# deploy-mixed.sh owns the canonical paths: state at
# /tmp/<RUN_ID>.state.ips and per-region keys at
# /tmp/<RUN_ID>-<region>.pem. The orchestrator reads those directly so
# the provisioner and the phases agree on locations.
STATE="/tmp/${RUN_ID}.state.ips"
PHASE_FILE="$STATE_DIR/phase"
LOG="$STATE_DIR/orchestrator.log"

log() { echo "[orch $(date -u +%FT%TZ)] $*" | tee -a "$LOG" >&2; }
phase_done() { echo "$1" >> "$PHASE_FILE"; log "PHASE COMPLETE: $1"; }
phase_is_done() { [ -f "$PHASE_FILE" ] && grep -qx "$1" "$PHASE_FILE"; }

aws() { command aws --profile "$PROFILE" "$@"; }

# node helpers: read the 10-field state rows.
# region az dc rack node arch itype iid pub priv
node_key() { echo "/tmp/${RUN_ID}-${1}.pem"; }
nsh() { # nsh <region> <pub> <cmd...>
  local region=$1 pub=$2; shift 2
  # `-n` (stdin from /dev/null) is essential: without it ssh reads from
  # the enclosing `while read ... < $STATE` loop's stdin and drains the
  # state file, so the loop terminates after the first batch.
  SSH_AUTH_SOCK="" ssh -n -i "$(node_key "$region")" $SSH_OPTS "ec2-user@$pub" "$@"
}
nscp() { # nscp <region> <src> <pub> <dst>
  local region=$1 src=$2 pub=$3 dst=$4
  SSH_AUTH_SOCK="" scp -i "$(node_key "$region")" $SSH_OPTS "$src" "ec2-user@$pub:$dst" >/dev/null 2>&1 < /dev/null
}

# ----- topology -----
REGIONS_X86=(us-east-1 us-west-2 eu-central-1)   # Intel "old"
REGIONS_ARM=(ap-northeast-1 sa-east-1)           # Graviton "new"
# The Graviton region added during migration (the 3rd arm region).
MIGRATE_TARGET_REGION=eu-west-1
ALL_REGIONS=("${REGIONS_X86[@]}" "${REGIONS_ARM[@]}")

# ports
C_CLIENT=8102; C_DNODE=8101; C_STATS=22222
R_CLIENT=9102; R_DNODE=9101; R_STATS=22223

log "orchestrator start: RUN_ID=$RUN_ID SRC_DIR=$SRC_DIR STATE_DIR=$STATE_DIR"
log "topology: 5 regions x 9 (3 racks x 3); x86=${REGIONS_X86[*]} arm=${REGIONS_ARM[*]}"

# Source the phase implementations.
# shellcheck source=/dev/null
. "$HERE/qual-phases.sh"

# ----- drive the pipeline -----
run_phase() {
  local name=$1 fn=$2
  if phase_is_done "$name"; then log "skip $name (already done)"; return 0; fi
  log "=== PHASE: $name ==="
  if "$fn"; then phase_done "$name"; return 0; fi
  log "PHASE FAILED: $name"; return 1
}

FAILED=""
for spec in \
  "provision:phase_provision" \
  "build:phase_build" \
  "distribute:phase_distribute" \
  "mount:phase_mount" \
  "matrix:phase_matrix" \
  "migrate:phase_migrate" \
  "jepsen:phase_jepsen"; do
  name="${spec%%:*}"; fn="${spec##*:}"
  if ! run_phase "$name" "$fn"; then FAILED="$name"; break; fi
done

if [ -n "$FAILED" ] && [ "$KEEP_ON_FAIL" = "1" ]; then
  log "phase $FAILED failed; KEEP_ON_FAIL=1, leaving cluster up for inspection"
  exit 1
fi

run_phase "teardown" phase_teardown || log "teardown had issues; check manually"

if [ -n "$FAILED" ]; then
  log "QUALIFICATION FAILED at phase: $FAILED"
  exit 1
fi
log "QUALIFICATION COMPLETE -- all phases passed, cluster torn down"
log "results: $STATE_DIR/results/"
exit 0
