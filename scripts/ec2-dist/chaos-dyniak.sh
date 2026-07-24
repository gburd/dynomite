#!/usr/bin/env bash
# Multi-region adversarial chaos test for Dyniak (data_store: dyniak).
#
# Provisions a small multi-region Dyniak cluster on local NVMe (never
# EBS, never /tmp, never ramfs -- noxu data lands on the instance-store
# device mounted at /mnt/data), plus a separate small load-generator
# instance per region. The load fleet drives verifiable CRDT traffic
# (counter increments + set adds) at the cluster while a fault injector
# induces net splits and node churn (nodes coming and going, ring
# adjustments). At the end a coordinator reconstructs the expected value
# per key from the recorded op history and confirms every surviving
# replica converged to it.
#
# Invariants proven:
#   * Always-available: single-key CRDT updates are accepted throughout
#     the run, including during partitions and ring changes (measured as
#     the fraction of accepted DtUpdate ops per generator).
#   * Steady p99: the per-generator p99 latency stays bounded through
#     the fault window.
#   * Convergence: after quiescence every counter equals the number of
#     increments routed to it, and every set equals the union of adds --
#     eventual consistency via CRDT merge, no lost update.
#
# Usage (detached, resumable):
#   RUN_ID=chaos-$(date +%Y%m%d-%H%M%S) SRC_DIR=/home/gburd/ws/dynomite \
#     setsid bash chaos-dyniak.sh > /tmp/$RUN_ID.orch 2>&1 < /dev/null &
#
# Phases (checkpointed in $STATE_DIR/phase, resumable on re-run with the
# same RUN_ID): provision build distribute mount launch load teardown.
set -uo pipefail

PROFILE="${PROFILE:-numa}"
RUN_ID="${RUN_ID:?set RUN_ID}"
SRC_DIR="${SRC_DIR:-/home/gburd/ws/dynomite}"
TAG=dyn-run
KEEP_ON_FAIL="${KEEP_ON_FAIL:-0}"

STATE_DIR="/tmp/${RUN_ID}"
STATE="/tmp/${RUN_ID}.state.ips"       # dyniak nodes
LOADSTATE="/tmp/${RUN_ID}.load.ips"    # load-gen instances
RESULTS="${STATE_DIR}/results"
mkdir -p "$STATE_DIR" "$RESULTS"

# --- topology: 3 regions x 2 dyniak nodes + 1 load-gen per region -----
# Small, single-DC-per-region cluster: each region is its own DC, each
# node its own rack (a full replica of the ring), so n_val = nodes/DC.
# Two nodes per region keeps the ring small and the fault blast radius
# meaningful (dropping one node still leaves a replica).
REGIONS=(us-east-1 us-west-2 eu-central-1)
declare -A DC=( [us-east-1]=dc-use1 [us-west-2]=dc-usw2 [eu-central-1]=dc-euc1 )
declare -A AZ=( [us-east-1]=us-east-1a [us-west-2]=us-west-2a [eu-central-1]=eu-central-1a )
# NVMe-bearing instance for dyniak nodes (m6id = local NVMe instance
# store). t3 load-gens have no local NVMe but never store data.
DYNIAK_ITYPE=m6id.xlarge
LOADGEN_ITYPE=t3.medium
declare -A AMI=(
  [us-east-1]=ami-0de568ccf3b0080d9 [us-west-2]=ami-05f8addf6121a3888
  [eu-central-1]=ami-0ae2eb6210612f5a0
)
NODES_PER_REGION="${NODES_PER_REGION:-2}"

# Ports: dyniak PBC 8087, HTTP 8098, dnode 8101, stats 22222.
KEY="/tmp/${RUN_ID}.pem"

aws() { command aws --profile "$PROFILE" "$@"; }
log() { echo "[chaos $(date -u +%H:%M:%S)] $*" | tee -a "$STATE_DIR/orchestrator.log" >&2; }
MY_IP="$(curl -s -m 10 https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]')"

phase_done()   { echo "$1" >> "$STATE_DIR/phase"; }
phase_is_done() { grep -qx "$1" "$STATE_DIR/phase" 2>/dev/null; }

# ssh/scp helpers (stdin from /dev/null inside read loops; own key).
nsh()  { local ip=$1; shift; SSH_AUTH_SOCK="" ssh -n -i "$KEY" -o StrictHostKeyChecking=no \
           -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=15 ec2-user@"$ip" "$@"; }
nscp() { local src=$1 ip=$2 dst=$3; SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no \
           -o IdentitiesOnly=yes -o IdentityAgent=none "$src" ec2-user@"$ip":"$dst" >/dev/null 2>&1; }

source "$(dirname "$0")/chaos-dyniak-phases.sh"

main() {
  log "chaos run $RUN_ID starting (regions: ${REGIONS[*]}, ${NODES_PER_REGION}/region + load-gens)"
  for ph in provision build distribute mount launch load teardown; do
    if phase_is_done "$ph"; then log "skip $ph (done)"; continue; fi
    log "=== PHASE: $ph ==="
    if "phase_$ph"; then
      phase_done "$ph"
    else
      log "PHASE FAILED: $ph"
      if [ "$KEEP_ON_FAIL" = "1" ] && [ "$ph" != "teardown" ]; then
        log "KEEP_ON_FAIL=1, leaving resources up for inspection"
        return 1
      fi
      # Always attempt teardown on failure to avoid orphaned cost.
      if [ "$ph" != "teardown" ]; then
        log "attempting teardown after failure"
        phase_teardown || log "teardown after failure had errors"
      fi
      return 1
    fi
  done
  log "chaos run $RUN_ID COMPLETE"
}

main
