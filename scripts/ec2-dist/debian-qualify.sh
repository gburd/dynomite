#!/usr/bin/env bash
# One-shot Debian EC2 qualification: launch a Debian 12 instance in the
# hotdog account, ship this tree's committed HEAD, run `cargo update`,
# then build + lint + test the workspace on a clean non-Nix host, pull
# the updated Cargo.lock back, and tear the instance down.
#
# The RELEASE (tag + crates.io publish) is deliberately NOT done here:
# the signing key and registry token live on the controller. This
# script produces the qualified Cargo.lock; the lead cuts the release
# locally.
#
# Usage:
#   RUN_ID=deb-$(date -u +%Y%m%d-%H%M%S) SRC_DIR=/home/gburd/ws/dynomite \
#     scripts/ec2-dist/debian-qualify.sh up
#   scripts/ec2-dist/debian-qualify.sh qualify   # cargo update + build + test
#   scripts/ec2-dist/debian-qualify.sh fetch-lock
#   scripts/ec2-dist/debian-qualify.sh down
set -uo pipefail

PROFILE="${PROFILE:-hotdog}"
RUN_ID="${RUN_ID:?set RUN_ID}"
SRC_DIR="${SRC_DIR:-/home/gburd/ws/dynomite}"
TAG=dyn-run
REGION="${REGION:-us-east-1}"
ITYPE="${ITYPE:-m6i.2xlarge}"           # 8 vCPU, 32 GiB -- comfortable build
AMI="${AMI:-ami-089045d4bb5fcc5d6}"     # us-east-1 Debian 12 amd64 (2026-09-23)
SSH_USER="${SSH_USER:-admin}"           # Debian AMIs log in as admin

STATE_DIR="/tmp/${RUN_ID}"
KEY="/tmp/${RUN_ID}.pem"
mkdir -p "$STATE_DIR"

aws() { command aws --profile "$PROFILE" "$@"; }
log() { echo "[debian-qual $(date -u +%H:%M:%S)] $*" >&2; }
MY_IP="$(curl -s -m 10 https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]')"

nsh()  { local ip=$1; shift; SSH_AUTH_SOCK="" ssh -n -i "$KEY" -o StrictHostKeyChecking=no \
           -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=15 "$SSH_USER"@"$ip" "$@"; }
nscp() { local src=$1 ip=$2 dst=$3; SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no \
           -o IdentitiesOnly=yes -o IdentityAgent=none "$src" "$SSH_USER"@"$ip":"$dst" >/dev/null 2>&1; }

phase_up() {
  [ -z "$MY_IP" ] && { log "could not determine controller public IP"; return 1; }
  [ -f "$KEY" ] || ssh-keygen -t ed25519 -N "" -f "$KEY" -q
  aws ec2 import-key-pair --region "$REGION" --key-name "${RUN_ID}-key" \
    --public-key-material "fileb://${KEY}.pub" \
    --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN_ID}]" >/dev/null 2>&1

  local vpc sg
  vpc=$(aws ec2 describe-vpcs --region "$REGION" --query 'Vpcs[?IsDefault==`true`].VpcId' --output text)
  sg=$(aws ec2 create-security-group --region "$REGION" --group-name "${RUN_ID}-sg" \
    --description "debian qualify $RUN_ID" --vpc-id "$vpc" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
    --query 'GroupId' --output text)
  # SSH only, from the controller /32.
  aws ec2 authorize-security-group-ingress --region "$REGION" \
    --group-id "$sg" --protocol tcp --port 22 --cidr "${MY_IP}/32" >/dev/null 2>&1
  echo "$sg" > "$STATE_DIR/sg"
  log "sg=$sg vpc=$vpc"

  # A 120 GiB gp3 root volume: the workspace all-features all-targets
  # target dir plus the incremental cache is large; the Debian AMI's
  # default root is small and a 60 GiB volume filled mid-build.
  local iid
  iid=$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" \
    --instance-type "$ITYPE" --count 1 --key-name "${RUN_ID}-key" --security-group-ids "$sg" \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=120,VolumeType=gp3}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${RUN_ID}}]" \
    --query 'Instances[0].InstanceId' --output text)
  echo "$iid" > "$STATE_DIR/iid"
  aws ec2 wait instance-running --region "$REGION" --instance-ids "$iid"
  local pub
  pub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  echo "$pub" > "$STATE_DIR/pub"
  log "instance: $iid $pub"

  local i
  for i in $(seq 1 20); do
    nsh "$pub" 'echo ok' 2>/dev/null | grep -q ok && { log "ssh ready"; break; }
    sleep 6
  done
  nsh "$pub" 'echo ok' 2>/dev/null | grep -q ok || { log "ssh never came up"; return 1; }

  # Toolchain + build deps. The flake pins rust 1.83 via
  # rust-toolchain.toml; rustup on the node honours it. quiche needs
  # cmake + clang; dynvec links openblas.
  log "installing toolchain + build deps (apt + rustup)"
  nsh "$pub" '
    set -e
    sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
      build-essential cmake clang perl pkg-config libssl-dev \
      libopenblas-dev git curl ca-certificates >/dev/null
    command -v cargo >/dev/null 2>&1 || curl -sSf https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1
  ' || { log "apt/rustup install failed"; return 1; }

  # Ship the committed tree (git ls-files -> tar), unpack on the node.
  git -C "$SRC_DIR" ls-files -z | tar --null -T - -czf "$STATE_DIR/src.tgz"
  nscp "$STATE_DIR/src.tgz" "$pub" '~/src.tgz'
  nsh "$pub" 'rm -rf ~/dynomite && mkdir -p ~/dynomite && tar -xzf ~/src.tgz -C ~/dynomite'
  log "up complete: $pub"
}

phase_qualify() {
  local pub; pub=$(cat "$STATE_DIR/pub")
  # The CI gate's core against the COMMITTED lock (noxu 7.11.0 + the
  # refreshed deps): build all-targets/all-features, workspace tests,
  # doctests, DST models, clippy, fmt. libopenblas is at the standard
  # apt path so no LD_LIBRARY_PATH juggling is needed.
  log "running cargo update + build + test on $pub (this takes a while)"
  nsh "$pub" '
    set -o pipefail
    . ~/.cargo/env
    cd ~/dynomite
    # Debug info dominates the target-dir size for an all-features
    # all-targets build; drop it so the disk holds the full build.
    export CARGO_PROFILE_DEV_DEBUG=0
    export CARGO_PROFILE_TEST_DEBUG=0
    echo "=== rustc / cargo ==="; rustc --version; cargo --version
    echo "=== noxu version resolved ==="; grep -A1 '"'"'name = "noxu"'"'"' Cargo.lock | grep version | head -1
    echo "=== fmt check ==="
    cargo fmt --all -- --check && echo FMT_OK
    echo "=== build workspace all-targets all-features ==="
    cargo build --workspace --all-targets --all-features 2>&1 | tail -5
    echo "=== clippy ==="
    cargo clippy --workspace --all-targets --all-features -- -D warnings 2>&1 | tail -5
    echo "=== nextest (install if missing) ==="
    command -v cargo-nextest >/dev/null 2>&1 || cargo install cargo-nextest --locked >/dev/null 2>&1
    cargo nextest run --workspace --all-features --no-fail-fast 2>&1 | tail -12
    echo "=== doctests (dyniak + engine) ==="
    cargo test --doc -p dyniak --features noxu,wasm 2>&1 | tail -2
    cargo test --doc -p dynomite-engine --all-features 2>&1 | tail -2
    echo "=== DST models ==="
    ( ulimit -v 8388608; cargo test -p model-tests 2>&1 | tail -2 )
    echo "=== df after build ==="; df -h / | tail -1
    echo "=== QUALIFY_DONE ==="
  '
}

phase_fetch_lock() {
  local pub; pub=$(cat "$STATE_DIR/pub")
  SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none \
    "$SSH_USER"@"$pub":'~/dynomite/Cargo.lock' "$STATE_DIR/Cargo.lock" >/dev/null 2>&1 \
    && log "fetched updated Cargo.lock to $STATE_DIR/Cargo.lock" || { log "fetch failed"; return 1; }
}

phase_down() {
  local ids sg kp
  ids=$(aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
  if [ -n "$ids" ]; then
    aws ec2 terminate-instances --region "$REGION" --instance-ids $ids >/dev/null 2>&1
    aws ec2 wait instance-terminated --region "$REGION" --instance-ids $ids 2>/dev/null
    log "terminated: $ids"
  fi
  for sg in $(aws ec2 describe-security-groups --region "$REGION" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[].GroupId' --output text 2>/dev/null); do
    aws ec2 delete-security-group --region "$REGION" --group-id "$sg" >/dev/null 2>&1 || true
  done
  for kp in $(aws ec2 describe-key-pairs --region "$REGION" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'KeyPairs[].KeyName' --output text 2>/dev/null); do
    aws ec2 delete-key-pair --region "$REGION" --key-name "$kp" >/dev/null 2>&1 || true
  done
  log "teardown complete for $RUN_ID"
}

case "${1:-}" in
  up) phase_up ;;
  qualify) phase_qualify ;;
  fetch-lock) phase_fetch_lock ;;
  down) phase_down ;;
  *) echo "usage: RUN_ID=... $0 {up|qualify|fetch-lock|down}" >&2; exit 1 ;;
esac
