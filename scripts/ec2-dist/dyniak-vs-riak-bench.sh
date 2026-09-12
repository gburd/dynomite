#!/usr/bin/env bash
# Single-region, two-node A/B benchmark: Dyniak (dynomited --features
# riak, data_store: dyniak, noxu on local NVMe) vs real Apache Riak KV
# (basho/riak-kv Docker image, bitcask backend), driven by the same
# dyniak-bench workloads over the Riak PBC protocol.
#
# This is a documentation/reproduction script, not a resumable
# orchestrator like chaos-dyniak.sh. It mirrors that script's
# conventions (profile, tagging, NVMe-mount pattern, ssh/scp helpers,
# teardown-by-tag) at a smaller scope: one dyniak node, one Riak node,
# no gossip ring, no multi-region.
#
# Usage:
#   RUN_ID=dynriak-$(date -u +%Y%m%d-%H%M%S) SRC_DIR=/home/gburd/ws/dynomite \
#     scripts/ec2-dist/dyniak-vs-riak-bench.sh up
#   scripts/ec2-dist/dyniak-vs-riak-bench.sh run     # requires the env below
#   scripts/ec2-dist/dyniak-vs-riak-bench.sh down
#
# Building dyniak-bench and dynomited MUST happen on the EC2 node
# (AL2023 glibc), not on the controller -- a locally-built binary
# linked against a Nix glibc will not run there.
set -uo pipefail

PROFILE="${PROFILE:-lava}"
RUN_ID="${RUN_ID:?set RUN_ID}"
SRC_DIR="${SRC_DIR:-/home/gburd/ws/dynomite}"
TAG=dyn-run
REGION="${REGION:-us-east-1}"
ITYPE="${ITYPE:-m6id.xlarge}"
AMI="${AMI:-ami-0de568ccf3b0080d9}"   # us-east-1 AL2023, x86_64

STATE_DIR="/tmp/${RUN_ID}"
KEY="/tmp/${RUN_ID}.pem"
mkdir -p "$STATE_DIR"

aws() { command aws --profile "$PROFILE" "$@"; }
log() { echo "[dyniak-vs-riak $(date -u +%H:%M:%S)] $*" >&2; }
MY_IP="$(curl -s -m 10 https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]')"

nsh()  { local ip=$1; shift; SSH_AUTH_SOCK="" ssh -n -i "$KEY" -o StrictHostKeyChecking=no \
           -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=15 ec2-user@"$ip" "$@"; }
nscp() { local src=$1 ip=$2 dst=$3; SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no \
           -o IdentitiesOnly=yes -o IdentityAgent=none "$src" ec2-user@"$ip":"$dst" >/dev/null 2>&1; }

phase_up() {
  [ -z "$MY_IP" ] && { log "could not determine controller public IP"; return 1; }
  [ -f "$KEY" ] || ssh-keygen -t ed25519 -N "" -f "$KEY" -q
  aws ec2 import-key-pair --region "$REGION" --key-name "${RUN_ID}-key" \
    --public-key-material "fileb://${KEY}.pub" \
    --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN_ID}]" >/dev/null 2>&1

  local vpc sg
  vpc=$(aws ec2 describe-vpcs --region "$REGION" --query 'Vpcs[?IsDefault==`true`].VpcId' --output text)
  sg=$(aws ec2 create-security-group --region "$REGION" --group-name "${RUN_ID}-sg" \
    --description "dyniak-vs-riak bench $RUN_ID" --vpc-id "$vpc" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
    --query 'GroupId' --output text)
  # Allowlist-only ingress from the controller's /32. Riak and dyniak
  # both use PBC 8087 / HTTP 8098; dyniak additionally needs its dnode
  # (8101), client (8102), and stats (22222) ports even though this
  # single-node setup does not use gossip.
  local p
  for p in 22 8087 8098 8101 8102 22222; do
    aws ec2 authorize-security-group-ingress --region "$REGION" \
      --group-id "$sg" --protocol tcp --port "$p" --cidr "${MY_IP}/32" >/dev/null 2>&1
  done
  echo "$sg" > "$STATE_DIR/sg"
  log "sg=$sg vpc=$vpc"

  local dyniak_iid riak_iid
  dyniak_iid=$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" \
    --instance-type "$ITYPE" --count 1 --key-name "${RUN_ID}-key" --security-group-ids "$sg" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${RUN_ID}-dyniak},{Key=role,Value=dyniak}]" \
    --query 'Instances[0].InstanceId' --output text)
  riak_iid=$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" \
    --instance-type "$ITYPE" --count 1 --key-name "${RUN_ID}-key" --security-group-ids "$sg" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${RUN_ID}-riak},{Key=role,Value=riak}]" \
    --query 'Instances[0].InstanceId' --output text)
  echo "$dyniak_iid" > "$STATE_DIR/dyniak.iid"
  echo "$riak_iid" > "$STATE_DIR/riak.iid"
  aws ec2 wait instance-running --region "$REGION" --instance-ids "$dyniak_iid" "$riak_iid"

  local dyniak_pub riak_pub
  dyniak_pub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$dyniak_iid" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  riak_pub=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$riak_iid" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  echo "$dyniak_pub" > "$STATE_DIR/dyniak.pub"
  echo "$riak_pub" > "$STATE_DIR/riak.pub"
  log "dyniak node: $dyniak_iid $dyniak_pub"
  log "riak node:   $riak_iid $riak_pub"

  local i ok=0
  for i in 1 2 3 4 5 6 7 8; do
    nsh "$dyniak_pub" 'echo ok' 2>/dev/null | grep -q ok && { ok=$((ok+1)); break; }
    sleep 5
  done
  for i in 1 2 3 4 5 6 7 8; do
    nsh "$riak_pub" 'echo ok' 2>/dev/null | grep -q ok && { ok=$((ok+1)); break; }
    sleep 5
  done
  [ "$ok" -eq 2 ] && log "both nodes reachable over ssh" || { log "ssh not ready on both nodes"; return 1; }

  # Mount instance-store NVMe on both nodes at /mnt/data (largest
  # unmounted disk, not by device name -- naming is not stable across
  # instance families).
  local pub
  for pub in "$dyniak_pub" "$riak_pub"; do
    nsh "$pub" '
      DEV=$(lsblk -bdpno NAME,SIZE,TYPE | awk "\$3==\"disk\"{print \$1, \$2}" | while read d sz; do
        mounted=$(lsblk -no MOUNTPOINT "$d" 2>/dev/null | grep -c .)
        [ "$mounted" -eq 0 ] && echo "$sz $d"
      done | sort -rn | head -1 | awk "{print \$2}")
      sudo mkfs.xfs -f "$DEV" >/dev/null 2>&1
      sudo mkdir -p /mnt/data && sudo mount "$DEV" /mnt/data && sudo chown ec2-user:ec2-user /mnt/data
    '
  done
  log "NVMe mounted on both nodes at /mnt/data"

  # Build dynomited + dyniak-bench on the dyniak node (AL2023 target).
  # cargo target dir MUST live on the mounted NVMe: the AL2023 AMI's
  # root volume is a bare 8G and fills up mid-build otherwise.
  git -C "$SRC_DIR" ls-files -z | tar --null -T - -czf "$STATE_DIR/src.tgz"
  nscp "$STATE_DIR/src.tgz" "$dyniak_pub" '~/src.tgz'
  nsh "$dyniak_pub" '
    set -e
    command -v cargo >/dev/null 2>&1 || { curl -sSf https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1; }
    . ~/.cargo/env 2>/dev/null || true
    sudo dnf install -y -q gcc gcc-c++ cmake perl openssl-devel clang git >/dev/null 2>&1 || true
    mkdir -p ~/build && find ~/build -mindepth 1 -delete 2>/dev/null
    tar -xzf ~/src.tgz -C ~/build
    mkdir -p /mnt/data/cargo-target
    cd ~/build
    CARGO_TARGET_DIR=/mnt/data/cargo-target cargo build --release -p dynomited --no-default-features --features riak
    CARGO_TARGET_DIR=/mnt/data/cargo-target cargo build --release -p dyniak-bench --features riak
    cp /mnt/data/cargo-target/release/dynomited ~/dynomited
    cp /mnt/data/cargo-target/release/dyniak-bench ~/dyniak-bench
  ' || { log "build failed on dyniak node"; return 1; }
  log "dynomited + dyniak-bench built on dyniak node"

  # Copy the AL2023-native dyniak-bench binary to the riak node too
  # (same AMI/arch, so no cross-build needed).
  SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none \
    ec2-user@"$dyniak_pub":'~/dyniak-bench' "$STATE_DIR/dyniak-bench" >/dev/null 2>&1
  nscp "$STATE_DIR/dyniak-bench" "$riak_pub" '~/dyniak-bench'
  nsh "$riak_pub" 'chmod +x ~/dyniak-bench'

  # dyniak: data_store: dyniak, riak PBC/HTTP listeners, noxu on NVMe.
  cat > "$STATE_DIR/dyniak.yml" <<YML
dyn_o_mite:
  listen: 0.0.0.0:8102
  dyn_listen: 0.0.0.0:8101
  stats_listen: 0.0.0.0:22222
  tokens: '0'
  datacenter: dc-bench
  rack: rack0
  data_store: dyniak
  noxu_path: /mnt/data/noxu
  servers:
  - 127.0.0.1:6379:1
  riak:
    pbc_listen: 0.0.0.0:8087
    http_listen: 0.0.0.0:8098
YML
  nscp "$STATE_DIR/dyniak.yml" "$dyniak_pub" '~/dyniak.yml'
  nsh "$dyniak_pub" '
    mkdir -p /mnt/data/noxu
    pkill -9 -x dynomited 2>/dev/null; sleep 1
    DYN_ADVERTISE_ADDR='"$dyniak_pub"' nohup ~/dynomited -c ~/dyniak.yml -p ~/dynomited.pid \
      -o ~/dynomited.log -v 4 > ~/dynomited.out 2>&1 < /dev/null &
    sleep 4
    ss -tln | grep -q :8087 && echo up
  ' | grep -q up && log "dyniak PBC up on $dyniak_pub:8087" || { log "dyniak did not come up"; return 1; }

  # Riak KV via the official basho/riak-kv Docker image. AL2023 has no
  # native Riak package; Docker + the upstream image is the reliable
  # path. The RUNNER_USER inside the image is uid 102 (riak), so the
  # bind-mounted data/log dirs must be owned by that uid, not root.
  nsh "$riak_pub" '
    sudo dnf install -y -q docker >/dev/null 2>&1
    sudo systemctl enable --now docker >/dev/null 2>&1
    sleep 3
    sudo docker pull basho/riak-kv:latest >/dev/null 2>&1
    sudo mkdir -p /mnt/data/riak-bitcask /mnt/data/riak-log
    sudo chown -R 102:105 /mnt/data/riak-bitcask /mnt/data/riak-log
    sudo docker rm -f riak >/dev/null 2>&1 || true
    sudo docker run -d --name riak \
      -p 8087:8087 -p 8098:8098 \
      -v /mnt/data/riak-bitcask:/var/lib/riak \
      -v /mnt/data/riak-log:/var/log/riak \
      -e WAIT_FOR_ERLANG=60 \
      --ulimit nofile=65536:65536 \
      basho/riak-kv:latest
  ' >/dev/null 2>&1
  local a
  for a in 1 2 3 4 5 6 7 8 9 10; do
    nsh "$riak_pub" 'sudo docker exec riak /usr/sbin/riak ping 2>/dev/null' 2>/dev/null | grep -q pong && break
    sleep 5
  done
  nsh "$riak_pub" 'sudo docker exec riak /usr/sbin/riak ping' 2>/dev/null | grep -q pong \
    && log "riak up on $riak_pub:8087" || { log "riak did not come up"; return 1; }

  # Activate the CRDT bucket types the workloads exercise.
  nsh "$riak_pub" '
    sudo docker exec riak /usr/sbin/riak-admin bucket-type create counters "{\"props\":{\"datatype\":\"counter\"}}" >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type create sets "{\"props\":{\"datatype\":\"set\"}}" >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type create maps "{\"props\":{\"datatype\":\"map\"}}" >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type activate counters >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type activate sets >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type activate maps >/dev/null 2>&1
  '
  log "up complete: dyniak=$dyniak_pub riak=$riak_pub"
}

phase_run() {
  local dyniak_pub riak_pub wl
  dyniak_pub=$(cat "$STATE_DIR/dyniak.pub")
  riak_pub=$(cat "$STATE_DIR/riak.pub")
  local tomls_dir; tomls_dir="$(dirname "$0")/../../dist/bench-reports"
  for wl in wl-pbc wl-crdt-counter wl-crdt-set wl-crdt-map; do
    local toml
    toml=$(find "$SRC_DIR/dist/bench-reports" -maxdepth 2 -name "${wl}.toml" | head -1)
    [ -z "$toml" ] && { log "missing $wl.toml"; continue; }
    nscp "$toml" "$dyniak_pub" "~/${wl}.toml"
    nscp "$toml" "$riak_pub" "~/${wl}.toml"
  done
  mkdir -p "$STATE_DIR/results/dyniak" "$STATE_DIR/results/riak"
  local dyniak_wl=(wl-pbc wl-crdt-counter wl-crdt-set)
  local riak_wl=(wl-pbc wl-crdt-counter wl-crdt-set wl-crdt-map)
  # wl-crdt-map is Riak-only: see the report's "map_update parity gap"
  # section for why it is not a fair dyniak comparison point yet.
  local outname
  for wl in "${dyniak_wl[@]}"; do
    outname=${wl#wl-}; outname=${outname#crdt-}
    nsh "$dyniak_pub" "cd ~ && ./dyniak-bench --config ${wl}.toml --out /tmp/run-${outname} > /tmp/run-${outname}.log 2>&1"
    mkdir -p "$STATE_DIR/results/dyniak/${outname}"
    SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none \
      ec2-user@"$dyniak_pub":"/tmp/run-${outname}/*.csv" "$STATE_DIR/results/dyniak/${outname}/" >/dev/null 2>&1
    log "dyniak $outname done"
  done
  for wl in "${riak_wl[@]}"; do
    outname=${wl#wl-}; outname=${outname#crdt-}
    nsh "$riak_pub" "cd ~ && ./dyniak-bench --config ${wl}.toml --out /tmp/run-${outname} > /tmp/run-${outname}.log 2>&1"
    mkdir -p "$STATE_DIR/results/riak/${outname}"
    SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none \
      ec2-user@"$riak_pub":"/tmp/run-${outname}/*.csv" "$STATE_DIR/results/riak/${outname}/" >/dev/null 2>&1
    log "riak $outname done"
  done
  log "results in $STATE_DIR/results"
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
  run) phase_run ;;
  down) phase_down ;;
  *) echo "usage: RUN_ID=... $0 {up|run|down}" >&2; exit 1 ;;
esac
