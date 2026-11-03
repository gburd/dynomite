#!/usr/bin/env bash
# Single Debian test host on the `chef` burner for the operational test
# suite: multi-node clusters in unprivileged network namespaces with tc
# netem, libfiu I/O faults, libumem allocation faults, FAINT, libdirectio,
# plus the C reference build for the oracle and differential tests.
#
# Every AWS resource carries the tag dyn-run=$RUN_ID; `down` deletes only
# resources with that tag. Signing keys and registry tokens never leave
# the controller.
#
# Usage:
#   export RUN_ID=optest-$(date -u +%Y%m%d-%H%M%S)
#   scripts/ec2-dist/chef-testhost.sh up        # launch + provision
#   scripts/ec2-dist/chef-testhost.sh sync      # ship committed HEAD (git ls-files)
#   scripts/ec2-dist/chef-testhost.sh run CMD   # run CMD in ~/dynomite on the host
#   scripts/ec2-dist/chef-testhost.sh ssh       # interactive shell
#   scripts/ec2-dist/chef-testhost.sh down      # terminate + delete SG + key
set -uo pipefail

PROFILE="${PROFILE:-chef}"
RUN_ID="${RUN_ID:?set RUN_ID}"
SRC_DIR="${SRC_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
TAG=dyn-run
REGION="${REGION:-us-east-1}"
ITYPE="${ITYPE:-c7i.8xlarge}"           # 32 vCPU, 64 GiB
DISK_GB="${DISK_GB:-300}"
SSH_USER="${SSH_USER:-admin}"           # Debian AMIs log in as admin
# Hard ceiling so a forgotten host dies on its own (minutes).
MAX_LIFETIME_MIN="${MAX_LIFETIME_MIN:-2880}"

STATE_DIR="${STATE_DIR:-$HOME/.cache/dyn-ec2/${RUN_ID}}"
KEY="$STATE_DIR/key"
mkdir -p "$STATE_DIR"

aws() { command aws --profile "$PROFILE" --region "$REGION" "$@"; }
log() { echo "[chef-testhost $(date -u +%H:%M:%S)] $*" >&2; }

nsh() {
  local ip; ip=$(cat "$STATE_DIR/pub")
  SSH_AUTH_SOCK="" ssh -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes \
    -o IdentityAgent=none -o ConnectTimeout=15 -o ServerAliveInterval=30 \
    "$SSH_USER@$ip" "$@"
}

latest_debian_ami() {
  aws ec2 describe-images --owners 136693071363 \
    --filters 'Name=name,Values=debian-13-amd64-2*' 'Name=architecture,Values=x86_64' \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text
}

phase_up() {
  local my_ip ami vpc sg iid pub
  my_ip=$(curl -s -m 10 https://checkip.amazonaws.com | tr -d '[:space:]')
  [ -n "$my_ip" ] || { log "cannot determine controller IP"; return 1; }
  [ "$(aws sts get-caller-identity --query Account --output text)" = 842964983020 ] \
    || { log "profile $PROFILE is not the chef burner"; return 1; }
  [ -f "$KEY" ] || ssh-keygen -t ed25519 -N "" -f "$KEY" -q
  aws ec2 import-key-pair --key-name "${RUN_ID}-key" --public-key-material "fileb://${KEY}.pub" \
    --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN_ID}]" >/dev/null
  vpc=$(aws ec2 describe-vpcs --query 'Vpcs[?IsDefault==`true`].VpcId' --output text)
  sg=$(aws ec2 create-security-group --group-name "${RUN_ID}-sg" --description "dynomite $RUN_ID" \
    --vpc-id "$vpc" --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
    --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp --port 22 \
    --cidr "${my_ip}/32" >/dev/null
  ami=$(latest_debian_ami)
  log "ami=$ami sg=$sg type=$ITYPE"
  iid=$(aws ec2 run-instances --image-id "$ami" --instance-type "$ITYPE" \
    --key-name "${RUN_ID}-key" --security-group-ids "$sg" \
    --instance-initiated-shutdown-behavior terminate \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$DISK_GB,VolumeType=gp3,Iops=6000,Throughput=500,DeleteOnTermination=true}" \
    --user-data "#!/bin/sh
shutdown -P +$MAX_LIFETIME_MIN" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=$RUN_ID}]" \
      "ResourceType=volume,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
    --query 'Instances[0].InstanceId' --output text) || return 1
  echo "$iid" > "$STATE_DIR/iid"
  aws ec2 wait instance-running --instance-ids "$iid"
  pub=$(aws ec2 describe-instances --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  echo "$pub" > "$STATE_DIR/pub"
  log "instance $iid at $pub; waiting for ssh"
  local i; for i in $(seq 1 40); do nsh true 2>/dev/null && break; sleep 5; done
  nsh true || { log "ssh never came up"; return 1; }
  phase_provision
}

phase_provision() {
  log "provisioning packages and fault-injection libraries"
  nsh 'bash -s' <<'REMOTE'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential cmake clang perl pkg-config libssl-dev \
  libopenblas-dev git curl ca-certificates autoconf automake libtool python3 \
  iproute2 iputils-ping netcat-openbsd valkey-server memcached libyaml-dev \
  faketime util-linux procps jq rsync >/dev/null
sudo systemctl disable --now valkey-server memcached >/dev/null 2>&1 || true
# Unprivileged user + network namespaces (the netem harness needs no root).
echo 'kernel.unprivileged_userns_clone=1' | sudo tee /etc/sysctl.d/90-userns.conf >/dev/null
sudo sysctl -q --system || true
sudo modprobe sch_netem sch_prio cls_u32 sch_htb 2>/dev/null || true
command -v cargo >/dev/null 2>&1 || curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >/dev/null 2>&1
. ~/.cargo/env
command -v cargo-nextest >/dev/null 2>&1 || cargo install -q cargo-nextest --locked
F=$HOME/fault; mkdir -p "$F/src" "$F/prefix"; cd "$F/src"
[ -d libfiu ] || git clone -q https://github.com/albertito/libfiu
[ -d libdirectio ] || git clone -q https://github.com/gburd/libdirectio
[ -d libumem ] || git clone -q https://codeberg.org/gregburd/libumem
[ -d faint ] || git clone -q https://github.com/misc0110/faint
( cd libfiu && make -s PREFIX="$F/prefix" >/dev/null && make -s PREFIX="$F/prefix" install >/dev/null )
( cd libdirectio && make -s >/dev/null && cp libdirectio.so.* "$F/prefix/lib/" )
( cd libumem && [ -f "$F/prefix/lib/libumem_malloc.so" ] || { ./autogen.sh >/dev/null 2>&1; \
  ./configure -q --prefix="$F/prefix" >/dev/null; make -s -j"$(nproc)" >/dev/null; make -s install >/dev/null; } )
echo "fault prefix: $(ls "$F/prefix/lib" | tr '\n' ' ')"
export PATH="$PATH:/usr/sbin:/sbin"
unshare -rn sh -c 'ip link set lo up && tc qdisc add dev lo root netem delay 5ms && echo NETNS_NETEM_OK'
REMOTE
}

phase_sync() {
  local tgz="$STATE_DIR/src.tgz"
  ( cd "$SRC_DIR" && git ls-files -z --recurse-submodules | tar --null -T - -czf "$tgz" )
  SSH_AUTH_SOCK="" scp -q -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes \
    -o IdentityAgent=none "$tgz" "$SSH_USER@$(cat "$STATE_DIR/pub"):src.tgz"
  nsh 'mkdir -p ~/dynomite && tar -xzf ~/src.tgz -C ~/dynomite'
  log "synced $(git -C "$SRC_DIR" rev-parse --short HEAD)"
}

phase_run() { nsh "export PATH=\$PATH:/usr/sbin:/sbin:\$HOME/fault/prefix/bin; cd ~/dynomite && . ~/.cargo/env && $*"; }

phase_down() {
  local ids
  ids=$(aws ec2 describe-instances --filters "Name=tag:$TAG,Values=$RUN_ID" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].InstanceId' --output text)
  if [ -n "$ids" ]; then
    aws ec2 terminate-instances --instance-ids $ids >/dev/null
    aws ec2 wait instance-terminated --instance-ids $ids
    log "terminated $ids"
  fi
  local sg kp
  for sg in $(aws ec2 describe-security-groups --filters "Name=tag:$TAG,Values=$RUN_ID" \
      --query 'SecurityGroups[].GroupId' --output text); do
    aws ec2 delete-security-group --group-id "$sg" && log "deleted $sg"
  done
  for kp in $(aws ec2 describe-key-pairs --filters "Name=tag:$TAG,Values=$RUN_ID" \
      --query 'KeyPairs[].KeyName' --output text); do
    aws ec2 delete-key-pair --key-name "$kp" && log "deleted key $kp"
  done
}

case "${1:-}" in
  up) phase_up ;;
  provision) phase_provision ;;
  sync) phase_sync ;;
  run) shift; phase_run "$@" ;;
  ssh) nsh ;;
  down) phase_down ;;
  *) sed -n '2,17p' "$0"; exit 2 ;;
esac
