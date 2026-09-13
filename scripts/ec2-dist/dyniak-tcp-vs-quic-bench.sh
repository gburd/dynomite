#!/usr/bin/env bash
# TCP-vs-QUIC head-to-head for dyniak's Riak PBC surface, plus a
# dyniak(TCP)-vs-real-Riak(TCP) leg, on two EC2 nodes. Built to
# re-measure after the write_frame/TCP_NODELAY fix (d4a027c) and to
# compare the TCP and QUIC transports for the same PBC framing.
#
# Differs from dyniak-vs-riak-bench.sh:
#   * builds dynomited + dyniak-bench with `--features riak,quic`,
#   * self-signs a TLS cert on the dyniak node (QUIC mandates TLS),
#   * adds a `quic_listen` to the dyniak config + a UDP SG rule,
#   * runs each PBC workload over BOTH transports on dyniak
#     (riak_pbc = TCP, riak_quic = QUIC),
#   * still runs dyniak-TCP vs Riak-TCP for the cross-system headline.
#
# Usage:
#   RUN_ID=dynqt-$(date -u +%Y%m%d-%H%M%S) SRC_DIR=/home/gburd/ws/dynomite \
#     scripts/ec2-dist/dyniak-tcp-vs-quic-bench.sh up
#   scripts/ec2-dist/dyniak-tcp-vs-quic-bench.sh run
#   scripts/ec2-dist/dyniak-tcp-vs-quic-bench.sh down
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
log() { echo "[dyn-tcp-quic $(date -u +%H:%M:%S)] $*" >&2; }
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
    --description "dyniak tcp-vs-quic bench $RUN_ID" --vpc-id "$vpc" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
    --query 'GroupId' --output text)
  # Allowlist-only ingress from the controller /32. TCP ports as the
  # sibling script, plus UDP 8103 for the QUIC PBC listener.
  local p
  for p in 22 8087 8098 8101 8102 22222; do
    aws ec2 authorize-security-group-ingress --region "$REGION" \
      --group-id "$sg" --protocol tcp --port "$p" --cidr "${MY_IP}/32" >/dev/null 2>&1
  done
  aws ec2 authorize-security-group-ingress --region "$REGION" \
    --group-id "$sg" --protocol udp --port 8103 --cidr "${MY_IP}/32" >/dev/null 2>&1
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

  # Build dynomited + dyniak-bench WITH the quic feature on the dyniak
  # node. Build from committed HEAD (git ls-files), so the NODELAY fix
  # is included.
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
    CARGO_TARGET_DIR=/mnt/data/cargo-target cargo build --release -p dynomited --no-default-features --features riak,quic
    CARGO_TARGET_DIR=/mnt/data/cargo-target cargo build --release -p dyniak-bench --features riak,quic
    cp /mnt/data/cargo-target/release/dynomited ~/dynomited
    cp /mnt/data/cargo-target/release/dyniak-bench ~/dyniak-bench
    # Self-signed cert for the QUIC listener (QUIC mandates TLS; the
    # bench client dials insecure, so the cert content is irrelevant).
    openssl req -x509 -newkey rsa:2048 -nodes -keyout ~/quic.key -out ~/quic.crt \
      -days 2 -subj "/CN=dyniak-bench" >/dev/null 2>&1
  ' || { log "build failed on dyniak node"; return 1; }
  log "dynomited + dyniak-bench (quic) built on dyniak node"

  SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none \
    ec2-user@"$dyniak_pub":'~/dyniak-bench' "$STATE_DIR/dyniak-bench" >/dev/null 2>&1
  nscp "$STATE_DIR/dyniak-bench" "$riak_pub" '~/dyniak-bench'
  nsh "$riak_pub" 'chmod +x ~/dyniak-bench'

  # dyniak config: TCP PBC 8087, HTTP 8098, and QUIC PBC on UDP 8103.
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
    quic_listen: 0.0.0.0:8103
    tls_cert: /home/ec2-user/quic.crt
    tls_key: /home/ec2-user/quic.key
YML
  nscp "$STATE_DIR/dyniak.yml" "$dyniak_pub" '~/dyniak.yml'
  nsh "$dyniak_pub" '
    mkdir -p /mnt/data/noxu
    pkill -9 -x dynomited 2>/dev/null; sleep 1
    DYN_ADVERTISE_ADDR='"$dyniak_pub"' nohup ~/dynomited -c ~/dyniak.yml -p ~/dynomited.pid \
      -o ~/dynomited.log -v 4 > ~/dynomited.out 2>&1 < /dev/null &
    sleep 4
    ss -tln | grep -q :8087 && ss -uln | grep -q :8103 && echo up
  ' | grep -q up && log "dyniak PBC(TCP 8087)+QUIC(UDP 8103) up on $dyniak_pub" \
     || { log "dyniak did not come up (TCP+QUIC)"; nsh "$dyniak_pub" 'tail -20 ~/dynomited.out ~/dynomited.log 2>/dev/null'; return 1; }

  # Real Riak via Docker (TCP only; Riak has no QUIC).
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
  nsh "$riak_pub" '
    sudo docker exec riak /usr/sbin/riak-admin bucket-type create counters "{\"props\":{\"datatype\":\"counter\"}}" >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type create sets "{\"props\":{\"datatype\":\"set\"}}" >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type activate counters >/dev/null 2>&1
    sudo docker exec riak /usr/sbin/riak-admin bucket-type activate sets >/dev/null 2>&1
  '
  log "up complete: dyniak=$dyniak_pub riak=$riak_pub"
}

# Run one workload file on one node, pull the CSVs. `label` may
# contain a `/` for the local results tree; the on-node paths use a
# flattened form so the --out dir and log redirect target exist.
run_one() {
  local pub=$1 wl=$2 label=$3
  local flat=${label//\//-}
  nsh "$pub" "cd ~ && mkdir -p /tmp/run-${flat} && ./dyniak-bench --config ${wl}.toml --out /tmp/run-${flat} > /tmp/run-${flat}.log 2>&1"
  mkdir -p "$STATE_DIR/results/${label}"
  SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none \
    ec2-user@"$pub":"/tmp/run-${flat}/*.csv" "$STATE_DIR/results/${label}/" >/dev/null 2>&1
  log "$label done"
}

phase_run() {
  local dyniak_pub riak_pub
  dyniak_pub=$(cat "$STATE_DIR/dyniak.pub")
  riak_pub=$(cat "$STATE_DIR/riak.pub")

  # Base TCP workloads (shipped, reused from the sibling script).
  local wl toml
  for wl in wl-pbc wl-crdt-counter wl-crdt-set; do
    toml=$(find "$SRC_DIR/dist/bench-reports" -maxdepth 2 -name "${wl}.toml" | head -1)
    [ -z "$toml" ] && { log "missing $wl.toml"; return 1; }
    nscp "$toml" "$dyniak_pub" "~/${wl}.toml"
    nscp "$toml" "$riak_pub" "~/${wl}.toml"
  done

  # Derive a QUIC PBC workload from wl-pbc: same shape, riak_quic
  # driver, UDP port 8103. Written on the controller, shipped to the
  # dyniak node.
  local pbc_toml; pbc_toml=$(find "$SRC_DIR/dist/bench-reports" -maxdepth 2 -name "wl-pbc.toml" | head -1)
  sed -e 's/kind = "riak_pbc"/kind = "riak_quic"/' -e 's/port = 8087/port = 8103/' \
    "$pbc_toml" > "$STATE_DIR/wl-pbc-quic.toml"
  nscp "$STATE_DIR/wl-pbc-quic.toml" "$dyniak_pub" '~/wl-pbc-quic.toml'

  # dyniak: TCP mixed, QUIC mixed, TCP counter, TCP set.
  run_one "$dyniak_pub" wl-pbc         dyniak/pbc-tcp
  run_one "$dyniak_pub" wl-pbc-quic    dyniak/pbc-quic
  run_one "$dyniak_pub" wl-crdt-counter dyniak/counter
  run_one "$dyniak_pub" wl-crdt-set     dyniak/set

  # Riak (TCP only): mixed, counter, set.
  run_one "$riak_pub" wl-pbc          riak/pbc-tcp
  run_one "$riak_pub" wl-crdt-counter riak/counter
  run_one "$riak_pub" wl-crdt-set     riak/set

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
