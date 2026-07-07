#!/usr/bin/env bash
# Multi-region, multi-rack, mixed-architecture cluster deployer.
#
# Models a customer migrating from Intel to Graviton:
#   * 5 regions, each a datacenter, 9 nodes per region;
#   * per region: 3 racks x 3 nodes (each rack a full replica, the 3
#     nodes in a rack partition the token ring -> n_val = 3);
#   * 3 regions on Intel (x86_64, m6id.xlarge, NVMe instance store) --
#     the "old" fleet;
#   * 2 regions on Graviton (arm64, m8gd.xlarge, NVMe instance store) --
#     the "new" fleet.
#
# Storage is LOCAL EPHEMERAL (instance store), moderate RAM/vCPU
# (xlarge = 4 vCPU / 16 GiB). Allowlist-only security groups (controller
# /32 + every node /32); never 0.0.0.0/0.
#
# Usage: deploy-mixed.sh {up|down|status}
set -euo pipefail

PROFILE=numa
RUN_ID="$(cat /tmp/dyn-ec2-runid 2>/dev/null | sed 's/RUN_ID=//')"
[ -z "$RUN_ID" ] && { echo "no RUN_ID in /tmp/dyn-ec2-runid" >&2; exit 1; }
TAG="dyn-run"
STATE="/tmp/${RUN_ID}.state.ips"

RACKS=3
NODES_PER_RACK=3   # 3 racks x 3 = 9 per region

# region -> arch (x86 = Intel, arm = Graviton). 3 Intel, 2 Graviton.
declare -A ARCH=(
  [us-east-1]=x86 [us-west-2]=x86 [eu-central-1]=x86
  [ap-northeast-1]=arm [sa-east-1]=arm
)
declare -A ITYPE=( [x86]=m6id.xlarge [arm]=m8gd.xlarge )
declare -A DC=(
  [us-east-1]=dc-use1 [us-west-2]=dc-usw2 [eu-central-1]=dc-euc1
  [ap-northeast-1]=dc-apne1 [sa-east-1]=dc-sae1
)
declare -A AMI_X86=(
  [us-east-1]=ami-0de568ccf3b0080d9 [us-west-2]=ami-05f8addf6121a3888
  [eu-central-1]=ami-0ae2eb6210612f5a0 [ap-northeast-1]=ami-0b5a8e1579cb5ed8e
  [sa-east-1]=ami-0de8ccbb3a0a00dba
)
declare -A AMI_ARM=(
  [us-east-1]=ami-0dc9c39dfb2c22a1e [us-west-2]=ami-0c0d650b2def3db6e
  [eu-central-1]=ami-07317784e1ea161a7 [ap-northeast-1]=ami-0f8f565aff0af885b
  [sa-east-1]=ami-08b4ccede7c8e90aa
)
# Extra region for the load-driven migration target (3rd Graviton
# region). AMIs resolved at add time when absent from the tables.
DC[eu-west-1]=dc-euw1
AZS[eu-west-1]="eu-west-1a eu-west-1b eu-west-1c"
declare -A AZS=(
  [us-east-1]="us-east-1a us-east-1b us-east-1c"
  [us-west-2]="us-west-2a us-west-2b us-west-2c"
  [eu-central-1]="eu-central-1a eu-central-1b eu-central-1c"
  [ap-northeast-1]="ap-northeast-1a ap-northeast-1c ap-northeast-1d"
  [sa-east-1]="sa-east-1a sa-east-1b sa-east-1c"
)
REGIONS=(us-east-1 us-west-2 eu-central-1 ap-northeast-1 sa-east-1)
# Ports: client 8102, dnode 8101, stats 22222, riak pbc 8087, http 8098,
# plus the differential Rust plane 9101/9102 and stats 22223.
MY_IP="$(curl -s -m 10 https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]')"

aws() { command aws --profile "$PROFILE" "$@"; }
log() { echo "[mixed $(date -u +%H:%M:%S)] $*" >&2; }

authorize_ip() {
  local region=$1 sg=$2 ip=$3
  aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" \
    --protocol tcp --port 22 --cidr "${ip}/32" >/dev/null 2>&1 || true
  aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" \
    --protocol tcp --port 8087-9102 --cidr "${ip}/32" >/dev/null 2>&1 || true
  aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" \
    --protocol tcp --port 22222-22223 --cidr "${ip}/32" >/dev/null 2>&1 || true
}

ensure_region_infra() {
  local region=$1
  local keyname="${RUN_ID}-key" keyfile="/tmp/${RUN_ID}-${region}.pem"
  if [ ! -f "$keyfile" ]; then
    aws ec2 create-key-pair --region "$region" --key-name "$keyname" \
      --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'KeyMaterial' --output text > "$keyfile" 2>/dev/null && chmod 600 "$keyfile"
  fi
  local sg
  sg=$(aws ec2 describe-security-groups --region "$region" \
    --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
  if [ -z "$sg" ] || [ "$sg" = "None" ]; then
    local vpc; vpc=$(aws ec2 describe-vpcs --region "$region" --query 'Vpcs[?IsDefault==`true`].VpcId' --output text)
    sg=$(aws ec2 create-security-group --region "$region" --group-name "${RUN_ID}-sg" \
      --description "dynomite mixed $RUN_ID" --vpc-id "$vpc" \
      --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'GroupId' --output text)
    authorize_ip "$region" "$sg" "$MY_IP"
  fi
  echo "$sg"
}

allowlist_ip_everywhere() {
  local ip=$1 region sg
  for region in "${REGIONS[@]}"; do
    sg=$(aws ec2 describe-security-groups --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
    [ -z "$sg" ] || [ "$sg" = "None" ] && continue
    authorize_ip "$region" "$sg" "$ip"
  done
}

launch_one() {
  local region=$1 az=$2 dc=$3 rack=$4 node=$5
  local sg; sg=$(ensure_region_infra "$region")
  local subnet; subnet=$(aws ec2 describe-subnets --region "$region" \
    --filters "Name=availability-zone,Values=$az" "Name=default-for-az,Values=true" \
    --query 'Subnets[0].SubnetId' --output text)
  local arch=${ARCH[$region]}
  local itype=${ITYPE[$arch]}
  local ami; [ "$arch" = x86 ] && ami=${AMI_X86[$region]} || ami=${AMI_ARM[$region]}
  local iid; iid=$(aws ec2 run-instances --region "$region" \
    --image-id "$ami" --instance-type "$itype" \
    --key-name "${RUN_ID}-key" --security-group-ids "$sg" --subnet-id "$subnet" \
    --associate-public-ip-address \
    --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${dc}-${rack}-${node}},{Key=arch,Value=${arch}}]" \
    --query 'Instances[0].InstanceId' --output text)
  aws ec2 wait instance-running --region "$region" --instance-ids "$iid"
  local pub; pub=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  local priv; priv=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
  allowlist_ip_everywhere "$pub"
  # State row: region az dc rack node arch itype iid pub priv
  echo "$region $az $dc $rack $node $arch $itype $iid $pub $priv"
}

up() {
  : > "$STATE"
  local region
  log "RUN_ID=$RUN_ID provisioning ${#REGIONS[@]} regions x $((RACKS*NODES_PER_RACK)) nodes (3 racks x 3)"
  for region in "${REGIONS[@]}"; do
    local azs=(${AZS[$region]})
    local r
    for r in $(seq 1 $RACKS); do
      local az=${azs[$(( (r-1) % ${#azs[@]} ))]}
      local n
      for n in $(seq 1 $NODES_PER_RACK); do
        local row; row=$(launch_one "$region" "$az" "${DC[$region]}" "r${r}" "n${n}")
        echo "$row" >> "$STATE"
        log "up: $row"
      done
    done
  done
  log "up complete; $(wc -l < "$STATE") nodes; state in $STATE"
}

status() {
  local region
  for region in "${REGIONS[@]}"; do
    aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running" \
      --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name,Tags[?Key==`Name`]|[0].Value]' \
      --output text 2>/dev/null || true
  done
}

down() {
  local region
  log "TEARDOWN $RUN_ID"
  for region in "${REGIONS[@]}"; do
    local ids; ids=$(aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    if [ -n "$ids" ]; then
      aws ec2 terminate-instances --region "$region" --instance-ids $ids >/dev/null 2>&1 || true
      aws ec2 wait instance-terminated --region "$region" --instance-ids $ids 2>/dev/null || true
      log "$region terminated"
    fi
    local sg
    for sg in $(aws ec2 describe-security-groups --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[].GroupId' --output text 2>/dev/null); do
      aws ec2 delete-security-group --region "$region" --group-id "$sg" >/dev/null 2>&1 || true
    done
    local kp
    for kp in $(aws ec2 describe-key-pairs --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'KeyPairs[].KeyName' --output text 2>/dev/null); do
      aws ec2 delete-key-pair --region "$region" --key-name "$kp" >/dev/null 2>&1 || true
    done
    rm -f "/tmp/${RUN_ID}-${region}.pem"
  done
  log "teardown complete"
}

# Provision one additional region (used by the migration to grow to a
# 3rd Graviton region). Resolves the AMI on demand, provisions 9 nodes
# (3 racks x 3), allowlists them everywhere, and APPENDS to $STATE.
add_region() {
  local region=$1 arch=$2
  [ -z "$region" ] || [ -z "$arch" ] && { echo "usage: add-region <region> <x86|arm>" >&2; return 1; }
  ARCH[$region]=$arch
  [ -z "${DC[$region]:-}" ] && DC[$region]="dc-${region//-/}"
  if [ -z "${AZS[$region]:-}" ]; then
    AZS[$region]=$(aws ec2 describe-availability-zones --region "$region" \
      --query 'AvailabilityZones[?State==`available`].ZoneName' --output text 2>/dev/null | tr '\t' ' ')
  fi
  # resolve AMI for the arch if not tabled
  if [ "$arch" = arm ] && [ -z "${AMI_ARM[$region]:-}" ]; then
    AMI_ARM[$region]=$(aws ec2 describe-images --region "$region" --owners amazon \
      --filters "Name=name,Values=al2023-ami-2023.*-arm64" "Name=state,Values=available" \
      --query 'reverse(sort_by(Images,&CreationDate))[0].ImageId' --output text 2>/dev/null)
  elif [ "$arch" = x86 ] && [ -z "${AMI_X86[$region]:-}" ]; then
    AMI_X86[$region]=$(aws ec2 describe-images --region "$region" --owners amazon \
      --filters "Name=name,Values=al2023-ami-2023.*-x86_64" "Name=state,Values=available" \
      --query 'reverse(sort_by(Images,&CreationDate))[0].ImageId' --output text 2>/dev/null)
  fi
  # add this region to REGIONS so allowlist_ip_everywhere covers it
  REGIONS+=("$region")
  local azs=(${AZS[$region]}) r n
  for r in $(seq 1 $RACKS); do
    local az=${azs[$(( (r-1) % ${#azs[@]} ))]}
    for n in $(seq 1 $NODES_PER_RACK); do
      local row; row=$(launch_one "$region" "$az" "${DC[$region]}" "r${r}" "n${n}")
      echo "$row" >> "$STATE"
      log "add: $row"
    done
  done
  log "add-region $region ($arch) complete: 9 nodes appended to $STATE"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  add-region) add_region "$2" "$3" ;;
  *) echo "usage: $0 {up|down|status|add-region <region> <x86|arm>}" >&2; exit 1 ;;
esac
