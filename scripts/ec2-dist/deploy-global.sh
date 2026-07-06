#!/usr/bin/env bash
# Global multi-region EC2 cluster for large-scale dynomite validation.
#
# Extends the 2-region qualification to a configurable global spread
# (default 5 regions x 3 nodes = 15 nodes) for the three data-store
# modes (memcache / valkey / dyniak), with membership churn, chaos,
# and benchmarking driven by the companion scripts.
#
# Allowlist-only security groups (controller /32 + every node /32);
# never 0.0.0.0/0 (the prior world-open rule got an account
# terminated). This is a burner AWS test account (no cost concern)
# but the security-policy discipline stays.
#
#   deploy-global.sh up      # provision the whole cluster
#   deploy-global.sh down    # terminate + delete everything by tag
#   deploy-global.sh status
#   deploy-global.sh add <region> <az> <dc> <n>   # launch one node (churn)
#   deploy-global.sh rm  <region> <iid>           # terminate one node (churn)
set -euo pipefail

PROFILE="${AWS_PROFILE:-numa}"
RUN_ID="$(cat /tmp/dyn-ec2-runid 2>/dev/null | sed 's/RUN_ID=//')"
[ -z "$RUN_ID" ] && { echo "no RUN_ID in /tmp/dyn-ec2-runid" >&2; exit 1; }
TAG="dyn-run"
STATE="/tmp/${RUN_ID}.state"
INSTANCE_TYPE="${DYN_INSTANCE_TYPE:-t3.medium}"   # 2 vCPU, 4 GB (dyniak + a local backend)
NODES_PER_REGION="${DYN_NODES_PER_REGION:-3}"

MY_IP="$(curl -s -m 10 https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]')"
[ -z "$MY_IP" ] && { echo "cannot determine controller IP" >&2; exit 1; }

# region -> DC name, AMI, AZ list. Five global regions.
declare -A DC=( [us-east-1]=dc-use1 [us-west-2]=dc-usw2 [eu-central-1]=dc-euc1 [ap-northeast-1]=dc-apne1 [sa-east-1]=dc-sae1 )
declare -A AMI=( [us-east-1]=ami-0de568ccf3b0080d9 [us-west-2]=ami-05f8addf6121a3888 [eu-central-1]=ami-0ae2eb6210612f5a0 [ap-northeast-1]=ami-0b9ef1a2afd0615e4 [sa-east-1]=ami-0de8ccbb3a0a00dba )
declare -A AZS=( [us-east-1]="us-east-1a us-east-1b us-east-1c" [us-west-2]="us-west-2a us-west-2b us-west-2c" [eu-central-1]="eu-central-1a eu-central-1b eu-central-1c" [ap-northeast-1]="ap-northeast-1a ap-northeast-1c ap-northeast-1d" [sa-east-1]="sa-east-1a sa-east-1b sa-east-1c" )
REGIONS=(us-east-1 us-west-2 eu-central-1 ap-northeast-1 sa-east-1)

# Ports: dynomite client 8102, dnode 8101, stats 22222, riak pbc
# 8087, riak http 8098; local backends valkey 6379 / memcached 11211
# (loopback only, not in the SG).
PORTS=(22 8102 8101 22222 8087 8098)

aws() { command aws --profile "$PROFILE" "$@"; }
log() { echo "[global $(date -u +%H:%M:%S)] $*" >&2; }

ensure_region_infra() {
  # key pair + security group in a region (idempotent).
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
      --description "dynomite global $RUN_ID" --vpc-id "$vpc" \
      --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'GroupId' --output text)
    for p in "${PORTS[@]}"; do
      aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" \
        --protocol tcp --port "$p" --cidr "${MY_IP}/32" >/dev/null 2>&1 || true
    done
  fi
  echo "$sg"
}

allowlist_ip_everywhere() {
  # add one /32 to every region's SG on the dynomite ports.
  local ip=$1
  local region
  for region in "${REGIONS[@]}"; do
    local sg; sg=$(aws ec2 describe-security-groups --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
    [ -z "$sg" ] || [ "$sg" = "None" ] && continue
    for p in 8102 8101 22222 8087 8098; do
      aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" \
        --protocol tcp --port "$p" --cidr "${ip}/32" >/dev/null 2>&1 || true
    done
  done
}

launch_one() {
  local region=$1 az=$2 dc=$3 n=$4
  local sg; sg=$(ensure_region_infra "$region")
  local subnet; subnet=$(aws ec2 describe-subnets --region "$region" \
    --filters "Name=availability-zone,Values=$az" "Name=default-for-az,Values=true" \
    --query 'Subnets[0].SubnetId' --output text)
  local iid; iid=$(aws ec2 run-instances --region "$region" \
    --image-id "${AMI[$region]}" --instance-type "$INSTANCE_TYPE" \
    --key-name "${RUN_ID}-key" --security-group-ids "$sg" --subnet-id "$subnet" \
    --associate-public-ip-address \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${dc}-${n}}]" \
    --query 'Instances[0].InstanceId' --output text)
  aws ec2 wait instance-running --region "$region" --instance-ids "$iid"
  local pub; pub=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  local priv; priv=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
  allowlist_ip_everywhere "$pub"
  echo "$region $az $dc $n $iid $pub $priv"
}

up() {
  : > "$STATE"
  local region
  log "RUN_ID=$RUN_ID provisioning ${#REGIONS[@]} regions x $NODES_PER_REGION nodes (${INSTANCE_TYPE})"
  : > "${STATE}.ips"
  for region in "${REGIONS[@]}"; do
    local azs=(${AZS[$region]}); local i=0
    while [ "$i" -lt "$NODES_PER_REGION" ]; do
      local az=${azs[$((i % ${#azs[@]}))]}
      i=$((i+1))
      local row; row=$(launch_one "$region" "$az" "${DC[$region]}" "n${i}")
      echo "$row" >> "${STATE}.ips"
      log "up: $row"
    done
  done
  log "up complete; $(wc -l < "${STATE}.ips") nodes; IPs in ${STATE}.ips"
}

status() {
  for region in "${REGIONS[@]}"; do
    aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running" \
      --query 'Reservations[].Instances[].[InstanceId,State.Name,PublicIpAddress,Tags[?Key==`Name`]|[0].Value]' \
      --output text 2>/dev/null || true
  done
}

down() {
  log "TEARDOWN $RUN_ID across ${#REGIONS[@]} regions"
  for region in "${REGIONS[@]}"; do
    local ids; ids=$(aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    if [ -n "$ids" ]; then
      aws ec2 terminate-instances --region "$region" --instance-ids $ids >/dev/null 2>&1 || true
      aws ec2 wait instance-terminated --region "$region" --instance-ids $ids 2>/dev/null || true
      log "$region terminated: $ids"
    fi
    for sg in $(aws ec2 describe-security-groups --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[].GroupId' --output text 2>/dev/null); do
      aws ec2 delete-security-group --region "$region" --group-id "$sg" >/dev/null 2>&1 || true
    done
    for kp in $(aws ec2 describe-key-pairs --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'KeyPairs[].KeyName' --output text 2>/dev/null); do
      aws ec2 delete-key-pair --region "$region" --key-name "$kp" >/dev/null 2>&1 || true
    done
    rm -f "/tmp/${RUN_ID}-${region}.pem"
  done
  log "teardown complete"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  add) launch_one "$2" "$3" "$4" "$5" ;;   # region az dc n
  rm) aws ec2 terminate-instances --region "$2" --instance-ids "$3" >/dev/null 2>&1 && echo "terminated $3" ;;
  *) echo "usage: $0 {up|down|status|add|rm}" >&2; exit 1 ;;
esac
