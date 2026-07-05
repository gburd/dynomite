#!/usr/bin/env bash
# Distributed dynomite qualification on EC2 across AZs and regions.
#
# Stands up a real 6-node dyniak cluster:
#   * us-east-2 (datacenter dc-use2): 3 nodes in AZs a/b/c
#   * us-west-2 (datacenter dc-usw2): 3 nodes in AZs a/b/c
# Nodes gossip and replicate across AZs (intra-DC) and across
# regions (inter-DC, real WAN latency). Every AWS resource is tagged
# `dyn-run=<RUN_ID>` so teardown can enumerate and delete by tag.
#
# Usage:
#   scripts/ec2-dist/deploy.sh up        # provision + launch
#   scripts/ec2-dist/deploy.sh down      # terminate + clean everything
#   scripts/ec2-dist/deploy.sh status    # list tagged resources
#
# Requires: AWS_PROFILE (numa) with EC2 create/terminate, and a
# RUN_ID in /tmp/dyn-ec2-runid.
set -euo pipefail

PROFILE="${AWS_PROFILE:-numa}"
RUN_ID="$(cat /tmp/dyn-ec2-runid 2>/dev/null | sed 's/RUN_ID=//')"
[ -z "$RUN_ID" ] && { echo "no RUN_ID in /tmp/dyn-ec2-runid" >&2; exit 1; }
TAG="dyn-run"
STATE="/tmp/${RUN_ID}.state"
INSTANCE_TYPE="t3.small"

# Controller public IP -- the only source allowed SSH, and the only
# non-node source allowed the dynomite ports (so the workload driver
# can reach the cluster). Never 0.0.0.0/0.
MY_IP="$(curl -s -m 10 https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]')"
[ -z "$MY_IP" ] && { echo "could not determine controller public IP" >&2; exit 1; }

# region -> DC name, AMI, AZ list
declare -A DC=( [us-east-2]=dc-use2 [us-west-2]=dc-usw2 )
declare -A AMI=( [us-east-2]=ami-0772d6acfbccb1275 [us-west-2]=ami-05f8addf6121a3888 )
declare -A AZS=( [us-east-2]="us-east-2a us-east-2b us-east-2c" [us-west-2]="us-west-2a us-west-2b us-west-2c" )
REGIONS=(us-east-2 us-west-2)

# Ports (dynomite): client 8102, dnode/peer 8101, stats 22222,
# riak pbc 8087, riak http 8098.
CLIENT=8102; DNODE=8101; STATS=22222; PBC=8087; HTTP=8098

aws() { command aws --profile "$PROFILE" "$@"; }
log() { echo "[deploy $(date -u +%H:%M:%S)] $*" >&2; }

sg_ingress() {
  # Allowlist-only ingress. No 0.0.0.0/0 anywhere (that was the
  # policy violation on the prior account). At creation we open the
  # dynomite ports + ssh ONLY to the controller's public IP; each
  # cluster node's /32 is added later by `allowlist_peers` once the
  # public IPs are known.
  local region=$1 sg=$2
  for p in 22 $CLIENT $DNODE $STATS $PBC $HTTP; do
    aws ec2 authorize-security-group-ingress --region "$region" \
      --group-id "$sg" --protocol tcp --port "$p" --cidr "${MY_IP}/32" >/dev/null 2>&1 || true
  done
}

allowlist_peers() {
  # After all node public IPs are known, add each node's /32 to
  # every region's SG on the dynomite ports so peers reach each
  # other over the WAN -- a tight allowlist, never 0.0.0.0/0.
  log "allowlisting ${#PEER_IPS[@]} peer /32s on all SGs (no world-open rules)"
  for region in "${REGIONS[@]}"; do
    local sg; sg=$(aws ec2 describe-security-groups --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[0].GroupId' --output text)
    for ip in "${PEER_IPS[@]}"; do
      for p in $CLIENT $DNODE $STATS $PBC $HTTP; do
        aws ec2 authorize-security-group-ingress --region "$region" \
          --group-id "$sg" --protocol tcp --port "$p" --cidr "${ip}/32" >/dev/null 2>&1 || true
      done
    done
  done
}

up() {
  : > "$STATE"
  log "RUN_ID=$RUN_ID  provisioning 6 nodes across ${REGIONS[*]}"
  # Key pair (per region), security group (default VPC), instances.
  for region in "${REGIONS[@]}"; do
    local keyname="${RUN_ID}-key"
    local keyfile="/tmp/${RUN_ID}-${region}.pem"
    aws ec2 create-key-pair --region "$region" --key-name "$keyname" \
      --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'KeyMaterial' --output text > "$keyfile" 2>/dev/null
    chmod 600 "$keyfile"
    local vpc; vpc=$(aws ec2 describe-vpcs --region "$region" \
      --query 'Vpcs[?IsDefault==`true`].VpcId' --output text)
    local sg; sg=$(aws ec2 create-security-group --region "$region" \
      --group-name "${RUN_ID}-sg" --description "dynomite dist qual $RUN_ID" \
      --vpc-id "$vpc" \
      --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'GroupId' --output text)
    sg_ingress "$region" "$sg"
    log "$region: key=$keyname sg=$sg vpc=$vpc"
    local i=0
    for az in ${AZS[$region]}; do
      i=$((i+1))
      local subnet; subnet=$(aws ec2 describe-subnets --region "$region" \
        --filters "Name=availability-zone,Values=$az" "Name=default-for-az,Values=true" \
        --query 'Subnets[0].SubnetId' --output text)
      local iid; iid=$(aws ec2 run-instances --region "$region" \
        --image-id "${AMI[$region]}" --instance-type "$INSTANCE_TYPE" \
        --key-name "$keyname" --security-group-ids "$sg" --subnet-id "$subnet" \
        --associate-public-ip-address \
        --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${DC[$region]}-n${i}}]" \
        --query 'Instances[0].InstanceId' --output text)
      echo "$region $az ${DC[$region]} n${i} $iid" >> "$STATE"
      log "launched $iid ($az, ${DC[$region]}-n${i})"
    done
  done
  log "waiting for instances to reach running + public IPs"
  # Resolve public/private IPs once running.
  : > "${STATE}.ips"
  while read -r region az dc n iid; do
    aws ec2 wait instance-running --region "$region" --instance-ids "$iid"
    local pub priv
    pub=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
    priv=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
      --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
    echo "$region $az $dc $n $iid $pub $priv" >> "${STATE}.ips"
    log "$iid running pub=$pub priv=$priv"
  done < "$STATE"
  # Collect every node's public /32 and allowlist them on all SGs
  # so peers reach each other over the WAN -- tight allowlist only.
  PEER_IPS=()
  while read -r region az dc n iid pub priv; do
    [ -n "$pub" ] && [ "$pub" != "None" ] && PEER_IPS+=("$pub")
  done < "${STATE}.ips"
  allowlist_peers
  log "up complete; IPs in ${STATE}.ips (SGs: ssh from ${MY_IP}/32 only; dynomite ports from controller + ${#PEER_IPS[@]} node /32s)"
}

status() {
  echo "== tagged resources for $RUN_ID =="
  for region in "${REGIONS[@]}"; do
    echo "-- $region instances --"
    aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].[InstanceId,State.Name,PublicIpAddress,Tags[?Key==`Name`]|[0].Value]' \
      --output text 2>/dev/null || true
  done
}

down() {
  log "TEARDOWN $RUN_ID: terminating instances, deleting SGs + keypairs"
  for region in "${REGIONS[@]}"; do
    local ids; ids=$(aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    if [ -n "$ids" ]; then
      log "$region terminating: $ids"
      aws ec2 terminate-instances --region "$region" --instance-ids $ids >/dev/null 2>&1 || true
      aws ec2 wait instance-terminated --region "$region" --instance-ids $ids 2>/dev/null || true
    fi
    # SGs (only deletable after instances gone).
    for sg in $(aws ec2 describe-security-groups --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[].GroupId' --output text 2>/dev/null); do
      aws ec2 delete-security-group --region "$region" --group-id "$sg" >/dev/null 2>&1 \
        && log "$region deleted sg $sg" || log "$region sg $sg delete deferred"
    done
    # Key pairs.
    for kp in $(aws ec2 describe-key-pairs --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'KeyPairs[].KeyName' --output text 2>/dev/null); do
      aws ec2 delete-key-pair --region "$region" --key-name "$kp" >/dev/null 2>&1 \
        && log "$region deleted keypair $kp" || true
    done
    rm -f "/tmp/${RUN_ID}-${region}.pem"
  done
  log "teardown complete"
}

case "${1:-}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 {up|down|status}" >&2; exit 1 ;;
esac
