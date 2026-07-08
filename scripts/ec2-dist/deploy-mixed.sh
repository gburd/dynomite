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
declare -A AZS=(
  [us-east-1]="us-east-1a us-east-1b us-east-1c"
  [us-west-2]="us-west-2a us-west-2b us-west-2c"
  [eu-central-1]="eu-central-1a eu-central-1b eu-central-1c"
  [ap-northeast-1]="ap-northeast-1a ap-northeast-1c ap-northeast-1d"
  [sa-east-1]="sa-east-1a sa-east-1b sa-east-1c"
)
REGIONS=(us-east-1 us-west-2 eu-central-1 ap-northeast-1 sa-east-1)
# Extra region for the load-driven migration target (3rd Graviton
# region), added after the base tables are declared so `set -u` does not
# trip on an assignment to an undeclared associative-array element.
# The AMI is resolved on demand in add_region.
DC[eu-west-1]=dc-euw1
AZS[eu-west-1]="eu-west-1a eu-west-1b eu-west-1c"
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

# A managed prefix list holds many /32 entries but is referenced by a
# single SG rule per port-group. With 45 mesh nodes, per-IP /32 rules
# (45 x 3 = 135) blow past the 60-rule SG quota and later nodes are
# silently dropped, breaking the cross-region dnode mesh. The prefix
# list keeps the SG at 3 mesh rules regardless of node count.
ensure_prefix_list() { # ensure_prefix_list <region> -> plId on stdout
  local region=$1 pl
  pl=$(aws ec2 describe-managed-prefix-lists --region "$region" \
    --filters "Name=tag:$TAG,Values=$RUN_ID" \
    --query 'PrefixLists[0].PrefixListId' --output text 2>/dev/null)
  if [ -z "$pl" ] || [ "$pl" = "None" ]; then
    pl=$(aws ec2 create-managed-prefix-list --region "$region" \
      --prefix-list-name "${RUN_ID}-mesh" --address-family IPv4 \
      --max-entries 100 \
      --tag-specifications "ResourceType=prefix-list,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'PrefixList.PrefixListId' --output text 2>/dev/null)
  fi
  echo "$pl"
}

# Authorize an SG against its region's mesh prefix list (idempotent).
authorize_prefix_list() { # authorize_prefix_list <region> <sg> <plId>
  local region=$1 sg=$2 pl=$3 pg
  for pg in "22" "8087-9102" "22222-22223"; do
    local fp=${pg%%-*} tp=${pg##*-}
    aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" \
      --ip-permissions "IpProtocol=tcp,FromPort=${fp},ToPort=${tp},PrefixListIds=[{PrefixListId=${pl}}]" \
      >/dev/null 2>&1 || true
  done
}

# Add an IP as a /32 to every region's mesh prefix list. The prefix
# list must be modified with its current version; retry on the version
# conflict that concurrent adds can cause.
add_ip_to_prefix_lists() { # add_ip_to_prefix_lists <ip>
  local ip=$1 region pl ver
  for region in "${REGIONS[@]}"; do
    pl=$(ensure_prefix_list "$region")
    [ -z "$pl" ] || [ "$pl" = "None" ] && continue
    local attempt
    for attempt in 1 2 3 4 5; do
      ver=$(aws ec2 describe-managed-prefix-lists --region "$region" \
        --prefix-list-ids "$pl" --query 'PrefixLists[0].Version' --output text 2>/dev/null)
      if aws ec2 modify-managed-prefix-list --region "$region" \
        --prefix-list-id "$pl" --current-version "$ver" \
        --add-entries "Cidr=${ip}/32,Description=mesh" >/dev/null 2>&1; then
        break
      fi
      sleep 2
    done
  done
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
  # Wire the SG to the region's mesh prefix list (idempotent) so every
  # mesh node is reachable via 3 rules instead of one /32 rule each.
  local pl; pl=$(ensure_prefix_list "$region")
  [ -n "$pl" ] && [ "$pl" != "None" ] && authorize_prefix_list "$region" "$sg" "$pl"
  echo "$sg"
}

allowlist_ip_everywhere() {
  local ip=$1
  # Mesh reachability comes entirely from the per-region prefix list
  # (one entry per node, referenced by 3 SG rules). Do NOT add node IPs
  # as individual SG /32 rules -- 45 nodes x 3 port-groups = 135 rules
  # blows the 60-rule SG quota and later nodes get silently dropped,
  # which is exactly the bug that broke the DC_ONE mesh.
  add_ip_to_prefix_lists "$ip"
}

# Launch an entire rack (NODES_PER_RACK instances) in ONE run-instances
# --count call, then tag each and emit one state row per node. Batching
# cuts the run-instances API-call volume by NODES_PER_RACK, which avoids
# the RequestLimitExceeded throttling that per-node launches hit. Emits
# rows on stdout; returns nonzero only if the batch call itself never
# succeeds.
launch_rack() {
  local region=$1 az=$2 dc=$3 rack=$4
  local sg; sg=$(ensure_region_infra "$region")
  local subnet; subnet=$(aws ec2 describe-subnets --region "$region" \
    --filters "Name=availability-zone,Values=$az" "Name=default-for-az,Values=true" \
    --query 'Subnets[0].SubnetId' --output text)
  local arch=${ARCH[$region]}
  local itype=${ITYPE[$arch]}
  local ami; [ "$arch" = x86 ] && ami=${AMI_X86[$region]} || ami=${AMI_ARM[$region]}
  local ids="" attempt
  for attempt in 1 2 3 4 5 6; do
    ids=$(aws ec2 run-instances --region "$region" \
      --image-id "$ami" --instance-type "$itype" --count "$NODES_PER_RACK" \
      --key-name "${RUN_ID}-key" --security-group-ids "$sg" --subnet-id "$subnet" \
      --associate-public-ip-address \
      --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=arch,Value=${arch}},{Key=Name,Value=${dc}-${rack}}]" \
      --query 'Instances[].InstanceId' --output text 2>/dev/null)
    [ -n "$ids" ] && break
    log "run-instances $dc-$rack (count $NODES_PER_RACK) attempt $attempt empty (throttle?); backing off"
    sleep $((attempt * 8))
  done
  [ -z "$ids" ] && { log "FAILED to launch rack $dc-$rack after retries"; return 1; }
  # name each instance n1..nK and wait for it running + IPs
  local idx=0 iid
  for iid in $ids; do
    idx=$((idx+1))
    aws ec2 create-tags --region "$region" --resources "$iid" \
      --tags "Key=Name,Value=${dc}-${rack}-n${idx}" >/dev/null 2>&1
  done
  aws ec2 wait instance-running --region "$region" --instance-ids $ids 2>/dev/null
  idx=0
  for iid in $ids; do
    idx=$((idx+1))
    local pub="" priv="" a
    for a in 1 2 3 4 5 6; do
      pub=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
      priv=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null)
      [ -n "$pub" ] && [ "$pub" != None ] && [ -n "$priv" ] && [ "$priv" != None ] && break
      sleep 5
    done
    if [ -z "$pub" ] || [ "$pub" = None ]; then
      log "no public IP for $iid ($dc-$rack-n${idx}); terminating"
      aws ec2 terminate-instances --region "$region" --instance-ids "$iid" >/dev/null 2>&1
      continue
    fi
    allowlist_ip_everywhere "$pub"
    echo "$region $az $dc $rack n${idx} $arch $itype $iid $pub $priv"
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
  # run-instances with retry: AWS throttles rapid sequential launches
  # (RequestLimitExceeded), returning an empty InstanceId. Retry with
  # backoff so we never write a phantom (blank-IP) state row.
  local iid="" attempt
  for attempt in 1 2 3 4 5; do
    iid=$(aws ec2 run-instances --region "$region" \
      --image-id "$ami" --instance-type "$itype" \
      --key-name "${RUN_ID}-key" --security-group-ids "$sg" --subnet-id "$subnet" \
      --associate-public-ip-address \
      --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${dc}-${rack}-${node}},{Key=arch,Value=${arch}}]" \
      --query 'Instances[0].InstanceId' --output text 2>/dev/null)
    [ -n "$iid" ] && [ "$iid" != "None" ] && break
    log "run-instances $dc-$rack-$node attempt $attempt returned empty (throttle?); backing off"
    sleep $((attempt * 5))
  done
  if [ -z "$iid" ] || [ "$iid" = "None" ]; then
    log "FAILED to launch $dc-$rack-$node after retries; skipping (no state row)"
    return 1
  fi
  aws ec2 wait instance-running --region "$region" --instance-ids "$iid" 2>/dev/null
  # pub/priv can lag instance-running by a beat (eventual consistency);
  # retry until both resolve.
  local pub="" priv=""
  for attempt in 1 2 3 4 5 6; do
    pub=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
      --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
    priv=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
      --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null)
    [ -n "$pub" ] && [ "$pub" != "None" ] && [ -n "$priv" ] && [ "$priv" != "None" ] && break
    sleep 5
  done
  if [ -z "$pub" ] || [ "$pub" = "None" ]; then
    log "FAILED to get public IP for $iid ($dc-$rack-$node); terminating + skipping"
    aws ec2 terminate-instances --region "$region" --instance-ids "$iid" >/dev/null 2>&1
    return 1
  fi
  allowlist_ip_everywhere "$pub"
  # State row: region az dc rack node arch itype iid pub priv
  echo "$region $az $dc $rack $node $arch $itype $iid $pub $priv"
}

up() {
  : > "$STATE"
  local region
  log "RUN_ID=$RUN_ID provisioning ${#REGIONS[@]} regions x $((RACKS*NODES_PER_RACK)) nodes (3 racks x 3, batched per rack)"
  for region in "${REGIONS[@]}"; do
    local azs=(${AZS[$region]})
    local r
    for r in $(seq 1 $RACKS); do
      local az=${azs[$(( (r-1) % ${#azs[@]} ))]}
      local rows
      if rows=$(launch_rack "$region" "$az" "${DC[$region]}" "r${r}"); then
        echo "$rows" >> "$STATE"
        log "up: rack ${DC[$region]}-r${r} ($(echo "$rows" | grep -c .) nodes)"
      else
        log "up: SKIP rack $region r${r} (launch failed)"
      fi
    done
  done
  log "up complete; $(grep -c . "$STATE") nodes; state in $STATE"
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
    # Prefix lists must be deleted AFTER the SGs that reference them.
    local pl
    for pl in $(aws ec2 describe-managed-prefix-lists --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'PrefixLists[].PrefixListId' --output text 2>/dev/null); do
      aws ec2 delete-managed-prefix-list --region "$region" --prefix-list-id "$pl" >/dev/null 2>&1 || true
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
  local azs=(${AZS[$region]}) r
  for r in $(seq 1 $RACKS); do
    local az=${azs[$(( (r-1) % ${#azs[@]} ))]}
    local rows
    if rows=$(launch_rack "$region" "$az" "${DC[$region]}" "r${r}"); then
      echo "$rows" >> "$STATE"
      log "add: rack ${DC[$region]}-r${r} ($(echo "$rows" | grep -c .) nodes)"
    else
      log "add: SKIP rack $region r${r} (launch failed)"
    fi
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
