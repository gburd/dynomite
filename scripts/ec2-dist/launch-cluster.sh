#!/usr/bin/env bash
# Generate per-node dyniak configs for the distributed EC2 cluster
# and launch dynomited on every node. Reads the IP map written by
# deploy.sh (<RUN_ID>.state.ips: region az dc n iid pub priv).
#
# Ring: 6 nodes, evenly-spaced u32 tokens. Each node advertises its
# public IP on the dnode (peer) port so gossip and cross-region
# replication traverse the real WAN. rack = the AZ; dc = the region's
# datacenter name. Each node's dyn_seeds lists the other five.
set -euo pipefail

RUN_ID="$(cat /tmp/dyn-ec2-runid | sed 's/RUN_ID=//')"
IPS="/tmp/${RUN_ID}.state.ips"
CLIENT=8102; DNODE=8101; STATS=22222; PBC=8087; HTTP=8098

dsh() { local ip=$1 region=$2; shift 2; SSH_AUTH_SOCK="" ssh -i "/tmp/${RUN_ID}-${region}.pem" \
  -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=15 ec2-user@"$ip" "$@"; }

# Assign evenly-spaced ring tokens (u32) in file order.
mapfile -t NODES < "$IPS"
N=${#NODES[@]}
declare -a TOK
for i in "${!NODES[@]}"; do
  # spread across 0 .. 4294967295
  TOK[$i]=$(( i * 4294967296 / N ))
done

# Build the global seed list: "pub:DNODE:rack:dc:token" per node.
SEEDS_ALL=()
for i in "${!NODES[@]}"; do
  read -r region az dc n iid pub priv <<< "${NODES[$i]}"
  SEEDS_ALL+=("${pub}:${DNODE}:${az}:${dc}:${TOK[$i]}")
done

# For each node: write a config listing the OTHER five as seeds,
# push it, and launch dynomited.
for i in "${!NODES[@]}"; do
  read -r region az dc n iid pub priv <<< "${NODES[$i]}"
  tok=${TOK[$i]}
  # seeds = all except self
  seed_lines=""
  for j in "${!NODES[@]}"; do
    [ "$j" -eq "$i" ] && continue
    seed_lines+="    - ${SEEDS_ALL[$j]}"$'\n'
  done
  # dyniak node: binds on 0.0.0.0 so the public IP is reachable;
  # advertises its public IP to peers via the seed the peers hold.
  cfg=$(cat <<YML
dyn_o_mite:
  listen: 0.0.0.0:${CLIENT}
  dyn_listen: 0.0.0.0:${DNODE}
  stats_listen: 0.0.0.0:${STATS}
  servers:
    - 127.0.0.1:9999:1
  tokens: '${tok}'
  datacenter: ${dc}
  rack: ${az}
  data_store: 2
  noxu_path: /home/ec2-user/noxu
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  enable_gossip: true
  gos_interval: 1000
  timeout: 5000
  dyn_seed_provider: simple_provider
  dyn_seeds:
${seed_lines}  riak:
    pbc_listen: 0.0.0.0:${PBC}
    http_listen: 0.0.0.0:${HTTP}
YML
)
  echo "$cfg" > "/tmp/${RUN_ID}-cfg-${dc}-${n}.yml"
  # push config + launch
  SSH_AUTH_SOCK="" scp -i "/tmp/${RUN_ID}-${region}.pem" -o StrictHostKeyChecking=no \
    -o IdentitiesOnly=yes -o IdentityAgent=none \
    "/tmp/${RUN_ID}-cfg-${dc}-${n}.yml" ec2-user@"$pub":~/dynomite.yml >/dev/null 2>&1
  dsh "$pub" "$region" 'pkill -9 dynomited 2>/dev/null; rm -rf ~/noxu; chmod +x ~/dynomited; nohup ~/dynomited -c ~/dynomite.yml > ~/dynomited.log 2>&1 < /dev/null & sleep 1; echo "launched on '"$dc-$n"' pid $(pgrep -f dynomited | head -1)"' 2>&1 | tail -1
done
echo "all nodes launched; token ring: ${TOK[*]}"
