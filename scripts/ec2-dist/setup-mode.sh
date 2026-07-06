#!/usr/bin/env bash
# Per-node setup + config generation for the global cluster, per mode.
#
# For each node (from <RUN_ID>.state.ips) this:
#   * installs the local backend the mode needs
#       valkey  -> valkey (or redis6) on 127.0.0.1:6379
#       memcache-> memcached on 127.0.0.1:11211
#       dyniak  -> none (in-process transactional noxu)
#   * writes a dynomite config with the whole-ring seed list
#   * (re)launches dynomited
#
# MODE in {valkey, memcache, dyniak}. The dynomited binary must
# already be staged at ~/dynomited on every node.
#
# Usage: setup-mode.sh <MODE>
set -euo pipefail

MODE="${1:?usage: setup-mode.sh <valkey|memcache|dyniak>}"
RUN_ID="$(cat /tmp/dyn-ec2-runid | sed 's/RUN_ID=//')"
IPS="/tmp/${RUN_ID}.state.ips"
CLIENT=8102; DNODE=8101; STATS=22222; PBC=8087; HTTP=8098

case "$MODE" in
  valkey)   DS=0 ;;
  redis)    DS=0; MODE=valkey ;;
  memcache) DS=1 ;;
  dyniak)   DS=2 ;;
  *) echo "bad MODE $MODE" >&2; exit 1 ;;
esac

dsh() { local ip=$1 region=$2; shift 2; SSH_AUTH_SOCK="" ssh -i "/tmp/${RUN_ID}-${region}.pem" \
  -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=15 ec2-user@"$ip" "$@"; }
scp_to() { local ip=$1 region=$2 src=$3 dst=$4; SSH_AUTH_SOCK="" scp -i "/tmp/${RUN_ID}-${region}.pem" \
  -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none "$src" ec2-user@"$ip":"$dst" >/dev/null 2>&1; }

# Ring tokens (evenly spaced u32) in file order.
mapfile -t NODES < "$IPS"
N=${#NODES[@]}
declare -a TOK
# Dynomite token model: each datacenter independently covers the ENTIRE
# u32 ring, so every DC holds a full copy of the keyspace. A node's token
# is its index WITHIN its DC times (2^32 / nodes-in-that-DC), not its
# global index. This mirrors real Dynomite, where DC_ONE writes fan out
# to the local-DC owner of a key's token and replicate to peer DCs.
declare -A DC_COUNT DC_SEEN
for i in "${!NODES[@]}"; do
  read -r _r _az d _n _i _p _pv <<< "${NODES[$i]}"
  DC_COUNT[$d]=$(( ${DC_COUNT[$d]:-0} + 1 ))
done
for i in "${!NODES[@]}"; do
  read -r _r _az d _n _i _p _pv <<< "${NODES[$i]}"
  idx=${DC_SEEN[$d]:-0}
  TOK[$i]=$(( idx * 4294967296 / ${DC_COUNT[$d]} ))
  DC_SEEN[$d]=$(( idx + 1 ))
done

# Global seed list "pub:DNODE:rack:dc:token".
SEEDS_ALL=()
for i in "${!NODES[@]}"; do
  read -r region az dc n iid pub priv <<< "${NODES[$i]}"
  SEEDS_ALL+=("${pub}:${DNODE}:${az}:${dc}:${TOK[$i]}")
done

backend_install() {
  # Best-effort install of the local backend for the mode.
  case "$MODE" in
    valkey)
      echo 'sudo dnf install -y -q valkey 2>/dev/null || sudo dnf install -y -q redis6 2>/dev/null || true
            (valkey-server --daemonize yes --bind 127.0.0.1 --port 6379 2>/dev/null || redis6-server --daemonize yes --bind 127.0.0.1 --port 6379 2>/dev/null || redis-server --daemonize yes --bind 127.0.0.1 --port 6379 2>/dev/null) ;
            sleep 1' ;;
    memcache)
      echo 'sudo dnf install -y -q memcached 2>/dev/null || true; memcached -d -l 127.0.0.1 -p 11211 -u ec2-user 2>/dev/null || sudo systemctl start memcached 2>/dev/null || true; sleep 1' ;;
    dyniak) echo 'true' ;;
  esac
}

for i in "${!NODES[@]}"; do
  read -r region az dc n iid pub priv <<< "${NODES[$i]}"
  tok=${TOK[$i]}
  seed_lines=""
  for j in "${!NODES[@]}"; do
    [ "$j" -eq "$i" ] && continue
    seed_lines+="    - ${SEEDS_ALL[$j]}"$'\n'
  done

  if [ "$MODE" = "dyniak" ]; then
    tail_cfg=$(cat <<YML
  data_store: 2
  noxu_path: /home/ec2-user/noxu
  riak:
    pbc_listen: 0.0.0.0:${PBC}
    http_listen: 0.0.0.0:${HTTP}
YML
)
    servers_line="    - 127.0.0.1:9999:1"
  elif [ "$MODE" = "valkey" ]; then
    tail_cfg="  data_store: 0"
    servers_line="    - 127.0.0.1:6379:1"
  else
    tail_cfg="  data_store: 1"
    servers_line="    - 127.0.0.1:11211:1"
  fi

  cfg=$(cat <<YML
dyn_o_mite:
  listen: 0.0.0.0:${CLIENT}
  dyn_listen: 0.0.0.0:${DNODE}
  stats_listen: 0.0.0.0:${STATS}
  servers:
${servers_line}
  tokens: '${tok}'
  datacenter: ${dc}
  rack: ${az}
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  enable_gossip: true
  gos_interval: 1000
  timeout: 5000
  dyn_seed_provider: simple_provider
  dyn_seeds:
${seed_lines}${tail_cfg}
YML
)
  echo "$cfg" > "/tmp/${RUN_ID}-cfg-${MODE}-${dc}-${n}.yml"
  scp_to "$pub" "$region" "/tmp/${RUN_ID}-cfg-${MODE}-${dc}-${n}.yml" '~/dynomite.yml'
  binst=$(backend_install)
  dsh "$pub" "$region" "sudo pkill -9 -x dynomited 2>/dev/null; for i in 1 2 3 4 5 6 7 8 9 10; do ss -tln | grep -qE ':8101|:8102|:8098|:8087' || break; sleep 1; done; find ~/noxu -mindepth 1 -delete 2>/dev/null; rmdir ~/noxu 2>/dev/null; ${binst}; chmod +x ~/dynomited; DYN_ADVERTISE_ADDR=${pub} nohup ~/dynomited -c ~/dynomite.yml > ~/dynomited.log 2>&1 < /dev/null & sleep 2; echo '${dc}-${n} '\$(pgrep -x dynomited|head -1)" 2>&1 | tail -1
done
echo "mode=$MODE launched on $N nodes; ring tokens spaced across u32"
