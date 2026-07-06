#!/usr/bin/env bash
# Side-by-side differential deployment: run BOTH the C Netflix dynomite
# reference and the Rust dynomited on every node, with identical
# topology, fronting the same local backend, so a driver can send the
# same op to both and compare replies.
#
# Port plan (per node):
#   C dynomite : client 8102, dnode 8101, stats 22222
#   Rust       : client 9102, dnode 9101, stats 22223
# Both front the same backend (valkey 6379 / memcached 11211). The two
# proxy meshes are independent (separate dnode planes) but topology-
# identical: same racks, same tokens, same consistency, same n_val.
#
# Binaries staged on every node: ~/dynomite-c (C), ~/dynomited (Rust).
#
# MODE in {valkey, memcache}. (dyniak has no C equivalent; the C
# reference is redis/memcache only, so the differential covers those.)
#
# Usage: setup-differential.sh <valkey|memcache>
set -euo pipefail

MODE="${1:?usage: setup-differential.sh <valkey|memcache>}"
RUN_ID="$(cat /tmp/dyn-ec2-runid | sed 's/RUN_ID=//')"
IPS="/tmp/${RUN_ID}.state.ips"

# Each proxy gets its OWN backend instance so the two independent
# rings never cross-contaminate (a key routed differently by the two
# rings must not land in a shared store). C uses the standard backend
# port; Rust uses a second instance on the next port.
case "$MODE" in
  valkey)   DS=0 ; C_BACKEND="127.0.0.1:6379:1" ; R_BACKEND="127.0.0.1:6380:1" ;;
  memcache) DS=1 ; C_BACKEND="127.0.0.1:11211:1" ; R_BACKEND="127.0.0.1:11212:1" ;;
  *) echo "bad MODE $MODE" >&2; exit 1 ;;
esac

# C ports / Rust ports.
C_CLIENT=8102; C_DNODE=8101; C_STATS=22222
R_CLIENT=9102; R_DNODE=9101; R_STATS=22223

dsh() { local ip=$1 region=$2; shift 2; SSH_AUTH_SOCK="" ssh -i "/tmp/${RUN_ID}-${region}.pem" \
  -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none -o ConnectTimeout=15 ec2-user@"$ip" "$@"; }
scp_to() { local ip=$1 region=$2 src=$3 dst=$4; SSH_AUTH_SOCK="" scp -i "/tmp/${RUN_ID}-${region}.pem" \
  -o StrictHostKeyChecking=no -o IdentitiesOnly=yes -o IdentityAgent=none "$src" ec2-user@"$ip":"$dst" >/dev/null 2>&1; }

mapfile -t NODES < "$IPS"

# One rack per node, token 0 (full replica; n_val = nodes-per-DC),
# matching setup-mode.sh. Same for both C and Rust.
declare -a RACK
declare -A DC_SEEN
for i in "${!NODES[@]}"; do
  read -r _r _az d _n _i _p _pv <<< "${NODES[$i]}"
  RACK[$i]="${d}-r$(( ${DC_SEEN[$d]:-0} + 1 ))"
  DC_SEEN[$d]=$(( ${DC_SEEN[$d]:-0} + 1 ))
done

# Seed lists for each mesh: "pub:DNODE:rack:dc:token" (token 0).
C_SEEDS=(); R_SEEDS=()
for i in "${!NODES[@]}"; do
  read -r region az dc n iid pub priv <<< "${NODES[$i]}"
  C_SEEDS+=("${pub}:${C_DNODE}:${RACK[$i]}:${dc}:0")
  R_SEEDS+=("${pub}:${R_DNODE}:${RACK[$i]}:${dc}:0")
done

backend_launch() {
  # Start TWO independent backend instances: one for C (standard
  # port) and one for Rust (next port). Each proxy fronts only its
  # own, so the two rings never share a store.
  case "$MODE" in
    valkey)   echo 'sudo dnf install -y -q valkey 2>/dev/null || sudo dnf install -y -q redis6 2>/dev/null || true; VS=$(command -v valkey-server || command -v redis6-server || command -v redis-server); "$VS" --daemonize yes --bind 127.0.0.1 --port 6379 2>/dev/null; "$VS" --daemonize yes --bind 127.0.0.1 --port 6380 2>/dev/null; sleep 1' ;;
    memcache) echo 'sudo dnf install -y -q memcached 2>/dev/null || true; memcached -d -l 127.0.0.1 -p 11211 -u ec2-user 2>/dev/null; memcached -d -l 127.0.0.1 -p 11212 -u ec2-user 2>/dev/null; sleep 1' ;;
  esac
}

# Consistency level (default DC_ONE; override with DYN_CONSISTENCY).
CONS="${DYN_CONSISTENCY:-DC_ONE}"

for i in "${!NODES[@]}"; do
  read -r region az dc n iid pub priv <<< "${NODES[$i]}"
  rack=${RACK[$i]}

  c_seed_lines=""; r_seed_lines=""
  for j in "${!NODES[@]}"; do
    [ "$j" -eq "$i" ] && continue
    c_seed_lines+="    - ${C_SEEDS[$j]}"$'\n'
    r_seed_lines+="    - ${R_SEEDS[$j]}"$'\n'
  done

  # C config: dyn_o_mite block, C ports.
  c_cfg=$(cat <<YML
dyn_o_mite:
  listen: 0.0.0.0:${C_CLIENT}
  dyn_listen: 0.0.0.0:${C_DNODE}
  stats_listen: 0.0.0.0:${C_STATS}
  servers:
    - ${C_BACKEND}
  tokens: '0'
  datacenter: ${dc}
  rack: ${rack}
  read_consistency: ${CONS}
  write_consistency: ${CONS}
  data_store: ${DS}
  dyn_seed_provider: simple_provider
  dyn_seeds:
${c_seed_lines}
YML
)
  # Rust config: same topology, Rust ports. Rust needs DYN_ADVERTISE_ADDR
  # for the wildcard bind (C derives its advertised addr the same way it
  # always has -- from the seed match, so no env needed for C).
  r_cfg=$(cat <<YML
dyn_o_mite:
  listen: 0.0.0.0:${R_CLIENT}
  dyn_listen: 0.0.0.0:${R_DNODE}
  stats_listen: 0.0.0.0:${R_STATS}
  servers:
    - ${R_BACKEND}
  tokens: '0'
  datacenter: ${dc}
  rack: ${rack}
  read_consistency: ${CONS}
  write_consistency: ${CONS}
  enable_gossip: true
  gos_interval: 1000
  data_store: ${DS}
  dyn_seed_provider: simple_provider
  dyn_seeds:
${r_seed_lines}
YML
)
  echo "$c_cfg" > "/tmp/${RUN_ID}-cdiff-c-${dc}-${n}.yml"
  echo "$r_cfg" > "/tmp/${RUN_ID}-cdiff-r-${dc}-${n}.yml"
  scp_to "$pub" "$region" "/tmp/${RUN_ID}-cdiff-c-${dc}-${n}.yml" '~/dynomite-c.yml'
  scp_to "$pub" "$region" "/tmp/${RUN_ID}-cdiff-r-${dc}-${n}.yml" '~/dynomite-r.yml'
  binst=$(backend_launch)
  # Kill any prior instances, free ports, launch both.
  dsh "$pub" "$region" "\
    sudo pkill -9 -x dynomite 2>/dev/null; sudo pkill -9 -x dynomited 2>/dev/null; \
    sudo fuser -k ${C_DNODE}/tcp ${C_CLIENT}/tcp ${C_STATS}/tcp ${R_DNODE}/tcp ${R_CLIENT}/tcp ${R_STATS}/tcp 2>/dev/null; \
    sleep 4; \
    ${binst}; \
    chmod +x ~/dynomite-c ~/dynomited; \
    nohup ~/dynomite-c -c ~/dynomite-c.yml > ~/dynomite-c.log 2>&1 </dev/null & \
    DYN_ADVERTISE_ADDR=${pub} nohup ~/dynomited -c ~/dynomite-r.yml > ~/dynomite-r.log 2>&1 </dev/null & \
    sleep 2; echo '${dc}-${n} C=\$(pgrep -x dynomite|head -1) R=\$(pgrep -x dynomited|head -1)'" 2>&1 | tail -1
done
echo "differential mode=$MODE consistency=$CONS launched on ${#NODES[@]} nodes (C:${C_CLIENT} Rust:${R_CLIENT})"
