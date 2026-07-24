#!/usr/bin/env bash
# Phase implementations for chaos-dyniak.sh. Sourced by it; relies on
# the config, helpers (aws/log/nsh/nscp), and phase_done markers defined
# there. Not executed standalone.

# --- infra: per-region SG that references a mesh prefix list ----------
# A prefix list holds every node+controller /32 and is referenced by one
# consolidated SG rule spanning ssh + all data ports (22-22223), so the
# SG stays at a single mesh rule regardless of node count -- well under
# the 60-rule quota.
ensure_region_infra() { # <region> -> sg on stdout
  local region=$1 sg pl vpc
  pl=$(aws ec2 describe-managed-prefix-lists --region "$region" \
    --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'PrefixLists[0].PrefixListId' --output text 2>/dev/null)
  if [ -z "$pl" ] || [ "$pl" = "None" ]; then
    pl=$(aws ec2 create-managed-prefix-list --region "$region" \
      --prefix-list-name "${RUN_ID}-mesh" --address-family IPv4 --max-entries 40 \
      --tag-specifications "ResourceType=prefix-list,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'PrefixList.PrefixListId' --output text 2>/dev/null)
    # Seed with the controller IP so we can ssh in immediately.
    local ver
    ver=$(aws ec2 describe-managed-prefix-lists --region "$region" --prefix-list-ids "$pl" \
      --query 'PrefixLists[0].Version' --output text 2>/dev/null)
    aws ec2 modify-managed-prefix-list --region "$region" --prefix-list-id "$pl" \
      --current-version "$ver" --add-entries "Cidr=${MY_IP}/32,Description=controller" >/dev/null 2>&1
    aws ec2 wait managed-prefix-list-modified --region "$region" --prefix-list-id "$pl" 2>/dev/null || sleep 5
  fi
  sg=$(aws ec2 describe-security-groups --region "$region" \
    --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
  if [ -z "$sg" ] || [ "$sg" = "None" ]; then
    vpc=$(aws ec2 describe-vpcs --region "$region" --query 'Vpcs[?IsDefault==`true`].VpcId' --output text)
    sg=$(aws ec2 create-security-group --region "$region" --group-name "${RUN_ID}-sg" \
      --description "dyniak chaos $RUN_ID" --vpc-id "$vpc" \
      --tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG,Value=$RUN_ID}]" \
      --query 'GroupId' --output text)
    # One consolidated rule (ssh + data ports) referencing the mesh pl.
    aws ec2 authorize-security-group-ingress --region "$region" --group-id "$sg" \
      --ip-permissions "IpProtocol=tcp,FromPort=22,ToPort=22223,PrefixListIds=[{PrefixListId=${pl}}]" \
      >/dev/null 2>&1
  fi
  echo "$sg"
}

# Add an IP to every region's mesh prefix list (retry on version race).
allowlist_everywhere() {
  local ip=$1 region pl ver attempt
  for region in "${REGIONS[@]}"; do
    pl=$(aws ec2 describe-managed-prefix-lists --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'PrefixLists[0].PrefixListId' --output text 2>/dev/null)
    [ -z "$pl" ] || [ "$pl" = "None" ] && continue
    for attempt in 1 2 3 4 5; do
      ver=$(aws ec2 describe-managed-prefix-lists --region "$region" --prefix-list-ids "$pl" \
        --query 'PrefixLists[0].Version' --output text 2>/dev/null)
      if aws ec2 modify-managed-prefix-list --region "$region" --prefix-list-id "$pl" \
        --current-version "$ver" --add-entries "Cidr=${ip}/32,Description=mesh" >/dev/null 2>&1; then
        break
      fi
      sleep 3
    done
  done
}

launch_one() { # <region> <sg> <itype> <role> <idx>  -> "iid pub priv" on stdout
  local region=$1 sg=$2 itype=$3 role=$4 idx=$5
  local ami=${AMI[$region]} az=${AZ[$region]} attempt out iid
  for attempt in 1 2 3 4 5; do
    out=$(aws ec2 run-instances --region "$region" --image-id "$ami" \
      --instance-type "$itype" --count 1 \
      --key-name "${RUN_ID}-key" --security-group-ids "$sg" \
      --placement "AvailabilityZone=$az" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=$TAG,Value=$RUN_ID},{Key=Name,Value=${RUN_ID}-${role}-${DC[$region]}-${idx}},{Key=role,Value=$role}]" \
      --query 'Instances[0].InstanceId' --output text 2>>"$STATE_DIR/provision.err")
    iid="$out"
    if [ -n "$iid" ] && [ "$iid" != "None" ]; then break; fi
    sleep $((attempt * 4))
  done
  [ -z "$iid" ] || [ "$iid" = "None" ] && { echo ""; return 1; }
  aws ec2 wait instance-running --region "$region" --instance-ids "$iid" 2>/dev/null
  local pub priv
  pub=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)
  priv=$(aws ec2 describe-instances --region "$region" --instance-ids "$iid" \
    --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text 2>/dev/null)
  [ -z "$pub" ] || [ "$pub" = "None" ] && { echo ""; return 1; }
  echo "$iid $pub $priv"
}

phase_provision() {
  : > "$STATE" ; : > "$LOADSTATE"
  # One key pair, imported into each region (same public key).
  if [ ! -f "$KEY" ]; then
    ssh-keygen -t ed25519 -N "" -f "$KEY" -q
    mv "$KEY.pub" "$KEY.pub.tmp"
  fi
  local region sg
  for region in "${REGIONS[@]}"; do
    aws ec2 import-key-pair --region "$region" --key-name "${RUN_ID}-key" \
      --public-key-material "fileb://${KEY}.pub.tmp" \
      --tag-specifications "ResourceType=key-pair,Tags=[{Key=$TAG,Value=$RUN_ID}]" >/dev/null 2>&1
  done
  # Ensure infra (prefix list + SG) in every region BEFORE launching.
  for region in "${REGIONS[@]}"; do
    sg=$(ensure_region_infra "$region")
    echo "$region $sg" >> "$STATE_DIR/sg.map"
    log "region $region sg=$sg"
  done
  # Launch dyniak nodes + one load-gen per region.
  local idx=0
  for region in "${REGIONS[@]}"; do
    sg=$(awk -v r="$region" '$1==r{print $2}' "$STATE_DIR/sg.map")
    local n
    for n in $(seq 1 "$NODES_PER_REGION"); do
      local row; row=$(launch_one "$region" "$sg" "$DYNIAK_ITYPE" node "$n") || { log "launch node $region/$n FAILED"; return 1; }
      # region dc rack node arch itype iid pub priv
      echo "$region ${DC[$region]} rack${n} n${n} x86 $DYNIAK_ITYPE $row" >> "$STATE"
      log "dyniak node up: $region rack${n} $(echo "$row" | awk '{print $2}')"
      idx=$((idx + 1))
    done
    local lrow; lrow=$(launch_one "$region" "$sg" "$LOADGEN_ITYPE" loadgen 1) || { log "launch loadgen $region FAILED"; return 1; }
    echo "$region ${DC[$region]} $lrow" >> "$LOADSTATE"
    log "loadgen up: $region $(echo "$lrow" | awk '{print $2}')"
  done
  # Allowlist every launched public IP into every region's mesh.
  local pub
  while read -r _ _ _ _ _ _ _ pub _; do allowlist_everywhere "$pub"; done < "$STATE"
  while read -r _ _ _ pub _; do allowlist_everywhere "$pub"; done < "$LOADSTATE"
  local nn ln
  nn=$(wc -l < "$STATE"); ln=$(wc -l < "$LOADSTATE")
  log "provisioned $nn dyniak nodes + $ln load-gens"
  [ "$nn" -ge $((NODES_PER_REGION * ${#REGIONS[@]})) ] && [ "$ln" -ge "${#REGIONS[@]}" ]
}


# --- build: compile the dyniak dynomited on one node, fetch binary ----
phase_build() {
  local brow breg bpub
  brow=$(head -1 "$STATE") || return 1
  breg=$(echo "$brow" | awk '{print $1}'); bpub=$(echo "$brow" | awk '{print $8}')
  log "building dynomited (riak) on $breg/$bpub"
  git -C "$SRC_DIR" archive --format=tar.gz -o "$STATE_DIR/src.tgz" HEAD 2>/dev/null || return 1
  nscp "$STATE_DIR/src.tgz" "$bpub" '~/src.tgz'
  nsh "$bpub" '
    set -e
    command -v cargo >/dev/null 2>&1 || { curl -sSf https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1; . ~/.cargo/env; }
    . ~/.cargo/env 2>/dev/null || true
    sudo dnf install -y -q gcc gcc-c++ cmake perl openssl-devel clang >/dev/null 2>&1 || true
    if [ -d ~/build ]; then find ~/build -mindepth 1 -delete 2>/dev/null; fi
    mkdir -p ~/build && tar -xzf ~/src.tgz -C ~/build
    cd ~/build
    cargo build --release -p dynomited --no-default-features --features riak 2>&1 | tail -2
    cp target/release/dynomited ~/dynomited && echo "BUILT $(md5sum ~/dynomited | cut -c1-12)"
  ' > "$STATE_DIR/build.log" 2>&1
  if ! grep -q "^BUILT" "$STATE_DIR/build.log"; then
    log "build failed; see $STATE_DIR/build.log"; tail -5 "$STATE_DIR/build.log" >&2; return 1
  fi
  SSH_AUTH_SOCK="" scp -i "$KEY" -o StrictHostKeyChecking=no -o IdentitiesOnly=yes \
    -o IdentityAgent=none ec2-user@"$bpub":'~/dynomited' "$STATE_DIR/dynomited" >/dev/null 2>&1
  [ -s "$STATE_DIR/dynomited" ] && log "fetched dynomited ($(du -h "$STATE_DIR/dynomited" | cut -f1))"
}

# --- distribute: binary to all nodes; driver to all load-gens --------
phase_distribute() {
  local ok=0 pub
  while read -r _ _ _ _ _ _ _ pub _; do
    nscp "$STATE_DIR/dynomited" "$pub" '~/dynomited' && nsh "$pub" 'chmod +x ~/dynomited' && ok=$((ok+1))
  done < "$STATE"
  local lok=0
  while read -r _ _ _ pub _; do
    nscp "$SRC_DIR/scripts/ec2-dist/chaos-crdt-driver.py" "$pub" '~/driver.py' && lok=$((lok+1))
  done < "$LOADSTATE"
  log "distributed binary to $ok nodes, driver to $lok load-gens"
  [ "$ok" -ge 1 ] && [ "$lok" -ge 1 ]
}

# --- mount: instance-store NVMe at /mnt/data for noxu -----------------
phase_mount() {
  local pub cnt=0
  while read -r _ _ _ _ _ _ _ pub _; do
    nsh "$pub" '
      DEV=$(lsblk -dpno NAME,MOUNTPOINT | awk "\$2==\"\" && \$1!~/nvme0/ {print \$1; exit}")
      if [ -n "$DEV" ] && [ ! -d /mnt/data/lost+found ]; then
        sudo mkfs.xfs -f "$DEV" >/dev/null 2>&1 || sudo mkfs.ext4 -F "$DEV" >/dev/null 2>&1
        sudo mkdir -p /mnt/data && sudo mount "$DEV" /mnt/data && sudo chown ec2-user:ec2-user /mnt/data
      fi
      mkdir -p /mnt/data/noxu 2>/dev/null && [ -w /mnt/data ] && echo MOUNTED || echo NODEV
    ' > "$STATE_DIR/mount.$pub" 2>&1
    grep -q MOUNTED "$STATE_DIR/mount.$pub" && cnt=$((cnt+1))
  done < "$STATE"
  log "NVMe mounted on $cnt dyniak nodes (data -> /mnt/data/noxu, real storage)"
  [ "$cnt" -ge 1 ]
}


# --- launch: write per-node dyniak config, start dynomited -----------
# Each region is a DC; each node is its own rack (a full ring replica),
# token 0, so n_val within a DC = nodes/region. dyn_seeds lists every
# OTHER node as host:port:rack:dc:token. noxu data lands on /mnt/data
# (real NVMe). The node advertises its public IP so cross-region peers
# match its gossip identity.
phase_launch() {
  # Build the global seed list once (all nodes, private IPs for intra-
  # AWS dnode traffic would need VPC peering; we use PUBLIC IPs since
  # the mesh SG allows them cross-region).
  local seeds=""
  while read -r region dc rack node arch itype iid pub priv; do
    seeds="${seeds}${pub}:8101:${rack}:${dc}:0\n"
  done < "$STATE"
  # Start dynomited on each node with the seed list minus itself.
  local region dc rack node arch itype iid pub priv
  while read -r region dc rack node arch itype iid pub priv; do
    local myseeds
    myseeds=$(printf "%b" "$seeds" | grep -v "^${pub}:" | sed 's/^/  - /')
    cat > "$STATE_DIR/conf-${pub}.yml" <<YML
dyn_o_mite:
  datacenter: ${dc}
  rack: ${rack}
  dyn_listen: 0.0.0.0:8101
  listen: 0.0.0.0:8102
  data_store: 2
  noxu_path: /mnt/data/noxu
  stats_listen: 0.0.0.0:22222
  servers:
  - 127.0.0.1:6379:1

  riak:
    pbc_listen: 0.0.0.0:8087
    http_listen: 0.0.0.0:8098
  tokens: '0'
  dyn_seeds:
${myseeds}
YML
    nscp "$STATE_DIR/conf-${pub}.yml" "$pub" '~/dyniak.yml'
    nsh "$pub" "sudo pkill -9 -x dynomited 2>/dev/null; \
      for _i in \$(seq 1 15); do pgrep -x dynomited >/dev/null || break; sudo pkill -9 -x dynomited; sleep 1; done; \
      for _i in \$(seq 1 20); do sudo ss -tln | grep -qE ':8101|:8102|:8087|:22222' || break; sleep 1; done; \
      DYN_ADVERTISE_ADDR=${pub} nohup ~/dynomited -c ~/dyniak.yml > ~/dynomited.log 2>&1 </dev/null & \
      for _i in \$(seq 1 20); do ss -tln | grep -q :8087 && break; sleep 1; done; echo started" >/dev/null 2>&1
    log "launched dyniak on $region/$pub"
  done < "$STATE"
  # Give gossip + PBC listeners time to come up, then verify PBC is
  # reachable on every node, retrying a clean restart on any node whose
  # PBC did not bind (a near-simultaneous restart of all nodes can race
  # a peer's reconnect to a dying socket and hit EADDRINUSE; an isolated
  # clean restart -- kill, poll ports free, launch, poll bound --
  # succeeds).
  sleep 20
  local region dc rack node arch itype iid pub priv
  while read -r region dc rack node arch itype iid pub priv; do
    local attempt
    for attempt in 1 2 3 4; do
      if nsh "$pub" 'ss -tln | grep -q :8087 && echo up' 2>/dev/null | grep -q up; then
        break
      fi
      log "retry dyniak restart on $region/$pub (attempt $attempt)"
      nsh "$pub" "sudo pkill -9 -x dynomited 2>/dev/null; \
        for _i in \$(seq 1 20); do pgrep -x dynomited >/dev/null || break; sudo pkill -9 -x dynomited; sleep 1; done; \
        for _i in \$(seq 1 25); do sudo ss -tln | grep -qE ':8101|:8102|:8087|:22222' || break; sleep 1; done; \
        DYN_ADVERTISE_ADDR=${pub} nohup ~/dynomited -c ~/dyniak.yml > ~/dynomited.log 2>&1 </dev/null & \
        for _i in \$(seq 1 25); do ss -tln | grep -q :8087 && break; sleep 1; done" >/dev/null 2>&1
    done
  done < "$STATE"
  # Final count.
  local up=0
  while read -r region dc rack node arch itype iid pub priv; do
    if nsh "$pub" 'ss -tln | grep -q :8087 && echo up' 2>/dev/null | grep -q up; then
      up=$((up+1))
    else
      log "WARN: PBC still not up on $pub after retries"
      nsh "$pub" 'tail -3 ~/dynomited.log 2>/dev/null | sed "s/\x1b\[[0-9;]*m//g"' 2>/dev/null | tail -3 >&2
    fi
  done < "$STATE"
  local total; total=$(wc -l < "$STATE")
  log "dyniak PBC up on $up/$total nodes"
  [ "$up" -ge "$total" ]
}

# --- fault injectors (run on the dyniak nodes via ssh) ---------------
# Partition: block dnode + PBC traffic to/from a set of peer IPs using
# iptables, hold, then heal. Churn: SIGSTOP/SIGCONT or kill+restart a
# node's dynomited.
inject_partition() { # <victim_pub> <peer_pub_csv> <hold_secs>
  local victim=$1 peers=$2 hold=$3 ip
  for ip in ${peers//,/ }; do
    nsh "$victim" "sudo iptables -A INPUT -s $ip -j DROP; sudo iptables -A OUTPUT -d $ip -j DROP" 2>/dev/null
  done
  log "partitioned $victim from [$peers] for ${hold}s"
  sleep "$hold"
  nsh "$victim" "sudo iptables -F" 2>/dev/null
  log "healed partition on $victim"
}

churn_node() { # <victim_pub> <down_secs>
  local victim=$1 down=$2
  nsh "$victim" 'sudo pkill -9 -x dynomited; for _i in $(seq 1 15); do pgrep -x dynomited >/dev/null || break; sudo pkill -9 -x dynomited; sleep 1; done' 2>/dev/null
  log "churned (killed) dynomited on $victim for ${down}s"
  sleep "$down"
  # Restart with a settle after kill so the kernel releases the listening
  # sockets, then poll ports-free, launch, poll-bound; retry a few times
  # if the rebind races a dying socket (EADDRINUSE aborts the whole
  # server build). A real deployment restarts dynomited under a process
  # manager (systemd), which is what this loop stands in for.
  local attempt
  for attempt in 1 2 3 4; do
    nsh "$victim" "sudo pkill -9 -x dynomited 2>/dev/null; sleep 3; \
      for _i in \$(seq 1 30); do sudo ss -tln | grep -qE ':8101|:8102|:8087|:22222' || break; sleep 1; done; \
      DYN_ADVERTISE_ADDR=${victim} nohup ~/dynomited -c ~/dyniak.yml > ~/dynomited.log 2>&1 </dev/null & \
      for _i in \$(seq 1 30); do ss -tln | grep -q :8087 && break; sleep 1; done" >/dev/null 2>&1
    if nsh "$victim" 'ss -tln | grep -q :8087 && echo up' 2>/dev/null | grep -q up; then
      log "restarted dynomited on $victim (node rejoined the ring, PBC up, attempt $attempt)"
      return 0
    fi
    log "churn restart on $victim did not bind PBC (attempt $attempt); retrying"
  done
  log "WARN: dynomited on $victim did not rebind PBC after churn"
  nsh "$victim" 'tail -2 ~/dynomited.log 2>/dev/null | sed "s/\x1b\[[0-9;]*m//g"' 2>/dev/null | tail -2 >&2
}


# --- load: drive CRDT traffic from every region + inject faults ------
# Each load-gen hammers its own-region dyniak node's PBC port with
# counter increments (a fixed keyspace shared across all regions, so the
# same key is updated concurrently from multiple regions -- the
# concurrent-write case CRDT merge must converge). A fault injector runs
# net splits and node churn during the load window. Afterwards the load
# stops, the cluster quiesces (anti-entropy + replication settle), and a
# fetch from every node is compared against the arithmetic expectation.
phase_load() {
  local dur="${LOAD_DURATION:-240}"      # seconds of load
  local keyspace="${KEYSPACE:-200}"
  # Reset the op history on every load-gen so a re-run does not inflate
  # the reconstructed expectation with a prior run's ops.
  local _lg
  while read -r _r _d _iid _lg _p; do
    nsh "$_lg" 'rm -f /tmp/chaos-history.jsonl /tmp/load-summary.json /tmp/load.err' 2>/dev/null
  done < "$LOADSTATE"
  # Start load on every load-gen against its region's first node.
  local region dc pub priv gi=0
  while read -r region dc iid pub priv; do
    # Find a dyniak node in the same region to target.
    local target
    target=$(awk -v r="$region" '$1==r{print $8; exit}' "$STATE")
    [ -z "$target" ] && { log "no dyniak target for loadgen $region"; return 1; }
    # Failover host list: local-region node first, then every other node.
    # A topology-aware client stays available when its preferred node is
    # down by routing to another; this measures CLUSTER availability.
    local others hostlist
    others=$(awk -v t="$target" '$8!=t{print $8}' "$STATE" | paste -sd,)
    hostlist="${target}${others:+,$others}"
    gi=$((gi+1))
    nsh "$pub" "nohup python3 ~/driver.py load --hosts $hostlist --port 8087 \
      --workload counter --btype counters --keyspace $keyspace \
      --duration $dur --seed $gi --gen-id gen${gi} \
      --history /tmp/chaos-history.jsonl > /tmp/load-summary.json 2>/tmp/load.err </dev/null & echo started" >/dev/null 2>&1
    log "load started on $region/$pub -> $target (gen${gi}, ${dur}s)"
  done < "$LOADSTATE"

  # --- adversarial fault schedule during the load window -----------
  # Give load a warm-up, then partition, churn, and heal on a cadence
  # that fits inside `dur`.
  sleep 30
  local n1_pub n2_pub n_use1 n_usw2 n_euc1
  n_use1=$(awk '$1=="us-east-1"{print $8; exit}' "$STATE")
  n_usw2=$(awk '$1=="us-west-2"{print $8; exit}' "$STATE")
  n_euc1=$(awk '$1=="eu-central-1"{print $8; exit}' "$STATE")
  # 1. Partition us-east-1 node from the other two regions (a net split)
  #    while load keeps writing to it -- writes must still be accepted
  #    (single-key CRDT, always-available) and converge after heal.
  inject_partition "$n_use1" "${n_usw2},${n_euc1}" 45
  sleep 15
  # 2. Churn: kill a us-west-2 node, let the ring adjust, restart it.
  churn_node "$n_usw2" 40
  sleep 15
  # 3. Partition the OTHER direction: split eu-central-1 off.
  inject_partition "$n_euc1" "${n_use1},${n_usw2}" 45
  sleep 15
  # 4. Churn us-east-1 during its own load.
  churn_node "$n_use1" 30

  # Wait for the load window to finish (drivers self-terminate at dur).
  log "faults done; waiting for load window to end + quiesce"
  sleep $((dur > 180 ? 60 : 30))
  # Extra quiescence for anti-entropy + replication to converge after
  # the last fault.
  sleep 60

  # --- collect per-generator availability + p99 --------------------
  : > "$RESULTS/load-summaries.txt"
  while read -r region dc iid pub priv; do
    nsh "$pub" 'cat /tmp/load-summary.json 2>/dev/null' 2>/dev/null >> "$RESULTS/load-summaries.txt"
    echo >> "$RESULTS/load-summaries.txt"
  done < "$LOADSTATE"
  log "load summaries:"; sed '/^$/d' "$RESULTS/load-summaries.txt" | tee -a "$STATE_DIR/orchestrator.log" >&2

  # --- reconstruct expected per-key counts from ALL histories ------
  : > "$RESULTS/all-history.jsonl"
  while read -r region dc iid pub priv; do
    nsh "$pub" 'cat /tmp/chaos-history.jsonl 2>/dev/null' 2>/dev/null >> "$RESULTS/all-history.jsonl"
  done < "$LOADSTATE"

  local checker_gen
  checker_gen=$(head -1 "$LOADSTATE" | awk '{print $4}')
  nscp "$SRC_DIR/scripts/ec2-dist/chaos-verify.py" "$checker_gen" '~/verify.py'
  # Ship the merged history to the checker gen.
  nscp "$RESULTS/all-history.jsonl" "$checker_gen" '~/all-history.jsonl'
  # Build a CSV of node public IPs for the checker to fetch from.
  awk '{print $8}' "$STATE" | paste -sd, > "$STATE_DIR/nodes.csv"
  local nodes_csv; nodes_csv=$(cat "$STATE_DIR/nodes.csv")
  nsh "$checker_gen" "python3 ~/verify.py --history ~/all-history.jsonl \
    --nodes $nodes_csv --port 8087 --keyspace $keyspace --workload counter" \
    > "$RESULTS/convergence.json" 2>"$RESULTS/convergence.err"
  log "convergence result:"; cat "$RESULTS/convergence.json" | tee -a "$STATE_DIR/orchestrator.log" >&2

  # Pass criteria: every node converged all keys AND availability high.
  python3 - "$RESULTS/convergence.json" "$RESULTS/load-summaries.txt" <<'PYCHK'
import json, sys
conv = json.load(open(sys.argv[1]))
ok = conv.get("all_converged", False)
worst_avail = 100.0
worst_p99 = 0.0
for line in open(sys.argv[2]):
    line=line.strip()
    if not line: continue
    try:
        s=json.loads(line)
        worst_avail=min(worst_avail, s.get("avail_pct",0))
        worst_p99=max(worst_p99, s.get("p99_ms",0))
    except Exception:
        pass
print(f"CHAOS-VERDICT converged={ok} worst_avail={worst_avail}% worst_p99={worst_p99}ms "
      f"diverged_nodes={conv.get('diverged_nodes')}")
# Availability must stay high (single-key CRDT writes always accepted);
# convergence must be total.
sys.exit(0 if (ok and worst_avail >= 99.0) else 1)
PYCHK
}

# --- teardown: terminate everything, delete SG/pl/keys, verify -------
phase_teardown() {
  local region ids sg pl kp
  for region in "${REGIONS[@]}"; do
    ids=$(aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=pending,running,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    if [ -n "$ids" ]; then
      aws ec2 terminate-instances --region "$region" --instance-ids $ids >/dev/null 2>&1
      aws ec2 wait instance-terminated --region "$region" --instance-ids $ids 2>/dev/null
      log "$region terminated"
    fi
    for sg in $(aws ec2 describe-security-groups --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[].GroupId' --output text 2>/dev/null); do
      aws ec2 delete-security-group --region "$region" --group-id "$sg" >/dev/null 2>&1 || true
    done
    for pl in $(aws ec2 describe-managed-prefix-lists --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'PrefixLists[].PrefixListId' --output text 2>/dev/null); do
      aws ec2 delete-managed-prefix-list --region "$region" --prefix-list-id "$pl" >/dev/null 2>&1 || true
    done
    for kp in $(aws ec2 describe-key-pairs --region "$region" \
        --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'KeyPairs[].KeyName' --output text 2>/dev/null); do
      aws ec2 delete-key-pair --region "$region" --key-name "$kp" >/dev/null 2>&1 || true
    done
  done
  log "teardown complete"
}
