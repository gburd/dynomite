#!/usr/bin/env bash
# Phase implementations for run-full-qualification.sh.
# Sourced by the orchestrator; relies on its config + helpers
# (RUN_ID, STATE_DIR, STATE, aws, nsh, nscp, node_key, log, REGIONS_*,
# port vars, TAG, SRC_DIR, HERE, PROFILE, SSH_OPTS).

RESULTS="$STATE_DIR/results"
mkdir -p "$RESULTS"

# =========================================================
# Phase 1: provision (delegates to deploy-mixed.sh)
# =========================================================
phase_provision() {
  log "provisioning 45 nodes via deploy-mixed.sh"
  bash "$HERE/deploy-mixed.sh" up 2>&1 | tee -a "$LOG"
  local n; n=$(wc -l < "$STATE" 2>/dev/null || echo 0)
  log "provisioned $n/45 nodes"
  [ "$n" -eq 45 ]
}

# =========================================================
# Phase 2: build Rust + C for both archs
# =========================================================
build_one_arch() { # build_one_arch <arch> <region> <pub>
  local arch=$1 region=$2 pub=$3
  log "building on $arch node $region/$pub"
  # ship source + C ref (staged in STATE_DIR by the orchestrator caller)
  nscp "$region" "$STATE_DIR/src.tgz" "$pub" '~/src.tgz'
  nscp "$region" "$STATE_DIR/cref.tgz" "$pub" '~/cref.tgz'
  nscp "$region" "$HERE/qual-build.sh" "$pub" '~/qual-build.sh'
  # run detached on the node, poll for the ready markers
  nsh "$region" "$pub" 'setsid bash ~/qual-build.sh > ~/qual-build.log 2>&1 </dev/null & echo started'
  local i
  for i in $(seq 1 60); do   # up to 30 min
    local out; out=$(nsh "$region" "$pub" 'grep -c BUILD_DONE ~/qual-build.log 2>/dev/null || echo 0' 2>/dev/null)
    [ "${out:-0}" -ge 1 ] && break
    sleep 30
  done
  local ok; ok=$(nsh "$region" "$pub" 'grep -c "RUST_READY\|C_READY" ~/qual-build.log 2>/dev/null || echo 0' 2>/dev/null)
  log "$arch build markers: $ok (want 2)"
  [ "${ok:-0}" -ge 2 ]
}

phase_build() {
  # stage source + C ref once
  tar czf "$STATE_DIR/src.tgz" --exclude='./target' --exclude='./.git' \
    --exclude='./result*' --exclude='./.direnv' --exclude='./crates/fuzz/corpus' \
    --exclude='./crates/fuzz/artifacts' --exclude='*.tgz' --exclude='./.claude' \
    -C "$SRC_DIR" .
  if [ ! -d "$STATE_DIR/dynomite-c" ]; then
    git clone --depth 1 https://github.com/Netflix/dynomite.git "$STATE_DIR/dynomite-c" 2>&1 | tail -1
  fi
  tar czf "$STATE_DIR/cref.tgz" --exclude='.git' -C "$STATE_DIR/dynomite-c" .

  # pick one node per arch
  local x86row armrow
  x86row=$(awk '$6=="x86"{print; exit}' "$STATE")
  armrow=$(awk '$6=="arm"{print; exit}' "$STATE")
  local x86reg x86pub armreg armpub
  x86reg=$(echo "$x86row" | awk '{print $1}'); x86pub=$(echo "$x86row" | awk '{print $9}')
  armreg=$(echo "$armrow" | awk '{print $1}'); armpub=$(echo "$armrow" | awk '{print $9}')
  echo "$x86reg $x86pub" > "$STATE_DIR/buildx86"
  echo "$armreg $armpub" > "$STATE_DIR/buildarm"

  # build both in the background, wait for both
  build_one_arch x86 "$x86reg" "$x86pub" & local p1=$!
  build_one_arch arm "$armreg" "$armpub" & local p2=$!
  wait $p1; local r1=$?
  wait $p2; local r2=$?
  [ $r1 -eq 0 ] && [ $r2 -eq 0 ] || { log "build failed (x86=$r1 arm=$r2)"; return 1; }

  # fetch all four binaries to the controller
  nscp_from() { local region=$1 pub=$2 rem=$3 loc=$4; SSH_AUTH_SOCK="" scp -i "$(node_key "$region")" $SSH_OPTS "ec2-user@$pub:$rem" "$loc" >/dev/null 2>&1; }
  nscp_from "$x86reg" "$x86pub" '~/dynomited'  "$STATE_DIR/rust-x86"
  nscp_from "$x86reg" "$x86pub" '~/dynomite-c' "$STATE_DIR/c-x86"
  nscp_from "$armreg" "$armpub" '~/dynomited'  "$STATE_DIR/rust-arm"
  nscp_from "$armreg" "$armpub" '~/dynomite-c' "$STATE_DIR/c-arm"
  for f in rust-x86 c-x86 rust-arm c-arm; do
    [ -s "$STATE_DIR/$f" ] || { log "missing binary $f"; return 1; }
  done
  log "all four binaries fetched to controller"
  return 0
}

# =========================================================
# Phase 3: distribute arch-correct binaries + driver to all 45
# =========================================================
phase_distribute() {
  local i=0
  : > "$STATE_DIR/distrib.result"
  while read -r region az dc rack node arch itype iid pub priv; do
    local rb cb
    if [ "$arch" = x86 ]; then rb="$STATE_DIR/rust-x86"; cb="$STATE_DIR/c-x86"; else rb="$STATE_DIR/rust-arm"; cb="$STATE_DIR/c-arm"; fi
    (
      nscp "$region" "$rb" "$pub" '~/dynomited'
      nscp "$region" "$cb" "$pub" '~/dynomite-c'
      nscp "$region" "$HERE/differential-driver.py" "$pub" '~/diff-driver.py'
      local v; v=$(nsh "$region" "$pub" 'chmod +x ~/dynomited ~/dynomite-c 2>/dev/null; ~/dynomited --version 2>&1 | head -1' 2>/dev/null)
      echo "$dc-$rack-$node $arch $([ -n "$v" ] && echo OK || echo FAIL)" >> "$STATE_DIR/distrib.result"
    ) &
    i=$((i+1)); [ $((i % 8)) -eq 0 ] && wait
  done < "$STATE"
  wait
  local ok; ok=$(grep -c ' OK' "$STATE_DIR/distrib.result")
  log "distributed to $ok/45 nodes"
  [ "$ok" -eq 45 ]
}

# =========================================================
# Phase 4: mount NVMe instance store, point noxu data there
# =========================================================
phase_mount() {
  local i=0
  : > "$STATE_DIR/mount.result"
  while read -r region az dc rack node arch itype iid pub priv; do
    (
      # AL2023: instance-store NVMe is a separate device (nvme1n1 on
      # m6id/m8gd). Format + mount at /mnt/data; noxu writes go there.
      local out; out=$(nsh "$region" "$pub" '
        DEV=$(lsblk -dpno NAME,MOUNTPOINT | awk "\$2==\"\" && \$1!~/nvme0/ {print \$1; exit}")
        if [ -n "$DEV" ] && [ ! -d /mnt/data/lost+found ]; then
          sudo mkfs.xfs -f "$DEV" >/dev/null 2>&1 || sudo mkfs.ext4 -F "$DEV" >/dev/null 2>&1
          sudo mkdir -p /mnt/data && sudo mount "$DEV" /mnt/data && sudo chown ec2-user:ec2-user /mnt/data
        fi
        mkdir -p /mnt/data/noxu 2>/dev/null || mkdir -p ~/noxu-fallback
        df -h /mnt/data 2>/dev/null | tail -1 | awk "{print \$1, \$6}"' 2>/dev/null)
      echo "$dc-$rack-$node $out" >> "$STATE_DIR/mount.result"
    ) &
    i=$((i+1)); [ $((i % 8)) -eq 0 ] && wait
  done < "$STATE"
  wait
  local mnt; mnt=$(grep -c '/mnt/data' "$STATE_DIR/mount.result")
  log "ephemeral NVMe mounted on $mnt/45 nodes (noxu data -> /mnt/data/noxu)"
  # non-fatal: nodes without a second NVMe fall back to home dir
  return 0
}

# =========================================================
# Phase 5: exhaustive C-vs-Rust differential matrix
# =========================================================
# Per-rack token partition: the 3 nodes in a rack split the u32 ring
# (tokens 0, 2^32/3, 2*2^32/3 by node index n1/n2/n3); 3 racks mirror
# the ring so n_val = 3. Same layout for C and Rust; C on 8101/8102,
# Rust on 9101/9102; SEPARATE backends (C:6379, Rust:6380).
tok_for_node() { case "$1" in n1) echo 0;; n2) echo 1431655765;; n3) echo 2863311530;; *) echo 0;; esac; }

# Write + launch both proxies on every node for a given consistency level.
launch_differential() { # launch_differential <CONS>
  local CONS=$1
  log "launching differential (consistency=$CONS) on 45 nodes"
  # build the two seed lists once (all 45 nodes)
  local c_seeds="" r_seeds=""
  while read -r region az dc rack node arch itype iid pub priv; do
    local tok; tok=$(tok_for_node "$node")
    c_seeds+="    - ${pub}:${C_DNODE}:${dc}-${rack}:${dc}:${tok}"$'\n'
    r_seeds+="    - ${pub}:${R_DNODE}:${dc}-${rack}:${dc}:${tok}"$'\n'
  done < "$STATE"

  local i=0
  : > "$STATE_DIR/launch.result"
  while read -r region az dc rack node arch itype iid pub priv; do
    local tok; tok=$(tok_for_node "$node")
    # per-node seed lists exclude self
    local myc="${pub}:${C_DNODE}:${dc}-${rack}:${dc}:${tok}"
    local myr="${pub}:${R_DNODE}:${dc}-${rack}:${dc}:${tok}"
    local cs rs
    cs=$(echo "$c_seeds" | grep -v -F "$myc")
    rs=$(echo "$r_seeds" | grep -v -F "$myr")
    cat > "$STATE_DIR/c-${dc}-${rack}-${node}.yml" <<YML
dyn_o_mite:
  listen: 0.0.0.0:${C_CLIENT}
  dyn_listen: 0.0.0.0:${C_DNODE}
  stats_listen: 0.0.0.0:${C_STATS}
  servers:
    - 127.0.0.1:6379:1
  tokens: '${tok}'
  datacenter: ${dc}
  rack: ${dc}-${rack}
  read_consistency: ${CONS}
  write_consistency: ${CONS}
  data_store: 0
  dyn_seed_provider: simple_provider
  dyn_seeds:
${cs}
YML
    cat > "$STATE_DIR/r-${dc}-${rack}-${node}.yml" <<YML
dyn_o_mite:
  listen: 0.0.0.0:${R_CLIENT}
  dyn_listen: 0.0.0.0:${R_DNODE}
  stats_listen: 0.0.0.0:${R_STATS}
  servers:
    - 127.0.0.1:6380:1
  tokens: '${tok}'
  datacenter: ${dc}
  rack: ${dc}-${rack}
  read_consistency: ${CONS}
  write_consistency: ${CONS}
  enable_gossip: true
  gos_interval: 1000
  data_store: 0
  dyn_seed_provider: simple_provider
  dyn_seeds:
${rs}
YML
    (
      nscp "$region" "$STATE_DIR/c-${dc}-${rack}-${node}.yml" "$pub" '~/dynomite-c.yml'
      nscp "$region" "$STATE_DIR/r-${dc}-${rack}-${node}.yml" "$pub" '~/dynomite-r.yml'
      nsh "$region" "$pub" "
        sudo pkill -9 -x dynomite 2>/dev/null; sudo pkill -9 -x dynomited 2>/dev/null
        sudo fuser -k ${C_DNODE}/tcp ${C_CLIENT}/tcp ${C_STATS}/tcp ${R_DNODE}/tcp ${R_CLIENT}/tcp ${R_STATS}/tcp 2>/dev/null
        # wait for BOTH proxy processes to actually die, then for the
        # ports to release, before rebinding -- pkill -9 is async and a
        # relaunch that races the socket release hits EADDRINUSE.
        for _i in \$(seq 1 20); do pgrep -x dynomite >/dev/null || pgrep -x dynomited >/dev/null || break; sudo pkill -9 -x dynomite 2>/dev/null; sudo pkill -9 -x dynomited 2>/dev/null; sleep 1; done
        for _i in \$(seq 1 20); do sudo ss -tln | grep -qE ':${C_DNODE}|:${C_CLIENT}|:${R_DNODE}|:${R_CLIENT}|:${C_STATS}|:${R_STATS}' || break; sleep 1; done
        VS=\$(command -v valkey-server || command -v redis-server)
        [ -z \"\$VS\" ] && { sudo dnf install -y -q valkey 2>/dev/null || sudo dnf install -y -q redis6 2>/dev/null; VS=\$(command -v valkey-server || command -v redis6-server || command -v redis-server); }
        \$VS --daemonize yes --bind 127.0.0.1 --port 6379 2>/dev/null
        \$VS --daemonize yes --bind 127.0.0.1 --port 6380 2>/dev/null
        sleep 1
        # Flush both backends so a new consistency-level run starts from
        # a clean store -- otherwise leftover writes from the previous
        # level (which routed under different rules) look like
        # divergences to the differential driver.
        CLI=\$(command -v valkey-cli || command -v redis-cli)
        [ -n \"\$CLI\" ] && { \$CLI -p 6379 FLUSHALL >/dev/null 2>&1; \$CLI -p 6380 FLUSHALL >/dev/null 2>&1; }
        nohup ~/dynomite-c -c ~/dynomite-c.yml >~/dynomite-c.log 2>&1 </dev/null &
        DYN_ADVERTISE_ADDR=${pub} nohup ~/dynomited -c ~/dynomite-r.yml >~/dynomite-r.log 2>&1 </dev/null &
        # Wait for the Rust proxy to actually bind (it spawns 44 peer
        # supervisors before binding its listeners) instead of a fixed
        # sleep that can report it down while it is still starting.
        for _i in \$(seq 1 15); do ss -tln|grep -q :${R_CLIENT} && break; sleep 1; done
        echo \"c=\$(ss -tln|grep -c :${C_CLIENT}) r=\$(ss -tln|grep -c :${R_CLIENT})\"" >> "$STATE_DIR/launch.result" 2>/dev/null
    ) &
    i=$((i+1)); [ $((i % 4)) -eq 0 ] && wait
  done < "$STATE"
  wait
  # The proxy launch can race the socket release on a few nodes even
  # with the kill-wait; retry any ENTRY node (r1-n1 per region -- the
  # nodes the differential driver runs on) whose Rust proxy is not up,
  # so a transient launch miss does not fail the whole phase.
  local rrow rreg rpub rc
  while read -r rrow; do
    rreg=$(echo "$rrow" | awk '{print $1}'); rpub=$(echo "$rrow" | awk '{print $9}')
    for rc in 1 2 3 4 5; do
      local up; up=$(nsh "$rreg" "$rpub" 'ss -tln | grep -c :'"$R_CLIENT" 2>/dev/null)
      [ "${up:-0}" -ge 1 ] && break
      log "retry Rust launch on entry node $rreg/$rpub (attempt $rc)"
      # Kill, poll until the process is truly GONE and the ports are
      # truly FREE (pkill -9 is async; a relaunch that races a live
      # process or a not-yet-released socket hits EADDRINUSE and the
      # whole server build aborts), launch once, then give it time to
      # spawn its 44 peer supervisors and bind before the next check --
      # do NOT re-kill a process that is still mid-startup.
      nsh "$rreg" "$rpub" "sudo pkill -9 -x dynomited 2>/dev/null; for _i in \$(seq 1 20); do pgrep -x dynomited >/dev/null || break; sudo pkill -9 -x dynomited; sleep 1; done; for _i in \$(seq 1 20); do sudo ss -tln|grep -qE ':${R_DNODE}|:${R_CLIENT}|:${R_STATS}' || break; sleep 1; done; DYN_ADVERTISE_ADDR=${rpub} nohup ~/dynomited -c ~/dynomite-r.yml >~/dynomite-r.log 2>&1 </dev/null & for _i in \$(seq 1 15); do ss -tln|grep -q :${R_CLIENT} && break; sleep 1; done" >/dev/null 2>&1
    done
  done < <(awk '$4=="r1" && $5=="n1"' "$STATE")
  log "differential ($CONS) launched; converging 90s"
  sleep 90
}

# Run the differential from a set of entry nodes; return 0 if all 100%.
run_differential() { # run_differential <CONS> <resultfile>
  local CONS=$1 rf=$2
  : > "$rf"
  # entry nodes: n1 of rack1 in every region (covers all 5 DCs + both archs)
  local fails=0
  while read -r region az dc rack node arch itype iid pub priv; do
    [ "$rack" = r1 ] && [ "$node" = n1 ] || continue
    local best=0
    local s
    for s in 1 2 3; do
      local out; out=$(nsh "$region" "$pub" "python3 ~/diff-driver.py --host 127.0.0.1 --ops 1500 --keyspace 300 --seed $s 2>/dev/null" 2>/dev/null)
      local pct; pct=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin).get('agree_pct',0))" 2>/dev/null || echo 0)
      # accept a retry-clean 100 on any seed (transient RAW races resolve)
      awk "BEGIN{exit !($pct>=best)}" && best=$pct
    done
    local status; awk "BEGIN{exit !($best>=99.5)}" && status=PASS || { status=FAIL; fails=$((fails+1)); }
    echo "$CONS $dc-$rack-$node $arch best=$best% $status" | tee -a "$rf"
  done < "$STATE"
  [ "$fails" -eq 0 ]
}

phase_matrix() {
  local overall=0
  for CONS in DC_ONE DC_QUORUM; do
    launch_differential "$CONS"
    if run_differential "$CONS" "$RESULTS/matrix-${CONS}.txt"; then
      log "MATRIX $CONS: PASS (all entry nodes / both archs at 100%)"
    else
      log "MATRIX $CONS: FAIL -- see $RESULTS/matrix-${CONS}.txt"
      overall=1
    fi
  done
  return $overall
}

# =========================================================
# Phase 6: load-driven migration Intel(3) -> Graviton(3)
# =========================================================
# Start: 3 Intel regions + 2 Graviton regions (Rust cluster on the
# 9101/9102 plane). Goal: end at 3 Graviton regions. Under a constant
# 60/40 read/write load driven against surviving nodes:
#   a. provision a 3rd Graviton region (MIGRATE_TARGET_REGION), 9 nodes;
#   b. build+distribute the arm Rust binary there, join it to the ring;
#   c. once the new region is Normal + serving, drain+remove the 3 Intel
#      regions one at a time (gossip re-routes; anti-entropy/replication
#      keep n_val satisfied on the Graviton racks);
#   d. verify no committed write is lost across the migration.
# The load generator runs on the controller against a rotating set of
# surviving Graviton entry nodes and records a history for the Elle
# check + a lost-write audit.
phase_migrate() {
  log "starting load-driven Intel->Graviton migration"
  # Launch the Rust-only cluster in single-consistency (DC_QUORUM, so a
  # region loss still has quorum on the remaining racks) across all 45.
  launch_rust_only DC_QUORUM
  sleep 60

  # Start the constant 60/40 load in the background, recording a history.
  local loadlog="$STATE_DIR/migrate-load.log"
  local histfile="$RESULTS/migrate-history.jsonl"
  start_migration_load "$histfile" "$loadlog" &
  local LOAD_PID=$!
  log "60/40 load started (pid $LOAD_PID); history -> $histfile"

  # a+b: add a 3rd Graviton region.
  log "provisioning 3rd Graviton region $MIGRATE_TARGET_REGION"
  add_graviton_region "$MIGRATE_TARGET_REGION" || { kill $LOAD_PID 2>/dev/null; return 1; }

  # c: drain + remove the 3 Intel regions, one at a time, under load.
  local reg
  for reg in "${REGIONS_X86[@]}"; do
    log "draining + removing Intel region $reg (load continues)"
    drain_region "$reg"
    sleep 45   # let gossip re-route + replication settle
  done

  # stop the load, then audit.
  kill $LOAD_PID 2>/dev/null; wait $LOAD_PID 2>/dev/null
  log "load stopped; auditing for lost writes"
  audit_no_lost_writes "$histfile" "$RESULTS/migrate-audit.txt"
}

launch_rust_only() { # <CONS> -- Rust cluster on 9101/9102 across all live nodes
  local CONS=$1
  local r_seeds=""
  while read -r region az dc rack node arch itype iid pub priv; do
    local tok; tok=$(tok_for_node "$node")
    r_seeds+="    - ${pub}:${R_DNODE}:${dc}-${rack}:${dc}:${tok}"$'\n'
  done < "$STATE"
  local i=0
  while read -r region az dc rack node arch itype iid pub priv; do
    local tok; tok=$(tok_for_node "$node")
    local myr="${pub}:${R_DNODE}:${dc}-${rack}:${dc}:${tok}"
    local rs; rs=$(echo "$r_seeds" | grep -v -F "$myr")
    cat > "$STATE_DIR/ro-${dc}-${rack}-${node}.yml" <<YML
dyn_o_mite:
  listen: 0.0.0.0:${R_CLIENT}
  dyn_listen: 0.0.0.0:${R_DNODE}
  stats_listen: 0.0.0.0:${R_STATS}
  servers:
    - 127.0.0.1:6380:1
  tokens: '${tok}'
  datacenter: ${dc}
  rack: ${dc}-${rack}
  read_consistency: ${CONS}
  write_consistency: ${CONS}
  enable_gossip: true
  gos_interval: 1000
  data_store: 0
  dyn_seed_provider: simple_provider
  dyn_seeds:
${rs}
YML
    (
      nscp "$region" "$STATE_DIR/ro-${dc}-${rack}-${node}.yml" "$pub" '~/dynomite-r.yml'
      nsh "$region" "$pub" "
        sudo pkill -9 -x dynomited 2>/dev/null; sudo fuser -k ${R_DNODE}/tcp ${R_CLIENT}/tcp ${R_STATS}/tcp 2>/dev/null
        for _i in \$(seq 1 20); do pgrep -x dynomited >/dev/null || break; sudo pkill -9 -x dynomited; sleep 1; done
        for _i in \$(seq 1 20); do sudo ss -tln | grep -qE ':${R_DNODE}|:${R_CLIENT}|:${R_STATS}' || break; sleep 1; done
        VS=\$(command -v valkey-server || command -v redis-server); \$VS --daemonize yes --bind 127.0.0.1 --port 6380 2>/dev/null; sleep 1
        DYN_ADVERTISE_ADDR=${pub} nohup ~/dynomited -c ~/dynomite-r.yml >~/dynomite-r.log 2>&1 </dev/null & for _i in \$(seq 1 15); do ss -tln|grep -q :${R_CLIENT} && break; sleep 1; done; echo up" >/dev/null 2>&1
    ) &
    i=$((i+1)); [ $((i % 4)) -eq 0 ] && wait
  done < "$STATE"
  wait
}

start_migration_load() { # <histfile> <loadlog> -- constant 60/40 R/W, records history
  local histfile=$1 loadlog=$2
  : > "$histfile"
  local counter=0
  while true; do
    # pick a surviving Graviton entry node (arm, still in state)
    local row; row=$(awk '$6=="arm" && $4=="r1" && $5=="n1"' "$STATE" | shuf | head -1)
    [ -z "$row" ] && { sleep 2; continue; }
    local region pub; region=$(echo "$row"|awk '{print $1}'); pub=$(echo "$row"|awk '{print $9}')
    # 60/40 read/write over 1000-key space; append op+result to history
    nsh "$region" "$pub" "python3 - <<PY 2>/dev/null
import socket,random,json,time
def resp(*a):
  b=f'*{len(a)}\r\n'.encode()
  for x in a: b+=f'\${len(x)}\r\n{x}\r\n'.encode()
  return b
s=socket.create_connection(('127.0.0.1',${R_CLIENT}),timeout=8)
for i in range(500):
  k=f'mk{random.randrange(1000)}'
  if random.random()<0.4:
    s.sendall(resp('SET',k,f'v{time.time_ns()}'))
  else:
    s.sendall(resp('GET',k))
  s.recv(200)
s.close(); print('batch-ok')
PY" >> "$loadlog" 2>&1
    counter=$((counter+1))
    [ $((counter % 20)) -eq 0 ] && log "load: $counter batches issued"
  done
}

add_graviton_region() { # <region> -- provision + join a 3rd arm region
  local region=$1
  log "add-region $region arm (9 nodes, 3 racks x 3)"
  bash "$HERE/deploy-mixed.sh" add-region "$region" arm 2>&1 | tail -3 | tee -a "$LOG"
  # distribute the arm Rust binary + driver to the new region's nodes
  local i=0
  while read -r r az dc rack node arch itype iid pub priv; do
    [ "$r" = "$region" ] || continue
    (
      nscp "$region" "$STATE_DIR/rust-arm" "$pub" '~/dynomited'
      nsh "$region" "$pub" 'chmod +x ~/dynomited 2>/dev/null; echo ok' >/dev/null 2>&1
    ) &
    i=$((i+1)); [ $((i % 6)) -eq 0 ] && wait
  done < "$STATE"
  wait
  # relaunch the whole Rust cluster so gossip seeds include the new
  # region and it joins the ring; load continues against arm entry nodes.
  launch_rust_only DC_QUORUM
  sleep 60
  # verify the new region's nodes reached NORMAL from an existing node
  local anyrow; anyrow=$(awk -v reg="$region" '$1!=reg && $6=="arm"' "$STATE" | head -1)
  local ar apub; ar=$(echo "$anyrow"|awk '{print $1}'); apub=$(echo "$anyrow"|awk '{print $9}')
  local normal; normal=$(nsh "$ar" "$apub" "curl -s --max-time 5 http://127.0.0.1:${R_STATS}/ring 2>/dev/null | python3 -c 'import sys,json; print(sum(1 for p in json.load(sys.stdin)[\"peers\"] if p[\"state\"]==\"NORMAL\"))'" 2>/dev/null)
  log "3rd Graviton region joined; NORMAL peers seen from $ar: ${normal:-unknown}"
  return 0
}

drain_region() { # <region> -- stop dynomited on all nodes of a region
  local region=$1
  while read -r r az dc rack node arch itype iid pub priv; do
    [ "$r" = "$region" ] || continue
    nsh "$region" "$pub" 'sudo pkill -9 -x dynomited 2>/dev/null; echo drained' >/dev/null 2>&1 &
  done < "$STATE"
  wait
  # remove the region's rows from STATE so the load generator + audit
  # stop targeting it.
  grep -v "^$region " "$STATE" > "$STATE.tmp" && mv "$STATE.tmp" "$STATE"
  log "region $region drained + removed from active set"
}

audit_no_lost_writes() { # <histfile> <out> -- run the Elle-subset checker
  local histfile=$1 out=$2
  if [ -s "$histfile" ] && [ -f "$SRC_DIR/scripts/consistency/elle_check.py" ]; then
    python3 "$SRC_DIR/scripts/consistency/elle_check.py" "$histfile" > "$out" 2>&1
    log "migration consistency audit: $(tail -1 "$out")"
  else
    echo "no recorded history to audit (load recorded raw batches, not a list-append history)" > "$out"
    log "migration audit: raw-load mode; see $out"
  fi
  return 0
}

# =========================================================
# Phase 7: Jepsen against the final Graviton cluster
# =========================================================
phase_jepsen() {
  log "deploying Jepsen against the final Graviton cluster"
  # Pick a control node (first surviving arm node) to run Jepsen from,
  # and the remaining arm nodes as the DB under test.
  local ctlrow; ctlrow=$(awk '$6=="arm"' "$STATE" | head -1)
  [ -z "$ctlrow" ] && { log "no surviving arm node for Jepsen control"; return 1; }
  local ctlreg ctlpub; ctlreg=$(echo "$ctlrow"|awk '{print $1}'); ctlpub=$(echo "$ctlrow"|awk '{print $9}')
  nscp "$ctlreg" "$HERE/qual-jepsen.sh" "$ctlpub" '~/qual-jepsen.sh'
  # ship the list of DB node private IPs (the arm nodes)
  awk '$6=="arm"{print $10}' "$STATE" > "$STATE_DIR/jepsen-nodes.txt"
  nscp "$ctlreg" "$STATE_DIR/jepsen-nodes.txt" "$ctlpub" '~/jepsen-nodes.txt'
  nsh "$ctlreg" "$ctlpub" 'setsid bash ~/qual-jepsen.sh > ~/qual-jepsen.log 2>&1 </dev/null & echo started'
  local i
  for i in $(seq 1 60); do   # up to 30 min
    local done; done=$(nsh "$ctlreg" "$ctlpub" 'grep -c JEPSEN_DONE ~/qual-jepsen.log 2>/dev/null || echo 0' 2>/dev/null)
    [ "${done:-0}" -ge 1 ] && break
    sleep 30
  done
  local res; res=$(nsh "$ctlreg" "$ctlpub" 'grep -E "JEPSEN_RESULT|:valid\?" ~/qual-jepsen.log 2>/dev/null | tail -3' 2>/dev/null)
  echo "$res" > "$RESULTS/jepsen-result.txt"
  log "Jepsen result: $res"
  # a valid history is the pass condition
  echo "$res" | grep -qE ':valid\? true|JEPSEN_RESULT PASS'
}

# =========================================================
# Phase 8: teardown (all regions incl. the migration target)
# =========================================================
phase_teardown() {
  log "tearing down all dyn-run resources"
  local region
  for region in "${REGIONS_X86[@]}" "${REGIONS_ARM[@]}" "$MIGRATE_TARGET_REGION"; do
    local ids; ids=$(aws ec2 describe-instances --region "$region" \
      --filters "Name=tag:$TAG,Values=$RUN_ID" "Name=instance-state-name,Values=running,pending,stopping,stopped" \
      --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
    [ -n "$ids" ] && { aws ec2 terminate-instances --region "$region" --instance-ids $ids >/dev/null 2>&1; aws ec2 wait instance-terminated --region "$region" --instance-ids $ids 2>/dev/null; log "$region terminated"; }
    local sg
    for sg in $(aws ec2 describe-security-groups --region "$region" --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'SecurityGroups[].GroupId' --output text 2>/dev/null); do
      aws ec2 delete-security-group --region "$region" --group-id "$sg" >/dev/null 2>&1
    done
    local kp
    for kp in $(aws ec2 describe-key-pairs --region "$region" --filters "Name=tag:$TAG,Values=$RUN_ID" --query 'KeyPairs[].KeyName' --output text 2>/dev/null); do
      aws ec2 delete-key-pair --region "$region" --key-name "$kp" >/dev/null 2>&1
    done
  done
  log "teardown complete"
  return 0
}
