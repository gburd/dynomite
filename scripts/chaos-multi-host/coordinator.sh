#!/usr/bin/env bash
#
# Multi-host chaos coordinator (pass 3+).
#
# Drives a 4-DC dynomite cluster across:
#
#   floki  (this host) - DC1, Linux x86_64, has the source
#   arnold (Tailscale) - DC2, Fedora 44 x86_64
#   nuc    (LAN, via arnold ProxyJump) - DC3, FreeBSD 15 amd64
#   meh    (LAN) - DC4, Linux x86_64 (fish login shell)
#
# Each host runs:
#   * 1 redis (native on floki/nuc/meh, podman container on arnold)
#   * 1 dynomited bound on 0.0.0.0 with peer/client/stats ports
#   * 1 workload-driver.py issuing every Redis feature class to
#     127.0.0.1:CLIENT_LISTEN_PORT (the local dynomited)
#   * 1 chaos-injector.sh that SIGSTOP/SIGKILLs dynomited and
#     periodically bounces redis
#
# Remote command dispatch
# -----------------------
#
# Every SSH callsite uses the bash-stdin form:
#
#     "${RUNNER[@]}" bash -s <<EOF
#     <command body>
#     EOF
#
# The body is interpreted by bash on the remote host regardless
# of the operator's login shell. meh's login shell is fish;
# arnold/floki use bash; nuc uses /usr/local/bin/bash. The
# previous string-argument form ("${RUNNER[@]}" "<cmd>") was
# parsed by the remote login shell, which mangled bash-only
# syntax (env-prefix assignments, redirection chains, $! pid
# capture) under fish. The bash-stdin form sidesteps the login
# shell entirely.
#
# Variable interpolation still happens on the local side at
# heredoc-expansion time. Use <<EOF (unquoted) when the body
# needs to interpolate $VARS; use <<'EOF' (quoted) for static
# bodies. Remote-only $vars must be escaped as \$var.
#
# HOSTS_OVERRIDE
# --------------
#
# A comma-separated host filter. Unset means all four hosts run.
# Set to a subset (for example "meh") to exercise only those
# hosts. Used by smoke-coordinator.sh.

set -euo pipefail

# ---- configuration ----

RUN_ID="${RUN_ID:-$(date -u +%Y%m%d-%H%M%SZ)}"
DURATION="${CHAOS_DURATION_SECS:-7200}"   # 2 hours

REPO="/home/gburd/ws/dynomite"
LOCAL_LOGS="$REPO/target/chaos-multi-host/$RUN_ID"
mkdir -p "$LOCAL_LOGS"

# Source the driver fan-out helper (compute_driver_specs,
# driver_pidfile_for). Resolve it next to this script so the
# coordinator works from any cwd.
COORD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./driver-spec.sh
. "$COORD_DIR/driver-spec.sh"

DATASTORE_PORT=17100
DYN_LISTEN_PORT=18101
CLIENT_LISTEN_PORT=18102
STATS_LISTEN_PORT=22222
RIAK_PBC_PORT=21800
# Riak HTTP gateway port (MODE=combined riak instance).
# start-host.sh derives the same default (PBC + 1) internally and
# shifts it into the riak port band; we pin the base name here so
# the teardown / status tooling can reference it.
RIAK_HTTP_PORT=$((RIAK_PBC_PORT + 1))

# Base offered QPS per host. For MODE=combined this is split
# three ways across the redis, memcache, and riak drivers so the
# total load is unchanged from the single-driver modes.
BASE_QPS="${CHAOS_QPS:-200}"

# Differential-mode (P3-3.9) port shifts. The C `dynomite`
# reference proxy listens on Rust + 100 so a single host can
# run both proxies without colliding. start-host.sh derives
# the same shifts internally; this file pins the names used
# by the workload-driver fan-out flags.
CLIENT_LISTEN_PORT_C=$((CLIENT_LISTEN_PORT + 100))

MODE="${MODE:-redis}"
export MODE

# Mode validation. The coordinator dispatches to per-host
# start-host.sh which knows redis|memcache|riak|differential|
# combined. Reject anything else early so the operator sees the
# typo on the lead host instead of waiting for four parallel SSH
# failures.
case "$MODE" in
    redis|memcache|riak|differential|combined) ;;
    *)
        echo "unknown MODE=$MODE (expected redis|memcache|riak|differential|combined)" >&2
        exit 2
        ;;
esac

# Per-class retry budget passed through to workload-driver.py.
# Operator-typical Dynomite client SDKs retry once on NoTargets
# (transient gossip churn), never on Timeout (genuine
# unavailability), and twice on Closed (peer reset; the next
# reconnect almost always succeeds). The chaos rig adopts the
# same defaults unless an operator overrides via the env. Set
# ``RETRY_POLICY=""`` to disable retries entirely (matches the
# pre-2026-05-25 behaviour where every error counted as a
# failure); see ``docs/operations/chaos.md`` for the wider
# discussion.
#
# Pass-4 redis-mode triage (2026-05-25) showed Closed dominating
# the failure mix at >99.9% of failures during chaos cycles, with
# zero retries firing because Closed was not in the default
# policy. Two retries on Closed (vs one on NoTargets) reflects
# both the higher base rate and the cheap per-attempt cost (a
# single TCP reconnect against the local engine).
#
# Pass-5 (2026-05-26) added per-class exponential backoff with
# jitter to break the thundering-herd retries that re-saturated
# a freshly-restarted dynomited's listener. The new per-class
# suffix is ``:<base_ms>:<max_ms>``; sleep between attempts is
# ``min(base_ms * 2^attempt, max_ms) * uniform(0.5, 1.5)``.
RETRY_POLICY="${RETRY_POLICY-NoTargets:1:50:200,Timeout:0,Closed:2:100:1000}"
export RETRY_POLICY

# Per-DC distinct tokens. Distinct token slices on the ring
# force keys to hash into a specific DC, exercising outbound
# peer connections from the dispatcher's `Replicas` plan. With
# `DC_QUORUM` consistency, the dispatcher fans out to every
# replica in the local DC; with each DC owning a distinct token
# range and only one node per DC, that's still LocalDatastore
# for keys hashing into the local range and Replicas (cross-DC)
# for keys that don't. Pass-1 used identical tokens on every
# node so cross-DC routing was never triggered. Pass-3+ uses a
# 4-way split (floki=0, arnold=1G, nuc=2G, meh=3G of u32::MAX)
# so every DC owns one quadrant of the ring.
TOKENS_FLOKI="0"
TOKENS_ARNOLD="1073741824"
TOKENS_NUC="2147483648"
TOKENS_MEH="3221225472"

FLOKI_TS_IP="100.104.16.13"
ARNOLD_TS_IP="100.117.233.104"
ARNOLD_LAN_IP="192.168.1.37"
NUC_LAN_IP="192.168.1.61"
MEH_LAN_IP="192.168.1.185"

SSH_KEY="$HOME/.ssh/id_ed25519"
SSH_BASE_OPTS=(-o IdentitiesOnly=yes -i "$SSH_KEY"
               -o ControlMaster=no -o ControlPath=none
               -o StrictHostKeyChecking=accept-new
               -o ServerAliveInterval=30)

# All remote runners are bare-ssh: they exec `bash -s` on the
# remote and the script body arrives via stdin. No `bash -lc`
# wrapper, no remote string parsing.
ARNOLD_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" arnold)
NUC_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" -o ProxyJump=arnold gburd@nuc)
# LAN-direct path to nuc, bypassing arnold's ProxyJump. Used
# during teardown when arnold may be SIGSTOPped or restarting
# under chaos and the proxy hop is liable to wedge. May fail if
# floki cannot reach the nuc LAN over Tailscale subnet routing;
# the teardown handler falls back to NUC_SSH on failure.
NUC_DIRECT_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" "gburd@$NUC_LAN_IP")
MEH_SSH=(env SSH_AUTH_SOCK="" ssh "${SSH_BASE_OPTS[@]}" meh)

ARNOLD_RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"
NUC_RSYNC_E="ssh ${SSH_BASE_OPTS[*]} -o ProxyJump=arnold"
MEH_RSYNC_E="ssh ${SSH_BASE_OPTS[*]}"

# Local-floki runner: empty array so "${LOCAL_RUN[@]}" bash -s
# expands to `bash -s` and the heredoc fires the local shell.
LOCAL_RUN=()

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" | tee -a "$LOCAL_LOGS/coordinator.log" ; }

# HOSTS_OVERRIDE filter: empty/unset means all hosts run.
HOSTS_OVERRIDE="${HOSTS_OVERRIDE:-}"
host_enabled() {
    local h="$1"
    if [ -z "$HOSTS_OVERRIDE" ]; then
        return 0
    fi
    case ",$HOSTS_OVERRIDE," in
        *",$h,"*) return 0 ;;
        *)        return 1 ;;
    esac
}

# Failed-host tracking. A host enters FAILED_HOSTS when any of
# its bootstrap / start / workload / injector steps return
# non-zero. Downstream steps gate on host_active (which combines
# host_enabled with "not failed"), so a single failure -- e.g.
# memcached missing on nuc when MODE=memcache, or a stale
# dynomited binary built without --features riak when MODE=riak
# -- removes that host from the rest of the run instead of
# bubbling up via set -e and aborting the whole pass before the
# other hosts even start.
#
# Teardown and the post-run rsync skip hosts not in host_active,
# so a failed host does not produce cascading SSH errors.
FAILED_HOSTS=""

mark_host_failed() {
    local h="$1"; shift
    local reason="$*"
    case ",$FAILED_HOSTS," in
        *",$h,"*) ;;
        *) FAILED_HOSTS="${FAILED_HOSTS:+$FAILED_HOSTS,}$h" ;;
    esac
    log "  WARN host $h marked failed: $reason"
}

host_active() {
    local h="$1"
    host_enabled "$h" || return 1
    case ",$FAILED_HOSTS," in
        *",$h,"*) return 1 ;;
    esac
    return 0
}

# Per-run health flags inspected at coordinator exit.
#
# WORKLOAD_RUNNING: number of hosts whose start_workload returned
# 0. The coordinator exits non-zero if zero workload drivers ever
# launched (the run is genuinely broken; the duration sleep is
# pointless).
#
# DURATION_REACHED: set to 1 when the duration sleep completes
# without interrupt. Combined with WORKLOAD_RUNNING > 0 it
# satisfies the "at least one host ran for the full workload
# duration" exit criterion.
WORKLOAD_RUNNING=0
DURATION_REACHED=0

# Each host's view of the cluster. Pass-3 has full 4-way
# connectivity: floki and arnold see each other over Tailscale;
# nuc/meh are LAN; arnold acts as the LAN gateway for cross-DC
# (floki <-> arnold via Tailscale; arnold <-> nuc/meh via LAN).
# meh sees the LAN directly and reaches arnold/nuc on LAN; it
# reaches floki via the Tailscale-bridged arnold seed.

floki_seeds() {
    cat <<SEEDS
    - $ARNOLD_TS_IP:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
SEEDS
}

arnold_seeds() {
    cat <<SEEDS
    - $FLOKI_TS_IP:$DYN_LISTEN_PORT:rack-1:dc-floki:$TOKENS_FLOKI
    - $NUC_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-nuc:$TOKENS_NUC
    - $MEH_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-meh:$TOKENS_MEH
SEEDS
}

nuc_seeds() {
    cat <<SEEDS
    - $ARNOLD_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
    - $MEH_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-meh:$TOKENS_MEH
SEEDS
}

meh_seeds() {
    cat <<SEEDS
    - $ARNOLD_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-arnold:$TOKENS_ARNOLD
    - $NUC_LAN_IP:$DYN_LISTEN_PORT:rack-1:dc-nuc:$TOKENS_NUC
SEEDS
}

# ---- per-host start ----

# Generic remote-host start. Writes seeds.yml and start-args
# via nested heredocs (literal-quoted to disable a second round
# of expansion on the remote bash) and then invokes the
# start-host.sh script. The runner is the SSH array.
#
# Returns the SSH/start-host.sh exit code. The caller is
# responsible for marking the host failed when this returns
# non-zero; we do not call mark_host_failed here so the helper
# stays composable (start_floki, for example, has its own
# bootstrap branch that needs to call us under the same
# semantics).
start_host() {
    local label="$1"; shift
    local tokens="$1"; shift
    local seeds_str="$1"; shift
    local runner=("$@")
    log "starting $label tokens=$tokens"

    # FreeBSD's /bin/sh is a different shell than bash; pick
    # bash explicitly for the start-host script.
    local bash_path=/bin/bash
    case "$label" in
        dc-nuc) bash_path=/usr/local/bin/bash ;;
    esac

    local rc=0
    "${runner[@]}" bash -s <<EOF >> "$LOCAL_LOGS/$label-start.log" 2>&1 || rc=$?
set -euo pipefail
mkdir -p /scratch/dynomite-chaos/run /scratch/dynomite-chaos/logs

# Persist seeds.yml. Unique inner terminator avoids any
# accidental clash with seed payload bytes.
cat > /scratch/dynomite-chaos/run/seeds.yml <<'__CHAOS_SEEDS_END__'
$seeds_str
__CHAOS_SEEDS_END__

# Persist start-args so the chaos injector can restart
# dynomited with the same arguments after a SIGKILL. We bake
# MODE / TOKENS / port values into the file at write time so
# the chaos-injector's source of start-args sees the
# operator-selected mode rather than defaulting to 'redis'
# when MODE is unset in its environment.
#
# The SEEDS line is the tricky one: this whole heredoc is
# itself inside the outer SSH-payload heredoc, so escape
# levels matter. We need the FILE to contain literally:
#
#     SEEDS="$(cat /scratch/dynomite-chaos/run/seeds.yml)"
#
# - The DOUBLE QUOTES are mandatory: an unquoted multi-line
#   command substitution word-splits at source time and
#   bash interprets seed lines starting with '-' as commands.
#   Pass-6 hit exactly this on host meh: chaos-injector died
#   with 'SEEDS: unbound variable' at line 310.
# - The \$ on this side becomes $ after the OUTER heredoc
#   evaluates. The inner heredoc is unquoted on the remote
#   so it then evaluates $(cat seeds.yml) at write time,
#   baking the multi-line YAML into the file. The chaos
#   injector sources start-args; the quoted assignment
#   preserves newlines.
cat > /scratch/dynomite-chaos/run/start-args <<__CHAOS_ARGS_END__
MODE='$MODE'
TOKENS='$tokens'
SEEDS="\$(cat /scratch/dynomite-chaos/run/seeds.yml)"
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
RIAK_PBC_PORT=$RIAK_PBC_PORT
__CHAOS_ARGS_END__

if [ '$MODE' = combined ]; then
    # MODE=combined: three independent pools per host, one per
    # backend, on distinct port bands. start-host.sh selects the
    # data_store + backend + band from the INSTANCE env and
    # writes each instance under its own run subdir.
    cmb_rc=0
    for cmb_inst in redis memcache riak; do
        INSTANCE=\$cmb_inst MODE='$MODE' $bash_path /scratch/dynomite-chaos/src/scripts/chaos-multi-host/start-host.sh $label '$tokens' "\$(cat /scratch/dynomite-chaos/run/seeds.yml)" $DATASTORE_PORT $DYN_LISTEN_PORT $CLIENT_LISTEN_PORT $STATS_LISTEN_PORT $RIAK_PBC_PORT || cmb_rc=\$?
    done
    exit \$cmb_rc
else
    MODE='$MODE' $bash_path /scratch/dynomite-chaos/src/scripts/chaos-multi-host/start-host.sh $label '$tokens' "\$(cat /scratch/dynomite-chaos/run/seeds.yml)" $DATASTORE_PORT $DYN_LISTEN_PORT $CLIENT_LISTEN_PORT $STATS_LISTEN_PORT $RIAK_PBC_PORT
fi
EOF
    if [ "$rc" -ne 0 ]; then
        log "  $label start failed (rc=$rc); see $LOCAL_LOGS/$label-start.log"
        return "$rc"
    fi
    log "  $label dynomited up"
    return 0
}

start_floki() {
    log "preparing floki tokens=$TOKENS_FLOKI"
    local rc=0
    mkdir -p /scratch/dynomite-chaos/run /scratch/dynomite-chaos/logs /scratch/dynomite-chaos/build/release || rc=$?
    if [ "$rc" -eq 0 ]; then
        cp -f "$REPO/target/release/dynomited" /scratch/dynomite-chaos/build/release/dynomited || rc=$?
    fi
    if [ "$rc" -eq 0 ]; then
        # rsync source so the injector can find scripts via the
        # same /scratch/dynomite-chaos/src layout used on the
        # remotes.
        mkdir -p /scratch/dynomite-chaos/src || rc=$?
    fi
    if [ "$rc" -eq 0 ]; then
        rsync -a --delete --exclude target/ --exclude .git/ --exclude _/dynomite/.git/ \
            "$REPO"/ /scratch/dynomite-chaos/src/ || rc=$?
    fi
    if [ "$rc" -ne 0 ]; then
        log "  dc-floki bootstrap failed (rc=$rc)"
        return "$rc"
    fi
    SEEDS_STR=$(floki_seeds)
    cat > /scratch/dynomite-chaos/run/seeds.yml <<EOF
$SEEDS_STR
EOF
    cat > /scratch/dynomite-chaos/run/start-args <<EOF
MODE='$MODE'
TOKENS='$TOKENS_FLOKI'
SEEDS=\$(cat /scratch/dynomite-chaos/run/seeds.yml)
DATASTORE_PORT=$DATASTORE_PORT
DYN_LISTEN_PORT=$DYN_LISTEN_PORT
CLIENT_LISTEN_PORT=$CLIENT_LISTEN_PORT
STATS_LISTEN_PORT=$STATS_LISTEN_PORT
RIAK_PBC_PORT=$RIAK_PBC_PORT
EOF
    if [ "$MODE" = combined ]; then
        # MODE=combined: launch the three local pools (one per
        # backend) on distinct port bands. start-host.sh selects
        # the data_store + backend + band from INSTANCE.
        local cmb_inst
        for cmb_inst in redis memcache riak; do
            INSTANCE="$cmb_inst" bash "$REPO/scripts/chaos-multi-host/start-host.sh" \
                dc-floki "$TOKENS_FLOKI" "$SEEDS_STR" \
                "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" "$RIAK_PBC_PORT" \
                >> "$LOCAL_LOGS/dc-floki-start.log" 2>&1 || rc=$?
        done
    else
        bash "$REPO/scripts/chaos-multi-host/start-host.sh" \
            dc-floki "$TOKENS_FLOKI" "$SEEDS_STR" \
            "$DATASTORE_PORT" "$DYN_LISTEN_PORT" "$CLIENT_LISTEN_PORT" "$STATS_LISTEN_PORT" "$RIAK_PBC_PORT" \
            >> "$LOCAL_LOGS/dc-floki-start.log" 2>&1 || rc=$?
    fi
    if [ "$rc" -ne 0 ]; then
        log "  dc-floki start failed (rc=$rc); see $LOCAL_LOGS/dc-floki-start.log"
        return "$rc"
    fi
    log "  dc-floki dynomited up"
    return 0
}

# ---- workload + injector ----

start_workload() {
    local label="$1"; shift
    local _bash_path="$1"; shift
    local runner=("$@")
    log "starting workload-driver(s) on $label (mode=$MODE)"
    # Mode wiring varies per backend; compute_driver_specs (from
    # driver-spec.sh) centralises the mapping:
    #  * riak: dial the PBC listener at $RIAK_PBC_PORT, not the
    #    engine's client_listen.
    #  * differential (P3-3.9 phases 3+4): fan every op out to
    #    both the Rust and C proxies and record per-op
    #    agreed/divergent/one_side_failed verdicts.
    #  * combined: THREE drivers per host, one per co-located
    #    pool -- a redis (RESP + FT.*) driver on the redis band's
    #    client_listen, a memcache driver on the memcache band's
    #    client_listen, and a riak PBC driver on the riak band's
    #    pbc_listen -- with the offered QPS split three ways so
    #    total load is unchanged. Each spec carries its own
    #    band-shifted --port / --riak-pbc-port.
    #  * redis|memcache: the existing single-port path.
    #
    # Each spec is <api_suffix>\t<qps>\t<mode_flags>. The empty
    # suffix keeps the legacy workload-<label>.ndjson +
    # workload.pid layout; suffixed drivers write
    # workload-<label>-<api>.ndjson + driver-<api>.pid so
    # teardown can find and kill every driver.
    local specs
    specs="$(compute_driver_specs "$MODE" "$BASE_QPS" \
        "$CLIENT_LISTEN_PORT" "$CLIENT_LISTEN_PORT_C" "$RIAK_PBC_PORT")"
    local rc=0
    local launched=0
    local api_suffix d_qps d_flags
    while IFS=$'\t' read -r api_suffix d_qps d_flags; do
        [ -z "$d_qps" ] && continue
        local out_file="/scratch/dynomite-chaos/logs/workload-$label$api_suffix.ndjson"
        local err_file="/scratch/dynomite-chaos/logs/workload-$label$api_suffix.stderr"
        local pidfile
        pidfile="$(driver_pidfile_for "$api_suffix" /scratch/dynomite-chaos/run)"
        local one_rc=0
        "${runner[@]}" bash -s <<EOF || one_rc=$?
nohup python3 /scratch/dynomite-chaos/src/scripts/chaos-multi-host/workload-driver.py \\
    --host 127.0.0.1 --port $CLIENT_LISTEN_PORT \\
    $d_flags \\
    --label $label \\
    --out $out_file \\
    --duration $DURATION \\
    --qps $d_qps \\
    --retry-on='$RETRY_POLICY' \\
    > $err_file 2>&1 < /dev/null &
echo \$! > $pidfile
EOF
        if [ "$one_rc" -ne 0 ]; then
            log "  workload-driver$api_suffix start failed on $label (rc=$one_rc)"
            rc="$one_rc"
        else
            launched=$((launched + 1))
        fi
    done <<<"$specs"
    if [ "$rc" -ne 0 ] || [ "$launched" -eq 0 ]; then
        return 1
    fi
    return 0
}

start_injector() {
    local label="$1"; shift
    local bash_path="$1"; shift
    local runner=("$@")
    log "starting chaos-injector on $label"
    # P3-3.9 phase 5: when the differential rig is active,
    # tell the chaos-injector to fan SIGSTOP/SIGCONT/SIGKILL
    # out to BOTH the Rust dynomited and the C `dynomite`
    # reference proxy via the INJECT_C_PROXY_TOO env knob.
    # The chaos-injector reads the knob from start-args (if
    # the caller wrote it there) or from its own environment;
    # we use the env-prefix path because it lives entirely on
    # this side of the SSH boundary and does not require the
    # remote start-host.sh to know about phase-5 wiring.
    local injector_env=""
    if [ "$MODE" = "differential" ]; then
        injector_env="INJECT_C_PROXY_TOO=1 "
    fi
    local rc=0
    # Plumb the operator-selected fault classes through to the
    # remote injector. The injector's MODE_FAULTS env knob picks
    # which fault families to exercise; without this pass-through,
    # the remote always runs the default (process-only) regardless
    # of what the operator set in their shell.
    local mf="${MODE_FAULTS:-process}"
    "${runner[@]}" bash -s <<EOF || rc=$?
MODE_FAULTS=$mf ${injector_env}nohup $bash_path /scratch/dynomite-chaos/src/scripts/chaos-multi-host/chaos-injector.sh $label \\
    > /scratch/dynomite-chaos/logs/injector-$label.stderr 2>&1 < /dev/null &
echo \$! > /scratch/dynomite-chaos/run/injector.pid
EOF
    if [ "$rc" -ne 0 ]; then
        log "  chaos-injector start failed on $label (rc=$rc)"
        return "$rc"
    fi
    return 0
}

# ---- teardown ----

# Teardown timeout-and-continue policy.
#
# Every remote step here is wrapped in `timeout --signal=KILL`
# so a wedged SSH (e.g. ProxyJump hop stuck behind a
# SIGSTOPped arnold) cannot block the next chaos mode. A real
# teardown takes only seconds; the budgets below are generous.
#
#   step                   budget   on-timeout
#   ---------------------  -------  -----------------------------
#   dc-floki kill+cleanup  60s      WARN; continue to next host
#   dc-arnold kill+cleanup 60s      WARN; continue to next host
#   dc-nuc via direct LAN  30s      try ProxyJump fallback
#   dc-nuc via ProxyJump   60s      WARN; continue to next host
#   dc-meh kill+cleanup    60s      WARN; continue to next host
#   rsync arnold logs      60s      WARN; logs may be partial
#   rsync nuc logs         60s      WARN; logs may be partial
#   rsync meh logs         60s      WARN; logs may be partial
#
# Per-host teardown failures are NON-fatal. The coordinator's
# only job here is to free the next mode in the pass-3 sequence.
teardown() {
    log "==> TEARDOWN"

    # Single shell snippet executed on each host. The body is
    # piped to remote bash via `bash -s`, so $f and $(cat ...)
    # are evaluated on the remote side without any local-vs-
    # remote escaping confusion.
    local remote_cmd
    remote_cmd=$(cat <<'REMOTE_EOF'
RUN=/scratch/dynomite-chaos/run
# Driver + injector pids first (graceful TERM). MODE=combined
# adds driver-memcache.pid alongside the redis/riak drivers.
for f in "$RUN"/workload.pid "$RUN"/driver-redis.pid "$RUN"/driver-memcache.pid "$RUN"/driver-riak.pid "$RUN"/injector.pid; do
    [ -f "$f" ] && pid=$(cat "$f") && kill -TERM "$pid" 2>/dev/null
done
sleep 2
# Hard KILL sweep. Covers the single-mode dynomited.pid /
# dynomite-c.pid at the run root AND the MODE=combined
# per-instance dynomited pids under $RUN/<instance>/.
for f in "$RUN"/workload.pid "$RUN"/driver-redis.pid "$RUN"/driver-memcache.pid "$RUN"/driver-riak.pid "$RUN"/injector.pid "$RUN"/dynomited.pid "$RUN"/dynomite-c.pid "$RUN"/redis/dynomited.pid "$RUN"/memcache/dynomited.pid "$RUN"/riak/dynomited.pid; do
    [ -f "$f" ] && pid=$(cat "$f") && kill -KILL "$pid" 2>/dev/null
done
# External backends: the single-mode layout records one
# redis.pid at the run root; the combined layout records one
# per instance subdir (the riak/noxu instance has none).
for rp in "$RUN"/redis.pid "$RUN"/redis/redis.pid "$RUN"/memcache/redis.pid "$RUN"/riak/redis.pid; do
    [ -f "$rp" ] || continue
    id=$(cat "$rp")
    case "$id" in
        container:*) (command -v podman >/dev/null && podman rm -f "${id#container:}") || (command -v docker >/dev/null && docker rm -f "${id#container:}") ;;
        *) kill -KILL "$id" 2>/dev/null ;;
    esac
done
true
REMOTE_EOF
)

    # Teardown only iterates ACTIVE hosts (host_enabled and not
    # in FAILED_HOSTS). Hosts that never started never had their
    # /scratch/dynomite-chaos/run pidfiles populated, so calling
    # the per-host kill snippet against them would either no-op
    # (best case) or hit a wedged SSH (worst case) -- skipping
    # them avoids cascading errors that previously masked the
    # original failure in the report.
    if host_active floki; then
        log "  teardown dc-floki"
        timeout --signal=KILL 60s bash -s <<<"$remote_cmd" \
            >> "$LOCAL_LOGS/dc-floki-teardown.log" 2>&1 \
            || log "  WARN dc-floki teardown timed out after 60s; continuing"
    elif host_enabled floki; then
        log "  skip teardown dc-floki (host marked failed)"
    fi

    if host_active arnold; then
        log "  teardown dc-arnold"
        timeout --signal=KILL 60s "${ARNOLD_SSH[@]}" bash -s <<<"$remote_cmd" \
            >> "$LOCAL_LOGS/dc-arnold-teardown.log" 2>&1 \
            || log "  WARN dc-arnold teardown timed out after 60s; continuing"
    elif host_enabled arnold; then
        log "  skip teardown dc-arnold (host marked failed)"
    fi

    if host_active nuc; then
        # nuc: try LAN-direct first because the normal ProxyJump
        # route may be wedged when arnold is mid-chaos-restart.
        # Fall back to ProxyJump if the LAN is not reachable from
        # floki over Tailscale subnet routing.
        log "  teardown dc-nuc"
        if timeout --signal=KILL 30s "${NUC_DIRECT_SSH[@]}" bash -s <<<"$remote_cmd" \
                >> "$LOCAL_LOGS/dc-nuc-teardown.log" 2>&1; then
            log "    dc-nuc teardown via direct SSH"
        elif timeout --signal=KILL 60s "${NUC_SSH[@]}" bash -s <<<"$remote_cmd" \
                >> "$LOCAL_LOGS/dc-nuc-teardown.log" 2>&1; then
            log "    dc-nuc teardown via ProxyJump (direct failed)"
        else
            log "  WARN dc-nuc teardown timed out via both direct and ProxyJump; continuing"
        fi
    elif host_enabled nuc; then
        log "  skip teardown dc-nuc (host marked failed)"
    fi

    if host_active meh; then
        log "  teardown dc-meh"
        timeout --signal=KILL 60s "${MEH_SSH[@]}" bash -s <<<"$remote_cmd" \
            >> "$LOCAL_LOGS/dc-meh-teardown.log" 2>&1 \
            || log "  WARN dc-meh teardown timed out after 60s; continuing"
    elif host_enabled meh; then
        log "  skip teardown dc-meh (host marked failed)"
    fi

    if host_active arnold; then
        log "  rsync arnold logs"
        timeout --signal=KILL 60s \
            rsync -az -e "$ARNOLD_RSYNC_E" \
                arnold:/scratch/dynomite-chaos/logs/ "$LOCAL_LOGS/arnold-logs/" \
            || log "  WARN arnold log rsync timed out after 60s; continuing"
    fi
    if host_active nuc; then
        log "  rsync nuc logs"
        timeout --signal=KILL 60s \
            rsync -az -e "$NUC_RSYNC_E" \
                gburd@nuc:/scratch/dynomite-chaos/logs/ "$LOCAL_LOGS/nuc-logs/" \
            || log "  WARN nuc log rsync timed out after 60s; continuing"
    fi
    if host_active meh; then
        log "  rsync meh logs"
        timeout --signal=KILL 60s \
            rsync -az -e "$MEH_RSYNC_E" \
                meh:/scratch/dynomite-chaos/logs/ "$LOCAL_LOGS/meh-logs/" \
            || log "  WARN meh log rsync timed out after 60s; continuing"
    fi
    if host_active floki; then
        log "  copy floki logs"
        cp -r /scratch/dynomite-chaos/logs "$LOCAL_LOGS/floki-logs" 2>/dev/null || true
    fi

    log "  done; logs at $LOCAL_LOGS"
}

# ---- main ----

trap teardown EXIT INT TERM

log "================================================================"
log "multi-host chaos coordinator starting"
log "  run id:   $RUN_ID"
log "  duration: $DURATION s"
log "  mode:     $MODE"
log "  retry:    ${RETRY_POLICY:-<none>}"
if [ -n "$HOSTS_OVERRIDE" ]; then
    log "  hosts:    $HOSTS_OVERRIDE (HOSTS_OVERRIDE)"
else
    log "  hosts:    floki arnold nuc meh"
fi
log "  logs:     $LOCAL_LOGS"
log "================================================================"

# Self-healing source bootstrap: if a remote host's
# /scratch/dynomite-chaos/src is missing, rsync the local
# source there before the start step needs it. Fast no-op when
# the tree is already present and up to date.
#
# Returns non-zero if any of the rsync / ssh steps fail; the
# caller marks the host failed. Each step explicitly captures
# its own exit code so the function still propagates errors
# even when called from a `||` context (where bash disables
# `set -e` for the entire function body).
#
# Issue B (Pass-7 nuc bootstrap failure): the rsync transport
# (`-e ssh ... -o ProxyJump=arnold`) fails on nuc with
# `rsync: connection unexpectedly closed` even though plain
# `ssh -J arnold nuc` works. The ProxyJump hop tunnels small
# command streams happily but tears down rsync's bandwidth-
# heavy stream when nuc's Tailscale-to-floki path is degraded.
# As a defensive fallback we now:
#   1. probe the runner with a `bash -c 'echo alive'` first;
#      if even that fails, skip rsync and surface the SSH
#      breakage clearly;
#   2. attempt rsync as before;
#   3. if rsync fails but the runner probe succeeded, fall
#      back to a `tar | ssh tar -x` pipe. tar over a single
#      ssh channel is more tolerant of flaky ProxyJump
#      tunnels than rsync's protocol negotiation, at the
#      cost of always sending a full tree (no delta).
bootstrap_remote_src() {
    local label="$1"; shift
    local rsync_target="$1"; shift
    local rsync_e="$1"; shift
    local push_binary="${1:-yes}"; shift 2>/dev/null || true
    local mkdir_runner=("$@")
    log "bootstrap $label src (push_binary=$push_binary)"

    # Probe the SSH runner first. If we can't even shell out
    # to the host, neither rsync nor tar will work; mark
    # failed without burning the rsync timeout budget.
    local probe_rc=0
    timeout --signal=KILL 30s "${mkdir_runner[@]}" bash -s <<'EOF' >/dev/null 2>&1 || probe_rc=$?
echo alive
EOF
    if [ "$probe_rc" -ne 0 ]; then
        log "  $label: ssh probe failed (rc=$probe_rc); host unreachable"
        return "$probe_rc"
    fi

    local rc=0
    "${mkdir_runner[@]}" bash -s <<'EOF' || rc=$?
mkdir -p /scratch/dynomite-chaos/src \
         /scratch/dynomite-chaos/run \
         /scratch/dynomite-chaos/logs \
         /scratch/dynomite-chaos/build/release
EOF
    if [ "$rc" -ne 0 ]; then
        return "$rc"
    fi

    # Source-tree push. rsync first; tar pipe on failure.
    local src_rc=0
    rsync -a --delete \
        --exclude target/ --exclude .git/ --exclude _/dynomite/.git/ \
        -e "$rsync_e" \
        "$REPO/" "$rsync_target:/scratch/dynomite-chaos/src/" || src_rc=$?
    if [ "$src_rc" -ne 0 ]; then
        log "  $label: rsync src failed (rc=$src_rc); falling back to tar | ssh"
        # tar-pipe fallback: stream a tarball through the
        # already-proven SSH channel. The runner array is
        # the SSH command; we append a remote `bash -c` that
        # extracts into the destination. We do NOT ship
        # --delete semantics (tar can only add/overwrite),
        # but the destination dir was created above with
        # mkdir -p so a pre-existing partial src tree is
        # acceptable -- the operator's source tree is
        # additive over a session anyway.
        if ! tar -cf - -C "$REPO" \
                --exclude=target \
                --exclude=.git \
                --exclude=_/dynomite/.git \
                . \
            | "${mkdir_runner[@]}" bash -c 'tar -xf - -C /scratch/dynomite-chaos/src/'; then
            log "  $label: tar | ssh fallback also failed"
            return 1
        fi
        log "  $label: tar | ssh fallback succeeded"
    fi

    # Ship the locally-built dynomited binary when the remote
    # OS+arch matches the build host. nuc runs FreeBSD, so the
    # caller passes push_binary=no and the operator is
    # expected to maintain a FreeBSD-native binary at
    # /scratch/dynomite-chaos/build/release/dynomited.
    if [ "$push_binary" = "yes" ] && [ -x "$REPO/target/release/dynomited" ]; then
        local bin_rc=0
        rsync -a -e "$rsync_e" \
            "$REPO/target/release/dynomited" \
            "$rsync_target:/scratch/dynomite-chaos/build/release/dynomited" || bin_rc=$?
        if [ "$bin_rc" -ne 0 ]; then
            log "  $label: rsync binary failed (rc=$bin_rc); falling back to ssh cat"
            # Smaller payload than the source tree, but the
            # same channel-quality issue applies. cat-pipe
            # is the smallest reasonable fallback.
            if ! "${mkdir_runner[@]}" bash -c \
                    'cat > /scratch/dynomite-chaos/build/release/dynomited && chmod +x /scratch/dynomite-chaos/build/release/dynomited' \
                    < "$REPO/target/release/dynomited"; then
                log "  $label: ssh cat binary fallback also failed"
                return 1
            fi
            log "  $label: ssh cat binary fallback succeeded"
        fi
    fi
    return 0
}

# Per-host bootstrap. A failure here removes the host from the
# rest of the run; downstream steps gate on host_active.
if host_active arnold; then
    bootstrap_remote_src dc-arnold arnold "$ARNOLD_RSYNC_E" no "${ARNOLD_SSH[@]}" \
        || mark_host_failed arnold "bootstrap_remote_src failed"
fi
if host_active nuc; then
    bootstrap_remote_src dc-nuc gburd@nuc "$NUC_RSYNC_E" no "${NUC_SSH[@]}" \
        || mark_host_failed nuc "bootstrap_remote_src failed"
fi
if host_active meh; then
    bootstrap_remote_src dc-meh meh "$MEH_RSYNC_E" yes "${MEH_SSH[@]}" \
        || mark_host_failed meh "bootstrap_remote_src failed"
fi

# MODE=differential phase 1+2 substrate: ensure each active
# host has the C `dynomite` reference binary built and cached
# under /scratch/dynomite-chaos/cref-build/dynomite. The
# helper is idempotent against the submodule's git commit
# hash, so re-runs are cheap. A build failure on a single host
# marks that host failed (and is excluded from the rest of
# the run) but does not abort the coordinator -- the
# coordinator-robustness work from the fault-library stage
# already provides this behaviour.
#
# Phases 3-5 (workload fan-out, reply comparison, chaos
# integration) are documented in
# docs/journal/2026-05-26-differential-chaos-substrate.md and
# are explicit follow-ups; this stage only stands the parallel
# clusters up.
if [ "$MODE" = "differential" ]; then
    log "==> MODE=differential: ensuring C dynomite on each active host"
    if host_active floki; then
        bash "$REPO/scripts/chaos-multi-host/build_cref_remote.sh" floki \
            >> "$LOCAL_LOGS/dc-floki-cref-build.log" 2>&1 \
            || mark_host_failed floki "build_cref_remote.sh failed"
    fi
    if host_active arnold; then
        bash "$REPO/scripts/chaos-multi-host/build_cref_remote.sh" arnold \
            >> "$LOCAL_LOGS/dc-arnold-cref-build.log" 2>&1 \
            || mark_host_failed arnold "build_cref_remote.sh failed"
    fi
    if host_active nuc; then
        bash "$REPO/scripts/chaos-multi-host/build_cref_remote.sh" nuc \
            >> "$LOCAL_LOGS/dc-nuc-cref-build.log" 2>&1 \
            || mark_host_failed nuc "build_cref_remote.sh failed (FreeBSD; expected; see journal)"
    fi
    if host_active meh; then
        bash "$REPO/scripts/chaos-multi-host/build_cref_remote.sh" meh \
            >> "$LOCAL_LOGS/dc-meh-cref-build.log" 2>&1 \
            || mark_host_failed meh "build_cref_remote.sh failed"
    fi
fi

src_check() {
    local label="$1"; shift
    local runner=("$@")
    local rc=0
    "${runner[@]}" bash -s <<'EOF' || rc=$?
[ -d /scratch/dynomite-chaos/src ]
EOF
    if [ "$rc" -ne 0 ]; then
        log "$label:src missing (rc=$rc)"
        return "$rc"
    fi
    return 0
}

if host_active arnold; then
    src_check arnold "${ARNOLD_SSH[@]}" || mark_host_failed arnold "src_check failed"
fi
if host_active nuc; then
    src_check nuc "${NUC_SSH[@]}" || mark_host_failed nuc "src_check failed"
fi
if host_active meh; then
    src_check meh "${MEH_SSH[@]}" || mark_host_failed meh "src_check failed"
fi

if host_active floki; then
    start_floki || mark_host_failed floki "start_floki failed"
fi
if host_active arnold; then
    start_host dc-arnold "$TOKENS_ARNOLD" "$(arnold_seeds)" "${ARNOLD_SSH[@]}" \
        || mark_host_failed arnold "start_host failed"
fi
if host_active nuc; then
    start_host dc-nuc "$TOKENS_NUC" "$(nuc_seeds)" "${NUC_SSH[@]}" \
        || mark_host_failed nuc "start_host failed"
fi
if host_active meh; then
    start_host dc-meh "$TOKENS_MEH" "$(meh_seeds)" "${MEH_SSH[@]}" \
        || mark_host_failed meh "start_host failed"
fi

# Brief settle so any deferred state is in place.
sleep 5

if host_active floki; then
    if start_workload dc-floki /bin/bash "${LOCAL_RUN[@]}"; then
        WORKLOAD_RUNNING=$((WORKLOAD_RUNNING + 1))
    else
        mark_host_failed floki "start_workload failed"
    fi
fi
if host_active arnold; then
    if start_workload dc-arnold /bin/bash "${ARNOLD_SSH[@]}"; then
        WORKLOAD_RUNNING=$((WORKLOAD_RUNNING + 1))
    else
        mark_host_failed arnold "start_workload failed"
    fi
fi
if host_active nuc; then
    if start_workload dc-nuc /usr/local/bin/bash "${NUC_SSH[@]}"; then
        WORKLOAD_RUNNING=$((WORKLOAD_RUNNING + 1))
    else
        mark_host_failed nuc "start_workload failed"
    fi
fi
if host_active meh; then
    if start_workload dc-meh /bin/bash "${MEH_SSH[@]}"; then
        WORKLOAD_RUNNING=$((WORKLOAD_RUNNING + 1))
    else
        mark_host_failed meh "start_workload failed"
    fi
fi

if host_active floki; then
    start_injector dc-floki /bin/bash "${LOCAL_RUN[@]}" \
        || mark_host_failed floki "start_injector failed"
fi
if host_active arnold; then
    start_injector dc-arnold /bin/bash "${ARNOLD_SSH[@]}" \
        || mark_host_failed arnold "start_injector failed"
fi
if host_active nuc; then
    start_injector dc-nuc /usr/local/bin/bash "${NUC_SSH[@]}" \
        || mark_host_failed nuc "start_injector failed"
fi
if host_active meh; then
    start_injector dc-meh /bin/bash "${MEH_SSH[@]}" \
        || mark_host_failed meh "start_injector failed"
fi

if [ -n "$FAILED_HOSTS" ]; then
    log "  hosts failed during start: $FAILED_HOSTS"
fi

if [ "$WORKLOAD_RUNNING" -eq 0 ]; then
    log "==> ERROR: zero workload-drivers launched; not sleeping for $DURATION s"
    log "==> failed hosts: ${FAILED_HOSTS:-<none>}"
    trap - EXIT INT TERM
    teardown
    log "==> coordinator done (no host completed the workload duration)"
    exit 1
fi

log "==> $WORKLOAD_RUNNING workload-driver(s) up; sleeping for $DURATION seconds"
sleep "$DURATION"
DURATION_REACHED=1

log "==> duration elapsed"
trap - EXIT INT TERM
teardown
log "==> coordinator done"

if [ "$WORKLOAD_RUNNING" -ge 1 ] && [ "$DURATION_REACHED" -eq 1 ]; then
    log "==> exit 0 ($WORKLOAD_RUNNING host(s) completed the workload duration; failed: ${FAILED_HOSTS:-<none>})"
    exit 0
fi
log "==> exit 1 (no host completed the workload duration)"
exit 1
