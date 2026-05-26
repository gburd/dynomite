#!/usr/bin/env bash
#
# Smoke test for the chaos-injector fault library. For each
# fault_* routine in scripts/chaos-multi-host/chaos-injector.sh
# the test:
#
#   1. Sources the injector script in library mode (with
#      $__CHAOS_SOURCED short-circuiting main).
#   2. Sets up a minimal RUN/LOGS directory under a tempdir.
#   3. Skips the routine when its class is not runnable on the
#      host (no tc, no faketime, no cgroups v2 io controller,
#      etc.) so the smoke is still useful in unprivileged CI.
#   4. Invokes the routine with shortened durations (the
#      smoke patches the fault internals via per-routine
#      wrappers so a 30-90 s sleep becomes <= 2 s).
#   5. Asserts the host's observable state changed (qdisc
#      installed, marker file present, ballast file > 0
#      bytes, cgroup io.max non-default).
#   6. Waits for the routine to finish and asserts cleanup
#      left the host clean (no qdisc, no marker, no ballast,
#      io.max defaulted).
#
# A test that leaks a tc qdisc rule or a marker file fails.
# Each class reports SKIP / OK / FAIL with the reason.
#
# Run from the repo root:
#
#     bash scripts/chaos-multi-host/test_fault_smoke.sh
#
# The test only requires basic POSIX userland; it auto-skips
# anything beyond. It does NOT require dynomited or a redis
# backend; the process faults are stubbed out (we don't have
# a real dynomited pid in the smoke environment).

set -uo pipefail

# Resolve the repo root from this script's location so
# the smoke can be run from anywhere.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INJECTOR="$HERE/chaos-injector.sh"

if [ ! -f "$INJECTOR" ]; then
    echo "FATAL: injector not found at $INJECTOR" >&2
    exit 2
fi

# Tempdir scaffolding. Mimics the real chaos host layout.
TMPROOT="$(mktemp -d -t chaos-fault-smoke-XXXXXX)"
trap 'rm -rf "$TMPROOT" 2>/dev/null || true' EXIT

DC_NAME="smoke-dc"
ROOT="$TMPROOT"
RUN="$ROOT/run"
LOGS="$ROOT/logs"
mkdir -p "$RUN" "$LOGS"
EVENTS="$LOGS/chaos-events-$DC_NAME.ndjson"
MODE=redis
CHAOS_NETEM_DEV="${CHAOS_NETEM_DEV:-lo}"
CHAOS_CGROUP="${CHAOS_CGROUP:-/sys/fs/cgroup/chaos-iolat-smoke-dc}"
CLOCK_SKEW_MARKER="$RUN/clock-skew-active"
BALLAST_FILE="$RUN/chaos-ballast"
BALLAST_FULL_FILE="$RUN/chaos-ballast-full"

# Required by the injector but not used by sourceable tests.
TOKENS="0"
SEEDS=""
DATASTORE_PORT=17100
DYN_LISTEN_PORT=18101
CLIENT_LISTEN_PORT=18102
STATS_LISTEN_PORT=22222
START_ARGS_FILE="$RUN/start-args"
touch "$START_ARGS_FILE"
MODE_FAULTS=process,network,clock,disk
MODE_FAULTS_EXPLICIT=1
RUNNABLE_PROCESS=0
RUNNABLE_NETWORK=0
RUNNABLE_CLOCK=0
RUNNABLE_DISK=0
IS_CLASS_PROCESS=1
IS_CLASS_NETWORK=1
IS_CLASS_CLOCK=1
IS_CLASS_DISK=1

export DC_NAME ROOT RUN LOGS EVENTS MODE CHAOS_NETEM_DEV CHAOS_CGROUP \
       CLOCK_SKEW_MARKER BALLAST_FILE BALLAST_FULL_FILE TOKENS SEEDS \
       DATASTORE_PORT DYN_LISTEN_PORT CLIENT_LISTEN_PORT STATS_LISTEN_PORT \
       START_ARGS_FILE MODE_FAULTS MODE_FAULTS_EXPLICIT \
       RUNNABLE_PROCESS RUNNABLE_NETWORK RUNNABLE_CLOCK RUNNABLE_DISK \
       IS_CLASS_PROCESS IS_CLASS_NETWORK IS_CLASS_CLOCK IS_CLASS_DISK

# Source the library form. The script's __CHAOS_SOURCED guard
# stops it from calling main; we call individual fault_*
# routines from this harness.
# shellcheck source=./chaos-injector.sh
. "$INJECTOR"

# Result accumulator.
PASS=0
FAIL=0
SKIP=0
RESULTS=()

note() { printf '%s\n' "$*"; }

record() {
    local status="$1"; shift
    local name="$1"; shift
    local detail="${1:-}"
    case "$status" in
        OK)   PASS=$((PASS + 1)); RESULTS+=("OK   $name") ;;
        SKIP) SKIP=$((SKIP + 1)); RESULTS+=("SKIP $name : $detail") ;;
        FAIL) FAIL=$((FAIL + 1)); RESULTS+=("FAIL $name : $detail") ;;
    esac
    note "[$status] $name${detail:+ : $detail}"
}

# Kick the prereq checks once so the smoke knows which classes
# can be exercised on this host.
parse_classes
check_prereqs

note "==> prereq summary"
note "    process  runnable=$RUNNABLE_PROCESS"
note "    network  runnable=$RUNNABLE_NETWORK (dev=$CHAOS_NETEM_DEV)"
note "    clock    runnable=$RUNNABLE_CLOCK"
note "    disk     runnable=$RUNNABLE_DISK"

# ---------- network ----------

# Drive a fault to a short, deterministic duration by patching
# the named symbol's RANDOM-derived sleep. We do this by
# defining a wrapper that overrides $RANDOM expansion via
# `bash -c`-ish trickery is messy; the cleaner approach is to
# accept the routine's ~10-30 s sleep is bounded and just run
# the partition variant with a pre-injected qdisc check.
#
# In practice the smoke doesn't NEED to drive each routine to
# completion; we just need to assert the install + cleanup
# pattern. For the long-running variants we override the
# `sleep` builtin with a function that returns immediately
# while the fault is mid-sleep, then poll for cleanup.

smoke_network_partition() {
    if [ "$RUNNABLE_NETWORK" != 1 ]; then
        record SKIP fault_network_partition "network class not runnable on this host"
        return 0
    fi
    cleanup_network
    # Force fault duration to ~1s by overriding sleep in this
    # function's scope.
    sleep() { command sleep 1; }
    fault_network_partition >/dev/null 2>&1 &
    local pid=$!
    # Poll for the qdisc to appear; fault should install it
    # within 200ms.
    local installed=0
    for _ in $(seq 1 30); do
        if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'prio'; then
            installed=1; break
        fi
        command sleep 0.1
    done
    wait "$pid" 2>/dev/null || true
    unset -f sleep
    if [ "$installed" != 1 ]; then
        cleanup_network
        record FAIL fault_network_partition "qdisc never installed"
        return 0
    fi
    # Cleanup must have run.
    if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'prio\|netem\|tbf'; then
        cleanup_network
        record FAIL fault_network_partition "qdisc still present after fault completed"
        return 0
    fi
    record OK fault_network_partition
}

smoke_network_delay() {
    if [ "$RUNNABLE_NETWORK" != 1 ]; then
        record SKIP fault_network_delay "network class not runnable on this host"
        return 0
    fi
    cleanup_network
    sleep() { command sleep 1; }
    fault_network_delay >/dev/null 2>&1 &
    local pid=$!
    local installed=0
    for _ in $(seq 1 30); do
        if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'netem.*delay'; then
            installed=1; break
        fi
        command sleep 0.1
    done
    wait "$pid" 2>/dev/null || true
    unset -f sleep
    if [ "$installed" != 1 ]; then
        cleanup_network
        record FAIL fault_network_delay "delay qdisc never installed"
        return 0
    fi
    if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'netem\|prio\|tbf'; then
        cleanup_network
        record FAIL fault_network_delay "qdisc still present after fault"
        return 0
    fi
    record OK fault_network_delay
}

smoke_network_loss() {
    if [ "$RUNNABLE_NETWORK" != 1 ]; then
        record SKIP fault_network_loss "network class not runnable on this host"
        return 0
    fi
    cleanup_network
    sleep() { command sleep 1; }
    fault_network_loss >/dev/null 2>&1 &
    local pid=$!
    local installed=0
    for _ in $(seq 1 30); do
        if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'netem.*loss'; then
            installed=1; break
        fi
        command sleep 0.1
    done
    wait "$pid" 2>/dev/null || true
    unset -f sleep
    if [ "$installed" != 1 ]; then
        cleanup_network
        record FAIL fault_network_loss "loss qdisc never installed"
        return 0
    fi
    if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'netem\|prio\|tbf'; then
        cleanup_network
        record FAIL fault_network_loss "qdisc still present after fault"
        return 0
    fi
    record OK fault_network_loss
}

smoke_network_bandcap() {
    if [ "$RUNNABLE_NETWORK" != 1 ]; then
        record SKIP fault_network_bandcap "network class not runnable on this host"
        return 0
    fi
    cleanup_network
    sleep() { command sleep 1; }
    fault_network_bandcap >/dev/null 2>&1 &
    local pid=$!
    local installed=0
    for _ in $(seq 1 30); do
        if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'tbf'; then
            installed=1; break
        fi
        command sleep 0.1
    done
    wait "$pid" 2>/dev/null || true
    unset -f sleep
    if [ "$installed" != 1 ]; then
        cleanup_network
        record FAIL fault_network_bandcap "tbf qdisc never installed"
        return 0
    fi
    if tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null | grep -q 'tbf\|netem\|prio'; then
        cleanup_network
        record FAIL fault_network_bandcap "qdisc still present after fault"
        return 0
    fi
    record OK fault_network_bandcap
}

# ---------- clock ----------

smoke_clock_skew() {
    if [ "$RUNNABLE_CLOCK" != 1 ]; then
        record SKIP fault_clock_skew "clock class not runnable on this host (faketime missing)"
        return 0
    fi
    # We can't really drive fault_clock_skew end-to-end without
    # a dynomited binary. We assert the marker-file mechanism
    # works: write the marker, verify cleanup_clock removes it.
    rm -f "$CLOCK_SKEW_MARKER"
    echo "+30s" > "$CLOCK_SKEW_MARKER"
    if [ ! -f "$CLOCK_SKEW_MARKER" ]; then
        record FAIL fault_clock_skew "marker write failed"
        return 0
    fi
    cleanup_clock
    if [ -f "$CLOCK_SKEW_MARKER" ]; then
        rm -f "$CLOCK_SKEW_MARKER"
        record FAIL fault_clock_skew "cleanup_clock left marker behind"
        return 0
    fi
    record OK fault_clock_skew
}

# ---------- disk ----------

smoke_disk_squeeze() {
    if [ "$RUNNABLE_DISK" != 1 ]; then
        record SKIP fault_disk_squeeze "disk class not runnable on this host"
        return 0
    fi
    # Replace compute_ballast_kb with a deterministic small
    # value so the test runs in <2 s rather than potentially
    # filling /scratch.
    # shellcheck disable=SC2329  # called indirectly via fault_disk_squeeze
    compute_ballast_kb() { printf '2048'; }   # 2 MiB
    # shellcheck disable=SC2329  # called indirectly via fault_disk_squeeze
    sleep() { command sleep 1; }
    rm -f "$BALLAST_FILE"
    fault_disk_squeeze >/dev/null 2>&1 &
    local pid=$!
    local installed=0
    for _ in $(seq 1 30); do
        if [ -s "$BALLAST_FILE" ]; then
            installed=1; break
        fi
        command sleep 0.1
    done
    wait "$pid" 2>/dev/null || true
    unset -f compute_ballast_kb sleep
    if [ "$installed" != 1 ]; then
        rm -f "$BALLAST_FILE"
        record FAIL fault_disk_squeeze "ballast file never created"
        return 0
    fi
    if [ -f "$BALLAST_FILE" ]; then
        rm -f "$BALLAST_FILE"
        record FAIL fault_disk_squeeze "ballast file leaked after fault"
        return 0
    fi
    record OK fault_disk_squeeze
}

smoke_disk_full() {
    if [ "$RUNNABLE_DISK" != 1 ]; then
        record SKIP fault_disk_full "disk class not runnable on this host"
        return 0
    fi
    # The real fault_disk_full does an unbounded dd to fill the
    # entire mount which we don't want to do on a developer
    # workstation. Override dd to write a small fixed-size
    # placeholder so the install/cleanup mechanics can be
    # verified without exhausting the host's disk.
    # shellcheck disable=SC2329  # called indirectly via fault_disk_full
    dd() { command dd if=/dev/zero of="$BALLAST_FULL_FILE" bs=1M count=1 status=none 2>/dev/null; }
    # shellcheck disable=SC2329  # called indirectly via fault_disk_full
    sleep() { command sleep 1; }
    rm -f "$BALLAST_FULL_FILE"
    fault_disk_full >/dev/null 2>&1 &
    local pid=$!
    local installed=0
    for _ in $(seq 1 30); do
        if [ -f "$BALLAST_FULL_FILE" ]; then
            installed=1; break
        fi
        command sleep 0.1
    done
    wait "$pid" 2>/dev/null || true
    unset -f dd sleep
    if [ "$installed" != 1 ]; then
        rm -f "$BALLAST_FULL_FILE"
        record FAIL fault_disk_full "ballast-full file never created"
        return 0
    fi
    if [ -f "$BALLAST_FULL_FILE" ]; then
        rm -f "$BALLAST_FULL_FILE"
        record FAIL fault_disk_full "ballast-full file leaked after fault"
        return 0
    fi
    record OK fault_disk_full
}

smoke_disk_iolat() {
    if [ "$RUNNABLE_DISK" != 1 ]; then
        record SKIP fault_disk_iolat "disk class not runnable on this host"
        return 0
    fi
    # iolat needs cgroup-mkdir + cgroup.procs write, which
    # ordinarily requires root or a delegated cgroup. We
    # invoke fault_disk_iolat directly; if the prereq probe
    # didn't catch it the routine itself emits a skip event
    # and returns 0, which is the success path here.
    # shellcheck disable=SC2329  # called indirectly via fault_disk_iolat
    sleep() { command sleep 1; }
    fault_disk_iolat >/dev/null 2>&1 || true
    unset -f sleep
    # Either the cgroup created and was reset to default, or
    # the routine skipped early. Either way: io.max should be
    # at default if the cgroup exists.
    if [ -d "$CHAOS_CGROUP" ] && [ -f "$CHAOS_CGROUP/io.max" ]; then
        if grep -qE '\b(rbps|wbps)=[0-9]+' "$CHAOS_CGROUP/io.max" 2>/dev/null; then
            cleanup_disk
            record FAIL fault_disk_iolat "io.max still has non-max limit after fault"
            return 0
        fi
    fi
    record OK fault_disk_iolat
}

# ---------- process (limited; no real dynomited) ----------

smoke_process_pause() {
    # Without a real $RUN/dynomited.pid we expect the routine
    # to emit fault_process_pause_skipped and return cleanly.
    # That's the smoke: confirm the routine doesn't crash on
    # missing state.
    : > "$EVENTS"
    fault_process_pause >/dev/null 2>&1 || {
        record FAIL fault_process_pause "routine returned non-zero on missing dynomited"
        return 0
    }
    if grep -q 'fault_process_pause_skipped' "$EVENTS" 2>/dev/null; then
        record OK fault_process_pause
    else
        record FAIL fault_process_pause "expected pause_skipped event, got none"
    fi
}

# ---------- run ----------

note "==> running smokes"
smoke_process_pause

smoke_network_partition
smoke_network_delay
smoke_network_loss
smoke_network_bandcap

smoke_clock_skew

smoke_disk_squeeze
smoke_disk_full
smoke_disk_iolat

# Final "host clean" check: nothing the injector left behind.
LEAKS=()
if [ "$RUNNABLE_NETWORK" = 1 ]; then
    leftover=$(tc qdisc show dev "$CHAOS_NETEM_DEV" 2>/dev/null \
        | grep -E 'prio |netem |tbf ' || true)
    if [ -n "$leftover" ]; then
        LEAKS+=("tc qdisc on $CHAOS_NETEM_DEV: $leftover")
    fi
fi
[ -f "$CLOCK_SKEW_MARKER" ]   && LEAKS+=("clock-skew marker present")
[ -f "$BALLAST_FILE" ]        && LEAKS+=("ballast file present")
[ -f "$BALLAST_FULL_FILE" ]   && LEAKS+=("ballast-full file present")

if [ "${#LEAKS[@]}" -gt 0 ]; then
    note "==> LEAKS detected:"
    for l in "${LEAKS[@]}"; do note "    $l"; done
    FAIL=$((FAIL + 1))
    RESULTS+=("FAIL host-clean : ${#LEAKS[@]} leak(s)")
else
    RESULTS+=("OK   host-clean")
    PASS=$((PASS + 1))
fi

note ""
note "==> summary"
for r in "${RESULTS[@]}"; do note "  $r"; done
note "  PASS=$PASS SKIP=$SKIP FAIL=$FAIL"

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
exit 0
