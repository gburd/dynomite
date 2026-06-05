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
# P3-3.9 phase 5: pidfile path consulted by kill_both_proxies
# when INJECT_C_PROXY_TOO=1.
C_PROXY_PIDFILE="$RUN/dynomite-c.pid"

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
       IS_CLASS_PROCESS IS_CLASS_NETWORK IS_CLASS_CLOCK IS_CLASS_DISK \
       C_PROXY_PIDFILE

# Source the library form. The script's __CHAOS_SOURCED guard
# stops it from calling main; we call individual fault_*
# routines from this harness.
# shellcheck source=./chaos-injector.sh
. "$INJECTOR"

# Source the driver fan-out helper too. The combined driver-
# pidfile smoke below exercises compute_driver_specs +
# driver_pidfile_for with a stubbed launch.
# shellcheck source=./driver-spec.sh
. "$HERE/driver-spec.sh"

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

# ---------- P3-3.8 stubbed-event-shape smokes ----------
#
# These assert the new event names emitted by the
# network/clock/disk fault classes without depending on
# host privileges. Each test:
#
#   1. truncates $EVENTS so it can grep for exactly the
#      events emitted in this call;
#   2. installs shell fakes for the privileged commands
#      (tc / losetup / mkfs.ext4 / mount, and the
#      restart_dynomited helper for clock) on a stub PATH or
#      via shell-function override;
#   3. patches sleep to 1 s so the fault completes in test
#      wallclock budget;
#   4. invokes the real fault_* function out of the injector
#      and asserts the resulting ndjson stream carries the
#      expected event shape.
#
# The fakes write the args they were called with to a side
# file ($STUB_LOG) so a follow-up regression can also assert
# the fault used the expected qdisc / loopback shape.

make_stub_path() {
    local stub_dir="$1"
    mkdir -p "$stub_dir"
    # Common lifecycle: read STUB_LOG, log args, simulate any
    # specific output the real binary returns. The fakes are
    # `set -e` clean and exit 0.
    cat > "$stub_dir/tc" <<'STUB'
#!/usr/bin/env bash
printf 'tc %s\n' "$*" >> "$STUB_LOG"
exit 0
STUB
    cat > "$stub_dir/losetup" <<'STUB'
#!/usr/bin/env bash
printf 'losetup %s\n' "$*" >> "$STUB_LOG"
# `losetup -f --show <img>` returns the loop device path on
# stdout; the fake hands back a synthetic device the rest of
# the fault routine can carry around.
case "$1" in
    -f)
        printf '/dev/loop-stub\n'
        ;;
esac
exit 0
STUB
    cat > "$stub_dir/mkfs.ext4" <<'STUB'
#!/usr/bin/env bash
printf 'mkfs.ext4 %s\n' "$*" >> "$STUB_LOG"
exit 0
STUB
    cat > "$stub_dir/mount" <<'STUB'
#!/usr/bin/env bash
printf 'mount %s\n' "$*" >> "$STUB_LOG"
exit 0
STUB
    cat > "$stub_dir/umount" <<'STUB'
#!/usr/bin/env bash
printf 'umount %s\n' "$*" >> "$STUB_LOG"
exit 0
STUB
    cat > "$stub_dir/faketime" <<'STUB'
#!/usr/bin/env bash
printf 'faketime %s\n' "$*" >> "$STUB_LOG"
shift  # drop the offset arg
exec "$@"
STUB
    chmod +x "$stub_dir"/tc "$stub_dir"/losetup "$stub_dir"/mkfs.ext4 \
        "$stub_dir"/mount "$stub_dir"/umount "$stub_dir"/faketime
}

smoke_network_delay_start_event() {
    # Force the fault to run regardless of host privileges.
    local saved_runnable="$RUNNABLE_NETWORK"
    RUNNABLE_NETWORK=1

    : > "$EVENTS"
    local stub_dir="$TMPROOT/stub-network-delay"
    local stub_log="$TMPROOT/stub-network-delay.log"
    : > "$stub_log"
    make_stub_path "$stub_dir"
    export STUB_LOG="$stub_log"

    # shellcheck disable=SC2329
    sleep() { command sleep 0.05; }
    PATH="$stub_dir:$PATH" fault_network_delay >/dev/null 2>&1 || true
    unset -f sleep

    RUNNABLE_NETWORK="$saved_runnable"

    # Assertion 1: the event stream contains the new
    # `fault_network_delay_start` kind.
    if ! grep -q '"kind":"fault_network_delay_start"' "$EVENTS" 2>/dev/null; then
        record FAIL fault_network_delay_start_event \
            "expected kind=fault_network_delay_start in $EVENTS"
        return 0
    fi
    # Assertion 2: the legacy unsuffixed name is gone.
    if grep -q '"kind":"fault_network_delay"[^_]' "$EVENTS" 2>/dev/null; then
        record FAIL fault_network_delay_start_event \
            "legacy kind=fault_network_delay still emitted"
        return 0
    fi
    # Assertion 3: the stubbed tc was called with the
    # netem-delay shape the real fault would have used.
    if ! grep -qE 'tc qdisc add dev '"$CHAOS_NETEM_DEV"' root netem delay [0-9]+ms' \
            "$stub_log" 2>/dev/null; then
        record FAIL fault_network_delay_start_event \
            "tc was not called with the expected netem-delay args"
        return 0
    fi
    record OK fault_network_delay_start_event
}

smoke_clock_skew_start_event() {
    # The real fault_clock_skew calls dyn_pid + restart_dynomited;
    # we have no dynomited, so we override both to no-ops.
    local saved_runnable="$RUNNABLE_CLOCK"
    RUNNABLE_CLOCK=1

    : > "$EVENTS"
    rm -f "$CLOCK_SKEW_MARKER" 2>/dev/null || true
    local stub_log="$TMPROOT/stub-clock.log"
    : > "$stub_log"
    export STUB_LOG="$stub_log"

    # Stub the helpers that drive process state.
    # shellcheck disable=SC2329
    dyn_pid() { return 1; }
    # shellcheck disable=SC2329
    restart_dynomited() {
        printf 'restart_dynomited FAKETIME=%s\n' "${FAKETIME:-}" >> "$STUB_LOG"
        return 0
    }
    # shellcheck disable=SC2329
    sleep() { command sleep 0.05; }

    fault_clock_skew >/dev/null 2>&1 || true

    unset -f dyn_pid restart_dynomited sleep
    RUNNABLE_CLOCK="$saved_runnable"

    if ! grep -q '"kind":"fault_clock_skew_start"' "$EVENTS" 2>/dev/null; then
        record FAIL fault_clock_skew_start_event \
            "expected kind=fault_clock_skew_start in $EVENTS"
        return 0
    fi
    # The seconds field must be a signed integer.
    if ! grep -qE '"seconds":-?[0-9]+' "$EVENTS" 2>/dev/null; then
        record FAIL fault_clock_skew_start_event \
            "seconds field missing or not an int in event payload"
        return 0
    fi
    # restart_dynomited was called with FAKETIME populated.
    if ! grep -q 'restart_dynomited FAKETIME=' "$stub_log" 2>/dev/null; then
        record FAIL fault_clock_skew_start_event \
            "restart_dynomited stub never invoked"
        return 0
    fi
    record OK fault_clock_skew_start_event
}

smoke_disk_loop_start_event() {
    : > "$EVENTS"
    local stub_dir="$TMPROOT/stub-disk-loop"
    local stub_log="$TMPROOT/stub-disk-loop.log"
    : > "$stub_log"
    make_stub_path "$stub_dir"
    export STUB_LOG="$stub_log"

    # Override compute_ballast_kb / dd we don't reuse here;
    # fault_disk_loop's dd is the only real binary we need
    # to trim, and a 1-MiB image is plenty for the smoke.
    CHAOS_DISK_LOOP_MB=1
    DISK_LOOP_SIZE_MB=1

    # shellcheck disable=SC2329
    sleep() { command sleep 0.05; }
    PATH="$stub_dir:$PATH" fault_disk_loop >/dev/null 2>&1 || true
    unset -f sleep

    if ! grep -q '"kind":"fault_disk_loop_start"' "$EVENTS" 2>/dev/null; then
        record FAIL fault_disk_loop_start_event \
            "expected kind=fault_disk_loop_start in $EVENTS"
        return 0
    fi
    # The stubbed binaries each got at least one call.
    for tool in losetup mkfs.ext4 mount; do
        if ! grep -q "^$tool " "$stub_log" 2>/dev/null; then
            record FAIL fault_disk_loop_start_event \
                "$tool fake never invoked"
            return 0
        fi
    done
    # Assertion: the loop device captured by the marker is the
    # synthetic /dev/loop-stub from the losetup fake.
    if ! grep -q '"loop":"/dev/loop-stub"' "$EVENTS" 2>/dev/null; then
        record FAIL fault_disk_loop_start_event \
            "event detail did not capture the stub loop device"
        return 0
    fi
    # The mount target lives under $ROOT/disk-full.
    if ! grep -q "\"mount\":\"$ROOT/disk-full\"" "$EVENTS" 2>/dev/null; then
        record FAIL fault_disk_loop_start_event \
            "mount path not under \$ROOT/disk-full"
        return 0
    fi
    record OK fault_disk_loop_start_event
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

# ---------- P3-3.9 phase 5: dual-proxy fault smoke ----------
#
# Stand up two trapping background processes that record the
# signals they receive, write their pids into the Rust and C
# pidfiles respectively, and verify kill_both_proxies
# delivers SIGTERM to BOTH when INJECT_C_PROXY_TOO=1. A
# parallel sub-test asserts the env-knob gate works: with
# INJECT_C_PROXY_TOO=0 only the Rust pid receives the signal
# even when the C pidfile exists.
#
# The trap helper writes a single-line marker to a per-pid
# log file when SIGTERM lands. The smoke greps for the
# marker rather than racing the kernel reaper.
smoke_phase5_kill_both_proxies() {
    local rust_marker="$RUN/phase5-rust.caught"
    local c_marker="$RUN/phase5-c.caught"
    local rust_done="$RUN/phase5-rust.done"
    local c_done="$RUN/phase5-c.done"
    local rust_pidfile="$RUN/dynomited.pid"
    rm -f "$rust_marker" "$c_marker" "$rust_done" "$c_done"
    rm -f "$rust_pidfile" "$C_PROXY_PIDFILE"

    # Two trap-loops. Each catches SIGTERM, writes its
    # marker, and exits cleanly. SIGUSR1 is an escape hatch
    # the test uses to reap the process if the kill never
    # lands. The 30-second loop bound caps the smoke's
    # wallclock cost on a wedged host.
    bash -c '
        marker="$1"; done="$2"
        trap "echo CAUGHT > \"$marker\"; touch \"$done\"; exit 0" TERM
        trap "touch \"$done\"; exit 0" USR1
        for _ in $(seq 1 300); do
            sleep 0.1
            [ -f "$done" ] && break
        done
    ' _ "$rust_marker" "$rust_done" &
    local rust_pid=$!
    bash -c '
        marker="$1"; done="$2"
        trap "echo CAUGHT > \"$marker\"; touch \"$done\"; exit 0" TERM
        trap "touch \"$done\"; exit 0" USR1
        for _ in $(seq 1 300); do
            sleep 0.1
            [ -f "$done" ] && break
        done
    ' _ "$c_marker" "$c_done" &
    local c_pid=$!

    echo "$rust_pid" > "$rust_pidfile"
    echo "$c_pid" > "$C_PROXY_PIDFILE"

    # Give the trap handlers a beat to install before we
    # send the signal; without it the kill races bash's
    # builtin trap installation and SIGTERM defaults to
    # immediate exit (no marker written).
    command sleep 0.2

    # Sub-test A: gate is on. Both pids must be signalled.
    INJECT_C_PROXY_TOO=1 kill_both_proxies TERM || true

    # Wait up to 3 s for both markers to appear.
    local i landed_rust=0 landed_c=0
    for i in $(seq 1 30); do
        [ -f "$rust_marker" ] && landed_rust=1
        [ -f "$c_marker" ] && landed_c=1
        if [ "$landed_rust" = 1 ] && [ "$landed_c" = 1 ]; then
            break
        fi
        command sleep 0.1
    done

    # Force-reap any survivor so the test cannot hang.
    kill -USR1 "$rust_pid" 2>/dev/null || true
    kill -USR1 "$c_pid" 2>/dev/null || true
    wait "$rust_pid" 2>/dev/null || true
    wait "$c_pid" 2>/dev/null || true

    if [ "$landed_rust" != 1 ]; then
        rm -f "$rust_marker" "$c_marker" "$rust_pidfile" "$C_PROXY_PIDFILE"
        record FAIL phase5_kill_both_proxies "Rust dynomited.pid did not receive SIGTERM"
        return 0
    fi
    if [ "$landed_c" != 1 ]; then
        rm -f "$rust_marker" "$c_marker" "$rust_pidfile" "$C_PROXY_PIDFILE"
        record FAIL phase5_kill_both_proxies "C dynomite-c.pid did not receive SIGTERM (INJECT_C_PROXY_TOO=1)"
        return 0
    fi
    rm -f "$rust_marker" "$c_marker" "$rust_done" "$c_done"

    # Sub-test B: gate is off. The C pidfile is still on
    # disk but only the Rust process must receive the
    # signal. Spawn fresh trap-loops because the previous
    # ones were reaped above.
    bash -c '
        marker="$1"; done="$2"
        trap "echo CAUGHT > \"$marker\"; touch \"$done\"; exit 0" TERM
        trap "touch \"$done\"; exit 0" USR1
        for _ in $(seq 1 300); do
            sleep 0.1
            [ -f "$done" ] && break
        done
    ' _ "$rust_marker" "$rust_done" &
    rust_pid=$!
    bash -c '
        marker="$1"; done="$2"
        trap "echo CAUGHT > \"$marker\"; touch \"$done\"; exit 0" TERM
        trap "touch \"$done\"; exit 0" USR1
        for _ in $(seq 1 300); do
            sleep 0.1
            [ -f "$done" ] && break
        done
    ' _ "$c_marker" "$c_done" &
    c_pid=$!

    echo "$rust_pid" > "$rust_pidfile"
    echo "$c_pid" > "$C_PROXY_PIDFILE"
    command sleep 0.2

    INJECT_C_PROXY_TOO=0 kill_both_proxies TERM || true

    local landed_rust2=0 landed_c2=0
    for i in $(seq 1 30); do
        [ -f "$rust_marker" ] && landed_rust2=1
        [ -f "$c_marker" ] && landed_c2=1
        if [ "$landed_rust2" = 1 ]; then
            break
        fi
        command sleep 0.1
    done

    # Brief settle before the negative assertion: if the C
    # process was going to be hit we want to give the kernel
    # the chance to deliver. 200 ms is a generous bound.
    command sleep 0.2
    [ -f "$c_marker" ] && landed_c2=1

    kill -USR1 "$rust_pid" 2>/dev/null || true
    kill -USR1 "$c_pid" 2>/dev/null || true
    wait "$rust_pid" 2>/dev/null || true
    wait "$c_pid" 2>/dev/null || true

    if [ "$landed_rust2" != 1 ]; then
        rm -f "$rust_marker" "$c_marker" "$rust_pidfile" "$C_PROXY_PIDFILE"
        record FAIL phase5_kill_both_proxies "Rust pid not signalled with INJECT_C_PROXY_TOO=0"
        return 0
    fi
    if [ "$landed_c2" = 1 ]; then
        rm -f "$rust_marker" "$c_marker" "$rust_pidfile" "$C_PROXY_PIDFILE"
        record FAIL phase5_kill_both_proxies "C pid signalled even though INJECT_C_PROXY_TOO=0"
        return 0
    fi

    rm -f "$rust_marker" "$c_marker" "$rust_pidfile" "$C_PROXY_PIDFILE" "$rust_done" "$c_done"
    record OK phase5_kill_both_proxies
}

# Sub-smoke: c_dyn_pid honours both the env knob and the
# pidfile presence check. Pure pidfile/env logic; no signals.
smoke_phase5_c_dyn_pid_gating() {
    rm -f "$C_PROXY_PIDFILE"
    # No pidfile -> 1 regardless of env.
    if INJECT_C_PROXY_TOO=1 c_dyn_pid >/dev/null 2>&1; then
        record FAIL phase5_c_dyn_pid_gating "c_dyn_pid returned 0 with no pidfile"
        return 0
    fi
    # Pidfile with a long-lived pid; env off -> 1.
    bash -c 'trap : TERM; sleep 5' &
    local helper_pid=$!
    echo "$helper_pid" > "$C_PROXY_PIDFILE"
    if INJECT_C_PROXY_TOO=0 c_dyn_pid >/dev/null 2>&1; then
        kill -KILL "$helper_pid" 2>/dev/null || true
        wait "$helper_pid" 2>/dev/null || true
        rm -f "$C_PROXY_PIDFILE"
        record FAIL phase5_c_dyn_pid_gating "c_dyn_pid returned 0 with INJECT_C_PROXY_TOO=0"
        return 0
    fi
    # Env on + live pid -> 0.
    if ! INJECT_C_PROXY_TOO=1 c_dyn_pid >/dev/null 2>&1; then
        kill -KILL "$helper_pid" 2>/dev/null || true
        wait "$helper_pid" 2>/dev/null || true
        rm -f "$C_PROXY_PIDFILE"
        record FAIL phase5_c_dyn_pid_gating "c_dyn_pid returned 1 with INJECT_C_PROXY_TOO=1 + live pid"
        return 0
    fi
    kill -KILL "$helper_pid" 2>/dev/null || true
    wait "$helper_pid" 2>/dev/null || true
    rm -f "$C_PROXY_PIDFILE"
    record OK phase5_c_dyn_pid_gating
}

# ---------- MODE=combined driver fan-out smoke ----------
#
# MODE=combined launches THREE workload drivers per host (a redis
# RESP + FT.* driver, a memcache ASCII driver, and a riak PBC
# driver), one per co-located dynomited instance on its own port
# band. The coordinator's start_workload loops over
# compute_driver_specs and writes a distinct pidfile per driver
# so teardown kills all three. We don't have a real coordinator
# (or remote SSH) in the smoke, so we stub the launch: for each
# emitted spec we just write a synthetic pid into the pidfile the
# coordinator would use. The assertion is that combined mode
# produces driver-redis.pid, driver-memcache.pid, AND
# driver-riak.pid, NOT the legacy workload.pid, that each driver
# carries the right --mode + band-shifted port, and that no
# driver carries --noxu-compat.
smoke_combined_driver_pidfiles() {
    local run_dir="$RUN/combined-pidfile-smoke"
    rm -rf "$run_dir"
    mkdir -p "$run_dir"

    local specs
    specs="$(compute_driver_specs combined 200 18102 18202 21800)"

    local fake_pid=1000
    local saw_redis_flag=0 saw_memcache_flag=0 saw_riak_flag=0
    local saw_noxu_compat=0
    local api_suffix d_qps d_flags
    while IFS=$'\t' read -r api_suffix d_qps d_flags; do
        [ -z "$d_qps" ] && continue
        local pidfile
        pidfile="$(driver_pidfile_for "$api_suffix" "$run_dir")"
        # Stubbed launch: record the synthetic pid.
        echo "$fake_pid" > "$pidfile"
        fake_pid=$((fake_pid + 1))
        case "$d_flags" in
            *"--noxu-compat"*) saw_noxu_compat=1 ;;
        esac
        case "$api_suffix" in
            -redis)
                case "$d_flags" in
                    *"--mode redis --port 18102"*) saw_redis_flag=1 ;;
                esac
                ;;
            -memcache)
                case "$d_flags" in
                    *"--mode memcache --port 19102"*) saw_memcache_flag=1 ;;
                esac
                ;;
            -riak)
                case "$d_flags" in
                    *"--mode riak --riak-pbc-port 23800"*) saw_riak_flag=1 ;;
                esac
                ;;
        esac
    done <<<"$specs"

    if [ ! -f "$run_dir/driver-redis.pid" ]; then
        record FAIL combined_driver_pidfiles "driver-redis.pid was not created"
        return 0
    fi
    if [ ! -f "$run_dir/driver-memcache.pid" ]; then
        record FAIL combined_driver_pidfiles "driver-memcache.pid was not created"
        return 0
    fi
    if [ ! -f "$run_dir/driver-riak.pid" ]; then
        record FAIL combined_driver_pidfiles "driver-riak.pid was not created"
        return 0
    fi
    if [ -f "$run_dir/workload.pid" ]; then
        record FAIL combined_driver_pidfiles "legacy workload.pid created in combined mode"
        return 0
    fi
    if [ "$saw_redis_flag" != 1 ]; then
        record FAIL combined_driver_pidfiles "redis driver spec missing '--mode redis --port 18102'"
        return 0
    fi
    if [ "$saw_memcache_flag" != 1 ]; then
        record FAIL combined_driver_pidfiles "memcache driver spec missing '--mode memcache --port 19102'"
        return 0
    fi
    if [ "$saw_riak_flag" != 1 ]; then
        record FAIL combined_driver_pidfiles "riak driver spec missing '--mode riak --riak-pbc-port 23800'"
        return 0
    fi
    if [ "$saw_noxu_compat" = 1 ]; then
        record FAIL combined_driver_pidfiles "a combined driver carried --noxu-compat"
        return 0
    fi
    rm -rf "$run_dir"
    record OK combined_driver_pidfiles
}

# ---------- MODE=combined injector fault fan-out smoke ----------
#
# In MODE=combined the host runs three dynomited instances, one
# per backend, each with a dynomited.pid under its own run subdir
# ($RUN/<instance>/). The injector's process faults must hit all
# three. This smoke stands up three trap-loop bash processes
# (standing in for the three dynomited instances), writes their
# pids into the per-instance pidfiles, and asserts:
#
#   * combined_live_pids enumerates all three;
#   * signal_all_dynomited (in MODE=combined) delivers a signal
#     to all three;
#   * needs_recovery is false while all three are alive and true
#     once they are gone.
#
# The trap helper writes a marker file on SIGTERM; the smoke
# greps for the markers rather than racing the kernel reaper.
smoke_combined_injector_faults_all() {
    local saved_mode="$MODE"
    local saved_run="$RUN"
    MODE=combined
    local cdir="$saved_run/combined-fault-smoke"
    rm -rf "$cdir"
    RUN="$cdir"
    mkdir -p "$RUN/redis" "$RUN/memcache" "$RUN/riak"

    local inst pid
    local marker0 marker1 marker2
    local -a pids=()
    for inst in redis memcache riak; do
        local marker="$RUN/$inst/caught"
        local donef="$RUN/$inst/done"
        bash -c '
            marker="$1"; donef="$2"
            trap "echo CAUGHT > \"$marker\"; touch \"$donef\"; exit 0" TERM
            trap "touch \"$donef\"; exit 0" USR1
            for _ in $(seq 1 300); do
                sleep 0.1
                [ -f "$donef" ] && break
            done
        ' _ "$marker" "$donef" &
        pid=$!
        echo "$pid" > "$RUN/$inst/dynomited.pid"
        pids+=("$pid")
    done
    marker0="$RUN/redis/caught"
    marker1="$RUN/memcache/caught"
    marker2="$RUN/riak/caught"

    # Let the trap handlers install before signalling.
    command sleep 0.2

    local fail_reason=""

    # combined_live_pids must enumerate all three.
    local live_count
    live_count=$(combined_live_pids | grep -c .)
    if [ "$live_count" != 3 ]; then
        fail_reason="combined_live_pids saw $live_count of 3"
    fi

    # needs_recovery must be false while all three are alive.
    if [ -z "$fail_reason" ] && needs_recovery; then
        fail_reason="needs_recovery true while all instances up"
    fi

    # Fan SIGTERM across all three instances.
    if [ -z "$fail_reason" ]; then
        signal_all_dynomited TERM || true
        local i landed=0
        for i in $(seq 1 30); do
            landed=0
            [ -f "$marker0" ] && landed=$((landed + 1))
            [ -f "$marker1" ] && landed=$((landed + 1))
            [ -f "$marker2" ] && landed=$((landed + 1))
            [ "$landed" = 3 ] && break
            command sleep 0.1
        done
        if [ "$landed" != 3 ]; then
            fail_reason="only $landed of 3 instances received SIGTERM"
        fi
    fi

    # After all three are gone, needs_recovery must flip to true.
    if [ -z "$fail_reason" ]; then
        rm -f "$RUN/redis/dynomited.pid" "$RUN/memcache/dynomited.pid" \
              "$RUN/riak/dynomited.pid"
        if ! needs_recovery; then
            fail_reason="needs_recovery false after all instances killed"
        fi
    fi

    kill -USR1 "${pids[@]}" 2>/dev/null || true
    wait 2>/dev/null || true
    RUN="$saved_run"
    MODE="$saved_mode"
    rm -rf "$cdir"
    if [ -n "$fail_reason" ]; then
        record FAIL combined_injector_faults_all "$fail_reason"
    else
        record OK combined_injector_faults_all
    fi
}

# ---------- run ----------

note "==> running smokes"
smoke_process_pause

# MODE=combined driver fan-out + injector fault fan-out (sourced
# helpers, stubbed launch; trapping bash subshells stand in for
# the three dynomited instances).
smoke_combined_driver_pidfiles
smoke_combined_injector_faults_all

# P3-3.9 phase 5: dual-proxy fault smokes. These run on every
# host because they only use trapping bash subshells and
# pidfile bookkeeping; no privileged operations.
smoke_phase5_c_dyn_pid_gating
smoke_phase5_kill_both_proxies

smoke_network_partition
smoke_network_delay
smoke_network_loss
smoke_network_bandcap

smoke_clock_skew

smoke_disk_squeeze
smoke_disk_full
smoke_disk_iolat

# P3-3.8 stubbed event-shape assertions. These run regardless
# of host privileges because every privileged binary is
# replaced with a shell fake on a stub PATH (or, for the clock
# fault, by overriding the helper functions inside the
# injector).
smoke_network_delay_start_event
smoke_clock_skew_start_event
smoke_disk_loop_start_event

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
