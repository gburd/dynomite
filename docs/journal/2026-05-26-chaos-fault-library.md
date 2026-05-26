# 2026-05-26: chaos fault library (P3-3.8)

Extends the chaos injector beyond the
`SIGSTOP / SIGKILL / redis-bounce` rotation that has driven
chaos passes 1-4. Adds three new fault families gated behind
a `MODE_FAULTS` environment variable:

* `network`: tc-qdisc-driven partition, delay, loss, bandcap.
* `clock`: faketime-driven wall-clock skew (positive and
  negative).
* `disk`: tmpfs squeeze / full plus cgroups-v2 io.max latency
  injection.

Default behaviour (when `MODE_FAULTS` is unset) is unchanged
from the pre-2026-05-26 injector: legacy three-timer
process-only schedule, byte-for-byte identical.

## Files touched

* `scripts/chaos-multi-host/chaos-injector.sh` (rewritten):
  factored into `fault_*` subroutines; added `parse_classes`,
  `check_prereqs`, `cleanup_*`, unified `scheduler_unified`,
  preserved `scheduler_legacy` for the unset-env path. Made
  the script sourceable via a `BASH_SOURCE`-vs-`$0` guard so
  the smoke test can call individual fault routines.
* `scripts/chaos-multi-host/start-host.sh` (small change):
  honours `FAKETIME` env (and the `$RUN/clock-skew-active`
  marker file) to wrap the dynomited launch with `faketime`.
  No-op when faketime is missing.
* `scripts/chaos-multi-host/test_fault_smoke.sh` (new): per-
  fault smoke harness with prereq-driven skips and a final
  host-clean assertion.
* `docs/operations/chaos.md`: new "Fault library" section
  with each class's mechanics, prerequisites, and cleanup
  semantics.

## Design

### Class taxonomy

`MODE_FAULTS` is a comma-separated list. Each class has a
fixed sub-fault set; the unified scheduler weights classes
equally (per the spec) and picks a sub-fault uniformly within
the chosen class.

```
process  -> { pause, kill, redis_bounce }
network  -> { partition, delay, loss, bandcap }
clock    -> { skew (positive xor negative per cycle) }
disk     -> { squeeze, full, iolat }
```

`MODE_FAULTS=process,network` therefore gives a 50% process
chance and 50% network chance per cycle, then a uniform
sub-fault within whichever was picked. Cycle cadence is the
existing 60-180 s window.

### Default-when-unset

The spec mandates that default behaviour with `MODE_FAULTS`
unset is identical to today. We distinguish unset from
explicitly set with `${MODE_FAULTS+set}`:

```bash
MODE_FAULTS_EXPLICIT=0
if [ -n "${MODE_FAULTS+set}" ]; then
    MODE_FAULTS_EXPLICIT=1
fi
```

When unset, `main()` routes to `scheduler_legacy`, which is a
direct copy of the previous three-independent-timer loop
(NEXT_PAUSE / NEXT_KILL / NEXT_REDIS_BOUNCE). That keeps the
event-stream shape identical (`pause_start`, `pause_end`,
`kill`, `restart`, `redis_bounce`, `recovery_restart` events
all unchanged) so the existing report generator sees no
schema drift in default-mode runs.

When `MODE_FAULTS` is explicitly set (even to `=process`)
the injector switches to `scheduler_unified` and emits new
event kinds (`fault_<class>_<sub>` / `*_end` /
`*_skipped` / `scheduler_fire` / `injector_classes` /
`prereq_skip`). `generate-report.py` already buckets event
kinds histogrammatically so the new kinds appear in the
report's "Chaos events by kind" table without code changes.

### Cleanup-on-trap

Every fault subroutine has a paired cleanup. The trap
collapses to a single `cleanup_all` that calls each
class's cleanup unconditionally and idempotently:

* `cleanup_network`: `tc qdisc del dev <dev> root`. No-op if
  no qdisc was installed.
* `cleanup_clock`: `rm -f $RUN/clock-skew-active`. No-op if
  the marker is absent. Note we deliberately do NOT kill and
  restart dynomited under TERM: the coordinator's teardown
  phase wants a still-running dynomited so it can collect
  logs.
* `cleanup_disk`: `rm -f` both ballast files; iterate the
  iolat cgroup's `io.max` and reset every device entry to
  `max`. The cgroup itself is kept across faults to avoid
  the cgroup-process-move dance.

`cleanup_all` runs in two places:

1. The TERM/INT trap on the injector's top level.
2. The injector's `main()` startup, before
   `scheduler_legacy` or `scheduler_unified`. This scrubs
   any state a previous run left behind when it died
   mid-fault without running its trap (typical cause:
   `pkill -9 chaos-injector.sh`).

This is the load-bearing invariant of the design: the test
harness asserts the host is clean after the smoke; the
operator runs `MODE_FAULTS=...` passes back-to-back without
manual cleanup; an injector that crashes mid-fault does not
poison the next pass.

### Per-class prerequisite check

Each enabled class probes its prerequisites once at startup
and emits a `prereq_skip` event with the failure reason if
unmet. The probe path:

* `process`: trivially runnable (we always have the
  ability to signal child PIDs).
* `network`: `command -v tc` AND a no-op qdisc add+del. The
  ingress-handle probe is tried first because it is
  cheaper than the root-handle probe; the root-handle probe
  is the fallback so we don't spuriously succeed on hosts
  where ingress is allowed but root replacement is not.
* `clock`: `command -v faketime` only. We don't try to
  exec a no-op `faketime` because that would require
  spawning a child process per probe.
* `disk`: presence of `/sys/fs/cgroup/cgroup.controllers`
  with `io` listed, plus write access to `$RUN`. The disk
  class is "partially runnable" if write access is fine but
  the `io` controller is missing; in that case `iolat`
  self-skips at invocation time with a per-call
  `fault_disk_iolat_skipped` event.

The `injector_classes` event reports both the configured
list and the runnable list, so a chaos pass on a host where
some prereq is missing produces an unambiguous record of
which classes actually fired.

## Smoke test

`scripts/chaos-multi-host/test_fault_smoke.sh` sources the
injector in library mode (the `BASH_SOURCE != $0` short-
circuits `main`) and invokes each fault routine against a
tempdir that mimics the on-host `$ROOT` layout. Each fault
case:

1. Skip on missing prereqs (we do NOT pretend the fault ran;
   we record SKIP in the per-test summary).
2. Override the routine's long sleep with a 1 s sleep so the
   smoke runs in under 30 s end-to-end.
3. Spawn the fault in the background; poll for the
   observable state change (qdisc installed / marker file
   created / ballast file size > 0) within 3 s.
4. `wait` for the routine to finish naturally, then assert
   cleanup ran (state observably reverted).
5. End-of-run host-clean check: `tc qdisc show dev lo`
   reports nothing chaos-related; no marker / ballast file
   in `$RUN`.

A leaked qdisc / marker / ballast is a hard FAIL.

### Smoke results on the dev box

The dev box (floki, NixOS, unprivileged `gburd` user) cannot
exercise the network or clock classes natively because:

* `gburd` lacks `CAP_NET_ADMIN`.
* `faketime` is not in the global PATH.

Both gaps are bridged via flake-provided tooling and a user
network namespace:

```
$ bash scripts/chaos-multi-host/test_fault_smoke.sh
... PASS=5 SKIP=5 FAIL=0           (process + disk run; net+clock skip)

$ unshare -rn bash -c 'ip link set lo up;
    bash scripts/chaos-multi-host/test_fault_smoke.sh'
... PASS=9 SKIP=1 FAIL=0           (process + net + disk; clock skips)

$ nix-shell -p libfaketime --run \
    "bash scripts/chaos-multi-host/test_fault_smoke.sh"
... PASS=6 SKIP=4 FAIL=0           (process + disk + clock; net skips)

$ nix-shell -p libfaketime --run \
    "unshare -rn bash -c 'ip link set lo up;
       bash scripts/chaos-multi-host/test_fault_smoke.sh'"
... PASS=10 SKIP=0 FAIL=0          (full matrix)
```

Trap-driven mid-fault cleanup verified separately: a manual
test that installs a netem delay then sends SIGTERM mid-
sleep confirmed the trap fires `cleanup_network` and the
qdisc is gone afterwards.

## Operational notes

### Granting `CAP_NET_ADMIN` on the chaos hosts

The chaos rig runs as the unprivileged `gburd` user on
`floki` / `arnold` / `nuc` / `meh`. Three options to grant
`CAP_NET_ADMIN`:

1. Run the injector under `sudo` (loses isolation; not
   preferred).
2. `setcap cap_net_admin=ep /usr/bin/bash` on each host
   (broad; only acceptable for chaos-only hosts).
3. Run the injector under a user network namespace that
   isolates the qdisc state from the host's real interfaces.
   This is what the smoke uses (`unshare -rn`); the
   trade-off is that tc operations don't affect the real
   network and so the chaos pass becomes a no-op for
   inter-host traffic.

For real multi-host chaos, option 1 is what we use; option
3 is for the in-process Stage 16 chaos test only. The
coordinator does not currently set up either; operators
opt in by pre-granting the capability before launching the
pass.

### `faketime` install on chaos hosts

`floki` and `meh` (NixOS): `libfaketime` is in the flake's
dev shell, so `nix develop` + launching the injector from
within picks it up.

`arnold` (Fedora): `dnf install libfaketime` plus the
`/usr/bin/faketime` wrapper.

`nuc` (FreeBSD): `pkg install libfaketime`. The FreeBSD
package installs `faketime(1)` in `/usr/local/bin`.

### cgroups v2 + io controller

NixOS, Fedora, and FreeBSD's linuxulator all expose the
`io` controller. On NixOS specifically the unprivileged
user typically has access to the `user.slice` cgroup
delegated by systemd; the chaos injector's `iolat` sub-
fault writes to `/sys/fs/cgroup/chaos-iolat-<DC>`, which
needs to be either created by root once and chowned, or
delegated via systemd's `Delegate=yes`. Until the operator
sets one of those up, the `iolat` sub-fault self-skips
cleanly with `fault_disk_iolat_skipped {"reason":"cgroup-
mkdir-failed"}` events; the rest of the disk class still
runs.

## Forward references

Pass-3 and pass-4 reports under
`dist/chaos-reports/v0.1.0/` describe the
process-only-rotation chaos passes. P3-3.8 (this work)
unblocks pass-5 onward to exercise the full network + clock
+ disk fault matrix. The hard-constraint set of paths for
this stage forbids touching `dist/`, so the cross-references
to those reports live here in the journal rather than as
edits to the curated reports themselves; the lead is
expected to fold a "Lessons / forward references" line into
the next pass-5 report describing how P3-3.8 changed the
rotation.

## Verification

```
$ bash -n scripts/chaos-multi-host/chaos-injector.sh        # syntax: clean
$ shellcheck scripts/chaos-multi-host/chaos-injector.sh     # clean
$ shellcheck scripts/chaos-multi-host/test_fault_smoke.sh   # only SC1091
$ shellcheck scripts/chaos-multi-host/start-host.sh         # only pre-existing SC2034
$ shellcheck scripts/netem/*.sh                             # only pre-existing SC1091
$ bash scripts/check_no_todos.sh           # rc=0
$ bash scripts/check_no_port_comments.sh   # rc=0
$ bash scripts/check_ascii.sh              # rc=0
$ cargo build --workspace --all-targets --locked     # clean
```

Smoke matrix above: full matrix PASS=10 / SKIP=0 / FAIL=0
under `unshare -rn` + `nix-shell -p libfaketime`.

## Known limitations / deferred

* `fault_disk_iolat` approximates "5 ms+/op latency" via a
  throughput cap because cgroups v2 doesn't expose a direct
  latency-injection knob. The operator-visible effect (slow
  redis -> dispatcher queue growth -> client timeouts) is
  the same; if a future kernel ships an `io.latency` knob in
  cgroups v2 we'll switch to it.
* The 60-second integration test mentioned in the brief
  (`MODE_FAULTS=network` against a single-host dynomited
  with workload-driver) is gated on the dev box gaining
  `CAP_NET_ADMIN` outside a netns. The smoke harness
  validates each fault's install/cleanup mechanics without
  needing a real dynomited; the operator runs the
  integration test on the chaos rig hosts where they have
  the capability.
* `MODE_FAULTS=` (empty string) is interpreted as "no
  classes enabled", which leaves the unified scheduler with
  nothing to fire each cycle. Each cycle emits a
  `scheduler_noop` event so the operator can tell at a
  glance that the pass was running without any chaos.
