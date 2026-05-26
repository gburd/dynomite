# 2026-05-26 - Robust chaos coordinator (pass-4 follow-up)

## Summary

Pass-4 (multi-host redis chaos, 4 hosts, 2h, 91.08% success)
exposed two coordinator-level fragilities that block the next
multi-mode chaos pass from completing cleanly:

1. **One bad host kills the whole pass.** When `nuc` (FreeBSD)
   lacks `memcached` on PATH, `start-host.sh` exits 1, the
   coordinator's `set -e` propagates that failure, and the EXIT
   trap fires teardown before the other three hosts even start.
   Same dynamic for `riak` mode against the operator-installed
   FreeBSD-native dynomited binary (built without
   `--features riak`). The pass-4 report shows two of three
   modes died at the 1-minute mark for this reason.
2. **Retries do not fire under chaos.** 99.9% of pass-4 redis
   failures landed in class `Closed` (peer dropped the
   connection mid-stream during a SIGKILL cycle). The default
   retry policy was `NoTargets:1,Timeout:0`, which has no
   budget for `Closed`, so the workload-driver recorded zero
   retries and counted every connection-reset as a failure.

This commit fixes both. The 4-host happy path (all hosts up,
no failures) is observably identical to today; the
divergent paths are the new robust behaviour.

## Goal A: per-host start failures are non-fatal

`scripts/chaos-multi-host/coordinator.sh` now treats per-host
bootstrap, src_check, start_host, start_workload, and
start_injector as independent failure domains:

* New helpers `mark_host_failed <h> <reason>` and
  `host_active <h>` (which combines `host_enabled` with "not
  in `FAILED_HOSTS`") gate every per-host step.
* The five existing per-host setup helpers each return
  non-zero on failure now. `bootstrap_remote_src`,
  `start_host`, `start_workload`, `start_injector`, and
  `src_check` all use the explicit `|| rc=$?` pattern at every
  step instead of relying on `set -e`, because bash disables
  `set -e` inside a function called from a `||` context. The
  callers wrap each helper in `... || mark_host_failed <h> ...`
  so a single failure removes that host from the rest of the
  run instead of aborting the whole pass.
* `teardown` and the post-run `rsync` both iterate
  `host_active` rather than `host_enabled`. Hosts that never
  populated `/scratch/dynomite-chaos/run/*.pid` are skipped
  (logged as "skip teardown ... (host marked failed)") so a
  failed host does not produce cascading SSH errors that
  previously masked the original failure in the report.

### Exit-code policy

`WORKLOAD_RUNNING` counts hosts whose `start_workload`
returned 0. `DURATION_REACHED` is set after the duration
sleep completes. The coordinator exits:

* `0` if `WORKLOAD_RUNNING >= 1 && DURATION_REACHED == 1`,
  i.e. at least one host ran for the full workload duration.
  The closing log line names the failed hosts (if any) so the
  operator can spot a degraded pass at a glance.
* `1` if `WORKLOAD_RUNNING == 0` (no host ever launched a
  driver; the run is genuinely broken and the duration sleep
  is pointless -- we exit immediately after teardown rather
  than sleep for 2h).
* `1` if interrupted before the duration completes (the
  EXIT/INT/TERM trap fires teardown and bash exits with
  128 + signal).

## Goal B: default retry policy now covers Closed

```sh
RETRY_POLICY="${RETRY_POLICY-NoTargets:1,Timeout:0,Closed:2}"
```

Two retries on `Closed` (vs one on `NoTargets`) reflects two
realities measured in pass-4: `Closed` is much more common
under chaos (99.9% of all failures), and the per-attempt cost
is just a TCP reconnect against the local engine (sub-
millisecond). The workload-driver's `parse_retry_policy`
already accepted `Closed` as a class, so no Python change was
needed.

## Smoke recipe

The smokes below ran on the lead's machine
(`/home/gburd/ws/dynomite`) against the operator-typical
chaos hosts (floki / arnold / nuc / meh) at 2026-05-26 16:1*Z.

### Test A - happy single-host path

```bash
RUN_ID=smoke-test-a HOSTS_OVERRIDE=floki \
  CHAOS_DURATION_SECS=30 MODE=redis \
  bash scripts/chaos-multi-host/coordinator.sh
```

* coordinator log: `retry: NoTargets:1,Timeout:0,Closed:2`
  (confirms the new default is wired).
* exit 0 with `==> exit 0 (1 host(s) completed the workload
  duration; failed: <none>)`.

### Test B - nuc fails on memcached, floki survives

This reproduces the pass-4 memcache-mode failure exactly:

```bash
RUN_ID=smoke-test-b HOSTS_OVERRIDE=floki,nuc \
  CHAOS_DURATION_SECS=30 MODE=memcache \
  bash scripts/chaos-multi-host/coordinator.sh
```

Expected behaviour and observed result:

```
[hh:mm:ss] starting dc-nuc tokens=2147483648
[hh:mm:ss]   dc-nuc start failed (rc=1); see ...dc-nuc-start.log
[hh:mm:ss]   WARN host nuc marked failed: start_host failed
[hh:mm:ss] starting workload-driver on dc-floki (mode=memcache)
[hh:mm:ss]   hosts failed during start: nuc
[hh:mm:ss] ==> 1 workload-driver(s) up; sleeping for 30 seconds
...
[hh:mm:ss]   skip teardown dc-nuc (host marked failed)
...
[hh:mm:ss] ==> exit 0 (1 host(s) completed the workload duration; failed: nuc)
```

`dc-nuc-start.log` contains the exact pre-existing error
message: `memcached not on PATH and no podman/docker
available`. nuc is excluded from teardown and rsync (no
cascading SSH errors); floki completes the duration; exit 0.

### Test C - mid-stream kill emits retries.Closed

```bash
RUN_ID=smoke-test-c HOSTS_OVERRIDE=floki \
  CHAOS_DURATION_SECS=30 MODE=redis \
  bash scripts/chaos-multi-host/coordinator.sh > /tmp/smoke.log 2>&1 &
COORD=$!
# Wait for the workload to start, then SIGKILL dynomited
# mid-stream. The chaos-injector's recovery_restart logic
# brings dynomited back within ~10s; the workload-driver
# observes ECONNRESET on its open socket and walks the
# Closed:2 retry budget.
while ! grep -q 'workload-driver(s) up' /tmp/smoke.log; do sleep 0.5; done
sleep 5
DYN=$(cat /scratch/dynomite-chaos/run/dynomited.pid)
kill -KILL "$DYN"
wait $COORD
```

Observed `retries.Closed` aggregated across all NDJSON rows:

```
zset/Closed:      558,  multikey/Closed: 540
strings/Closed:  1630,  scripting/Closed: 258
hash/Closed:      786,  keyspace/Closed:  578
set/Closed:       486,  list/Closed:     522
total Closed retries: 5358
```

(With the old default policy these would all have been
recorded as `failures.Closed` and `retries.Closed` would have
been 0, exactly matching the pass-4 report.)

### Test D - HOSTS_OVERRIDE tolerates unknown names

```bash
RUN_ID=smoke-test-d \
  HOSTS_OVERRIDE=floki,arnold,meh,nonexistent-host \
  CHAOS_DURATION_SECS=20 MODE=redis \
  bash scripts/chaos-multi-host/coordinator.sh
```

`nonexistent-host` is not one of the four known names so the
coordinator silently skips it (the `host_enabled` predicate
returns true only for names the script knows how to dispatch
to). floki+arnold+meh all run normally; exit 0 with `==> exit
0 (3 host(s) completed the workload duration; failed:
<none>)`.

## Tests

* `python3 scripts/chaos-multi-host/workload-driver.py
  --self-test`: 35 tests OK (unchanged; Closed-retry coverage
  was already present).
* `python3 scripts/chaos-multi-host/test_workload_driver.py`:
  11 tests OK (new file). Pins the post-pass-4 default policy
  parses to `{NoTargets: 1, Timeout: 0, Closed: 2}` and
  exercises the full Closed-retry control flow:
  - default policy parses to three classes,
  - one Closed then success -> retry=1, op=SET,
  - two Closed retries consumed -> third call wins, retry=2,
  - three Closed -> budget exhausted, fail with err=Closed,
  - OSError travels the same Closed path,
  - mixed NoTargets + Closed exhausts independent budgets,
  - Timeout still has zero budget in the default policy.
  Plus four `classify_error` tests confirming
  `ConnectionError` and `OSError` map to `Closed` for all
  three modes (redis, memcache, riak).
* `bash -n scripts/chaos-multi-host/coordinator.sh`: clean.
* `shellcheck scripts/chaos-multi-host/coordinator.sh`: clean.
* `bash scripts/check_no_todos.sh`: clean.
* `bash scripts/check_no_port_comments.sh`: clean.
* `bash scripts/check_ascii.sh`: clean.

## Files touched

```
scripts/chaos-multi-host/coordinator.sh        (+~150, -~30)
scripts/chaos-multi-host/test_workload_driver.py  (new, +263)
docs/journal/2026-05-26-chaos-coordinator-robust.md  (new)
```

`workload-driver.py`, `start-host.sh`, `chaos-injector.sh`,
`smoke-coordinator.sh`, and `generate-report.py` are
unchanged.

## Open questions / next steps

* Pass-5 design: re-run the 3-mode multi-host pass with the
  robust coordinator and compare per-mode success rates. The
  expectation is that redis improves from 91.08% to >99% (the
  Closed retries that previously counted as failures get
  absorbed) and that memcache/riak modes complete the full
  duration on the 3 hosts that have the prerequisites, with
  nuc cleanly excluded.
* The chaos injector's kill cycle is 8-12 minutes
  (`scripts/chaos-multi-host/chaos-injector.sh:193`); a 30s
  smoke does not see an injector-driven kill, which is why
  Test C uses a manual SIGKILL. A future operator-only smoke
  could shorten the injector's kill interval via env override
  to exercise the kill path end-to-end without the manual
  step; that is out of scope here.
* Adding a per-class "max retry budget reached" gauge to the
  workload-driver NDJSON would let the report flag DCs whose
  Closed budget is consistently exhausted (the
  cluster-genuinely-down signal). This is a follow-up,
  separate from the coordinator robustness work.
