# Pass-2 Multi-Host Chaos Report

**Run ID**: `pass2-20260522-032705Z`
**Window**: 2026-05-22 03:27:05Z to ~05:00Z (~1h 33m on arnold, 1h 39m on nuc, 52m on floki)
**Coordinator**: died at ~03:39Z when an external process supervisor killed the parent shell during a long-poll wait. Per-host components (dynomited / workload-driver / chaos-injector) had been spawned via `nohup` and outlived the coordinator, but the orchestrator itself was gone and the scheduled 2-hour teardown never fired. Run terminated manually at 05:07Z.

## Topology change vs pass-1

The whole point of pass-2 was to switch to **per-DC distinct tokens** so cross-DC routing actually fires:

| DC          | Token         | Hash-ring slice    |
|-------------|---------------|--------------------|
| `dc-floki`  | 0             | `[0, 1431655764]`  |
| `dc-arnold` | 1431655765    | `[1431655765, 2863311529]`  |
| `dc-nuc`    | 2863311530    | `[2863311530, 4294967295]`  |

This forces ~2/3 of every host's traffic to hash into ANOTHER DC's range. With `read_consistency: DC_ONE`, the dispatcher's `Replicas` arm is supposed to route those out via the per-peer outbound channels wired in commit `f9e9a6b` (pass-2 design).

Pass-2 binaries on all hosts include:
* outbound peer connection wiring (`peer_supervisor` task per non-local peer, `DnodeServerConn::run_with`)
* phi-accrual detector attached to each `Peer` (commits `65ccb12` + `979510f`)
* pidfile flock retry on EAGAIN (commit `c69c410`)
* chaos-injector recovery-on-missing-dynomited loop (commit `7e0d2a9`)
* coordinator stdin-piped start-args (commit `691ac52`)

## Headline numbers

| | dc-floki | dc-arnold | dc-nuc | total |
|---|---|---|---|---|
| Wall time | 52 min | 1h 33m | 1h 39m | |
| Workload windows | 311 | 559 | 595 | |
| Successful requests | 529,220 | 865,018 | 1,058,806 | **2,453,044** |
| Failed requests | 7,314 | 264 | 12,997 | 20,575 |
| Fail % | 1.4% | **0.03%** | 1.2% | 0.83% |
| Mean throughput (ops/sec) | ~170 | ~155 | ~178 | |

## Chaos events (filtered to pass-2 window only)

| Event | dc-floki | dc-arnold | dc-nuc |
|---|---|---|---|
| pause cycles | 29 | 0 | 61 |
| SIGKILL+restart | 5 | 0 | 9 |
| redis bounces | 0 | 4 | 0 |
| recovery_restart (bug #11) | 0 | **510** | 0 |
| pause_skipped (chaos missed because dynomited was down) | 0 | 63 | 0 |

## Bug #11: recovery-restart loop thrashing (FIXED for next run)

The chaos-injector's recovery-restart loop introduced in pass-1 commit `7e0d2a9` had a debounce bug: it fired `restart_dynomited` every 5s if `dyn_pid()` returned no live pid. start-host.sh takes 5-15s to bring dynomited up cleanly (TCP listener bind, redis backend connect, AUTH handshake, wait for stats endpoint), so the loop would commonly fire a second restart while the first was still in progress. The two restart attempts then competed for the pidfile flock; only one could win, the other failed, and the loop fired again 5s later.

**On arnold this loop ran for the entire 93-minute run**: 510 recovery_restart events plus 520 total restart events plus 63 pause_skipped events plus 0 actual kill/pause cycles. Arnold's injector never escaped the recovery loop long enough for the scheduled chaos timer to fire.

**Net effect**: arnold's nominal "0.03% fail rate" is misleading — chaos was effectively *not running* on that DC. The dynomited supervisor's reconnect loop kept absorbing the thrashing, so client traffic was largely unaffected, but the test wasn't exercising what it was supposed to.

**Fix**: commit `a6d085e` (already pushed) requires two consecutive 5s checks of "dynomited missing" before firing a recovery restart. The next chaos run will use the fixed injector and arnold should exhibit the same kill/pause/restart pattern as nuc.

## What pass-2 still validated

Even with the arnold thrashing, pass-2 produced real signal:

1. **Cross-platform stability under chaos**: nuc (FreeBSD) survived 9 SIGKILL+restart cycles cleanly (no flock errors, no failed restarts). floki survived 5 cycles before being killed by the coordinator-death.

2. **Pidfile flock retry held**: zero `flock ... EAGAIN` errors on either floki or nuc. The retry budget (10 × 100ms) is sufficient for normal operating conditions; arnold's thrashing exposed an orthogonal issue (the recovery loop) but the flock retry itself worked as designed.

3. **Per-DC token topology accepted**: all three DCs successfully started with distinct tokens and ran throughout. The peer_supervisor tasks for outbound peer connections were spawned (visible in startup logs) and reconnect cleanly when the remote peer cycles. We DID NOT directly validate that cross-DC routing carried request traffic — the dispatcher's `Replicas` plan only fires when consistency requires fan-out, and `DC_ONE` (configured) routes locally even with mismatched tokens because `plan()` returns `LocalDatastore` when no DC-local replica owns the token, which on a 1-node-per-DC topology means it falls through to local. That's a topology limitation, not a code bug; pass-3 with `DC_QUORUM` (or 3+ nodes per DC) would actually exercise the peer routing.

4. **Workload self-connection hardening held**: zero "Address already in use (os error 48)" errors on FreeBSD nuc, where pass-1 had two of them. The new connect logic in workload-driver.py rejects loopback ephemeral-port collisions and retries cleanly.

5. **No EVAL parser regressions**: arnold + nuc together executed ~85k EVAL invocations with all RESP frames decoded correctly (would have shown up as `scripting/ConnectionError` if broken).

## Bugs found across pass-1 and pass-2

| # | Bug | Status | Commit |
|---|---|---|---|
| 1 | Local datastore unwired in binary | fixed | `f40d137` |
| 2 | QUIT didn't close client connection | fixed | `f40d137` |
| 3 | proxy::run hung on shutdown | fixed | `f40d137` |
| 4 | preconnect default = true (vs C false) | fixed | `f40d137` |
| 5 | EVAL parser had three sub-bugs | fixed | `543367c` |
| 6 | integration test read_until couldn't terminate | fixed | `543367c` |
| 7 | coordinator start-args heredoc double-eval | fixed | `691ac52` |
| 8 | FreeBSD loopback ephemeral self-connect | fixed | `65ccb12` |
| 9 | pidfile flock race on fast restart | fixed | `c69c410` |
| 10 | chaos-injector single-shot restart | fixed | `7e0d2a9` |
| 11 | **recovery-restart loop thrashing** | **fixed (this commit chain)** | `a6d085e` |
| 12 | stale dynomited not killed on restart_dynomited | known, queued | (none yet) |

## What pass-3 should do

1. Re-run the 2-hour pass-2 topology with the bug #11 debounce in place. Expect arnold to show normal kill/pause/restart cadence.
2. Add a watcher process that's robust to coordinator death — the `setsid` + double-fork pattern, or systemd-style.
3. Cap the run duration of the parent shell rather than the coordinator process so the coordinator can run autonomously after launch.
4. Once cross-DC routing actually fires (3+ nodes per DC, or `DC_QUORUM` consistency), instrument it to count cross-DC bytes vs local bytes and confirm the per-peer outbound channels are doing real work.

## Run artifacts

```
target/chaos-multi-host/pass2-20260522-032705Z/
  coordinator.log            (run start; ends at "sleeping for 7200 seconds")
  watch.log                  (one snapshot before coordinator died)
  {floki,arnold,nuc}-logs/
    dynomited-dc-*.log
    workload-dc-*.ndjson    (300-595 windows depending on host lifetime)
    chaos-events-dc-*.ndjson
    redis-dc-*.log          (where applicable)
  report.md                  (auto-generated table)
```
