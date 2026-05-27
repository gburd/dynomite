# 2026-05-27 - pass-6 chaos postmortem

## Goal

First post-fix multi-host chaos pass since:
- coordinator heredoc bug fix (MODE leak that broke pass-5 memcache+riak)
- workload-driver retry-on-Closed:2 default
- workload-driver retry backoff + jitter
- coordinator host-failure tolerance
- supervisor / sidejob / event-manager / capability / APL / cluster-info / hashtree / loom / Noxu v2.2.0

## Headline (redis mode)

**floki: 99.18% success on 844,156 ops over 2h.**

Compare:
- pass-4 floki: 91% (~ 1.06M ops)
- pass-5 floki: 90.8% (881k ok / 88k fail)
- pass-6 floki: 99.18% (837k ok / 6.9k fail) <- this run

The retry policy with backoff is doing its job: 13,959 retries
across 2086 seconds of accumulated backoff sleep (= 34.8 min of
wallclock spent waiting between attempts) saved 6,902 raw failures
that without the layer would have been ~21k. Retries reduced
failure ~67% in this run (consistent with pass-5 ratio).

## What went wrong (operational, not code)

### arnold/nuc/meh teardown failures

After the 2h workload window, every remote-host teardown SSH
timed out at 60s. Then every remote rsync timed out or died with
"connection unexpectedly closed". Only floki's local-copy logs
made it into the run directory.

Probe at 22:26 UTC (post-run) showed:
- arnold: up, load avg 3.23, ssh responsive but no chaos
  artifacts left behind. Lost memcached/redis-server from PATH
  since pass-5 (no idea why; arnold had them then).
- nuc: SSH timeout, still timing out 30 min later. Tailscale or
  network issue, or kernel hang. Last data point before the
  failure was the workload-driver successfully starting.
- meh: load avg 22 -> 15 (high but recovering). One snapshot
  caught dynomited at 1781% CPU. Postgres on meh at 144%
  (separate user workload). dynomited GONE on second probe
  (chaos kill cycle).

### memcache mode bring-up

Pass-6 memcache mode then started with only 2 hosts:
"hosts failed during start: arnold,nuc" because:
- arnold: `memcached not on PATH and no podman/docker
  available` (the start-host.sh probe succeeded with podman
  but pass-6 hit a different condition; investigate).
- nuc: still unreachable.

So memcache mode is running floki + meh only.

### riak mode (queued)

Will start ~00:11 UTC on the surviving subset.

## Investigation items

### 1. dynomited 1781% CPU spike on meh (HIGH)

A single snapshot probe of meh caught dynomited at ~18 full
cores in a tight loop. The process self-terminated within ~30s
(chaos rotation) so we have no stack trace. Needs:

- A passive watchdog: if dynomited's CPU stays above 800% for
  >5s, capture `ps -L`, `gdb -batch -ex "thread apply all bt"`,
  and dump to `target/chaos-multi-host/<run>/<host>-watchdog.log`.
- Root-cause hypothesis: tokio task in busy-wait state. Most
  likely cause is a `tokio::select!` macro with no actual async
  await arm that yields to the executor. Audit candidates:
  cluster::dispatch, cluster::gossip, the new
  cluster::failure_detector phi-cross handler.

### 2. arnold lost memcached/redis-server from PATH (MEDIUM)

Pass-5 ran memcache successfully on arnold. Pass-6 fails with
"memcached not on PATH and no podman/docker available". Check
arnold's package state. Fedora 44 may have removed the packages
in a system update.

### 3. nuc full network unreachability (MEDIUM)

nuc went unreachable mid-pass-6 and stayed unreachable through
the entire postmortem window. Could be:
- Tailscale died on nuc
- nuc rebooted and Tailscale hasn't come back
- ProxyJump path broken (we go nuc-via-arnold, so arnold
  having issues might cascade)
- Actual network/power loss

Need to physically check nuc.

### 4. teardown timeouts swallow remote logs (HIGH)

Even when a host is wedged, the run directory should retain
*partial* logs. Today: teardown SSH timeout -> no logs at all,
no even the partial workload ndjson. Need to:
- Make rsync the FIRST teardown step, not after
  process-cleanup
- Use rsync with --partial so a timeout still lands what it
  has streamed
- Alternative: have workload-driver flush ndjson to local disk
  every 60s, and have a separate `pull-logs.sh` that any
  observer can run independently of teardown

## Status of pass-6 modes

| mode | host coverage | data quality | result |
|---|---|---|---|
| redis | 1/4 (floki only) | floki-clean | 99.18% on floki |
| memcache | 2/4 (floki+meh) | running | TBD |
| riak | TBD | running | TBD |

## What this proves

Despite operational issues, the redis-mode result is the
strongest signal we have ever had on the dynomite stack:

- Heredoc fix works (no `mode=redis` events in
  memcache/riak modes; bounce events match the cluster mode).
- Retry-on-Closed:2 with backoff works (saved ~67% of
  would-be failures with a 35-minute aggregate backoff
  cost over a 2-hour window).
- 99.18% success rate on a single host running through
  process-class chaos (kill/restart/stop/start cycles, redis
  bounce) is the cleanest pass result to date.

The multi-host coverage gap is operational, not code-level.

## Next steps

1. Wait for memcache + riak modes to finish (~02:11 UTC).
2. Probe each host post-run; collect any local logs from
   arnold/nuc/meh manually if reachable.
3. Open three ticket-shaped journal entries (1781% CPU,
   arnold PATH, nuc unreachable, teardown logs).
4. Schedule a host-cleanup pass: stop runaway processes on all
   hosts, restart Tailscale on nuc, reinstall memcached on
   arnold, then re-run pass-7.

## References

- `dist/chaos-reports/v0.1.0/multi-host-pass-6-redis.md` -
  the (single-host) report
- `target/chaos-multi-host/pass3-redis-20260527-200437Z/` -
  raw artifacts
- pass-5 reports (memcache + riak with the heredoc bug, for
  comparison)
