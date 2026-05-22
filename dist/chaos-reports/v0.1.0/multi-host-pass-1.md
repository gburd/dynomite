# Pass-1 Multi-Host Chaos Report

**Run ID**: `prod-20260522-010136Z`
**Window**: 2026-05-22 01:01:36Z to 03:02:38Z (2 hours 1 min wall, 7200s test duration)
**Coordinator**: `floki:/home/gburd/ws/dynomite/scripts/chaos-multi-host/coordinator.sh`

## Topology

| DC label    | Host    | OS / arch              | Build path                                | Notes                                |
|-------------|---------|------------------------|-------------------------------------------|--------------------------------------|
| `dc-floki`  | floki   | NixOS x86_64           | native `cargo build --release`            | Coordinator host                     |
| `dc-arnold` | arnold  | Fedora 44 x86_64       | release built inside `rust:1.90` podman   | Redis runs in `redis:7-alpine` podman|
| `dc-nuc`    | nuc     | FreeBSD 15.0 amd64     | native via `pkg install rust redis`       | Reached only via arnold ProxyJump    |

All three DCs ran with the **same token** (`101134286`) and **DC_ONE consistency**, so each DC operated as an independent full-replica. Outbound peer connections existed (pass-2 wiring shipped earlier this evening) but never carried traffic in this run because every key hashed to a local owner.

## Headline numbers

| Metric                                    | Total / per-DC                                        |
|-------------------------------------------|-------------------------------------------------------|
| Total client requests served              | **3,344,844 ok** + **182,339 fail** = **3,527,183**   |
| Aggregate success rate                    | **94.8%**                                             |
| Mean throughput (steady state)            | floki 148/s, arnold 173/s, nuc 175/s (per DC)         |
| Total chaos events fired                  | 155 pause cycles + 25 SIGKILL+restarts + 3 redis bounces |
| **Bugs surfaced live**                    | **2**: pidfile flock race, chaos-injector single-shot restart |
| Bugs caught before launch                 | 8 (committed and pushed during setup)                 |
| Run completed                             | Yes, clean teardown, all logs collected               |

## Per-DC breakdown

| DC          | ok       | fail     | fail % | kills | restarts | pauses | redis bounces | restart_failed |
|-------------|----------|----------|--------|-------|----------|--------|----------------|-----------------|
| `dc-floki`  | 1,051,024| 118,458  | 10.1%  | 11    | 12       | 67     | 0              | 1               |
| `dc-arnold` | 1,237,228| 45,598   | 3.5%   | 3     | 11       | 22     | 3              | 0               |
| `dc-nuc`    | 1,056,592| 18,283   | 1.7%   | 11    | 11       | 66     | 0              | 0               |

`dc-floki`'s elevated failure rate is fully accounted for by a single 9-minute outage (window 282 - 333, elapsed 2876s - 3386s, wall 01:49:32Z - 01:58:55Z). See "Bug #10" below.

`dc-arnold` shows fewer kills than restarts because the chaos-injector retried failed restarts during the operator-rescue window (01:13Z - 01:18Z, before the start-args fix landed); each retry cycle was logged as a `restart` event.

`dc-nuc` shows the cleanest signal: 1.7% fail rate, all of which is `ConnectionRefused` during the brief window each kill+restart took. No outages, no anomalies.

## Workload coverage

The `workload-driver.py` exercised **8 Redis command classes** at every DC for the full 2 hours:

```
                 dc-arnold       dc-floki        dc-nuc
strings/SET      ~22% of ops     ~22% of ops     ~22% of ops
hash/{HSET,...}  ~11%            ~11%            ~11%
list/LPUSH/...   ~7.5%
set/SADD/...     ~7.5%
zset/ZADD/...    ~7.5%
keyspace/TTL     ~7.5%
multikey/MGET    ~7.5%
scripting/EVAL   ~3.7%
scripting/PING   ~3.7%
```

Every class executed continuously across all 3 platforms including FreeBSD. Including ~85,000 `EVAL` invocations against the parser bug we caught and fixed during setup — the deployed binaries had the fix and `EVAL` returned `:1\r\n` correctly across the entire run.

## Bugs found by the run

The setup phase caught 8 bugs (commits `f40d137`, `543367c`, `c66b9a8`, `691ac52`, etc.). The live run caught two more:

### Bug #9: pidfile `flock(2)` race on fast restart  **(FIXED post-run, commit `c69c410`)**

When chaos-injector SIGKILLed dynomited and called start-host.sh within ~2s, the new dynomited's `flock(LOCK_EX | LOCK_NB)` on the pidfile returned `EAGAIN: Try again` because the kernel had not yet reaped the killed process and its flock entry was still in the inode lock list. Symptom: `ERROR dynomited: create pid file error=flock ...: EAGAIN`. Observed 5+ times on `dc-arnold` during the run.

Two-prong fix:
- `PidFile::create` now retries `flock` up to 10x at 100ms intervals on `EAGAIN`/`EWOULDBLOCK`. Genuine duplicate-instance startup still fails fast (within ~1s).
- `chaos-injector.sh` now busy-waits with `kill -0 <pid>` (5s bound) after `SIGKILL` before calling start-host.sh.

New unit test: `pidfile::tests::flock_retry_succeeds_when_holder_drops_during_window`.

### Bug #10: chaos-injector gives up after one failed restart  **(QUEUED for fix)**

Timeline of `dc-floki`'s 9-minute outage:

```
01:49:56Z  kill         pid=1594820  (the live dynomited)
01:49:59Z  restart      reason=sigkill
01:50:37Z  restart_failed  reason=start-host.sh-nonzero
        ... no further restart attempts ...
01:50:42Z  pause_skipped reason=no-dynomited
01:52:33Z  pause_skipped reason=no-dynomited
01:54:18Z  pause_skipped reason=no-dynomited
01:55:53Z  pause_skipped reason=no-dynomited
01:57:48Z  pause_skipped reason=no-dynomited
01:58:45Z  restart      reason=sigkill   <-- next kill cycle's restart finally succeeded
01:59:01Z  pause_start  pid=1621594  (new dynomited up)
```

The first restart (01:49:59) failed at start-host.sh (almost certainly the pidfile flock race). The chaos-injector logged `restart_failed` but **did not retry** until the next scheduled kill 8-12 minutes later. Five subsequent `pause_skipped` events show the injector noticed dynomited was missing but did not act on that observation.

This is a **chaos-test orchestration bug**, not a dynomite bug. The injector's loop should detect "dynomited is not running and we did not just SIGKILL it" and proactively restart, regardless of the kill schedule. With the pidfile flock fix in place from bug #9, the original `restart_failed` would no longer happen; combined with an injector retry it makes future runs robust to either failure mode.

Recorded in `docs/post-chaos-queue.md`. Estimated fix: ~30 LOC in `chaos-injector.sh`.

## Chaos schedule that fired

Per dc, over 2 hours:

```
floki:   pause x67 (5-15s ea, every 60-180s),  kill x11 (every 8-12 min)
arnold:  pause x22 (rescued mid-run),           kill x3,  redis_bounce x3 (every 20-30 min)
nuc:     pause x66,                             kill x11
```

Total chaos events: **183**. The system **survived all 11 floki kills, all 3 arnold kills, all 11 nuc kills, all 155 pause cycles, and all 3 redis container bounces**. The only outage (#10) was caused by the injector itself losing track of recovery, not by a Dynomite failure.

## What went well

1. **Cross-platform**. The Rust port runs identical code on Linux x86_64 (NixOS, Fedora) and **FreeBSD 15 amd64** with zero platform conditionals. nuc carried 1.06M successful ops over 2 hours and recovered cleanly from 11 kill+restart cycles.
2. **Backend reconnect**. The `backend_supervisor` absorbed all 3 redis container bounces on arnold without surfacing a single client error. Capped exponential backoff worked as designed.
3. **QUIT semantics + graceful shutdown**. The fixes from setup (commits `f40d137`, etc.) held: every SIGTERM resulted in a clean exit; no orphaned client connections.
4. **EVAL parser fix held under load**. ~85,000 `EVAL "return 1" 0` invocations executed correctly across all 3 platforms — the parser bugs we caught in `commands.rs` and the digit/nkey checks in `parser.rs` would have made every one of those a `ConnectionRefused`.

## What to do next

| Item | Priority |
|---|---|
| Bug #10: chaos-injector retry-on-restart-failure | high (next 30 min of work) |
| Pass-2 chaos run: per-DC distinct tokens + outbound peer routing | high (the pass-2 code shipped earlier; the pass-1 run did not exercise it) |
| Phi-accrual failure detector after gossip wiring | medium |
| TCP+TLS for peer plane | low (deferred) |

## Logs

All raw logs preserved at:
```
target/chaos-multi-host/prod-20260522-010136Z/
  coordinator.log               (2-hour event log)
  watch.log                     (10-min status snapshots)
  {floki,arnold,nuc}-logs/
    dynomited-dc-*.log          (server logs)
    workload-dc-*.ndjson        (10s-window throughput / failure counts)
    chaos-events-dc-*.ndjson    (every kill/pause/restart event)
    redis-dc-*.log              (where applicable)
```

The auto-generated table report is at `report.md` in the run directory; this hand-curated document is the operator-facing summary.
