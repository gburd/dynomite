# 2026-05-24 chaos-multi-mode + four-host rig

**Branch**: `stage/chaos-multi-mode`
**Scope**: extend `scripts/chaos-multi-host/` to support all
three datastore modes (redis, memcache, riak placeholder) and
add a fourth host (`meh`) to the cluster.

## Summary of changes

* `scripts/chaos-multi-host/start-host.sh`
  - New `MODE` env var (default `redis`).
  - `MODE=memcache`: starts `memcached` instead of
    `redis-server`, sets `data_store: 1` in the dynomited
    config, probes the backend with the memcache `version`
    handshake. Falls back to a `memcached:1.6-alpine` container
    when no native binary is on `PATH`, mirroring the existing
    redis container fallback.
  - `MODE=riak`: emits a `==> WARNING: Riak mode requires the
    dyniak crate, not yet available; falling back to redis`
    warning to stderr and runs as `redis`. The driver does the
    same on its side, so the warning is visible in both
    `start-host.log` and `workload-*.stderr`.
  - New `ROOT` env override (default `/scratch/dynomite-chaos`)
    so smoke harnesses can rerun the script under any prefix.

* `scripts/chaos-multi-host/chaos-injector.sh`
  - Reads `MODE` from `start-args` (defaults to `redis` for
    back-compat with pre-multi-mode files).
  - Threads `MODE=$MODE` through to every restart of
    `start-host.sh` so dynomited comes back up with the same
    parser family on every kill cycle.
  - Native-path backend bounce branches on `MODE`: spawns
    `memcached` for memcache, `redis-server` otherwise.

* `scripts/chaos-multi-host/coordinator.sh`
  - Adds `meh` (192.168.1.185, NixOS x86_64) as a fourth DC.
    Pre-flight checks `/scratch/dynomite-chaos/src` exists,
    starts dynomited, workload, and injector on it, and rsyncs
    its logs at teardown.
  - Re-balances the token ring for four hosts: each DC owns
    ~25% of the u32 space. Dispatcher's `Replicas` route now
    fires for ~3/4 of every host's traffic (was ~2/3 in the
    pass-2 three-host rig).
  - Threads `MODE` from the env into start-args, the start-host
    invocation, and the workload driver.
  - Drops the "pass 1" header now that the rig serves as the
    permanent multi-host harness.

* `scripts/chaos-multi-host/workload-driver.py`
  - New `--mode {redis,memcache,riak}` flag (default `redis`).
  - New `MemcacheConn` and `MemcacheError` classes implementing
    the memcache ASCII protocol with the same loopback
    self-connection avoidance trick as `RespConn`. No
    third-party dependencies (avoiding `pymemcache` keeps the
    driver portable to FreeBSD's stock `python3`).
  - New `MEMCACHE_WORKLOADS` table covering the four ASCII
    classes (`set`, `get`, `arith`, `delete`) at the same QPS
    as the redis driver.
  - Logs `mode` in every NDJSON row so post-run reports can
    distinguish a memcache pass from a redis pass.
  - `riak` mode prints a one-shot warning at startup and runs
    as `redis`.

* `scripts/chaos-multi-host/live-status.sh`
  - Adds `MEH_SSH` and a `snapshot_host dc-meh ...` call so the
    live status page renders four hosts. `watch-status.sh`
    delegates to `live-status.sh` and so picked up the change
    automatically.
  - `generate-report.py` already discovers hosts dynamically
    from the `*-logs/` subdirectories under the run dir, so it
    needed no edits to handle the fourth host.

* `docs/operations/chaos.md` (new)
  - Operator manual for the multi-host rig: hosts, token slots,
    modes, workload classes, run/smoke procedure.

* `dist/chaos-reports/README.md`
  - Cross-link to the new operator manual and to the existing
    pass-1 / pass-2 reports.

## SSH preflight to `meh`

```
$ ssh meh "uname -srm"
Linux 6.12.83 x86_64
$ ssh meh hostname
meh
```

`meh` is reachable directly on the LAN at `192.168.1.185`; no
ProxyJump is needed. The host's interactive shell is fish but
SSH-driven `bash -c` invocations work as expected, so the
coordinator's existing `bash -lc` style runner pattern is
identical to floki's. Tailscale is not installed on `meh`; we
use its LAN IP everywhere.

## Smoke results

A 60-second local smoke pass was run for each MODE on the lead
host (no SSH) using a private harness that exercises the real
`start-host.sh` + `workload-driver.py` against a loopback
backend. Per-mode summary (each line is the final aggregated
NDJSON row):

```
redis:    elapsed=65.0s ok=12204 fail=0 ops/s=187.8
          classes: strings/3553 hash/1863 zset/1249 multikey/1194
                   list/1254 set/1230 keyspace/1242 scripting/619
memcache: elapsed=65.0s ok=11174 fail=0 ops/s=171.9
          classes: set/3997 get/3839 arith/2228 delete/1110
riak:     elapsed=65.0s ok=12391 fail=0 ops/s=190.6  (mode=redis)
          warning observed in start-host.log AND workload.stderr:
            "WARNING: Riak mode requires the dyniak crate,
             not yet available; falling back to redis"
          classes: same as redis
```

All three smokes met the contract (>30s of traffic, >0
successful operations) and produced zero client-visible
failures. The riak smoke confirms the fall-back path: the
warning appears in both the host launcher and the driver, and
the run otherwise behaves as a redis pass.

## Tests

`cargo nextest run --workspace` -> `728 tests run: 728 passed,
4 skipped`. No source code under `crates/*/src/` or
`crates/*/tests/` was touched; the count is unchanged from the
baseline.

## Outstanding

* The actual 4-host multi-host pass is the operator's job, not
  this work item's. The infrastructure is ready; the report
  for the first 4-host run will land at
  `dist/chaos-reports/v0.1.0/multi-host-pass-3.md` after the
  pass.
* When the `dyniak` crate lands, swap the placeholder warning
  for real Riak protocol launching in both `start-host.sh` and
  `workload-driver.py`. The plumbing (MODE arg, --mode flag) is
  already in place.
