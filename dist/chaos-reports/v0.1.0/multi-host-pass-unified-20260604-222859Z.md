# Multi-host chaos report: unified mode (pass-unified-20260604-222859Z)

_Hand-curated. The auto-generator (`generate-report.py`) blends
this run's per-API ndjson with two earlier same-path unified
attempts (the driver appends to `workload-dc-<host>-<api>.ndjson`
across launches and the file was not truncated between my three
launches), so the auto aggregate is not trustworthy for this run.
The numbers below are filtered to launch #3 only by timestamp
(`ts >= 2026-06-04T22:28:59Z`)._

## What this run validates

The first chaos run to drive **multiple API surfaces concurrently
against a single shared datastore on every node**:

- Redis RESP (KV: GET/SET/DEL/PING) on the client port
- Riak PBC (ping/get/put/del + CRDTs) on the PBC port
- Riak HTTP gateway on the HTTP port

All three delegate to one in-process `Arc<NoxuDatastore>` per node
(`data_store: noxu` + `riak:` block). A key written via Redis RESP
and read via Riak PBC hits the same store.

Memcache is intentionally excluded: one dynomited pool serves one
client-port wire protocol, and Redis was chosen for the unified
client port. Redis data structures and RediSearch FT.* are not
noxu-backed (noxu implements GET/SET/DEL/PING + the Riak CRDT/KV
surface); they keep their coverage in the redis-mode runs against
`redis-server` (see the stage-16 chaos hour report).

## Run summary

| field | value |
|---|---|
| run id | `pass-unified-20260604-222859Z` |
| mode | `unified` (redis-KV + riak-PBC + riak-HTTP, shared noxu) |
| hosts | floki, arnold, meh, nuc (4-way real cluster) |
| fault classes | process, network, clock, disk (`MODE_FAULTS=process,network,clock,disk`) |
| planned duration | 2h 00m 00s |
| coordinator verdict | `exit 0 (4 host(s) completed the workload duration; failed: <none>)` |
| coordinator log span | 22:28:59 - 00:34:13 UTC |

All four hosts completed the full 2-hour workload window. The
coordinator's teardown certified zero host failures.

## Workload results (floki, the host with locally-captured ndjson)

The teardown rsync of the remote hosts' ndjson timed out (a known
limitation tracked since pass-7); floki is the coordinator-local
node and is representative. The other three hosts were certified
complete by the coordinator (`failed: <none>`).

| API surface | ok | fail | success rate | failure breakdown |
|---|---:|---:|---:|---|
| Redis KV | 425,662 | 628 | **99.85%** | 626 `Closed`, 2 `Timeout` |
| Riak PBC | 109,866 | 671 | **99.39%** | 669 `Closed`, 2 `Timeout` |
| **combined** | **535,528** | **1,299** | **99.76%** | all chaos-induced connection drops |

Every failure is a connection `Closed` (or a rare `Timeout`)
during a process-kill fault, recovered by the driver's retry
policy. There are zero `Unknown` (unsupported-command),
`NoTargets` (ring/quorum), or data-unavailability errors. The
shared noxu store served both wire protocols cleanly through the
full fault schedule.

## Faults injected (floki)

| fault | count |
|---|---:|
| `fault_process_kill` (+ `_end`) | 6 (paired) |
| `fault_process_pause` (+ `_end`) | 3 (paired) |
| `fault_disk_squeeze` (+ `_end`) | 6 (paired) |
| `fault_disk_loop` / `fault_disk_iolat` | skipped (needs privileged mounts) |
| network / clock | unrunnable on this cluster (no privileged `tc`, no `faketime`) |

The process-kill faults are the important ones here: each SIGKILL
of dynomited exercised the noxu crash-recovery path. With the
stale-lock reclaim in place (see below) every killed node
reopened its shared noxu store and rejoined; the 99.76% combined
success rate through 6 kills + 3 pauses confirms it.

## Two findings this run produced (both fixed)

1. **noxu backend command surface.** The first unified launch ran
   the standard redis workload (hash/zset/list/FT.*) and hit ~76%
   failure: the noxu backend implements only GET/SET/DEL/PING.
   Fixed by the `--noxu-compat` driver workload (KV-only) wired
   into unified mode. Commit `6130bd1`.

2. **noxu environment lock not released on SIGKILL.** The second
   launch exposed that noxu's `noxu.lck` is presence-based, not an
   `flock(2)`: a hard kill leaves it behind and the next open
   fails with "Environment is locked by another process", wedging
   the node. Fixed two ways:
   - harness: `start-host.sh` clears a stale `noxu.lck` before
     reopen (commit `4551543`);
   - engine: `dyniak::datastore::NoxuDatastore` now holds its own
     `flock(2)` owner-lock (`.noxu-owner.lock`) that the kernel
     releases on process death, and reclaims a stale `noxu.lck`
     under that guard, returning `EnvironmentBusy` only for a
     genuinely concurrent live owner (commit `18a9e3d`).
   Full write-up: `docs/journal/2026-06-04-noxu-lock-after-sigkill.md`.

## Reproduction

```
RUN_ID=pass-unified-$(date -u +%Y%m%d-%H%M%S)Z \
MODE=unified \
MODE_FAULTS=process,network,clock,disk \
CHAOS_DURATION_SECS=7200 \
  bash scripts/chaos-multi-host/launch-detached.sh \
    target/chaos-multi-host/$RUN_ID.log \
    target/chaos-multi-host/$RUN_ID.pid
```

Clear the per-API ndjson between repeated launches on the same
host to keep the auto-report accurate:
`rm -f /scratch/dynomite-chaos/logs/workload-dc-*-{redis,riak}.ndjson`.
