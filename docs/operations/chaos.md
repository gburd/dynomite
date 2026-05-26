# Multi-host chaos infrastructure

This page documents the multi-host chaos rig under
`scripts/chaos-multi-host/`. It is independent of the in-repo
Stage 16 `crates/dynomite/tests/stage_16_chaos.rs` test (which
runs everything in-process inside a single host) and is what the
operator uses to soak a real 4-DC dynomite cluster across actual
machines for hours at a time.

The companion mdBook chapter
`docs/book/src/operations/chaos.md` covers the in-process
Stage 16 test; the curated reports under
`dist/chaos-reports/v0.1.0/` are produced by the multi-host
rig described here.

## Hosts

The rig drives a four-host cluster, one DC per host:

| DC label    | Host    | OS / arch              | Reachable via             |
|-------------|---------|------------------------|---------------------------|
| `dc-floki`  | floki   | NixOS x86_64           | local (coordinator host)  |
| `dc-arnold` | arnold  | Fedora 44 x86_64       | Tailscale                 |
| `dc-nuc`    | nuc     | FreeBSD 15 amd64       | LAN via arnold ProxyJump  |
| `dc-meh`    | meh     | NixOS x86_64           | LAN (192.168.1.185)       |

Each DC owns ~1/4 of the 32-bit hash ring:

| DC          | Token         | Hash-ring slice                |
|-------------|---------------|--------------------------------|
| `dc-floki`  | 0             | `[0, 1073741823]`              |
| `dc-arnold` | 1073741824    | `[1073741824, 2147483647]`     |
| `dc-nuc`    | 2147483648    | `[2147483648, 3221225471]`     |
| `dc-meh`    | 3221225472    | `[3221225472, 4294967295]`     |

This forces ~3/4 of every host's traffic to hash into a remote DC
and travel out via the dispatcher's `Replicas` route, exercising
the outbound peer connections and the phi-accrual failure
detector.

## Datastore modes

The coordinator and workload driver both honour a `MODE`
environment variable selecting which datastore family the
cluster should serve:

| `MODE`     | Backend       | dynomited `data_store` | Workload protocol |
|------------|---------------|------------------------|-------------------|
| `redis`    | redis-server  | 0                      | RESP-2            |
| `memcache` | memcached     | 1                      | memcache ASCII    |
| `riak`     | (placeholder) | 0 (falls back)         | RESP-2 (warns)    |

`riak` mode is a deliberate placeholder: the driver and host
launcher both emit a clear warning ("Riak mode requires the
dyn-riak crate, not yet available; falling back to redis") and
then run as `redis`. When the `dyn-riak` crate lands and
dynomited grows native Riak protocol support, this mode will
start `riak-kv` and emit Riak-protobuf traffic; until then the
flag exists so callers can wire `MODE=riak` into their tooling
without breakage.

The `data_store` config field is the only knob dynomited needs
to switch parser families on the client port; the seed list,
gossip, and peer wiring are protocol-agnostic.

### Workload classes

`workload-driver.py` covers every command class the relevant
parser supports.

In `redis` mode (8 classes, weighted):

```
strings   30%   SET GET GETSET INCR DECR INCRBY APPEND STRLEN GETRANGE
hash      15%   HSET HGET HDEL HMSET HMGET HGETALL HEXISTS HKEYS HLEN
set       10%   SADD SREM SMEMBERS SCARD SISMEMBER
zset      10%   ZADD ZREM ZSCORE ZCARD ZRANGE ZINCRBY
list      10%   LPUSH RPUSH LPOP RPOP LRANGE LLEN LINDEX
keyspace  10%   DEL EXISTS EXPIRE TTL PERSIST TYPE
multikey  10%   MGET MSET
scripting  5%   EVAL PING
```

In `memcache` mode (4 classes, weighted):

```
set       35%   set add replace append prepend
get       35%   get gets
arith     20%   incr decr (with periodic seed via set)
delete    10%   delete
```

The arith class periodically seeds the counter via `set` so
`incr` / `decr` have a chance of hitting a numeric value rather
than always observing `NOT_FOUND`.

### Driver dependencies

The workload driver has no third-party Python dependencies.
Both the RESP-2 and memcache ASCII clients are hand-rolled inline
so the test exercises exactly the bytes that go on the wire and
runs unchanged on FreeBSD's stock `python3`. The flake's
`python3` is sufficient.

### Retry semantics (`--retry-on`)

A real Dynomite operator's client SDK retries on transient
errors before treating them as a failure. The chaos workload
driver mirrors that: by default it retries `NoTargets` once and
never retries `Timeout`, controlled by a `--retry-on` flag and
a matching `RETRY_POLICY` env knob on `coordinator.sh`.

```
--retry-on=NoTargets:1,Timeout:0   (default)
--retry-on=NoTargets:3,Timeout:1,Closed:0
--retry-on=                        (empty -> never retry; pre-2026-05-25 behaviour)
```

The spec is a comma-separated list of `Class[:N]` entries.
`N` is the per-call retry budget (missing `:N` defaults to 1).
Valid classes:

| Class             | Source                                                 |
|-------------------|--------------------------------------------------------|
| `NoTargets`       | RESP `-DYNOMITE: ...`, memcache `SERVER_ERROR ... no quorum`, Riak `RpbErrorResp` matching "NoTargets" / "no quorum" |
| `Timeout`         | `socket.timeout` from a recv, Riak `errmsg` containing "timeout" |
| `Closed`          | `ConnectionError` from a hand-rolled reader, `OSError` (ECONNRESET, EPIPE) |
| `WrongConnection` | RESP `-NOAUTH ...` (clears after reconnecting)         |

Anything that does not match a known shape is `Unknown` and is
never retried (treated as a failure on the first attempt).

Each retry consumes 1 from the per-class budget; budgets reset
between workload ops. When the budget is exhausted the request
counts as a failure with the original error class.

#### Reading retries vs failures

Every per-second NDJSON snapshot now carries three counters:

```json
{
  "counts":   { "strings/SET": 12345 },
  "failures": { "strings/NoTargets": 3 },
  "retries":  { "strings/NoTargets": 12, "strings/Timeout": 2 }
}
```

* High `retries`, low `failures`: the cluster wobbled but the
  driver absorbed the wobble. Healthy.
* High `retries` AND high `failures`: a class is genuinely past
  its budget. Investigate.
* Low `retries`, high `failures`: rare for the new defaults
  but means most failures are in the `Unknown` bucket
  (unmapped server-error shapes; non-recoverable). Update the
  classifier in `workload-driver.py` if a recurring class is
  being miscategorised.

The note in `failures` keys: the second segment is now the
semantic class name (`NoTargets`, `Timeout`, `Closed`,
`WrongConnection`, `Unknown`) rather than the Python exception
type name (`RespError`, `ConnectionError`, `MemcacheError`,
`RiakPbcError`). The report generator picks this up
transparently; tooling that parsed the old key shape needs
updating.

#### Behaviour change

Default behaviour changed on 2026-05-25: previously every
raised exception counted as a failure. To restore the old
semantics exactly (every error counts), pass `--retry-on=`
on the workload-driver CLI or set `RETRY_POLICY=""` in the
coordinator's environment. Reports generated before this date
have no `retries` field; the report generator treats the
missing field as zero.

## Running a pass

From the coordinator host (floki):

```bash
# 2-hour redis pass (default)
RUN_ID="prod-$(date -u +%Y%m%d-%H%M%SZ)" \
CHAOS_DURATION_SECS=7200 \
  scripts/chaos-multi-host/coordinator.sh

# 2-hour memcache pass
RUN_ID="prod-mc-$(date -u +%Y%m%d-%H%M%SZ)" \
MODE=memcache \
CHAOS_DURATION_SECS=7200 \
  scripts/chaos-multi-host/coordinator.sh

# Override the retry policy (more aggressive)
RUN_ID="prod-aggressive-$(date -u +%Y%m%d-%H%M%SZ)" \
RETRY_POLICY="NoTargets:3,Timeout:1" \
  scripts/chaos-multi-host/coordinator.sh

# Disable retries entirely (pre-2026-05-25 behaviour)
RUN_ID="prod-noretry-$(date -u +%Y%m%d-%H%M%SZ)" \
RETRY_POLICY="" \
  scripts/chaos-multi-host/coordinator.sh

# Detached (immune to terminal SIGHUP / parent-shell exit):
RUN_ID="$RUN_ID" CHAOS_DURATION_SECS=7200 MODE=redis \
  scripts/chaos-multi-host/launch-detached.sh \
    /tmp/chaos-prod.log /tmp/chaos-prod.pid
```

While a run is live:

```bash
# Single-shot snapshot
scripts/chaos-multi-host/live-status.sh "$RUN_ID"

# Periodic logger (10 min interval by default)
scripts/chaos-multi-host/watch-status.sh "$RUN_ID" 600
```

After the run completes, the per-host logs are rsynced into
`target/chaos-multi-host/$RUN_ID/{floki,arnold,nuc,meh}-logs/`.
Generate the morning report with:

```bash
python3 scripts/chaos-multi-host/generate-report.py \
    target/chaos-multi-host/"$RUN_ID"
```

The report auto-discovers the four hosts from the `*-logs/`
subdirectories, so adding (or losing) a host between passes does
not require any code change.

## Per-host preparation

The coordinator assumes each remote host has:

* `/scratch/dynomite-chaos/src/` populated with a checkout of the
  repo (rsync'd from the coordinator). On `meh` and `arnold` the
  source can be the host's persistent checkout; the coordinator
  reads scripts from `$ROOT/src/scripts/chaos-multi-host/` so any
  mirror will do.
* A `dynomited` binary at either
  `/scratch/dynomite-chaos/build/release/dynomited` (preferred)
  or `/scratch/dynomite-chaos/src/target/release/dynomited`.
* Either `redis-server` and/or `memcached` on `PATH`, or a
  container runtime (`podman` or `docker`) so the host launcher
  can fall back to a container-image backend.

The `meh` host (the new fourth DC, NixOS x86_64) typically uses
the same native-binary setup as `floki`: redis-server and
memcached come from the flake's dev shell, and dynomited is a
plain `cargo build --release` artefact.

## Smoke

The agent that ships changes to this rig is expected to run a
60-second local smoke pass for each MODE before declaring done.
The smoke spins up `start-host.sh` + `workload-driver.py` against
a loopback backend on the lead host (no SSH) and confirms the
driver produces traffic with at least one successful operation.
See the journal entry
`docs/journal/2026-05-24-chaos-multi-mode.md` for the smoke
results from the addition of `meh` and the multi-mode work.

## Fault library

The chaos injector ships four families of failure modes,
selected per pass via the `MODE_FAULTS` environment variable
on `coordinator.sh`:

```bash
MODE_FAULTS=process,network,clock,disk \
  scripts/chaos-multi-host/coordinator.sh
```

When `MODE_FAULTS` is unset (the default) the injector runs the
legacy three-timer process-only schedule unchanged, byte-for-
byte identical to the pre-2026-05-26 behaviour. Setting the env
var explicitly (even to `MODE_FAULTS=process`) switches to the
unified scheduler that picks one fault uniformly at random
across the enabled-and-runnable classes every 60-180 s. The
coordinator does not parse `MODE_FAULTS` itself; it just
exports it through to each per-host injector.

### Process (`process`)

Default, always available. No host prerequisites beyond the
ability to signal child PIDs.

| Sub-fault                    | What it does                                  |
|------------------------------|-----------------------------------------------|
| `fault_process_pause`        | SIGSTOP dynomited for 5-15 s, then SIGCONT    |
| `fault_process_kill`         | SIGKILL dynomited and restart via start-host  |
| `fault_process_redis_bounce` | Restart the local redis/memcached datastore   |

### Network (`network`)

Per-host network jitter via `tc qdisc` on a configurable
device (default `lo`; override with `CHAOS_NETEM_DEV=eth0` or
`CHAOS_NETEM_DEV=ts0` for Tailscale).

| Sub-fault                    | What it does                                  |
|------------------------------|-----------------------------------------------|
| `fault_network_partition`    | 100% drop on the dynomite peer port (5-30 s)  |
| `fault_network_delay`        | 50-200 ms one-way delay (30-90 s)             |
| `fault_network_loss`         | 1-5% packet loss (30-60 s)                    |
| `fault_network_bandcap`      | 1 Mbit/s throughput cap (30 s)                |

**Prerequisites**: `tc` from `iproute2` plus `CAP_NET_ADMIN`
on the injector process. The flake ships `iproute2`; the
operator is responsible for granting the capability (the rig
runs as the unprivileged `gburd` user, so this typically
means a `setcap cap_net_admin=ep` on the injector binary, an
unprivileged user namespace, or running the injector under
sudo).

If either prerequisite is missing the network class is
dropped from the runnable set at start-up. The injector emits
a `prereq_skip` event with the reason and continues with the
remaining classes; every host's `injector_classes` event also
lists both the configured and the runnable class set so the
operator can confirm at a glance which classes actually
fired during a pass.

Cleanup is a single `tc qdisc del dev <dev> root` and is
idempotent. The SIGTERM trap calls it on every exit path; a
startup-time scrub also runs it once so a previous injector
that died mid-fault does not leak a qdisc into the next pass.

### Clock (`clock`)

Wall-clock skew applied to a fresh dynomited launch via
`faketime` (libfaketime).

| Sub-fault                    | What it does                                  |
|------------------------------|-----------------------------------------------|
| `fault_clock_skew` (positive)| `+30..+120 s` skew for 60-120 s               |
| `fault_clock_skew` (negative)| `-10 s` skew for 60-120 s                     |

The single `fault_clock_skew` routine picks the sign uniformly
per cycle; the negative variant exercises the gossip
phi-accrual detector under a peer whose `now()` runs behind
the cluster.

The mechanic: the injector writes the offset to
`$RUN/clock-skew-active`, kills dynomited, and calls
`restart_dynomited` with `FAKETIME=<offset>` exported.
`start-host.sh` honours the env knob (and falls back to
reading the marker file) to wrap the dynomited launch with
`faketime "$FAKETIME"`. After the duration, the marker is
removed, dynomited is killed again and restarted under the
real clock.

**Prerequisites**: `faketime` on `PATH`. The flake's
`libfaketime` package ships it; on hosts where it is missing
the class is dropped at start-up.

Cleanup removes the marker file. The dynomited running under
faketime keeps running until either the next clock-skew cycle
or the coordinator's teardown phase, which is the desired
behaviour: log collection sees a dynomited that exited with
the skew that was applied.

### Disk (`disk`)

Per-host I/O degradation against the host's tmpfs and the
redis backend's block device.

| Sub-fault                    | What it does                                  |
|------------------------------|-----------------------------------------------|
| `fault_disk_squeeze`         | Fill `/scratch` to 95% (capped at 10 GiB)     |
| `fault_disk_full`            | `dd` until `ENOSPC`, hold for 5 s             |
| `fault_disk_iolat`           | cgroups-v2 `io.max` 1 MiB/s cap on redis      |

The `iolat` sub-fault creates a cgroup at
`/sys/fs/cgroup/chaos-iolat-<DC>`, moves the redis pid into
it, and writes a 1 MiB/s read+write `io.max` limit for the
block device backing `/scratch`. Approximating "5 ms+/op
latency" via a throughput cap is a deliberate pragmatic
choice: cgroups v2 only exposes throughput and IOPS knobs,
and the operator-visible failure mode (slow redis ->
dispatcher queue growth -> client timeouts) is the same.

**Prerequisites**:

* cgroups v2 mounted at `/sys/fs/cgroup`.
* The `io` controller listed in `cgroup.controllers`.
* Write access to the chosen cgroup path (typically root or a
  systemd-delegated subtree).

`squeeze` and `full` only need write access to `$RUN`; if
those are met but the `io` controller is missing, the disk
class is still runnable but `iolat` self-skips at fault
invocation time with a `fault_disk_iolat_skipped` event.

Cleanup removes both ballast files and resets every entry in
`io.max` to `max`. The cgroup itself is kept across faults
to avoid the move-process-back-to-root dance; a stale cgroup
with default `io.max` settings is harmless.

### Required-tool detection

The injector probes each enabled class once at start-up:

* `network`: `command -v tc` plus a no-op qdisc add+del.
  If both fail, the class is dropped.
* `clock`: `command -v faketime`. If missing, drop.
* `disk`: `/sys/fs/cgroup/cgroup.controllers` exists, lists
  `io`, and `$RUN` is writable. The `iolat` sub-fault has
  finer-grained checks at invocation time.

The results land in two events on the chaos-events stream:

```
{"kind":"prereq_skip","detail":{"class":"network","reason":"tc-add-denied"}}
{"kind":"injector_classes","detail":{"configured":"process,network,clock,disk","runnable":"process,disk"}}
```

The report generator picks both up histogrammatically; an
operator can confirm at a glance which classes actually fired
during a pass.

### Cleanup-on-trap policy

Every fault routine pairs an install with a matching cleanup.
The SIGTERM/SIGINT trap on the injector top-level calls
`cleanup_all`, the union of the per-class cleanups, all
idempotent: `tc qdisc del`, `rm -f` of marker / ballast
files, `io.max` reset to `max`. `cleanup_all` also runs once
at injector startup so a previous run that died mid-fault
without running its trap does not leak state into the next
pass.

The smoke test
`scripts/chaos-multi-host/test_fault_smoke.sh` exercises each
fault routine with a shortened duration and asserts the host
is clean afterwards. A leaked tc qdisc, marker file, or
ballast file is a hard test failure. Run it locally before
shipping changes that touch the injector:

```bash
bash scripts/chaos-multi-host/test_fault_smoke.sh
```

The smoke skips classes whose prereqs are missing on the dev
box; to exercise the full matrix on a developer workstation,
combine an unprivileged user network namespace (which grants
`CAP_NET_ADMIN`) with a flake-provided `faketime`:

```bash
nix-shell -p libfaketime --run \
  "unshare -rn bash -c 'ip link set lo up; \
     bash scripts/chaos-multi-host/test_fault_smoke.sh'"
```
