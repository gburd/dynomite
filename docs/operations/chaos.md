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
