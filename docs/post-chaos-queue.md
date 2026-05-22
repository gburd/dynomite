# Post-chaos work queue

Items to pick up once the in-flight 2h chaos run completes
(coordinator pid in `/tmp/chaos-prod.pid`; ETA 2026-05-22 03:01Z).

## 1. Redis AUTH support (`redis_requirepass`)

**Source**: bug surfaced by `lampmanyao/dynomite` fork review
(`docs/upstream-fork-review.md`).

**Gap**: Production Redis deployments commonly require a password.
`backend_supervisor` in `crates/dynomited/src/server.rs` opens a TCP
connection to the configured backend but never authenticates. A
backend with `requirepass` set will reject every subsequent
command with `NOAUTH Authentication required`.

**Implementation**:

1. **Config**: add `redis_requirepass: Option<String>` to
   `crates/dynomite/src/conf/pool.rs::ConfPool`. Match the C field
   name. Document it in the rustdoc and in
   `crates/dynomited/conf/dynomite.yml.example`.

2. **AUTH handshake**: extend `backend_supervisor` (and
   `peer_supervisor` for the dnode plane if peers behind auth) to
   perform an authenticate step right after the TCP handshake but
   before draining the request queue:

   ```rust
   if let Some(pw) = pool_cfg.redis_requirepass.as_deref() {
       let cmd = build_resp(&[b"AUTH", pw.as_bytes()]);
       transport.write_all(&cmd).await?;
       let reply = read_one_resp_line(&mut transport).await?;
       if !reply.starts_with(b"+OK") {
           return Err(NetError::Auth(format!(
               "redis AUTH rejected: {}",
               String::from_utf8_lossy(&reply).trim()
           )));
       }
   }
   ```

   The reply parsing is intentionally tiny (read-until-CRLF). The
   request channel must NOT receive any pending request until AUTH
   succeeds.

3. **Failure handling**: AUTH rejection is fatal for the
   connection (and therefore the supervisor reconnects with
   bounded backoff, same as any other connect failure). Log the
   first rejection at `error!`, subsequent retries at `warn!`.

4. **No-password fast path**: when `redis_requirepass` is `None`
   the supervisor skips the AUTH block entirely (no behavior
   change for existing configs).

5. **Memcache**: deferred. Memcache binary AUTH is more involved
   (SASL); operators who need it can use a sidecar.

**Tests**:

- Unit test in `cluster/dispatch.rs`: not needed, AUTH is
  transport-level not dispatch-level.
- Unit test in `dynomited::server::tests`: build a Server with
  `redis_requirepass: Some("secret")`, verify the AUTH bytes show
  up first on a stub backend that records writes.
- Integration test under `crates/dynomited/tests/`: spawn
  `redis-server --requirepass secret`, configure dynomited with
  the same password, verify `SET / GET` work end-to-end. Skipped
  when `redis-server` is missing, gated behind `--features
  integration`.

**Estimated effort**: 4-6 hours including tests + docs.

**Validation against the chaos pipeline**: after the AUTH change
lands, optionally re-run a 30-min chaos pass with all three DCs
configured to use a passworded redis. The existing rebuild cycle
(podman on arnold, native on nuc, native on floki) is unchanged.

## 2. Pidfile flock race on fast restart

**Source**: bug surfaced by the live chaos run on arnold:

```
ERROR dynomited: create pid file
      error=flock /scratch/dynomite-chaos/run/dynomited.pid:
            EAGAIN: Try again
      path=/scratch/dynomite-chaos/run/dynomited.pid
```

Observed 5 times across ~3 SIGKILL+restart cycles on arnold during
the 2h pass-1 chaos run.

**Cause**: chaos-injector.sh does `kill -KILL <pid>` then
`sleep 2` then calls start-host.sh, which spawns a new dynomited
via `nohup ... &`. The new dynomited tries to `flock(LOCK_EX |
LOCK_NB)` the pidfile. The kernel may not have fully reaped the
killed process yet, so its flock entry is still in the inode's
lock list, causing EAGAIN.

**Impact during pass-1**: zero client-visible failures. The
backend_supervisor's reconnect retry loop eventually won (~2-3
restart attempts per kill before one succeeded). The system is
tolerant; the noise just shows up as `restart_failed` events in
the chaos-events ndjson.

**Two-pronged fix**:

1. In `dynomited::pidfile`, retry the `flock` call up to N
   times (e.g. 5 times at 100ms intervals) before bailing. The
   kernel reaps in microseconds normally; a tiny retry budget
   absorbs the race entirely.

2. In `chaos-injector.sh`, after `kill -KILL`, busy-wait until
   `kill -0 <pid>` returns non-zero (process is fully gone)
   before calling start-host.sh. Bound the wait to ~5s.

Fix (1) is the load-bearing one (helps any operator-driven
restart, not just chaos). Fix (2) is a chaos-test hygiene fix.

**Estimated effort**: 1 hour for both, including a unit test
that opens an flock and verifies the new pidfile loop retries.

## 3. (optional) Workload-driver source-port hardening

**Source**: bug surfaced by the live chaos run (FreeBSD-only loopback
ephemeral-port collision when nuc's dynomited died).

**Gap**: when dynomited goes down on FreeBSD, the workload's
reconnect attempt to `127.0.0.1:18102` can have its kernel-picked
ephemeral source port collide with `18102` itself, producing a
self-connection that blocks the next dynomited rebind.

**Mitigation in workload-driver.py**:

```python
def connect(self):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Force the ephemeral source out of the dynomite/redis range
    # so a loopback-self-connection cannot occur on FreeBSD.
    sock.bind(("127.0.0.1", 0))  # let kernel pick, but
    # ... actually the right fix is `setsockopt IP_PORTRANGE` or
    # binding into a known-safe high range. Both are platform
    # specific; experiment.
    sock.connect((self.host, self.port))
    return sock
```

This is a workload-driver issue, not a dynomite issue. Low
priority.

**Estimated effort**: 30-60 min including a FreeBSD repro.

## 4. Pass-2 multi-host chaos run

**Prereqs**:
- Item 1 (AUTH) lands so a more realistic deployment is testable.
- The pass-2 design at `docs/chaos-pass2-design.md` is implemented
  already (see commit `f9e9a6b`); coordinator topology needs
  switching to per-DC distinct tokens (`floki=0,
  arnold=1431655765, nuc=2863311530`) to actually exercise the
  cross-DC routing.

Then re-run the 2-hour chaos with `read_consistency: DC_ONE`
(coalescer not yet wired) and observe whether peer-channel
fan-out to remote DCs works under chaos (kills, pauses, redis
bounces).

## 5. Curate the pass-1 chaos report

After the post-run reporter generates `report.md`, hand-curate
it into `dist/chaos-reports/v0.1.0/multi-host-pass-1.md`
including:

- the 8 bugs found (most listed in commit messages already)
- timeline of kills / pauses across the run
- any observed throughput dips correlated with chaos events
- per-host CPU / memory if the metrics make it easy
- what pass-2 should additionally measure
