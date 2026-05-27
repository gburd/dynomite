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

## 6. Phi-accrual failure detector for peer plane (post-gossip)

**Source**: discussion of Cassandra/Akka/Riak-style failure
detection vs Dynomite's current consecutive-failure model.

**Why**: today our peer health story is the C reference's:
periodic gossip ticks + a counter that ejects after N missed
updates. That model is brittle for cross-DC links with high
latency variance (e.g. our floki<->arnold Tailscale path at
95ms +/- 200ms). The Cassandra-style accrual detector adapts
the threshold to historical inter-arrival times and gives a
continuous suspicion value rather than a binary up/down.

**Scope**: peer plane only. Backends (redis) get pinged by
actual requests, not heartbeats; `auto_eject::AutoEject` is the
right model there and stays.

**Ordering**: depends on gossip being wired in the binary,
which is currently logged as 'deferred'. Sequence:
  1. Wire gossip task in `dynomited::server` (~2 days).
  2. Add `cluster::failure_detector::PhiAccrual` next to
     `net::auto_eject::AutoEject` (~1 day with tests).
  3. Feed gossip heartbeats into the detector; emit
     `PeerState::Down` transitions when phi crosses the
     configured threshold (default 8, configurable via
     `phi_threshold:` pool config).

**Implementation sketch**:
```rust
pub struct PhiAccrual {
    window_size: usize,
    intervals: VecDeque<f64>,    // ms between recent heartbeats
    last_heartbeat: Option<Instant>,
    threshold: f64,              // default 8.0
}

impl PhiAccrual {
    pub fn record_heartbeat(&mut self, now: Instant) {
        if let Some(prev) = self.last_heartbeat {
            let dt_ms = now.duration_since(prev).as_secs_f64() * 1000.0;
            if self.intervals.len() == self.window_size {
                self.intervals.pop_front();
            }
            self.intervals.push_back(dt_ms);
        }
        self.last_heartbeat = Some(now);
    }

    pub fn phi(&self, now: Instant) -> f64 {
        let Some(last) = self.last_heartbeat else { return 0.0; };
        let elapsed_ms = now.duration_since(last).as_secs_f64() * 1000.0;
        // Exponential model: -log10(P(elapsed > x)) given mean.
        let mean = self.intervals.iter().sum::<f64>()
            / self.intervals.len().max(1) as f64;
        if mean <= 0.0 { return 0.0; }
        // P(X > elapsed) = exp(-elapsed / mean) for exponential.
        // phi = -log10(P) = elapsed / (mean * ln(10))
        elapsed_ms / (mean * std::f64::consts::LN_10)
    }

    pub fn is_suspect(&self, now: Instant) -> bool {
        self.phi(now) > self.threshold
    }
}
```

A per-peer instance lives next to the peer's `PeerState`. The
gossip task calls `record_heartbeat` on incoming gossip; a
periodic ticker checks `phi` against threshold and updates
`PeerState::Down` when crossed. AutoEject for backends stays
untouched.

**Estimated effort**: ~1 day for the detector itself; gossip
wiring is a separate ~2 day item.

## 7. Quoracle-based operator tool (deferred, optional)

**Source**: review of the quoracle crate at
`ssh://git@codeberg.org/gregburd/rs-quoracle.git`.

**Idea**: a `dyn-quoracle-tool` binary (analogous to our existing
`dyn-hash-tool`) that takes a YAML pool config + chosen
consistency level and emits read/write resilience numbers, load
distribution under various read fractions, capacity ceilings,
and recommended token placements. Pure offline analysis; not on
the runtime path.

**Adopt as runtime dep**: no. Dynomite's dispatcher does not
benefit from runtime LP-based quorum optimization; consistency
levels are static.

**Adopt as tool**: optional, deferred until a user asks. The
value is real (helps operators design clusters and reason about
failure tolerance) but it's a separate side project.

**Estimated effort**: 1-2 days for a useful first cut.

## Latent bug: `make_error` produces empty Msg with no payload

**Source**: surfaced during the 2026-05-23 audit. Fixing F1
(the three pre-v0.1.0 conformance failures, commit `490a258`)
exposed it.

**Symptom**: any code path that returns
`DispatchOutcome::Error` - `NoTargets`, peer-channel full
(`try_send` failure), datastore-channel full - sends 0 wire
bytes to the client and the client hangs until its read
timeout. The error Msg is built by
`crate::msg::response::make_error`, which sets type / flags /
error code / dyn-error code on a fresh `Msg::new(...)` but
never attaches an mbuf with the on-the-wire error string. The
client driver's `handle_response` iterates `env.rsp.mbufs()`
and writes each chunk; with zero mbufs there is nothing to
write.

**Severity**: every operational error currently masquerades as
a hang. Operators see "client timeout" and have no signal in
their logs that the cluster actively decided to reject the
request.

**Fix**: wrap (or replace) `make_error` so it encodes the
appropriate wire format alongside the metadata:

* Redis: `-Dynomite: <message>\r\n` for `RspRedisError`,
  `-DYNOMITE: <message>\r\n` (or the matching specific prefix
  `-NOAUTH`, `-LOADING`, `-OOM`, `-BUSY`, `-NOSCRIPT`,
  `-READONLY`, `-MISCONF`, `-BUSYKEY`) for the typed error
  variants.
* Memcache: `SERVER_ERROR <message>\r\n` for
  `RspMcServerError`, `ERROR\r\n` for `RspMcError`.

The message text comes from the `DynErrorCode`. Build the
mbuf from the `Conn`'s `MbufPool` (the dispatcher does not
hold one today; either thread one through or attach the mbuf
in the client driver right before `handle_response`).

**Estimated effort**: half a day. Add per-`DynErrorCode`
message strings, attach the mbuf in `make_error`, write a
regression test that exercises every error path in the
dispatcher (`NoTargets`, both `try_send` paths) and asserts
the client receives a parseable error reply rather than a
timeout.

## Pass-3 follow-ups (queued 2026-05-25 from redis-mode results)

Pass-3 redis mode showed a 14-point success-rate drop vs pass-2
(99.17% -> 85.31%) and a 5-hour nuc teardown hang.  See
`dist/chaos-reports/v0.1.0/multi-host-pass-3-redis.md` (forthcoming
via item P3-2.6 below).

### Tier 1 (small, high-value; from this run's findings)

#### P3-1.1 Token-coverage validation

Add `cluster::pool::validate_token_coverage()` that asserts the
union of every peer's token range is `[0, u32::MAX]` with no gaps.
Run at config validation time.  Reject configs where the ring is
incomplete.  Likely root cause of the 14-point drop: the chaos
coordinator's hardcoded 4-way split with only 3 hosts running
left ~25% of the ring (3G..4G) unowned, so every key hashing
into that range returned `NoTargets` (now visible as a real error
since the `make_error` fix shipped).  Half-day effort.

#### P3-1.2 Teardown timeout

Wrap every teardown SSH in `timeout 60 ssh ...` so a wedged tunnel
cannot block the next mode.  For nuc specifically: try direct SSH
over Tailscale first; fall back to ProxyJump only on failure.
20 minute fix; saves 10+ hours per pass-3.

#### P3-1.3 Restart-failed residual diagnosis

arnold and nuc still show 11% `restart_failed` rate even with
bug #12's stale-kill in place (73 + 76 events in pass-3).  Capture
the actual error from `start-host.sh` when it fails (currently
just an event tag); add `event restart_failed_detail "<stderr-tail>"`.
Run pass-4 with the captured details and triage from there.
Half-day investigation budget.

#### P3-1.4 meh as 4th chaos host

Refactor the SSH runner pattern in `coordinator.sh` from
`"${runner[@]}" "<cmd>"` (string-arg, login-shell-dependent) to
`"${runner[@]}" bash -s <<<"<cmd>"` (stdin-piped, login-shell-
independent) at every callsite.  About 7-10 callsites.  Add meh
back to the host list once that lands.  Unblocks 4-host chaos.
1-2 days.

### Tier 2 (deeper; medium effort)

#### P3-2.5 Gossip-driven peer-state oscillation

With phi-accrual now live, every kill/restart cycle moves a peer
Normal->Down->Normal as gossip catches up.  Each transition has
a settling window where `Replicas` plans return NoTargets.  Add
metrics: per-minute peer-state-transition count; classify errors
by cause (peer-Down vs timeout vs channel-full).  If gossip churn
dominates the error budget, expose `gossip_phi_threshold` more
prominently and tune via pass-5.  3-5 days.

#### P3-2.6 Auto-generate per-mode chaos report

Extend `scripts/chaos-multi-host/generate-report.py` to take
`--run-id` and emit
`dist/chaos-reports/v0.1.0/multi-host-pass-N-<mode>.md` with
the standard table (per-host + aggregate counts, success rate,
chaos events by kind, top failure reasons).  Eliminates the
hand-pasting that the chaos lead has been doing every run.
1 day.

#### P3-2.7 Workload-driver retry semantics

Current driver counts every error as a failure.  In production
a Dynomite operator's client SDK retries on `NoTargets` and
`Timeout`.  Add `--retry-on <comma-separated-errors>` to
`workload-driver.py` (default: retry NoTargets x1, Timeout x0).
Re-run a focused 30-minute redis chaos to separate "transient
gossip churn" from "genuine data unavailability".  1 day.

### Tier 3 (broader; 1+ weeks each)

#### P3-3.8 Network-partition + clock-skew + disk-full chaos modes

Stub scripts under `scripts/netem/` exist but are not wired
into the injector rotation.  Pass-3 only exercised
SIGSTOP/SIGKILL/redis-bounce.  Extend `chaos-injector.sh` with
`MODE_FAULTS=process+network+clock+disk` env knob; per-pass we
pick which faults to enable.  1 week.

#### P3-3.9 Differential rig integration with chaos

**Phase 1+2 (substrate): DONE 2026-05-26.**
  See `docs/journal/2026-05-26-differential-chaos-substrate.md`.
  `MODE=differential` in `coordinator.sh` and `start-host.sh`
  brings up a Rust dynomited and a C `dynomite` side by side
  on every chaos host; the C cluster listens on shifted
  ports (Rust + 100) and shares the backend redis. The C
  binary is built per host by
  `scripts/chaos-multi-host/build_cref_remote.sh` (idempotent
  against the submodule's git commit hash). Single-host
  smoke at `scripts/chaos-multi-host/smoke-differential.sh`.

**Phase 3+4 (workload fan-out + reply comparison): DONE 2026-05-26.**
  See `docs/journal/2026-05-26-differential-phases-3-4.md`.
  `workload-driver.py --mode differential` dispatches every
  op to both proxies in parallel via a `DualConn` thread
  pair, captures both replies, and consults
  `scripts/chaos-multi-host/differential_allowlist.py`
  (8 allowlist entries covering INFO/TIME timing fields,
  CLIENT connection bookkeeping, and unordered
  KEYS/SCAN/SMEMBERS/HKEYS/HVALS/HGETALL responses) to
  classify each op as `agreed`, `divergent`, or
  `one_side_failed`. The NDJSON output gains three new
  buckets per row plus a capped `divergent_samples` list.
  Existing `counts`/`failures`/`retries` are unchanged.
  Coordinator wires `CLIENT_LISTEN_PORT_C` and passes the
  fan-out flags. Smoke extended with a 30s differential
  workload assertion (>0 `agreed`).

**Phase 5 (chaos applied to both proxies in lockstep):**
  still queued. `chaos-injector.sh` currently kills only
  the Rust dynomited; phase 5 adds parallel pidfile lookup
  for `dynomite-c.pid` so process-level faults
  (SIGSTOP/SIGKILL, redis-bounce) hit both proxies on the
  same schedule. Network/clock/disk faults are host-level
  and already affect both processes. ~3 days. Design notes
  live in
  `docs/journal/2026-05-26-differential-chaos-substrate.md`
  under "Phase 5".

### Random-slicing investigation (parallel to Tier 1)

Independent design item: a friend's article on InfoQ
(https://www.infoq.com/articles/dynamo-riak-random-slicing/)
plus a reference C implementation at `hash_demo.c` describes
random-slicing as an alternative to consistent hashing.  Key
property: intervals always cover the full ring by construction,
so a 3-of-4 host topology cannot leave a quadrant unowned.
Investigate fit as an alternative `Distribution` mode alongside
`vnode` / `ketama` / `modula`.  Design-only deliverable;
implementation gated on operator approval.
