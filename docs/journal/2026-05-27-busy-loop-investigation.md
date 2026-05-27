# 2026-05-27 - busy-loop investigation: backend_supervisor reconnect storm

## Smoking gun

Pass-6 chaos (`pass3-redis-20260527-200437Z`) caught one snapshot of
`dynomited` on host `meh` at 1781% CPU (~18 cores). The process
self-recovered when the chaos kill cycle terminated it ~30 s later,
so no live stack trace exists; the forensic evidence is in
`target/chaos-multi-host/pass3-redis-20260527-200437Z/meh-logs/`.

Timeline excerpt from `dynomited-dc-meh.log` over the 22:25-22:27 UTC
window:

```
22:25:25.527  WARN backend_supervisor{ds=Memcache}: backend connection ended; reconnecting backend=127.0.0.1:17100 error=protocol parse error: Error
22:25:30.537  WARN backend_supervisor{ds=Memcache}: backend connection ended; reconnecting backend=127.0.0.1:17100 error=protocol parse error: Error
22:25:35.547  WARN backend_supervisor{ds=Memcache}: backend connection ended; reconnecting backend=127.0.0.1:17100 error=protocol parse error: Error
... (sustained 5 s cadence in the log; underlying loop is much faster)
```

The 5 s cadence in the log is from tracing rate-limiting; the
underlying reconnect loop was not throttled.

## Root cause

`backend_supervisor` in `crates/dynomited/src/server.rs` reconnected
with a flat `tokio::time::sleep(Duration::from_millis(50))` after
`run_one_backend_conn` returned an error. With:

* meh's chaos harness left a `redis:7-alpine` container bound to
  `127.0.0.1:17100` because the pass-6 teardown SSH timed out at 60 s.
* The coordinator then reconfigured `dynomited` into memcache mode
  (`pass3-memcache-...`) and pointed it at the same port.
* Every memcache probe hit a Redis server that returned `-ERR unknown
  command`, which the memcache parser treats as a fatal `Error`
  variant.

The supervisor's flat 50 ms sleep meant ~20 reconnects/sec against a
working TCP port; combined with the parser's allocation pattern and
the response-channel teardown work the symptoms matched the observed
1781% CPU shape.

## Fix

* Replace the flat 50 ms sleep with multiplicative-jitter exponential
  backoff: 50 ms initial, 5 s cap, factor 2.0, jitter uniform
  `[0.5, 1.5]`.
* Reset the backoff to its initial value only when the previous run
  successfully parsed at least one frame. A connection that opens,
  fails its first parse, and disconnects no longer earns a free
  reset (this was the original bug shape: connect succeeded so the
  pre-fix code reset `backoff_ms` on every iteration).
* Register a Prometheus counter `backend_reconnect_total{backend,
  reason}` so the same shape is detectable from metrics scrapes
  even when the per-attempt `WARN` line is suppressed by tracing
  rate limiting.
* Throttle the per-reconnect `WARN` log to the first three failures
  plus every tenth thereafter (`should_log_reconnect`).

The same shape almost certainly affects `peer_supervisor` and the
client-driven AUTH-failure path, but the brief explicitly scoped
the fix to `backend_supervisor`. Follow-up tracked separately.

## Reproduction

`crates/dynomited/tests/regression_busy_loop.rs`:

* `supervisor_throttles_reconnect_storm_against_always_closing_backend`
  - Spawns a synthetic backend that accepts every TCP connect and
    immediately closes the socket.
  - Drives `backend_supervisor` against it for 2 s.
  - Asserts `<= 30` reconnect attempts in the window. Pre-fix this
    is ~40 (flat 50 ms cadence); post-fix the median is 6.
  - Asserts the `backend_reconnect_total{reason="closed"}` counter
    incremented at least once per observed accept.
* `supervisor_cpu_stays_bounded_against_always_closing_backend`
  - Same harness; reads `/proc/self/stat` field 14 (`utime`) before
    and after the 2 s window.
  - Asserts user-mode CPU stays under 1.5 cores.

## Verification

```
cargo test -p dynomited --test regression_busy_loop  # 6 passed
cargo nextest run -p dynomited                       # 74 passed
cargo nextest run --workspace --features dynomited/riak  # 1558 passed
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
bash scripts/check_no_todos.sh
bash scripts/check_no_port_comments.sh
bash scripts/check_ascii.sh
```

## Files touched

* `crates/dynomited/Cargo.toml` - add `prometheus`, `rand` deps.
* `crates/dynomited/src/lib.rs` - register new `metrics` module.
* `crates/dynomited/src/metrics.rs` - new; `backend_reconnect_total`.
* `crates/dynomited/src/server.rs` - jittered-exp backoff in
  `backend_supervisor`; new helpers (`jittered_backoff`,
  `next_backoff_ms`, `classify_reconnect_reason`,
  `should_log_reconnect`, `record_reconnect_and_back_off`,
  `spawn_backend_supervisor_for_testing`); `run_one_backend_conn`
  now reports `frames_ok` to the supervisor.
* `crates/dynomited/tests/regression_busy_loop.rs` - new; six tests
  covering the helper invariants, the metric, the reconnect-rate
  bound, and the CPU bound.
