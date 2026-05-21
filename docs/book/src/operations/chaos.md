# Chaos test

The Stage 16 chaos test exercises a multi-DC dynomite cluster
under continuous failure injection for one hour and asserts
that no client request observes a permanent error and that
every documented invariant holds throughout the run. It is
the final pre-tag gate.

## Location

* Test: `crates/dynomite/tests/stage_16_chaos.rs`
* Failure injectors: `scripts/netem/`
  * `partition_dc.sh`
  * `slow_peer.sh`
  * `flap.sh`
  * `gc_pause.sh`
  * `clock_skew.sh`
* Run artefact: `target/chaos/<run-id>/report.md`

## Modes

The test honours two environment variables:

| Variable             | Default | Purpose                                               |
|----------------------|---------|-------------------------------------------------------|
| `CHAOS_DURATION_SECS`| `3600`  | Wall-clock duration of the steady-state phase.        |
| `CHAOS_SEED`         | `0`     | Deterministic seed for the workload + injector RNGs. |

The test is gated behind `--features chaos` so a stock
`cargo test` does not pull it in. Two practical run shapes:

```bash
# 60-second smoke (CI-friendly when --features chaos is set)
CHAOS_DURATION_SECS=60 \
  cargo nextest run --workspace --features chaos --test stage_16_chaos

# 1-hour production run (manual, requires CAP_NET_ADMIN)
sudo -E env "PATH=$PATH" \
  cargo nextest run --release --workspace --features chaos --test stage_16_chaos \
    --no-capture
```

## Prerequisites

The test self-checks each prerequisite and emits a `SKIP`
notice (test passes, body is a no-op) when one is missing:

* `redis-server` on `PATH`. The harness spawns one Redis
  backend per dynomite node.
* `tc` (iproute2) and `CAP_NET_ADMIN` (or root). Required by
  every `scripts/netem/*` injector.
* `faketime` (libfaketime). Required by the clock-skew
  injector at the 30-minute mark.

When run without these, the test prints a structured `SKIP`
report and exits 0; the smoke variant is therefore safe to
run on any developer laptop.

## Topology timeline

```
t=0            bootstrap: 3-node single-DC cluster.
t=0..600s      grow to 9 nodes across 3 DCs (3+3+3 racks).
t=600..3000s   steady state.
t=3000..3600s  shrink to 3 nodes single-DC.
```

Plus mid-run nodes joining and leaving on a Poisson cadence
(mean inter-arrival 90 s) so every transition in
`dyn_state_t` (INIT, STANDBY, WRITES_ONLY, RESUMING, NORMAL,
JOINING, DOWN, RESET, UNKNOWN) is observed at least once.

## Workload mix

* 50 / 50 read / write steady state.
* 60-second 90%-write spike windows every 10 minutes.
* Three concurrent client populations: interactive
  (P99 budget 5 ms), batch (P99 5 s), background (no
  latency budget).

## Invariants asserted

| Invariant                                          | Where          |
|----------------------------------------------------|----------------|
| No request returns the wrong key's value           | client thread  |
| Quorum-acked writes survive any single-node failure| dispatcher     |
| `DC_EACH_SAFE_QUORUM` writes survive a DC partition| dispatcher     |
| Auto-eject reinstates within `server_retry_timeout`| failure detector|
| Gossip converges within 60 s of any topology event | gossip task    |
| No tokio task is detached without `into_detached()`| Stage-13 hooks |

Any violation aborts the run and writes a non-zero exit
status; otherwise the report ends with `STATUS: PASS`.

## Cleanup discipline

The test installs an `on_drop` sweep that:

1. Sends `SIGTERM` then `SIGKILL` to every spawned dynomited
   instance.
2. Sends `SIGTERM` to every spawned redis-server.
3. Removes every `tc qdisc` injected on the loopback device.
4. Unsets `FAKETIME` for every spawned process.
5. Captures a final `tc qdisc show dev lo` and asserts it
   matches the pre-test baseline.

If any sweep step fails the test's exit code reflects the
sweep failure, not the workload outcome, so the harness can
distinguish between "workload failed" and "host left dirty".

## Report artefact

The run produces `target/chaos/<run-id>/report.md` containing:

* Start / end times and total wall clock.
* Topology timeline event log.
* Per-injector firing counts.
* `dyn_state_t` per-state hit counter.
* Per-window throughput and tail-latency histograms
  (interactive / batch / background).
* Any invariant violations (empty on a clean run).

The report is the artefact the v0.1.0 release tag references.
The lead curates the production-mode run output into
`dist/chaos-reports/v0.1.0/report.md` before the signed tag.
