# Pass-3 Multi-Host Chaos Report (redis mode)

**Run ID**: `pass3-redis` (replace with the live run id once
the chaos coordinator emits it; coordinator artifacts under
`target/chaos-multi-host/pass3-*`).
**Window**: ~2 hours, three hosts (`floki`, `arnold`, `nuc`).
**Mode**: redis-only workload; SIGSTOP/SIGKILL/redis-bounce
chaos schedule (the same shape pass-2 used, plus the bug #11
debounce fix).

## Headline numbers

| | total |
|---|---|
| Successful requests | 3,097,369 |
| Failed requests | 533,356 |
| Success rate | **85.31%** |

This is a **14-point drop** vs pass-2's 99.17% on the same
workload mix. The drop is the entire signal pass-3 surfaced.

## Per-host breakdown

(Numbers are taken from the per-host `workload-dc-*.ndjson`
files; the chaos-events ndjson lives next to them and is the
source for the chaos-event tallies in the next table.)

| | floki | arnold | nuc |
|---|---|---|---|
| Wall time | (~2h) | (~2h) | (~2h) |
| Successful requests | 1,061k | 1,002k | 1,034k |
| Failed requests | 168k | 187k | 178k |
| Fail % | ~13.7% | ~15.7% | ~14.7% |

The per-host fail rates are consistent within ~2 percentage
points, which rules out a single-host bug (no DC is
materially worse than the others). It is also consistent
with the chaos schedule landing on each host with similar
cadence.

## Chaos events by kind

(Source: per-host `chaos-events-dc-*.ndjson`; same shape as
pass-2.)

| Event | floki | arnold | nuc |
|---|---|---|---|
| pause cycles | ~30 | ~30 | ~60 |
| SIGKILL+restart | ~5 | ~5 | ~9 |
| redis bounces | 0 | ~4 | 0 |
| recovery_restart | ~0 | ~0 | ~0 |
| pause_skipped | 0 | 0 | 0 |

The bug #11 debounce held: arnold no longer thrashes its
recovery-restart loop, so the chaos schedule actually
exercised every host this pass. Pass-2's distorted "0.03%"
on arnold is gone; the failure rate now reflects real chaos.

## Original framing (pre-validation)

The brief that triggered this work read pass-3's drop as
"1/4 of the ring unowned" because the coordinator hardcoded
a 4-way token split (`0`, `1G`, `2G`, `3G`) for four hosts
but only spawned three. That framing predicted a static
config-time gap that a token-coverage validator would catch.

## Revised root-cause analysis

A close read of the dispatcher
(`crates/dynomite/src/cluster/vnode.rs::dispatch` plus
`crates/dynomite/src/cluster/dispatch.rs::collect_routable`)
shows the original framing is **incorrect**. The vnode
dispatcher implements wrap-around semantics: tokens above
the highest continuum entry wrap to the first peer, tokens
at or below the lowest entry resolve to the first peer, and
tokens in the middle are bracketed. There is no `(lo, hi)`
window in `[0, u32::MAX]` that fails to resolve. The full
walk is captured in
`docs/journal/2026-05-25-token-coverage-validation.md`
(section "Why `Gap` is structurally impossible").

This means:

* If the live conf listed all four peers and one never
  started, the continuum has all four entries; tokens
  hashing into the missing peer's arc resolve to it
  deterministically. Once gossip's phi-accrual flags the
  peer as `PeerState::Down`,
  `dispatch.rs::collect_routable` filters the entry out
  for reads. **The dispatcher then returns
  `DispatchPlan::NoTargets` for that arc.** This is a
  runtime liveness issue, not a config-time gap.
* If the live conf listed only three peers, the three
  remaining tokens partition the entire ring under the
  wrap-around rule. `validate_token_coverage` (the new
  config-time validator) returns `Ok(())` here, correctly,
  and no `NoTargets` is ever emitted.

Pass-3's 14-point drop, therefore, is most plausibly
**gossip phi-accrual settling** during the kill/restart
cycles:

1. Chaos injector SIGKILLs `dynomited` on host A.
2. Hosts B and C continue gossiping with each other but
   stop receiving heartbeats from A.
3. Phi-accrual on B and C climbs over the suspicion
   threshold (default 8.0) within roughly 8x the gossip
   interval (default 1s) -> ~8s settling window.
4. During those ~8 seconds, B and C still believe A is
   `Normal`. Reads that hash into A's arc are forwarded to
   A's outbound channel, which silently drops them (the
   peer-supervisor task is reconnecting), or returns
   `DispatchPlan::NoTargets` once the supervisor's channel
   is closed.
5. Once phi crosses the threshold, B and C reclassify A as
   `Down`. Reads into A's arc now return `NoTargets`
   immediately at the planner layer.
6. Restart fires; chaos-injector debounce ensures only one
   restart is in flight. A comes back up, gossip resumes,
   first heartbeat promotes A back to `Normal`.

A 14% fail rate over a 2-hour window with ~5-9 kill cycles
per host plus the pause cycles is consistent with each kill
producing a brief `NoTargets` window that scales by the
number of hosts (3) and the fraction of the keyspace each
host owns (1/3).

## What pass-3 did NOT validate

* It did not directly attribute the failures to gossip
  settling. The error counters in pass-3 are the pre-fix
  single-bucket `client_err` counter that bundles every
  cause into one number.
* It did not measure the actual settling window per kill
  cycle. We assume ~8s based on the default phi threshold
  and gossip interval; pass-4 should measure it.

## What pass-4 will validate

The metrics shipped alongside this report
(`docs/journal/2026-05-25-gossip-churn-metrics.md`) split
the single error bucket into per-cause counters:

* `dispatch_no_targets_total{dc,rack,consistency_level}` -
  the planner-layer "no routable peer" count, the cause we
  expect to dominate during gossip settling.
* `dispatch_peer_send_full_total{peer_idx,peer_dc}` and
  `dispatch_peer_send_closed_total{peer_idx,peer_dc}` -
  per-peer outbound-channel saturation.
* `dispatch_backend_send_full_total` and
  `dispatch_backend_send_closed_total` - local backend
  saturation.
* `dispatch_response_timeout_total{consistency_level}` -
  coalescer-level timeouts.
* `peer_state_transitions_total{peer_idx,from_state,to_state}` -
  every gossip-driven state flip.
* `peer_state_current{peer_idx,dc,rack}` - live state
  gauge.
* `gossip_phi_score_milli{peer_idx,dc,rack}` - the live
  phi value (in thousandths) so an operator can correlate
  the threshold setting against the observed value.

Pass-4 will run the same 2-hour redis chaos schedule with
these metrics scraped at 1s cadence. The expected
correlation is:

* `peer_state_transitions_total{from=NORMAL,to=DOWN}`
  fires exactly once per kill (modulo network jitter).
* `dispatch_no_targets_total` rate spikes for ~8s after each
  `(from=NORMAL,to=DOWN)` transition, then drops back to
  zero.
* `peer_state_transitions_total{from=DOWN,to=NORMAL}` fires
  exactly once per restart, ~5-15s after the kill.
* The integral of `dispatch_no_targets_total` over the
  whole run should match the integral of the per-host
  `workload-dc-*.ndjson` failure-rate spike to within 5%.

If the integral matches, pass-3's 14% drop is fully
explained by gossip settling and the next remediation step
is tuning the phi threshold (or pre-blacklisting conf-listed
peers that fail the initial connect for longer than
`gos_node_timeout_ms`). If the integral does NOT match, the
remaining error budget is in the `peer_send_*` or
`response_timeout` buckets and the remediation moves to
backpressure / channel sizing.

## References

* Revised dispatcher walk: `docs/journal/2026-05-25-token-coverage-validation.md`
* Metrics journal: `docs/journal/2026-05-25-gossip-churn-metrics.md`
* Metrics docs: `docs/book/src/operations/metrics.md`
  (Failure-cause counters section)
* Pass-2 baseline: `dist/chaos-reports/v0.1.0/multi-host-pass-2.md`
* Pass-1 baseline: `dist/chaos-reports/v0.1.0/multi-host-pass-1.md`
