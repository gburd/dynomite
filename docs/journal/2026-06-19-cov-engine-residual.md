# Coverage: engine residual core + supporting files

Date: 2026-06-19
Branch: stage/cov-engine-residual
Worktree: /home/gburd/ws/wt-cov-engine-residual

## Goal

Close the residual coverage gaps in the dynomite ENGINE core and a
few supporting-tier files left after the protocol/stats/util raise.
Core target 95% line+function; supporting target 75% line+function.

## Method

Tests only. Touched production source only for two pure,
behaviour-preserving test-visibility refactors:

* `core/log/host.rs`: extracted the hostname-cleaning logic into a
  private `sanitize_hostname(&str)` so the empty-fallback arm is
  unit-testable without a real empty hostname. No behaviour change.

All other changes are new `#[cfg(test)]` tests, new `tests/` files,
or additions to existing in-file test modules (no reordering).

## Files raised (before -> after, line% / function%)

CORE (target 95/95):

| File | Before | After |
|---|---|---|
| net/tls.rs | 76.54 / 66.25 | 90.17 / 76.40 (handshake/transport deviation) |
| core/log/mod.rs | 80.22 / 78.85 | 93.98 / 96.30 (defensive arms deviation) |
| core/log/syslog.rs | 89.29 / 66.67 | 95.95 / 85.19 (write-error edge deviation) |
| core/log/format.rs | 94.44 / 87.50 | 100.00 / 100.00 |
| core/log/host.rs | 91.30 / 100.00 | 96.88 / 100.00 |
| capability/registry.rs | 85.71 / 78.95 | 98.86 / 100.00 |
| capability/negotiator.rs | 89.47 / 85.71 | 100.00 / 100.00 |
| capability/mod.rs | (raised) | 100.00 / 100.00 |
| failure_detector.rs | 92.76 / 90.91 | 98.26 / 100.00 |
| admin_rpc.rs | 96.40 / 93.02 | 99.47 / 95.65 |
| hints.rs | 95.17 / 90.57 | 97.36 / 96.83 |
| random_slicing.rs | 87.46 / 87.88 | 95.69 / 97.78 |
| msg/response.rs | 94.25 / 100.00 | 97.96 / 100.00 |

SUPPORTING (target 75/75):

| File | Before | After |
|---|---|---|
| runtime/throttle.rs | 70.49 / 66.67 | 96.72 / 100.00 |
| embed/extension.rs | 57.14 / 50.00 | 100.00 / 100.00 |
| dynomite-vec/src/engine.rs | 82.50 / 69.23 | 100.00 / 100.00 |
| dynomite-vec/src/turbo_index.rs | 79.70 / 68.18 | 98.23 / 100.00 |

Every supporting file clears 75/75. Every core file clears the
95/95 gate on at least the function axis except where a documented
deviation applies (tls handshake/transport, the two log defensive
files, where the residual lines are reasoned-unreachable).

## Property tests added (hegel)

`crates/dynomite/tests/cluster_residual_property.rs`:

* `phi_is_monotone_in_silence` -- phi-accrual suspicion is
  monotone non-decreasing in the silence interval given a stable
  heartbeat history.
* `negotiation_outcome_is_commutative` -- both peers pick the same
  highest-common capability value regardless of which side runs
  the merge.

`random_slicing` determinism / full-coverage / rebalance already
have property tests in `tests/random_slicing_property.rs`; not
duplicated.

## Tests added

* In-file `#[cfg(test)]` additions: extension, throttle (integration
  file), response, log/format, log/host, log/syslog, log/mod,
  capability/registry, failure_detector, admin_rpc, hints,
  random_slicing, net/tls, dynvec engine + turbo_index.
* New `tests/` files: `core_log_state.rs` (global-install lifecycle
  in its own process), `cluster_capability_residual.rs` (decode
  error arms + stock-cap arms), `cluster_residual_property.rs`.

Approx 60 new test functions + 2 property tests.

## Deviations proposed (appended to docs/coverage-deviations.md)

* net/tls.rs: live-handshake-only paths (resolver bodies, transport
  poll impls, mid-read io error arms). Covered by the integration
  test `peer_plane_tls.rs`, not by the per-file unit gate.
* core/log/mod.rs: `LoggerWriter` stderr None-fallback (lines 146,
  160), the sink-write-error counter arm (151-153, needs a failing
  sink), and a dead `Level::WARN` arm in a test-only closure (604).
* core/log/syslog.rs: the `?` write-error edges of the two
  `format_event` `write!` calls (135, 193).
* hashkit/random_slicing.rs: empty-table `is_empty`/`index_for`
  arms (no public empty constructor) + assert-message strings.
* cluster/hints.rs: the 64-bit-unreachable `checked_add` overflow
  guard in `decode_records` (660). Permission-denied disk paths are
  covered on non-root runners and no-op as root.

## Bugs found

None. No production logic changed beyond the pure host.rs refactor.

## Verification

* `cargo nextest run -p dynomite-engine` -> 1238 passed, 1 skipped.
* `cargo nextest run -p dynomite-vec` -> 91 passed.
* `cargo test --doc -p dynomite-engine -p dynomite-vec` -> green.
* `cargo clippy -p dynomite-engine -p dynomite-vec --all-targets
  -- -D warnings` -> clean.
* `cargo fmt -p dynomite-engine -p dynomite-vec -- --check` -> clean.
* ASCII check on all changed files -> clean.
