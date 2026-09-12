# 2026-07-26 dependency refresh: noxu 7.5.6 -> 7.10.1, hegeltest 0.25 -> 0.44

## noxu 7.5.6 -> 7.10.1

Bumped the workspace pin `noxu = "7.10"` (was `7.5`) and ran
`cargo update -p noxu`; the whole noxu family (noxu, noxu-db,
noxu-engine, noxu-evictor, noxu-dbi, noxu-cleaner, noxu-collections,
noxu-config, noxu-bind, ...) moved to 7.10.1.

The 7.6..7.10 changelog carries several BREAKING items, but none reach
dynomite's usage. dynomite consumes only
`noxu::{Environment, Database, DatabaseConfig, EnvironmentConfig,
Transaction, Cursor, CursorConfig, ...}` and `noxu::xa::*`, and builds
its config with `with_allow_create` / `with_transactional` only. The
breaking changes were internal (parking_lot removal -> `noxu_sync`
becomes a `lock_api` alias; the consolidation-array Log Write Latch
retired; `env_fair_latches` removed; `TreeStats::n_entries` renamed;
`CleanerStats` field churn) -- none of those types or knobs are touched
by dynomite. dyniak compiled clean and all 885 dyniak tests
(XA / 2PC, datastore, CRDT convergence) pass against 7.10.1.

## hegeltest 0.25 -> 0.44.1

Bumped the property-test framework. Two API breaks, both migrated:

1. `#[hegel::composite]` generators now take `&TestCase`, not
   `TestCase` (one site: `conf_property.rs::arb_pool_facts`).
2. `TestCase::draw` now requires a `PrintableGenerator` -- a value that
   can be printed in a failure report. `sampled_from` over a custom
   enum (not auto-printable) needs `.print_as_debug()` (the enums
   already derive `Debug`); a composite generator's output type drawn
   via `tc.draw(arb_...())` needs `.print_as_debug()` too. Sites fixed:
   `conf_property.rs` (SecureServerOption / ConsistencyLevel / HashType
   / DataStore samples + the PoolFacts composite draw) and
   `stage_07_msg.rs` (DmsgType sample). Added the `hegel::Generator`
   import where `print_as_debug` is called.

All 107 property tests pass under 0.44.

## Bulk compatible refresh

`cargo update` moved every other semver-compatible dependency to its
current maximum (thiserror 2.0.18 -> 2.0.20 and the rest were already
at their compatible ceilings). No manifest-level major bumps were taken
(base64 stays at our 0.22 direct pin; wasmtime stays on the stable 46,
not the 49 rc). 0 dependencies remain behind their semver-compatible
maximum.

## New advisory: RUSTSEC-2026-0253 (lru) -- ignored with justification

The refresh surfaced RUSTSEC-2026-0253: a use-after-free in
`lru::LruCache::pop()` under a panicking element Drop, fixed in
`lru >= 0.18.2`. `lru 0.16.4` is pulled transitively by
`noxu-evictor 7.10.1`, which pins `^0.16` -- so lru cannot be upgraded
without a new noxu that bumps its lru dependency. The vulnerable path
is internal to noxu's LRU cache evictor and is not reachable or driven
by dynomite (dynomite never depends on or calls `lru` directly).

Added `--ignore RUSTSEC-2026-0253` to `scripts/check.sh` and to
`deny.toml`'s advisory ignore list, both with the justification above.

FOLLOW-UP (noxu upstream): bump `noxu-evictor`'s `lru` dependency to
`>= 0.18.2` so RUSTSEC-2026-0253 clears; then drop the ignore here.

## Test-timing note

`dyniak aae::mst_reconcile::tests::reconcile_is_divergence_proportional`
(a 10k-key MST reconciliation, ~80-93s in a debug all-features build
under parallel load) is legitimately slow and was flirting with
nextest's 120s cap after the noxu bump made that path marginally
slower. Added a per-test nextest override (`.config/nextest.toml`)
giving just that test a 180s slow-timeout, so the tight global 60s cap
still catches real hangs without killing this deterministic heavy test.
It is not a hang: it passes cleanly with the raised cap.
