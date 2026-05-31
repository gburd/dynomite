# 2026-05-24 - Entropy reconciliation run-loop wiring

**Branch**: `stage/entropy-runloop`
**Status**: READY_FOR_REVIEW

## Context

`dynomited::server::Server` previously emitted a warning at
startup whenever `recon_key_file:` was set in the YAML pool
config:

```
recon_key_file is set but the entropy run loop is not yet wired (deferred)
```

The warning was the only observable outcome: the periodic
entropy reconciliation cycle that ships in
`crates/dynomite/src/entropy/{send,receive,util}.rs` was not
driven. This change wires the run loop end-to-end so a
configured key + IV pair drives a real reconciliation cycle.

## What changed

### New: `dynomite::entropy::driver`

* `EntropyDriver` - long-running task that walks the
  `ServerPool`'s peer list at a configured cadence and pushes a
  snapshot to each non-local peer.
* `reconcile_with_peer` - one-shot helper for a single
  per-peer cycle. The driver and tests call this directly; it
  builds an `EntropyConfig`, dials `peer:port`, and forwards to
  `EntropySender::push_with_material` so the AES key loaded
  once at startup is reused across cycles (no re-reading the
  on-disk file every five minutes).
* `ReconCycle` - per-cycle counters
  (`peers_attempted`, `peers_exchanged`, `ranges_diverged`,
  `ranges_repaired`) emitted at INFO level on every tick. The
  current implementation interprets a non-empty pushed
  snapshot as one repaired divergent range; richer per-range
  Merkle-tree comparison is a future stage.
* `DEFAULT_RECON_INTERVAL` (5 minutes) and
  `DEFAULT_ENTROPY_PORT` (8105, mirrors the C reference's
  `ENTROPY_PORT` macro) constants.

### `dynomite::entropy::send`

* Added `EntropySender::push_with_material(cfg, source, override_material)`.
  When `override_material` is supplied, the file-loading step
  is skipped and the pre-loaded `EntropyMaterial` is used
  verbatim. The historical `EntropySender::push(cfg, source)`
  is now a thin wrapper that forwards `None` for the override.

### `dynomite::conf::ConfPool`

* Added `recon_interval_seconds: Option<u64>` (the only conf
  change permitted by the brief). Defaults to 300 seconds via
  `apply_defaults`. Operators can override with
  `recon_interval_seconds: 60` (etc.) in the YAML pool stanza.

### `dynomited::server::Server`

* Replaced the `has_recon_keys: bool` flag with
  `entropy_driver: Option<EntropyDriver>` and
  `entropy_key_path: Option<PathBuf>`. The driver is built at
  construction time only when:
  1. `recon_key_file:` resolves to a non-empty path,
  2. `recon_iv_file:` resolves to a non-empty path,
  3. both files exist on disk, and
  4. `dynomite::entropy::util::load_material` succeeds.
  Any other branch logs a single WARN and disables the task,
  preserving today's behaviour for pools that do not opt in.
* New helper `build_entropy_driver` factored out so the giant
  `Server::build` does not grow.
* New public accessors `Server::entropy_enabled() -> bool`
  and `Server::entropy_cadence() -> Option<Duration>` so tests
  (and external embedders) can verify the wiring without
  scraping logs.
* `Server::run` spawns the driver task with a clone of the
  shutdown `watch::Receiver<bool>`. On shutdown the task
  drains its current per-peer cycle and exits; the run loop
  also calls `JoinHandle::abort()` as belt-and-braces.
* The deferred-warning is replaced with an INFO event:
  ```
  entropy reconciliation task started key_file=... cadence_seconds=...
  ```
  When the entropy task is disabled (the common case in CI
  hosts that do not have `conf/recon_key.pem` on disk) the
  helper logs a single WARN and the run loop is silent.

## Tests

`crates/dynomited/tests/entropy_runloop.rs` (new, 8 tests):

1. `server_builds_entropy_driver_when_recon_key_file_is_valid`
   - asserts `entropy_enabled()` is true and the configured
   cadence propagates.
2. `server_skips_entropy_when_recon_key_file_is_empty` -
   regression: explicit `recon_key_file: ''` disables the
   task.
3. `server_skips_entropy_when_recon_key_file_is_missing` -
   regression: a non-existent path on disk disables the task.
4. `reconcile_with_peer_pushes_snapshot_to_receiver` -
   integration with a live `EntropyReceiver`; asserts the
   per-cycle counters and the sink's bytes.
5. `driver_runs_cycle_then_drains_on_shutdown` - integration
   with a live receiver; asserts the driver pushes at least
   once, then honours `watch::Sender::send(true)` and exits
   within 2s.
6. `driver_zero_cadence_rounds_up_to_default` - pins the
   "cadence = 0 means default" contract.
7. `empty_cycle_is_default` - `ReconCycle::default()` is a
   true zero.
8. `two_drivers_reconcile_with_each_others_receivers` - the
   in-process distillation of the brief's "two dynomited
   instances with deliberately divergent state" scenario:
   each driver pushes a divergent snapshot at a peer
   receiver, both report `ranges_diverged = 1` and
   `ranges_repaired = 1`.

`crates/dynomite/src/entropy/driver.rs` (new module, 6
unit tests): cadence defaults, cycle accumulator helpers,
local-peer skipping, pre-set shutdown short-circuit.

## Verification

```
cargo build --workspace --all-targets --locked            OK
cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -p dyniak -p dyn-admin -- --check  OK
cargo clippy --workspace --all-targets --all-features -- -D warnings  OK
cargo nextest run --workspace                             1125 passed (was 1111)
cargo test --doc --workspace                              613 passed
bash scripts/check_no_todos.sh                            OK
bash scripts/check_no_port_comments.sh                    OK
bash scripts/check_ascii.sh                               OK
```

## Notes

* The driver currently uses an empty `StaticSnapshot` as the
  default `SnapshotSource`. A richer source (per-range Merkle
  digests pulled from the embedded data store) is a future
  stage; the driver shape and counters already accommodate
  it. Embedders that need a different source today can plug
  their own `BoxedSnapshotSource` through the
  `dynomite::entropy::driver::EntropyDriver::new` constructor
  (the `dynomited` binary builds the driver internally; that
  internal wiring will become an embedding hook when the
  source is generalised).
* `EntropyConfig` has non-optional `key_file` / `iv_file`
  fields; the driver supplies empty `PathBuf`s when calling
  `push_with_material(cfg, source, Some(material))` because
  the override short-circuits the file load. A cleaner long-term
  refactor would split `EntropyConfig` into "wire knobs" and
  "material source", but that touches the
  `crates/dynomite/src/entropy/` public API more broadly than
  this stage's scope.
* The `recon_interval_seconds` directive defaults via
  `apply_defaults` so a zero value (which would otherwise
  busy-loop) rounds back to the documented 300 seconds via the
  `EntropyDriver::new` cadence-zero fallback.
