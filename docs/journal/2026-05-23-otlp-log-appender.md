# 2026-05-23 - OTLP log appender wiring

Branch: `feat/otlp-log-appender` (off `main` at `e6037c5`).

Wires up the second pillar of the four-pillar observability
plan: when `observability.otlp_logs_endpoint` is set,
`dynomited` now installs an OpenTelemetry `LoggerProvider` that
exports `tracing` events as OTLP log records to the configured
collector. The fmt layer (stderr or `--output` file) keeps
writing in parallel; the OTLP log appender mirrors the same
event stream to the collector. Operators that leave the field
unset see no behaviour change at all (no OTel SDK install, no
exporter task).

Closes deliverable 2 of the four-pillar observability brief
referenced in `docs/journal/2026-05-23-audit.md` ("OTLP log
appender wiring (deps in tree; ~1 day)" - the deps were
referenced but not actually present; this branch lands them
plus the wiring).

## Files touched

- `Cargo.toml` - workspace deps:
  - `opentelemetry-appender-tracing = "0.27"` added (new),
  - `opentelemetry_sdk` features grew `"logs"`,
  - `opentelemetry-otlp` features grew `"logs"`,
  - replaced the "OTLP log appender ... never landed" comment
    with the new appender comment block.
- `crates/dynomited/Cargo.toml` - new
  `opentelemetry-appender-tracing` dep (workspace).
- `crates/dynomited/src/observability.rs` - rewritten:
  - new `init_otlp_logger(cfg) -> Result<Option<LoggerProvider>, ObservabilityError>`,
  - new `otlp_logs_enabled(cfg) -> bool` predicate,
  - new `otlp_any_enabled(cfg) -> bool` predicate (master
    switch the binary uses to dispatch between the fmt-only
    and the layered install paths),
  - `TracerGuard` renamed to `ObservabilityGuard` (now holds
    both the trace and the log provider) with a
    `pub type TracerGuard = ObservabilityGuard;` alias for
    one-step migration of any out-of-tree caller,
  - `install_global` now accepts `cfg` and builds whichever
    of (trace, log) layer combination is requested by the
    config; both layers are stacked onto the shared fmt
    layer + EnvFilter Registry,
  - the layer composition uses
    `Vec<Box<dyn Layer<Registry>>>` to side-step the
    trait-object stacking limit (a `Box<dyn Layer<Registry>>`
    is only a `Layer<Registry>`; once another layer is
    layered onto Registry the inner subscriber type changes
    and a second boxed layer cannot stack on top).
- `crates/dynomited/src/main.rs` - the dispatch now uses
  `otlp_any_enabled` (was `otlp_traces_enabled`); the guard
  variable is renamed to `obs_guard`; the shutdown call still
  runs on the runtime so both batch processors flush.
- `crates/dynomited/tests/observability.rs` - new tests:
  - `otlp_logs_enabled_returns_false_for_default_config`,
  - `init_otlp_logger_with_no_endpoint_returns_none`,
  - `otlp_log_appender_bytes_reach_mock_listener` (`#[ignore]`d
    like the parallel `otlp_grpc_bytes_reach_mock_listener`
    trace test; runs in 3s under nextest with
    `--include-ignored`).
- `docs/book/src/operations/tracing.md` - documentation
  expanded to cover both pillars: title is now "Distributed
  tracing and OTLP logs", the configuration example shows the
  new `otlp_logs_endpoint` knob, the operator-knobs table
  grows a row, and a new "Implementation notes" bullet covers
  the appender's body / attribute mapping.
- `docs/book/src/SUMMARY.md` - SUMMARY entry retitled to
  match the broader scope.

## Test counts

Baseline on `main` before this branch (`e6037c5`):

- `cargo nextest run --workspace`: 653 tests run, 3 skipped.
- `cargo test --doc --workspace`: 576 dynomite + 12 dynomited
  doctests (588 total).

After this branch:

- `cargo nextest run --workspace`: **658 tests run, 4 skipped**
  (+5 tests, +1 ignored). The new tests:
  - 3 unit tests inside `observability.rs`
    (`otlp_logs_enabled_returns_true_for_set_endpoint`,
    `init_otlp_logger_with_no_endpoint_returns_none`,
    `init_otlp_logger_with_empty_endpoint_returns_none`),
  - 2 integration tests in `tests/observability.rs`
    (`otlp_logs_enabled_returns_false_for_default_config`,
    `init_otlp_logger_with_no_endpoint_returns_none`),
  - 1 `#[ignore]`d integration test
    (`otlp_log_appender_bytes_reach_mock_listener`).
- `cargo nextest run -p dynomited --features integration --test conformance --profile conformance`:
  **34 tests run, 0 skipped** (unchanged).
- `cargo nextest run -p dynomited --features integration --test observability`:
  6 tests run, 2 skipped (was 4/1; +2 non-ignored, +1 ignored).
- `cargo test --doc --workspace`: **576 dynomite + 15
  dynomited doctests** (+3 dynomited; new doctests on
  `init_otlp_logger`, `otlp_logs_enabled`, `otlp_any_enabled`).
- `cargo build --workspace --all-targets --locked`: clean.
- `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -- --check`:
  clean.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  clean.
- `scripts/check_no_todos.sh`,
  `scripts/check_no_port_comments.sh`, `scripts/check_ascii.sh`:
  all clean.

The ignored OTLP-bytes-flow tests both pass when run with
`--include-ignored` (each takes ~3s):

```
PASS [ 3.010s] dynomited::observability otlp_grpc_bytes_reach_mock_listener
PASS [ 3.011s] dynomited::observability otlp_log_appender_bytes_reach_mock_listener
```

## Design notes

- **Why a `Vec<Box<dyn Layer<Registry>>>` for the layer
  composition.** The fmt layer has type
  `Box<dyn Layer<Registry> + Send + Sync>`. Stacking a second
  boxed-trait layer on top via `.with()` does not type-check,
  because the first `with` re-parameterises the subscriber
  from `Registry` to `Layered<.., Registry>`, and the
  trait-object's `Layer<Registry>` impl does not extend to the
  new shape. Concrete generic layer types
  (`tracing_opentelemetry::OpenTelemetryLayer`, the
  `OpenTelemetryTracingBridge`) impl `Layer<S>` for any `S`,
  so the workaround is to wrap them in `Box<dyn Layer<Registry>>`
  and collect into a `Vec`. `Vec<L: Layer<S>>` itself impls
  `Layer<S>`, so the whole composite slots into Registry as
  one layer.
- **Why both pillars share `install_global`.** The brief asked
  for two providers but a single subscriber. `tracing` only
  allows one global default, so the OTLP-on path needs to
  install all the layers atomically. Splitting into two
  install entry points would fight the `OnceLock` inside
  `tracing-subscriber`'s default-dispatcher. The unified
  `ObservabilityGuard` keeps both providers alive across the
  runtime so their batch processors flush during `shutdown`.
- **Why `LoggerProvider::shutdown()` runs inside the runtime.**
  The OTel SDK's batch processor uses `tokio::spawn` to drain
  in-flight log records. Calling `shutdown` outside a runtime
  panics. The shutdown path in `main.rs` was already
  `runtime.block_on(async move { g.shutdown() })` for the
  trace provider; the same call now drains both providers in
  one go because they share a guard.
- **Why the appender does not get its own `service_name`.** The
  brief specified the same `service_name` as the trace
  exporter; `build_resource(cfg)` is the single source of
  truth and is reused for both providers.

## Open questions / follow-ups

- **Sampling for log records.** The OTel logs SDK 0.27 family
  does not have a sampler abstraction (unlike the trace SDK).
  Volume gating is via `RUST_LOG`. Documented in tracing.md.
- **`opentelemetry-appender-tracing 0.32`** is in the cache
  but was not adopted; the rest of the OTel stack is at 0.27,
  and bumping the appender alone would require bumping
  everything else. The 0.32 family also reshapes the
  `LogRecord` trait. Deferred to a coordinated full-stack
  bump.
- **Trace-log correlation.** The appender emits log records
  with the active span's trace and span IDs when the
  `tracing-opentelemetry` layer is also active. With both
  pillars on, a collector can join records to spans on
  `trace_id`/`span_id`. No explicit work was needed; this
  follows from the OTel context-propagation design.

## Verification commands run

```
cargo build --workspace --all-targets --locked --offline
cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
cargo nextest run -p dynomited --features integration --test conformance --profile conformance
cargo nextest run -p dynomited --features integration --test observability
cargo test --doc --workspace
bash scripts/check_no_todos.sh
bash scripts/check_no_port_comments.sh
bash scripts/check_ascii.sh
```

All clean.
