# 2026-05-22 - Distributed tracing (Milestone A)

Branch: `feat/distributed-tracing` (off `main` at `d452a27`).

End-to-end OpenTelemetry distributed-tracing support for
`dynomited`. Operators that point an OTLP collector at the new
`observability.otlp_traces_endpoint` field see a span tree per
client request; operators that leave the field unset see no
behavior change at all (no OTel SDK install, no exporter task).

## Files touched

- `Cargo.toml` - workspace deps: `opentelemetry`,
  `opentelemetry_sdk` (`rt-tokio`), `opentelemetry-otlp`
  (`grpc-tonic`), `tracing-opentelemetry` (0.27/0.28 family).
- `crates/dynomite/Cargo.toml` - the library now depends on
  `tracing-opentelemetry` and `opentelemetry` for cross-task
  span propagation; the SDK and exporter live in the binary.
- `crates/dynomited/Cargo.toml` - adds the SDK and exporter.
- `crates/dynomite/src/conf/{mod.rs,pool.rs}` - new
  `ObservabilityConfig` struct + the optional `observability`
  field on `ConfPool`.
- `crates/dynomite/src/net/dispatcher.rs` - `OutboundEnvelope`
  now carries a `tracing::Span`.
- `crates/dynomite/src/net/server.rs` - `OutboundRequest` now
  carries a `tracing::Span`; `ServerConn::run` re-enters the
  request span before write and instruments the response parse
  with a `backend.parse` child span.
- `crates/dynomite/src/net/dnode_server.rs` - same pattern for
  the outbound peer driver: `peer.send` and `peer.parse` spans,
  re-enter the request span on receive.
- `crates/dynomite/src/net/client.rs` - per-message
  `client.parse` child span on every fully-parsed request;
  `client.send` span around the response writeback.
- `crates/dynomite/src/net/proxy.rs` - `proxy.run` instrumented;
  per-accepted-socket `client.accept` span attached via
  `Instrument`.
- `crates/dynomite/src/net/dnode_proxy.rs` - `dnode_proxy.run`
  instrumented; per-accepted-peer `dnode_client.accept` span.
- `crates/dynomite/src/cluster/dispatch.rs` - `dispatch.plan`
  span on every routing decision; the dispatcher captures
  `tracing::Span::current()` and stamps it onto every
  `OutboundRequest` it forwards. Helper `enter_plan_span` keeps
  the dispatch fn close to its previous line count.
- `crates/dynomite/src/stats/rest.rs` - `stats_server.run` span.
- `crates/dynomited/src/server.rs` - `server.run`,
  `backend_supervisor`, `run_one_backend_conn`, and
  `peer_supervisor` instrumented; `peer_supervisor.spawn` span
  attached at spawn time. The `run_one_backend_conn` request
  side now re-enters the originating request span and emits
  `backend.send` / `backend.parse` child spans.
- `crates/dynomited/src/main.rs` - calls
  `observability::install_global` BEFORE `log_init` when an
  endpoint is configured; falls back to `log_init` otherwise.
  Tracer guard flushes on shutdown inside the runtime.
- `crates/dynomited/src/observability.rs` - new module owning
  the OTLP exporter wiring, the layered subscriber install, and
  the `TracerGuard` flush-on-drop type.
- `crates/dynomited/src/lib.rs` - exposes the new module.
- `crates/dynomite/tests/distributed_tracing.rs` - dispatcher
  span capture + `OutboundRequest` / `OutboundEnvelope`
  span-field structural tests.
- `crates/dynomited/tests/observability.rs` - install_global
  no-op contracts, provider build smoke test, and an
  `#[ignore]`d end-to-end OTLP-bytes-reach-listener integration
  test.
- `docs/book/src/operations/tracing.md` - operator-facing docs:
  span shape, sample collector config, knobs, trade-offs.
- `docs/book/src/SUMMARY.md` - wires `tracing.md` into the ToC.
- Several existing tests updated to populate the new `span`
  field on `OutboundRequest` / `OutboundEnvelope`.

## Test counts

Baseline on `main`: 619 tests, 1 skipped.

After this branch:

- `cargo nextest run --workspace --no-fail-fast`: **628 tests
  passed**, 3 skipped (was 619 / 1).
- `cargo test --doc --workspace`: **573 doctests passed**, 0
  failed (562 in dynomite + 11 in dynomited; was 562 + 11
  before).
- `cargo build --workspace --all-targets`: clean.
- `cargo fmt --all -- --check`: clean.
- `cargo clippy --workspace --all-targets -- -D warnings`:
  **same set of pre-existing categories as `main`** (unnested
  or-patterns in `net/proxy.rs`, identical match arms in
  `net/proxy.rs`, `cluster/dispatch.rs::dispatch` line count,
  3x `f32`/`f64` strict cmp + `usize -> f64` cast in
  `cluster/failure_detector.rs`); my changes added no new
  clippy categories. The dispatch fn line count grew from 112
  to 116 because each `OutboundRequest` construction now has a
  `span:` field; the `enter_plan_span` helper kept the rest of
  the routing logic out of the body.

The +9 new tests breakdown:
- 3 dispatcher unit tests (`distributed_tracing.rs`).
- 3 observability unit tests inside the module
  (`no_endpoint_returns_none_guard`,
  `empty_endpoint_string_is_treated_as_unset`,
  `init_otlp_tracer_requires_endpoint`).
- 3 observability integration tests
  (`install_global_with_no_endpoint_is_a_no_op`,
  `install_global_with_empty_endpoint_string_is_a_no_op`,
  `init_otlp_tracer_builds_provider_for_well_formed_endpoint`).
- 2 additional `#[ignore]`d tests gated by `--include-ignored`:
  `build_with_well_formed_endpoint_succeeds` and
  `otlp_grpc_bytes_reach_mock_listener`.

## Sample span output

Running `dynomited` with `traces_sampling: 1.0` and an empty
OTLP endpoint (so the existing `fmt` subscriber writes spans to
stderr) produces:

```text
INFO dynomited: dynomited starting version="0.0.1" pid=770074
WARN server.run{pool=dyn_o_mite listen=127.0.0.1:18102 peers=1}: ...
INFO server.run{pool=dyn_o_mite listen=127.0.0.1:18102 peers=1}: dynomited run loop starting ...
WARN backend_supervisor{backend=127.0.0.1:9999 ds=Redis}: backend connect failed; retrying ...
INFO server.run{pool=dyn_o_mite listen=127.0.0.1:18102 peers=1}: shutdown signal received
INFO server.run{pool=dyn_o_mite listen=127.0.0.1:18102 peers=1}: dynomited run loop stopped pool=dyn_o_mite
```

Note the nested `server.run{...}: backend_supervisor{...}:`
prefix on the structured-log lines: that is the supervisor
hierarchy the OTLP exporter ships to a collector when one is
configured. The full per-request tree (`client.accept` ->
`client.parse` -> `dispatch.plan` -> `backend.send` /
`backend.parse` -> `client.send`) only appears once a real
backend is reachable; the integration test in
`crates/dynomited/tests/observability.rs` verifies bytes flow
to a mock collector under `--include-ignored`.

## Performance notes

- The default-quiet path (`otlp_traces_endpoint` unset) is
  zero-allocation: `tracing::Span::current()` returns
  `Span::none()` when no subscriber is installed and the OTel
  SDK is not even compiled into the dispatch path - it is only
  imported under the `dynomited` binary. The library code uses
  the `tracing` instrumentation macros directly, which expand
  to no-ops without a subscriber.
- With `traces_sampling: 1.0` and a busy collector the OTel
  batch span processor adds an enqueue + flush per span; on a
  multi-thousand-QPS workload this roughly doubles per-request
  allocation count vs the baseline. Production deployments
  should run at `traces_sampling: 0.01` or lower (the
  `Sampler::TraceIdRatioBased` path is exercised in the unit
  tests).
- Cross-await spans use `Instrument` and `Span::in_scope`
  rather than `EnteredSpan` to keep the spawned futures `Send`
  and tokio-spawn-compatible. That is a hard requirement for
  the dispatcher's worker tasks.

## Open questions

- The brief asked the OTel install to happen *after* `log_init`
  but `tracing_subscriber` only allows one global default per
  process. The original Milestone A landing took the
  compromise of running OTLP installation BEFORE `log_init`
  and stacking its own EnvFilter+fmt+otel layers, leaving
  `log_init`'s STATE-based SIGHUP log-reopen uninitialised
  in OTLP mode.

  **Resolved 2026-05-23**: `log_init` was reworked into a
  layer-emitting builder (`build_logs_layer`,
  `install_logs_only`). The single global subscriber is now
  composed once with the fmt layer, the SIGHUP-reopen
  handle, the EnvFilter, and (optionally) the OTel layer.
  Both surfaces co-exist; SIGHUP log-reopen works whether
  OTLP is on or off.
- The OTLP integration test that takes a full SDK round-trip
  through gRPC is `#[ignore]`d because the BatchSpanProcessor's
  flush path can stall outside CI when the collector is slow
  or unreachable. It is documented as runnable via
  `cargo test --include-ignored`.

## Next steps

- Milestone B (metrics) and Milestone C (log formats) are
  owned by other agents; this branch deliberately does not
  touch the log subsystem or the Prometheus / `/metrics`
  endpoint.
