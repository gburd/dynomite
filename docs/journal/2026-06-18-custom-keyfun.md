# 2026-06-18 -- Custom keyfun as a WebAssembly module (stage/custom-keyfun)

## Goal

Implement Riak's user-defined `chash_keyfun` (`CUSTOM = 99`) as a
real WebAssembly-backed keyfun in dyniak, and prove it with a test
that compiles REAL Rust to `wasm32-unknown-unknown` and runs it at
runtime to route live keys. No hand-written WAT for the headline
test; an operator-supplied Rust keyfun crate, compiled to WASM,
routes keys through the cluster routing path.

## What landed

### KeyFun::Custom

- `crates/dyniak/src/datatypes/keyfun.rs`: added
  `KeyFun::Custom(String)` (the `String` is the WASM module id).
  This makes `KeyFun` `Clone` rather than `Copy`; `Std` /
  `BucketOnly` stay allocation-free. Method receivers changed from
  `self` to `&self`.
- `from_wire(99)` no longer hard-errors: it decodes to
  `Custom("")` (the wire selector carries no module name, exactly
  like Riak's numeric selector). `to_wire()` maps `Custom` back to
  `99`.
- `route_bytes` panics on `Custom` (documented invariant: route
  `Custom` through the router's store); `try_route_bytes` returns
  `KeyFunError::Custom`. Added `is_custom`, `custom_module`, and a
  `route_len -> Option<usize>`.

### Module id in bucket props

- `BucketProps` gained `custom_keyfun_module: Option<String>`.
  `effective_keyfun()` threads it into `KeyFun::Custom(module_id)`.
- `RpbBucketProps` gained a dyniak-extension field
  `chash_keyfun_module: Option<Vec<u8>>` at tag 32 so PBC clients
  name the module (replacing Riak's `{modfun, Mod, Fun}`).

### WASM ABI and store

- `crates/dyniak/src/datatypes/keyfun_wasm.rs` (new, `wasm`-gated):
  `WasmKeyfunStore` wrapping the existing
  `mapreduce::wasm::WasmModuleStore`. Exports `keyfun_alloc` /
  `keyfun_route` (same linear-memory shape as MapReduce's
  `phase_alloc` / `phase_apply`, under keyfun-specific names so a
  module cannot be mis-used). Input framing:
  `bucket_len(u32-le) ++ bucket ++ key_len(u32-le) ++ key`. Output
  bytes ARE the route bytes.
- Refactored `mapreduce/wasm.rs`: extracted a protocol-agnostic
  `WasmModuleStore::run_module_raw(id, input, alloc, apply) ->
  Result<Vec<u8>, WasmRawError>` core. MapReduce `apply_phase` now
  CBOR-encodes/decodes around it; the keyfun store frames/uses raw
  bytes around it. Same `WasmLimits` (memory cap + fuel + epoch
  watchdog) apply to both; MapReduce behaviour is unchanged.
- Error mapping: trap/timeout -> `KeyFunError::Runtime`,
  oversize-memory -> `KeyFunError::MemoryLimit`, missing module ->
  `KeyFunError::ModuleNotFound`, non-zero status -> `Runtime` with
  the module's error string. Routing never panics or hangs.

### Routing (option (b): router owns the store)

- `BucketRouter` gained a `wasm`-gated `keyfun_store` field,
  `with_keyfun_store(..)` builder, and `keyfun_store()` accessor.
- New `BucketRouter::try_route(..) -> Result<RouteDecision,
  KeyFunError>`. `Std` / `BucketOnly` use the pure path; `Custom`
  runs the module through the store and feeds the output to
  `hash64`. `route(..)` delegates to `try_route` and only the pure
  variants reach it from production callers.
- `server.rs` get/put/del now call `try_route` and surface a
  routing error as an `RpbErrorResp` instead of panicking.

### Config-time safety

- `handle_set_bucket` rejects a `CUSTOM` selection with an empty
  module id or an unregistered module (emits `RpbErrorResp`), so
  routing never silently switches to a missing keyfun. Chosen over
  a logged Std fall-back as the safer option.
- `handle_get_bucket` echoes `chash_keyfun_module` so set/get
  round-trips.

## Tests

- Headline: `crates/dyniak/tests/wasm_rust_fixtures.rs` builds two
  fixture crates with `cargo build --release --target
  wasm32-unknown-unknown` (scratch under `/scratch` via
  `CARGO_TARGET_DIR`; gated to SKIP cleanly if the target is
  unavailable, runs for real on the pinned toolchain):
  - `tests/fixtures/keyfun-reverse` (`#![no_std]` cdylib): routes
    `<bucket>:<reversed key>`. The test registers the compiled
    `.wasm` as a custom keyfun, routes several live keys, asserts
    the route bytes exactly, and proves a Std bucket routes to a
    different hash. A negative test asserts an unregistered module
    surfaces `ModuleNotFound` cleanly.
  - `tests/fixtures/mapreduce-double` (std cdylib, ciborium +
    serde_json): a map phase doubling numeric `value`. Proves the
    Rust->WASM path for MapReduce phases too.
- Fast WAT unit tests in `keyfun_wasm.rs` for the ABI mechanics
  (reverse-key, trap, oversize, non-zero status, empty/unregistered
  id, shared-store-with-mapreduce) so the quick loop avoids the
  wasm-compile cost.
- `bucket_props_routing.rs`: CUSTOM-unregistered and CUSTOM-empty
  rejected at set-bucket time; CUSTOM-registered round-trips.
- Existing keyfun / router / bucket_props / MapReduce-wasm tests
  stay green (597 dyniak tests with `noxu,wasm`; 571 with `noxu`).

## Notes / divergences

- The fixture crates are excluded from the workspace (both via
  `workspace.exclude` and an empty `[workspace]` table in each
  manifest) and built for a non-host target, so
  `cargo build --workspace` never touches them.
- Removed a pre-existing dead `RouteCtx::set_wasm` method in
  `proto/http/routes.rs` that failed clippy `-D warnings` under
  `noxu,wasm` on `main`; it had no callers and `with_wasm` covers
  construction. No `#[allow]` was added.
- Production `dynomited` does not yet construct a `BucketRouter` /
  `RoutingHooks` (routing hooks exist only in tests today), so no
  new `dynomited` config block was added; the reuse seam is
  `WasmKeyfunStore::from_module_store`, letting a deployment wrap a
  `build_wasm_store_from_config` store for keyfuns. A unit test
  pins that one store serves both phases and keyfuns.

## Parity

- `docs/parity.md` D4 updated; new D4a documents the WASM-keyfun
  realisation of Riak's `{modfun, Mod, Fun}` custom keyfun.
