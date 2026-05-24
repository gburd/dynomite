# dyn-riak MapReduce Wasm phase fitting

Date: 2026-05-24
Branch: `stage/dyn-riak-mapreduce-wasm`
Status: READY_FOR_REVIEW

## Scope

Promotes the `Phase::WasmModule { module_id, fn_name, ... }`
variant from a typed-not-implemented placeholder
(`MrError::WasmNotImplemented`) into a real execution path.
Operators can now ship user-defined map and reduce phases as Wasm
modules and reference them from a MapReduce job.

The work lands behind a new `wasm` cargo feature on `dyn-riak`,
off by default, so the Wasm runtime is paid for only by operators
that opt in. The `--features dyn-riak/wasm` test suite grows by
ten cases (eight unit, two integration); the no-feature suite
grows by zero.

## ABI choice: linear memory vs component model

The brief offers two options for the host-module boundary:

1. **Linear-memory ABI**: the module exports its `memory`, plus
   `phase_alloc(len) -> ptr` (a bump allocator) and
   `phase_apply(in_ptr, in_len, out_ptr_ptr, out_len_ptr) -> i32`
   (the entry point). The host calls `phase_alloc` to get an
   input buffer, writes CBOR-encoded inputs into module memory,
   calls `phase_apply`, and reads the output pointer / length
   from the meta slot.
2. **Component model**: a higher-level Wasm interface description
   that lets module authors declare typed records, lists, and
   strings without managing memory by hand. Tooling translates
   the record types into language-specific bindings.

We pick **linear-memory** for v1 for three reasons:

* The component model adds a `wit-bindgen` toolchain dependency
  for module authors and a `wasmtime::component::*` surface for
  the host. Both are heavier than the raw `wasmtime::*` runtime
  surface we already need.
* The component model's stability story is still in motion in
  the wasmtime 26.x line. Pinning to the linear-memory ABI keeps
  the host code on the runtime's mature surface.
* The CBOR `Vec<serde_json::Value>` payload is opaque to the
  module: identity transforms work without any encoding
  knowledge. The 4-line WAT identity fixture in
  `mapreduce/wasm.rs::tests::IDENTITY_WAT` proves the ABI is
  small enough to handwrite.

The component-model story is forwards-compatible: when the
component model stabilises further, we can add a second variant
(say `Phase::WasmComponent`) and a second hook impl side by side
with the linear-memory one. The current API surface does not
preclude this.

## ABI in one paragraph

The module exports:

* `memory` (linear memory).
* `phase_alloc(len: i32) -> i32` (bump allocator).
* `phase_apply(in_ptr: i32, in_len: i32, out_ptr_ptr: i32,
   out_len_ptr: i32) -> i32`.

The host calls `phase_alloc(input_len)` for the input buffer,
`phase_alloc(8)` for an 8-byte meta slot, writes the CBOR-encoded
`Vec<serde_json::Value>` at `in_ptr`, and invokes `phase_apply`.
The module writes its output pointer + length to the two meta
slots and returns `0` on success or non-zero on error. On
success the host decodes CBOR; on error the meta-pointed bytes
are interpreted as a UTF-8 diagnostic string.

## Resource-limit defaults

Configurable per `WasmModuleStore` via `WasmLimits`:

| Limit         | Default       | Enforcement                          |
|---------------|---------------|--------------------------------------|
| `memory_bytes`| 16 MiB        | `wasmtime::ResourceLimiter`          |
| `fuel`        | 10,000,000    | `Config::consume_fuel(true)` + `Store::set_fuel` |
| `timeout_ms`  | 5,000 ms      | epoch interruption + watchdog thread |

Memory growth past the cap surfaces as
`MrError::WasmMemoryLimit`. Both fuel exhaustion and epoch
interruption surface as `MrError::WasmExecutionTimeout`; callers
do not have to discriminate. Generic Wasm traps (`unreachable`,
divide by zero, missing imports, ...) come back as
`MrError::WasmRuntime(message)`.

The watchdog thread polls every `min(20ms, timeout/4 + 1ms)`,
calls `Engine::increment_epoch()` when the deadline elapses, and
joins on guard drop. It is started inside `apply_phase` and
guaranteed to be torn down before the function returns; the
`WatchdogGuard` impl is one allocation and one `std::thread`.

The store reuses one `wasmtime::Engine` across all registered
modules so compilation cost is amortised; the per-call `Store`
state is always fresh so phases never share runtime state.

## Wiring

* `Phase::WasmModule` no longer returns `WasmNotImplemented`
  unconditionally. The executor now takes
  `Option<Arc<dyn WasmHook>>` through the new
  `run_job_with_wasm` entry point. When `None`, the existing
  `WasmNotImplemented` error preserves the no-Wasm path.
  Existing `run_job(job, registry)` continues to work; it
  delegates to `run_job_with_wasm(job, registry, None)`.
* The `WasmHook` trait is always-compiled (no feature gate). The
  `wasm` cargo feature gates only the `crate::mapreduce::wasm`
  module that provides the `WasmModuleStore: WasmHook` impl.
* `WasmModuleStore` is the storage + executor; it pre-compiles
  modules at registration time, holds them keyed by `module_id`,
  and runs them on demand. `WasmPhaseExecutor` is not a separate
  type; the brief uses both names but they share state, so a
  single struct is the cleaner shape.
* The trait method `apply_phase` is synchronous; the executor
  calls it from inside `tokio::task::spawn_blocking` so the
  runtime is never blocked by long-running modules.

## Deferred ConfRiak integration

The brief calls for a `[[mapreduce.wasm_modules]]` array under a
future ConfRiak block. Wiring through ConfRiak is deferred; the
admin / config slice has not landed yet. The deferred work is
mechanical:

```toml
[[mapreduce.wasm_modules]]
id   = "my_phase"
path = "/etc/dynomited/wasm/my_phase.wasm"
```

The seam exists today: `mapreduce::wasm::load_modules_from_config`
takes a `&[(String, P: AsRef<Path>)]` plus `WasmLimits` and
returns a populated `WasmModuleStore`. ConfRiak will translate
its parsed array into that signature in the follow-up. Path
resolution, hot reload, and signature verification are all out
of scope for v1.

## Tests

Per-area count delta:

| Area                                     | New tests |
|------------------------------------------|-----------|
| `mapreduce::wasm` (unit)                 | 8         |
| `mapreduce_wasm.rs` (integration, gated) | 2         |
| Total                                    | 10        |

Workspace nextest counts:

| Profile                         | Count   |
|---------------------------------|---------|
| `cargo nextest run --workspace` | 1006 (unchanged) |
| `... --features dyn-riak/wasm`  | 1016 (+10) |
| `... --all-features`            | 1033     |

The eight unit tests in `mapreduce::wasm::tests` cover:

* `identity_module_round_trips_inputs`: `WasmModuleStore::apply_phase`
  round-trips a heterogeneous `Vec<Value>` through the WAT identity
  module. Establishes the happy path.
* `identity_module_through_executor`: the same identity module driven
  through `run_job_with_wasm`. Asserts the wrapping
  `{bucket, key, value, data}` shape passes through unchanged.
* `unknown_module_id_is_typed_error`: missing module surfaces
  `WasmModuleNotFound`, not a generic runtime error.
* `infinite_loop_is_killed_by_fuel_or_timeout`: a `(loop (br 0))`
  module trapped by a 100k-fuel + 250ms-timeout cap; asserts
  `WasmExecutionTimeout`.
* `over_memory_limit_is_typed_error`: a `memory.grow 4096` (256 MiB)
  module against the default 16-MiB cap; asserts `WasmMemoryLimit`.
* `registering_invalid_bytes_surfaces_compile_error`: bytes that
  do not parse as Wasm or WAT come back as
  `WasmStoreError::Compile`.
* `module_ids_are_sorted`: the diagnostic listing is deterministic.
* `load_modules_from_config_reads_disk`: the ConfRiak seam reads
  WAT bytes from disk and populates the store.

The two integration tests in `tests/mapreduce_wasm.rs` are
`#[cfg(feature = "wasm")]`-gated:

* `wasm_map_then_builtin_reduce_count`: a 2-phase pipeline
  threading a Wasm-identity map phase into the builtin
  `reduce_count`. Asserts the count matches the input length.
* `wasm_phase_without_hook_returns_typed_error`: omitting the hook
  preserves the existing `WasmNotImplemented` behaviour at the
  public API boundary.

## Files touched

New:
* `crates/dyn-riak/src/mapreduce/wasm.rs`
* `crates/dyn-riak/tests/mapreduce_wasm.rs`
* `docs/journal/2026-05-24-mapreduce-wasm.md` (this file)

Modified (additive):
* `Cargo.toml` (workspace `wasmtime = "26"` entry, no default
  features, opt-in `cranelift,runtime,wat`).
* `crates/dyn-riak/Cargo.toml` (new `wasm` feature gating
  `dep:wasmtime` + `dep:ciborium`).
* `crates/dyn-riak/src/mapreduce/mod.rs` (re-exports
  `run_job_with_wasm`, `WasmHook`, and the feature-gated
  `WasmLimits` / `WasmModuleStore` / `WasmStoreError` /
  `load_modules_from_config`).
* `crates/dyn-riak/src/mapreduce/executor.rs`:
  * Added `WasmHook` trait.
  * Added `run_job_with_wasm` entry point; `run_job` now
    delegates to it with `None`.
  * `Phase::WasmModule` arm dispatches through the hook on
    `tokio::task::spawn_blocking`.
  * Five new `MrError` variants: `WasmModuleNotFound`,
    `WasmExecutionTimeout`, `WasmMemoryLimit`, `WasmRuntime`,
    `WasmEncoding`. Existing `WasmNotImplemented` remains for
    the "no hook plugged in" case.

## Verification

```
cargo build --workspace --all-targets --locked          OK
cargo build -p dyn-riak --features wasm                 OK
cargo fmt -p dyn-riak                                   OK
cargo clippy --workspace --all-targets --all-features \
          -- -D warnings                                OK
cargo nextest run --workspace                           OK (1006 + 4 skipped)
cargo nextest run --workspace --features dyn-riak/wasm  OK (1016 + 4 skipped)
cargo nextest run --workspace --all-features            OK (1033 + 4 skipped)
cargo test --doc -p dyn-riak --features wasm            OK
scripts/check_ascii.sh                                  OK
scripts/check_no_todos.sh                               OK
scripts/check_no_port_comments.sh                       OK
```

## Allowances

None. The work compiles cleanly under `clippy::pedantic` without
any `#[allow]` directives.

## Deferred

* **ConfRiak `[[mapreduce.wasm_modules]]` block**. The
  `load_modules_from_config(&[(id, path)], limits)` seam exists;
  ConfRiak wiring lands when the admin/config slice does.
* **Per-module limit overrides**. `WasmLimits` is store-wide
  today. Per-module overrides require either a richer registry
  type or a config-block extension; out of scope for v1.
* **Streaming output**. `WasmModuleStore::apply_phase` returns a
  full `Vec<Value>`. A streaming variant (the module emits one
  output at a time via a host-import callback) is the obvious
  next slice once the streaming MapReduce response sink lands.
* **Component-model phases**. See the ABI-choice section.
* **WASI / host imports**. The current Linker is empty: modules
  cannot read files, open sockets, or call other host functions.
  WASI integration is its own decision (which preview, sandbox
  policy, capability set) and is deferred.
* **Hot reload**. Re-registering a module replaces the previous
  compiled artefact, but there is no admin command surface yet.
  Lands with the admin slice.
* **Signature verification**. Modules are accepted as raw bytes;
  no signing / signature checking. Lands with the admin slice.
