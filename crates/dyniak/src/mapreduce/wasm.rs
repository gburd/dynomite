//! Wasm phase fitting for MapReduce.
//!
//! Operators ship user-defined map / reduce phases as Wasm modules
//! and reference them from a [`Phase::WasmModule`] entry. The
//! executor calls into the [`WasmModuleStore`] (plugged in via
//! [`run_job_with_wasm`]) to run the module on the inbound batch.
//!
//! [`Phase::WasmModule`]: crate::mapreduce::phase::Phase::WasmModule
//! [`run_job_with_wasm`]: crate::mapreduce::executor::run_job_with_wasm
//!
//! # ABI
//!
//! Phase modules speak a small linear-memory ABI. They export:
//!
//! * `memory` -- the module's linear memory.
//! * `phase_alloc(len: i32) -> i32` -- bump allocator; returns a
//!   pointer to a fresh `len`-byte region inside `memory`.
//! * `phase_apply(in_ptr: i32, in_len: i32,
//!                out_ptr_ptr: i32, out_len_ptr: i32) -> i32` --
//!   the phase entry point. The host has already written `in_len`
//!   bytes of CBOR-encoded `Vec<serde_json::Value>` at `in_ptr`.
//!   The module writes its output buffer pointer and length to
//!   `*out_ptr_ptr` and `*out_len_ptr` respectively, and returns
//!   `0` on success or non-zero on failure. On failure the bytes
//!   at `*out_ptr_ptr` / `*out_len_ptr` are interpreted as a
//!   UTF-8 error string instead of CBOR.
//!
//! The host calls `phase_alloc` once for the input buffer and once
//! for an 8-byte slot that holds the output `(ptr, len)` pair.
//!
//! # Resource limits
//!
//! Each invocation runs in a fresh [`wasmtime::Store`] with three
//! caps:
//!
//! * `memory_bytes` -- per-module linear-memory ceiling enforced
//!   through [`wasmtime::ResourceLimiter`]. Default: 16 MiB.
//! * `fuel` -- maximum Wasm instructions executed before the
//!   runtime traps with `OutOfFuel`. Default: 10 million.
//! * `timeout_ms` -- wall-clock deadline enforced through
//!   [`wasmtime::Engine::increment_epoch`] from a watchdog thread.
//!   Default: 5 seconds.
//!
//! Both fuel exhaustion and epoch interruption surface as
//! [`MrError::WasmExecutionTimeout`]. Memory growth past the cap
//! surfaces as [`MrError::WasmMemoryLimit`].

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use wasmtime::{
    Config, Engine, Linker, Memory, Module, ResourceLimiter, Store, StoreLimits,
    StoreLimitsBuilder, TypedFunc,
};

use crate::mapreduce::executor::{MrError, WasmHook};

/// Per-invocation resource limits for a Wasm phase.
///
/// Values are interpreted by [`WasmModuleStore`] when each phase
/// invocation is set up. They are the same for every module
/// registered with the store; per-module overrides are not part of
/// the v1 surface.
#[derive(Clone, Copy, Debug)]
pub struct WasmLimits {
    /// Maximum bytes the module's linear memory may grow to.
    pub memory_bytes: usize,
    /// Maximum number of Wasm instructions the module may execute.
    pub fuel: u64,
    /// Wall-clock deadline in milliseconds.
    pub timeout_ms: u64,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            memory_bytes: 16 * 1024 * 1024,
            fuel: 10_000_000,
            timeout_ms: 5_000,
        }
    }
}

/// Errors produced when registering modules with [`WasmModuleStore`]
/// or loading them from disk.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WasmStoreError {
    /// Engine initialisation failed.
    #[error("wasm engine init failed: {0}")]
    Engine(String),
    /// Module compilation failed (bad bytes, unsupported feature,
    /// schema validation error, ...).
    #[error("wasm module compile failed for {id}: {message}")]
    Compile {
        /// Module identifier the caller registered.
        id: String,
        /// Human-readable failure reason.
        message: String,
    },
    /// Filesystem read failed in [`load_modules_from_config`].
    #[error("wasm module read failed: {0}")]
    Read(String),
}

/// Compiled-module store that doubles as the [`WasmHook`]
/// implementation.
///
/// Operators populate the store at startup with `(module_id,
/// bytes)` pairs; each entry is pre-compiled via
/// [`Module::new`](wasmtime::Module::new). At run time the
/// executor looks up modules by id, instantiates a fresh
/// [`wasmtime::Store`] per invocation, and dispatches to
/// `phase_apply`.
///
/// One [`Engine`] is shared across all registered modules so
/// compilation cost is amortised; per-call [`Store`] state is
/// always fresh so phases never share runtime state.
pub struct WasmModuleStore {
    engine: Engine,
    modules: Mutex<HashMap<String, Module>>,
    limits: WasmLimits,
}

impl WasmModuleStore {
    /// Build a store with the [`WasmLimits::default`] caps.
    pub fn new() -> Result<Self, WasmStoreError> {
        Self::with_limits(WasmLimits::default())
    }

    /// Build a store with custom per-invocation limits.
    pub fn with_limits(limits: WasmLimits) -> Result<Self, WasmStoreError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|e| WasmStoreError::Engine(e.to_string()))?;
        Ok(Self {
            engine,
            modules: Mutex::new(HashMap::new()),
            limits,
        })
    }

    /// Register `bytes` as the module identified by `id`. `bytes`
    /// can be a Wasm binary or, when the `wat` feature of
    /// `wasmtime` is enabled (which it is for this crate), WAT
    /// text.
    ///
    /// Re-registering an `id` replaces the previous module.
    pub fn register(&self, id: impl Into<String>, bytes: &[u8]) -> Result<(), WasmStoreError> {
        let id = id.into();
        let module = Module::new(&self.engine, bytes).map_err(|e| WasmStoreError::Compile {
            id: id.clone(),
            message: e.to_string(),
        })?;
        self.modules
            .lock()
            .expect("WasmModuleStore mutex")
            .insert(id, module);
        Ok(())
    }

    /// Number of registered modules.
    pub fn count(&self) -> usize {
        self.modules.lock().expect("WasmModuleStore mutex").len()
    }

    /// Whether a module with `id` is registered.
    pub fn contains(&self, id: &str) -> bool {
        self.modules
            .lock()
            .expect("WasmModuleStore mutex")
            .contains_key(id)
    }

    /// Sorted list of registered module ids. Useful for diagnostic
    /// output and admin CLIs.
    pub fn module_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .modules
            .lock()
            .expect("WasmModuleStore mutex")
            .keys()
            .cloned()
            .collect();
        ids.sort();
        ids
    }

    /// Per-invocation limits the store applies to every call.
    pub fn limits(&self) -> WasmLimits {
        self.limits
    }
}

impl WasmHook for WasmModuleStore {
    fn apply_phase(
        &self,
        module_id: &str,
        _fn_name: &str,
        inputs: &[Value],
    ) -> Result<Vec<Value>, MrError> {
        // Encode inputs as a CBOR-serialised Vec<Value>.
        let mut input_bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&inputs, &mut input_bytes)
            .map_err(|e| MrError::WasmEncoding(format!("encode: {e}")))?;

        let out_bytes = self
            .run_module_raw(module_id, &input_bytes, "phase_alloc", "phase_apply")
            .map_err(|e| e.into_mr_error(module_id))?;

        let outputs: Vec<Value> = ciborium::de::from_reader(out_bytes.as_slice())
            .map_err(|e| MrError::WasmEncoding(format!("decode: {e}")))?;
        Ok(outputs)
    }
}

/// Failure modes of [`WasmModuleStore::run_module_raw`].
///
/// The raw runner is protocol-agnostic (it moves bytes in and
/// bytes out), so its errors are translated by each caller into
/// their own typed error: MapReduce phases into [`MrError`], the
/// keyfun store into
/// [`crate::datatypes::keyfun::KeyFunError`].
#[derive(Debug)]
pub enum WasmRawError {
    /// The module id was not registered with the store.
    NotFound,
    /// The module trapped after a denied memory growth.
    MemoryLimit,
    /// The module ran out of fuel or hit its wall-clock deadline.
    Timeout,
    /// The module returned a non-zero status code; the bytes are
    /// the module's (UTF-8 lossy) error string.
    Status {
        /// Non-zero status the module returned.
        code: i32,
        /// Module-supplied error text.
        message: String,
    },
    /// Any other host-side or wasm-level runtime failure.
    Runtime(String),
    /// Input length did not fit an `i32`.
    InputTooLarge,
}

impl WasmRawError {
    /// Translate into the MapReduce [`MrError`] taxonomy.
    fn into_mr_error(self, module_id: &str) -> MrError {
        match self {
            Self::NotFound => MrError::WasmModuleNotFound(module_id.to_string()),
            Self::MemoryLimit => MrError::WasmMemoryLimit,
            Self::Timeout => MrError::WasmExecutionTimeout,
            Self::Status { code, message } => {
                MrError::WasmRuntime(format!("wasm phase returned error code {code}: {message}"))
            }
            Self::Runtime(m) => MrError::WasmRuntime(m),
            Self::InputTooLarge => MrError::WasmEncoding("input length exceeds i32".into()),
        }
    }
}

impl WasmModuleStore {
    /// Run the module registered as `module_id` over `input`,
    /// returning its raw output bytes.
    ///
    /// The module is driven through the linear-memory ABI shared
    /// by MapReduce phases and custom keyfuns: an allocator
    /// export named `alloc_name` (`fn(i32) -> i32`) and an entry
    /// point named `apply_name`
    /// (`fn(in_ptr, in_len, out_ptr_ptr, out_len_ptr) -> i32`).
    /// The host writes `input` at the allocated pointer, calls
    /// the entry point, then reads back the `(out_ptr, out_len)`
    /// the module wrote into the 8-byte meta slot. A zero return
    /// code means the output bytes are the result; a non-zero
    /// code means they are an error string.
    ///
    /// Each call runs in a fresh [`Store`] bounded by the store's
    /// [`WasmLimits`] (memory cap + fuel + wall-clock deadline),
    /// so a buggy or hostile module cannot hang or OOM the host.
    ///
    /// # Errors
    ///
    /// Returns a [`WasmRawError`] describing the failure; callers
    /// map it into their own typed error.
    pub fn run_module_raw(
        &self,
        module_id: &str,
        input: &[u8],
        alloc_name: &str,
        apply_name: &str,
    ) -> Result<Vec<u8>, WasmRawError> {
        let module = {
            let modules = self.modules.lock().expect("WasmModuleStore mutex");
            modules
                .get(module_id)
                .cloned()
                .ok_or(WasmRawError::NotFound)?
        };
        let input_len = i32::try_from(input.len()).map_err(|_| WasmRawError::InputTooLarge)?;

        // Set up the per-call store with memory + fuel + epoch.
        let state = WasmState {
            limits: StoreLimitsBuilder::new()
                .memory_size(self.limits.memory_bytes)
                .build(),
            memory_bytes_cap: self.limits.memory_bytes,
            memory_limit_hit: false,
        };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| s as &mut dyn ResourceLimiter);
        store
            .set_fuel(self.limits.fuel)
            .map_err(|e| WasmRawError::Runtime(format!("set_fuel: {e}")))?;
        // Trap as soon as the engine epoch advances by one tick;
        // the watchdog increments the epoch when the wall-clock
        // deadline elapses.
        store.set_epoch_deadline(1);

        let watchdog = WatchdogGuard::start(
            self.engine.clone(),
            Duration::from_millis(self.limits.timeout_ms),
        );

        let result = self.run_raw_inner(
            &mut store, &module, input, input_len, alloc_name, apply_name,
        );
        drop(watchdog);
        result
    }

    fn run_raw_inner(
        &self,
        store: &mut Store<WasmState>,
        module: &Module,
        input_bytes: &[u8],
        input_len: i32,
        alloc_name: &str,
        apply_name: &str,
    ) -> Result<Vec<u8>, WasmRawError> {
        let linker: Linker<WasmState> = Linker::new(&self.engine);
        let instance = linker
            .instantiate(&mut *store, module)
            .map_err(|e| classify_raw(store, &e))?;

        let memory: Memory = instance
            .get_memory(&mut *store, "memory")
            .ok_or_else(|| WasmRawError::Runtime("module did not export 'memory'".into()))?;
        let alloc: TypedFunc<i32, i32> = instance
            .get_typed_func(&mut *store, alloc_name)
            .map_err(|e| WasmRawError::Runtime(format!("missing {alloc_name}: {e}")))?;
        let apply: TypedFunc<(i32, i32, i32, i32), i32> = instance
            .get_typed_func(&mut *store, apply_name)
            .map_err(|e| WasmRawError::Runtime(format!("missing {apply_name}: {e}")))?;

        // Allocate input buffer.
        let in_ptr = alloc
            .call(&mut *store, input_len)
            .map_err(|e| classify_raw(store, &e))?;
        // Allocate 8 bytes for the output meta (out_ptr + out_len).
        let out_meta_ptr = alloc
            .call(&mut *store, 8)
            .map_err(|e| classify_raw(store, &e))?;

        // Write input bytes into module memory.
        let in_off = usize_from_i32(in_ptr)?;
        memory
            .write(&mut *store, in_off, input_bytes)
            .map_err(|e| WasmRawError::Runtime(format!("memory.write input: {e}")))?;

        // Invoke the entry point.
        let out_meta_len_ptr = out_meta_ptr
            .checked_add(4)
            .ok_or_else(|| WasmRawError::Runtime("output meta pointer overflow".into()))?;
        let result_code = apply
            .call(
                &mut *store,
                (in_ptr, input_len, out_meta_ptr, out_meta_len_ptr),
            )
            .map_err(|e| classify_raw(store, &e))?;

        // Read output meta.
        let mut meta = [0u8; 8];
        let out_meta_off = usize_from_i32(out_meta_ptr)?;
        memory
            .read(&*store, out_meta_off, &mut meta)
            .map_err(|e| WasmRawError::Runtime(format!("memory.read meta: {e}")))?;
        let out_ptr = i32::from_le_bytes([meta[0], meta[1], meta[2], meta[3]]);
        let out_len = i32::from_le_bytes([meta[4], meta[5], meta[6], meta[7]]);

        let out_len_us = usize_from_i32(out_len)?;
        let out_off = usize_from_i32(out_ptr)?;
        let mut out_bytes = vec![0u8; out_len_us];
        if out_len_us > 0 {
            memory
                .read(&*store, out_off, &mut out_bytes)
                .map_err(|e| WasmRawError::Runtime(format!("memory.read output: {e}")))?;
        }

        if result_code != 0 {
            let msg = String::from_utf8_lossy(&out_bytes).into_owned();
            return Err(WasmRawError::Status {
                code: result_code,
                message: msg,
            });
        }

        Ok(out_bytes)
    }
}

/// Per-store state attached to each [`Store`]: holds the memory
/// limiter and a flag the limiter sets when it denies a growth
/// request. The flag lets [`classify`] turn a downstream
/// `unreachable` trap into a typed [`MrError::WasmMemoryLimit`].
struct WasmState {
    limits: StoreLimits,
    memory_bytes_cap: usize,
    memory_limit_hit: bool,
}

impl ResourceLimiter for WasmState {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > self.memory_bytes_cap {
            self.memory_limit_hit = true;
            return Ok(false);
        }
        self.limits.memory_growing(current, desired, maximum)
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        self.limits.table_growing(current, desired, maximum)
    }
}

fn usize_from_i32(v: i32) -> Result<usize, WasmRawError> {
    usize::try_from(v).map_err(|_| WasmRawError::Runtime(format!("invalid pointer/length: {v}")))
}

/// Translate a [`wasmtime::Error`] into a typed [`WasmRawError`].
///
/// The runtime returns `Trap` values for fuel exhaustion, epoch
/// interruption, and other wasm-level traps. The [`WasmState`]
/// flag also tells us whether the failure was preceded by a
/// denied memory growth, which lets us promote a generic
/// `unreachable` trap to [`WasmRawError::MemoryLimit`].
fn classify_raw(store: &Store<WasmState>, e: &wasmtime::Error) -> WasmRawError {
    if store.data().memory_limit_hit {
        return WasmRawError::MemoryLimit;
    }
    if let Some(trap) = e.downcast_ref::<wasmtime::Trap>() {
        if matches!(trap, wasmtime::Trap::OutOfFuel | wasmtime::Trap::Interrupt) {
            return WasmRawError::Timeout;
        }
    }
    WasmRawError::Runtime(e.to_string())
}

/// Wall-clock watchdog. The executor's per-call [`Store`] is set
/// up with `set_epoch_deadline(1)`, meaning the runtime traps the
/// next time the engine's epoch counter advances. The watchdog
/// thread bumps the engine's epoch when the configured timeout
/// elapses.
///
/// Holding the guard keeps the watchdog alive; dropping it signals
/// the watchdog to stop and joins the thread. The watchdog also
/// stops on its own if the timeout fires.
struct WatchdogGuard {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl WatchdogGuard {
    fn start(engine: Engine, timeout: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_w = Arc::clone(&stop);
        let tick = Duration::from_millis(20).min(timeout / 4 + Duration::from_millis(1));
        let handle = std::thread::spawn(move || {
            let start = Instant::now();
            while !stop_w.load(Ordering::Relaxed) {
                if start.elapsed() >= timeout {
                    engine.increment_epoch();
                    return;
                }
                std::thread::sleep(tick);
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Build a fully populated [`WasmModuleStore`] from a list of
/// `(module_id, path)` pairs.
///
/// Each path is read from disk and registered via
/// [`WasmModuleStore::register`]. Both Wasm binaries (`.wasm`) and
/// WAT text (`.wat`) are accepted; `wasmtime::Module::new` decides
/// between them.
///
/// This is the seam for a future ConfRiak `[[mapreduce.wasm_modules]]`
/// block. The block is sketched out as
///
/// ```toml
/// [[mapreduce.wasm_modules]]
/// id   = "my_module"
/// path = "/etc/dynomited/wasm/my_module.wasm"
/// ```
///
/// `ConfRiak::wasm_modules` is wired to this function by
/// `dynomited`'s Riak server builder: each configured
/// `[[mapreduce.wasm_modules]]` entry is loaded into the returned
/// store, which is then carried into the HTTP `/mapred` handler so
/// a [`crate::mapreduce::Phase::WasmModule`] job reaches the
/// configured modules. The executor can also take a pre-built
/// [`WasmModuleStore`] programmatically.
pub fn load_modules_from_config<P: AsRef<Path>>(
    modules: &[(String, P)],
    limits: WasmLimits,
) -> Result<WasmModuleStore, WasmStoreError> {
    let store = WasmModuleStore::with_limits(limits)?;
    for (id, path) in modules {
        let bytes = std::fs::read(path.as_ref())
            .map_err(|e| WasmStoreError::Read(format!("{}: {e}", path.as_ref().display())))?;
        store.register(id.clone(), &bytes)?;
    }
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapreduce::executor::run_job_with_wasm;
    use crate::mapreduce::job::{Inputs, KeyDatum, MapReduceJob};
    use crate::mapreduce::phase::Phase;
    use crate::mapreduce::registry::PhaseRegistry;
    use crate::mapreduce::{builtins, WasmHook};
    use std::sync::Arc;

    /// Identity module: `phase_apply` copies the input bytes to a
    /// fresh allocation and writes (out_buf, in_len) to the meta
    /// slot. CBOR is opaque to the module.
    const IDENTITY_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $heap_top (mut i32) (i32.const 1024))
          (func $alloc_inner (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap_top))
            (global.set $heap_top
              (i32.add (global.get $heap_top) (local.get $len)))
            (local.get $ptr))
          (func (export "phase_alloc") (param $len i32) (result i32)
            (call $alloc_inner (local.get $len)))
          (func (export "phase_apply")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (local $out_buf i32)
            (local.set $out_buf (call $alloc_inner (local.get $in_len)))
            (memory.copy
              (local.get $out_buf)
              (local.get $in_ptr)
              (local.get $in_len))
            (i32.store (local.get $out_ptr_ptr) (local.get $out_buf))
            (i32.store (local.get $out_len_ptr) (local.get $in_len))
            (i32.const 0)))
    "#;

    /// Infinite-loop module: `phase_apply` enters a tight loop and
    /// never returns. Fuel runs out (or the watchdog fires) and
    /// the trap surfaces as [`MrError::WasmExecutionTimeout`].
    const INFINITE_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $heap_top (mut i32) (i32.const 1024))
          (func (export "phase_alloc") (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap_top))
            (global.set $heap_top
              (i32.add (global.get $heap_top) (local.get $len)))
            (local.get $ptr))
          (func (export "phase_apply")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (loop $loop (br $loop))
            (i32.const 0)))
    "#;

    /// Memory-limit module: `phase_apply` asks for a 256-MiB
    /// growth, which exceeds the default 16-MiB cap. The limiter
    /// denies the growth, the module's `unreachable` runs, and
    /// the trap is classified as [`MrError::WasmMemoryLimit`].
    const MEMORY_HOG_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $heap_top (mut i32) (i32.const 1024))
          (func (export "phase_alloc") (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap_top))
            (global.set $heap_top
              (i32.add (global.get $heap_top) (local.get $len)))
            (local.get $ptr))
          (func (export "phase_apply")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (if (i32.eq (memory.grow (i32.const 4096)) (i32.const -1))
              (then unreachable))
            (i32.const 0)))
    "#;

    fn registry() -> Arc<PhaseRegistry> {
        Arc::new(builtins::default_registry())
    }

    #[tokio::test]
    async fn identity_module_round_trips_inputs() {
        let store = WasmModuleStore::new().expect("store");
        store
            .register("identity", IDENTITY_WAT.as_bytes())
            .expect("register");
        assert_eq!(store.count(), 1);
        assert!(store.contains("identity"));

        let inputs = vec![
            serde_json::json!(1),
            serde_json::json!("hello"),
            serde_json::json!({"k": [1, 2, 3]}),
        ];
        let outs = store
            .apply_phase("identity", "ignored", &inputs)
            .expect("apply ok");
        assert_eq!(outs, inputs);
    }

    #[tokio::test]
    async fn identity_module_through_executor() {
        let store: WasmModuleStore = WasmModuleStore::new().expect("store");
        store
            .register("identity", IDENTITY_WAT.as_bytes())
            .expect("register");
        let hook: Arc<dyn WasmHook> = Arc::new(store);

        let job = MapReduceJob {
            inputs: Inputs::KeyData(vec![
                KeyDatum::with_value("b", "k1", serde_json::json!(10)),
                KeyDatum::with_value("b", "k2", serde_json::json!(20)),
            ]),
            phases: vec![Phase::WasmModule {
                module_id: "identity".into(),
                fn_name: "apply".into(),
                arg: None,
                keep: true,
            }],
            timeout_ms: None,
        };
        let out = run_job_with_wasm(job, registry(), Some(hook))
            .await
            .expect("ok");
        assert_eq!(out.len(), 2);
        // The identity passes the wrapping {bucket,key,value,data}
        // structure straight through.
        assert_eq!(out[0].value["bucket"], "b");
        assert_eq!(out[0].value["key"], "k1");
        assert_eq!(out[0].value["value"], serde_json::json!(10));
    }

    #[tokio::test]
    async fn unknown_module_id_is_typed_error() {
        let store = WasmModuleStore::new().expect("store");
        let err = store
            .apply_phase("does_not_exist", "f", &[serde_json::json!(1)])
            .expect_err("error");
        assert!(matches!(err, MrError::WasmModuleNotFound(ref s) if s == "does_not_exist"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn infinite_loop_is_killed_by_fuel_or_timeout() {
        // Aggressive caps so the test finishes in well under a
        // second: 100k fuel exhausts in microseconds; 250 ms
        // wall-clock backs it up.
        let limits = WasmLimits {
            memory_bytes: 16 * 1024 * 1024,
            fuel: 100_000,
            timeout_ms: 250,
        };
        let store = WasmModuleStore::with_limits(limits).expect("store");
        store
            .register("loop", INFINITE_WAT.as_bytes())
            .expect("register");
        let err = tokio::task::spawn_blocking(move || {
            store.apply_phase("loop", "f", &[serde_json::json!(1)])
        })
        .await
        .expect("join")
        .expect_err("trap");
        assert!(matches!(err, MrError::WasmExecutionTimeout));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn over_memory_limit_is_typed_error() {
        // Default 16 MiB cap; module asks for 4096 pages = 256
        // MiB, which exceeds it.
        let store = WasmModuleStore::new().expect("store");
        store
            .register("hog", MEMORY_HOG_WAT.as_bytes())
            .expect("register");
        let err = tokio::task::spawn_blocking(move || {
            store.apply_phase("hog", "f", &[serde_json::json!(1)])
        })
        .await
        .expect("join")
        .expect_err("trap");
        assert!(matches!(err, MrError::WasmMemoryLimit));
    }

    #[tokio::test]
    async fn registering_invalid_bytes_surfaces_compile_error() {
        let store = WasmModuleStore::new().expect("store");
        let err = store
            .register("bad", b"not a wasm module")
            .expect_err("compile error");
        assert!(matches!(err, WasmStoreError::Compile { .. }));
    }

    #[tokio::test]
    async fn module_ids_are_sorted() {
        let store = WasmModuleStore::new().expect("store");
        store
            .register("zebra", IDENTITY_WAT.as_bytes())
            .expect("register");
        store
            .register("apple", IDENTITY_WAT.as_bytes())
            .expect("register");
        let ids = store.module_ids();
        assert_eq!(ids, vec!["apple".to_string(), "zebra".to_string()]);
    }

    #[tokio::test]
    async fn load_modules_from_config_reads_disk() {
        // Compile WAT text to a Wasm binary on disk and load it
        // through the config path.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let path = tmp.path().join("identity.wat");
        std::fs::write(&path, IDENTITY_WAT).expect("write");
        let store =
            load_modules_from_config(&[("identity".to_string(), path)], WasmLimits::default())
                .expect("load");
        assert!(store.contains("identity"));
        assert_eq!(store.count(), 1);
    }
}
