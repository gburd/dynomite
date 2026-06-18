//! WebAssembly-backed custom keyfun routing.
//!
//! This module is the dyniak realisation of Riak's user-defined
//! `chash_keyfun` ([`crate::datatypes::keyfun::KeyFun::Custom`]).
//! Riak lets an operator point `chash_keyfun` at an Erlang
//! `{modfun, Mod, Fun}` that selects which bytes of a
//! `(bucket, key)` request are fed to the consistent-hash ring.
//! dyniak realises the same intent with an operator-supplied
//! WebAssembly module: the module receives the framed
//! `(bucket, key)` and returns the route bytes verbatim, which the
//! router then hands to [`dynomite::hashkit::hash64`] exactly as it
//! does for the built-in `Std` / `BucketOnly` keyfuns.
//!
//! # ABI
//!
//! A keyfun module speaks the same linear-memory ABI as a
//! MapReduce phase, under keyfun-specific export names so a single
//! module can never be mistaken for the wrong role:
//!
//! * `memory` -- the module's linear memory.
//! * `keyfun_alloc(len: i32) -> i32` -- bump allocator; returns a
//!   pointer to a fresh `len`-byte region inside `memory`.
//! * `keyfun_route(in_ptr: i32, in_len: i32,
//!                 out_ptr_ptr: i32, out_len_ptr: i32) -> i32` --
//!   the entry point. The host has written `in_len` framed input
//!   bytes at `in_ptr` (see the framing below). The module writes
//!   its output buffer pointer / length to `*out_ptr_ptr` /
//!   `*out_len_ptr` and returns `0` on success. A non-zero return
//!   means the output bytes are a UTF-8 error string instead of
//!   route bytes.
//!
//! ## Input framing
//!
//! The input is the length-prefixed pair
//!
//! ```text
//! bucket_len: u32-le | bucket bytes | key_len: u32-le | key bytes
//! ```
//!
//! so the module can split bucket from key without a separator
//! collision. The output bytes ARE the route bytes; no framing is
//! applied to the output.
//!
//! # Resource limits
//!
//! Every call runs under the wrapped store's [`WasmLimits`]
//! (memory cap + fuel + wall-clock deadline), so a buggy or
//! hostile keyfun cannot hang or OOM the routing path. A trap,
//! fuel-exhaustion, deadline, or oversize-memory failure surfaces
//! as a typed [`KeyFunError`]; routing never panics or hangs on a
//! bad module.

use std::sync::Arc;

use crate::datatypes::keyfun::KeyFunError;
use crate::mapreduce::wasm::{WasmLimits, WasmModuleStore, WasmRawError, WasmStoreError};

/// Allocator export name a keyfun module must provide.
pub const KEYFUN_ALLOC: &str = "keyfun_alloc";
/// Entry-point export name a keyfun module must provide.
pub const KEYFUN_ROUTE: &str = "keyfun_route";

/// Store of operator-supplied custom-keyfun WASM modules.
///
/// Cheap to clone via [`Arc`]; the router holds one of these and
/// consults it for every [`crate::datatypes::keyfun::KeyFun::Custom`]
/// routing decision. Internally it wraps the same
/// [`WasmModuleStore`] the MapReduce executor uses, so module
/// registration, compilation caching, and resource limiting are
/// shared machinery.
#[derive(Clone)]
pub struct WasmKeyfunStore {
    inner: Arc<WasmModuleStore>,
}

impl std::fmt::Debug for WasmKeyfunStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmKeyfunStore")
            .field("modules", &self.inner.module_ids())
            .finish()
    }
}

impl WasmKeyfunStore {
    /// Build an empty store with [`WasmLimits::default`] caps.
    ///
    /// # Errors
    ///
    /// [`WasmStoreError::Engine`] if the wasm engine fails to
    /// initialise.
    pub fn new() -> Result<Self, WasmStoreError> {
        Ok(Self {
            inner: Arc::new(WasmModuleStore::new()?),
        })
    }

    /// Build an empty store with custom per-invocation limits.
    ///
    /// # Errors
    ///
    /// [`WasmStoreError::Engine`] if the wasm engine fails to
    /// initialise.
    pub fn with_limits(limits: WasmLimits) -> Result<Self, WasmStoreError> {
        Ok(Self {
            inner: Arc::new(WasmModuleStore::with_limits(limits)?),
        })
    }

    /// Wrap an existing [`WasmModuleStore`].
    ///
    /// Lets a deployment share one module store between MapReduce
    /// phases and keyfuns: the same registered modules are
    /// available to both, distinguished only by which exports they
    /// provide (`phase_*` vs `keyfun_*`).
    #[must_use]
    pub fn from_module_store(inner: Arc<WasmModuleStore>) -> Self {
        Self { inner }
    }

    /// Borrow the wrapped module store (e.g. to register modules
    /// that are also used as MapReduce phases).
    #[must_use]
    pub fn module_store(&self) -> &Arc<WasmModuleStore> {
        &self.inner
    }

    /// Register `bytes` (a `.wasm` binary or `.wat` text) as the
    /// keyfun module identified by `id`.
    ///
    /// # Errors
    ///
    /// [`WasmStoreError::Compile`] if the bytes do not compile.
    pub fn register(&self, id: impl Into<String>, bytes: &[u8]) -> Result<(), WasmStoreError> {
        self.inner.register(id, bytes)
    }

    /// Whether a module with `id` is registered.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.inner.contains(id)
    }

    /// Number of registered keyfun modules.
    #[must_use]
    pub fn count(&self) -> usize {
        self.inner.count()
    }

    /// Sorted list of registered keyfun module ids.
    #[must_use]
    pub fn module_ids(&self) -> Vec<String> {
        self.inner.module_ids()
    }

    /// Frame `(bucket, key)` into the keyfun input layout:
    /// `bucket_len(u32-le) ++ bucket ++ key_len(u32-le) ++ key`.
    #[must_use]
    pub fn frame_input(bucket: &[u8], key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + bucket.len() + key.len());
        // Lengths are clamped at u32; routing keys never approach
        // 4 GiB, so a saturating cast cannot lose information for
        // any realistic input and keeps the framing total.
        let blen = u32::try_from(bucket.len()).unwrap_or(u32::MAX);
        let klen = u32::try_from(key.len()).unwrap_or(u32::MAX);
        buf.extend_from_slice(&blen.to_le_bytes());
        buf.extend_from_slice(bucket);
        buf.extend_from_slice(&klen.to_le_bytes());
        buf.extend_from_slice(key);
        buf
    }

    /// Run the keyfun module `module_id` over `(bucket, key)` and
    /// return the route bytes the cluster hash should consume.
    ///
    /// # Errors
    ///
    /// * [`KeyFunError::ModuleNotFound`] if `module_id` is empty or
    ///   not registered.
    /// * [`KeyFunError::Runtime`] if the module traps, runs out of
    ///   fuel, hits its deadline, or returns a non-zero status.
    /// * [`KeyFunError::MemoryLimit`] if the module exceeds its
    ///   memory cap.
    pub fn route_bytes(
        &self,
        module_id: &str,
        bucket: &[u8],
        key: &[u8],
    ) -> Result<Vec<u8>, KeyFunError> {
        if module_id.is_empty() {
            return Err(KeyFunError::ModuleNotFound(module_id.to_string()));
        }
        let input = Self::frame_input(bucket, key);
        self.inner
            .run_module_raw(module_id, &input, KEYFUN_ALLOC, KEYFUN_ROUTE)
            .map_err(|e| map_raw_error(module_id, e))
    }
}

/// Translate a [`WasmRawError`] into the keyfun error taxonomy.
fn map_raw_error(module_id: &str, e: WasmRawError) -> KeyFunError {
    match e {
        WasmRawError::NotFound => KeyFunError::ModuleNotFound(module_id.to_string()),
        WasmRawError::MemoryLimit => KeyFunError::MemoryLimit(module_id.to_string()),
        WasmRawError::Timeout => KeyFunError::Runtime {
            module: module_id.to_string(),
            message: "execution timed out (fuel or wall-clock deadline)".into(),
        },
        WasmRawError::Status { code, message } => KeyFunError::Runtime {
            module: module_id.to_string(),
            message: format!("module returned status {code}: {message}"),
        },
        WasmRawError::Runtime(m) => KeyFunError::Runtime {
            module: module_id.to_string(),
            message: m,
        },
        WasmRawError::InputTooLarge => KeyFunError::Runtime {
            module: module_id.to_string(),
            message: "framed input length exceeds i32".into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WAT keyfun that reverses the key bytes and ignores the
    /// bucket. Exercises the keyfun ABI and the input framing
    /// without paying the real-Rust-to-wasm compile cost.
    const REVERSE_KEY_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $heap_top (mut i32) (i32.const 1024))
          (func $alloc (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap_top))
            (global.set $heap_top
              (i32.add (global.get $heap_top) (local.get $len)))
            (local.get $ptr))
          (func (export "keyfun_alloc") (param $len i32) (result i32)
            (call $alloc (local.get $len)))
          (func (export "keyfun_route")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (local $blen i32)
            (local $klen i32)
            (local $kstart i32)
            (local $out i32)
            (local $i i32)
            ;; blen = load u32 at in_ptr
            (local.set $blen (i32.load (local.get $in_ptr)))
            ;; klen = load u32 at in_ptr + 4 + blen
            (local.set $klen
              (i32.load
                (i32.add (local.get $in_ptr)
                  (i32.add (i32.const 4) (local.get $blen)))))
            ;; kstart = in_ptr + 4 + blen + 4
            (local.set $kstart
              (i32.add (local.get $in_ptr)
                (i32.add (i32.const 8) (local.get $blen))))
            ;; out = alloc(klen)
            (local.set $out (call $alloc (local.get $klen)))
            ;; reverse copy
            (local.set $i (i32.const 0))
            (block $done
              (loop $loop
                (br_if $done (i32.ge_s (local.get $i) (local.get $klen)))
                (i32.store8
                  (i32.add (local.get $out) (local.get $i))
                  (i32.load8_u
                    (i32.add (local.get $kstart)
                      (i32.sub (i32.sub (local.get $klen) (local.get $i))
                        (i32.const 1)))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $loop)))
            (i32.store (local.get $out_ptr_ptr) (local.get $out))
            (i32.store (local.get $out_len_ptr) (local.get $klen))
            (i32.const 0)))
    "#;

    /// WAT keyfun that always traps.
    const TRAP_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "keyfun_alloc") (param $len i32) (result i32)
            (i32.const 1024))
          (func (export "keyfun_route")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            unreachable))
    "#;

    /// WAT keyfun that asks for far more memory than the cap.
    const MEMORY_HOG_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (func (export "keyfun_alloc") (param $len i32) (result i32)
            (i32.const 1024))
          (func (export "keyfun_route")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (if (i32.eq (memory.grow (i32.const 4096)) (i32.const -1))
              (then unreachable))
            (i32.const 0)))
    "#;

    /// WAT keyfun that returns a non-zero status with an error
    /// string in the output buffer.
    const STATUS_ERR_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 2048) "boom")
          (func (export "keyfun_alloc") (param $len i32) (result i32)
            (i32.const 1024))
          (func (export "keyfun_route")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (i32.store (local.get $out_ptr_ptr) (i32.const 2048))
            (i32.store (local.get $out_len_ptr) (i32.const 4))
            (i32.const 7)))
    "#;

    #[test]
    fn reverse_key_wat_routes() {
        let store = WasmKeyfunStore::new().expect("store");
        store
            .register("reverse", REVERSE_KEY_WAT.as_bytes())
            .expect("register");
        let out = store
            .route_bytes("reverse", b"users", b"alice")
            .expect("ok");
        assert_eq!(out, b"ecila");
        // Bucket is ignored: same key in a different bucket routes
        // identically.
        let out2 = store
            .route_bytes("reverse", b"orders", b"alice")
            .expect("ok");
        assert_eq!(out2, b"ecila");
    }

    #[test]
    fn empty_module_id_is_not_found() {
        let store = WasmKeyfunStore::new().expect("store");
        let err = store.route_bytes("", b"b", b"k").expect_err("err");
        assert!(matches!(err, KeyFunError::ModuleNotFound(_)));
    }

    #[test]
    fn unregistered_module_is_not_found() {
        let store = WasmKeyfunStore::new().expect("store");
        let err = store.route_bytes("nope", b"b", b"k").expect_err("err");
        assert!(matches!(err, KeyFunError::ModuleNotFound(ref s) if s == "nope"));
    }

    #[test]
    fn trapping_module_is_runtime_error() {
        let store = WasmKeyfunStore::new().expect("store");
        store
            .register("trap", TRAP_WAT.as_bytes())
            .expect("register");
        let err = store.route_bytes("trap", b"b", b"k").expect_err("err");
        assert!(matches!(err, KeyFunError::Runtime { .. }));
    }

    #[test]
    fn oversize_memory_is_memory_limit_error() {
        let store = WasmKeyfunStore::new().expect("store");
        store
            .register("hog", MEMORY_HOG_WAT.as_bytes())
            .expect("register");
        let err = store.route_bytes("hog", b"b", b"k").expect_err("err");
        assert!(matches!(err, KeyFunError::MemoryLimit(_)));
    }

    #[test]
    fn nonzero_status_is_runtime_error_with_message() {
        let store = WasmKeyfunStore::new().expect("store");
        store
            .register("status", STATUS_ERR_WAT.as_bytes())
            .expect("register");
        let err = store.route_bytes("status", b"b", b"k").expect_err("err");
        match err {
            KeyFunError::Runtime { ref message, .. } => {
                assert!(message.contains("status 7"), "got {message}");
                assert!(message.contains("boom"), "got {message}");
            }
            other => panic!("expected Runtime, got {other:?}"),
        }
    }

    #[test]
    fn frame_input_layout_is_length_prefixed() {
        let framed = WasmKeyfunStore::frame_input(b"bk", b"key");
        assert_eq!(
            framed,
            [
                &2u32.to_le_bytes()[..],
                b"bk",
                &3u32.to_le_bytes()[..],
                b"key"
            ]
            .concat()
        );
    }

    #[test]
    fn shares_one_module_store_with_mapreduce() {
        // A deployment can register modules once into a single
        // WasmModuleStore and use it for both MapReduce phases
        // (phase_* exports) and keyfuns (keyfun_* exports). The
        // keyfun store wraps the same module store; registering
        // through either surface lands in the shared map.
        let shared = std::sync::Arc::new(WasmModuleStore::new().expect("module store"));
        let keyfun = WasmKeyfunStore::from_module_store(shared.clone());
        keyfun
            .register("reverse", REVERSE_KEY_WAT.as_bytes())
            .expect("register");
        // The module is visible through the shared MapReduce store.
        assert!(shared.contains("reverse"));
        // And routes through the keyfun surface.
        let out = keyfun.route_bytes("reverse", b"b", b"abc").expect("route");
        assert_eq!(out, b"cba");
        assert_eq!(keyfun.module_store().count(), 1);
    }
}
