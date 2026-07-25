//! Precommit hooks: a WASM veto/transform run before an object write
//! commits.
//!
//! Riak lets a bucket attach precommit hooks that inspect a write
//! before it is stored: the hook may accept it unchanged, transform the
//! value, or reject the write with a reason (a veto). Dyniak realises
//! the hook as a WASM module (the same linear-memory ABI as custom
//! keyfuns and MapReduce phases), named per bucket via the
//! `precommit_module` property.
//!
//! The hook module exports `hook_alloc` (`fn(i32) -> i32`) and
//! `precommit` (`fn(in_ptr, in_len, out_ptr_ptr, out_len_ptr) -> i32`).
//! The host frames the object value as input; a zero return code means
//! the output bytes are the (possibly transformed) value to store, and
//! a non-zero code means the output bytes are a rejection reason and
//! the write is vetoed.
//!
//! Postcommit hooks (fire-and-forget notification after a successful
//! write) are tracked follow-up: they need an async side-effect plane
//! and do not affect the write's success, unlike the precommit veto.

#![cfg(feature = "wasm")]

use std::sync::Arc;

use crate::mapreduce::wasm::{WasmModuleStore, WasmRawError};

/// Allocator export a precommit hook module must provide.
pub const HOOK_ALLOC: &str = "hook_alloc";
/// Entry-point export a precommit hook module must provide.
pub const HOOK_PRECOMMIT: &str = "precommit";

/// Outcome of running a precommit hook over an object value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrecommitOutcome {
    /// The write is accepted; store these bytes (the hook may have
    /// transformed the value, or returned it unchanged).
    Accept(Vec<u8>),
    /// The write is vetoed with this reason string.
    Reject(String),
}

/// Error running a precommit hook.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PrecommitError {
    /// The named hook module is not registered.
    #[error("precommit hook module not found: {0}")]
    ModuleNotFound(String),
    /// The module trapped, ran out of fuel, hit its deadline, or
    /// exceeded its memory cap.
    #[error("precommit hook {module} failed: {message}")]
    Runtime {
        /// Hook module id.
        module: String,
        /// Failure detail.
        message: String,
    },
}

/// Runs precommit hooks against a shared WASM module store.
#[derive(Clone)]
pub struct PrecommitHooks {
    store: Arc<WasmModuleStore>,
}

impl std::fmt::Debug for PrecommitHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrecommitHooks").finish_non_exhaustive()
    }
}

impl PrecommitHooks {
    /// Wrap a WASM module store.
    #[must_use]
    pub fn new(store: Arc<WasmModuleStore>) -> Self {
        Self { store }
    }

    /// Register a hook module under `id`.
    ///
    /// # Errors
    /// Surfaces a compile / validation error from the WASM store.
    pub fn register(
        &self,
        id: impl Into<String>,
        bytes: &[u8],
    ) -> Result<(), crate::datatypes::keyfun::KeyFunError> {
        self.store.register(id.into(), bytes).map_err(|e| {
            crate::datatypes::keyfun::KeyFunError::Runtime {
                module: "precommit".into(),
                message: e.to_string(),
            }
        })
    }

    /// Whether a module is registered under `id`.
    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.store.contains(id)
    }

    /// Run the precommit hook `module_id` over `value`.
    ///
    /// # Errors
    /// [`PrecommitError::ModuleNotFound`] if the module is not
    /// registered, [`PrecommitError::Runtime`] if it traps / times out
    /// / exceeds its memory cap.
    pub fn run(&self, module_id: &str, value: &[u8]) -> Result<PrecommitOutcome, PrecommitError> {
        if module_id.is_empty() || !self.store.contains(module_id) {
            return Err(PrecommitError::ModuleNotFound(module_id.to_string()));
        }
        match self
            .store
            .run_module_raw(module_id, value, HOOK_ALLOC, HOOK_PRECOMMIT)
        {
            // Zero status: the output is the (possibly transformed)
            // value to store.
            Ok(out) => Ok(PrecommitOutcome::Accept(out)),
            // A non-zero status surfaces as WasmRawError::Status whose
            // message is the module's rejection reason -- the veto.
            Err(WasmRawError::Status { message, .. }) => Ok(PrecommitOutcome::Reject(message)),
            Err(WasmRawError::NotFound) => {
                Err(PrecommitError::ModuleNotFound(module_id.to_string()))
            }
            Err(e) => Err(PrecommitError::Runtime {
                module: module_id.to_string(),
                message: format!("{e:?}"),
            }),
        }
    }
}

impl crate::router::PrecommitRunner for PrecommitHooks {
    fn run(&self, module_id: &str, value: &[u8]) -> Result<Vec<u8>, crate::router::PrecommitVeto> {
        match PrecommitHooks::run(self, module_id, value) {
            Ok(PrecommitOutcome::Accept(v)) => Ok(v),
            Ok(PrecommitOutcome::Reject(reason)) => {
                Err(crate::router::PrecommitVeto::Rejected(reason))
            }
            Err(e) => Err(crate::router::PrecommitVeto::Error(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A precommit hook that accepts the write unchanged (returns 0 and
    /// echoes the value). Implements the hook_alloc + precommit ABI.
    const ACCEPT_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (global $heap_top (mut i32) (i32.const 1024))
          (func $alloc_inner (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap_top))
            (global.set $heap_top
              (i32.add (global.get $heap_top) (local.get $len)))
            (local.get $ptr))
          (func (export "hook_alloc") (param $len i32) (result i32)
            (call $alloc_inner (local.get $len)))
          (func (export "precommit")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (local $out_buf i32)
            (local.set $out_buf (call $alloc_inner (local.get $in_len)))
            (memory.copy (local.get $out_buf) (local.get $in_ptr) (local.get $in_len))
            (i32.store (local.get $out_ptr_ptr) (local.get $out_buf))
            (i32.store (local.get $out_len_ptr) (local.get $in_len))
            (i32.const 0)))
    "#;

    /// A precommit hook that VETOES every write: returns status 1 and a
    /// fixed reason string "denied".
    const REJECT_WAT: &str = r#"
        (module
          (memory (export "memory") 1)
          (data (i32.const 2048) "denied")
          (global $heap_top (mut i32) (i32.const 1024))
          (func (export "hook_alloc") (param $len i32) (result i32)
            (local $ptr i32)
            (local.set $ptr (global.get $heap_top))
            (global.set $heap_top
              (i32.add (global.get $heap_top) (local.get $len)))
            (local.get $ptr))
          (func (export "precommit")
            (param $in_ptr i32) (param $in_len i32)
            (param $out_ptr_ptr i32) (param $out_len_ptr i32)
            (result i32)
            (i32.store (local.get $out_ptr_ptr) (i32.const 2048))
            (i32.store (local.get $out_len_ptr) (i32.const 6))
            (i32.const 1)))
    "#;

    fn hooks() -> PrecommitHooks {
        PrecommitHooks::new(Arc::new(WasmModuleStore::new().expect("wasm store")))
    }

    #[test]
    fn accept_hook_passes_the_value_through() {
        let h = hooks();
        h.register("ok", ACCEPT_WAT.as_bytes()).expect("register");
        assert_eq!(
            h.run("ok", b"payload").expect("run"),
            PrecommitOutcome::Accept(b"payload".to_vec())
        );
    }

    #[test]
    fn reject_hook_vetoes_with_a_reason() {
        let h = hooks();
        h.register("no", REJECT_WAT.as_bytes()).expect("register");
        assert_eq!(
            h.run("no", b"payload").expect("run"),
            PrecommitOutcome::Reject("denied".to_string())
        );
    }

    #[test]
    fn unknown_module_is_not_found() {
        let h = hooks();
        assert!(matches!(
            h.run("absent", b"x"),
            Err(PrecommitError::ModuleNotFound(_))
        ));
    }
}
