//! C Dynomite reference functions exposed as test oracles.
//!
//! The transliteration plan (`docs/transliteration/PLAN.md`, section 5.2)
//! requires every pure-logic C function to be checked against its Rust
//! transliteration on random inputs. This crate compiles those C functions
//! (see `build.rs`) and wraps each one in a safe Rust function with the
//! same inputs and outputs, so property tests can call the C and the Rust
//! side by side.
//!
//! The crate is test-only (`publish = false`) and is the one place in the
//! transliterated engine where `unsafe` is permitted: every wrapper crosses
//! the C ABI. Each `unsafe` block documents the invariant that makes the
//! call sound.
//!
//! When the C reference tree is absent the build sets `c_oracle_absent`,
//! [`available`] returns `false`, and no C symbols are linked.

/// `true` when the C reference was compiled into this crate.
pub const fn available() -> bool {
    cfg!(not(c_oracle_absent))
}

#[cfg(not(c_oracle_absent))]
pub mod hashkit;
#[cfg(not(c_oracle_absent))]
pub mod vnode;
