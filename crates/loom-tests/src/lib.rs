//! Loom model-check harness for the Dynomite Rust port's concurrency
//! primitives.
//!
//! # Why this is a standalone crate
//!
//! Loom is a model checker: it runs a concurrent program under
//! every legal interleaving of atomic operations and asserts
//! invariants. To do so, it shadows `std::sync::atomic` with its
//! own atomic types whose operations record into the model. This
//! requires the program-under-test to use loom's atomics rather
//! than std's, gated on `#[cfg(loom)]`.
//!
//! Tokio gates large swathes of its API behind `cfg(not(loom))`,
//! so a crate that depends on tokio (such as `dynomite`) does not
//! link under `--cfg loom`. The standard workaround, used by
//! tokio's own loom tests, is to **re-model the primitives** in a
//! crate that does not transitively pull in tokio, then exercise
//! the model under loom. The model's invariants must match the
//! production code's, so the responsibility is on the test author
//! to keep them in sync.
//!
//! # What is modeled here
//!
//! 1. A simplified hint-store enqueue/drain primitive (mirrors
//!    [`dyn_riak::handoff::HintStore`]'s contended path).
//! 2. A simplified phi-accrual sample-record/observe primitive
//!    (mirrors [`dynomite::cluster::failure_detector`]'s running
//!    statistics).
//! 3. A simplified mbuf alloc/free primitive (mirrors
//!    [`dynomite::io::mbuf::MbufPool`]'s free list).
//!
//! # Running the tests
//!
//! ```text
//! RUSTFLAGS='--cfg loom' cargo test -p loom-tests --release
//! ```
//!
//! Without the `--cfg loom` flag the tests compile to no-ops; the
//! crate is in the workspace unconditionally so `cargo metadata`
//! and `cargo build` see it under default builds.

#![cfg_attr(not(test), allow(dead_code))]

#[cfg(loom)]
pub(crate) mod hint_store;

#[cfg(loom)]
pub(crate) mod phi_accrual;

#[cfg(loom)]
pub(crate) mod mbuf_pool;

// Without --cfg loom the crate is empty; this stub satisfies
// `cargo build` and produces no warnings.
#[cfg(not(loom))]
/// No-op marker visible in default builds; loom tests are gated
/// on `--cfg loom`.
pub fn loom_disabled_marker() -> &'static str {
    "loom tests are gated on --cfg loom; build with RUSTFLAGS='--cfg loom'"
}
