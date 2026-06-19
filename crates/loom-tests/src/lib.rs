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
//! link under `--cfg loom`. There are two ways to work around
//! that:
//!
//! 1. **Re-model the primitive in a tokio-free crate.** This crate
//!    holds those starter models: a hint-store enqueue/drain
//!    primitive, a phi-accrual sample-record/observe primitive,
//!    and an mbuf alloc/free primitive. Each is a hand-rolled
//!    miniature of its production cousin -- the algorithm matches
//!    by inspection, not by sharing source. Use this pattern for
//!    fresh primitives whose production home still has heavy
//!    runtime dependencies.
//! 2. **Lift the primitive into a tokio-free crate.** When the
//!    algorithm is small enough to stand alone, lift it into a
//!    sibling crate that depends only on `std` (and `loom` under
//!    `--cfg loom`). The production embed becomes a thin wrapper
//!    that adds runtime ergonomics on top. The
//!    [`throttle-core`](../throttle_core/index.html) crate is the
//!    reference example of this pattern: it owns the AtomicU64 +
//!    `Mutex<Instant>` token-bucket algorithm, and
//!    `dynomite::runtime::throttle` reduces to a tokio-async
//!    wrapper plus the Prometheus metric.
//!
//! # What is modeled here
//!
//! 1. A simplified hint-store enqueue/drain primitive (mirrors
//!    the `dyniak::handoff::HintStore` contended path).
//! 2. A simplified phi-accrual sample-record/observe primitive
//!    (mirrors the `dynomite::cluster::failure_detector` running
//!    statistics).
//! 3. A simplified mbuf alloc/free primitive (mirrors
//!    the `dynomite::io::mbuf::MbufPool` free list).
//!
//! The token-bucket throttle is NOT modeled here: it is exercised
//! directly under loom in `crates/throttle-core/tests/loom.rs`,
//! against the production type. Lifted-and-tested beats
//! re-modeled when the algorithm fits in a standalone crate.
//!
//! # Running the tests
//!
//! ```text
//! bash scripts/loom.sh
//! ```
//!
//! which is shorthand for
//!
//! ```text
//! RUSTFLAGS='--cfg loom' RUSTDOCFLAGS='--cfg loom' \
//!     cargo test -p loom-tests -p throttle-core --release
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
