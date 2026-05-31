# Throttle-core: lift token-bucket primitive into a tokio-free crate

Date: 2026-05-28
Branch: `stage/throttle-core-loom`
Author: Greg Burd <greg@burd.me>

## Goal

Extract the `Throttle` token-bucket algorithm from
`crates/dynomite/src/runtime/throttle.rs` into a standalone
crate `crates/throttle-core/` so it can be model-checked under
loom. Tokio gates large parts of its public API on
`cfg(not(loom))`, which means anything that depends on
`dynomite` (which depends on tokio) cannot link under
`RUSTFLAGS='--cfg loom'`. The split lets the production
algorithm be exercised directly under loom while the dynomite
embed keeps its tokio-async ergonomics.

## What landed

* New crate `crates/throttle-core` (no tokio, only `std` +
  `thiserror` + `loom` under `--cfg loom`).
  * `Throttle<C: Clock>` owns the `AtomicU64` bucket count and
    the `Mutex<Instant>` last-refill timestamp. Generic over a
    `Clock` trait so loom tests inject a deterministic
    `ManualClock`.
  * `SystemClock` (default, delegates to `Instant::now`) and
    `ManualClock` (`AtomicU64` offset in nanoseconds, advanced
    explicitly from tests).
  * Method set: `new`, `with_clock`, `try_acquire`,
    `acquire_blocking` (sync, `std::thread::sleep` wait loop),
    `available`, `capacity`, `refill_per_sec`.
  * `ThrottleError` typed-error enum returned by
    `acquire_blocking` (covers `RequestExceedsCapacity` and
    `ZeroRefillExhausted`).
  * Blanket `Clock` impls for `&C`, `std::sync::Arc<C>`, and
    (under `--cfg loom`) `loom::sync::Arc<C>` so callers can
    share a clock across threads without newtype wrapping.
* `crates/dynomite/src/runtime/throttle.rs` rewritten as a thin
  wrapper that holds `Inner<SystemClock>` plus the static
  metric label. Public API surface is unchanged:
  `Throttle::new`, `Throttle::with_name`, `try_acquire`,
  `acquire` (async, `tokio::time::sleep` wait loop), `available`,
  `capacity`, `refill_per_sec`. `ThrottleError` is re-exported
  from `dynomite::runtime` for symmetry.
* `crates/throttle-core/tests/loom.rs` (`#![cfg(loom)]`):
  * `two_threads_never_overgrant` -- capacity 2, zero refill,
    four contending `try_acquire(1)` calls; total grants must
    be exactly 2.
  * `refill_grants_resume_after_clock_advance` -- drain, then
    advance the `ManualClock` from a worker thread; the
    post-join `try_acquire(1)` must always observe the refill.
  * `try_acquire_zero_is_always_true` -- the no-op admission
    check is robust against racing acquisitions.
  * `capacity_one_serializes_grants` -- two contending threads,
    capacity 1, zero refill; exactly one grant across all
    interleavings.
* `scripts/loom.sh` now sets both `RUSTFLAGS='--cfg loom'` and
  `RUSTDOCFLAGS='--cfg loom'` (the latter is needed so doctests
  compile against the loom-built rlib without panicking from
  outside `loom::model`) and runs the sweep over both
  `loom-tests` and `throttle-core`.
* `crates/loom-tests/src/lib.rs` documentation updated to
  contrast the two patterns: re-modeled primitives stay in
  `loom-tests` (hint_store, phi_accrual, mbuf_pool); algorithms
  small enough to lift get their own crate (throttle-core is
  the reference example).
* Workspace `Cargo.toml`: new member `crates/throttle-core`.
* Dynomite `Cargo.toml`: path-dep on `throttle-core`.

## Verification

Run from the worktree root:

```
RUSTFLAGS='--cfg loom' RUSTDOCFLAGS='--cfg loom' \
  cargo test -p throttle-core --release
# 11 unit tests + 4 loom tests + 1 doctest, all green.

cargo nextest run --workspace --features riak
# 1569 prod tests, all green.

cargo clippy --workspace --all-targets --all-features -- -D warnings
# Clean.

cargo fmt --all -- --check
# Clean (in-repo files; pre-existing fmt diffs in path-dep
# `../noxu/crates/noxu-*` are upstream and out of scope).
```

## Notes / decisions

1. **Why `RUSTDOCFLAGS` in `scripts/loom.sh`.** Without it,
   `cargo test --doc` builds the doctest binaries without
   `--cfg loom`, but the throttle-core rlib it links to was
   built with loom. Constructing a `Throttle` then triggers a
   loom atomic constructor outside `loom::model`, which panics.
   Setting `RUSTDOCFLAGS='--cfg loom'` tags the doctest source
   with the same cfg gate, and the doctest example wraps its
   body in `# #[cfg(not(loom))] fn run() { ... } # run();` so
   the generated `fn main()` compiles to a no-op under loom.
2. **Clock trait abstraction.** Loom's scheduler does not
   shadow `std::time`, so calling `Instant::now()` from inside
   a loom model is technically legal but non-deterministic and
   the model checker cannot reproduce a failing schedule. The
   `Clock` trait + `ManualClock` plumb a deterministic time
   source through `try_acquire`'s refill computation.
   `ManualClock` stores an offset in nanoseconds in an
   `AtomicU64` (the loom-shadowed one under `--cfg loom`), so
   the clock itself is part of the model.
3. **Why `Mutex<Instant>` for `last_refill`.** The original
   used `parking_lot::Mutex`; we switched to `std::sync::Mutex`
   so the same source compiles under both default and loom
   builds (loom shadows the std type). The `expect("invariant:
   throttle last_refill mutex must not be poisoned")` matches
   the project's `.expect("invariant: ...")` policy.
4. **Wrapper kept the `name: &'static str` field for the
   metric label.** That stays in dynomite because Prometheus
   is dynomite-side. throttle-core has no metric coupling.
5. **`acquire_blocking` returns `Result` rather than panicking.**
   The async dynomite wrapper preserves the panic semantics for
   backward compat with the existing integration tests
   (`runtime_throttle.rs`, all 7 still green). The blocking
   path in throttle-core uses typed errors because it is the
   crate's only fallible entry point and surfaces directly to
   non-tokio callers.

## Status

`STATUS: READY_FOR_REVIEW`
