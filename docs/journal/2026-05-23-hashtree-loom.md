# 2026-05-23 - hashtree loom coverage for lazy segment digest

## Summary

Added loom-checked tests for the lazy-init path on
`hashtree::Segment::hash`. The crate landed earlier today
(commit `3a45d39`) using `std::cell::OnceCell<Hash>`, which is
not thread-safe and would have made `HashTree` `!Sync`. This
session lifts the cache to a thread-safe primitive and proves
the contract under loom.

## Approach (option (b))

Per AGENTS.md, the workspace forbids unsafe everywhere. Loom
0.7 does not ship a shadow for `OnceLock`, so option (a) (a
hand-rolled `AtomicBool + UnsafeCell` cell) would have required
a module-level `#![allow(unsafe_code)]` plus a justification
entry. Option (b) keeps the workspace forbid intact:

- New module `crates/hashtree/src/lazy_hash.rs` exposes a
  `LazyHash` newtype with two cfg-gated back-ends:
  - `cfg(not(loom))`: `std::sync::OnceLock<Hash>` (production).
  - `cfg(loom)`: `loom::sync::Mutex<Option<Hash>>`.
- Both back-ends honour the same `get_or_init`/`new` surface
  and the same observable contract (single stable result per
  instance, no torn reads).
- `Segment` now holds `LazyHash` instead of `OnceCell<Hash>`;
  `Segment` therefore becomes `Sync`, and so does `HashTree`.

## Files touched

- `crates/hashtree/src/lib.rs`
  - swap `std::cell::OnceCell` for `LazyHash`
  - gate `mod tests` on `cfg(all(test, not(loom)))` so the
    existing unit tests are not invoked outside `loom::model`
  - top-level doctest now wraps its body in
    `#[cfg(not(loom))] fn main()` so RUSTDOCFLAGS='--cfg loom'
    sees a no-op example
- `crates/hashtree/src/lazy_hash.rs` (new)
- `crates/hashtree/Cargo.toml`
  - `[target.'cfg(loom)'.dependencies] loom = { workspace = true }`
- `crates/hashtree/tests/basic.rs`,
  `crates/hashtree/tests/properties.rs`
  - `#![cfg(not(loom))]` so the integration suite is skipped
    under `--cfg loom` (loom primitives must be touched only
    inside a `loom::model` closure)
- `crates/hashtree/tests/loom.rs` (new) - 3 model tests
- `scripts/loom.sh`
  - include `-p hashtree` and pass `RUSTDOCFLAGS='--cfg loom'`
    so doctests pick up the cfg

## Tests added

`crates/hashtree/tests/loom.rs`:

1. `concurrent_segment_hash_consistent` - two threads call
   `segment_hash(0)` on the same tree; loom verifies both
   threads observe the same digest under every interleaving.
2. `root_under_concurrent_segment_walks` - two walkers traverse
   segments in opposite orders and then read `root()`; loom
   verifies all three reads agree.
3. `diff_under_concurrent_init` - two cold trees with the same
   multiset are diffed concurrently in opposite directions;
   loom verifies the diff is empty under every interleaving.

Plus three local non-loom unit tests for `LazyHash` itself
(stability of the cached value, clone produces a fresh cell,
default constructs an empty cell).

## Verification

```
cargo build -p hashtree                   # OK
cargo test -p hashtree                    # 7 + 0 + 4 + 1 doctest, all green
cargo nextest run -p hashtree             # 23/23
cargo nextest run -p dyniak             # 431/431
cargo clippy -p hashtree --all-targets -- -D warnings   # clean
RUSTFLAGS='--cfg loom' cargo clippy -p hashtree --all-targets --release -- -D warnings   # clean
bash scripts/loom.sh                       # 6/6 (3 hashtree + 3 loom-tests) plus 1 doctest
```

`cargo fmt --check` is noisy from an unrelated path
(`/home/gburd/ws/noxu/`); within the dynomite worktree there are
no fmt diffs.

## Notes / caveats

- The loom alt back-end is over-synchronised relative to
  production: a `Mutex<Option<Hash>>` serialises every
  observer, whereas `OnceLock` lets readers fast-path. The
  *observable* contract under test (single stable digest, no
  torn read) is preserved, which is the property loom is
  exhaustively verifying. The performance contract belongs to
  benchmarks, not loom.
- `LazyHash::Clone` always produces a fresh, empty cell. That
  is a behavioural change vs. the old `OnceCell<Hash>` (which
  preserved the cache across clones), but `HashTree` clones
  are infrequent and recomputing a leaf digest is cheap
  relative to maintaining a cloneable thread-safe cell on the
  loom path. This is documented in the module-level rustdoc.

## Result

```
STAGE: hashtree-loom
STATUS: READY_FOR_REVIEW
BRANCH: stage/hashtree-loom
LAZY_INIT_APPROACH: OnceLock-kept-with-cfg-loom-alt
LOOM_TESTS: 3
```
