# 2026-05-30 - tre-sys: justification for `unsafe_code` allowance

## Why this entry exists

`AGENTS.md` Section 5 forbids `unsafe` workspace-wide unless a
journal entry justifies the exception, links to the upstream
constraint, and documents the safety invariants and test
coverage. This entry covers the introduction of the
`crates/tre-sys/` crate, which is the only place in the
workspace where `unsafe` is permitted.

## Where unsafe lives

`unsafe_code` is set to `allow` in `crates/tre-sys/Cargo.toml`
under `[lints.rust]`. Every other crate in the workspace keeps
the workspace default of `unsafe_code = "forbid"`. The bound is
enforced by the workspace lint table; if a future change tries
to introduce `unsafe` elsewhere it fails compilation.

Inside `tre-sys` the `unsafe` blocks are clustered in:

* `src/lib.rs::raw` - the `extern "C"` declarations themselves.
* `src/lib.rs` safe wrappers (`safe_regncomp`,
  `safe_reganexec`, `safe_regfree`, `safe_regerror`,
  `default_regaparams`) - each `unsafe { ... }` block carries a
  contiguous `// SAFETY:` comment describing the contract that
  must hold at the call site.
* `src/lib.rs::OwnedRegex::Drop` - a single `safe_regfree`
  call.
* `tests/raw_ffi.rs` - exercises the FFI directly so the
  contract is documented in test form.

## Why unsafe is necessary

TRE is a C library. Calling C functions from Rust requires
`extern "C"` declarations, and those are unconditionally
`unsafe` in the language. There is no safe way to express a
foreign function call that hands ownership of a heap-allocated
TNFA across the Rust/C boundary; the language does not have a
sound model for that without `unsafe`.

## Safety invariants enforced

The `tre-sys` crate documents and tests the following
invariants:

1. `regex_t` is zero-initialised before any `tre_regncomp`
   call (`empty_regex_t`).
2. `tre_regncomp` is called exactly once per `regex_t` before
   any matcher entry point reads it.
3. On a non-`REG_OK` return from `tre_regncomp`, the `regex_t`
   is dropped without `tre_regfree` (which TRE leaves
   indeterminate on compile failure). `compile()` enforces
   this: it returns the un-freed `regex_t` only inside a
   successful `Ok(OwnedRegex)`.
4. `tre_regfree` is called exactly once on a successfully
   compiled `regex_t`. `OwnedRegex::Drop` is the unique drop
   point.
5. String buffers passed to `tre_regncomp` and `tre_reganexec`
   are valid for the indicated length and outlive the call.
   The slice borrow in `safe_regncomp` / `safe_reganexec`
   guarantees this.
6. `regamatch_t::pmatch` is null with `nmatch == 0`, so TRE
   never dereferences an unwritable submatch buffer in our
   call path.
7. `regex_t` is not shared across threads. Enforced by the
   `PhantomData<*const ()>` field on `dyntext::tre::TreCompiledPattern`,
   which drops both `Send` and `Sync`. A unit test in
   `dyntext::tre::tests` documents the bound.

## Test coverage

* `crates/tre-sys/tests/raw_ffi.rs` - 6 tests that drive the
  raw FFI directly:
  * exact match returns cost 0
  * one-typo match returns cost 1
  * over-budget input returns `REG_NOMATCH`
  * `tre_regncomp` rejects invalid patterns
  * `tre_regerror` formats known codes
  * `CString` patterns compile equivalently to byte slices
* `crates/dyntext/tests/tre_wrapper.rs` - 14 tests on the safe
  wrapper, including the brief's verbatim demo assertions and
  a 1024-iteration drop loop that a future leak regression
  would expose under valgrind.
* `crates/dyntext/tests/regex_search_approx.rs` - 5 tests that
  drive `TextIndex::search_regex_approx`.

Total Phase 3 tests: 28 new tests; all 94 workspace tests in
the two crates pass under `cargo nextest`.

## New dependencies

| Crate | Type | Version | Notes |
|---|---|---|---|
| `cc` | build-dep on `tre-sys` | `1.0` | Already a transitive workspace dep at 1.2.63. Used to compile vendored TRE. |
| `pkg-config` | build-dep on `tre-sys` | `0.3` | Probes for system libtre before falling back to vendored build. |
| `libc` | runtime dep on `tre-sys` | `0.2` | Already a transitive workspace dep at 0.2.186. Used for `c_int`, `c_char`, `size_t` typedefs. |
| `tre-sys` | runtime dep on `dyntext` | path | New first-party FFI crate. |

No `Cargo.lock` churn beyond the addition of `tre-sys`,
`pkg-config`, and an explicit `libc` direct edge.

## Vendored TRE source

The TRE C library is added as a git submodule at
`crates/tre-sys/vendor/tre/`, pinned to upstream tag `v0.8.0`.
The build script first probes for a system-installed `libtre`
(>= 0.8.0) via `pkg-config` (controllable with the
`TRE_NO_PKG_CONFIG` env var). When none is found, and the
default `vendored` Cargo feature is enabled, it compiles the
13 source files under `lib/` into a static `libtre.a` via the
`cc` crate.

The build is intentionally narrow: `TRE_APPROX` is on, but
`TRE_WCHAR` and `TRE_MULTIBYTE` are off so the vendored
library treats input as raw bytes regardless of the host
locale. The minimal `config.h` and `tre-config.h` under
`crates/tre-sys/config/` capture these settings without
running autoconf.

## libtre availability and CI

This host (and both Codeberg/GitHub Actions runners) lack a
system `libtre-dev` package. The `vendored` feature is
default-on so the static-linking path runs everywhere out of
the box. If a future host gains `libtre-dev`, set
`default-features = false` on the `tre-sys` dependency to use
the system shared library; the `pkg-config` probe will pick
it up automatically.

The escape hatch documented in the brief ("`#[cfg(not(feature
= \"tre\"))]` no-op stub") is not needed because the vendored
build succeeds on every host where `cc` itself works, which
includes both CI runners.

## What this enables

Phase 3 of the dyntext design is now complete: callers can
use `dyntext::tre::TreCompiledPattern` to do approximate-regex
matching with up to k typos, and
`TextIndex::search_regex_approx` ties that into the document
store. Phase 2's regex prefix extractor will plug into
`search_regex_approx` later to gate the full-scan fallback
behind the trigram funnel.
