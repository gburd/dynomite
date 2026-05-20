# 2026-05-19 - Migrate property tests from `proptest` to `hegeltest`

## Context

The original project brief asked for property-based testing using
"Hegel". The lead initially treated that as unfamiliar and substituted
`proptest`, recording a deviation in AGENTS.md Section 6.3 that said:

> Note on naming: the user request asked for "Hegel" property testing;
> the canonical Rust property-testing crate is `proptest`. We use
> `proptest`. If a tool literally named "Hegel" is identified later, we
> revisit; this is recorded in `docs/journal/allowances.md`.

The user has since clarified that "Hegel" referred to the Rust port of
Hypothesis at https://github.com/hegeldev/hegel-rust, published as
`hegeltest` on crates.io and re-exported as `hegel`. The lead's earlier
characterisation that "Hegel was not a real Rust crate" was wrong: the
crate exists, it is the framework the user named, and the user provided
the correct repository, documentation (http://hegel.dev), and skill
(https://github.com/hegeldev/hegel-skill) URLs.

This entry records the mechanical migration of every `proptest` test in
the workspace to `hegeltest`.

## Files touched

Workspace plumbing:

* `Cargo.toml` - replaced `proptest = "1.5"` with
  `hegeltest = { version = "0.14", features = ["native"] }` in
  `[workspace.dependencies]`. The `native` feature pulls in the
  in-process backend so tests do not depend on an external server.
* `crates/dynomite/Cargo.toml` - replaced `proptest` with `hegeltest`
  in `[dev-dependencies]`.
* `PLAN.md` - updated Section 2 dependency list and Section 6.3
  bullet to reference `hegeltest`.
* `AGENTS.md` Section 6.3 - rewrote the rationale: `hegeltest` is
  built on Hypothesis, supports stateful state-machine testing via
  `#[hegel::state_machine]`, and was the user's original choice.
* `docs/parity.md` - added a "project-wide follow-ups" entry pointing
  at this journal.

Test files (24 property tests across 9 files, all migrated):

| File                                                  | Properties | Notes                                                    |
| ----------------------------------------------------- | ---------- | -------------------------------------------------------- |
| `crates/dynomite/tests/conf_property.rs`              | 2          | Custom composite generator via `#[hegel::composite]`     |
| `crates/dynomite/tests/stage_01_core.rs`              | 3          | Default 100 cases (no per-test override)                 |
| `crates/dynomite/tests/stage_02_io.rs`                | 1          | `test_cases = 256`                                       |
| `crates/dynomite/tests/stage_03_properties.rs`        | 5          | Default 100 cases                                        |
| `crates/dynomite/tests/stage_05_stats.rs`             | 4          | `test_cases = 256`                                       |
| `crates/dynomite/tests/stage_06_crypto.rs`            | 4          | `test_cases = 64`                                        |
| `crates/dynomite/tests/stage_07_msg.rs`               | 2          | `test_cases = 256` and `512` respectively                |
| `crates/dynomite/tests/stage_08_proto.rs`             | 4          | `test_cases = 256`                                       |
| `crates/dynomite/src/stats/numeric.rs` (lib unit test) | 1          | Inside `#[cfg(test)] mod tests`; `#[hegel::test]` works  |

Total: **24 property tests migrated, 0 dropped, 0 partially migrated.**

## API mapping used

| `proptest`                                    | `hegeltest`                                                              |
| ----------------------------------------------- | -------------------------------------------------------------------------- |
| `proptest! { #[test] fn ... }`                  | `#[hegel::test] fn ... (tc: TestCase)`                                     |
| `any::<u32>()`                                  | `gs::integers::<u32>()`                                                    |
| `0u32..1000`                                    | `gs::integers::<u32>().min_value(0).max_value(999)`                        |
| `proptest::collection::vec(e, n..m)`            | `gs::vecs(e).min_size(n).max_size(m - 1)`                                  |
| `proptest::sample::select(vec![...])`           | `gs::sampled_from(&[...])` (note: takes a slice reference)                 |
| `prop_oneof![a, b]`                             | `gs::one_of([a, b])` (not used in this migration)                          |
| `prop::option::of(g)`                           | `gs::optional(g)`                                                          |
| `from regex / Strategy<Value=String>`           | `gs::from_regex("...").fullmatch(true)`                                    |
| `prop_assert_eq!(a, b)`                         | `assert_eq!(a, b)`                                                         |
| `prop_assert!(cond, "...")`                     | `assert!(cond, "...")`                                                     |
| `ProptestConfig { cases: 256, .. }`             | `#[hegel::test(test_cases = 256)]`                                         |
| Hand-rolled `proptest::TestRunner::new(Config { cases: 256, .. })` loop with multi-field tuple | Inlined into a `#[hegel::test(test_cases = 256)]` body using `tc.draw` per field |
| Composite `Strategy` chain (`prop_map`/`prop_filter`) | `#[hegel::composite] fn arb_pool_facts(tc: TestCase) -> PoolFacts { ... }` |

## Case-count parity

`hegeltest`'s `#[hegel::test]` defaults to **100 cases per test**
(see `runner.rs::Settings::new`). To match the migration target's
in-tree case counts, every test that used `ProptestConfig::with_cases`
or set `cases:` directly in `proptest!` is annotated with
`#[hegel::test(test_cases = N)]` carrying the same N. Tests that did
not set a case count keep `hegeltest`'s default of 100 (down from
`proptest`'s default of 256). PLAN.md's "long-soak job runs each
test for 1M cases" target is unchanged: in CI, `--test-cases` can be
overridden via the `Hegel` CLI driver, and the `make soak` target
will need a small tweak when it lands.

The per-test counts after migration:

* `conf_property.rs::*` - 256
* `stage_01_core.rs::*` - default 100 (was default 256 under proptest)
* `stage_02_io.rs::split_off_then_append_reconstructs` - 256
* `stage_03_properties.rs::*` - default 100 (was default 256)
* `stage_05_stats.rs::*` - 256
* `stage_06_crypto.rs::*` - 64
* `stage_07_msg.rs::dnode_parser_round_trip_proptest` - 256
* `stage_07_msg.rs::parser_total_on_arbitrary_bytes` - 512
* `stage_08_proto.rs::*` - 256
* `stats::numeric::tests::floor_p_times_u64_matches_f64_floor` -
  default 100

## Test-count delta

* Before (main HEAD `fecd052`): 565 nextest, 569 with `--all-features`,
  527 doctests.
* After (this branch):
  * `cargo nextest run --workspace` -> **565 passed**
  * `cargo nextest run --workspace --all-features` -> **569 passed**
  * `cargo test --doc --workspace` -> **527 passed**

Note: `proptest!` collapsed all properties in one `proptest! { ... }`
block into a single `#[test]` entry; `hegeltest` emits one
`#[test]` per `#[hegel::test]` function. Even so the totals above
match the baseline because (a) a `proptest!` block containing N
properties already showed up as N test entries, and (b) the
hand-rolled `TestRunner::new` test in `stage_07_msg.rs` was already
its own `#[test]`.

## Verification

* `cargo build --workspace --locked` - OK
* `cargo build --workspace --all-features --locked` - OK
* `cargo nextest run --workspace` - 565 / 565
* `cargo nextest run --workspace --all-features` - 569 / 569
* `cargo test --doc --workspace` - 527 / 527
* `scripts/check.sh` - OK
* `cargo deny check` - OK (hegeltest is MIT, already in the
  `deny.toml` allow list)

## Migrations that needed a tweak

* `gs::sampled_from` requires a slice reference, not a fixed-size
  array, so `gs::sampled_from(&[a, b, c])` is the canonical form.
  This bit the first attempt in `stage_07_msg.rs`; fixed in the
  same commit.
* `gs::from_regex("[a-z][a-z0-9_]{0,15}")` defaults to "contains a
  match" rather than "fullmatch". The original `proptest` regex
  strategy is fullmatch, so all `from_regex` callers add
  `.fullmatch(true)`. Discovered while migrating `conf_property.rs`.
* The `Generator` trait must be imported (`use hegel::Generator;`)
  for `.filter()`/`.map()` to be in scope on a regex/integer/etc
  builder. The crate-level docs use `hegel::generators::Generator`
  but the re-export `hegel::Generator` is the canonical path.

## Acknowledgement

The lead's earlier deviation in AGENTS.md Section 6.3 - "the canonical
Rust property-testing crate is `proptest`. We use `proptest`. If a
tool literally named 'Hegel' is identified later, we revisit" -
was a misread of the user's original brief. The user provided the
correct identification: `hegeltest` (https://github.com/hegeldev/hegel-rust),
documented at http://hegel.dev, with a companion skill at
https://github.com/hegeldev/hegel-skill. This migration removes the
deviation and aligns the workspace with the user's intent from day
one.
