# 2026-05-30 -- Workspace dependency refresh (second pass)

Branch: `stage/deps-refresh-may30`
Worker: deps-refresh agent (second pass)

## Goal

Second-pass refresh of the non-`noxu-*` workspace dependencies
following the May 26 sweep (commit 847c8e6, journal entry
`docs/journal/2026-05-26-deps-refresh.md`). Bump every dep with
a newer release since May 26, retry the deferred items the May
26 pass could not land, fix any API drift, and verify the
workspace stays green.

The Noxu path-deps (`noxu-db`, `noxu-cleaner`, `noxu-config`,
`noxu-dbi`, `noxu-engine`, `noxu-evictor`, `noxu-latch`,
`noxu-log`, `noxu-recovery`, `noxu-sync`, `noxu-tree`,
`noxu-txn`, `noxu-util`) are out of scope; another worker owns
the Noxu side. The lockfile picks up `2.3.0 -> 2.4.2` for them
automatically, but no manifest pin changes.

## Bumps applied (manifest)

| Crate                          | Before | After    | Notes                                                                                                                                                                                                                                                       |
| ------------------------------ | ------ | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| nix                            | 0.30   | 0.31     | No drift. Patch lands transparently against the existing `dup2_*` / `Flock` call sites.                                                                                                                                                                     |
| quiche                         | 0.22   | 0.28     | Six minor releases applied without API drift in our usage. The 0.29 release pulled in the BoringSSL-based `boring` dependency chain, which needs `libclang` at build time and is not currently provisioned in this dev shell; held at 0.28. |
| wasmtime                       | 26     | 36.0.8   | Cleared the entire stack of 12 wasmtime advisories that were live on `main`. The fix windows for RUSTSEC-2026-0085..0096 and RUSTSEC-2026-0114 are `>=36.0.7,<37 OR >=42.0.2,<43 OR >=43.0.1`; 36.0.x is the only fix train compatible with our pinned rustc 1.90 toolchain (42.x and 43.x require rustc 1.91; 44+ require 1.92; 45 requires 1.93). The lockfile resolves to 36.0.10 via the `^36.0.8` caret. The `dyniak::mapreduce::wasm` module compiles cleanly without code changes; the `Engine` / `Module` / `Store` API surface we lean on is stable across 26 -> 36. |
| criterion                      | 0.5    | 0.8      | API drift: `criterion::black_box` was deprecated in 0.6 and the replacement is `std::hint::black_box`. Updated the imports across all eight bench files (`benches/{crypto,dnode,hashkit,mbuf,parsers,quorum,random_slicing,tokens}.rs`) to import `black_box` from `std::hint` instead. No call-site behaviour changes; `std::hint::black_box` is a drop-in replacement.                                                                                                                                       |
| opentelemetry                  | 0.27   | 0.32     | API drift: see "OpenTelemetry 0.27 -> 0.32" below.                                                                                                                                                                                                          |
| opentelemetry_sdk              | 0.27   | 0.32     | API drift: see "OpenTelemetry 0.27 -> 0.32" below. The `rt-tokio` feature is no longer required for our `with_batch_exporter(exporter)` call site; dropped from the feature list.                                                                          |
| opentelemetry-otlp             | 0.27   | 0.32     | API drift: see "OpenTelemetry 0.27 -> 0.32" below. No call-site changes beyond the type renames.                                                                                                                                                            |
| tracing-opentelemetry          | 0.28   | 0.33     | Tracks the OTel stack. No drift in our usage (`tracing_opentelemetry::layer().with_tracer(tracer)`).                                                                                                                                                        |
| opentelemetry-appender-tracing | 0.27   | 0.32     | Tracks the OTel stack. No drift in our usage (`OpenTelemetryTracingBridge::new(provider)`).                                                                                                                                                                 |
| hyper (dyniak)               | 1.9    | 1.10     | Lockfile-side bump only since `^1.9` already accepts 1.10.x; manifest pin updated for clarity. No drift.                                                                                                                                                    |

Total: **9** workspace deps bumped at the manifest level (counting the OTel
family as five entries, since each pin is independent), plus `hyper` as a
clarifying manifest update on top of an existing lockfile pickup.

## Lockfile-only patch refreshes (no manifest change)

`cargo update` (no `--workspace`) picked up patches across the
existing caret pins: `hegeltest 0.14.8 -> 0.14.17`, `hyper 1.9.0
-> 1.10.1`, `serde_json 1.0.149 -> 1.0.150`, `socket2 0.6.3 ->
0.6.4`, `mio 1.2.0 -> 1.2.1`, `log 0.4.29 -> 0.4.30`, plus
transitive `wasm-bindgen`, `web-sys`, `js-sys`, `cc`,
`bumpalo`, `autocfg`, `either`, `http`, `memchr`,
`typenum`, `uuid`, `wasm-encoder`/`wasmparser`/`wast`/`wat`,
`zerocopy`. Nothing that touches our public API surface.

## API drift

### OpenTelemetry 0.27 -> 0.32

The 0.27 -> 0.28 boundary renamed every "owned SDK type" to a
`Sdk*` prefix and reshaped the `Resource` constructor; 0.29 ->
0.32 are otherwise drop-in for our usage. The drift is
concentrated in `crates/dynomited/src/observability.rs`:

1. `opentelemetry_sdk::trace::TracerProvider` ->
   `opentelemetry_sdk::trace::SdkTracerProvider`. The
   `TracerProvider` name now refers to the trait
   `opentelemetry::trace::TracerProvider`; the concrete struct
   is `SdkTracerProvider`. Updated the `use` line, the
   `ObservabilityGuard.tracer` field type, the
   `init_otlp_tracer` return type, and the `build_tracer_provider`
   return type and inner builder call.
2. `opentelemetry_sdk::logs::LoggerProvider` ->
   `opentelemetry_sdk::logs::SdkLoggerProvider`. Same shape as
   the tracer: trait vs struct rename, applied at four call
   sites.
3. `Resource::new(vec![KeyValue::new("service.name", name)])` ->
   `Resource::builder().with_attribute(KeyValue::new(...)).build()`.
   The `Resource::new` constructor was made `pub(crate)` and the
   public surface moved to a builder. Equivalent semantics; one
   call site in `build_resource`.
4. `with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)`
   -> `with_batch_exporter(exporter)`. The 0.28 SDK moved the
   async-runtime-aware batch processor into a separate module
   (`log_processor_with_async_runtime`) and made the default
   `with_batch_exporter` use a dedicated background thread. The
   `rt-tokio` feature is no longer required by our call sites
   and was dropped from the `opentelemetry_sdk` feature list. No
   functional regression: the batch processor still flushes on
   `provider.shutdown()` which our `ObservabilityGuard` already
   calls.

The `ObservabilityGuard::shutdown` already swallowed the result
of `provider.shutdown()` via `Result<_, _>::Err` matching, and
the new `OTelSdkResult` type satisfies the `Display` bound the
existing match arms use. No further changes needed for the
shutdown path.

### Criterion 0.5 -> 0.8

`criterion::black_box` was deprecated in 0.6 in favour of the
upstream-stabilised `std::hint::black_box`. The replacement is a
drop-in: identical signature, identical semantics, no behavioural
change on either x86_64 or aarch64. Eight bench files updated.

### Wasmtime 26 -> 36

No source-level drift in our usage. The `Engine`, `Module`,
`Store`, and `Linker` API surface our `dyniak::mapreduce::wasm`
fitting depends on is stable across the 26 -> 36 train.

### Hyper 1.9 -> 1.10

No drift; the `^1.9` caret already accepted 1.10.x. The manifest
update is for clarity only.

## Deferred (and why)

| Crate                          | Current | Latest    | Reason                                                                                                                                                                                                                                                       |
| ------------------------------ | ------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| wasmtime                       | 36.0.8  | 45.0.0    | The 42.x / 43.x trains require rustc 1.91; 44.x requires 1.92; 45.x requires 1.93. We are pinned to rustc 1.90 by `rust-toolchain.toml`. 36.0.x is the highest train compatible with 1.90 and is one of the patched fix windows for the wasmtime CVEs. Bumping further requires a coordinated toolchain bump, which is its own piece of work.                                                                                                                                                                                       |
| quiche                         | 0.28    | 0.29      | The 0.29 release switched from the bundled `quiche-aws-lc` chain to a `boring` (BoringSSL-based) chain that requires `libclang` at build time. The dev shell does not currently provision libclang via the flake; doing so is its own change. Holding at 0.28.                                                                                                                                                                                                                                                                                                                                                                                                          |
| serde_yaml                     | 0.9     | 0.9.34+deprecated | Crate is upstream-deprecated. Switching to a maintained alternative (`serde_yml`, `serde_yaml_ng`, etc.) is a separate decision tracked elsewhere.                                                                                                |
| webpki-roots                   | 0.26    | 1.0       | Major bump; would chain through `hyper-rustls` (still on 0.27.x against webpki-roots 0.26). The 0.26 pin is intentional per the existing comment in `Cargo.toml`.                                                                                                                                          |
| rustls                         | 0.23    | 0.24-dev  | 0.24 is a `-dev` pre-release; not stable.                                                                                                                                                                                                                   |
| aes / cbc / cipher / sha1 / rsa | 0.8 / 0.1 / 0.4 / 0.10 / 0.9 | 0.9 / 0.2 / 0.5 / 0.11 / 0.10-rc | The RustCrypto chain bumps in lockstep across `cipher 0.4 -> 0.5`. Touches every block-cipher and AEAD call site in `dynomite::crypto`. `rsa 0.10` is still an RC. Deferred. |
| rand / rand_core               | 0.8 / 0.6 | 0.10 / 0.10 | Two consecutive majors (0.8 -> 0.9 reorganised the `Rng` trait; 0.9 -> 0.10 again). High-touch in entropy / token / fuzz call sites. Out of scope for a deps-refresh pass.                                                                                                                                                                                                                            |
| bincode                        | 1.3     | 3.0       | Major API rework at the 1.x -> 2.x boundary (`bincode::serialize`/`deserialize` -> `bincode::serde::encode_to_vec`/`decode_from_slice` plus a configuration argument). One pair of call sites in `crates/hashtree/src/lib.rs`. Tractable but out of budget for this pass; deferred to a dedicated bincode-2.x bump pass that also clears RUSTSEC-2025-0141 (`bincode 1.x` is unmaintained). |
| rustls-pemfile                 | 2       | 2.2       | Already the latest stable within the existing `^2` pin; the lockfile picks up patches automatically. Crate remains upstream-archived (RUSTSEC-2025-0134); the canonical migration is to `rustls-pki-types::pem::PemObject` and is its own piece of work.                                                                                                                                  |

Total deferred: **8** family-bumps (vs 9 in the May 26 pass; the
opentelemetry, wasmtime, criterion, quiche, and nix items have moved out of
the deferred list this pass; rustls-pki-types is now a separate item).

## Gates

| Gate                                                                                                | Status |
| --------------------------------------------------------------------------------------------------- | ------ |
| `cargo build --workspace --all-targets`                                                             | green  |
| `cargo build --workspace --all-targets --all-features`                                              | green  |
| `cargo build --workspace --all-targets --locked`                                                    | green  |
| `cargo nextest run --workspace --all-features` (1608 tests, 4 skipped)                              | green  |
| `cargo test --doc --workspace`                                                                      | green  |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings`                              | green  |
| `cargo fmt -p dynomite -p dynomited -p dyn-hash-tool -p dyn-encoding -p dyniak -p dyn-admin -- --check` | green  |
| `scripts/check_no_todos.sh` / `check_no_port_comments.sh` / `check_ascii.sh`                        | green  |
| `cargo deny check`                                                                                  | unchanged from `main` (pre-existing license + wildcard issues; no new diagnostics introduced). |
| `cargo audit --deny warnings --ignore RUSTSEC-2023-0071 --ignore RUSTSEC-2024-0436`                 | reduced; **all 12 wasmtime advisories cleared** (RUSTSEC-2026-0085, -0086, -0087, -0088, -0089, -0091, -0092, -0093, -0094, -0095, -0096, -0114). The `bincode 1.3` (RUSTSEC-2025-0141, unmaintained), `rustls-pemfile 2.2` (RUSTSEC-2025-0134, unmaintained), and `lru` (RUSTSEC-2026-0002, transitive via tonic) advisories remain and are deferred per the table above. |
| `mdbook build docs/book`                                                                            | not run (binary not on PATH outside `nix develop`); `scripts/check.sh` already wraps the call in `command -v mdbook` so this is consistent with the repo's CI gate. |

## Notes

- No `#[allow(...)]` annotations were added.
- No new dependencies were added.
- No `cargo public-api`-visible surface change was introduced.
  The `observability::TracerGuard` alias plus the
  `init_otlp_tracer` / `init_otlp_logger` return-type changes go
  from the trait-import-aliased `TracerProvider` /
  `LoggerProvider` names to their concrete `SdkTracerProvider` /
  `SdkLoggerProvider` siblings; both are re-exported from
  `opentelemetry_sdk` so out-of-tree callers can update their
  imports in lockstep with the OTel SDK bump.
- The `Cargo.lock` retains transitive duplicates for `socket2`
  (0.5 + 0.6) and `thiserror` (1.0 + 2.0), dictated by the
  still-pinned `tonic` and `tokio` chains.
- Author and committer normalised to `Greg Burd <greg@burd.me>`
  per AGENTS.md Section 14a.

## CVE summary

**Cleared this pass (12)**:
- RUSTSEC-2026-0085 wasmtime (panic in `flags` lifting)
- RUSTSEC-2026-0086 wasmtime (host data leakage with 64-bit tables and Winch)
- RUSTSEC-2026-0087 wasmtime (segfault on `f64x2.splat` Cranelift x86-64)
- RUSTSEC-2026-0088 wasmtime (data leakage between pooling allocator instances)
- RUSTSEC-2026-0089 wasmtime (host panic on `table.fill` Winch)
- RUSTSEC-2026-0091 wasmtime (OOB write transcoding component model strings)
- RUSTSEC-2026-0092 wasmtime (panic transcoding misaligned UTF-16 strings)
- RUSTSEC-2026-0093 wasmtime (heap OOB read transcoding UTF-16 to latin1+utf16)
- RUSTSEC-2026-0094 wasmtime (improperly masked `table.grow` Winch)
- RUSTSEC-2026-0095 wasmtime (Winch sandbox-escaping memory access)
- RUSTSEC-2026-0096 wasmtime (miscompiled aarch64 Cranelift guest heap)
- RUSTSEC-2026-0114 wasmtime (panic allocating oversized table)

**Remaining (3, all unmaintained-warnings, deferred)**:
- RUSTSEC-2025-0141 bincode 1.x (deferred: 1.x -> 2.x is API rework)
- RUSTSEC-2025-0134 rustls-pemfile (deferred: migration to rustls-pki-types is its own change)
- RUSTSEC-2026-0002 lru (transitive via tonic; deferred until tonic upgrades)
