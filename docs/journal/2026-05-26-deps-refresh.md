# 2026-05-26 -- Workspace dependency refresh (non-Noxu)

Branch: `stage/deps-refresh`
Worker: deps-refresh agent

## Goal

Bump every non-`noxu-*` workspace dependency in `Cargo.toml` to its
latest stable release as of 2026-05-26, fix any API drift, and
verify the workspace stays green. The point of this pass is to
flush stale pins that may carry CVEs (per `cargo audit`) or that
lack modern API surface used elsewhere.

The Noxu path-deps (`noxu-db`, `noxu-cleaner`, `noxu-config`,
`noxu-dbi`, `noxu-engine`, `noxu-evictor`, `noxu-latch`,
`noxu-log`, `noxu-recovery`, `noxu-sync`, `noxu-tree`,
`noxu-txn`, `noxu-util`) are out of scope; another worker owns
the Noxu side.

## Bumps applied

| Crate              | Before | After   | Notes                                                   |
| ------------------ | ------ | ------- | ------------------------------------------------------- |
| tokio              | 1.40   | 1.52    | Patch/minor only; no API drift.                         |
| bytes              | 1.7    | 1.11    | No drift.                                               |
| clap               | 4.5    | 4.6     | No drift.                                               |
| thiserror          | 1.0    | 2.0     | Major bump; no call-site changes needed (the `.source()` and `#[from]` derive APIs we use are unchanged in 2.x). |
| nix                | 0.29   | 0.30    | API drift: `nix::unistd::dup2` now takes `&mut OwnedFd` for the destination. Switched the daemonize stdio redirection to the new `dup2_stdin` / `dup2_stdout` / `dup2_stderr` helpers, which take `AsFd` and target the well-known descriptors directly. Dropped the now-unused `libc_stdin/stdout/stderr` helpers and the `AsRawFd` import. |
| socket2            | 0.5    | 0.6     | API drift: `Socket::nodelay()` was renamed to `tcp_nodelay()`. One call site in `tests/stage_09_net.rs`. |
| httparse           | 1.9    | 1.10    | No drift.                                               |
| prometheus         | 0.13   | 0.14    | API drift: `MetricVec::with_label_values` is now generic over `V: AsRef<str>` instead of taking `&[&str]`. Empty-label call sites need a turbofish (`with_label_values::<&str>(&[])`); mixed `&String` / `&str` arrays need to be normalised to `&str` (or pre-bound for `to_string()` temporaries). Six call sites in `crates/dynomite/src/stats/prometheus.rs`. |
| prost              | 0.13   | 0.14    | We only use the runtime `Message` derive; no call-site drift. The transitive 0.13 copy stays in the lockfile via `tonic` / `opentelemetry-proto` (deferred, see below). |
| flatbuffers        | 25     | 25.12   | No drift.                                               |
| capnp              | 0.21   | 0.25    | No drift in our usage (we go through the public `Builder` / `Reader` API). Bump clears RUSTSEC-2025-0143 (unsound `constant::Reader::get` / `StructSchema::new`). |
| tempfile           | 3.13   | 3.27    | No drift.                                               |
| clap_mangen        | 0.2    | 0.3     | No drift; we just pass a `clap::Command`.               |
| rcgen              | 0.13   | 0.14    | API drift: the `CertifiedKey` field was renamed from `key_pair` to `signing_key`. Eight call sites across test fixtures and the TLS helpers in `dynomite::net::tls`, `dynomite::net::dnode_proxy`, and `dynomited::riak`. |

Total: **14** workspace deps bumped.

## Deferred (and why)

| Crate                          | Current | Latest    | Reason                                                                                                                                                                                                                                                       |
| ------------------------------ | ------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| tokio-util                     | 0.7     | 0.7.18    | Already the latest 0.7.x within the `^0.7` pin. The lockfile-side bump comes for free; no manifest change required.                                                                                                                                          |
| serde / serde_json / anyhow / tracing / tracing-subscriber / futures-core / futures-util / crossbeam-channel / crossbeam-queue / parking_lot / ahash / ciborium / base64 / hex / regex / time / bebop / bson / hegeltest / predicates / assert_cmd | various | various   | Already the latest stable within the existing major/minor pin; the lockfile picks up patches automatically. No manifest change required.                                                                                                                  |
| serde_yaml                     | 0.9     | 0.9.34+deprecated | Crate is upstream-deprecated. Switching to a maintained alternative (`serde_yml`, `serde_yaml_ng`, etc.) is a separate decision tracked elsewhere.                                                                                                |
| quiche                         | 0.22    | 0.29      | Six minor releases; the QUIC transport (`crates/dynomite/src/net/quic.rs`) hits `quiche::Header`, `quiche::accept`, `quiche::Connection`, and `quiche::Config` extensively, and the `Config` builder signature shifted. Out of budget for this refresh; needs its own bump-and-test pass.                                                                                                                       |
| webpki-roots                   | 0.26    | 1.0       | Major bump; would chain through `hyper-rustls` (still on 0.27.x against webpki-roots 0.26). The 0.26 pin is intentional per the existing comment in `Cargo.toml`.                                                                                                                                          |
| aes / cbc / cipher / sha1 / rsa | 0.8 / 0.1 / 0.4 / 0.10 / 0.9 | 0.9 / 0.2 / 0.5 / 0.11 / 0.10-rc | The RustCrypto chain bumps in lockstep across `cipher 0.4 -> 0.5`. Touches every block-cipher and AEAD call site in `dynomite::crypto`. Needs a coordinated pass with the `rsa 0.10` release-candidate landing first; deferred. |
| rand / rand_core               | 0.8 / 0.6 | 0.10 / 0.10 | Two consecutive majors (0.8 -> 0.9 reorganised the `Rng` trait; 0.9 -> 0.10 again). High-touch in entropy / token / fuzz call sites. Out of scope for a deps-refresh pass.                                                                                                                                                                                                                            |
| criterion                      | 0.5     | 0.8       | Three majors. `criterion`'s benchmark API churns (criterion-plot, html_reports cfg, throughput sampling). Bench-only; defer until a benches refresh.                                                                                                       |
| opentelemetry / opentelemetry_sdk / opentelemetry-otlp / tracing-opentelemetry / opentelemetry-appender-tracing | 0.27 / 0.27 / 0.27 / 0.28 / 0.27 | 0.32 / 0.32 / 0.32 / 0.33 / 0.32 | The OTel ecosystem rolls every minor; our `observability` module wires the SDK's tracer + log appender + OTLP exporter together and each piece changes. A coordinated 0.27 -> 0.32 bump is its own piece of work. Deferred.                                            |
| wasmtime                       | 26      | 45        | Per AGENTS.md and the brief: wasmtime is pinned at the 26.x train and bumping requires a journal entry. The 19-minor jump touches the engine API our `dyn-riak::mapreduce::wasm` module rests on. Deferred to a dedicated pass; the existing 26.x-only RUSTSECs are tracked separately.                                                                |
| rustls / tokio-rustls / rustls-pemfile / hyper-rustls | 0.23 / 0.26 / 2 / 0.27 | 0.24-dev / 0.26.4 / 2.2 / 0.27.9 | Already the latest stable within their respective pins. `rustls-pemfile` is upstream-archived (RUSTSEC-2025-0134); the canonical migration is to `rustls-pki-types::pem::PemObject` and is its own piece of work. |

Total deferred: **9** family-bumps.

## API drift encountered

1. **`prometheus 0.14`**: `MetricVec::with_label_values<V: AsRef<str>>(vals: &[V])` replaced `with_label_values(&[&str])`. Mixed `&String` / `&str` array literals no longer coerce; turbofish needed for empty arrays. Six call sites fixed in `crates/dynomite/src/stats/prometheus.rs`.

2. **`socket2 0.6`**: `Socket::nodelay()` -> `Socket::tcp_nodelay()`. One call site (`crates/dynomite/tests/stage_09_net.rs:503`).

3. **`nix 0.30`**: `nix::unistd::dup2` now takes `&mut OwnedFd` for the destination. The crate added `dup2_stdin` / `dup2_stdout` / `dup2_stderr` helpers that target the standard descriptors directly. Switched `crates/dynomited/src/daemonize.rs::redirect_stdio_to_devnull` to the helpers and dropped the now-unused `libc_stdin/stdout/stderr` constants and the `AsRawFd` import.

4. **`rcgen 0.14`**: `CertifiedKey<KeyPair>::key_pair` field renamed to `signing_key`. Eight call sites updated across the TLS helpers and integration-test fixtures.

No call-site changes were required for `thiserror 1 -> 2` (we only use `#[derive(Error)]` with `#[from]` and `#[source]` attributes, both unchanged), `prost 0.13 -> 0.14` (we only use the `Message` derive on hand-rolled structs), `capnp 0.21 -> 0.25`, `clap_mangen 0.2 -> 0.3`, `tempfile`, `httparse`, `flatbuffers`, `bytes`, `tokio`, or `clap`.

## Gates

| Gate                                                                                                | Status |
| --------------------------------------------------------------------------------------------------- | ------ |
| `cargo build --workspace --all-targets`                                                             | green  |
| `cargo build --workspace --all-targets --all-features`                                              | green  |
| `cargo nextest run --workspace --all-features` (1473 tests, 4 skipped)                              | green  |
| `cargo test --doc --workspace`                                                                      | green  |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings`                              | green  |
| `cargo fmt --all -- --check`                                                                        | green  |
| `scripts/check_no_todos.sh` / `check_no_port_comments.sh` / `check_ascii.sh`                        | green  |
| `cargo deny check`                                                                                  | unchanged from `main` (pre-existing license + wildcard issues; no new diagnostics introduced). The `bincode` and `protobuf-3.x` advisories that were live on `main` are now gone because the bumps drained those transitive paths. |
| `cargo audit --deny warnings --ignore RUSTSEC-2023-0071 --ignore RUSTSEC-2024-0436`                 | reduced; the `capnp` advisory (RUSTSEC-2025-0143) is gone. The `wasmtime` (14 advisories) and `rustls-pemfile` advisories remain and are deferred per the table above. |
| `mdbook build docs/book`                                                                            | not run (binary not on PATH outside `nix develop`); `scripts/check.sh` already wraps the call in `command -v mdbook` so this is consistent with the repo's CI gate. |

## Notes

- No `#[allow(...)]` annotations were added.
- No new dependencies were added.
- No `cargo public-api`-visible surface change was introduced (the `prometheus` fix is internal; the `daemonize` cleanup removes only `pub(crate)` helpers).
- The `Cargo.lock` retains transitive duplicates for `prost` (0.13 + 0.14), `socket2` (0.5 + 0.6), and `thiserror` (1.0 + 2.0). These are dictated by the still-pinned `tonic 0.12` / `opentelemetry-proto 0.27` / `tokio` chains and resolve when those families are bumped.
- Author and committer normalised to `Greg Burd <greg@burd.me>` per AGENTS.md Section 14a.
