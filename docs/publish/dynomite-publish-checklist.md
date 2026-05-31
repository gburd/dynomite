# dynomite publish-readiness checklist

This document captures everything needed to push `dynomite` to crates.io.
Audit performed on branch `stage/dynomite-publish-ready` against
workspace version `0.0.1`.

## TL;DR

* `cargo build -p dynomite` -- PASS
* `cargo nextest run -p dynomite` -- PASS (906 tests, 1 skipped)
* `cargo test --doc -p dynomite` -- PASS (672 doctests)
* `cargo clippy -p dynomite --all-targets --all-features -- -D warnings` -- PASS
* `cargo fmt -p dynomite -- --check` -- PASS
* `cargo publish --dry-run -p dynomite` -- BLOCKED on path-deps not yet on crates.io
  (dyntext, dynvec, gen-fsm, throttle-core, tre-sys). The metadata is correct;
  the failure is the documented chicken-and-egg of multi-crate workspaces.

## Cargo metadata audit

The `[package]` block in `crates/dynomite/Cargo.toml` carries every
field crates.io requires:

| Field             | Value                                                              |
|-------------------|--------------------------------------------------------------------|
| `name`            | `dynomite`                                                         |
| `version`         | `0.0.1` (workspace)                                                |
| `description`     | one-line, ASCII, ends with a period                                |
| `license`         | `Apache-2.0` (workspace)                                           |
| `repository`      | `https://github.com/gburd/dynomite` (workspace)                    |
| `homepage`        | `https://github.com/gburd/dynomite`                                |
| `documentation`   | `https://docs.rs/dynomite`                                         |
| `readme`          | `README.md` (the focused embed-as-library README)                  |
| `keywords`        | `["dynamo", "distributed", "cluster", "replication", "redis"]`     |
| `categories`      | `["database-implementations", "network-programming", "concurrency"]` |
| `rust-version`    | `1.90` (workspace)                                                 |
| `edition`         | `2021` (workspace)                                                 |
| `authors`         | `["The Dynomite Rust Port Authors"]` (workspace)                   |

The `include = [...]` directive trims the published tarball to just the
sources, the kept embed-cookbook examples, and the README. The full
list of files in the tarball is captured in the journal entry.

Tarball contents (165 entries, all ASCII):

* `Cargo.toml`, `Cargo.lock`, `Cargo.toml.orig`, `.cargo_vcs_info.json`
* `README.md`
* `examples/{demo_vector_text,embedded_cluster3,embedded_custom_transport_sketch,embedded_minimal,embedded_single_node,random_slicing}.rs`
* `src/**/*.rs` (the engine source tree)

Tests, benches, and fixtures are deliberately excluded from the
tarball but remain in the repo for the workspace's own CI.

## Public surface

The brief enumerated the public items intended for embedders. Every one
is `pub` and documented in the present tree. Concrete locations:

| Symbol                             | Crate path                                |
|------------------------------------|-------------------------------------------|
| `Server`, `ServerBuilder`, `ServerHandle` | `dynomite::embed::{server, builder}` (re-exported at `dynomite::embed`) |
| `Datastore`, `MemoryDatastore`, `RedisDatastore`, `MemcacheDatastore` | `dynomite::embed::hooks` |
| `SeedsProvider`, `SimpleSeedsProvider`, `DnsSeedsProvider`, `FloridaSeedsProvider` | `dynomite::embed::hooks` |
| `CryptoProvider`, `RustCryptoProvider`, `CryptoProviderError` | `dynomite::embed::hooks` |
| `MetricsSink`, `LoggingMetricsSink`, `MetricsError` | `dynomite::embed::hooks` |
| `Transport`, `TcpTransport`, `ConnRole`         | `dynomite::embed` (also accessible at `io::reactor`)  |
| `Protocol`, `BoxFuture`            | `dynomite::embed::hooks`                  |
| `EmbedError`                       | `dynomite::embed::error`                  |
| `ServerEvent`, `EventStream`, `ConnId`, `ConnRoleTag`, `PeerDownReason`, `CloseReason`, `PeerId` | `dynomite::embed::events` |
| `DatacenterSnapshot`, `RackSnapshot`, `PeerSnapshot`, `RingSnapshot` | `dynomite::embed::snapshots` |
| `ClusterEvent`, `EventManager`, `Subscriber`, `TokenRange` | `dynomite::events` |
| `VectorRegistry`, `VectorTable`, `VectorTableInfo`, `VectorSchema`, `RegistryError` | `dynomite::vector` |
| FT.* registry / dispatch surface   | `dynomite::proto::redis::ft`              |
| `Stats`, `Snapshot`, `MetricSpec`, `Histogram`, `Aggregator`, `describe_stats`  | `dynomite::stats` |
| `DynError`, `Status`, `Sec`, `Msec`, `Usec`, `MsgId`, `SecureServerOption` | re-exported at `dynomite::` (also at `dynomite::core::types`) |

Cluster, conf, hashkit, msg, net, proto, runtime, seeds, util, crypto, io,
admin, entropy modules are all `pub` so embedders can reach internals
when they need to. Hiding any of them is out of scope for this audit:
the worker stages have been adding to these surfaces since Stage 1, and
flipping visibility would require a parallel review pass against every
test fixture and example.

## Examples kept (6) vs moved out (3)

Per the brief, "embed cookbook" examples are kept; CI/test fixture
generators are moved out so they do not bloat the published tarball.

Kept in `crates/dynomite/examples/`:

* `demo_vector_text.rs` -- canonical embed example: vector + text.
* `embedded_cluster3.rs` -- in-process three-node cluster.
* `embedded_custom_transport_sketch.rs` -- shape of a custom transport.
* `embedded_minimal.rs` -- smallest runnable embedding (5-call chain).
* `embedded_single_node.rs` -- one-node engine fronting Redis at :6379.
* `random_slicing.rs` -- random-slicing distribution example.

Moved to `crates/dynomited/examples/` (dynomited has `publish = false`):

* `gen_crypto_fixture.rs`         -- regenerates `tests/fixtures/crypto/`.
* `generate_aes_fuzz_seeds.rs`    -- regenerates fuzz seed corpus.
* `generate_hashkit_vectors.rs`   -- regenerates hashkit JSON vectors.

The classification rule used: an example stays in the published crate
only if it demonstrates an embedding pattern an external user would
plausibly study. Generators that only refresh in-tree test data are
ops tooling, not embed cookbook material.

## Path-dep status

All four direct path-deps now carry a `version = "0.0.1"` so cargo
records the version in the published crate's manifest:

```
dyntext       = { path = "../dyntext",       version = "0.0.1" }
dynvec        = { path = "../dynvec",        version = "0.0.1" }
gen-fsm       = { path = "../gen-fsm",       version = "0.0.1" }
throttle-core = { path = "../throttle-core", version = "0.0.1" }
```

One transitive path-dep was also fixed:

```
# crates/dyntext/Cargo.toml
tre-sys = { path = "../tre-sys", version = "0.0.1" }
```

The remaining workspace path-deps (`hashtree`, `dyniak`, `dyn-encoding`,
`dyn-admin`, `dyn-hash-tool`, `dynomited`, `loom-tests`, `sup`,
`fuzz`) are not in dynomite's transitive set and do not block
publish. `sup` is wired in dynomite's lib but only via an internal
runtime crate; verified `sup` is NOT in dynomite's manifest.

## Publish order

To get `dynomite` on crates.io, publish the deps first, in this
order. Each step is `cargo publish -p <crate>`:

1. `throttle-core`  -- no path-deps, no transitive blockers.
2. `gen-fsm`        -- no path-deps.
3. `tre-sys`        -- no path-deps; ships vendored TRE C source via
                       `vendor/tre/` git submodule, so `cargo publish`
                       must run after `git submodule update --init`.
4. `dynvec`         -- depends on `noxu = "3.0.2"` (already on crates.io)
                       plus only workspace deps.
5. `dyntext`        -- depends on `tre-sys` (step 3); blocked until 3 lands.
6. `dynomite`       -- depends on all of the above.

Each step requires roughly one minute for the crates.io index to make
the new version visible to subsequent `publish` invocations.

## Reproducing the dry-run

```bash
cd /home/gburd/ws/dynomite
nix develop
git submodule update --init --recursive
cargo build -p dynomite
cargo nextest run -p dynomite
cargo test --doc -p dynomite
cargo clippy -p dynomite --all-targets --all-features -- -D warnings
cargo fmt -p dynomite -- --check
cargo package -p dynomite --allow-dirty --list   # tarball preview
cargo publish --dry-run -p dynomite              # blocks on path-deps
```

The `cargo publish --dry-run` failure is expected and informative:

```
error: failed to prepare local package for uploading

Caused by:
  no matching package named `dyntext` found
  location searched: crates.io index
  required by package `dynomite v0.0.1 (...)`
```

The error disappears after the publish-order steps 1-5 land on crates.io.

## Pre-publish gotchas

* The TRE git submodule (`crates/tre-sys/vendor/tre`) MUST be
  initialised before `cargo publish -p tre-sys` runs; the build
  script reads the submodule at compile time and panics otherwise.
* The workspace pins `version = "0.0.1"`. crates.io rejects republish
  of an existing version, so the FIRST publish establishes the slot;
  bumping is required for subsequent attempts.
* `cargo publish` does not auto-tag. After all six packages land, tag
  the commit and push to both GitHub and the Codeberg mirror per
  `AGENTS.md` Section 14.
* `dynomited` and the other workspace members have `publish = false`
  or do not need to be on crates.io for the engine to be installable;
  no work is required there.
* `dynomite` ships under `Apache-2.0` (workspace license). The user
  brief mentioned `Apache-2.0 OR MIT`; the workspace is Apache-2.0
  only, and the brief said "match the rest of the workspace", so
  Apache-2.0 was kept. If dual licensing is desired, that is a
  workspace-wide change beyond the scope of this audit.

## Verification artifacts

* `cargo package -p dynomite --list`: 165 entries, ASCII only.
* Tests: 906 passing, 0 failing, 1 skipped.
* Doctests: 672 passing, 0 failing.
* Clippy: clean under `--all-targets --all-features -D warnings`.
* fmt: clean.
* Build: clean (release path not exercised in this audit;
  `scripts/check.sh` covers it on the merge gate).

## Sign-off

Once steps 1-5 of the publish order are complete, the dry-run rerun
will succeed and `cargo publish -p dynomite` is safe to run.
