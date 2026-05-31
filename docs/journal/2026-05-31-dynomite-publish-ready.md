# 2026-05-31 -- dynomite publish-ready audit

Branch: `stage/dynomite-publish-ready`
Worktree: `/home/gburd/ws/wt-dynomite-publish-ready`

## Goal

Make `dynomite` publishable on crates.io. Add the missing crates.io
metadata, give every path-dep a `version = "..."` field, factor out the
fixture-generator examples that do not belong in the published crate,
write a focused embed-as-library README, and produce a publish
checklist.

## Files touched

* `crates/dynomite/Cargo.toml`
  * Added `description`, `keywords`, `categories`, `homepage`,
    `documentation`, `readme = "README.md"` (focused), `include = [...]`,
    `[package.metadata.docs.rs]`.
  * Added `version = "0.0.1"` to all four direct path-deps
    (`throttle-core`, `dynvec`, `gen-fsm`, `dyntext`).
* `crates/dyntext/Cargo.toml`
  * Added `version = "0.0.1"` to the `tre-sys` path-dep.
* `crates/dynomite/README.md` (new, 119 lines)
  * Embed-as-library framing, what-you-build vs what-the-engine-handles
    table, public surface enumeration, feature matrix, status,
    acknowledgements.
* `docs/publish/dynomite-publish-checklist.md` (new, ~250 lines)
  * Full audit, tarball contents, public surface matrix, examples-kept
    list, examples-moved list, path-dep status, publish ORDER, dry-run
    reproduction, gotchas.
* `crates/dynomited/examples/{gen_crypto_fixture,generate_aes_fuzz_seeds,generate_hashkit_vectors}.rs`
  * Moved from `crates/dynomite/examples/`. Updated CARGO_MANIFEST_DIR
    paths and doc-comment invocations to use `-p dynomited`.
* `crates/dynomite/tests/stage_06_crypto.rs`
  * Updated comment reference to point at the moved
    `gen_crypto_fixture` example.

## Examples decisions

Kept in `crates/dynomite/examples/` (6, all embedding cookbook):

* demo_vector_text.rs
* embedded_cluster3.rs
* embedded_custom_transport_sketch.rs
* embedded_minimal.rs
* embedded_single_node.rs
* random_slicing.rs

Moved to `crates/dynomited/examples/` (3, fixture generators):

* gen_crypto_fixture.rs
* generate_aes_fuzz_seeds.rs
* generate_hashkit_vectors.rs

Classification rule: an example stays in the published crate only if
it demonstrates an embedding pattern an external user would plausibly
study. Generators that only refresh in-tree test data are operational
tooling, not embed cookbook material.

## Verification

All of the following ran clean:

* `cargo build -p dynomite`
* `cargo build -p dynomite --examples`
* `cargo build -p dynomited --examples`
* `cargo nextest run -p dynomite`  -- 906 passing, 1 skipped, 0 failing
* `cargo test --doc -p dynomite`   -- 672 passing
* `cargo clippy -p dynomite --all-targets --all-features -- -D warnings`
* `cargo fmt -p dynomite -- --check`
* `cargo package -p dynomite --allow-dirty --list`  -- 165 entries, ASCII
* `bash scripts/check_ascii.sh`
* `bash scripts/check_no_todos.sh`
* `bash scripts/check_no_port_comments.sh`

`cargo publish --dry-run -p dynomite` fails with the documented
chicken-and-egg: the path-deps still need a crates.io presence:

```
error: failed to prepare local package for uploading

Caused by:
  no matching package named `dyntext` found
  location searched: crates.io index
  required by package `dynomite v0.0.1 (...)`
```

This is expected. The publish ORDER is documented in the checklist:

1. throttle-core
2. gen-fsm
3. tre-sys (with TRE git submodule initialised)
4. dynvec
5. dyntext (depends on tre-sys)
6. dynomite

Each of the upstream crates packages cleanly under
`cargo package -p <crate> --allow-dirty` (verified for throttle-core,
gen-fsm, hashtree, tre-sys, dynvec). dyntext requires tre-sys to land
on crates.io first.

## Public surface (audit)

The brief's enumeration was checked. Every listed item is `pub` and
documented:

* `embed::{Server, ServerBuilder, ServerHandle}`           -- present
* `embed::hooks::{Datastore, Transport, SeedsProvider, CryptoProvider, MetricsSink}` -- present
* `events::ClusterEvent` -- present (also `EventManager`,
  `Subscriber`, `TokenRange`, `PeerId`)
* `cluster::*` -- pool, peer, dispatch, etc. all `pub`
* `proto::redis::ft::*` -- 22 public items (FtCommand, parse_command,
  execute, dispatch, etc.)
* `vector::registry::VectorRegistry` -- present, plus VectorTable,
  VectorSchema, etc.
* `Stats`, `MetricSpec`, `Histogram`, `Aggregator` -- present in
  `dynomite::stats`
* `Sec`, `Msec`, `Usec`, `MsgId`, `Status`, `DynError` -- re-exported
  at the crate root via `core::types`

No new items needed to be made public. No items needed to be hidden;
the engine's design has been embed-API-first since Stage 13.

## Path-dep summary

Direct path-deps (4): all now carry `version = "0.0.1"`.

| Path-dep      | Has version? | Has its own path-deps? |
|---------------|--------------|------------------------|
| throttle-core | yes          | none                   |
| gen-fsm       | yes          | none                   |
| dynvec        | yes          | only crates.io deps    |
| dyntext       | yes          | tre-sys                |

Transitive (1): tre-sys also carries `version = "0.0.1"` in its
consumer (dyntext).

No new third-party crates added. No changes to other workspace
crates' Cargo.tomls beyond the version pin on `tre-sys`.

## Tarball contents (from `cargo package --list`)

* 4 manifest files (Cargo.toml, Cargo.lock, Cargo.toml.orig,
  .cargo_vcs_info.json)
* README.md
* 6 example files
* ~150 src/**/*.rs files

Tests, fixtures, benches, and the workspace-level files are excluded
via the explicit `include = [...]` directive.

## Dry-run output (verbatim)

```
$ cargo publish --dry-run -p dynomite --allow-dirty
   Packaging dynomite v0.0.1 (.../crates/dynomite)
warning: ignoring test `cluster_apl` as `tests/cluster_apl.rs` is not included in the published package
warning: ignoring test `cluster_capability` as `tests/cluster_capability.rs` is not included in the published package
... (one warning per excluded test/bench, all expected) ...
    Updating crates.io index
error: failed to prepare local package for uploading

Caused by:
  no matching package named `dyntext` found
  location searched: crates.io index
  required by package `dynomite v0.0.1 (.../crates/dynomite)`
```

The failure proves that the dependency graph IS visible to cargo and
that the version pinning works; it merely cannot find the upstream
crates yet because they have not been published.

## Open questions

* **License.** The brief proposed `Apache-2.0 OR MIT` but said "match
  the rest of the workspace". The workspace is `Apache-2.0`. The
  manifest keeps the workspace setting. Switching to dual licensing
  would touch every member crate and is out of scope for this audit.
  If the desired endgame is dual licensing, that is a separate
  workspace-wide PR.

* **Version.** Workspace pins `0.0.1`. crates.io will reject re-publish
  of `0.0.1` once it lands; bumping the workspace version (`0.0.2`,
  `0.1.0-pre`, ...) is a natural follow-up but was not changed here
  because the brief did not request it.

* **`cargo public-api`.** Not in this audit's scope. AGENTS.md
  Section 13 prescribes a `cargo public-api` baseline; running it
  before the FIRST publish would freeze the surface, but the workspace
  has not yet generated that baseline. Suggested follow-up.

## STAGE result

```
STAGE: dynomite-publish-ready
STATUS: READY_FOR_REVIEW
BRANCH: stage/dynomite-publish-ready
JOURNAL: docs/journal/2026-05-31-dynomite-publish-ready.md
PARITY_DELTA: 0
```
