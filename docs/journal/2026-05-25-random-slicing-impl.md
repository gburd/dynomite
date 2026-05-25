# Random-slicing implementation -- 2026-05-25

Implements the random-slicing distribution per
`docs/design/random-slicing-integration.md` (the approved
design from earlier today).

## Files touched

* `crates/dynomite/src/hashkit/murmur3.rs` -- added the
  canonical `MurmurHash3_x64_128` plus the truncated 64-bit
  variant `murmur3_x64_64`. Pinned with the public
  `MurmurHash3_x64_128("", 1)` golden vector.
* `crates/dynomite/src/hashkit/mod.rs` -- new `HashType`
  variant `Murmur3X64_64`, new `hash64(...)` companion to
  the existing `hash(...)`, re-exports of `RandomSlices` and
  `RandomSlicesError`.
* `crates/dynomite/src/hashkit/random_slicing.rs` -- new
  module: `RandomSlices` struct, three builders
  (`from_uniform`, `from_weights`, `from_sizes`), `claimant_for`
  / `index_for` lookup, `RandomSlicesError` typed via
  `thiserror`.
* `crates/dynomite/src/conf/enums.rs` -- new `Distribution`
  enum (`Vnode`, `Ketama`, `Modula`, `Random`,
  `RandomSlicing`); legacy aliases parse but emit a
  deprecation warning at runtime via `resolved_distribution()`.
  `HashType::Murmur3X64_64` now round-trips through YAML.
* `crates/dynomite/src/conf/error.rs` -- new
  `BadDistribution(String)` variant.
* `crates/dynomite/src/conf/pool.rs` -- promoted the deprecated
  `distribution: Option<String>` field to a typed
  `Option<Distribution>`, added `distribution_shadow:` field,
  added `resolved_distribution()` accessor.
* `crates/dynomite/src/cluster/datacenter.rs` -- new `RackRing`
  enum and `Rack::ring()` / `Rack::set_random_slices()` /
  `Rack::random_slices()` accessors. `clear_continuums()`
  resets the ring back to `Continuum`.
* `crates/dynomite/src/cluster/pool.rs` -- `PoolConfig` now
  carries `distribution` and `distribution_shadow`;
  `ServerPool::rebuild_ring` installs random-slicing tables
  whenever the live OR shadow distribution requires them.
* `crates/dynomite/src/cluster/dispatch.rs` --
  `collect_routable` dispatches on `Distribution`, the shadow
  comparison runs whenever both modes resolve a non-empty
  routing set, and the global `SHADOW_DISAGREEMENTS`
  `AtomicU64` counter is bumped on disagreement. New public
  helpers `distribution_shadow_disagreement_total()` and
  `reset_distribution_shadow_disagreement_total()`.
* `crates/dynomited/src/cli.rs` -- new
  `--distribution-shadow=<vnode|random_slicing>` flag and a
  matching extension-block line in the help banner.
* `crates/dynomited/src/main.rs` -- the CLI shadow flag is
  applied to `cfg.pool_mut().distribution_shadow` before
  `Server::build`. New `apply_riak_distribution_default`
  helper (only compiled with `--features riak`) flips
  the pool's default distribution to `RandomSlicing` when a
  Riak listener is configured and the operator has not set
  one explicitly.
* `crates/dyn-admin/src/commands/distribution_dump.rs` and
  `crates/dyn-admin/src/main.rs` -- new
  `dyn-admin distribution-dump` subcommand.
* `crates/dynomite/benches/random_slicing.rs` -- criterion
  bench comparing the slice-table lookup against vnode
  dispatch at 16 / 64 / 256 / 1024 peers.
* `crates/dynomite/tests/random_slicing.rs` -- 10k-key
  uniform-distribution check, pass-3 Down-peer narrative,
  shadow-counter integration test.
* `crates/dynomite/tests/random_slicing_property.rs` --
  hegeltest properties (full coverage, deterministic
  claimant, rebalance moves O(1/N), ascending bounds).
* `crates/dynomite/examples/random_slicing.rs` -- embedded
  end-to-end example.
* `docs/parity.md` -- new Deviation `D3` entry.
* `docs/book/src/configuration.md` -- new `Distribution
  modes` section.
* `docs/book/src/operations/distribution.md` -- new operator
  guide; linked from `SUMMARY.md`.
* `docs/book/src/operations/riak.md` -- note about the Riak
  distribution default flip.
* `docs/design/random-slicing-integration.md` -- new
  Section 2.1 "Replication in Dynomite vs classic Dynamo"
  per the operator's request.

## Open notes

* **Peer-state interaction**: the slice table includes every
  peer regardless of state. Down peers are masked by the
  dispatcher's existing `is_routable()` filter; the design
  matches the vnode path's contract. Documented in `D3`.
* **Shadow comparison is route-set based**: the dispatcher
  compares the post-`collect_routable` peer-index sets, so the
  counter only increments when the chosen *primary* differs,
  not when ranking changes are triggered by `n_val` capping
  downstream. Adequate for migration validation.
* **Default unchanged for non-Riak builds**: `Distribution::Vnode`
  remains the default; existing tests that build pools without
  specifying a distribution continue to pass.
* **No new dependencies** beyond what hegeltest and criterion
  already provide.

## Verification

```
cargo build --workspace --all-targets --locked
cargo build -p dynomited --features riak --all-targets --locked
cargo test -p dynomite --test random_slicing
cargo test -p dynomite --test random_slicing_property
cargo test -p dynomite --lib hashkit:: 
cargo bench -p dynomite --bench random_slicing -- --quick
```

Status: READY_FOR_REVIEW.
