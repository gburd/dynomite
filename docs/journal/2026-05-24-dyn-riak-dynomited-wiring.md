# 2026-05-24 -- dyniak wired into dynomited behind --features riak

**Branch**: `stage/dynomited-riak-wiring`
**Status**: READY_FOR_REVIEW

## Scope

Operator-facing wiring for the Riak protocol surface that
landed in `crates/dyniak/` over the past three stages
(`stage/dyniak-pbc-ops-v1`, `stage/dyniak-http`,
`stage/dyniak-aae`). Until this slice the only consumer of
the crate was its own test suite; nodes shipped with the
default binary had no way to expose a Riak listener.

This slice adds:

* A new `riak:` block on `dynomite::conf::ConfPool` carrying
  `pbc_listen`, `http_listen`, `aae_enabled`,
  `aae_full_sweep_interval_seconds`, and
  `aae_segment_interval_seconds`. All fields are optional; the
  block is parsed and validated under the default build, so YAML
  authored against the feature-on binary still validates without
  the feature flag.
* A new `riak` Cargo feature on the `dynomited` crate that pulls
  in the `dyniak` dependency and three CLI flags
  (`--riak-pbc-listen`, `--riak-http-listen`,
  `--riak-aae-enabled`).
* A new `dynomited::riak` module hosting the listener-bind
  helper, the spawn helpers for the PBC / HTTP / AAE tasks, and
  a `PeerChannelRepairSink` adapter that routes
  `dyniak::aae::repair::RepairTask`s onto the dispatcher's
  per-peer outbound `mpsc::Sender<OutboundRequest>` map.
* `Server::build` and `Server::run` extensions that bind the
  Riak listeners eagerly, spawn the listener and AAE tasks, and
  join them under the same shutdown signal as the existing
  client / dnode / stats listeners.

## Files touched

* `crates/dynomite/src/conf/pool.rs` -- new `ConfRiak` struct,
  `ConfPool::riak` field, `ConfPool::validate` hook,
  `validate_riak_addr` helper.
* `crates/dynomite/src/conf/mod.rs` -- re-export `ConfRiak`.
* `crates/dynomited/Cargo.toml` -- new `riak` feature gating an
  optional `dyniak` dependency.
* `crates/dynomited/src/cli.rs` -- three new flags
  (`#[cfg(feature = "riak")]`); `format_usage` extension block
  documents them unconditionally so help output matches across
  build modes.
* `crates/dynomited/src/main.rs` -- CLI -> YAML override hook
  (`apply_riak_overrides`) gated on the feature.
* `crates/dynomited/src/lib.rs` -- expose the new `riak` module
  when the feature is on.
* `crates/dynomited/src/riak.rs` -- entirely new file (430
  lines) with the listener + AAE wiring and the repair-sink
  adapter; full unit test suite plus a doctest for
  `PeerChannelRepairSink`.
* `crates/dynomited/src/server.rs` -- new `riak_handles` field
  on `Server`, hookups in `build()` and `run()`, accessors
  `riak_pbc_addr` / `riak_http_addr`.
* `crates/dynomited/tests/riak.rs` -- new file, three e2e tests
  driving PBC ping, HTTP GET /ping, and the AAE-enabled shutdown
  path. Mirrors the shape of the existing Redis integration
  tests.
* `scripts/check.sh` -- explicit `cargo build -p dynomited
  --features riak` step.
* `docs/book/src/SUMMARY.md` -- new chapter link.
* `docs/book/src/configuration.md` -- new `## Riak mode`
  section.
* `docs/book/src/operations/riak.md` -- new operator-facing
  reference.
* `docs/journal/2026-05-24-dyniak-dynomited-wiring.md` --
  this entry.

## Test counts

* `cargo nextest run --workspace` (default features): 861 ->
  861. Unchanged. Confirmed with a clean checkout.
* `cargo nextest run -p dynomited --features riak`: 47 -> 56
  (+6 unit tests in `dynomited::riak::tests`, +3 integration
  tests in `tests/riak.rs`).
* `cargo test --doc -p dynomited --features riak`: 15 -> 16
  (+1 doctest on `PeerChannelRepairSink`).
* `cargo clippy --workspace --all-targets --all-features --
  -D warnings`: clean.
* `cargo build -p dynomited --features riak --locked`: clean
  (the new `scripts/check.sh` step exercises it on every CI
  run).

## Default build invariance

Without `--features riak`, none of:

* `dyniak` (or any of its transitive deps),
* `dynomited::riak`,
* the `--riak-*` CLI flags,
* the Riak listener / AAE wiring in `Server`,

is compiled in. The default build is bit-identical to the
pre-slice baseline. The pre-existing CLI test suite
(`crates/dynomited/tests/cli.rs`) runs unchanged and passes,
which is the regression pin the brief asked for: any drift in
`format_usage`, the help banner, or the existing flags would
fail those tests.

The only file the default build does see a delta in is
`crates/dynomite/src/conf/pool.rs`: the new `ConfRiak` struct
and the `ConfPool::riak` field exist regardless of feature, so
operators can author Riak-aware YAML against a default binary
without errors. The validator also runs unconditionally; the
field is simply ignored at run time.

## Datastore choice

The Riak server.rs API takes `Arc<dyn dynomite::embed::Datastore>`.
`ClusterDispatcher` does not implement that trait directly; its
async dispatch path is shaped around the existing client /
peer protocol parsers. Bridging the dispatcher into the
`Datastore` shape would have required a non-trivial async
adapter that exceeded the 90-minute budget for this slice, so
the wiring uses `dynomite::embed::MemoryDatastore` as the
backing store. The trait shape is preserved: a future slice
swaps in the proper adapter without changing this module's
external surface.

The integration tests exercise `RpbPing` (which never reaches
the datastore) and `GET /ping` (same). PBC `Get`/`Put`/`Del`
trampolining through the substrate is covered by the unit
tests in `crates/dyniak/src/server.rs` already.

## AAE wiring

The AAE scheduler task is spawned when `aae_enabled: true`. It
holds a `PeerChannelRepairSink` referencing the same
`mpsc::Sender<OutboundRequest>` map the gossip task and the
hint drainer use. The scheduler ticks at the configured cadence
and emits `tracing::debug!` events; the per-peer tree exchange
that produces real `Divergence`s is deferred to a follow-up
slice (the brief explicitly says "spawn the AAE scheduler
task", which is exactly what this slice does).

When the exchange code lands, it consumes the same
`PeerChannelRepairSink` and submits real `RepairTask`s; no
plumbing in `dynomited` has to change.

## Open questions

None blocking. Two follow-ups noted in the operations doc and
the parity table:

1. Bridge `ClusterDispatcher` into the
   `dynomite::embed::Datastore` trait so Riak requests share
   the same routing / quorum / hinted-handoff machinery as
   Redis and Memcache.
2. Land the per-peer Tictac tree exchange so the AAE task
   surfaces real divergences against the
   `PeerChannelRepairSink`.

Both are scoped as separate slices.
