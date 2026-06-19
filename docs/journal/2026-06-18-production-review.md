# Production-readiness review (2026-06-18)

A customer has started building on dynomite. This is the coordinated
audit to take the workspace to production grade across:

1. Tidy / professional structure.
2. Comments that accurately describe the code (no aspirational,
   misleading, or incomplete comments).
3. Documentation in sync with the code (rustdoc, mdBook, README).
4. Polished man pages.
5. Tests that genuinely validate functionality and stability.
6. Coverage gates: >= 95% for core components, >= 75% for the rest.
7. Property-based testing via hegel (hegeltest).
8. stateright for TLA-like model checking of the distributed protocols.

## Baseline findings (lead survey)

- Workspace: 16 member crates, ~130k LOC. Structure is sound; fixtures
  and fuzz are correctly excluded from the host build graph.
- `missing_docs` is `warn` in core crates; under `RUSTFLAGS=-D warnings`
  pub items are documented.
- **Doc build is NOT clean**: broken intra-doc links fail
  `RUSTDOCFLAGS=-D warnings cargo doc`:
  - `crates/gen-fsm/src/transition.rs:11` -> `crate::EventType::Enter`
  - `crates/dyn-sup/src/atomics.rs:29` -> `loom::sync::atomic`
- **Man page is stale** (`crates/dynomited/man/dynomited.8` +
  `src/bin/gen-man.rs`): version `v0.0.1` (now 0.1.0), "Redis is
  currently the primary backend ... Memcached partially implemented"
  (now Valkey + memcache + dyniak first-class), and a leaked rustdoc
  link `[`Self::show_version`]` rendered verbatim in an option help.
- stateright: NOT yet a dependency. To be added for model checking.
- hegel: already used in 23 test files; expand coverage to the
  protocols/invariants that lack it.
- Coverage gate exists (`scripts/coverage_gate.sh`) but runs with
  `|| true` (non-blocking). Production grade requires it to block.

## Workstreams (parallel, non-overlapping ownership)

- W1 docs+manpage: fix broken intra-doc links; regenerate + polish the
  man page; sync README + mdBook + lib.rs top docs with current
  reality (Valkey rename, data_store {valkey,memcache,dyniak}, the
  feature set shipped this cycle: links, cross-node XA, custom keyfun,
  FT.* persistence, durable handoff, unix sockets).
- W2 comment-accuracy: audit every non-aspirational-language hit in
  src; rewrite comments that are aspirational/misleading/incomplete to
  describe what the code actually does.
- W3 stateright: add stateright model checks for the distributed
  protocols (XA 2PC safety/atomicity, quorum/consistency decision,
  ring routing determinism, gossip convergence).
- W4 property tests: close hegel property-test gaps on the parsers,
  hash/token arithmetic, mbuf, quorum, and the new features.
- W5 coverage: measure, raise to >=95% core / >=75% rest, make the
  gate blocking with per-tier thresholds.

The lead fixes shared/cross-cutting files (Cargo.toml, check.sh,
parity.md) on merge.

## Coverage baseline (measured 2026-06-18, --features riak)

Workspace TOTAL: 82.54% line / 80.38% function / 81.86% region.

Per-crate line coverage:
- dyn-encoding 93.3, dynomite-text 93.8, throttle-core 92.5,
  dynomite-vec 90.7, dyn-hashtree 91.0, dyniak 90.1, dyn-sup 89.5,
  tre-sys 87.0, dyn-admin 84.4, dynomite 82.0, dyn-hash-tool 78.4,
  dynomited 68.9, dynomite-search 67.1, dyniak-bench 64.3,
  gen-fsm 54.3.

Core low spots driving the gap:
- proto/redis/parser.rs 54.7%, commands.rs 51.0%, repair/make.rs 25%,
  repair/reconcile.rs 55%, stats/mod.rs 65.8%.
- gen-fsm: transition.rs 0%, action.rs 20.8%, handler.rs 50%.
- dynomite-search 67% (FT.* persistence path undertested).
- dynomited/main.rs 24.8%, daemonize.rs 6.8% (process entry points --
  legitimate Deviation candidates), server.rs 61.5%.

Tier policy for the blocking gate:
- Core (engine proto/cluster/io/hashkit/crypto, dyniak storage/proto):
  >= 95%.
- Supporting (search, gen-fsm, sup, encoding, text, vec, hashtree,
  throttle): >= 75% (push high where cheap).
- Tools/benches (dyniak-bench, dyn-hash-tool, dyn-admin, the dynomited
  process-entry shims main.rs/daemonize.rs): >= 75% with documented
  Deviations for the un-coverable process bootstrap.
