# Changelog

All notable changes to the Rust port of dynomite are documented
in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project adheres to [Semantic Versioning](https://semver.org/).

The Rust port reproduces the algorithms, configuration grammar,
on-the-wire DNODE protocol, and operator-visible behaviour of
the original [Netflix Dynomite][netflix-dynomite] C engine
(BSD-licensed). The mention here is the only acknowledgement of
the upstream project outside `README.md`, `NOTICE`, and `LICENSE`.

[netflix-dynomite]: https://github.com/Netflix/dynomite

## [1.1.1] - 2026-06-30

Patch release. Bug fixes only; no API or behaviour changes for
correctly-written clients.

### Fixed

- **Transaction-written objects are now readable through the object
  API.** A value written via `POST /transactions` was stored as raw
  bytes while the HTTP GET and PBC RpbGet read paths expect the
  canonical `HttpObject` storage envelope, so reading a
  transaction-written object back via HTTP GET returned
  `500 stored object is corrupt`. The transaction write path now
  wraps values in the same envelope every other write path uses, so
  all storage methods interoperate. A round-trip regression test was
  added (the previous test only inspected the datastore handle, never
  the read path).
- **Multi-key transactions no longer abort the node process under
  load** (via noxu 6.4.2). A transaction touching adjacent keys could
  hit an illegal `RangeInsert -> Write` lock upgrade in the storage
  engine, which panicked, poisoned the transaction lock, and then
  escalated to a `process::abort()` from a destructor. noxu 6.4.2
  fixes the lock-upgrade handling and makes abort-in-`Drop`
  panic-safe. Both bugs were found by the new consistency-
  verification harness driving the real transaction endpoint.
- `anyhow` 1.0.102 -> 1.0.103 clears RUSTSEC-2026-0190
  (`Error::downcast_mut` unsoundness).

### Changed

- Dependency: noxu 6.4.1 -> 6.4.2 (the transaction crash fix above;
  no API change).

### Added

- `scripts/consistency/txn_history_workload.py`: a workload that
  drives the real dyniak transaction endpoint and records an
  Elle-style list-append history, the foundation of the
  consistency-verification initiative
  (`docs/journal/2026-06-19-consistency-verification.md`).

### Validation

- 2927 nextest workspace tests pass under `--features riak`; the
  consistency workload that deterministically aborted the process on
  6.4.1 now runs 60s / 6623 transactions at a 100% commit rate with
  the node alive and zero read anomalies; `cargo deny` and
  `cargo audit` are clean.

## [1.1.0] - 2026-06-19

Minor release. Purely additive over 1.0.0; no breaking changes.

### Added

- **The embedded server now serves the client plane over its bound
  `listen:` socket.** A client connecting to a started embedded
  node is parsed, routed through the cluster dispatcher, and -- for
  a request that resolves to the local node -- answered by the
  configured `Datastore` hook (the same hook
  `ServerHandle::inject_request` uses). Previously the embedded
  accept loop bound the socket but closed every connection, so
  `ServerBuilder::listen()` silently dropped traffic.
- **`ClusterDispatcher::with_local_datastore`** -- attaches an
  in-process `Datastore` hook so a `DispatchPlan::LocalDatastore`
  request is answered by the hook (the parsed request `Msg`,
  asynchronously) instead of being relayed over the external
  backend byte channel. Takes precedence over `with_backend` for
  local requests.

### Changed

- `ClusterDispatcher`'s `Debug` is now a manual implementation (the
  `Datastore` embedding trait has no `Debug` bound, so the
  local-datastore hook is reported as a presence flag). No public
  signature change.
- The `dyn_listen:` peer plane is unchanged: embedded multi-node
  setups forward between in-process nodes through the in-process
  registry, and cross-process peer serving remains the `dynomited`
  binary's responsibility. A custom `transport_listener` setter on
  `ServerBuilder` is still future work; custom transports today go
  through `Proxy` / `QuicProxy` directly.
- Documentation resynced: removed stale references to the
  no-longer-vendored Netflix C tree across the living docs (the
  historical journal entries keep their as-written provenance);
  corrected the embedding docs that described the old in-process-
  only accept behaviour and shipped features in future tense; the
  differential build script now sources the C oracle from a local
  upstream checkout (`DYNOMITE_C_REF`) rather than a removed
  submodule.

### Validation

- 2927 nextest workspace tests pass under `--features riak` on Rust
  1.95 (incl. a new test that connects a real RESP client to the
  embedded `listen:` socket and round-trips a request through the
  dispatcher and `Datastore` hook); 16 stateright model checks
  pass; `cargo deny` and `cargo audit` are clean.

## [1.0.0] - 2026-06-19

First stable release. The public API of the embedding crates is now
covered by SemVer. Adds first-class Riak-compatible storage,
cross-node distributed transactions, WebAssembly user-defined
map/reduce phases and hash keyfuns, durable search indexes and hinted
handoff, QUIC and Unix-socket transports, and TLA-style model
checking. Refreshes the dependency tree (noxu 6.4.1, wasmtime 46,
hegeltest 0.23) and raises the minimum supported Rust to 1.95.

### Changed (BREAKING)

- **Datastore identity renamed Redis -> Valkey.** `data_store:`
  accepts `valkey` (canonical), `memcache`, and `dyniak`; `redis`
  remains a back-compatible alias for `valkey`. The bundled programs
  follow the Valkey project (`valkey-server`, `valkey-cli`,
  `valkey-benchmark`). The RESP wire protocol, the `proto::redis`
  module, the `ReqRedis*` / `RspRedis*` message types, and the FT.*
  surface keep their names (vendor-neutral; predate the fork).
- **Minimum supported Rust is now 1.95** (was 1.90), required by
  wasmtime 46.
- **noxu 4.1.0 -> 6.4.1.** A new `OperationStatus::KeyEmpty` variant
  is handled as a miss on the point K/V paths; no other dyniak API
  change was required. The composite-key on-disk format changed
  upstream in noxu 5.0.0, which affects only environments that
  persist multi-field primary keys (dyniak does not).
- **PBC object model is now `RpbContent` / `RpbLink`** on the wire
  (Riak's published schema): `RpbPutReq.content` at tag 4 replaces
  the flat `value`, and the temporary top-level index shim is folded
  into `RpbContent.indexes`. A legacy flat-value put is rejected with
  a decode error.

### Added

- **dyniak cross-node XA two-phase commit** over the dnode peer
  plane: presumed-abort, a durable fsync'd in-doubt log, bounded
  commit retry, idempotent commit/rollback, and a cold-restart
  recovery scan that re-drives unconfirmed commits.
- **dyniak object links** persisted end to end (HTTP `Link:` headers
  and the shared PBC/HTTP storage form) and walked by the MapReduce
  `Phase::Link` link phase.
- **WebAssembly MapReduce phases** reachable through `POST /mapred`,
  and a **custom `chash_keyfun` implemented as a WASM keyfun** so an
  operator-supplied Rust crate compiled to `wasm32-unknown-unknown`
  selects the routing input at runtime (memory- and CPU-bounded).
- **Whole-bucket MapReduce inputs** via `list_keys_stream`.
- **Durable FT.* search-index persistence** (snapshots survive a
  restart) and a **durable hinted-handoff segment store** (one
  CRC-protected append log per peer, torn-tail-safe replay).
- **QUIC transport** for the dyniak PBC surface and **Unix-socket
  datastore backends** for the proxy path.
- **`dyn-admin ring-status`** now shows the full multi-peer ring
  (tokens, dc, rack, liveness) via a new `/ring` JSON endpoint.
- **stateright model checks** (`crates/model-tests`) for XA 2PC
  atomicity and liveness (with a negative control the checker
  catches), quorum acceptance, ring routing, and gossip convergence.

### Changed

- Coverage gate is tiered (core >= 95%, supporting/tool >= 75%) and
  blocking; `docs/coverage-deviations.md` carries a concrete reason
  per below-tier file. `cargo deny` and `cargo audit` are now
  blocking in `scripts/check.sh`.
- Rustdoc, man page, README, and the mdBook are resynced with the
  code; the workspace doc build is clean under `-D warnings`.

### Validation

- 2926 nextest workspace tests pass under `--features riak` on Rust
  1.95 (up from 2017 at 0.1.0); 16 stateright model checks pass;
  real-Rust-to-WASM keyfun and map-phase fixtures run against
  wasmtime 46.

## [0.1.0] - 2026-06-03

First general-availability release. Engineering Definition-of-Done
(`AGENTS.md` Section 14) audited end-to-end; 11 of 14 items met outright,
3 with documented Deviations under `docs/parity.md`.

Published crates (all at 0.1.0):

- `dynomite-engine` -- embeddable Dynamo-style replication engine
  (token-ring partitioning, gossip cluster, hinted handoff, AAE,
  CommandExtension hook for FT.* dispatch)
- `dynomite-search` -- RediSearch FT.* surface (FT.CREATE/SEARCH/INFO/
  LIST/DROPINDEX/AGGREGATE/EXPLAIN/ALTER + suggestion-dictionary
  FT.SUGADD/SUGGET/SUGDEL/SUGLEN + filter expressions + cluster-
  distributed FT.SEARCH broadcast)
- `dynomite-vec` -- vector storage + HNSW + TurboHnswIndex over
  packed turbovec codes (Fp32 / Fp16 / Int8 / 2,3,4-bit codecs)
- `dynomite-text` -- trigram + bloom + TRE-backed approximate-regex
  (K=0 via `regex`, K>=1 via TRE)
- `dyniak` -- Riak-compatible HTTP + PBC surface; ITC causality
  clocks (replaces DvvSet/Vclock; deliberate divergence from
  upstream Riak documented under `docs/parity.md` Ambiguities)

Validation:

- 2017 nextest workspace tests pass under `--features riak`
- Stage 16 chaos hour: 1,781,193 ops across 4 hosts (arnold, floki,
  meh, nuc) with 92.22% aggregate success and zero invariant
  violations under all-fault-classes mode
  (`dist/chaos-reports/v0.1.0/multi-host-pass-stage16-20260602-175558Z.md`)
- 1h fuzz soak per target: 6 of 7 clean (3.7 billion runs total);
  `dnode_parse` finding (oversized length-field OOM) fixed; artifact
  preserved as regression seed
- QUIC differential conformance suite: 3 tests pass over QUIC
  transport (mirrors the TCP scenarios)
- Coverage gate: workspace measured via `cargo llvm-cov`; sub-95%
  files documented as Deviations in `docs/parity.md`
- Public API baselines committed under `dist/public-api/` for 11
  crates
- mdbook builds clean; embedding examples compile and run
- Dual-CI green on Codeberg (Forgejo Actions) + GitHub Actions on
  the same commit through `scripts/check.sh`

Milestones since pre-1.0 series:

- ITC migration replaces DvvSet/Vclock for every causality path
  (Riak protobuf context-blob bytes now carry `Itc::encode()`
  output; deliberate divergence from upstream Riak)
- `dyniak-bench` 0.0.1 -- a Rust port of Basho's `basho_bench` with
  HdrHistogram stats and plotters-rendered SVG graphs
- Network-partition / clock-skew / disk-full chaos modes wired
  into `chaos-injector.sh` (selected via `MODE_FAULTS` env knob)
- Differential-rig phase 5: chaos faults applied to BOTH the Rust
  and the C reference proxies in lockstep when
  `INJECT_C_PROXY_TOO=1` is set
- Stage 16 commit-author normalisation via `git filter-repo`: every
  commit on `main` is now authored by `Greg Burd <greg@burd.me>`

Known deferrals (each documented in `docs/parity.md` or a journal
entry):

- Network + clock fault classes are wired into the injector but
  require operator-scope privileges (`tc` for netem, `faketime`
  for clock skew) and were unrunnable on the test cluster; the
  injector logs `injector_classes` with the runnable subset and
  the chaos hour ran with process + disk faults active.
- Coverage Deviations for 11 sub-95% files (mostly trait
  default-impls, error paths only reachable under specific
  platform conditions, and loom-only test entry points).

## [Unreleased]

Stage 16 deliverables (final stage):

- 1-hour chaos / punishment / stress test at
  `crates/dynomite/tests/stage_16_chaos.rs`. Smoke variant
  (60 s) runs in nextest under `--features chaos` plus
  `CHAOS_DURATION_SECS=60`; production-mode 1-hour run is the
  manual pre-tag gate the lead executes before the v0.1.0
  signed tag.
- Failure-injection helpers under `scripts/netem/`:
  `partition_dc.sh`, `slow_peer.sh`, `flap.sh`, `gc_pause.sh`,
  `clock_skew.sh`. All scripts no-op cleanly when
  `CAP_NET_ADMIN` is missing.
- Distribution packaging under `dist/`: systemd unit, Docker
  image (multi-stage, rust:1.90-bookworm builder ->
  debian:bookworm-slim runtime), deb / rpm hook stubs, HOWTO.
- Dual-platform CI: `.forgejo/workflows/ci.yml` mirrors
  `.github/workflows/ci.yml`; both invoke the single
  `scripts/check.sh` source of truth.
- `scripts/check_clean.sh`: cleanup-sweep gate enforcing the
  AGENTS.md Section 14c discipline (no agent cruft, no
  committed gitignored content, no committed
  `target/chaos/<run-id>/` data, no committed
  `crates/fuzz/{corpus,artifacts}` data).
- mdBook polish: `docs/book/src/operations/chaos.md`,
  `docs/book/src/operations/release.md`, refreshed
  `SUMMARY.md`.

## [0.1.0] - 2026-05-19

Initial release of the Rust port. Drop-in replacement
(`dynomited` binary + embeddable `dynomite` library crate)
for the C engine, with feature parity validated by the Stage
14 conformance suite and stress-tested by the Stage 16 chaos
run.

### Added

#### Stage 0 - Foundation

- Workspace layout (`crates/dynomite`, `crates/dynomited`,
  `crates/dyn-hash-tool`, `crates/fuzz`).
- Nix flake pinning Rust 1.90.0 and the dev-shell toolset.
- Shared lints (`forbid(unsafe_code)`, `pedantic`,
  `rust_2018_idioms`).
- C-symbol parity matrix scaffold at `docs/parity.md`.
- `scripts/check.sh` single-source-of-truth CI gate.
- Hygiene scripts: `check_no_todos.sh`,
  `check_no_port_comments.sh`, `check_ascii.sh`.

#### Stage 1 - Core types and process scaffolding

- `dynomite::core::{context, loop_, types, signal, log,
  ring_queue, msg_index, setting}` mirroring the reference
  engine's `dyn_core.{c,h}`, `dyn_signal.{c,h}`,
  `dyn_log.{c,h}`, `dyn_ring_queue.{c,h}`.
- `Status` / `DynError` taxonomy replacing `rstatus_t`.
- Cross-thread C2G/G2C ring queue built on
  `crossbeam-channel`.
- Histogram primitives mirroring `dyn_histogram.{c,h}`.

#### Stage 2 - I/O

- `io::mbuf` pooled message buffer (`dyn_mbuf.{c,h}`),
  `io::cbuf` SPSC ring buffer (`dyn_cbuf.h`), `io::reactor`
  tokio-driven equivalent of the C epoll loop. Read/write
  vectors land in lockstep with the reference paths so the
  Stage 8 wire-format parsers can be plugged in without
  reshuffling lifetimes.

#### Stage 3 - Hashkit

- `hashkit::{md5, fnv1, fnv1a, hsieh, jenkins, murmur,
  one_at_a_time}` byte-identical to the C `dyn_hashkit_*`
  family.
- `hashkit::{ketama, modula, random}` continuum builders,
  `DynToken` (mirrors `dyn_token`) with the integer-bignum
  parsing semantics the C engine relies on for ring math.
- 1024-case property-test soak under
  `crates/dynomite/tests/stage_03_properties.rs`.

#### Stage 4 - Configuration

- `conf::{Config, ConfPool, ConfListen, ConfServer,
  ConfDynSeed, SecureServerOption, ConsistencyLevel,
  DataStore}` parsed via `serde_yaml` with strict
  `deny_unknown_fields`.
- All 30+ tunable defaults from `dyn_conf.h` lifted into
  `conf::pool::defaults`.
- Validators reproducing every `conf_validate_*` arm:
  numeric ranges, mbuf size alignment, IPv6 listen syntax,
  bracketed addresses, weight column.

#### Stage 5 - Stats

- `stats::{Stats, Snapshot, PoolStats, ServerStats,
  ServiceInfo, MetricSpec, PoolField, ServerField,
  Histogram}` with the per-pool, per-server, per-DC, and
  per-rack tables matching `dyn_stats.{c,h}`.
- HTTP / 1.1 `/info`, `/describe`, `/peer/state`, and
  POST mutator endpoints on the stats listener; the JSON
  shape is byte-identical to the C engine on the
  shared corpus.
- `describe_stats()` machine-readable schema for embedders.

#### Stage 6 - Crypto (RustCrypto migration)

- AES-128-CBC payload encrypt/decrypt via `aes` + `cbc`
  with PKCS#7 padding (parity with the C
  `EVP_aes_128_cbc()` path).
- RSA OAEP (SHA-1, MGF1-SHA-1) wrap / unwrap of session
  keys via the `rsa` crate, replacing the OpenSSL ABI
  use in `dyn_crypto.c`.
- PEM loader (`crypto::pem`) honouring the bundled
  fixtures byte-for-byte.
- Migration to a pure-Rust crypto stack to remove the
  symbol clash with `quiche`'s bundled BoringSSL (Stage 9
  blocker resolution).

#### Stage 7 - Messages and DNODE codec

- `msg::{Msg, MsgType, MsgQueue, MsgIndex, RequestState,
  ResponseMgr}` mirroring `dyn_message.{c,h}`,
  `dyn_request.c`, `dyn_response.c`,
  `dyn_response_mgr.{c,h}`.
- 179 `MsgType` variants exhaustively enumerated to match
  `MSG_REQ_*` / `MSG_RSP_*` / `MSG_DNODE_*`.
- DNODE codec: header (mbuf magic, version, flags, length,
  msg-id, type, dc, rack, sender, msg-seqno, parent-id,
  consistency, payload-checksum), payload framing,
  fragment / coalesce, write-replication policy.

#### Stage 8 - Protocol parsers

- `proto::redis::{request, response}` and
  `proto::memcache::{ascii, binary}` parsers driven by a
  resumable byte-by-byte state machine matching the C
  switch graphs.
- 10 repair Lua scripts at
  `crates/dynomite/src/proto/redis/lua/` byte-identical
  to the C `repair-store/lua/*.lua` set; loaded via
  `EVAL` so server-side parity is preserved.
- Differential corpus parity for both protocols.

#### Stage 9 - Transports and connection FSMs

- `net::{conn, conn_pool, client, proxy, dnode_listener,
  dnode_client, dnode_peer_client, dnode_peer_server,
  auto_eject, quic}` covering the 6 connection roles from
  the C engine.
- TCP and (optional) QUIC listeners; both IPv4 and IPv6.
- TLS payload protection on the peer plane (matching
  `dyn_dnode_peer.c`).
- `Dispatcher` trait seam consumed by Stage 10.

#### Stage 10 - Cluster, gossip, seeds, dispatcher

- `cluster::{pool, datacenter, rack, peer, snitch,
  gossip, dispatch}` plus `seeds::{Provider, Static,
  Florida}` for live peer discovery.
- Gossip round driver (period, fanout, suspect / DOWN
  failure detection) reproducing the C `dyn_gossip.c`
  state machine.
- Cluster-aware dispatcher: DC-quorum, rack-quorum, and
  DC_EACH_SAFE_QUORUM fan-out; fragmenter for multi-key
  Redis ops; coalescer aligning with `dyn_response_mgr`.
- HTTP florida client built on `httparse` plus tokio.

#### Stage 11 - Entropy reconciliation

- `entropy::{send, receive, util}` covering
  `dyn_entropy_snd.c`, `dyn_entropy_rcv.c`, and
  `dyn_entropy_util.c`.
- Snapshot pipeline driven over RESP (no
  `redis-cli`-shellout dependency).
- Pluggable `SnapshotSource` / `SnapshotSink` so embedders
  can route the stream wherever they like.
- Six documented deviations versus the broken C reference
  (PKCS#7 padding, length-prefixed chunks, unified
  framing, `BGREWRITEAOF` over RESP, throughput throttle
  removal, key/IV honoured) all entered in
  `docs/parity.md` and pinned by tests.

#### Stage 12 - dynomited binary

- `crates/dynomited` workspace member with the
  `dynomited` binary, manpage at `dynomited(8)`, and a
  `gen-man` helper binary.
- Full CLI (`-c`, `-d`, `-p`, `-t`, `-v`, `-D`,
  `--describe-stats`, `--version`, `--help`) parity with
  the C `dynomite` argument table.
- Daemonization (single `unsafe { fork() }` block,
  documented in `docs/journal/allowances.md`),
  pidfile management, signal table (SIGTERM, SIGINT,
  SIGHUP, SIGUSR1, SIGUSR2).
- Integration test (`crates/dynomited/tests/integration.rs`)
  spawning real redis-server + dynomited and driving a
  Redis SET / GET / QUIT round trip.

#### Stage 13 - Embedding API

- `dynomite::embed::{Server, ServerHandle, ServerBuilder,
  ServerHooks, ServerEvent, EventStream, Datastore,
  SeedsProvider, CryptoProvider, MetricsSink,
  TransportListener}`.
- SemVer-hardened with `#[non_exhaustive]` on every
  builder, options struct, and event variant; only the
  builder methods are stable surface.
- Examples under `crates/dynomite/examples/` (sidecar,
  custom datastore, crypto provider override, metrics
  sink).
- `cargo public-api` baseline committed under
  `docs/api/public-api.txt`.

#### Stage 14 - Conformance and regression suites

- Conformance harness at
  `crates/dynomited/tests/conformance.rs` driving real
  dynomited clusters across the canonical scenarios
  (single-node, three-node, multi-DC, QUIC).
- Differential rig at
  `crates/dynomited/tests/differential.rs` recording
  divergences against an optional C reference build.
- Lua repair script regression at
  `crates/dynomite/src/proto/redis/lua/tests.rs`.

#### Stage 15 - Fuzz, bench, coverage

- `crates/fuzz/`: 7 cargo-fuzz targets (`conf_parse`,
  `proto_redis_parse`, `proto_memcache_parse`,
  `dnode_parse`, `crypto_aes_decrypt`,
  `entropy_chunk_parse`, `gossip_parse`); deduplicated
  regression seeds under `crates/fuzz/seeds/`.
- 7 criterion micro-benches (`parsers`, `mbuf`,
  `hashkit`, `tokens`, `dnode`, `crypto`, `quorum`)
  plus the macro-throughput harness
  (`crates/dynomite/benches/macro_throughput.rs`).
- Coverage gate at `scripts/coverage_gate.sh` on a
  95% line / branch / function threshold; per-file
  deviations catalogued in `docs/coverage-deviations.md`.
- 1024-case Stage 15 property-test soak under
  `crates/dynomite/tests/stage_15_properties.rs`.

#### Stage 16 - Release, packaging, chaos verification

- `crates/dynomite/tests/stage_16_chaos.rs` 1-hour chaos
  test exercising every `dyn_state_t` variant under
  concurrent failure injection.
- `dist/` packaging tree (systemd unit, Docker image,
  deb / rpm hooks, HOWTO).
- `.forgejo/workflows/ci.yml` mirroring
  `.github/workflows/ci.yml`; both invoke
  `scripts/check.sh`.
- `scripts/check_clean.sh` cleanup-sweep gate.
- mdBook reference complete; chaos and release-process
  pages added.

### Cumulative deviations

Every intentional deviation versus the C reference is logged
in `docs/parity.md`. The five cumulative classes summarised
for the v0.1.0 release:

1. **Crypto stack**: pure-Rust RustCrypto (`aes`, `cbc`,
   `rsa`, `sha1`, `rand`) replaces OpenSSL bindings; lifts
   the symbol clash with `quiche`'s BoringSSL and removes
   the long-lived global `EVP_CIPHER_CTX *` re-key races.
   Per-call `Encryptor` / `Decryptor` instances are
   functionally equivalent and lock-free. RSA padding is
   PKCS#1 OAEP (SHA-1) matching the C
   `RSA_PKCS1_OAEP_PADDING` choice.
2. **Entropy reconciliation**: the C send and receive paths
   ship two different wire formats and rely on an external
   Lepton/Spark cluster to bridge them; the Rust port
   unifies the framing on the file-stream shape with PKCS#7
   padding and length-prefixed chunks, exposes the
   record-level interpretation through the embedding
   `SnapshotSink`, drops the hardcoded throughput throttle,
   and honours the `recon_key.pem` / `recon_iv.pem` file
   contents the C loader silently discarded.
3. **Configuration parser**: `serde_yaml` replaces the
   hand-rolled `conf_handler` / `conf_token_*` /
   `conf_event_*` machinery. Numeric ranges, IPv6 bracketed
   listen syntax, server-row weights, and explicit zero
   connection counts behave per the parity matrix; unknown
   keys are rejected (`#[serde(deny_unknown_fields)]`)
   instead of silently ignored.
4. **Connection ownership**: the C engine maintains an
   ad-hoc ref/unref dance across `dyn_connection.c`,
   `dyn_connection_pool.c`, `dyn_server.c`, and the
   listener layer. The Rust port uses `Arc<Conn>` plus
   ownership transfer on accept; `auto_eject` is lifted
   into its own module so the failure detector is not
   scattered across `dyn_server.c` / `dyn_dnode_peer.c`.
   The DNODE peer-client decrypt error is collapsed to a
   single opaque variant at the boundary to close the
   Vaudenay padding-oracle surface.
5. **Coalesced messages and message-type taxonomy**:
   `DynErrorCode::BadFormat.message()` returns a stable
   `"Bad message format"` literal instead of falling
   through to `strerror(8)` (`"Exec format error"` on
   Linux) the way the C `dn_strerror` switch's default arm
   does. `MsgType` exposes 179 explicit variants with
   compile-time exhaustiveness.

### Testing baseline at v0.1.0

- 608 nextest tests at default features.
- 664 nextest tests at `--all-features`.
- 566 doctests.
- 1024-case property-test soak per Stage 3 / 15
  properties suite.
- 7 cargo-fuzz targets, 60 s smoke per target in
  CI, 1 h soak target per release.
- Coverage gate: 95% line / branch / function threshold
  with documented per-file deviations under
  `docs/coverage-deviations.md`.
- Chaos test: 1-hour production-mode run preceding every
  release tag; smoke 60 s variant available under
  `--features chaos` for development cycles.

[Unreleased]: https://github.com/gburd/dynomite/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/gburd/dynomite/releases/tag/v0.1.0
