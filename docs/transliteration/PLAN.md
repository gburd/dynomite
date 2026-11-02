# Transliteration Plan: C Dynomite -> Rust, function for function

Status: ACTIVE. Supersedes `PLAN.md` for all work on the C-compatible core.
Owner: Greg Burd. Authority for deviations: `docs/transliteration/deviations.md`.

## 0. Why this plan exists

The project goal was always "functionally identical" to Netflix Dynomite.
A 2026-10 audit found the shipped Rust engine is not that:

* 840 C function names (868 definitions) in `~/src/dynomite/src` at
  `2046b05`; `docs/parity.md` mentions 699 of them, 141 never.
* About 82 parity rows still read "deferred to Stage N".
* Verified behavioural gaps: the stats/admin HTTP surface (C: 25+ commands,
  Rust: 4 paths); local node state gating (STANDBY / WRITES_ONLY /
  RESUMING); `g_timeout_factor`; timestamp-based read repair (C repairs 13
  commands via Lua add/rem-set scripts, Rust repairs single-key GET by vote);
  `read_repairs_enabled` is parsed but does not gate the Rust dispatch path;
  ported repair functions with no production caller; `msg_clone`,
  `msg_tmo_*`, `craft_ok_rsp`, `redis_copy_bulk` and others absent.
* The C-vs-Rust differential rig ran once (2026-05-24, 101 commands, single
  node) and is non-blocking. `scripts/check_parity.sh` was never written.
* Every recorded deviation was self-authorised by an agent.

This plan replaces the engine's C-compatible core with a transliteration:
every C function, every operational feature, every C test, reproduced in
Rust with the C logic, checked against the C binary, and ledgered.

## 1. Definitions

* **Transliteration**: for each C function, a Rust function in the module
  that mirrors the C file, carrying the same control flow, the same
  branches, the same error and edge behaviour, the same constants. Names
  follow the C name in snake_case (already the C style). Line-for-line where
  the C is line-level logic; a structural port where an approved deviation
  replaces the substrate (section 3).
* **Oracle**: the C reference binary and C object code built from
  `~/src/dynomite` at the pinned commit, used to produce expected outputs.
* **Ledger**: `docs/transliteration/ledger.tsv`, one row per C symbol,
  machine-generated from the C tree and machine-checked against the Rust
  tree by `scripts/check_parity.sh`.
* **Authorised deviation**: a row in `docs/transliteration/deviations.md`
  with `Authorized-by: Greg Burd <date>` and passing evidence that it is
  better than the C behaviour on the axis it changes. Agents may propose;
  only the owner authorises.

## 2. Ground truth and pins

* C reference: `~/src/dynomite`, commit `2046b05` (upstream `master` tip,
  verified 2026-10: zero newer upstream commits). `DYNOMITE_C_REF` points
  here. The tree stays out of this repository (removed in `2561d13`).
* Rust toolchain: `rust-toolchain.toml` (1.95.0).
* Storage: noxu 7.11 now; noxu 8.x when the owner announces it (section 9).
* Hosts: this host for development and the oracle; AWS burner account
  `chef` (842964983020) for Debian/AL2023 qualification, multi-node
  differential, chaos and benchmarks.

## 3. Deviation catalogue

The full ledger with evidence lives in `deviations.md`. Summary of the
owner's 2026-10 authorisations:

**Substrate (authorised, structural):** the C event loop
(`event/dyn_epoll.c`, `dyn_kqueue.c`, `dyn_evport.c`, `dyn_core.c`'s
`core_loop`), `dyn_dict.c`, `dyn_rbtree.c`, `dyn_array.c`, and connection
ref-counting are replaced by tokio and Rust collections / ownership. Every
C function in those files still gets a ledger row naming its Rust
replacement and a behavioural test of the observable effect (ordering,
timeouts, eviction, iteration order where the C exposes it).

**Additions (authorised, must stay strictly additive):** dyniak (noxu), the
noxu datastore, QUIC, vector/text search, CRDTs, cross-node transactions,
hinted handoff, anti-entropy, random slicing, the embedding API. "Strictly
additive" means: with the C configuration (no Rust-only keys), the binary
behaves as the C binary. Each addition is off by default or reachable only
through a Rust-only config key, verified by the differential suite running
the C configs unchanged.

**Behavioural deviations (pending review, section 8):** about 40 entries in
`docs/parity.md` "Deviations" were agent-recorded. Each is re-examined:
keep (with evidence and owner sign-off), or revert to the C behaviour.

## 4. Gates (what "done" means for any symbol)

A C symbol is DONE only when all of these hold, enforced by tooling:

1. **Ledger row** `ported` (or `replaced:<rust path>` for authorised
   substrate, or `deviation:<id>` with an authorised deviation).
2. **Rust home exists** at the path the row names (checked by
   `check_parity.sh` grepping for the definition).
3. **Oracle test**: a test that runs the same inputs through the C function
   (via the C FFI test harness, section 5.2) or the C binary (section 5.3)
   and asserts identical output, OR, for substrate replacements, a
   behavioural test of the C-observable effect.
4. **C test ported**: any C test that exercises it (`src/dyn_test.c`,
   `test/*.py`, `scripts/dynomite/*.py`) has a Rust counterpart.
5. **No production orphan**: the Rust function has a production caller, or
   the ledger row says why not (the C function was itself dead code, with
   the C call-graph evidence).

The whole-port gate adds: differential suite strict and green (section 5.3),
performance gate green (section 6), check.sh green.

## 5. Verification architecture

### 5.1 Ledger tooling (`scripts/check_parity.sh`)

* `scripts/transliteration/c_inventory.sh`: universal-ctags over
  `$DYNOMITE_C_REF/src` (excluding generated Makefiles) producing every
  function, macro, struct, enum, enumerator, global variable, and stats
  metric with file and line. Output: `ledger.tsv` skeleton.
* `ledger.tsv` columns: `kind`, `c_symbol`, `c_file`, `c_line`, `status`
  (`todo|ported|replaced|deviation|dead`), `rust_path`, `oracle_test`,
  `c_test`, `notes`.
* `scripts/check_parity.sh` (CI gate): regenerate the inventory, fail on
  any C symbol missing from the ledger, any row whose `rust_path` does not
  resolve, any `deviation` row without an authorised entry, and (once a
  stage is closed) any `todo` row in that stage's files. Prints a progress
  summary per C file.

### 5.2 Function-level oracle (C FFI test harness)

* `crates/c-oracle/` (dev-dependency only, `publish = false`, test-only):
  `build.rs` compiles the pure-logic C translation units from
  `$DYNOMITE_C_REF` with `cc` into a static library and exposes them via a
  thin `extern "C"` declaration module.
* Pure-logic targets (no event loop, no sockets): hashkit (all hash
  functions, ketama, modula, random, token arithmetic), `dyn_string`,
  `dyn_util` number parsing, `dyn_crypto` (AES/RSA), redis and memcache
  parsers (`redis_parse_req/rsp`, `memcache_parse_req/rsp`), the dnode
  parser (`dyn_parse_core`, `dmsg_write`), redis repair script builders,
  histogram, conf parsing of YAML strings, response-mgr quorum decisions.
* For each, a `hegeltest` property: random inputs to C and Rust, outputs
  must match byte for byte. Shrinking finds the minimal divergent input.
* The `c-oracle` crate is gated on `DYNOMITE_C_REF` being set; CI hosts with
  the C tree run it as a hard gate. It is the only `unsafe` in the
  workspace and lives in a test-only crate (justified in
  `docs/journal/allowances.md`).

### 5.3 System-level oracle (differential suite)

* Revive and extend `crates/dynomited/tests/differential.rs`:
  * Corpus covers all 134 redis and 13 memcache request types, error paths,
    pipelining, large payloads, multi-key commands, scripts, hash tags.
  * Multi-node: 1, 3 (one DC, three racks), and 2x3 (two DCs) clusters,
    every consistency level (DC_ONE, DC_QUORUM, DC_SAFE_QUORUM,
    DC_EACH_SAFE_QUORUM), with read repair on and off.
  * Admin/stats HTTP: every `/` command, byte comparison after normalising
    timestamps and uptime.
  * Gossip and failure: kill/restart a node, compare ring views, eject
    timing, and recovery between C and Rust clusters.
  * CLI: every flag, including `-t` config test output and `-D`
    describe-stats output.
* `DIFFERENTIAL_STRICT=1` becomes the default. A divergence fails the gate
  unless it matches a normaliser tied to an authorised deviation.
* Mixed clusters (C and Rust nodes in one ring) for wire compatibility of
  the DNODE plane, gossip, and entropy.

### 5.4 C tests ported

* `src/dyn_test.c` (10 test functions: rsa, aes, aes_msg, msg_recv_chain,
  tailq, ...) -> `crates/dynomite/tests/c_dyn_test.rs`, one Rust test per C
  test, same inputs.
* `test/func_test.py`, `dual_run.py`, `load.py`, `cluster_generator.py`,
  `dyno_cluster.py`, `scripts/dynomite/dyn_redis_test.py`,
  `dyn_mc_test.py`, the YAML request fixtures -> Rust under
  `crates/dynomited/tests/c_func/`, scenario for scenario. The existing
  `conformance/` adaptation is audited against these and completed.

## 6. Performance gate ("as well or better on all dimensions")

Measured on chef EC2, same instance type, same datastore (redis-server and
memcached from the distro), C and Rust built release, A/B interleaved, n>=5
per point, report median and variance, STABLE/NOISY verdict per the
`pg-benchmark-rigor` discipline.

Dimensions and the pass condition (Rust >= C, or within noise):

| Dimension | Workload | Metric |
|---|---|---|
| Throughput | redis-benchmark / memtier GET, SET, mixed, pipelined 1/16/64 | ops/s |
| Latency | fixed-rate at 50/80/95% of C max | p50, p99, p99.9 |
| Multi-node | 3-node and 2x3 at each consistency level | ops/s, p99 |
| Memory | steady state and peak under load | RSS |
| CPU | ops per core-second | CPU efficiency |
| Connections | 10k idle client conns, churn rate | RSS, accept/s |
| Startup / recovery | cold start, node rejoin | time to serving |
| Failure | partition, peer kill (netem scenarios) | availability, recovery time |

A regression blocks the release of the affected stage. Results committed to
`dist/bench-reports/transliteration/`.

## 7. Work breakdown

Stages follow the C dependency order. Each stage: generate ledger rows,
transliterate, write oracle tests, port C tests, run the gates, review
(AGENTS.md section 9), merge. A stage is closed when its files have zero
`todo` rows.

| Stage | C files | Notes |
|---|---|---|
| T0 | tooling | `c_inventory.sh`, `ledger.tsv`, `check_parity.sh`, `c-oracle` crate skeleton, `build_cref.sh` refresh, chef bootstrap script |
| T1 | `dyn_types`, `dyn_string`, `dyn_util`, `dyn_log`, `dyn_signal`, `dyn_setting`, `dyn_histogram`, `dyn_cbuf.h`, `dyn_queue.h` | logging levels incl. SIGUSR1/2 log-level and SIGTTIN/TTOU, SIGSEGV stacktrace |
| T2 | `hashkit/*` | oracle-pinned every algorithm; replaces the hand-walked vectors |
| T3 | `dyn_mbuf`, `dyn_array`, `dyn_dict*`, `dyn_rbtree`, `dyn_ring_queue`, `dyn_task` | substrate rows: replaced, with behavioural tests |
| T4 | `dyn_conf` | every key, every validation branch, `-t` output, oracle on YAML |
| T5 | `dyn_crypto`, `entropy/*` | C wire formats first; Rust-only formats only via authorised deviations |
| T6 | `dyn_message`, `dyn_request`, `dyn_response`, `dyn_response_mgr` | `msg_clone`, `msg_tmo_*`, `craft_ok_rsp`, `simulate_ok_rsp`, `msg_payload_crc32`, `g_timeout_factor` |
| T7 | `proto/dyn_redis`, `proto/dyn_memcache` | parsers oracle-pinned; `redis_copy_bulk`, `memcache_copy_bulk`, coalesce chain paths |
| T8 | `proto/dyn_redis_repair`, `dyn_proto_repair.h` | timestamp read repair, Lua add/rem-set scripts, all 13 repairable commands |
| T9 | `dyn_connection*`, `dyn_client`, `dyn_server`, `dyn_proxy` | local node state gating, `admin_opt`, `auto_eject`, `read_repairs_enabled` gate incl. the `> DC_ONE` condition |
| T10 | `dyn_dnode_*`, `dyn_vnode`, `dyn_node_snitch` | dnode plane, peer timeouts, throttles |
| T11 | `dyn_gossip`, `seedsprovider/*` | C gossip and timestamp failure detection as the default; SWIM/phi via deviation review |
| T12 | `dyn_stats` | every metric, every admin HTTP command, `cluster_describe` |
| T13 | `dyn_core`, `dynomite.c` | init/teardown order, CLI flags, daemonize, pidfile, signals |
| T14 | `dyn_test.c`, `test/*.py`, `scripts/dynomite/*` | C tests ported in full |
| T15 | differential strict + mixed clusters | system-level oracle green |
| T16 | performance gate on chef | section 6 |
| T17 | deviation review closeout | every deviation authorised or reverted |
| T18 | additions re-integration | dyniak, search, QUIC, etc. rebased on the transliterated core; strictly additive check |
| T19 | noxu 8.x | section 9 |

T1-T13 can run with parallel workers per file group once T0 lands; the
dependency order above is the merge order.

## 8. Decisions the owner must make

Agents bring each item to the owner with evidence and a recommendation
before committing to it. Open items as of plan creation:

1. **Read repair**: C timestamp/Lua add-set scheme vs the Rust vote-count
   scheme. Default recommendation: transliterate the C scheme as the default
   (it is the C behaviour and is causally stronger), keep vote-count only if
   the owner authorises it as an alternative mode.
2. **Failure detection**: C gossip timestamp liveness vs phi-accrual / SWIM.
3. **Entropy wire formats**: the Rust length-prefixed / unified framing vs
   the C fixed `cipher_size` framing (C-node interop depends on this).
4. **Crypto**: AES-256-GCM addition and PKCS#7 padding vs C zero padding.
5. **Config strictness**: about a dozen Stage-4 entries where Rust rejects
   input C accepts.
6. **Each remaining entry** in `docs/parity.md` "Deviations" (section 3).
7. **Existing Rust-only behaviour** that changes C-config behaviour, found
   while running the C configs unchanged.

Decisions are recorded in `deviations.md` with the owner's sign-off line.

## 9. noxu 8.x

When the owner announces noxu 8.x: bump on a branch, run the full gate on
chef Debian, re-run the dyniak and noxu-datastore benches. noxu sits under
the additions, not under the C-compatible core, so the transliteration does
not wait on it.

## 10. Progress reporting

`scripts/check_parity.sh --summary` prints per-file and total counts of
`todo / ported / replaced / deviation / dead`. Each stage's merge journal
includes that summary before and after.
