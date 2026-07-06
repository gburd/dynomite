# RAMP-Fast read-atomic transactions -- prototype

Date: 2026-07-05
Branch: `proto/ramp` (off `main` @ `2edfd65`)
Worktree: `/home/gburd/ws/wt-ramp`
Author: Greg Burd <greg@burd.me>

## Goal

Prototype Read-Atomic Multi-Partition (RAMP) transactions as a
lower-latency, AP-native complement to dyniak's cross-node XA / 2PC
(`crates/dyniak/src/datastore/xa.rs`, `xa_net.rs`) for the common
"atomic multi-key read/write without full serializability" case.
Read-atomic isolation: a transaction sees ALL or NONE of another
transaction's writes (no fractured reads), with no locks and no
reader ever blocking on a writer.

Reference: Bailis, Fekete, Ghodsi, Hellerstein, Stoica, "Scalable
Atomic Visibility with RAMP Transactions" (SIGMOD 2014).

## What was built

RAMP-Fast, as an ADDITIVE path alongside XA (XA is untouched).

### Modules

* `crates/dyniak/src/ramp.rs` -- the pure read-atomic core:
  * `RampItem` (key, ts, sibling-key set, value): the RAMP-Fast
    write metadata is the SET of sibling keys the writing
    transaction touched.
  * `select(round1) -> Vec<(key, ts)>`: the exact RAMP-Fast
    second-round decision. For each observed item and each sibling
    its metadata names, if the reader's current version of the
    sibling is older than the item's ts, the reader is behind and
    must upgrade to the highest such required ts. Empty result =>
    round 1 was fracture-free => no second round (the common case).
  * `RampClock`: per-coordinator unique + monotonic timestamps
    (high 16 bits = coordinator id, low 48 = counter). RAMP needs
    only uniqueness + per-writer monotonicity, not a global total
    order -- that is why it is lock-free and AP-native.

* `crates/dyniak/src/ramp_store.rs` -- the coordinator + storage:
  * `RampStore` trait: `put_version` (PREPARE, invisible),
    `commit_pointer` (COMMIT, monotonic visible-pointer advance),
    `latest_visible`, `get_version`. Implemented on
    `NoxuDatastore` over a dedicated keyspace disjoint from the
    primary K/V + 2i tags:
    - versioned item: `V\0{key}\0<ts-be>` -> encoded (siblings, value)
    - latest-visible pointer: `L\0{key}` -> `<ts-be>`
  * `RampCoordinator::write`: two-phase, non-blocking. PREPARE
    writes every item invisible at one shared ts with the sibling
    set; COMMIT advances every key's pointer. A reader never waits.
  * `RampCoordinator::read` / `read_with_rounds` / free `ramp_read`:
    round 1 (latest-visible + metadata per key) + conditional round
    2 driven by `select`. Because PREPARE writes the version before
    COMMIT advances the pointer, a second-round fetch by ts always
    finds the version, even mid-commit.
  * Free `ramp_write` / `ramp_read` + a process-wide atomic clock so
    the HTTP layer (which holds a shared `Arc`-store, not a
    `&mut RampCoordinator`) can mint unique monotonic timestamps.

### API (chosen: dedicated endpoints)

`POST /ramp/transactions` (write) and `POST /ramp/read` (read), wired
in `crates/dyniak/src/proto/http/routes.rs`. Chosen over a mode flag
on `/transactions` because RAMP's read is a first-class operation
(the client issues a RAMP read, gets a fracture-free snapshot back),
which the XA/`TxnBatch` write-only shape does not express. Both
endpoints are `#[cfg(feature = "noxu")]` and reply `501` on a
non-RAMP datastore, mirroring the existing `/transactions` probe.

### dnode wire hook

Added `DmsgType::RampPrepare = 23` in
`crates/dynomite/src/proto/dnode.rs` (value 22 reserved for the
concurrently-developed RiakReplica type per the dispatch brief).
It routes to `DmsgDispatch::Bypass` like the XA frames. This is the
wire hook for the cross-node fan-out; the single-node coordinator
does not yet emit it (see scope).

## Scope: single-node read-atomic, fully correct

This slice implements RAMP-Fast for the SINGLE-NODE / local multi-key
case: a transaction's keys all live in one process's store and the
coordinator fans PREPARE / COMMIT / read rounds across them
in-process. The read-atomic ISOLATION algorithm -- fractured-read
prevention via write versioning, sibling metadata, and the
1+conditional-2nd-round read -- is fully implemented, real, and
gated (NOT stubbed).

The multi-partition wire fan-out over the dnode plane (emit
`RampPrepare` to the peer that owns each key, collect versioned read
rounds cross-node) is the documented NEXT STEP. It does not change
the isolation algorithm: RAMP's atomicity is a property of the
per-item versioning + sibling metadata, not of where the items live.
`select` and the two-phase visibility logic move unchanged to the
cross-node coordinator; only the transport under `RampStore` changes
(a `RampStore` impl that fans over `RampPrepare` instead of the local
noxu keyspace).

## The DST + Elle gate (AGENTS.md Section 6.5)

### DST (`crates/model-tests/src/ramp.rs`, wired into `scripts/model.sh`)

Models RAMP-Fast writers + a reader over concurrent multi-key
transactions. The model drives the SAME decision core as production:
`select_missing` is the model's copy of `dyniak::ramp::select`, and
the write path is the same two-phase (PREPARE = version present
invisible, COMMIT = pointer advance) the coordinator runs, so the
gate covers the real rule.

* Safety invariant -- **read atomicity / no fractured read**
  (`always`): a completed read never observes txn T's write to one
  key while missing T's write to a sibling key T also wrote. Includes
  the classic scenario: a writer PREPAREs both keys but COMMITs them
  in any interleaving with a concurrent reader (the partial-apply
  window); the faithful reader's second round repairs it.
* Liveness -- **read terminates (non-blocking)** (`eventually`):
  every started read reaches Done under fair scheduling (a reader
  never waits on a writer).
* NEGATIVE CONTROL: `BrokenRamp` models a reader that SKIPS the
  second round (returns the round-1 snapshot verbatim). The checker
  FINDS the fractured-read counterexample against it
  (`broken_ramp_produces_fractured_read`), proving the invariant has
  teeth. A false-alarm guard
  (`faithful_ramp_at_control_shape_is_safe`) confirms the faithful
  model at the exact same shape is clean -- the violation is the
  skipped round, not the harness.

Six model tests, all green; state spaces > 20 unique states (asserted
non-trivial).

### Elle (`scripts/consistency/`)

* `txn_history_workload.py` extended with a `--ramp` mode that drives
  the RAMP endpoints (`/ramp/read`, `/ramp/transactions`) against a
  live `dynomited`, recording the identical list-append history shape.
* `crates/dyniak/tests/ramp_history.rs` records a golden history from
  REAL RAMP code (the same `ramp_write` / `ramp_read` the HTTP
  handler calls) running 60 multi-key list-append transactions over 4
  logical processes, and writes it to
  `crates/dynomited/tests/fixtures/consistency/ramp_list_append.jsonl`.
* `scripts/consistency/check.sh` checks that golden history and
  reports PASS -- no anomalies of the covered classes (DUP, G1a
  aborted/dirty read, NONMONO per-process monotonic read, CYCLE
  ww/wr dependency cycle). The checker self-test still catches its
  seeded lost-update + dirty-read, so the gate has teeth.

What the recorded RAMP history validates: RAMP is read-atomic, NOT
serializable. The list-append checker's classes are exactly the ones
a read-atomic AP store must satisfy: no dirty reads (G1a), no
lost/duplicated writes (DUP), per-process monotonic reads (NONMONO),
and no ww/wr dependency cycle over committed transactions (CYCLE).
Read atomicity's specific guarantee -- no fractured "read your
siblings" -- is what the DST model proves directly (the checker's
per-key append order plus NONMONO catches a reader that saw a
sibling's stale version, because the append it should have seen would
be missing); the DST model is the authoritative fractured-read
oracle, the Elle history is the real-code corroboration.

### hegel (`crates/dyniak/tests/ramp_properties.rs`)

Three properties, each >= 256 cases:

* `version_body_round_trips`: a versioned item's (siblings, value)
  body encodes and reads back identically through the store.
* `select_triggers_iff_fractured`: `select` returns a non-empty
  repair set exactly when the round-1 snapshot is fractured (checked
  against an independent reference predicate), and every repair
  upgrades (never downgrades) a key's ts.
* `atomic_visibility_over_interleavings`: a RAMP write of a random
  key subset, with COMMITs interleaved arbitrarily around a
  concurrent read, never yields a fractured snapshot (the read is
  all-old or all-new for the write set, never a mix).

## Latency vs XA

Not measured as wall-clock: single-node in-process timing is
dominated by the shared noxu store for both paths, so a local number
is meaningless. The meaningful RAMP win is ROUND-COUNT / blocking:

* XA (cross-node): PREPARE (fan-out + collect votes) THEN COMMIT
  (fan-out + collect acks) = two blocking phases, and a reader of a
  key mid-transaction can be blocked by the resource-manager lock.
* RAMP-Fast (cross-node, the target): a read is ONE round in the
  contention-free case, a second round only under concurrency, and a
  reader NEVER blocks on a writer. A write is two phases but both
  non-blocking.

The round-count claim is what the DST model and the
`read_with_rounds` API assert directly (`rounds == 1` contention
free, `rounds == 2` on a partial-apply window). A real cross-node
latency comparison is the follow-up when the dnode fan-out lands.

## Gates

Run from `/home/gburd/ws/wt-ramp` with the openblas `LD_LIBRARY_PATH`:

* `cargo build -p dyniak --features noxu --locked` -- clean
* `cargo build -p model-tests --locked` -- clean
* `cargo nextest run -p dyniak --features noxu` -- 727 passed
* `cargo test -p model-tests` -- 22 passed
* `cargo test -p dyniak --features noxu --doc` -- 53 passed
* `cargo clippy -p dyniak -p model-tests --all-targets --features noxu -- -D warnings` -- clean
* `cargo clippy -p dyniak --all-targets --features noxu,wasm,search -- -D warnings` -- clean
* `cargo fmt -p dyniak -p model-tests -- --check` -- clean
* `bash scripts/model.sh` -- 22 passed (RAMP + XA + quorum + ring + gossip)
* `bash scripts/consistency/check.sh` -- OK (RAMP golden history anomaly-free)

Tests added: 6 DST model tests, 3 hegel properties, 1 golden-history
recorder, 7 RAMP unit tests (ramp.rs + ramp_store.rs), 1 dnode
dispatch assertion = 18 test entry points (many run >= 256 cases or
exhaustive state-space searches).

## Limitations (production gaps)

1. **Single-node only.** Cross-node dnode fan-out is the documented
   next step. The `RampPrepare = 23` DmsgType and Bypass routing are
   in place as the hook; no receive handler emits/consumes it yet.
2. **No version garbage collection.** Every RAMP write leaves a
   versioned record under `V\0...`; a real deployment needs a GC pass
   that reclaims versions older than the oldest possible in-flight
   read (RAMP's paper discusses a low-watermark scheme). Not built.
3. **No abort path.** RAMP-Fast as implemented always commits a
   write (PREPARE then COMMIT). A client-abort or a coordinator
   crash between PREPARE and COMMIT leaves invisible versions that GC
   reclaims; this is safe (never visible) but untested here.
4. **Process-wide HTTP clock resets on restart.** The single-node
   HTTP coordinator's counter starts at 0 each process; monotonicity
   holds within a process. A durable/hybrid-logical clock is needed
   before cross-node so timestamps survive restart and order across
   coordinators without collision beyond the coordinator-id split.
5. **Elle history is in-process real code, not a live `dynomited`
   binary run.** The exercised code path is identical (the HTTP
   handler is a thin JSON shim over `ramp_write`/`ramp_read`), and
   the `--ramp` workload mode records the same shape against a live
   binary for the EC2 distributed runs, but no live-binary RAMP
   history is committed yet.
6. **RAMP-Small / RAMP-Hybrid not implemented.** Only RAMP-Fast (the
   1+conditional-2nd-round variant). Small (constant metadata, always
   2 rounds) and Hybrid (Bloom-filter metadata) are out of scope.
