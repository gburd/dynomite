# Consistency-verification initiative (2026-06-19)

Goal: prove dyniak provides the consistency it advertises -- especially
cross-node multi-key XA transactions -- by exercising and checking the
REAL dyniak/noxu code, not abstractions. Four workstreams, in
dependency order.

## Ground truth (verified before starting)

- The chaos rig DOES drive the real `dynomited` binary + real
  dyniak/noxu over real PBC/TCP sockets. It is not an abstraction.
  Its gap is that it records only per-op-class counts/failures, not a
  per-operation history (op, args, observed value, invoke/return
  times). So it catches errors and crashes but cannot detect a
  consistency anomaly (a read that returns an impossible value while
  every op "succeeds").
- The stateright models (`crates/model-tests`) are deliberately
  self-contained abstractions of the protocol -- the Cargo.toml says
  so. They check a hand-written state machine, not the production
  `CrossNodeCoordinator` / `XaPeer`.
- Checker availability on crates.io (verified):
  - `elle-rs`: does NOT exist.
  - `elle`: 0.0.1, a placeholder/stub -- not a usable Elle port.
  - `porcupine` 0.2.4 / `porcupine-rs` 0.3.0: real, but
    linearizability of a SINGLE object only.
  There is no mature Rust port of Elle (Jepsen's transactional
  anomaly checker). This forces an honest split (below).
- The XA protocol logic is already partly separable: `handle_prepare`,
  `handle_commit`, `handle_rollback`, `resolve`, `local_prepare` are
  largely pure decision fns; `execute`, `prepare_all`, `commit_branch`,
  `redrive_commit` are the async I/O drivers. So the #3 refactor has
  natural seams.

## Tool decision: Porcupine vs Elle (the question asked)

- Porcupine checks LINEARIZABILITY of a single object. Correct for the
  single-key Valkey/register path; it cannot express multi-key
  transaction atomicity.
- Elle checks TRANSACTIONAL consistency (serializability / SI) by
  cycle-detection over a dependency graph, and names the anomaly
  (G0/G1/G2, lost update). Correct for the XA multi-key path.

Therefore: for dyniak's headline cross-node multi-key claim, the
Elle MODEL is the right one. Since no mature Rust Elle exists, the
plan is:
- CI / Rust-native (W3): Porcupine-rs for the single-key linearizable
  path, plus an Elle-style list-append cycle-detection checker we
  write (the algorithm is well documented; we implement the subset we
  need: write-write and write-read dependency edges over a recorded
  history, then cycle-detect). Scoped honestly as "Elle-subset," not
  "Elle."
- Release-qualification only (W4): real Jepsen + Elle (Clojure/JVM)
  driving a real multi-node dynomited cluster. The gold standard; out
  of CI because it needs the JVM/Clojure toolchain and long runtimes.

## Workstreams

W1 (foundation) -- chaos history recording. Make the chaos workload
   driver emit a per-operation history (op, key(s), value, process,
   invoke-time, return-time, outcome) in the Jepsen/Elle edn-or-json
   shape, in addition to the existing counts. This is the prerequisite
   for any checker and is the smallest, highest-leverage change. No new
   system-under-test code; the workload already hits real dyniak.

W2 (CI checker) -- Rust-native checkers consuming the W1 history:
   Porcupine-rs for single-key linearizability; an Elle-subset
   transaction-cycle checker for the multi-key XA history. Wire into a
   dedicated test target (not the default fast suite; a `consistency`
   profile), runnable in CI.

W3 (model = code) -- extract the XA protocol decision logic into a
   pure, deterministic state machine in dyniak (no sockets, no fsync,
   no tokio) that BOTH the production `CrossNodeCoordinator`/`XaPeer`
   drive AND the stateright model drives. This closes the
   model-vs-implementation gap honestly: the model checks the same
   logic the code runs, not a copy. The async drivers become a thin
   I/O shell over the pure core.

W4 (release-qual Jepsen) -- a real Jepsen test (Clojure project under
   `qa/jepsen/`) that builds dynomited, stands up a multi-node cluster,
   drives concurrent multi-key txns through a generator + partition
   nemesis, and checks the history with Elle. Documented as a
   release-qualification gate, NOT CI. Includes the build/run scripts
   and a make target.

## Sequencing

W1 first (everything downstream needs the history). Then W2 and W3 can
proceed in parallel (different areas: W2 is a new checker crate
consuming history files; W3 refactors dyniak XA + model-tests). W4 last
(it can reuse the W1 history shape and the W3 pure core for its own
sanity, but is otherwise independent).

## Honest scope notes

- "Elle-subset" is not Elle. It will detect the anomaly classes we
  implement edges for (lost update, dirty read, and write-cycle G0);
  full G2/predicate anomalies are W4/Jepsen's job. The doc and the
  checker output will say exactly which classes it covers.
- W3 is a real refactor of safety-critical code. Every existing XA
  test must stay green; the pure core gets its own unit + property
  tests; the stateright model imports the pure core.
- None of W1-W3 replaces W4. Jepsen+Elle remains the authority for
  "does the shipped system actually provide the consistency it
  claims." W1-W3 make that cheaper to trust and catch regressions in
  CI; W4 is the release-gate proof.
