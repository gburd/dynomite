# AGENTS.md - Operating Manual for the Dynomite Rust Port

This file is the standing instructions for any agent (or human) working in
this repository. It is read first on every session. The plan in `PLAN.md`
is the *what*; this file is the *how*. Both are authoritative; this file
takes precedence on process conflicts. The C reference tree under
`_/dynomite/` is the spec - when behavior is ambiguous, re-read the C and
match it.

The project is set up to run end-to-end without human approval. Agents are
expected to keep their own notes, validate their own work, peer-review one
another, and only escalate (by leaving a `BLOCKED:` note in `docs/journal/`)
when a decision genuinely requires human input.

---

## 1. Mission and non-negotiables

**Mission**: produce a Rust workspace that is functionally identical to
Netflix Dynomite (C). Library + binary. TCP and QUIC transports. IPv4 and
IPv6. Full test, fuzz, bench, and docs coverage.

**Non-negotiables**:

1. **No stubs, no `unimplemented!()`, no `todo!()`, no `panic!("TODO")`.**
   If you cannot complete a function, leave it un-merged.
2. **No `#[allow(...)]` to suppress warnings** unless there is a written
   justification in `docs/journal/allowances.md` linking to the exact
   compiler/clippy message and the upstream issue/limitation.
3. **No comments saying "ported from C", "matches dyn_xxx.c", or similar.**
   Acknowledgement of the original C project lives only in `README.md` and
   `LICENSE`/`NOTICE`. The Rust code reads as a first-class Rust codebase.
4. **No non-ASCII characters in code, comments, or docs.** This includes
   smart quotes, em-dashes, ellipses-as-one-character, arrows, accented
   letters in identifiers or strings (except where a test fixture
   intentionally exercises non-ASCII payload bytes - those go in `.bin`
   files).
5. **No agent cruft committed.** Scratch files, agent notes, conversation
   logs, .pi state, `.aider*`, `.history`, `*.swp`, `*.bak`,
   `*.orig`, etc. are gitignored and must never appear in commits.
6. **No reimagining.** When you find yourself "improving" the C design,
   stop, re-read the C, and reproduce its behavior. Improvements that we
   choose to make are recorded as a `Deviation:` entry in
   `docs/parity.md` with rationale.
7. **Every public item is documented**, every documented example is a
   doctest, and every doctest builds.
8. **Every commit must build, lint, format, and test cleanly.** Bisect must
   never land on a broken commit.

---

## 2. Repository layout (recap)

```
crates/dynomite/      # the engine (library)
crates/dynomited/     # the server binary
crates/dyn-hash-tool/ # hashing CLI util
crates/fuzz/          # cargo-fuzz harnesses
docs/book/            # mdBook reference manual
docs/parity.md        # C-to-Rust symbol mapping
docs/journal/         # rolling decision log (one .md per agent session)
test/                 # Python conformance harness (carry-over)
scripts/              # netem, multi-node helpers
_/dynomite/           # C reference; READ-ONLY
```

`_/dynomite/` is treated as read-only. Never modify it. If you need to
build the C binary for differential testing, build it in `target/cref/`.

---

## 3. Toolchain (Nix flake)

We run all commands inside `nix develop`. The flake pins every tool, so a
fresh checkout produces an identical environment.

### Required contents of `flake.nix`

```nix
{
  description = "Dynomite Rust port - dev shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            toolchain
            cargo-nextest
            cargo-fuzz
            cargo-criterion
            cargo-deny
            cargo-audit
            cargo-machete
            cargo-hack
            cargo-llvm-cov
            mdbook
            clang_18              # libfuzzer / quiche
            cmake                 # quiche
            perl                  # quiche / openssl
            pkg-config
            openssl
            iproute2              # tc
            iputils               # ping for network tests
            netcat-gnu
            tcpdump
            redis
            memcached
            python3
            (python3.withPackages (ps: with ps; [ pyyaml requests pytest ]))
            jq
            ripgrep
            fd
            git
          ];
          shellHook = ''
            export RUST_BACKTRACE=1
            export RUSTFLAGS="-D warnings"
            export DYNOMITE_C_REF="$PWD/_/dynomite"
          '';
        };
      });
}
```

### `rust-toolchain.toml`

```toml
[toolchain]
channel = "1.83.0"
components = ["rustc", "cargo", "rustfmt", "clippy", "rust-src", "rust-analyzer"]
profile = "default"
```

### Daily commands (always inside `nix develop`)

```
nix develop                                  # enter shell
cargo build --workspace --all-targets
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace
cargo test --doc --workspace
cargo deny check
cargo audit
mdbook build docs/book
```

`scripts/check.sh` runs the full local CI gate; it is what CI runs and what
every commit must pass. Agents must run it before declaring a stage done.

---

## 4. Git workflow

### Branching

* `main` is always green; only fast-forward merges from feature branches.
* One stage per feature branch: `stage/<n>-<short-name>`, e.g.
  `stage/3-hashkit`.
* Sub-tasks within a stage get short branches off the stage branch:
  `stage/3-hashkit/murmur3`. Agents working in parallel on the same stage
  use these.
* Agents working in parallel on different stages each take their own stage
  branch and use git worktrees (`git worktree add ../wt-stage-3 stage/3-...`)
  to avoid stomping each other's `target/` directory. The pi `Agent` tool's
  `isolation: "worktree"` option does this automatically and must be used
  for any parallel work.

### Commits

* Atomic, self-contained, build-clean.
* No "wip", "fix", "more", "lint" placeholder messages.
* **Format** (Conventional Commits):

  ```
  <type>(<scope>): <imperative summary, <=72 chars>

  <body wrapped at 72 cols, explaining *why*; reference the C file and
  function being ported, and the parity entry being created/updated.>

  Refs: PLAN.md Stage <n>
  Parity: docs/parity.md#<anchor>
  ```

  `type` in {feat, fix, refactor, perf, test, docs, build, ci, chore}.
  `scope` is the Rust module or crate path: `hashkit`, `proto::redis`,
  `cluster::gossip`, `dynomited`, `embed`, `flake`.

* No "Co-authored-by" or AI signatures. No emoji. No non-ASCII.
* Sign-off: `git commit -s` is required. CI rejects unsigned commits.

### Pull requests / merges

* Even in solo-agent mode, every stage merges via a documented PR-style
  flow: open a `merge/<stage>` markdown summary in `docs/journal/`, run
  `scripts/check.sh`, then fast-forward `main`. The summary includes the
  parity diff and the self-review (Section 9).
* `main` is fast-forward only.
* `git rebase -i` is fine on private stage branches; never rewrite `main`.

### `.gitignore` essentials

```
/target
/_/dynomite/build
/.direnv
/result
/result-*
*.swp
*.bak
*.orig
.aider*
.pi/
.history
.cargo/registry
.cargo/git
docs/journal/scratch/
crates/fuzz/corpus/
crates/fuzz/artifacts/
```

`.pi/` is dev-tooling state and never gets committed.

---

## 5. Coding standards

### Rust style

* `rustfmt` with default settings + `imports_granularity = "Crate"` and
  `group_imports = "StdExternalCrate"` (in `rustfmt.toml`).
* `clippy::pedantic` is **on** at the workspace root via
  `[lints.clippy]` in each crate's `Cargo.toml`. Specific lints we relax
  (with rationale in `docs/journal/allowances.md`):
  * `module_name_repetitions = "allow"` (we mirror C module naming).
  * `must_use_candidate = "allow"`.
  * Everything else stays at deny/warn.
* Function length: prefer <100 lines; if the C original is a 600-line
  state machine (Redis parser is), keep it as one Rust function. Do not
  split parsers across helpers if it changes control flow.
* Public items: rustdoc with `# Examples` section, doctested.
* `unsafe`: forbidden by default (`#![forbid(unsafe_code)]` at the top of
  `lib.rs`). If a specific module needs `unsafe` (rare; e.g. an SPSC ring),
  isolate it in a single module with `#![allow(unsafe_code)]` and write a
  safety section in module-level docs.
* Error handling: `thiserror` for typed errors at module boundaries. The
  binary uses `anyhow::Result<()>` only at `main`. Never `.unwrap()` outside
  tests; use `.expect("invariant: ...")` only when the invariant is
  documented in the same function's docs.
* No `lazy_static`; use `OnceLock`/`OnceCell`.
* No global mutable state outside `core::context::Context`. The C global
  `gn_pool` becomes a field on `Context`.

### Naming parity

The Rust module tree mirrors the C file names with the `dyn_` prefix
stripped: `dyn_message.c` -> `msg::message`, `dyn_dnode_msg.c` ->
`proto::dnode`. Public types keep their semantic C names: `Msg`, `Mbuf`,
`Conn`, `ServerPool`, `Datacenter`, `Rack`, `Peer`, `DynToken`. C `enum`
values become Rust enum variants with the same names (`MSG_TYPE_CODEC`
generates `MsgType::ReqRedisGet`, etc.).

### Two-layer pattern (idiomatic + parity)

When C code shapes do not map well to idiomatic Rust (e.g. callback vtables
on connections), we use the two-layer pattern from `rust-idiomatic`:

* **Inner layer** is idiomatic Rust: traits, enums, async fns, Result types.
* **Outer layer** is the parity API: a thin shim that mirrors the C
  function's signature semantically, used by the embedding API and tests.

Both must produce identical observable behavior. Tests cover both.

### Skill activation

The following skills are active for this project. Read each at session
start the first time you touch the relevant area:

* `c-to-rust` - on first port of any C file.
* `rust-idiomatic` - whenever shaping new public API.
* `rust-error-handling` - when defining a new error type.
* `rust-async` - when wiring tokio, channels, select, shutdown.
* `rust-ownership` - when modeling shared mutable state (`Arc<Mutex<...>>`,
  `tokio::sync::*`).
* `rust-traits` - when designing the embedding hooks (Datastore, Transport,
  ...).
* `rust-testing` - when designing a stage's test plan.
* `review-diff` - on every self-review (Section 9).
* `think-hard` - before any non-trivial design decision.
* `checkpoint` - at the end of every working session.
* `maintain-docs` - whenever PLAN.md, AGENTS.md, parity.md, or the mdBook
  changes.

---

## 6. Testing strategy

We use four classes of tests. Use the right one for the right job; don't
add tests just to pad coverage.

### 6.1 Unit tests

* Co-located with code in `mod tests { ... }`.
* Cover public function contracts and error branches.
* When the C source has assertions (`ASSERT(x == y)`), promote each to a
  Rust unit test if the assertion documents observable behavior.
* Keep them <100ms each; nothing that touches the network.

### 6.2 Regression tests

* Live in `crates/<crate>/tests/`.
* One file per stage: `tests/stage_03_hashkit.rs`, etc.
* Each fixed bug gets a permanent regression test named
  `regression_<short-description>` with a comment linking to the parity
  entry or commit that introduced the fix.
* Captured input corpora live in `tests/fixtures/<stage>/`.
* Differential corpora (C vs Rust output) are committed as `*.json.gz` so
  CI does not need to rebuild the C binary.

### 6.3 Property tests (`proptest`)

> Note on naming: the user request asked for "Hegel" property testing;
> the canonical Rust property-testing crate is `proptest`. We use
> `proptest`. If a tool literally named "Hegel" is identified later, we
> revisit; this is recorded in `docs/journal/allowances.md`.

* Use `proptest` for invariants whose state space is too large to
  enumerate. Good targets:
  * Hash + token arithmetic: `set_int_dyn_token(x); get_int(t) == x` for
    all `x` in domain.
  * mbuf split/merge: `merge(split(buf, k)) == buf` for all `k`.
  * Quorum decision: state-table function is consistent under any input
    permutation.
  * Parser totality: parsers never panic on arbitrary `Vec<u8>` and either
    return Err or a complete Msg.
  * Ring routing determinism: same key + same ring -> same primary peer.
* Each property test runs >= 1024 cases in CI (default) and 1M in the
  weekly long-soak job (`make soak`).

### 6.4 Fuzz tests (`cargo-fuzz` + libfuzzer)

* Targets live in `crates/fuzz/fuzz_targets/`.
* Mandatory targets: `proto_redis_parse`, `proto_memcache_parse`,
  `dnode_parse`, `conf_parse`, `crypto_aes_decrypt` (with a fixed key).
* Every PR that touches a parser must rebuild the corresponding fuzzer
  and run it for >= 60s before merge (`scripts/quickfuzz.sh`). Findings
  block merge.
* Weekly soak: each fuzzer runs 1h in CI; corpora get checked into
  `crates/fuzz/corpus/<target>/` after dedup.

### When to use what

| Concern | Use |
|---|---|
| Single-function input/output, single error path | unit |
| Multi-module flow, end-to-end behavior | regression |
| "For all valid X, property P holds" | property |
| "For arbitrary bytes, no panic / no UB" | fuzz |
| Real datastore protocol behavior | regression with `redis-server`/`memcached` from the flake |
| Concurrent state, races, cancellation | unit + `loom` (in `cfg(loom)`) for the SPSC/cbuf only; otherwise tokio multi-thread integration tests |

---

## 7. Benchmarking

### 7.1 Micro (criterion)

`crates/dynomite/benches/`:

* `parsers.rs` - per-protocol, per-command-class throughput on captured
  request frames; baseline = same parser on the same input compiled from C
  (where we have it as a static lib).
* `hashkit.rs` - each hash function on 16/64/256/1024B keys.
* `mbuf.rs` - alloc/dealloc, split, copy, recv simulation.
* `tokens.rs` - `set_int_dyn_token`, `dyn_token_cmp`, ring lookup.

Run via `cargo criterion --workspace`. CI compares against a baseline
checked in under `benches/baseline/` and fails if any micro-bench
regresses by >10% (configurable per-bench).

### 7.2 Macro

`scripts/bench/macro.sh` orchestrates:

* Bring up a 3-node Rust cluster + a 3-node C cluster on localhost (Stage
  14 differential rig).
* Drive both with `redis-benchmark` and `memtier-benchmark` (added to the
  flake when this stage starts) at fixed QPS targets and measure tail
  latencies.
* Repeat with `tc qdisc add dev lo root netem` configurations:
  * `delay 5ms`, `delay 5ms 1ms`, `delay 50ms loss 1%`,
  * `loss 5%`, `corrupt 0.1%`, `duplicate 0.1%`, `reorder 25% 50%`.
* Repeat over TCP and QUIC.
* Output results to `target/bench/macro-<git-sha>.json`; CI uploads.

### 7.3 Distributed-failure simulations on a single host

`scripts/netem/` contains canned scenarios:

* `partition_dc.sh` - drops traffic between two simulated DCs by source
  port range (every Rust node is bound to a known port band).
* `slow_peer.sh` - injects 200ms one-way delay on one peer.
* `flap.sh` - alternates connectivity at 1s intervals.
* `gc_pause.sh` - sends SIGSTOP/SIGCONT to one node for N seconds.
* `disk_full.sh` - mounts a tmpfs with a quota and points the entropy
  reconciliation at it.
* `clock_skew.sh` - uses `faketime` (added to flake) to skew one node by
  N seconds.

Every scenario has a corresponding integration test under
`crates/dynomite/tests/distributed/` that asserts the cluster's expected
graceful response (no data loss within quorum, auto-eject, gossip
recovery, repair triggered on read-after-partition, etc.).

These tests run under `cargo nextest run --test 'distributed_*'`. They
require `CAP_NET_ADMIN` (the flake's shellHook documents how to set up an
unprivileged netns + `setcap` so they run without root in CI).

---

## 8. Parallel agent execution

The project is designed for parallel execution by multiple agents. Use the
pi `Agent` tool with `run_in_background: true` and
`isolation: "worktree"` for any parallelizable stage.

### Orchestration model

There is one **lead agent** (the conversation you are in now) that:

1. Maintains `PLAN.md` and `AGENTS.md`.
2. Decides which stages can run in parallel (consult the dep graph in
   PLAN.md Section 4).
3. Spawns one **worker agent** per parallel-eligible stage, each on its
   own git worktree.
4. Spawns **reviewer agents** when a worker reports completion (see
   Section 9).
5. Runs the merge: rebases each completed stage branch onto `main` in the
   approved order and fast-forwards.

### Spawning workers

Use `Agent` with:

* `subagent_type: general-purpose` (or a custom `worker.md` agent if
  defined).
* `isolation: "worktree"` so the worker gets its own checkout and
  `target/`.
* `run_in_background: true` for parallelism.
* `prompt`: a self-contained brief that includes:
  * The stage number and exit gate from PLAN.md.
  * The list of C files to port.
  * Pointers to the active skills.
  * Reminders: no stubs, no warnings suppressed, ASCII only, no port
    comments, run `scripts/check.sh` before declaring done.
  * Where to write the journal entry on completion.
* `model`: prefer `sonnet` for big stages (parsers, cluster); `haiku` is
  fine for mechanical ports (hashkit algorithms).

### Worker contract

A worker:

1. Reads AGENTS.md and PLAN.md.
2. Reads every C file listed in its stage.
3. Activates the relevant skills.
4. Implements + tests + documents.
5. Runs `scripts/check.sh`.
6. Writes a journal entry at
   `docs/journal/<YYYY-MM-DD>-<stage>-<agent-id>.md` with: files
   touched, parity diff, test summary, open questions.
7. Pushes its stage branch (locally - `git push origin` is not used; the
   lead does the merge).
8. Returns a structured result:
   ```
   STAGE: <n>
   STATUS: <READY_FOR_REVIEW | BLOCKED:<reason>>
   BRANCH: stage/<n>-<name>
   JOURNAL: docs/journal/<file>.md
   PARITY_DELTA: <count of new entries>
   ```

### Concurrency safety

* No two workers may touch the same crate at once. The dep graph in
  PLAN.md Section 4 plus the worktree isolation guarantees this.
* Shared cross-cutting files (PLAN.md, AGENTS.md, docs/parity.md,
  Cargo.toml) are owned by the lead. Workers propose changes via their
  journal entry; the lead applies them on merge.
* Long-running fuzz/bench jobs run on a dedicated `bench/<topic>` agent
  and never block stage progress.

### Steering and supervision

* The lead checks in on background workers with `get_subagent_result`
  every few minutes (or on completion notifications).
* If a worker is going off-track (touching out-of-scope files, adding
  dependencies), the lead calls `steer_subagent` immediately with a
  corrective message that quotes the relevant AGENTS.md section.
* If a worker reports `BLOCKED:`, the lead reads the journal entry,
  decides, and either sends a steer or escalates to a human by leaving a
  top-level `BLOCKED:` line in `docs/journal/blocked.md`.

---

## 9. Self-review: third-party expert pass

Every stage gets an independent review **by an agent that did not write
the code**. This is not optional. The review must use the `review-diff`
skill and adopt a "neutral 3rd party expert" stance.

### Process

1. Worker finishes Stage N and posts `STATUS: READY_FOR_REVIEW`.
2. Lead spawns a reviewer agent:
   ```
   Agent(
     subagent_type: "Explore",   # read-only is intentional
     isolated: false,
     prompt: "<reviewer brief, see template below>",
     run_in_background: true
   )
   ```
3. Reviewer reads:
   * The C files for the stage.
   * The Rust diff (`git diff main...stage/<n>-<name>`).
   * `docs/parity.md` updates.
   * Journal entry from the worker.
4. Reviewer produces a markdown report at
   `docs/journal/review-<stage>-<agent-id>.md` with sections:
   * **Parity** - line-by-line check of C function -> Rust function
     mapping. List any C symbol with no Rust home that is not justified
     in `docs/parity.md`.
   * **Correctness** - call out any logic that diverges from the C
     reference. Cite line numbers in both trees.
   * **Style** - violations of Section 5.
   * **Tests** - are unit/regression/property/fuzz coverages right for
     this stage?
   * **Risk** - panics, unbounded resource use, deadlocks, race
     conditions, integer overflow, error swallowing.
   * **Verdict** - one of:
     * `APPROVE` - merge.
     * `APPROVE_WITH_NITS` - merge after worker addresses nits.
     * `REQUEST_CHANGES` - return to worker with explicit fix list.
     * `BLOCK` - stage must not merge; reasons require a human.
5. If `REQUEST_CHANGES`, the worker (resumed via `Agent.resume`) addresses
   each item, runs `scripts/check.sh`, updates the journal, and
   re-requests review. Loop until `APPROVE*` or `BLOCK`.

### Reviewer brief template

```
You are an independent reviewer. You did not write this code.
Adopt a third-party expert stance.

Stage: <n>
Branch: stage/<n>-<name>
C files: <list>
Rust diff: git diff main...stage/<n>-<name>

Your job:
1. Read AGENTS.md sections 5 and 9.
2. Read each C file in full. Read each new Rust file in full.
3. Build the parity matrix: every C function -> its Rust home.
4. For 5 randomly chosen C functions, walk the C and Rust line by line
   and confirm equivalence. Document the walk in your report.
5. Run `scripts/check.sh` and `cargo nextest run -p <crate>`.
6. Produce docs/journal/review-<stage>-<agent-id>.md per the template.

Do not modify code. Read-only review.
```

### Spot-checks

The lead also runs occasional **deep spot-checks**: pick one Rust
function at random per stage, read its C counterpart top to bottom, and
write a one-paragraph equivalence statement in `docs/parity.md`. This
catches reviewer drift.

---

## 10. Self-correction loop

When tests fail, the workflow is:

1. Capture the failure verbatim into the journal.
2. Re-read the C source for the failing area.
3. Identify the divergence (data structure layout, operator precedence,
   off-by-one in mbuf indexing, etc.).
4. Write a regression test that fails on the bug.
5. Fix the bug. Test now passes.
6. Update `docs/parity.md` with the lesson if it changes our parity
   understanding.

When clippy or compiler errors fail, **fix the cause, do not silence
the lint**. Allowances require a journal entry per Section 5.

When a stage's exit gate cannot be met because the C reference itself is
unclear or self-contradictory (rare but it happens - dead code paths in
Dynomite), record the situation in `docs/parity.md` under a
`# Ambiguities` section, choose the interpretation most consistent with
surrounding code, and add a regression test pinning the chosen behavior.

---

## 11. Documentation

### Mandatory docs surfaces

* `README.md` - quickstart, build, run, link to mdBook. **Only place that
  acknowledges the Netflix Dynomite C origin.**
* `PLAN.md` - the staged plan (kept current).
* `AGENTS.md` - this file (kept current).
* `docs/parity.md` - C-to-Rust mapping and ambiguity log.
* `docs/journal/` - rolling worklog.
* `docs/book/` - mdBook reference manual:
  * `src/SUMMARY.md`
  * `src/intro.md`
  * `src/architecture.md`
  * `src/configuration.md`
  * `src/protocols/redis.md`, `protocols/memcache.md`, `protocols/dnode.md`
  * `src/operations/recommendations.md` (port of `notes/recommendation.md`)
  * `src/embedding/index.md`, `embedding/server.md`, `embedding/hooks.md`,
    `embedding/examples.md`
  * `src/transports/tcp.md`, `transports/quic.md`
  * `src/security/crypto.md`
  * `src/internals/<one-per-major-module>.md`
* `crates/dynomite/src/lib.rs` - top-level `//!` doc with the embedding
  cookbook summary and a link to mdBook.
* Every public item in `dynomite::*` - rustdoc with at least one
  `# Examples` doctest.

`maintain-docs` runs at the end of each stage to confirm everything is
in sync.

---

## 12. CI gates (what `scripts/check.sh` runs)

```
set -euo pipefail
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-targets --locked
cargo nextest run --workspace --all-features
cargo test --doc --workspace
cargo deny check
cargo audit --deny warnings
mdbook build docs/book
scripts/quickfuzz.sh 60       # 60s smoke fuzz on every parser
scripts/check_no_todos.sh     # greps for unimplemented!/todo!/TODO/FIXME in src/
scripts/check_no_port_comments.sh  # greps for "ported from", "matches dyn_"
scripts/check_ascii.sh        # rejects non-ASCII in src/, docs/, tests/
scripts/check_parity.sh       # validates docs/parity.md against C and Rust
```

CI runs the same script on every push; merges to `main` are blocked on it.

---

## 13. Embedding API discipline

The embedding API is the public-facing crate surface and is governed by
SemVer once we cut 0.1. Until then:

* Any change to `pub use` items, public traits, or public structs in
  `crates/dynomite/src/embed/` requires a journal entry titled
  `api-change: ...` with a migration note.
* Every public trait (`Datastore`, `SeedsProvider`, `Transport`,
  `CryptoProvider`, `MetricsSink`) has at least one default impl shipped
  in-crate plus a non-trivial example in `crates/dynomite/examples/`.
* `cargo public-api` (added to flake) is run on every PR and the diff is
  pasted into the journal entry.

---

## 14. Definition of done (project-wide)

The port is complete when:

1. PLAN.md Stage 16 exit gates are all met.
2. `docs/parity.md` shows every C symbol mapped or justified.
3. The Stage 14 differential test passes for both transports.
4. The Stage 15 fuzz soak (1h per target) is clean.
5. The Stage 15 coverage gate reports >= 95% line coverage AND
   >= 95% branch coverage AND >= 95% function coverage workspace-
   wide via `cargo llvm-cov --workspace --all-features`. Anything
   below 95% has an explicit Deviation entry in `docs/parity.md`.
6. The Stage 15 micro and macro benches all run cleanly with
   committed baselines under `crates/dynomite/benches/baseline/`.
7. The Stage 16 chaos test runs for the full hour and reports
   zero invariant violations; the post-test sweep confirms the
   host is left in its pre-test state (no orphaned netem qdiscs,
   tokio tasks, child processes, faketime overrides). The
   `target/chaos/<run-id>/report.md` artifact is committed under
   `dist/chaos-reports/` for the release tag.
8. `mdbook build docs/book` is clean and the embedding examples
   compile and run.
9. `cargo public-api` shows a stable surface for two consecutive
   PRs.
10. The Python conformance harness (ported from
    `_/dynomite/test/`) is green against `dynomited`.
11. Both `.github/workflows/ci.yml` (GitHub Actions) and
    `.forgejo/workflows/ci.yml` (Codeberg Forgejo Actions) pass
    on the same commit. Both runners exercise the identical
    `scripts/check.sh` script as the single source of truth.
12. Every commit on `main` is authored by
    `Greg Burd <greg@burd.me>` after the Stage 16 history
    rewrite. Verified by
    `git log --format='%an <%ae>' | sort -u` returning exactly
    one line.
13. The release tag (`v0.1.0`) is signed and pushed to both
    GitHub origin and the Codeberg Forgejo mirror.
14. A "fresh checkout, `nix develop`, `scripts/check.sh`" loop
    runs end-to-end with no human input.

---

## 14a. Commit author normalisation

The project's release tag requires every commit on `main` to be
authored by `Greg Burd <greg@burd.me>`. Two enforcement points:

1. **Going forward (every new commit)**:
   * `git config user.name "Greg Burd"` and
     `git config user.email "greg@burd.me"` are set in the repo's
     `.git/config` (NOT committed). Subagent workers inherit
     this when they cd into the worktree because pi-tooling
     creates worktrees off the same git directory.
   * Subagent dispatch briefs explicitly direct workers to leave
     the author config alone; the worker's commits will be
     attributed to Greg Burd via the inherited config.
2. **Pre-tag history rewrite (Stage 16)**:
   * After all stages land, the lead runs
     `git filter-repo --commit-callback ...` (or
     `git filter-branch --env-filter ...`) over the entire
     `main` branch to set every commit's `author` and
     `committer` to `Greg Burd <greg@burd.me>`.
   * Pre-rewrite SHAs are recorded in
     `docs/journal/pre-rewrite-shas.md` for archaeology.
   * The rewrite is the LAST operation before the signed tag.
   * After the rewrite, no further commits land on `main` until
     the tag is pushed.

## 14b. Dual-platform CI

The project ships CI workflows for two forges:

* `.github/workflows/ci.yml`: GitHub Actions. Already exists.
* `.forgejo/workflows/ci.yml`: Codeberg Forgejo Actions. Created
  in Stage 16. Mirror of the GitHub workflow; both runners
  exercise the identical `scripts/check.sh` so divergence is
  impossible.

The `scripts/check.sh` script remains the single source of truth
for what "green CI" means. CI workflows are thin wrappers that
check out the repo, install the Nix flake's dev shell (or the
minimal dependencies needed by `scripts/check.sh`), and invoke
the script. Adding a new gate means editing `scripts/check.sh`,
NOT the workflow files.

Both pipelines run the same matrix:
* `cargo build --workspace --all-targets --locked`
* `cargo build --workspace --all-targets --all-features --locked`
* `cargo nextest run --workspace`
* `cargo nextest run --workspace --all-features`
* `cargo test --doc --workspace`
* `scripts/check.sh` (fmt, clippy, deny, audit, mdbook, hygiene)
* Stage 15 coverage gate (>= 95%)
* Stage 15 quickfuzz smoke (60s per target)
* Stage 14 conformance suite

The Stage 16 chaos test does NOT run in CI (it needs an hour
and `CAP_NET_ADMIN`). It runs as a manual pre-tag gate by the
lead.

## 14c. Cleanup discipline

No agent cruft, generated artifacts, or scratch files reach
`main`. Specifically:

* `target/`, `result*`, `.direnv`, `.cargo/registry`,
  `.cargo/git` are gitignored.
* `target/chaos/<run-id>/` chaos test outputs are NOT committed
  by default; only the final report under
  `dist/chaos-reports/<run-id>/report.md` is.
* `crates/fuzz/corpus/` and `crates/fuzz/artifacts/` are
  gitignored; only the dedup-curated regression seeds are
  committed under `crates/fuzz/seeds/`.
* The pi-tool's `.pi/` directory is gitignored.
* Every release tag is preceded by a manual sweep via
  `scripts/check_clean.sh` (added in Stage 16) that fails if
  any of these directories contain files git should track but
  doesn't, OR if any committed file is in a gitignored path.

---

## 15. Quick-start for a new agent session

1. `cd /home/gburd/ws/dynomite && nix develop`
2. `git status` - confirm clean tree (or worktree).
3. Read `PLAN.md` Section 3 for your stage.
4. Read `docs/parity.md` to see what is already done.
5. Read the C files for your stage.
6. Activate skills (Section 5).
7. Implement.
8. `scripts/check.sh`.
9. Write `docs/journal/<date>-<stage>-<agent>.md`.
10. Report status to the lead.

If you finish early, pick up the next unblocked stage from PLAN.md Section
4.

If you find yourself stuck for more than 30 minutes on a single decision,
write a `BLOCKED:` journal entry and stop. The lead will route it.

---

## 16. Out-of-band: what to do when something is missing

* **Missing skill**: read it from the path printed in the system prompt
  before doing the work. Do not guess.
* **Missing C file context**: re-read the C file in full. The C codebase
  is small enough (~38k LOC) that re-reading is cheap.
* **Missing Rust crate**: do not add a dependency without updating PLAN.md
  Section 2 and getting a journal entry approving the addition. The crate
  list there is the upper bound.
* **Missing tool**: add it to `flake.nix` in the same commit that needs
  it; never let `flake.nix` and the code drift.
