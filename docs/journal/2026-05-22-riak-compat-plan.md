# 2026-05-22: Riak-compatible API + Noxu backend planning

**Agent**: planning worker (general-purpose, sonnet)
**Branch**: feat/riak-compat-plan
**Scope**: design doc + workspace-level Cargo plumbing for the Noxu DB
storage backend and the Riak HTTP/PB protocol layer.

## Output

* `docs/riak-compat-plan.md` (987 lines): full design covering
  architecture, protocol surface, storage layer mapping onto Noxu,
  feature catalogue with effort estimates, riak-admin equivalent,
  Cargo plumbing notes, M1-M9 milestone plan, and ten open questions.
* Workspace `Cargo.toml`: 13 new path dependencies under
  `[workspace.dependencies]` for the noxu-* crate closure.
* `crates/dynomite/Cargo.toml`: optional `noxu-db` dep gated behind
  a new `riak-storage` feature (off by default).

## Files touched

```
M Cargo.toml
M Cargo.lock
M crates/dynomite/Cargo.toml
A docs/riak-compat-plan.md
A docs/journal/2026-05-22-riak-compat-plan.md
```

No source files (`crates/*/src/`) were modified. Per the brief, the
plan does not begin Riak protocol implementation.

## Verification

```
cargo build --workspace --all-targets --locked              OK
cargo build --workspace --all-targets --all-features --locked  OK
cargo nextest run --workspace --no-fail-fast                619 / 619
cargo test --doc --workspace                                OK
scripts/check_ascii.sh on the new doc                       no findings
```

The `--all-features` build is the meaningful resolver test for
Section 6: enabling `riak-storage` forces cargo to walk the entire
Noxu DB dep graph and compile all 13 noxu-* crates plus their
transitive dependencies.

## Cargo conflicts encountered

None blocking. Adding `riak-storage` brought in:

| Crate | Version | Notes |
|---|---|---|
| `thiserror` | 2.0.18 | Coexists with our 1.0 pin (cargo selects per-consumer). |
| `hashbrown` | 0.15.5 | New runtime dep. |
| `foldhash` | 0.1.5 | hashbrown 0.15 default hasher. |
| `allocator-api2` | 0.2.21 | hashbrown helpers. |
| `byteorder` | 1.5.0 | Stock. |
| `memmap2` | 0.9.10 | Used by Noxu's mmap path. |
| `fs2` | 0.4.3 | flock helpers. |
| `lru` | 0.12.5 | Stock. |

No reshuffling of `bytes`, `parking_lot`, `serde`, `tokio`,
`tracing`, or `log`. The dynomite crate's `forbid(unsafe_code)`
posture is undisturbed because nothing in `crate::dynomite` actually
imports `noxu_db` yet.

## Toolchain note

lamdb pins `1.95` in `rust-toolchain.toml`; dynomite pins `1.90`.
Both build under `1.90` today (edition 2024 needs only 1.85+). I did
not bump the dynomite toolchain in this branch because no actual
1.95 feature is in use; the bump becomes mandatory at M1 when we
start importing noxu-db. Captured as Section 8.5 open question and
as a Section 7 cross-team item.

## Highlights of the plan

1. **Architectural reuse is high.** The Datastore trait, the
   per-protocol parser modules, and `cluster::dispatch` already
   slot Riak in cleanly; nothing in the substrate has to change
   shape. The new code is concentrated in `proto::riak::*`,
   `crate::storage` (NoxuStore), `crate::crdt`, `crate::pipe`, and
   the `dyn-admin` binary.
2. **Out-of-scope items per user instruction**: Yokozuna / Search,
   Riak TS, Riak CS. Recorded throughout.
3. **Effort**: 16-21 weeks single-worker; 10-14 weeks with two
   parallel workers. M8 (CRDTs + MapReduce) is the biggest single
   milestone and subdivides cleanly.
4. **Hash function compatibility** is an explicit open question
   (8.1). Default recommendation: behavioural compatibility via
   murmur3, with `chash_keyfun: sha1` available for users who want
   to migrate from a real Riak.
5. **Per-vnode Noxu Database** is the storage shape: one Database
   per (vnode, kind) pair, plus a `cluster_state` and `bucket_types`
   DB shared across the node. Tictac AAE Merkle trees live in
   `vnode_<id>_aae`; hinted-handoff queues in `vnode_<id>_handoff`;
   2i indexes are `SecondaryDatabase` instances.
6. **Five Noxu cross-team items** (compaction throttle, live DB
   open/close, Sequence durability, cursor pagination across
   commits, memory-budget controls) need to be filed against the
   lamdb tracker before M1 lands.

## Risks identified

* Lamdb edition 2024 + 1.95 pin (mitigation in Section 7 / 8.5).
* Path deps block crates.io publishing of dynomite until lamdb
  publishes; we are pre-0.1, no immediate risk (Section 6.4).
* MapReduce is the longest-tail item and naturally lives at the end
  of the schedule; if we end up time-pressed, M8 is the right place
  to descope (CRDTs first, MapReduce later).

## Hand-off

This branch is ready for the lead's review. Once approved and
fast-forwarded, the natural next step is to spawn an M1 worker
agent on `feat/riak-storage-substrate` to scaffold `crate::storage`,
file the Noxu cross-team items, and prove a hand-crafted Put/Get
round-trip through `NoxuDatastore`.

## Status block

```
STAGE: riak-compat-plan
STATUS: READY_FOR_REVIEW
BRANCH: feat/riak-compat-plan
JOURNAL: docs/journal/2026-05-22-riak-compat-plan.md
DOC: docs/riak-compat-plan.md
NEW_DEPS: noxu-db plus closure (path = ../lamdb/crates/...)
```
