# 2026-05-24 - Pull latest upstream Noxu

Operator directive: pull in the latest Noxu code and use it.

## Lamdb state before

* Local lamdb main: `3bc6a12` ("feat(examples): add cash, cask, and ftdb application examples").
* Origin/main: had been force-updated to `79e14fe`.
* Local commits ahead of remote: 134.
* Remote commits ahead of local: 174.

The divergence is consistent with an upstream history rewrite
(merging a long-running `chore/stateright-and-hegel` branch into
main and force-pushing).

## Action taken

1. `git branch -f preserve/local-main-pre-2026-05-24 main`
   - preserves the operator's local 134 commits as a named ref
2. `git stash push -m 'lamdb-local-pre-pull-2026-05-24' --include-untracked`
   - preserves the operator's working-tree state (a deleted
     `.claude/scheduled_tasks.lock`, a modified `noxu-rep`
     submodule, two untracked files)
3. `git reset --hard origin/main`
   - local main now matches upstream

## Upstream activity since 3bc6a12

Highlights from `git log 3bc6a12..origin/main --oneline`:

* `chore(stateright-and-hegel)` merge with two-perspective review fixes
* `noxu-tree`: dormant descent functions converted to read_arc hand-over-hand
* `noxu-txn`: drain locks on commit error paths
* `noxu-cleaner`: log lock_manager.release errors in migrate_ln_slot
* `noxu-recovery`: log+continue on tree.insert errors during replay
* `noxu-txn`: propagate lock_manager errors from move_write_lock_to_new_lsn
* `spec`: replaced TLA+ with Stateright executable specs

Plus a new `v1.2.0` tag.

## Verification against our consumers

`crates/dyn-riak` is the only consumer of Noxu. The freshly-merged
Items 2+5 worker delivered NoxuDatastore as a first-class backend
plus 2i lookup; the merge against upstream Noxu HEAD was clean
with no source-level changes required.

| Gate | Result |
|---|---|
| `cargo build --workspace --all-targets --all-features --locked` | clean |
| `cargo nextest run --workspace` | 1111 passed, 4 skipped |
| `cargo nextest run -p dynomited --features riak` | 63 passed, 4 skipped |
| `cargo nextest run -p dyn-riak --features noxu` | 302 passed, 0 skipped |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | clean |
| `cargo fmt -p ... -- --check` (scoped to our crates) | clean |
| `cargo test --doc --workspace` | clean |
| `scripts/check_no_todos.sh` | clean |
| `scripts/check_no_port_comments.sh` | clean |
| `scripts/check_ascii.sh` | clean |

## Open question for the operator

The 134 local commits at `preserve/local-main-pre-2026-05-24`
are real lamdb work that may or may not be present in the new
upstream history. From dynomite's perspective we now consume
upstream HEAD; whatever needs to happen to reconcile lamdb's
local-vs-upstream story belongs in the lamdb tracker.
