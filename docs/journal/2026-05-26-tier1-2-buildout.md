# 2026-05-26 - tier-1+2 buildout session

## Brief

User asked for a single-session push covering:

1. New `gen-fsm` crate (gen_statem-style FSM driver, named for non-Erlang readers).
2. Refactor AAE exchange + dyn-riak get/put coordinators onto gen-fsm.
3. Tier-1 Riak idioms: capability negotiation (#1), APL annotations (#2),
   handoff FSM (#3), cluster_info dump (#4).
4. Tier-2 Riak idioms: supervisor-tree (#7), sidejob+throttle (#5+#6),
   gen-event manager (#8).
5. #12 reaper FSM (TTL-driven tombstone GC).
6. #13 hashtree extracted to its own library crate.
7. Bump Noxu to v2.0.0-rc1.
8. Refresh other workspace deps.
9. Loom-based concurrency tests.

Skipped per user direction: #9 riak_ensemble, #10 riak_repl, #11 hot
code reload.

## Execution model

Realistic scope: 4-6 weeks of focused work. Single-session ambition is
to land the foundation (gen-fsm, Noxu bump, deps refresh) plus as many
of the tier-1/2 standalones as time permits, dispatching parallel
workers via pi's `Agent(isolation: "worktree")`.

## Wave 1 (foundation, sequential):

- [x] gen-fsm crate (foreground, commit `89a2924`)
- [ ] Noxu v2.0.0-rc1 bump (worker `8685ea67`)
- [ ] Other deps refresh (worker `e204076a`, queued)

## Wave 2A (parallel, dispatched 2026-05-26 ~22:00 UTC):

- [ ] Capability negotiation (#1) - worker `176efe7a`
- [ ] APL annotations (#2) - worker `0b9b1220`
- [ ] cluster_info dump (#4) - worker `366f9eb5`
- [ ] Supervisor-tree (#7) - worker `f57c52be`
- [ ] Sidejob+throttle (#5+#6) - worker `add10bcf`
- [ ] gen-event manager (#8) - worker `322b4075`
- [ ] Hashtree library extract (#13) - worker `85ccf16c`
- [ ] Loom concurrency tests - worker `555b91e5`

## Wave 2B (after wave 2A merges; not yet dispatched):

- [ ] AAE refactor onto gen-fsm + new hashtree crate
- [ ] dyn-riak get_fsm + put_fsm onto gen-fsm
- [ ] Handoff FSM (#3)
- [ ] Reaper FSM (#12)

## Pi 4-concurrent cap

Pi enforces `max 4 concurrent` background agents. With 8 in the queue
plus noxu running, expect ~6 hours of total wallclock for Wave 2A.

## Conflict-resolution policy

Each worker creates files in its own subdir/module. Cross-cutting
touches (Cargo.toml `members`, `cluster::mod.rs`, `runtime::mod.rs`)
are unavoidable; lead resolves at merge time using the proven flow:

1. Rebase the branch onto current `main`.
2. If conflicts: hand-resolve (typically just `<<<<<<<` markers
   between independently-added blocks; keep both).
3. Run `cargo build --workspace --all-targets --locked` + the
   targeted `nextest run -p <crate>` for that worker's tests.
4. Run `bash scripts/check_no_todos.sh && bash scripts/check_ascii.sh
   && bash scripts/check_no_port_comments.sh`.
5. Merge `--no-ff` to main with a body crediting the worker.
6. Push.

## Stuck-point checklist (lessons from earlier in this session)

- After any conflict resolution + interruption, ALWAYS check
  `git status` AND `.git/rebase-merge/` before issuing the next
  command. Lost 5 min earlier when the rebase had auto-applied
  resolutions but `--continue` had not been run.
- Memory + token budget: subagent briefs MUST be self-contained
  (worker has no parent context). Aim for ~50 lines per brief
  with explicit verification commands.
- Subagent results truncate after a few hundred bytes; use
  `get_subagent_result` with `verbose: true` if the report
  matters.

## Tracking

Live status query:

```sh
for id in 8685ea67-6be6-4e7 176efe7a-f0c5-487 0b9b1220-7acc-4ba \
          366f9eb5-af33-4b5 f57c52be-1219-4f8 add10bcf-7fce-4ff \
          322b4075-92fd-4e8 85ccf16c-2d49-4d8 555b91e5-8d30-42b \
          e204076a-888f-433
do
    echo "agent $id"
    # use the get_subagent_result tool
done
```

## Definition of done

A worker's branch lands on `main` only after:
- Rebased clean
- `nextest run --workspace` green (1414+ tests; new tests counted)
- `cargo clippy --workspace --all-targets -- -D warnings` clean
- `bash scripts/check.sh` green for the crate the worker touched
- ASCII / no-todos / no-port-comments hygiene clean

Anything that lands and breaks `main` gets reverted within an hour.
