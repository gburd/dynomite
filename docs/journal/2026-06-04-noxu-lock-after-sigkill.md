# Chaos finding: noxu environment lock not released after SIGKILL

Date: 2026-06-04
Surfaced by: the unified chaos mode (Redis-KV + Riak-PBC + Riak-HTTP
against one shared in-process Noxu datastore) under
`MODE_FAULTS=process,network,clock,disk`.

## Symptom

When the chaos injector SIGKILLs a `dynomited` running with
`data_store: noxu`, the recovery restart fails:

```
server build failed
  error=server: configuration field 'noxu_path' is invalid:
  could not open Noxu environment at '.../noxu-dc-arnold':
  noxu: environment failure (UNEXPECTED_STATE): failed to init
  FileManager: Environment locked: Environment is locked by
  another process
```

The node never rejoins; both the Redis-KV driver and the Riak
PBC driver against that node go to 100% failure for the rest of
the run.

## Root cause

The Noxu environment guards single-writer access with a
presence-based lock file, `noxu.lck` (a 0-byte file in the
environment directory). It is NOT an advisory `flock(2)`: the
kernel does not release it when the holding process dies. After a
`kill -9` the file remains, and the next `NoxuDatastore::open_in`
treats it as "another live process holds the environment" and
refuses to open.

This is a real robustness gap: in production, a hard kill (OOM
killer, `kill -9`, power loss) of a dynomited fronting a Noxu
store leaves the store unopenable until an operator manually
removes the lock.

## Fix applied (harness)

`scripts/chaos-multi-host/start-host.sh` removes a stale
`noxu.lck` before reopening when `MODE=unified`. The chaos
coordinator guarantees a single owner per host and pre-kills
stale processes before every (re)start, so any lock present at
launch time is always stale. This is the same recovery step a
production operator performs after a hard kill.

## Recommended engine fix (IMPLEMENTED 2026-06-04, commit follows this entry)

`dyniak::datastore::NoxuDatastore::open_with_db_name` now acquires
a dyniak-owned sidecar lock -- an exclusive `flock(2)` on
`.noxu-owner.lock` -- before touching noxu. The kernel releases an
`flock` automatically when the holding process dies (graceful exit
OR `SIGKILL`), so acquiring it proves no live owner remains; at
that point any leftover `noxu.lck` is provably stale and is removed
before `Environment::open`. A genuinely concurrent opener fails the
non-blocking flock and gets the typed
`NoxuDatastoreError::EnvironmentBusy` rather than corrupting state.

With this in place the harness `rm noxu.lck` (start-host.sh, commit
4551543) is belt-and-suspenders: crash-recovery is correct at the
library level regardless of how dynomited is launched.

Regression tests in `crates/dyniak/src/datastore/noxu.rs`:

- `reopen_reclaims_stale_noxu_lck` -- plant a stale lock, confirm a
  fresh open reclaims it and preserves the pre-crash data.
- `concurrent_open_is_rejected_while_owner_lives` -- a second live
  open of the same path returns `EnvironmentBusy`.

A further upstream improvement (in the `noxu` crate itself: make
`noxu.lck` an flock rather than a presence file) would let us drop
the sidecar entirely, but that is outside this repo's control
(noxu is an external crate). The sidecar owner-lock is the correct
fix at the dyniak boundary.
