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

## Recommended engine fix (follow-up, not in this commit)

`dyniak::datastore::NoxuDatastore::open_in` (or the underlying
noxu environment open) should reclaim a stale lock itself:

- switch `noxu.lck` to an `flock(2)`/`fcntl` advisory lock so
  the kernel releases it on process death (preferred); or
- write the holder PID into the lock file and, on open, reclaim
  the lock when that PID is no longer alive.

Until then the presence-based lock requires external cleanup
after any non-graceful exit.
