# Chaos investigation: "Environment is locked" under SIGKILL — MISDIAGNOSED, then corrected

Date: 2026-06-04 (finding), 2026-06-05 (correction)
Surfaced by: the unified chaos mode (Redis-KV + Riak-PBC + Riak-HTTP
against one shared in-process Noxu datastore) under
`MODE_FAULTS=process,network,clock,disk`.

## Symptom (real)

During the unified chaos run, nodes that had been SIGKILLed by the
chaos injector reported, on restart:

```
server build failed
  error=server: configuration field 'noxu_path' is invalid:
  could not open Noxu environment: Environment is locked by
  another process
```

The affected node's drivers went to 100% failure.

## First diagnosis (WRONG)

I concluded noxu's `noxu.lck` was a *presence-based* lock (a file
whose mere existence means "locked") that a hard kill leaves
behind, and added two workarounds:

1. `start-host.sh` removed a "stale" `noxu.lck` before reopen.
2. `dyniak::datastore::NoxuDatastore` took its own `flock(2)`
   owner-lock and reclaimed the "stale" `noxu.lck` under it.

Both shipped (commits 4551543, 18a9e3d) and were then **reverted**
after the diagnosis was disproved.

## Correction (verified empirically 2026-06-05)

Noxu's lock is NOT presence-based. `noxu-log/src/file_manager.rs`
(noxu 3.0.2) acquires the lock with
`fs2::FileExt::try_lock_exclusive()` -- a real advisory `flock(2)`.
The kernel releases an `flock` automatically when the holding
process dies, including under `SIGKILL`.

Direct test on a pristine binary (no workarounds compiled in):

1. start dynomited with `data_store: noxu`; it creates and flocks
   `noxu.lck`.
2. `kill -9` it. The `noxu.lck` file remains on disk (expected --
   the lock is on the open file description, not the path).
3. immediately reopen with NO lock cleanup.

Result: the reopen **succeeds**. `try_lock_exclusive()` reacquires
the lock because the kernel already released the dead holder's
flock. noxu's crash-recovery is correct.

## Actual root cause of the chaos errors

The "Environment is locked by another process" errors were noxu
**correctly** rejecting a *genuinely concurrent* open. Two things
produced concurrent openers during the run:

- Stale chaos *coordinators* from two earlier aborted unified
  launches were still alive and respawning dynomited on the hosts.
  When a recovery-restart fired, a previous respawn was still live
  and holding the flock. (These coordinators were later found and
  killed; once gone, the respawn storm stopped.)
- A narrow recovery race where the coordinator can start a new
  dynomited a beat before the SIGKILLed one's file descriptor is
  fully reaped by the kernel. noxu's flock rejects the early
  opener -- which is the correct, safe behaviour. The injector's
  retry then succeeds once the fd is reaped.

Neither is a noxu defect. noxu's flock did exactly its job:
prevent two writers on one environment.

## Why the workarounds were wrong

- Removing `noxu.lck` while a live process holds an `flock` on the
  inode is **dangerous**: unlinking the path and letting a second
  process `create` + `flock` a brand-new inode lets both processes
  "hold the lock" on different inodes and open the same environment
  concurrently -- two writers, corruption risk. It only avoided
  harm in chaos because the coordinator pre-kills stale processes
  first, which is the actual single-owner guarantee.
- The dyniak `flock` owner-lock merely duplicated the `flock` noxu
  already takes. Redundant, and it muddied the ownership model.

Both reverted. The correct invariant is the one that already
existed: the coordinator guarantees a single owner per host by
pre-killing stale processes before every (re)start, and noxu's own
`flock` enforces mutual exclusion if anything slips through.

## Lesson

A sub-100% chaos success rate must trace to an induced fault. When
the "induced fault" explanation (stale lock from SIGKILL) did not
hold up under a direct test, the real cause turned out to be
operator error (stale coordinators) plus a benign, self-correcting
recovery race -- not an engine bug. Investigate before patching.
