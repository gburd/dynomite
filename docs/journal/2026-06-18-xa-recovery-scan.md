# 2026-06-18 -- Cross-node XA cold-restart recovery scan

Stage: stage/xa-recovery-scan (off main `fa606b0`)
Author: Greg Burd <greg@burd.me>

## Summary

Completed dyniak's cross-node XA by implementing the cold-restart
recovery scan that the prior cross-node slice
(`docs/journal/2026-06-18-xa-cross-node.md`) scoped as the remaining
boundary. A coordinator that restarts now re-reads its durable
in-doubt log and re-drives every unconfirmed commit forward to
completion, retiring each record once the commit is confirmed.

The durable log (write + read) and the bounded in-run commit retry
already existed; this slice adds the automatic startup scan on top.

## What landed

`crates/dyniak/src/datastore/xa_net.rs`:

* `InDoubtLog` gained tombstone-on-resolve. `record()` appends a `+`
  line (a new in-doubt branch); the new `resolve()` appends a `-` line
  (a tombstone retiring an earlier record for the same xid+env).
  `load()` nets tombstones against records in append order and returns
  only the still-unresolved branches, preserving first-write order.
  The log stays strictly append-only -- no temp-file rename.
* `RecoveryReport { recovered, still_in_doubt, errors }` -- the per-
  scan outcome counts (they sum to the number of records examined).
* `CrossNodeCoordinator::recover_in_doubt(&self) -> Result<RecoveryReport, _>`
  (async): loads the in-doubt records, maps each record's `env` to the
  branch that owns it, and re-drives the commit over the same path
  phase 2 uses (local branch commits inline; remote branch re-sends
  `XA_COMMIT` through its `XaTransport` with the bounded retry/backoff
  policy). A confirmed commit retires its record with `resolve()`; a
  record whose peer is still unreachable is left in place (counted
  `still_in_doubt`); a record with no owning branch / failed tombstone
  write is left in place (counted `errors`). The scan never rolls a
  prepared branch back.
* `CrossNodeCoordinator::new_with_recovery` /
  `new_with_recovery_retry` (async): build the coordinator and run one
  blocking recovery scan before returning, paired with the scan's
  `RecoveryReport`. This is the server boot entry point. `new` is
  unchanged and never scans, so in-memory and test coordinators see no
  behaviour change.
* Module docs rewritten: the "Recovery-scan boundary" section became
  "Cold-restart recovery scan" describing the implemented path.

## Design choices

* **Retire = tombstone, not compaction.** Tombstoning keeps the log
  append-only (no temp+rename) and is crash-safe: a crash after the
  commit lands on the peer but before the tombstone is written leaves
  the record live, so the next scan re-drives the commit -- which is
  idempotent on the peer (`xa_commit` reports `NotFound` for an
  already-resolved branch, mapped to success), so the replay is a
  no-op. A torn final line fails to parse and `load()` skips it,
  leaving the matching record live (same safe replay). Compaction
  would also have worked; tombstoning is the simpler-correct option
  for an append-only log.
* **Startup = blocking scan at construction.** In-doubt sets are small
  and a re-driven commit that already landed is a cheap idempotent
  no-op, so a bounded blocking scan before normal operation is the
  simplest safe wiring. Gated to the boot path
  (`new_with_recovery`); `new` does not scan, so the many test-only
  coordinator constructions are untouched.
* **Forward only.** Recovery only ever drives a prepared (voted-Ok)
  branch to commit (presumed commit). It never rolls one back -- that
  would risk atomicity loss. The commit-phase code already never rolls
  back a prepared branch; the scan inherits that.
* **Re-runnable / idempotent.** A scan with no surviving records is a
  no-op; a scan while a peer is down leaves the records and reports
  them `still_in_doubt`; a later scan after the peer returns recovers
  them. Re-driving a commit that already landed is idempotent success.

## Tests (5 new in `crates/dyniak/tests/xa_recovery.rs`)

* `dnode_cold_restart_recovery_drives_commit_and_retires_record` --
  real dnode loopback. The branch is durably prepared on the peer's
  env; the original coordinator could not confirm the commit (listener
  torn down) and recorded the branch, then is dropped (restart). A new
  listener over the same live env plus a FRESH coordinator over the
  same in-doubt log path runs `new_with_recovery_retry`: the commit is
  driven home (data visible), the record is retired, and a second
  `recover_in_doubt()` is a no-op.
* `recovery_with_peer_down_leaves_record_then_recovers_when_up` --
  recovery with the peer unreachable reports `still_in_doubt: 1` and
  leaves the record; flipping the peer back up and re-running recovers
  it. Re-runnable.
* `recovery_redrive_of_already_committed_branch_is_idempotent` -- the
  commit landed before the crash; recovery re-drives `XA_COMMIT`, the
  peer returns success, the record retires, no double-apply.
* `tombstone_load_returns_only_unresolved` -- resolving one of three
  records leaves the other two on `load()`, in first-write order;
  resolving the rest empties the log net.
* `crash_mid_resolve_redrives_safely` -- the commit landed but the
  tombstone was never appended (crash between commit and resolve); the
  record is still live, and recovery re-drives the idempotent commit
  and retires it.

The existing
`commit_phase_exhausts_retry_then_in_doubt_log_drives_recovery` and all
other cross-node + local XA tests stay green.

## Verification

* `cargo build -p dyniak --all-targets --features noxu --locked` clean
* `cargo build -p dyniak --all-targets --features noxu,search --locked`
  clean
* `cargo build -p dyniak --all-targets --features noxu,wasm,search
  --locked` clean
* `cargo nextest run -p dyniak --features noxu`: 591 passed
* `cargo test --doc -p dyniak --features noxu`: 49 passed
* `cargo clippy -p dyniak --all-targets --features noxu -- -D warnings`
  clean
* `cargo clippy -p dyniak --all-targets --features noxu,wasm,search --
  -D warnings` clean
* `cargo fmt -p dyniak -- --check` clean
* ASCII-only; no port comments; no stubs/todos; `forbid(unsafe_code)`.
