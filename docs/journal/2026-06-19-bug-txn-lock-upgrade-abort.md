# BUG: dyniak aborts (process death) under multi-key transaction load -- illegal noxu lock upgrade

Found 2026-06-19 by the consistency-verification harness (W1) on a
single-node dyniak instance, no faults injected. SECOND and more
serious of the two bugs the harness found on its first runs (the
first, the storage-format mismatch, is documented in
`2026-06-19-bug-txn-storage-format.md` and is fixed).

## Symptom

Under sustained multi-key transaction load through
`POST /buckets/<b>/transactions`, the `dynomited` process ABORTS
(hard process death, not a handled error). The W1 list-append
workload reproduces it deterministically within seconds: of ~268k
transactions, ~3 commit before the process dies. After death every
client sees connection-refused.

## Root cause (deterministic; full backtrace in cc-smoke/repro.log)

Three layers cascade into a process abort:

1. dyniak `put_object_in` (inside a transaction) fans one logical
   object write out to several noxu records: it first runs
   `clear_forward_for`, which CURSOR-SCANS the reverse-index region
   (taking a RANGE / `RangeInsert` lock), then `db.put`s the primary
   and forward-2i records (taking `Write` locks). When the lock
   state requires upgrading a `RangeInsert` lock to a `Write` lock on
   an overlapping region, noxu's lock manager rejects it:

     noxu-txn-6.4.1/src/thin_lock_impl.rs:53
       panic!("Illegal lock upgrade from RangeInsert to Write")

   This is reached specifically when a transaction OVERWRITES a key
   that already has secondary-index entries (so `clear_forward_for`
   does real work) -- i.e. the read-modify-write / repeated-append
   pattern, not first-time inserts. A single-key, no-2i workload does
   NOT trigger it; 500 sequential distinct puts and 2400 concurrent
   single-key RMW txns both ran clean. The list-append workload
   (overwriting indexed-ish keys repeatedly) triggers it reliably.

2. That `panic!` poisons the transaction's lock `Mutex`.

3. The transaction is then dropped on the error path
   (`NoxuDatastore::transaction` -> `txn.abort()` ->
   `Transaction::drop`). `Transaction::abort`
   (noxu-db-6.4.1/src/transaction.rs:599) does
   `inner.lock().unwrap()` on the now-poisoned mutex, which panics
   INSIDE the destructor:

     called `Result::unwrap()` on an `Err` value: PoisonError { .. }

   A panic in a `Drop` during unwinding is a non-recoverable
   "panic in a destructor during cleanup" -> Rust aborts the process.

## Impact

CRITICAL. dyniak's headline feature -- multi-key atomic transactions
-- crashes the entire node under realistic load (repeated
read-modify-write on keys with secondary indexes). Present in the
published 1.0.0 / 1.1.0 line. A customer using `POST /transactions`
will take node aborts.

## Layers implicated and fix direction

The root cause is precisely located in noxu's lock-upgrade matrix.
The `RangeInsert` lock is NOT acquired by dyniak's read; it is
acquired by noxu INTERNALLY as a next-key (phantom-prevention) lock
on the SUCCESSOR key's LSN whenever a transaction inserts a key
(`noxu-dbi-6.4.1/src/cursor_impl.rs`: "Acquires a `RangeInsert` lock
on the successor key's LSN for a new [insert]"). In a multi-key
batch that touches adjacent keys (the list-append workload writes
keys 0..5 repeatedly), inserting key `k` range-locks the LSN of its
successor; a later write in the same batch to that successor key
then needs a `Write` lock on an LSN this transaction already holds
as `RangeInsert`.

noxu's own matrix is self-contradictory on this case
(`noxu-txn-6.4.1/src/lock_type.rs`):
- `get_conflict`: `(RangeInsert, Write) => Allow` -- a RangeInsert
  holder does NOT conflict with another transaction's Write.
- `get_upgrade`: `(RangeInsert, Write) => Illegal` -- but the SAME
  transaction upgrading its own RangeInsert to Write is declared
  illegal and `panic!`s.

A RangeInsert lock is weaker than a Write lock on the same key;
upgrading it should be a normal escalation (`WritePromote`), not
`Illegal`. This is the noxu fix.

- noxu A (lock-upgrade matrix): `(RangeInsert, Write)` in
  `LockType::get_upgrade` should be `WritePromote`, not `Illegal`.
- noxu B (abort robustness): regardless of A, `Transaction::abort`
  unwraps a poisoned lock inside `Drop`
  (`noxu-db-6.4.1/src/transaction.rs:599`), turning any prior panic
  into a process abort. abort-in-drop must be panic-safe so a
  recoverable engine error never escalates to process death.

### dyniak-side mitigation is NOT viable for this bug

An earlier attempt to reorder `put_object_in` /
`delete_object_in` (take the primary write lock before the
reverse-index read) was made and REVERTED: it did not fix the
crash, because the `RangeInsert` lock is not produced by the
reverse-index read -- it is produced internally by noxu on
successor-key LSNs during inserts, which dyniak does not control.
In any multi-key batch over adjacent keys, some key is both a
successor-of-one-insert and a target-of-another-write, so no
dyniak-level operation ordering avoids the upgrade. The fix must be
in noxu. (The reorder was reverted rather than kept because an
unproven reordering of safety-critical storage code that does not
fix the bug is a liability, not a mitigation.)

The correct fix is the noxu release (A + B). This bug doc is the
reviewer's reference for incorporating that release.

## Repro

1. Start a `data_store: dyniak` node with `riak.http_listen`.
2. Run `scripts/consistency/txn_history_workload.py --http-port
   <p> --bucket cc --keys 6 --duration-secs 30 --out h.jsonl`.
3. The process aborts within seconds; the log shows
   "Illegal lock upgrade from RangeInsert to Write".

## Provenance

The chaos rig never caught this: its Riak workload uses PBC put/get
of random keys (mostly first-time inserts, and PBC's raw-fallback
masked the storage-format bug), and it does not drive the HTTP
multi-key transaction endpoint with overwrite-heavy read-modify-write
traffic. W1's list-append workload -- the standard Jepsen pattern --
exercises exactly the overwrite-with-existing-index path that
provokes the noxu illegal upgrade. This is the core argument for the
initiative: the headline transactional feature had a process-abort
bug reachable by ordinary use, undetected until a transaction-shaped
workload drove the real code.
