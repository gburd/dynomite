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

This spans noxu and dyniak:

- noxu A (lock manager): RangeInsert -> Write is treated as an
  illegal upgrade and `panic!`s. Either the upgrade should be legal
  (a range-insert lock subsumed by a write lock on the same key is a
  normal 2PL escalation) or the engine should return an error the
  caller can handle, NOT panic. This is a noxu fix.
- noxu B (abort robustness): `Transaction::abort` unwraps a poisoned
  lock inside `Drop`, turning any prior panic into a process abort.
  abort-in-drop must be panic-safe (use the poisoned guard's
  `into_inner`, or guard against poison) so a recoverable engine
  error never escalates to process death. noxu fix.
- dyniak C (lock ordering): `put_object_in`'s order
  (clear_forward_for range-scan THEN primary/2i writes) provokes the
  illegal upgrade. dyniak can avoid it by acquiring the primary write
  lock before the reverse-index range scan, or by not range-scanning
  the reverse index under the same txn lock scope. A dyniak-side
  ordering fix likely sidesteps the noxu bug without waiting on a
  noxu release.

The robust fix is BOTH: noxu must not panic on a lock-upgrade
conflict and must not abort the process from a destructor; dyniak
should order its lock acquisition to avoid the upgrade. Either alone
mitigates; both are correct.

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
