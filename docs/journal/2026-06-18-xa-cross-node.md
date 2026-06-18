# 2026-06-18 -- Cross-node XA two-phase commit

Stage: stage/xa-cross-node (off main `a7874fc`)
Author: Greg Burd <greg@burd.me>

## Summary

Extended dyniak's XA two-phase commit so a transaction branch can
live on a REMOTE peer reached over the dnode peer plane, not just on
a local in-process Noxu environment. The local-only coordinator in
`crates/dyniak/src/datastore/xa.rs` is unchanged and all its tests
stay green; the network leg is additive.

## What landed

* `crates/dynomite/src/proto/dnode.rs`: five new `DmsgType`
  discriminators on the existing dnode codec --
  `XaPrepare = 17`, `XaVote = 18`, `XaCommit = 19`,
  `XaRollback = 20`, `XaAck = 21`. `dmsg_process` bypasses all five
  (they are consumed by the dyniak XA handler, not the data plane),
  with the dispatch test updated to pin that.

* `crates/dyniak/src/datastore/xa_wire.rs`: a stdlib-only,
  length-prefixed wire codec for the four phase payloads
  (`XaPrepareMsg`, `XaVote`, `XaResolveMsg`, `XaAckMsg`) plus a
  portable `WireXid` and `XaWriteOp`. Big-endian lengths, `u8`
  discriminators. Round-trip and exhaustive-truncation tests.

* `crates/dyniak/src/datastore/xa_net.rs`: the network leg.
  * `XaTransport` trait -- the seam, one async method per phase
    (`prepare -> vote`, `commit -> ack`, `rollback -> ack`).
  * `RemoteXaBranch` -- a branch reached through an `XaTransport`.
  * `XaBranch` enum -- `Local(Box<XaParticipant>)` or
    `Remote(RemoteXaBranch)`; the cross-node coordinator drives a mix.
  * `XaPeer` -- receiver side. Owns one `XaParticipant` per
    environment name; turns an inbound prepare into
    start + apply + end + prepare and returns the vote, and an
    inbound commit / rollback into the idempotent resolution.
  * `InDoubtLog` -- durable append-only log (fsync per record) of
    branches that voted Ok but whose commit could not be confirmed.
  * `CrossNodeCoordinator` -- async coordinator running the protocol
    over `XaBranch`es, reusing the existing `XaParticipant` and
    format-id from `xa.rs`.
  * `DnodeXaTransport` + `serve_xa_peer` -- the production transport
    that frames each phase as a dnode message over TCP and the
    receiver loop that serves an `XaPeer` on a listener.

* `crates/dyniak/src/datastore/xa.rs`: `XaParticipant` gained public
  `xa()` and `apply_op()` accessors (so the cross-node coordinator and
  the peer handler drive the identical noxu XA calls), the format-id
  constant became `pub(crate)`, and the module doc was rewritten to
  reflect that cross-node is now implemented (the recovery-scan
  boundary is the only remaining scoping note).

## Protocol (presumed abort, forward commit)

Identical decision logic to the local coordinator; only delivery
changed.

1. Partition the batch across branches by the caller's route fn.
2. Phase 1 (prepare): local branches prepare inline; remote branches
   prepare concurrently via `join_all` over the async transport. One
   prepare round-trip carries the branch's writes
   (start + apply + end + prepare on the receiver) and elicits the
   vote.
3. Decision: all `Ok`/`ReadOnly` -> commit; any `Abort` vote OR any
   transport error/timeout in prepare -> presumed abort -> roll back
   every branch that may have prepared.
4. Phase 2 (commit): commit every `Ok` branch. `force_abort` rolls
   back all prepared branches (the local code's deliberate-abort
   path).

## In-doubt handling (the crux)

* Prepare-phase peer timeout or transport error == abort vote ==
  global rollback. (Presumed abort.)
* Commit-phase peer failure is NOT an abort. The branch voted Ok and
  is durably prepared on the peer (noxu's `xa_prepare` writes a
  fsynced `TxnPrepare` WAL frame), so the only correct resolution is
  forward. `commit_branch` retries `XA_COMMIT` with bounded,
  exponentially-backed-off attempts (`RetryPolicy`, default 5).
* If retry is exhausted, the branch is committed-in-doubt: the
  coordinator durably records `(xid, env)` in the `InDoubtLog`
  (`fsync` before returning) and surfaces an in-doubt error. It NEVER
  rolls back a branch that voted Ok in the commit phase -- that would
  violate atomicity.
* Idempotency: a peer that receives `XA_COMMIT`/`XA_ROLLBACK` for an
  xid it has already resolved finds no branch and noxu returns
  `XaError::NotFound`; `XaPeer::resolve` maps that to success, so a
  coordinator retry never double-applies or errors.

## Recovery-scan boundary (scoped follow-up)

Implemented: the durable in-doubt log (write + read) and the bounded
commit retry, which together resolve transient peer unavailability
within one coordinator run. Left as the remaining boundary: the
automatic cold-restart recovery scan -- a coordinator that has itself
restarted re-reading `InDoubtLog::load()` and re-driving the commits,
paired with a peer whose noxu env recovered a prepared branch via
`xa_recover()`. The durable record is already written and readable, so
the scan is a thin loop on top; `CrossNodeCoordinator` does not run it
on construction. The test
`commit_phase_exhausts_retry_then_in_doubt_log_drives_recovery`
demonstrates a manual recovery pass (re-read the durable record,
re-drive `handle_commit`) completing the transaction atomically.

## Tests (10 new + codec)

`crates/dyniak/tests/xa_cross_node.rs` (8):
* `mock_cross_node_commit_is_atomic` -- local + remote, both Ok.
* `dnode_loopback_commit_is_atomic` -- REAL dnode TCP loopback, two
  in-process nodes, multi-key txn spanning both, atomic commit.
* `dnode_loopback_abort_leaves_nothing_visible` -- real loopback,
  remote branch routed to an env the peer does not own -> Abort vote
  -> neither branch's writes visible.
* `prepare_phase_peer_failure_rolls_back_everything` -- prepare
  transport error -> global rollback.
* `commit_phase_timeout_recovers_within_retry_budget` -- 2 commit
  failures then success; retried 3x; no in-doubt record.
* `commit_phase_exhausts_retry_then_in_doubt_log_drives_recovery` --
  retry exhausted -> durable in-doubt record (re-read from a fresh
  `InDoubtLog`) -> recovery pass completes commit; branch never
  rolled back.
* `peer_commit_is_idempotent` / `peer_rollback_is_idempotent` --
  deliver the resolution twice; single apply, no error.

`xa_wire` unit tests (5): prepare/vote/resolve/ack round-trips,
exhaustive truncation rejection (never panics), TxnOp lowering.

All existing local-XA tests stay green (`commit_spans_two_branches`,
`commit_then_read_after_reopen_is_durable`, `force_abort_*`, etc.).

## Verification

* `cargo build -p dyniak --all-targets --features noxu --locked` clean
* `cargo build -p dynomite-engine --all-targets --locked` clean
* `cargo nextest run -p dyniak --features noxu`: 566 passed
* `cargo test --doc -p dyniak --features noxu`: 47 passed
* `cargo clippy -p dyniak --all-targets --features noxu -- -D warnings`
  clean; `cargo clippy -p dynomite-engine --all-targets -- -D warnings`
  clean
* `cargo fmt -p dyniak -- --check` clean
* ASCII-only; no port comments; no stubs/todos.

## Open questions

* `dynomite-engine` has no `noxu` feature (it routes through
  `riak-storage`); the verify command in the brief applies
  `--features noxu` to both packages, which errors on the engine. Used
  per-package feature flags instead. Noted for the lead in case the
  CI gate needs the same split.
