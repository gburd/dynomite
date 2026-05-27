# 2026-05-27 - hashtree migration survey

## Brief

The user asked: now that `crates/hashtree/` exists as a generic
merkle/hashtree primitive, are there other places in dynomite where
we should migrate ad-hoc tree code onto it?

## Survey

Code search (`grep -rn 'merkle\|MerkleTree\|hashtree\|Merkle'`) over
`crates/`:

* `crates/dyn-riak/src/aae/tictac.rs` and friends - the existing
  TicTac AAE tree. The hashtree-extract worker correctly identified
  that this can NOT be expressed in terms of the new generic crate
  (its semantics are 2-level time-buckets, `(bucket, key, vclock)`
  entries, FNV-1a XOR aggregation, hand-rolled v1 binary snapshot)
  and left it in place.

* `crates/dynomite/src/entropy/driver.rs` line 14 - a comment
  describing how a *richer* embedder could ship per-range merkle
  digests via the `SnapshotSource` trait. The dynomite entropy
  protocol itself streams **full snapshots** over an AES-128-CBC
  encrypted channel; there is no merkle tree on the wire today.
  Migrating the embedder-level helper would be a brand-new feature,
  not a migration.

* `crates/dynomite/src/hashkit/md5.rs` and friends - per-key hash
  helpers (MD5, murmur3, jump, etc.). These are key->bucket
  mappings, not tree structures. Not a migration target.

* `crates/dynomite/src/msg/response_mgr.rs` - per-response u32
  checksum, not a tree. Not a target.

* gossip - peer state digest is a flat per-peer hash, not a merkle
  structure. Not a target.

## Verdict

**No migrations to do.** The generic `crates/hashtree/` crate stays
as a forward-looking primitive available for future feature work
that genuinely needs a (key, value_hash) merkle tree. Examples of
plausible future consumers:

1. A simplified entropy reconciliation protocol that ships range
   digests instead of full snapshots (saves bandwidth on idle
   ranges). Would replace today's full-snapshot streaming for
   embedders that opt in.
2. A future `dynomite::cache_digest` exposing a per-bucket merkle
   root for clients that want to do client-side change detection.
3. A potential future replication protocol where the destination
   ships a per-range merkle root and the source replies with a
   diff.

None of these are queued; the crate's only current consumer is its
own test suite. If a future consumer materializes, the migration
target is THAT consumer (use `hashtree` from day one), not a
retrofit of any existing code.

## TicTac stays

The existing `dyn-riak::aae::tictac` code stays. It is
purpose-built for Riak-style AAE with sibling-aware merge and
time-bucket aggregation, and the hashtree-extract worker correctly
flagged that bending the generic API to fit it would be a net
loss. Future AAE refactors (wave-2B onto gen-fsm) preserve TicTac
unchanged.
