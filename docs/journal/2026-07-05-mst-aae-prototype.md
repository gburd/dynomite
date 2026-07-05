# 2026-07-05 - Divergence-proportional AAE reconcile (MST prototype)

Branch: `proto/mst-aae` off `main` @ `2edfd65`
Worktree: `/home/gburd/ws/wt-mst-aae`
Author: Greg Burd <greg@burd.me>

## Goal

Make active anti-entropy (AAE) repair bandwidth proportional to
the *difference* between two replicas' key sets, not to the total
dataset size, so a globally-distributed cluster with frequent
membership churn recovers fast. Prototype behind the existing AAE
exchange interface, gated by the mandatory DST + Elle merge gate
(AGENTS.md Section 6.5).

## MST vs IBLT: the choice

Chose the **Merkle Search Tree** (Auvolat and Taiani, SRDS 2019).

* **MST composes with the state/CRDT model.** The AAE reconcile
  is a set-convergence problem; an MST is a search tree whose
  structure is a pure function of the key set, so two replicas
  with the same keys hold byte-identical trees and a top-down
  hash-pruned diff walks only divergent subtrees. That range-diff
  walk fits behind the existing tree interface (`root()` +
  recursive `diff()`) the same way the shipped Tictac tree does.
* **IBLT needs a good difference-size estimate.** An Invertible
  Bloom Lookup Table (Eppstein et al., SIGCOMM 2011) is simpler
  on the wire (one fixed-size sketch) but the sketch must be sized
  to the expected symmetric difference; undersize it and decode
  fails, oversize it and you have paid the bandwidth you were
  trying to save. Under membership churn the difference size is
  exactly the quantity that is hard to predict, so IBLT would need
  a separate estimator (or a resize-and-retry round trip). The MST
  self-sizes: the walk cost falls out of the tree structure with
  no prior estimate.

The prototype implements MST. IBLT remains a reasonable second
option if a future need is "one-shot fixed sketch over a
well-estimated diff".

## Structure

`crates/dyn-hashtree/src/mst.rs` -- a new module in the existing
hashtree crate (the right home: it already hosts the generic
merkle primitive, blake3, and hegeltest, and pulls no new deps).

* Each key's *level* is the count of leading base-`B` zero digits
  of `blake3(key)` (default `B = 16`, one hex digit per level).
  A fraction `1/B` of keys sit one level up, `1/B^2` two levels
  up: a set-deterministic balanced layout.
* A node at level `L` holds the keys at exactly level `L` (sorted)
  interleaved with `k+1` child subtrees at level `L-1` covering
  the ranges between its keys.
* A node hash is a blake3 Merkle digest over its `(key,
  value_hash)` pairs and its child hashes. The root is therefore a
  pure function of the `(key -> value_hash)` map: same keys,
  same root, any insertion order.
* Built bottom-up from a sorted key set (the shape the AAE storage
  fold produces). Immutable once built; the AAE path rebuilds it
  per reconcile, matching the shipped Tictac rebuild model.

`Mst::diff(&other)` walks both trees top-down, pruning any subtree
pair whose hashes match (one comparison prunes a whole matching
subtree), and returns an `MstDiff` carrying the symmetric
difference split into `only_here` / `only_there`, plus the count
of node-hash comparisons (the bandwidth proxy).

## Divergence-proportional reconcile behind the AAE interface

`crates/dyniak/src/aae/mst_reconcile.rs` (gated `#[cfg(feature =
"noxu")]`, like the existing `noxu_fold`):

* `build_mst(db)` walks the datastore's primary records via the
  same `fold_primary` cursor the Tictac rebuild uses, mapping each
  `(bucket, key)` to a flat composite key `bucket || 0x00 || key`
  and each value to `blake3(value)`.
* `reconcile_pull(local_db, local_mst, peer_mst, source)` diffs
  the two trees, then for each key the peer has that the local
  side lacks-or-differs, fetches the object from the `ObjectSource`
  (a trait; in-process `DatastoreSource` for tests, a wire peer in
  production) and applies it locally with `put_object`. One run
  repairs one direction; the AAE scheduler already drives both
  peers, so the pair converges after both directions run.

**Fallback / opt-in:** a new `ReconcileMode` enum on `ConfAae`
(lives in `config.rs`, not noxu-gated) defaults to
`ReconcileMode::Tictac`. The shipped fixed-grid path is untouched
and remains the default; MST is an alternative selected by
config. A deployment that has not opted in is byte-for-byte
unchanged. The change to the shared exchange surface is additive
(one config field, one new module, new re-exports) so the
parallel delta-shipping hook rebases cleanly.

## Bandwidth measurement (the win)

`crates/dyn-hashtree/tests/mst_bandwidth.rs` builds two replicas
sharing 100k keys, diverges a ratio of them, and reports keys
transferred for the fixed-grid (Tictac, 1024 segments, faithful
reimplementation: a dirty segment ships all its keys) vs the MST.

| dataset | diff | sym_diff | MST xfer | Tictac xfer | MST cmp | inflation |
|--------:|-----:|---------:|---------:|------------:|--------:|----------:|
| 100000  | 0.1% |      100 |      100 |       19142 |     211 | 191x      |
| 100000  | 1%   |     1000 |     1000 |      121942 |    1196 | 122x      |
| 100000  | 10%  |    10000 |    10000 |      200000 |   10701 | 20x       |

The MST transfers *exactly* the symmetric difference at every
ratio. The fixed grid inflates the transfer by the segment
occupancy (~N/1024 keys per dirty segment), so at low divergence
-- the churn-recovery case -- it ships 100-200x more.

Flat-in-dataset-size at a fixed 100-key diff (the headline
property):

| dataset | MST xfer | Tictac xfer |
|--------:|---------:|------------:|
| 10000   |      100 |        2180 |
| 50000   |      100 |        9712 |
| 200000  |      100 |       37896 |

MST stays at 100; the fixed grid climbs with N. This is the
divergence-proportional property: MST cost tracks the diff, the
fixed grid tracks the dataset.

## DST convergence invariant + negative control

`crates/model-tests/src/aae.rs` (wired into `scripts/model.sh`
via the `model-tests` crate). Two replicas hold `(key, version)`
sets; the model reproduces the *pure diff core* the code drives:
`keys_to_pull` = "keys present-or-newer on the source", which is
exactly what `Mst::diff(...).only_there()` yields and what
`reconcile_pull` fetches. Writes are bounded (finite state space);
reconciles are unbounded and pruned when they make no progress.

* **Convergence (liveness):** `eventually` the write budget is
  spent and both replicas hold the identical merged set -- no key
  lost, no spurious key, higher version retained on conflict.
* **Diff bound (efficiency):** `always` a pull moves at most
  `symmetric_difference` many keys.
* **No spurious keys (safety):** `always` a reconcile never
  invents a key neither replica held.
* **Negative control:** `BrokenAae` is the same model with one
  defect -- the diff permanently skips the lowest divergent key
  (as an MST walk would if it pruned a subtree it should have
  descended into). The key is skipped on every reconcile in every
  direction, so it never converges. The checker CATCHES it: the
  `convergence` property has a discovered counterexample. Test
  `broken_diff_fails_convergence` asserts the discovery exists.

The correct model checks green at `n_keys=3..4`, `n_versions=2`,
`writes=3..4`; the broken model is caught. This proves the model
has teeth (not a vacuous pass) as Section 6.5 requires.

## Property tests (hegel)

`crates/dyn-hashtree/tests/mst_properties.rs`, 256 cases each:

* `root_depends_only_on_key_set_not_order` -- same keys, forward
  vs reversed insertion, identical root.
* `self_diff_is_empty` -- `diff(t, t)` is empty.
* `reconcile_yields_union_on_both_sides` -- for arbitrary key
  sets, applying the diff converges both replicas to the A-wins
  union.
* `diff_len_equals_true_symmetric_difference` -- the diff result
  equals the exact symmetric difference computed independently.
* `diff_cost_never_exceeds_full_node_count` -- the comparison
  count never exceeds a full walk of both trees (safety upper
  bound; the *win* -- comparisons << N -- is asserted in the unit
  tests at large N).

## Consistency check note (Section 6.5 item 2)

The Elle-style consistency harness drives the built binary against
its wire API to detect read/write anomalies. A reconcile algorithm
is a *set-convergence* check, not a register/list-append history:
its correctness question is "do two divergent replicas converge to
the identical set", which is precisely what the DST convergence
model + negative control gate. The existing consistency harness
covers register/list-append linearizability of the datastore front
door, which the MST path does not change (it only changes how the
*background repair* computes which keys to exchange; the applied
`put_object` writes go through the same datastore path the
consistency harness already exercises). Per Section 6.5, a change
whose invariant is set-convergence is gated by the DST model; that
is done here. No new consistency-harness workload is warranted for
a prototype that does not alter the front-door write path.

## Tests added

* dyn-hashtree unit: 10 (mst.rs)
* dyn-hashtree property: 5 (mst_properties.rs)
* dyn-hashtree bandwidth: 2 (mst_bandwidth.rs)
* dyn-hashtree doctests: 2 new (crate example + value_hash)
* dyniak unit: 4 (mst_reconcile.rs)
* model-tests DST: 3 (aae.rs: correct x2 + negative control)

Total: 26 new tests. All green.

## Gates run (all green)

* `cargo build -p dyniak --features noxu --locked`
* `cargo build -p dyn-hashtree -p model-tests --locked`
* `cargo nextest run -p dyn-hashtree` -- 40 passed
* `cargo nextest run -p dyniak --features noxu aae::` -- 88 passed
* `cargo test -p model-tests` -- 19 passed
* `cargo test --doc -p dyn-hashtree` -- 3; `-p dyniak --features
  noxu` -- 51
* `cargo clippy -p dyniak -p dyn-hashtree -p model-tests
  --all-targets --features noxu -- -D warnings` -- clean
* `cargo clippy -p dyniak --all-targets --features
  noxu,wasm,search -- -D warnings` -- clean
* `cargo fmt ... -- --check` -- clean
* `bash scripts/model.sh` -- 19 passed
* ASCII scan, todo/port-comment scan -- clean
* No Cargo.lock change (no new dependencies)

## Honest limitations (production gaps)

1. **Rebuild-per-reconcile.** The MST is built fresh from a full
   storage fold each reconcile, inheriting the shipped Tictac
   tree's storage-agnostic-but-rebuild-heavy trade-off. A
   production MST would maintain an incrementally-updated tree
   (the paper's insert/delete ops) so a reconcile is O(diff) end
   to end, not O(N) build + O(diff) diff. The *diff* is
   divergence-proportional today; the *build* is not.
2. **No wire protocol yet.** `reconcile_pull` runs against an
   in-process `ObjectSource`. A production path needs an MST-diff
   wire codec (exchange node hashes level by level, pull only
   divergent subtrees over the network) rather than shipping the
   whole peer tree. The `MstDiff::comparisons` metric is the proxy
   for what that wire cost would be; the actual framing is future
   work. The existing three-phase Tictac exchange FSM is a model
   for how to stage it.
3. **Value = blake3(value), no vclock/DVV.** The MST value side is
   a content digest, so it detects "these bytes differ" but does
   not carry causality. The shipped Tictac path is DVV-aware
   (sibling merge); an MST-backed reconcile would need the same
   DVV plumbing on the apply side to avoid clobbering concurrent
   writes. The prototype applies last-writer-wins by fetch order.
4. **Level distribution assumes a good hash.** blake3 gives a
   uniform level distribution; a degenerate key set cannot skew it
   because levels derive from the hash, not the key bytes. The
   base-16 default gives ~log_16(N) height; not tuned against real
   key-size distributions.
5. **DST domain is small** (3-4 keys, 2 versions, bounded writes)
   for exhaustive BFS. It proves the invariants and catches the
   negative control but does not soak large state spaces; the
   `MODEL_SOAK=1` lane and the hegel property tests cover the
   wider input space.

## Files

* `crates/dyn-hashtree/src/mst.rs` (new)
* `crates/dyn-hashtree/src/lib.rs` (+`pub mod mst;`)
* `crates/dyn-hashtree/tests/mst_properties.rs` (new)
* `crates/dyn-hashtree/tests/mst_bandwidth.rs` (new)
* `crates/dyniak/src/aae/mst_reconcile.rs` (new)
* `crates/dyniak/src/aae/config.rs` (+`ReconcileMode`, field)
* `crates/dyniak/src/aae/mod.rs` (module + re-exports)
* `crates/model-tests/src/aae.rs` (new)
* `crates/model-tests/src/lib.rs` (+`pub mod aae;`)
