# 2026-07-05 -- Delta-state CRDT prototype

Author: Greg Burd <greg@burd.me>
Branch: `proto/delta-crdt` off `main` @ `2edfd65`
Worktree: `/home/gburd/ws/wt-delta-crdt`

## Goal

Cut replication / anti-entropy bandwidth by shipping delta-states
instead of full CRDT state, as a near-drop-in upgrade of dyniak's
existing state-based CRDTs. Prototype, but real and gated against the
Section 6.5 DST + consistency merge gate.

## Which CRDT, and why

Converted the **observed-remove set** (`crates/dyniak/src/datatypes/
set.rs`, `OrSet`) end to end. It is the canonical delta-CRDT example
(add-wins OR-Set with a dot store) and the existing implementation
already keys per-element metadata by `(actor, counter)` dot tags --
those tags ARE the causal context a delta needs, so no separate
dot-context type had to be introduced. The map is a composition of
these primitives; converting the set first is the load-bearing step.

New code:

* `crates/dyniak/src/datatypes/delta_set.rs`
  * `DeltaOrSet` -- delta-state OR-Set. Same add-wins observed-remove
    semantics, same `(actor, counter)` dots, same `Crdt::merge` /
    `value` behaviour as `OrSet`.
  * `add` / `remove` are **delta-mutators**: they mutate in place AND
    return an `OrSetDelta`, the join-irreducible fragment of the
    change (a single new dot for an add; the observed dots moved into
    the tombstone side for a remove).
  * `OrSetDelta` -- a delta is literally a sparse `DeltaOrSet` state
    (a subset of the element tag maps). "Join a delta" and "join a
    full state" are the *same* least-upper-bound operation, so there
    is no second lattice to keep in sync. This is the property the
    papers hinge on and the DST model + property tests prove.
  * `merge_delta` -- join a delta or a delta-interval. Idempotent,
    commutative, associative.
  * `DeltaBuffer` -- per-replica buffer of produced deltas, each
    stamped with a monotone local sequence number, plus per-peer
    acknowledged sequence numbers. `interval_since` collapses a run
    of buffered deltas into one delta-interval; `compact` bounds
    growth by dropping deltas every known peer has acked.

* `crates/dyniak/src/aae/delta_ship.rs`
  * `plan_shipment(state, buf, peer)` -- the shipping decision:
    * first contact (no ack point for `peer`) -> `Shipment::FullState`
      (graceful degradation to a state-CRDT; full-state shipping is
      the same join, just larger);
    * otherwise the delta-interval since the peer's ack point, or
      `Shipment::UpToDate` when the peer is already current.
  * `apply_shipment` -- apply either a delta or full state (same
    join) and return the sequence to acknowledge.

## AAE hook -- how the delta-interval ships

The tictac tree (`crate::aae::tictac`) and the three-phase exchange
still decide *when* and *for which key* a reconciliation runs -- I
did not touch the tree structure or the exchange codec (MST worker
owns those). The delta upgrade changes only the *payload*: instead of
shipping the whole CRDT value for a divergent key, `plan_shipment`
computes the delta-interval since the destination's last ack and
ships that; `apply_shipment` joins it. This is why the change is
near-drop-in -- the divergence detection is unchanged, only the bytes
carried differ, and full-state shipping remains the correct fallback.

## Bandwidth measurement

`crates/dyniak/tests/delta_crdt_bandwidth.rs` drives a realistic
workload (50 reconciliation rounds, ~20 adds/round with periodic
removes -- a large set that is stable with modest per-round churn,
which is the shape AAE sees) and compares total bytes-on-wire using
the same `wire_len` accounting for both strategies:

```
state-shipping = 873340 bytes
delta-shipping =  34940 bytes
state/delta ratio = 25x
```

The win compounds with the number of rounds: state-shipping re-sends
the entire (growing) set every round; delta-shipping sends only the
per-round churn after the first full-state round. The test asserts
>3x as a floor so it stays green under workload tweaks; the measured
ratio is ~25x.

## DST model (Section 6.5, item 1)

`crates/model-tests/src/delta_crdt.rs`, wired into `scripts/model.sh`
via the crate's default test run. An abstract stateright state machine
over a lossy / reordering / duplicating delta channel:

* **Strong eventual consistency** (`always`): any two replicas that
  have delivered the same *set* of deltas (any order, with
  duplicates) hold equal presence sets.
* **Monotonicity** (`always`): a replica's dot set never shrinks --
  the lattice-law consequence that gives a stable fixpoint.
* **Convergence reachable** (`sometimes`): the fully-converged state
  is reachable, so the model is not vacuously consistent.
* **Negative control** (`broken_mutator_violates_sec`): a remove
  mutator that drops the causal context -- it ships an element-keyed
  "delete whatever dots you hold now" instruction instead of the
  observed dots, making the join depend on the receiver's current
  state and therefore on delivery order. It is NOT join-irreducible.
  The checker finds an SEC counterexample (equal delivered sets,
  different presence), proving the model has teeth. The correct model
  under the identical channel has no such counterexample.

The join the model runs (set-union of dots) is the same join the
production `merge_delta` runs; the model re-expresses it rather than
linking `dyniak`, per the crate convention, so the bounded BFS stays
small (correct model: hundreds of states, sub-second).

## Property tests (hegel)

`crates/dyniak/tests/delta_crdt_properties.rs`, 8 tests x 256 cases:

* `delta_join_is_commutative` / `_associative` / `_idempotent` --
  the lattice laws hold on deltas (a delta is a state-lattice value).
* `delta_merges_equal_full_state_merge` -- the delta-mutation
  theorem: N delta-merges == one full-state merge.
* `delta_merge_matches_state_merge_across_two_replicas` -- delta and
  state reconciliation converge to the same value on both replicas
  (SEC).
* `shuffled_duplicated_deltas_converge` -- out-of-order + duplicated
  delivery of the same delta set equals the full-state producer.
* `add_wins_over_concurrent_remove` -- add-wins semantics preserved
  across the delta path.
* `value_projection_matches_presence` -- `value()` agrees with
  `contains()`.

Plus 11 unit tests in `delta_set.rs` and 4 in `delta_ship.rs`.

## Consistency harness (Section 6.5, item 2)

The Elle-style list-append / register history checker in
`scripts/consistency/` models linearizable single-object registers
and read-atomic multi-object transactions. An add-wins OR-Set is
neither: it has no total order to linearize against (concurrent
add/remove is a defined non-conflict, not an anomaly) and its
convergence is a join-semilattice property, not a
dependency-cycle-freedom property. A list-append history does not
express set-CRDT semantics.

Per the task brief and Section 6.5's own framing, a CRDT's
correctness is the convergence / lattice property, which the DST
model checks exhaustively over the adversarial channel and the
property tests check over 256+ generated cases each. The negative
control proves the DST model is not vacuous. This is the documented
and acceptable path for a CRDT convergence change: DST model +
property tests carry the gate; the register/list-append consistency
harness is not applicable to set-CRDT semantics.

## What a production rollout would still need

This is a prototype. Before shipping:

1. **Wire codec + framing.** `OrSetDelta` needs a real encode/decode
   into the exchange frame (a new phase or a flag on KEY-SYNC).
   `wire_len` is an *accounting* stand-in for the bench, not a
   serializer.
2. **Ack transport + persistence.** The `DeltaBuffer` ack points and
   the buffer itself must survive restart and travel on the wire
   (piggybacked on the exchange). A lost ack degrades to a larger
   interval or a full-state resync, both correct but wasteful.
3. **Buffer bounding under peer loss.** `compact` drops deltas all
   known peers have acked; a permanently-down peer would pin the
   buffer. Production needs a high-water cutoff that falls back to
   full-state resync for a lagging peer (the graceful-degradation
   path already exists; it needs a trigger).
4. **The other CRDTs.** Map, counter, register, flag, HLL are still
   state-based. The map is the next conversion (it composes the set
   delta with per-field deltas); the counter is trivial (per-actor
   delta); the register and flag follow the set pattern.
5. **Causal-consistency guarantee across keys.** This prototype gives
   per-key causal consistency (the dots are the context). A causal+
   delta-CRDT across keys would need a shared causal context
   (a dot-context / ITC frontier); I READ `itc.rs` but did not touch
   its core -- a cross-key context belongs in a new file coordinated
   with the HLC worker.
6. **Integration with the repair scheduler.** `plan_shipment` /
   `apply_shipment` are the decision + apply primitives; the repair
   scheduler (`aae::repair`) must call them per divergent CRDT key
   and thread the ack back.

## Gates (all green)

```
cargo build -p dyniak --features noxu --locked           OK
cargo build -p model-tests --locked                      OK
cargo nextest run -p dyniak --features noxu              734 passed
cargo test -p model-tests                                 18 passed
cargo clippy -p dyniak -p model-tests --all-targets \
    --features noxu -- -D warnings                        OK
cargo clippy -p dyniak --all-targets \
    --features noxu,wasm,search -- -D warnings            OK
cargo fmt -p dyniak -p model-tests -- --check             OK
cargo test -p dyniak --features noxu --doc                53 passed
bash scripts/model.sh                                     18 passed
```

Tests added: 11 (delta_set unit) + 4 (delta_ship unit) + 8 (delta
property) + 1 (bandwidth) + 2 (DST model) = 26.
