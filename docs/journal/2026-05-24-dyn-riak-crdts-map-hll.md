# 2026-05-24 -- dyniak Map and HyperLogLog (CRDT slice 2)

Branch: `stage/dyniak-crdts-map-hll`
Commit base: `9811ffb` (main: "fix(dyniak): make LwwRegister merge commutative on (ts, actor) tie")

## What landed

The two CRDTs deferred from the v0.0.3 first-slice CRDT work:

* `crates/dyniak/src/datatypes/map.rs` -- observed-remove
  map keying typed CRDT fields (counter, OR-set, LWW register,
  EW flag, nested map).
* `crates/dyniak/src/datatypes/hll.rs` -- HyperLogLog
  cardinality estimator (precision = 14 bits, m = 16384
  registers, Murmur3 hash routed through
  `dynomite::hashkit::hash`).

Plus the wire-side schema for both:

* `MapField`, `MapUpdate`, `MapOp`, `ScalarOp`, `ScalarValue`,
  `MapEntry`, `MapValue`, `RegisterOp`, `FlagOp`, `HllValue`
  added to `crates/dyniak/src/proto/pb/datatypes.rs`.
* The previously reserved tags fill in: `DtOp.map_op` (tag 3),
  `DtValue.map_value` (tag 3), `DtUpdateResp.map_value`
  (tag 5).
* `HllOp.adds` renamed to `HllOp.add_value` per brief; no
  external constructions to update.
* `MAP_FIELD_TYPE_COUNTER..MAP_FIELD_TYPE_MAP` constants
  (1..5) published alongside the `DATA_TYPE_*` set.

## Design choices

### ORSWOT-style map (state-based, not delta-state)

The map is **state-based**, not delta-state. State-based is
strictly easier to reason about (the merge function is the
join in the lattice and that's the entire contract), and the
brief's "associative, commutative, idempotent" target is
exactly what a state-based join gives. Delta-state would only
matter if we cared about gossip-message size; we don't, at
this slice.

Field presence follows the OR-Set "add wins on tie" rule.
Each field carries an add-tag set and a remove-tag set; a
field is exposed iff at least one add tag is not shadowed by
a remove tag. Updates mint a fresh tag per `(actor, counter)`
pair. Removals tombstone every currently-observed add tag; a
concurrent update through a different actor produces a tag
the tombstone has not seen and survives the merge.

The CRDT value carried by a field is merged independently of
the tag bookkeeping. Two replicas concurrently removing and
updating a counter field merge to a present field whose value
is the per-actor max of the contributions, exactly as the
counter's own merge dictates.

Nested maps recurse: a `FieldType::NestedMap` value holds a
`Box<Map>`, and the merge calls `Map::merge` on the inner map.

### Field key shape

A field key is `(name: Vec<u8>, field_type: FieldType)`. Two
fields with the same `name` but different `field_type` are
distinct, matching Riak's `riak_dt_map` namespacing rule. The
wire form is `MapField { name: bytes, field_type: int32 }`.
The `FieldType` discriminants match the wire constants, so a
client decoded `MapField.field_type` round-trips through
`FieldType::from_wire`/`to_wire`.

### Removal divergence from Riak's exact semantics

When a field is removed, the underlying CRDT value is left
intact; only the field's add-tag set is tombstoned. A
subsequent re-add through the same actor mints a fresh tag
and exposes the field again with whatever lattice state it
had before the remove.

Riak's reference behaves the same way at the CRDT layer; the
"reset on remove" semantics most clients expect emerge from
the client-side convention of always assigning before reading
after a remove. The brief specifically asked for any
divergence to be documented; this is the only one. The map's
test
`concurrent_remove_loses_to_concurrent_update` pins the
add-wins behaviour; the alternative (drop the value on
remove) breaks commutativity (`merge(a, b)` would zero out
contributions in `b` that arrived after `a`'s remove).

### HyperLogLog precision = 14 bits

`PRECISION = 14`, `REGISTER_COUNT = 2^14 = 16384`. Riak's
default. Memory cost: 16 KiB per HLL (one `u8` register per
slot, held as a `Box<[u8]>` so the type stays `Sized` while
the array lives on the heap).

The standard error envelope is `1.04 / sqrt(m) ~= 0.81%`. The
brief asks for tests at +/-5%, which leaves about 6 sigma of
headroom; statistical flake is not a concern at 10000 items.

The dense-only register layout is chosen over Riak's
sparse/dense hybrid. Sparse encoding saves memory at low
cardinalities at the cost of a mode-transition state machine
inside `add` and `merge`; for a Rust port whose first
consumer is the in-process datastore (where 16 KiB per HLL
is rounding error), the simpler dense form wins.

### Hash function

Murmur3 (the project's `HashType::Murmur3`) is the only
128-bit-output hash exposed by `dynomite::hashkit`. We splice
the low two `u32` words of the resulting `DynToken` into a
`u64` and use that as the HLL hash. The two-word slice gives
14 bits of register index and 50 bits of leading-zero
position, which sets the upper bound on representable
cardinality at `2^50` -- well above any plausible Riak
workload.

The brief allows "Murmur is fine" but plain Murmur is 32-bit;
with 14 bits of index, only 18 bits remain for `rho`, capping
the estimator at `2^18 ~= 262 K`. Murmur3 lifts that to
`2^50 ~= 10^15` for free, which is the right call.

### Cardinality formula

The original Flajolet et al. harmonic-mean estimator with the
small-range linear-counting correction:

* `Z = sum(2^(-M[j]) for j in 0..m)`
* `E_raw = alpha_m * m^2 / Z` where
  `alpha_m = 0.7213 / (1 + 1.079 / m)`.
* If `V` (number of zero registers) is non-zero and
  `E_raw <= 5/2 * m`, return `m * ln(m / V)` (linear
  counting).
* Otherwise return `E_raw`.

The 2007 large-range correction is dropped: with 64-bit
hashes the breakdown range (`2^32 / 30`) is unreachable.

The `f64 -> u64` projection through `Crdt::value` saturates
NaN, negatives, and overflow; the saturating helper carries a
function-scoped `#[allow(clippy::cast_*)]` documented in
`docs/journal/allowances.md` (one new row).

## Testing summary

Counts (relative to `main` at `9811ffb`):

| Suite | Before | After |
|---|---:|---:|
| `cargo nextest run --workspace` | 1006 | 1050 |
| ... thereof unit tests in `dyniak` | 212 | 240 |
| ... thereof integration tests | 28 | 39 |
| ... thereof property tests | 12 | 18 |

Total new tests: **44** = 28 unit (10 map + 7 hll + 11 proto)
+ 11 integration + 6 property.

Brief's mandatory scenarios each have a dedicated test:

* Three-typed-field map round trip:
  `map_three_typed_fields_add_remove_merge_round_trip` and
  the `three_typed_fields_round_trip_through_value` unit test.
* HLL distinct-items merge within +/-5%:
  `hll_two_replicas_distinct_items_merged_estimate_within_5pct`.
* HLL idempotency under set-equality:
  `hll_idempotent_under_set_equality`.
* Map merge laws (associative / commutative / idempotent) via
  `hegeltest`: `map_merge_is_*` x3 at 256 cases each.
* HLL merge laws via `hegeltest`: `hll_merge_is_*` x3 at 256
  cases each (asserting both register-array equality and
  cardinality equality).
* PBC `DtOp` / `DtValue` round trips with map and HLL
  payloads: `dt_op_with_map_payload_round_trips_via_prost`,
  `dt_value_with_map_payload_round_trips_via_prost`,
  `dt_op_with_hll_payload_round_trips_via_prost`,
  `dt_value_with_hll_payload_round_trips_via_prost`,
  `hll_value_message_round_trips`.

## Files touched

```
crates/dyniak/src/datatypes/hll.rs                    [new]
crates/dyniak/src/datatypes/map.rs                    [new]
crates/dyniak/src/datatypes/mod.rs                    [+10 lines, append-only]
crates/dyniak/src/proto/pb/datatypes.rs               [filled in reserved tags + tests]
crates/dyniak/src/proto/pb/mod.rs                     [+6 lines, append-only]
crates/dyniak/tests/datatypes_round_trip.rs           [+11 tests, append-only]
crates/dyniak/tests/datatypes_properties.rs           [+6 tests, append-only]
docs/journal/allowances.md                              [+1 row]
docs/journal/2026-05-24-dyniak-crdts-map-hll.md       [this file]
```

`crates/dyniak/src/lib.rs` did not need a new re-export:
`pub mod datatypes` already exposes the module tree, and
`HyperLogLog` / `Map` / friends are reachable through
`dyniak::datatypes::*`.

`crates/dynomite/`, `crates/dynomited/`, `crates/dyn-encoding/`,
`crates/dyn-hash-tool/`, `crates/dyn-admin/`,
`crates/dyniak/src/proto/http/`, `crates/dyniak/src/aae/`,
`crates/dyniak/src/server.rs`, `crates/dyniak/src/mapreduce/`,
and `scripts/` are untouched.

## Verification

```
cargo build --workspace --all-targets --locked      -- clean
cargo fmt -p dyniak -- --check                    -- clean
cargo clippy --workspace --all-targets \
         --all-features -- -D warnings              -- clean
cargo nextest run --workspace                       -- 1050 passed, 4 skipped
cargo test --doc --workspace                        -- 15 passed (incl. HLL doctest)
```

## Open questions

None at this slice. The dispatcher integration in
`server::handle_conn` for the new `DT_FETCH_*` / `DT_UPDATE_*`
codes (currently routed to the existing `Datastore` hook for
KV ops) is the natural next step but explicitly out of scope:
the wire types and the in-memory CRDTs are now both ready for
wiring into a `Datastore` extension in a follow-up slice.

## Deferred

Nothing on the CRDT lineup. Map and HLL complete the
five-type set called out in the v0.0.3 plan. Map's wire
schema, however, only covers the state-based representation;
the operation-based "deferred remove" extension Riak ships
for cross-cluster replication is unmodelled and left for a
later slice.
