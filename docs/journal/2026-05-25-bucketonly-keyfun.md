# 2026-05-25 -- bucket-only chash_keyfun

## Context

Operator follow-up on Riak-compat: surface the
`chash_bucketonly_keyfun` hashing variant per bucket. The
default Dynomite path hashes `<bucket>/<key>`; some Riak
deployments want every key in a bucket to land on the same
partition (effectively bucket-as-shard). Adding the toggle is
strictly non-breaking: existing bucket types without the field
default to `STD` and the existing distribution machinery is
untouched.

## Files touched

* `crates/dyn-riak/src/proto/pb/messages.rs` -- added
  `chash_keyfun: Option<u32>` at tag 30 with the canonical
  `STD/BUCKETONLY/CUSTOM` constants. Documented the schema
  extension; tag 9 stays reserved for the upstream Riak
  `RpbModFun` shape we do not model.
* `crates/dyn-riak/src/proto/pb/mod.rs` -- re-exported
  `CHASH_KEYFUN_*` constants alongside the existing `Rpb*`
  types.
* `crates/dyn-riak/src/datatypes/keyfun.rs` -- new module with
  `KeyFun::{Std, BucketOnly}` plus `route_bytes` (allocating)
  and `route_bytes_into` (buffer-reuse) helpers. Wire decode
  rejects `CUSTOM = 99` with a typed `KeyFunError::Custom` so
  a future user-defined slice has a non-breaking enum slot.
* `crates/dyn-riak/src/datatypes/mod.rs` -- registered the new
  module.

## Test deltas

* Unit (`crates/dyn-riak/src/datatypes/keyfun.rs::tests`):
  shape, round-trip, defaulting, empty-input totality.
* Unit (`crates/dyn-riak/src/proto/pb/messages.rs::tests`):
  `bucket_props_chash_keyfun_round_trips`,
  `bucket_props_default_omits_new_selectors`.
* Unit (`crates/dyn-riak/src/router.rs::tests`):
  `bucketonly_keyfun_collapses_keys_to_one_partition` (100
  keys land on one peer) and
  `std_keyfun_distributes_within_5_percent_of_uniform` (10k
  keys distributed to within 5 percent of uniform across 5
  peers).
* Integration (`crates/dyn-riak/tests/bucket_props_routing.rs`):
  `bucketonly_keyfun_routes_two_keys_to_same_primary` --
  drives two PUTs through `serve_pbc_with_routing` and asserts
  both land on the same per-peer outbound channel.
* Integration: `pbc_set_bucket_persists_chash_keyfun` --
  round-trips the wire-level `RpbBucketProps.chash_keyfun`
  through the registry-backed get-bucket / set-bucket pair.

## Parity delta

`docs/parity.md`: new Deviation entry covering the
`chash_keyfun: CUSTOM` slot. We reserve the value but do not
ship a user-defined keyfun.

## Open questions

None. The path is read-only against the distribution layer and
the proto extension is at a fresh tag (30) with no overlap
against the upstream Riak schema.
