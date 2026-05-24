# 2026-05-23 - Bucket types (Riak D3 / M3)

Branch: `stage/m3-bucket-types` (off `main` at `48cecc3`).

Lands the third post-chaos queue item: per-bucket routing
overrides modeled on Riak's bucket types but adapted to the
proxy's existing pool / consistency machinery. Closes M3 in
`docs/riak-compat-plan.md` and item 4 in
`docs/post-chaos-queue.md`.

## Scope

* New `ConfBucketType` schema in `crates/dynomite/src/conf/pool.rs`
  carrying `name`, `read_consistency`, `write_consistency`, and
  `n_val`. Plumbed through serde, validation, and YAML
  round-trip tests.
* New `ConfPool` fields:
  * `bucket_types: Vec<ConfBucketType>` (default empty).
  * `default_bucket_type: Option<String>` (default unset).
* Validator rejects:
  * Empty bucket-type names.
  * Duplicate bucket-type names within a pool.
  * Bucket-type stanzas with consistency strings outside the
    canonical four (`DC_ONE`, `DC_QUORUM`, `DC_SAFE_QUORUM`,
    `DC_EACH_SAFE_QUORUM`; case-insensitive).
  * `default_bucket_type` referencing an undefined entry.
* New `bucket_name(&[u8]) -> Option<&[u8]>` helper at
  `crates/dynomite/src/proto/redis/bucket.rs`. The convention is
  the simpler `<bucket>/<key>` form rather than Riak's three-level
  `bucket-type/bucket/key` tuple; the part before the first `/`
  is the bucket name, the rest (including the slash) is the user
  key. Empty prefix (`/leading`, `//double`) returns `None`.
  Five unit tests cover the slash variants plus a binary-safe
  smoke test.
* `cluster::pool::PoolConfig` carries a resolved `BucketType`
  list (`name` plus the typed `ConsistencyLevel` enums plus the
  `n_val` cap) and a `default_bucket_type` name. `PoolConfig::from_conf`
  populates the list; new helpers `lookup_bucket_type` and
  `resolve_bucket_type` give the dispatcher one call to find the
  applicable bundle. `PoolConfig` now also implements `Default`
  to keep test sites concise as the field count grows.
* `cluster::dispatch::ClusterDispatcher::plan` extracts the
  bucket name with `bucket_name`, calls `resolve_bucket_type`,
  and applies the bundle's consistency for the lifetime of one
  plan. After the plan is built the new `cap_replicas` helper
  truncates `DispatchPlan::Replicas` to the bundle's `n_val`
  (skipping the cap when `n_val == 0`).

## Tests

* `proto::redis::bucket::tests::*` (5 tests): no slash, single
  slash, multi-slash, empty prefix, binary-safe keys.
* `conf::pool::tests::*` (4 new tests): YAML round-trip,
  empty-by-default behaviour, duplicate-name rejection, unknown
  consistency rejection, unknown default rejection.
* `cluster::dispatch::tests::*` (7 new tests):
  * Bucket-type override beats pool default.
  * Slash-less key falls back to pool default.
  * Unknown bucket prefix uses `default_bucket_type` when set.
  * Unknown bucket prefix without a default uses pool default.
  * `n_val == 1` truncates to one target.
  * `n_val == 2` truncates to two targets.
  * `n_val == 0` does not cap.
  * `n_val` larger than the candidate set is a no-op.
* Documentation: new operator-facing prose and sample YAML in
  `docs/book/src/configuration.md`. Doctests on
  `ConfBucketType::read_level` / `::write_level` and the
  struct-level example exercise the parsing path.

## Test-count delta

| Suite       | Before | After |
|-------------|-------:|------:|
| `nextest`   |   658  |  676  |
| Doctests    |   591  |  595  |
| Conformance |   34   |   34  |

All workspace clippy and fmt gates remain clean.

## Notes / non-goals

* The protocol-buffers M2 RpbGetBucketTypeReq /
  RpbSetBucketTypeReq surfaces are deliberately out of scope
  here; the runtime reads the YAML stanza but does not yet
  expose mutation. M3 in `docs/riak-compat-plan.md` builds on
  this commit.
* Bucket types do **not** override the hash function (the third
  knob mentioned in the comparison doc). A per-bucket hash
  selector would shard the same key onto different rings under
  different prefixes; that breaks single-pool ring rebuilds and
  is not implemented. If we ever need it, the hook is to add a
  `hash: Option<HashType>` field on `ConfBucketType` and select
  it inside `plan` before the `hashkit::hash` call.
* `n_val` only caps the size of `DispatchPlan::Replicas`; it is
  not a quorum target by itself. The consistency level still
  decides whether reads/writes need majority acks; this matches
  the comparison doc's wording where `n_val` is the
  replication factor and `r`/`w` are the quorum sizes.
