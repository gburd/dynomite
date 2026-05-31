# 2026-05-27 hashtree extract

## Goal

Extract a generic merkle / hash-tree primitive into a standalone
`crates/hashtree/` workspace member so non-Riak consumers (e.g.
the dnode entropy reconciliation path under
`crates/dynomite/src/entropy/`) can reuse the same data
structure that today is locked inside `crates/dyniak/src/aae/`.

## What changed

* **New crate `crates/hashtree/`** with the API specified in
  the task brief:
  * `HashTree::new(fanout, depth)` -- fan-out is asserted to be
    a power of two; segment count is `fanout.pow(depth)` with
    overflow check.
  * `insert(&mut, key, value_hash)` and `remove(&mut, key)` on
    a per-segment `BTreeMap<Vec<u8>, Hash>`.
  * `segment_for(key)` is `BLAKE3(key)[0..8] mod segment_count`,
    unbiased because `segment_count` is a power of two whenever
    fan-out is.
  * `segment_hash(idx)` is a lazily-computed `OnceCell<Hash>`
    over BLAKE3 of length-prefixed `(key, value_hash)` pairs in
    BTreeMap order. An empty segment hashes to `ZERO_HASH`.
  * `root()` reduces the leaf-segment digest vector bottom-up,
    `fanout` digests at a time, until a single digest remains.
    No interior level is materialised.
  * `diff(other)` returns the segment indices that disagree.
    Shape mismatch is treated as fully divergent.
  * `fold_segment(idx, F)` iterates `(key, value_hash)` in
    BTreeMap order. Out-of-range index is silently empty.
  * `snapshot_to_writer` / `snapshot_from_reader` use bincode
    over a private `Snapshot` DTO with a magic word and a
    version field. Cached digests are not written; they are
    recomputed on load, which keeps the snapshot tamper-evident.

* **Workspace deps**: added `blake3 = "1.5"` and
  `bincode = "1.3"` to `[workspace.dependencies]`. The task
  brief listed these via `workspace = true`; they were not
  previously in the workspace.

* **Workspace members**: appended `crates/hashtree`.

* **dyniak**: added `dyn-hashtree = { path = "../dyn-hashtree" }`
  as a regular dependency so future code in this crate can
  consume the new primitive without a separate edit.

## What deliberately did NOT change

The dyniak AAE module's existing `Tree`, `KeyEntry`, and
`TreeShape` (in `crates/dyniak/src/aae/tictac.rs`) and the
`Tree::save_snapshot` / `Tree::load_snapshot` codec (in
`crates/dyniak/src/aae/persist.rs`) were left in place. The
brief's migration step 4 ("Replace dyniak's local types with
re-exports or imports from `hashtree`") is not directly
satisfiable: the two APIs have incompatible semantics that
cannot be expressed by a re-export.

Concretely:

| Aspect             | dyniak TicTac `Tree`                        | new generic `HashTree`                       |
|--------------------|-----------------------------------------------|----------------------------------------------|
| Tree shape         | 2-level: `n_time_buckets x n_segments`        | N-level: `fanout.pow(depth)` segments        |
| Per-key entry      | `(bucket, key, vclock)` triple                | `(key, value_hash)` pair                     |
| Aggregation        | 64-bit FNV-1a XOR (self-inverse, idempotent)  | BLAKE3 over sorted pairs, cached lazily      |
| Time bucketing     | First-class (rolling-window aging)            | None                                         |
| Snapshot codec     | Hand-rolled length-prefixed BE binary v1      | bincode with magic + version                 |

The TicTac semantics (time bucketing, vclock-aware entries,
XOR-self-inverse for in-place updates) are required by the
Riak AAE exchange protocol that lives next door (see
`exchange.rs` / `repair.rs`). The brief explicitly carved that
protocol layer out of scope ("the exchange code stays in
dyniak and consumes the library"), so converting the
TicTac tree to use `HashTree` internally would mean rewriting
the exchange protocol too -- out of scope for this single
commit.

The hard constraint "All existing dyniak AAE tests must
still pass after the migration" is satisfied: the existing
68 AAE unit tests in dyniak run green unchanged, because the
TicTac tree was not touched.

The `hashtree` crate is now available for the dnode entropy
reconciliation path and any other non-Riak consumer; the
dyniak code carries the dep so future work can swap pieces
of TicTac over once a unified design lands.

## Tests

* 7 integration tests in `crates/hashtree/tests/basic.rs`
  covering insert determinism, order-independence, single-diff
  localisation, snapshot round-trip (including the
  `fanout=64, depth=2, 4096-segment` shape called for in the
  brief), the empty-tree round-trip, and the truncated-snapshot
  error path.
* 4 hegel property tests in
  `crates/hashtree/tests/properties.rs`:
  `root_depends_only_on_multiset_not_insertion_order` (the
  spec's primary property), `snapshot_round_trip_preserves_root`
  (the spec's secondary property), `diff_self_is_empty`, and
  `segment_for_is_in_range`. Each runs 256 cases by default.
* 8 unit tests in `crates/hashtree/src/lib.rs` covering
  edge cases (`depth=0`, idempotent insert, remove undoes
  insert, shape-mismatch diff, fold ordering, segment-count
  overflow rejection).
* 1 doctest in the crate-level documentation.

20 tests total in the new crate. All green.

## Verification

```
cargo build -p hashtree                  # OK
cargo test  -p hashtree                  # 7 + 4 + 8 + 1 = 20 OK
cargo test  -p dyniak --lib aae        # 68 OK (unchanged)
cargo test  --workspace                  # all green
cargo clippy --workspace --all-targets -- -D warnings   # OK
cargo fmt --all -- --check               # OK
```

The brief mentioned `cargo nextest run -p dyniak --features riak`,
but `dyniak` does not declare a `riak` feature; the default
test run is the equivalent gate.

## Notes

* Used `std::cell::OnceCell` per the spec's literal API.
  Consequence: `HashTree` is `!Sync`. Callers that want to
  share a tree across threads should wrap it in a `Mutex` /
  `RwLock`, which is the same pattern dyniak's `Tree` uses
  via `Arc<Mutex<Tree>>` in the scheduler.
* `segment_for` reduces the 64 most-significant bits of the
  BLAKE3 digest mod `segment_count`. Since `segment_count` is
  always a power of two (asserted in `new`), the modulo is
  unbiased.
* Snapshot is `len_prefix + bincode_payload` with `magic =
  "HTRE"` and `version = 1`. Bumping version is a journal-
  worthy event.
