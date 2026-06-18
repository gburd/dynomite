# 2026-06-18 - FT.* search-index persistence (stage/ft-persist)

## Summary

Made the RediSearch FT.* index registry survive a `dynomited`
process restart (including a chaos process-kill). The
`VectorRegistry` was purely in-memory: a restart dropped every
index until the client re-issued `FT.CREATE` and re-fed data.
Added an optional snapshot-to-disk persistence backend.

## What landed

- `crates/dynomite-search/src/registry.rs`
  - New `VectorRegistry::open(dir, &SuggestionRegistry)`
    constructor (durable). `::new()` stays in-memory and
    unchanged.
  - `save(&SuggestionRegistry)` writes a full CBOR snapshot
    atomically (write `*.tmp`, flush + `sync_all`, rename).
    No-op when no persistence dir is set.
  - `is_persistent()` accessor.
  - On `open`, an existing snapshot is replayed: `create`
    each index from its stored schema, re-`upsert` every
    indexed document (vector read back out of the engine and
    decoded to `f32`, metadata preserved), re-provision
    FT.ALTER-added TEXT fields, re-`upsert_text_field` every
    stored text byte, re-`add` every suggestion.
  - New `SnapshotError` type (io / encode / decode / replay /
    vector-decode).
- `crates/dynomite-search/src/sugest_registry.rs` /
  `sugest.rs`: `to_snapshot` / `load_snapshot` on the
  suggestion registry plus a `snapshot_entries` helper on
  `SuggestionDict` (both `pub(crate)`).
- `crates/dynomite/src/conf/pool.rs`: new `search_index_dir:
  Option<PathBuf>` knob on `ConfPool` (mirrors `noxu_path`).
- `crates/dynomited/src/reload.rs`: `search_index_dir` flagged
  non-reloadable (registry is built at startup).
- `crates/dynomited/src/server.rs`: when `search_index_dir` is
  set, build the registry via `::open` and reload the prior
  snapshot; share the suggestion registry through
  `SearchExtension::with_suggestions`. A periodic snapshot task
  (5 s) plus a final save on clean shutdown is spawned only in
  durable mode. In-memory mode is byte-for-byte the old path.
- `docs/book/src/configuration.md`: new "Search index
  persistence" section.

## Snapshot format + strategy

- Format: CBOR (`ciborium`, already a workspace dep) of a
  `RegistrySnapshot { indexes, suggestions }`. File:
  `<search_index_dir>/search-snapshot.cbor`.
- Strategy: periodic full snapshot (5 s) + snapshot on clean
  shutdown, written atomically. A crash between snapshots
  loses at most the un-snapshotted delta - acceptable, since
  the FT.* workload already tolerates re-creation. The prior
  good snapshot always survives a mid-write crash (temp +
  rename). A stray `*.tmp` is ignored on load.

## Config knob

`search_index_dir:` on the pool body (`ConfPool`). Unset =>
in-memory (default, no regression). Set => durable.

## Constructors

- `VectorRegistry::new()` - in-memory (unchanged).
- `VectorRegistry::open(dir, &SuggestionRegistry)` - durable;
  loads any prior snapshot, else starts empty.

## Tests

`crates/dynomite-search/tests/persistence.rs` - 5 tests:
1. `round_trip_recovers_index_schema_docs_text_and_suggestions`
   - create + feed docs/text/alter-field/suggestions, save,
   drop, reopen, assert every index/schema/key/vector/text-hit/
   suggestion (with scores + payloads) recovered.
2. `stray_tmp_file_is_ignored_and_valid_snapshot_loads`.
3. `open_on_empty_dir_starts_empty` (no file written until
   save).
4. `new_is_in_memory_and_never_touches_disk`.
5. `save_is_atomic_and_reopen_sees_latest` (no stray tmp after
   clean save).

All 115 `dynomite-search` tests pass (110 prior + 5 new), 8
doctests pass, clippy `-D warnings` clean on `dynomite-search`
and `dynomited --features riak,search`, fmt clean. ConfPool /
reload tests still green.

## Dependency

`ciborium` added as a direct dep of `dynomite-search`. Already
in `Cargo.lock` (transitive via opentelemetry et al.) and a
declared `[workspace.dependencies]` entry; `cargo update -p
dynomite-search --offline` added a single graph edge, zero new
packages, zero version changes.

## Chaos relevance

This closes the gap behind `IndexResetByChaos` / `ft/Unknown`
in the combined chaos run. With persistence on and the
dynomited restart reload path, a SIGKILL no longer drops
indexes: the periodic snapshot on disk survives and the
restart reloads it. The chaos workload's recreate-on-miss
becomes a fallback rather than the primary recovery once this
lands. (SIGKILL gives no clean-shutdown hook, which is exactly
why the snapshot is periodic, not shutdown-only.)

## Open questions

- Snapshot cadence (5 s) is a fixed constant, not a config
  knob. If chaos kill cadence is tighter than the worst-case
  delta tolerance, expose it; YAGNI until measured.
- Large indexes snapshot the whole registry each tick (full
  rewrite). Fine for the current FT.* scale; an append log or
  per-index dirty-tracking is the upgrade path if snapshot
  cost shows up in profiles.
