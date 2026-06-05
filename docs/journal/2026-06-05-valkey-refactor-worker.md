# Valkey rename + data_store refactor

Stage: valkey-refactor (off `main`)
Branch: stage/valkey-refactor
Worktree: /home/gburd/ws/wt-valkey-refactor

## Summary

Two coupled changes:

1. Renamed the Redis *backend / tooling* identity to Valkey (the
   Linux Foundation open-source fork) while leaving the RESP wire
   protocol, command names, and RESP-named Rust API untouched.
2. Refactored `DataStore` from `{Redis, Memcache, Noxu}` to
   `{Valkey, Memcache, Dyniak}`, removed the noxu RESP veneer, and
   made `dyniak` a first-class `data_store` mode that serves the
   Riak PBC / HTTP surface against a transactional in-process Noxu
   environment with no RESP proxy.

This is a deliberate breaking change to the published
`dynomite-engine` public API. Versions were NOT bumped; the lead
cuts 0.2.0 after this lands.

## RENAMED (backend / tooling / data_store identity)

* `DataStore::Redis` -> `DataStore::Valkey`; `DataStore::Noxu` ->
  `DataStore::Dyniak` (all call sites across crates/).
* `conf::enums::DataStore`:
  * `from_name`: `valkey` is canonical; `redis` accepted as a
    back-compat alias mapping to `Valkey`; `memcache` /
    `memcached` -> `Memcache`; `dyniak` -> `Dyniak`; `noxu` arm
    removed.
  * `as_name` returns `valkey` / `memcache` / `dyniak`.
  * `from_int` / `as_int`: Valkey `0` (wire-compatible with
    existing `data_store: 0`), Memcache `1`, Dyniak `2`.
* `conf::ConfError::BadNoxuConfig` -> `BadDyniakConfig`; messages
  reworded to dyniak. `BadDataStore` message lists valkey /
  memcache / dyniak.
* `conf::set_noxu_supported` / `is_noxu_supported` ->
  `set_dyniak_supported` / `is_dyniak_supported`.
* `conf::pool`: `validate_noxu` -> `validate_dyniak`;
  `noxu_path` required iff `data_store: dyniak`; `deserialize_data_store`
  text reworded. `noxu_path` knob KEPT (it points at the Noxu
  environment dir; Noxu is the storage engine, dyniak is the mode).
* Backend binary discovery / docs / tests: `redis-server` ->
  `valkey-server`, `redis-cli` -> `valkey-cli`, `redis-benchmark`
  -> `valkey-benchmark`.
* `flake.nix`: `redis` package -> `valkey`.
* Chaos harness: combined-mode instances are now
  `valkey` / `memcache` / `dyniak`; start-host.sh INSTANCE
  handling, coordinator.sh fan-out + teardown, chaos-injector.sh
  per-instance fault loops, driver-spec.sh (`-valkey` suffix +
  `--mode valkey`; riak driver stays `--mode riak` against the
  dyniak instance's PBC port), generate-report.py column labels
  (valkey / memcache / riak), and workload-driver.py
  (`--mode valkey` RESP driver; `redis` kept as an alias).

## KEPT UNCHANGED (RESP wire protocol)

* `proto::redis` module and internals.
* `MsgType::ReqRedis*` and every RESP message-type variant.
* The RediSearch `FT.*` surface (`dynomite-search`).
* The word "RESP" everywhere.
* Redis-command names in code/tests (GET/SET/HSET/...).
* `redis_requirepass` config knob (mirrors the Valkey/Redis
  server `--requirepass` option; renaming would break more
  configs than the brief asked for).

## data_store refactor

* Deleted `crates/dynomited/src/noxu_backend.rs` (the GET/SET/DEL/
  PING RESP veneer over Noxu) plus its module declaration. There
  is no longer a "noxu as a RESP backend" path.
* `server.rs`: the old `data_store == Noxu` branch became the
  `data_store == Dyniak` branch. A dyniak pool now:
  * opens the Noxu environment via
    `NoxuDatastore::open_transactional` (`with_transactional(true)`),
  * hands it to the dyniak (Riak PBC / HTTP) surface,
  * spawns NO RESP backend supervisor and binds NO RESP client
    proxy (`proxy: Option<ProxyKind>`, `backend_handle: None`).
  `valkey` / `memcache` keep proxying to external backends
  exactly as before.
* `dyniak::datastore::noxu`: added `open_transactional` /
  `open_with_options(path, db_name, transactional)`; the existing
  non-transactional `open` / `open_in` / `open_with_db_name` are
  preserved for the tests/examples.
* The `riak:` block still carries the PBC / HTTP listener (and
  AAE / wasm) configuration; it is now the self-describing
  surface config for a `data_store: dyniak` pool. PBC + HTTP
  listener wiring preserved; on non-dyniak pools a present `riak:`
  block still falls back to the in-process `MemoryDatastore`.

## Tests touched

* `conf::enums` / `conf::pool` unit tests updated for the new
  variant names, the `redis` back-compat alias, and the dyniak
  validation messages; added `data_store_redis_alias_maps_to_valkey`.
* `dyniak::datastore::noxu`: added `open_transactional_round_trips`.
* `dynomited/tests/riak.rs`: `riak_pbc_2i_against_noxu_round_trip`
  now uses `data_store: dyniak` + transactional Noxu, no RESP
  proxy.
* Chaos tests: `test_driver_spec.sh`, `test_generate_report.py`
  updated for the valkey/dyniak rename.

## Verification (all green)

* `cargo build --workspace --all-targets --features riak --locked`
* `cargo build -p dynomited --all-targets` (no riak)
* `cargo nextest run --workspace --features riak` -> 2015 passed
* `cargo clippy --workspace --all-targets --features riak -- -D warnings`
* `cargo clippy -p dynomited --all-targets -- -D warnings` (no riak)
* `cargo fmt --all -- --check`
* `cargo test --doc -p dynomite-engine` -> 672 passed
* `cargo test --doc -p dynomite-search -p dyniak --features dyniak/noxu`
* `mdbook build docs/book`
* `bash scripts/chaos-multi-host/test_driver_spec.sh` -> PASS=23 FAIL=0
* `python3 scripts/chaos-multi-host/test_generate_report.py` -> OK
* `python3 scripts/chaos-multi-host/workload-driver.py --self-test` -> OK
* `rg 'DataStore::Noxu|noxu_backend' crates/` -> empty
* `rg 'redis-server|redis-cli|redis-benchmark' crates/ scripts/ flake.nix`
  -> empty

## Open questions / notes for the lead (0.2.0 API surface)

* Public API breaks: `DataStore::{Redis->Valkey, Noxu->Dyniak}`;
  `ConfError::BadNoxuConfig -> BadDyniakConfig`;
  `conf::{set,is}_noxu_supported -> {set,is}_dyniak_supported`.
* `noxu_path` knob name retained (Noxu is the storage engine);
  `data_store: redis` and integer `0` still load (alias to Valkey).
* The `riak:` block was kept as the dyniak surface's listener
  config rather than inlined as top-level pool fields, to keep
  the AAE / TLS / wasm sub-config and the existing wiring intact;
  a dyniak pool is now self-describing via that block.
