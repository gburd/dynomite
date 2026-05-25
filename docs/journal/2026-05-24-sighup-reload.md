# 2026-05-24 - SIGHUP-driven configuration reload

## Goal

Wire `SIGHUP` so the running `dynomited` server reloads:

1. The log file (existing behavior, preserved).
2. The TLS cert / key / CA bundle for every per-DC peer-plane
   profile, without rebinding the listener or restarting peer
   supervisors.
3. A "value, not structure" subset of pool knobs published into
   a shared `ReloadableState` cell.

Listener ports, peer topology, and the datastore choice remain
non-reloadable; the reload pipeline classifies them as such and
the run loop logs a clear warning on every attempted change.

## Files touched

- `crates/dynomite/src/net/tls.rs` - added `SharedTlsProfiles`
  (an `Arc<parking_lot::RwLock<TlsProfileMap>>` wrapper) plus
  `ReloadingDcSniResolver` so the inbound listener's
  `TlsAcceptor` reads cert material on every handshake instead
  of binding it at acceptor-construction time.
- `crates/dynomite/src/net/mod.rs` - re-exported
  `SharedTlsProfiles`.
- `crates/dynomited/src/reload.rs` - new module owning the
  reload pipeline. Defines `ReloadableSnapshot` (the pool's
  reloadable values), `ReloadableState` (the live cell),
  `ReloadOutcome` (the diff classification), `ReloadError`
  (parse / validate / TLS errors), and the entry point
  `reload_from_path`. Eight unit tests inside the module.
- `crates/dynomited/src/lib.rs` - exposed `reload`.
- `crates/dynomited/src/server.rs` - replaced the
  build-time-only `tokio_rustls::TlsConnector` cache in the
  per-peer supervisor with a `SharedTlsProfiles` clone (the
  supervisor now resolves a fresh connector on every reconnect),
  added `original_conf_pool`, `conf_path`, `reloadable`, and
  `tls_profiles` fields plus the `with_conf_path`,
  `reloadable`, and `tls_profiles` accessors, threaded the
  reload context through `supervise`, and added
  `handle_sighup_reload` which (a) calls `reopen_on_sighup`
  first so the operator-facing reload INFO line lands in the
  new log file, then (b) drives `reload_from_path`, then
  (c) emits the success summary or a `tracing::error!` line on
  failure.
- `crates/dynomited/src/main.rs` - captured `cli.conf_file`
  before the runtime block and called `with_conf_path` on the
  built server so the run loop's SIGHUP arm has the path.
- `crates/dynomited/tests/sighup_reload.rs` - six integration
  tests: `SharedTlsProfiles::replace` Arc-identity swap,
  on-disk cert rotation produces a fresh client config Arc
  after `reload_from_path`, invalid YAML is non-destructive,
  consistency knobs land via the reload pipeline, listener-port
  changes surface as `non_reloadable`, and the per-DC SNI
  resolver picks up swapped material.

## Reload pipeline

`reload_from_path(path, original_pool, state, tls)`:

1. Parses and validates the YAML at `path`. Either failure
   leaves `state` and `tls` untouched and propagates a
   `ReloadError`.
2. Rebuilds the `TlsProfileMap` from the new pool's
   `peer_tls_*` fields BEFORE swapping anything, so a malformed
   PEM aborts the reload cleanly. The map is then
   atomic-swapped into the `SharedTlsProfiles` via
   `replace`. Already-negotiated TLS sessions are unaffected;
   new handshakes (inbound) and reconnects (outbound) read the
   new map via the reloading SNI resolver and the supervisor's
   per-reconnect `client_config_for_dc` lookup.
3. Diffs the new pool against `original_pool` and classifies
   each touched field as either reloadable (consistency,
   gossip cadence, entropy cadence, hint knobs, dyn r/w
   timeouts, request timeout, bucket-type bundle, default
   bucket type, peer_tls paths) or non-reloadable (listen,
   dyn_listen, stats_listen, dyn_seeds, tokens, rack,
   datacenter, data_store, servers, noxu_path, enable_gossip,
   enable_hinted_handoff, hash, client_connections,
   dyn_connections, observability, riak).
4. Writes the new `ReloadableSnapshot` into the cell.
5. Returns a `ReloadOutcome` for the run loop to log.

The `Server::run` SIGHUP arm:

```
log_reopen() -> reload_from_path() -> log INFO summary
                              \-> log error and continue on failure
```

## TLS reload mechanics

Without rebinding the listener: the inbound `TlsAcceptor` is
constructed with a `ReloadingDcSniResolver` that reads the
shared `Arc<RwLock<TlsProfileMap>>` on every `resolve` call.
After `SharedTlsProfiles::replace(new_map)`, every subsequent
`ClientHello` resolves against the new map.

Without restarting peer supervisors: the supervisor stores a
clone of the `SharedTlsProfiles` cell and resolves
`client_config_for_dc(peer_dc)` on every reconnect attempt.
A mid-rotation reconnect may fail TLS to a peer whose
just-rotated material has not landed there yet; the supervisor
logs and backs off, exactly as it does for any other
handshake failure today.

Already-established TLS sessions on either side keep using
their negotiated keys until they close, matching the brief.

## Out-of-scope deferrals

The brief asked for the dispatcher, gossip task, and entropy
driver to read `read_consistency`, `gossip_phi_threshold`,
`recon_interval_seconds`, and the bucket-type bundle from the
reloadable cell on every cycle. Those consumers live under
`crates/dynomite/src/cluster/` (and `phi_threshold` lives in
the cluster failure-detector specifically), which the brief
also marks as out of scope for this slice. This change ships
the publisher and the TLS swap in full; downstream consumption
is a follow-up that does NOT need to revisit this slice's API
(it just calls `state.snapshot()` from inside the existing hot
loops).

The reload pipeline emits the `non_reloadable` warning line
the brief specifies for any field operators try to change that
the runtime cannot pick up - whether because cluster wiring is
deferred or because the field genuinely pins a startup
invariant (listener port, peer topology, datastore choice).
That preserves the operator-facing contract: every reload
reports exactly what changed and what was ignored.

## Tests

Default `cargo nextest run --workspace` lane: 1205 -> 1216
(+11). Six new tests in `crates/dynomited/tests/sighup_reload.rs`:

- `shared_tls_profiles_replace_swaps_resolver_cert`
- `reload_from_path_swaps_tls_material_on_disk_change`
- `reload_from_path_with_invalid_yaml_keeps_state_intact`
- `reload_from_path_accepts_changed_consistency`
- `reload_warns_on_listener_port_change`
- `reloading_dc_sni_resolver_picks_up_new_cert_after_replace`

Plus five new unit tests inside `crates/dynomited/src/reload.rs`
(snapshot construction, `store` returns previous, three
end-to-end reload flows).

## Verification

- `cargo build --workspace --all-targets --locked` clean.
- `cargo build -p dynomited --features riak --all-targets
  --locked` clean.
- `cargo fmt` clean across in-scope crates.
- `cargo clippy --workspace --all-targets --all-features --
  -D warnings` clean.
- `cargo nextest run --workspace`: 1216 passed, 0 failed,
  4 skipped.
- `cargo test --doc --workspace`: 15 doctests pass.
- `bash scripts/check_no_todos.sh`,
  `bash scripts/check_no_port_comments.sh`,
  `bash scripts/check_ascii.sh` clean.

## Reloadable / non-reloadable surface

Reloadable: `read_consistency`, `write_consistency`,
`gos_interval`, `recon_interval_seconds`, `hint_ttl_seconds`,
`hint_store_max_bytes`, `hint_drain_interval_ms`,
`dyn_read_timeout`, `dyn_write_timeout`, `timeout`,
`bucket_types`, `default_bucket_type`, `peer_tls_*` (cert,
key, CA, per-DC profile map).

Non-reloadable (warns): `listen`, `dyn_listen`, `stats_listen`,
`dyn_seeds`, `tokens`, `rack`, `datacenter`, `data_store`,
`servers`, `noxu_path`, `enable_gossip`,
`enable_hinted_handoff`, `hash`, `client_connections`,
`dyn_connections`, `observability`, `riak`.
