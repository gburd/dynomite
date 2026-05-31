# 2026-05-24 - per-DC peer-plane TLS profiles

Stage: cross-cutting (network)
Branch: stage/tls-per-dc-profiles

## Scope

Extends the peer-plane TLS surface added in commit `5eee3bf`
to support per-DC TLS profiles, so an operator running multiple
data centers can use different cert / key / CA bundles per DC.

YAML shape:

```yaml
dyn_o_mite:
  peer_tls_profiles:
    dc1:
      cert: /etc/dynomite/dc1.pem
      key: /etc/dynomite/dc1.key
      ca: /etc/dynomite/dc1-ca.pem
    dc2:
      cert: /etc/dynomite/dc2.pem
      key: /etc/dynomite/dc2.key
  # Legacy single-profile knobs still work and become the
  # implicit "default" profile, used for any DC not listed
  # in peer_tls_profiles.
  peer_tls_cert: /etc/dynomite/default.pem
  peer_tls_key:  /etc/dynomite/default.key
  peer_tls_ca:   /etc/dynomite/default-ca.pem
```

Behaviour matrix:

| `peer_tls_profiles` | legacy `peer_tls_*` | per-peer effect |
|--|--|--|
| empty | unset | plaintext peer plane (unchanged) |
| empty | set | every peer uses the legacy triple (unchanged) |
| has `dcN` | unset | DCs in the map use their entry; other DCs are plaintext |
| has `dcN` | set | DCs in the map use their entry; other DCs use the legacy triple |

## Implementation

### Conf surface

* New `ConfTlsProfile` struct with `cert / key / ca` fields
  plus a `validate(dc)` method that enforces the same `(cert,
  key)` cross-check the legacy fields use, and rejects `ca`
  without the cert / key pair.
* New `ConfPool::peer_tls_profiles: BTreeMap<String,
  ConfTlsProfile>` (default empty). `ConfPool::validate`
  iterates the map and validates every entry, plus rejects
  empty DC names.
* Re-exported as `dynomite::conf::ConfTlsProfile`.

### TLS engine surface

`crates/dynomite/src/net/tls.rs`:

* New `TlsProfileSpec { cert, key, ca }` (resolved on-disk
  paths).
* New `TlsProfileMap`: precompiled per-DC `Arc<ServerConfig>`
  + `Arc<ClientConfig>` + `Arc<CertifiedKey>`, plus an
  optional default profile carried through the same shape.
  `is_empty()`, `server_config_for_dc(dc)` /
  `client_config_for_dc(dc)` (each falling back to the
  default), `default_server_config()` /
  `default_client_config()`, `dc_names()`,
  `requires_client_auth()`, and `build_sni_acceptor()` which
  produces a single `tokio_rustls::TlsAcceptor` whose
  `ServerConfig` uses a custom `ResolvesServerCert` impl.
* New `dc_sni_hostname(dc) -> "dc-<dc>.dynomite.local"` plus
  the inverse parser `dc_from_sni_label`. Both ends of a
  peer-plane handshake set / parse this label so the listener
  can pick the matching cert without coupling the cert's SAN
  to the routing key.
* The custom `DcSniResolver` parses the SNI through
  `dc_from_sni_label`, looks up a per-DC `Arc<CertifiedKey>`,
  and falls back to the default `CertifiedKey` when the SNI
  is missing or does not match. Returning `None` aborts the
  handshake; only an unset-default + unrecognized-SNI path
  hits that branch.
* When at least one profile carries a CA bundle, the listener
  builds a single `WebPkiClientVerifier` whose root store is
  the union of every profile's CAs (mTLS uniform across
  SNI-routed certs). When no profile carries a CA, the
  listener uses `with_no_client_auth()`.

### Peer supervisor wiring

`crates/dynomited/src/server.rs`:

* `PeerTlsRuntime` now wraps the `TlsAcceptor` (built once
  via `TlsProfileMap::build_sni_acceptor`) plus the
  `TlsProfileMap` itself, so the supervisor can ask the map
  for a per-peer client config without re-loading PEM bytes
  per dial.
* `build_peer_tls_runtime` now folds both the legacy
  `peer_tls_*` triple and the new `peer_tls_profiles` map
  into a `TlsProfileMap` build call. Returns `Ok(None)` when
  both are empty (plaintext default preserved).
* The peer-supervisor spawn site captures `peer.dc()` and
  passes it through to the supervisor.
* `peer_supervisor` resolves a per-peer
  `Option<TlsConnector>` once at start (from the profile
  map's `client_config_for_dc(peer_dc)`) and dials with
  SNI=`dc-<peer_dc>.dynomite.local`. When the lookup returns
  `None` the supervisor stays on the plaintext path for that
  peer (matching the brief's "if neither is set, the
  connection is plaintext" contract).

### Riak gateways

The Riak PBC and HTTP listeners are out of scope for this
slice and continue to consume `riak.tls_*` exclusively. The
brief calls out that Riak gateways could draw from the local
DC's profile in a follow-up; that re-use is straightforward
once a `local_dc` argument threads through `build_handles`,
but would have meant touching `crates/dyniak` (out-of-scope
per the brief's hard constraints) and is deferred. The
existing `riak.tls_*` knobs stay backward-compatible.

## Backward compatibility

* Plaintext default unchanged: empty `peer_tls_profiles` +
  unset `peer_tls_*` -> no TLS.
* Legacy single-profile path unchanged: `peer_tls_cert` /
  `peer_tls_key` / `peer_tls_ca` produce a profile map with
  a single default entry, and the supervisor's per-DC lookup
  always returns that default for every peer.
* Existing `peer_plane_tls` integration tests pass without
  modification (verified locally).

## Tests

New tests:

* `crates/dynomite/src/net/tls.rs` (6): `dc_sni_hostname`
  round-trip, `tls_profile_map_empty_is_empty`,
  `tls_profile_map_default_only_falls_back`,
  `tls_profile_map_per_dc_overrides_default`,
  `tls_profile_map_per_dc_only_no_default`,
  `tls_profile_map_propagates_load_error`.
* `crates/dynomite/src/conf/pool.rs` (8): per-DC profile
  validate (unset, mismatched pair, ca-without-cert),
  `peer_tls_profiles_empty_dc_name_rejected`,
  `peer_tls_profiles_per_dc_pair_validates`,
  `peer_tls_profiles_per_dc_cert_without_key_rejected`,
  `peer_tls_profiles_yaml_round_trip`.
* `crates/dynomite/tests/peer_plane_tls.rs` (4):
  * `per_dc_profile_map_routes_dc1_to_dc2_via_sni` - drives a
    Redis-shaped DNODE frame over a `DnodeProxy` whose
    listener was built from a two-DC profile map; the client
    dials with SNI=`dc-dc2.dynomite.local` and the server's
    SNI resolver picks the dc2 cert. The test compares DER
    bytes of the cert presented by the listener against the
    on-disk PEM.
  * `per_dc_profile_map_falls_back_to_default_for_unknown_dc`
    - confirms `client_config_for_dc("dc3")` and
    `server_config_for_dc("dc3")` fall back to the default
    profile when the per-DC entry is absent (the per-peer
    supervisor relies on this contract).
  * `legacy_default_profile_only_round_trip` - default-only
    profile (built through the new `TlsProfileMap`) drives a
    Redis frame to verify the legacy `peer_tls_*` shape is
    backward-compatible.
  * `peer_tls_profile_cert_without_key_validation_fails` - a
    `ConfTlsProfile` with `cert` set but `key` unset is
    rejected by `validate`.

Test counts: `cargo nextest run --workspace` increases from
1187 to 1205 (+18). With `--all-features` it increases from
1236 to 1254.

## Lints

No new `#[allow(...)]` directives. The `peer_plane_tls`
integration test was split into a tail helper `drive_redis_frame`
to keep the per-DC happy-path test under the project's
`clippy::too_many_lines` budget. The `TlsProfileMap` Debug
impl uses `finish_non_exhaustive` because the precompiled
configs are intentionally hidden from the operator-visible
log line.

## Verification

```
cargo build --workspace --all-targets --locked       # OK
cargo build -p dynomited --features riak --all-targets --locked  # OK
cargo fmt -p dynomite -p dynomited -p dyn-hash-tool \
         -p dyn-encoding -p dyniak -p dyn-admin -- --check  # OK
cargo clippy --workspace --all-targets --all-features -- -D warnings  # OK
cargo nextest run --workspace            # 1205 / 1205 passed
cargo nextest run --workspace --all-features  # 1254 / 1254 passed
cargo test --doc --workspace             # 15 passed
bash scripts/check_no_todos.sh           # OK
bash scripts/check_no_port_comments.sh   # OK
bash scripts/check_ascii.sh              # OK
```

## Known follow-ups

1. Wire Riak gateways to draw from the local DC's profile
   when `riak.tls_*` is unset (requires touching
   `crates/dyniak`; out-of-scope here).
2. Reload-on-SIGHUP for cert rotation (still deferred from
   the original peer-plane TLS slice).
3. Per-profile mTLS scopes: today the listener's mTLS root
   store is the union of every profile's CAs. A stricter
   shape would only accept a peer cert chained to the CA of
   the SNI-routed profile; that requires a custom
   `ClientCertVerifier` keyed on SNI and is a separate slice.
