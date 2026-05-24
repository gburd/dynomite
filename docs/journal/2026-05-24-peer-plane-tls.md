# 2026-05-24 - peer-plane TLS

Stage: cross-cutting (network)
Branch: stage/peer-plane-tls

## Scope

This slice adds explicit TLS for two surfaces:

* The DNODE peer plane (inbound `DnodeProxy` listener and outbound
  per-peer supervisors in `dynomited::server`).
* The Riak protocol gateways (`dyn_riak::serve_pbc` and
  `dyn_riak::serve_http`).

Both surfaces remain plaintext by default. Operators opt in by
setting two PEM paths each: `peer_tls_cert` + `peer_tls_key` for
the peer plane, and `riak.tls_cert` + `riak.tls_key` for the Riak
gateways. An optional CA bundle (`peer_tls_ca` / `riak.tls_ca`)
turns the listener into a mutual-TLS deployment.

## Format choices

* PEM, not DER. PEM is what Riak / Dynomite operators already
  ship; the `pem_key_file:` knob the engine has historically
  consumed for the inter-node AES handshake is also PEM. The
  loader (`crates/dynomite/src/net/tls.rs::load_server_config`)
  uses `rustls-pemfile` and tolerates RSA, PKCS#8, and SEC1
  private keys via `rustls_pemfile::private_key`.
* Self-signed test certs are generated at runtime via `rcgen`
  (already a workspace dev-dep). No PEM material is committed.

## mTLS opt-in

Mutual TLS is gated on a third path being present:

* `peer_tls_ca` / `riak.tls_ca` set: the listener requires every
  inbound peer / client to present a certificate signed by a CA
  in that PEM bundle. Built via
  `rustls::server::WebPkiClientVerifier`.
* CA path unset: the listener still terminates TLS but does not
  request a client certificate (`with_no_client_auth()`).

The CA path also drives the outbound trust anchors for the dnode
peer supervisors. When `peer_tls_ca` is set, the peer supervisor
pins that bundle as its only trust root; when unset, the bundled
`webpki_roots` Mozilla roots are used. Operators running an
internal-only PKI must set the CA path; operators chaining peer
certs to a public CA can leave it unset.

## rustls vs native-tls

We picked `tokio-rustls` 0.26 over `tokio-native-tls`. Drivers:

1. The transitive crypto stack is already rustls-shaped: the
   QUIC transport (`crates/dynomite/src/net/quic.rs`) uses the
   bundled `aws-lc-rs` provider that ships inside `quiche`, and
   the `rustls-pki-types` 1.x lockfile entry was already
   present.
2. The project's CI runs in `nix develop` without a system
   OpenSSL, so a native-tls + OpenSSL stack would force the
   shell into a much heavier dependency set.
3. `rustls` 0.23 lets us select the `ring` provider explicitly,
   matching what `rcgen`'s test fixtures expect; we install it
   once via a `OnceLock` in `tls.rs::ensure_provider_installed`.

The chosen versions:
* `rustls = "0.23"` (default-features off, `std` + `ring`)
* `tokio-rustls = "0.26"` (default-features off, `ring`)
* `rustls-pemfile = "2"`
* `webpki-roots = "0.26"`
* `hyper-rustls = "0.27"` (dev-only, used by the Riak HTTP TLS
  test for the client-side connector path)

## Touched files

* `Cargo.toml` (workspace): five new workspace deps.
* `crates/dynomite/Cargo.toml`: `rustls`, `tokio-rustls`,
  `rustls-pemfile`, `webpki-roots`.
* `crates/dyn-riak/Cargo.toml`: same set + `hyper-rustls` /
  `http-body-util` / `hyper-util` extra features for tests.
* `crates/dynomited/Cargo.toml`: same set as `dynomite`.
* `crates/dynomite/src/net/tls.rs`: new module. Exposes
  `load_server_config`, `load_client_config`, `acceptor_from`,
  `connector_from`, `server_name_owned`, `TlsServerTransport`,
  `TlsClientTransport`, and the `TlsError` enum.
* `crates/dynomite/src/net/mod.rs`: re-exports + `NetError::Tls`
  variant.
* `crates/dynomite/src/net/dnode_proxy.rs`: `with_tls(acceptor)`
  builder + accept-loop branch that wraps each inbound socket
  via `TlsAcceptor::accept`. Handshake failures are logged at
  `warn!` and the connection is dropped, matching the existing
  policy for malformed peers.
* `crates/dynomite/src/conf/pool.rs`: three new `Option<PathBuf>`
  fields on `ConfPool` (`peer_tls_cert`, `peer_tls_key`,
  `peer_tls_ca`); same triple on `ConfRiak` (`tls_cert`,
  `tls_key`, `tls_ca`); `validate_tls_pair` cross-check in
  `ConfPool::validate` and `ConfRiak::validate`.
* `crates/dyn-riak/src/server.rs`: new `serve_pbc_tls(listener,
  ds, acceptor)`. The plaintext `serve_pbc` keeps its existing
  signature; both end up in `serve_pbc_inner` so the per-frame
  loop is shared.
* `crates/dyn-riak/src/proto/http/mod.rs`: new `serve_http_tls`
  using `tokio_rustls::TlsAcceptor` + `TokioIo` -> hyper's
  `http1::serve_connection`.
* `crates/dyn-riak/src/lib.rs`: re-exports `serve_pbc_tls` /
  `serve_http_tls`.
* `crates/dynomited/src/server.rs`: `PeerTlsRuntime` struct;
  `build_peer_tls_runtime(&ConfPool)`; the peer supervisor
  spawn loop threads the runtime through; the supervisor wraps
  each connected `TcpStream` via `TlsConnector::connect` when
  the runtime is `Some`, falling through the existing
  reconnect-backoff machinery on TLS failure.
* `crates/dynomited/src/riak.rs`: `RiakHandles.tls`,
  `build_riak_tls_acceptor`, dispatch to `serve_*_tls` when
  `tls.is_some()` in `spawn_listeners`.

## Tests

New tests:

* `crates/dynomite/src/net/tls.rs` (5): loader smoke tests for
  cert/key load, missing-cert error path, empty-cert error
  path, default `webpki_roots` client config, and the
  `server_name_owned` helper.
* `crates/dynomite/src/net/dnode_proxy.rs` (2): `bind_returns_local_addr`
  also asserts `!has_tls()`; new `with_tls_attaches_acceptor`
  exercises the builder.
* `crates/dynomite/src/conf/pool.rs` (7): `peer_tls_pair_*`,
  `riak_tls_*` validation cases.
* `crates/dynomite/tests/peer_plane_tls.rs` (3): full
  round-trip of a Redis-shaped DNODE frame over a `DnodeProxy`
  with TLS, plaintext-mode regression, and a `webpki_roots`
  smoke.
* `crates/dyn-riak/tests/tls_round_trip.rs` (4): `RpbPingReq`
  over TLS, plaintext PBC `RpbPingReq` (regression), `GET
  /ping` over TLS, plaintext `GET /ping` (regression).
* `crates/dynomited/src/riak.rs` (2): `build_handles` loads the
  TLS acceptor when both paths are set; rejects mismatched
  pair.

Counts: nextest `--workspace` increases from 1006 to 1026
(+20). With `--all-features` it increases to 1045.

## Deferred: STARTTLS for Riak PBC

`RpbStartTls` (msg code 255) and `RpbAuthReq` (256) are the
documented Riak path for opportunistic TLS upgrade on a
plaintext PBC connection. They are not implemented in this
slice. The always-on TLS surface (operator sets
`riak.tls_cert`, the listener terminates TLS for every
accepted connection) is the v1 contract and is sufficient for
operators wiring a fronting load balancer.

To implement later: in `dyn_riak::server::handle_conn`,
recognize `MessageCode::StartTls` (a new entry on the
`MessageCode` enum, value 255), respond with an
`RpbStartTlsResp`, then upgrade the in-flight stream to a
`tokio_rustls::server::TlsStream` and continue the read /
write loop on the upgraded stream. The hand-off requires
threading a per-connection `Option<TlsAcceptor>` through
`handle_conn` and handling the half-handshake state where the
listener has accepted but not yet completed the upgrade. The
`Frame` reader already uses `tokio::io::AsyncRead`, so the
swap is straightforward; what makes it a separate slice is
the test surface: STARTTLS deployments need round-trip tests
with an `RpbAuthReq` (cert auth) and an `RpbAuthResp`
(success / error), plus a regression test for the
"upgrade-without-Auth" path.

## Backward compatibility

The plaintext path is the default everywhere. The conf
defaults leave `peer_tls_cert` / `peer_tls_key` / `tls_cert` /
`tls_key` at `None`. With the existing test suite intact,
`stage_09_net::dnode_peer_round_trip` continues to exercise
the plaintext DNODE path via plain `TcpTransport`. The
`dyn-riak::tests::pbc_round_trip` and
`dyn-riak::tests::http_round_trip` integration tests are
unchanged.

## Lints

No new `#[allow]` directives were required. The dnode_proxy
test's `bind_returns_local_addr` keeps its existing
`#[tokio::test]` attribute and the new TLS helper test in the
same module is similarly free of allowances.

## Known follow-ups

1. STARTTLS for the Riak PBC server (above).
2. Per-DC TLS profiles. The `secure_server_option:` knob
   already supports a `dc` / `rack` / `all` switch for the AES
   payload encryption; the new `peer_tls_*` fields are
   pool-wide and do not yet honor that selector. Wiring
   per-DC TLS would mean carrying a `TlsConnector` per DC on
   the supervisor and is a separate slice.
3. Reload-on-SIGHUP for cert rotation. `Server::handle_reload`
   currently re-reads the YAML but does not rebuild the TLS
   acceptor; in production deployments this is expected to be
   driven by an orchestration layer that restarts the binary
   on cert rotation. Hot-rotating the acceptor without
   dropping in-flight peer connections is also a follow-up.
