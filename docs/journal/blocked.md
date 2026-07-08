# Blockers / Cross-cutting follow-ups

Items requiring a human decision are top-level. Cross-cutting cleanups
that are not blockers but need to be addressed at a planned point are
listed below.

## Cross-cutting follow-ups

### `conf_parse` cargo-fuzz target

AGENTS.md Section 6.4 lists `conf_parse` as a mandatory fuzz target.
Deferred: the fuzz target lands in the same crate
(`crates/fuzz/`) as `proto_redis_parse`, `proto_memcache_parse`,
`dnode_parse`, and `crypto_aes_decrypt`. Those parsers are the
high-risk fuzz targets and arrive in Stages 7 and 8. The fuzz crate
is created at that point with all five targets; until then, Stage
4 has property tests covering parser totality on arbitrary YAML
strings via `proptest`.

### `crypto_aes_decrypt` cargo-fuzz target

AGENTS.md Section 6.4 also lists `crypto_aes_decrypt` as a
mandatory fuzz target. Deferred to the same fuzz-crate creation
in Stage 7 / Stage 8 alongside `conf_parse` and the protocol
parsers. The Stage 6 review (review/stage-6) flagged its absence;
folding it into the single fuzz-crate dispatch is cheaper than
spinning up a one-target crate now.

### Crypto padding-oracle surface (resolved)

The Stage 6 review flagged that `crypto::aes::decrypt_to_vec`
returns either `CryptoError::BadPadding` or
`CryptoError::DecryptionFailed`, distinguishable to a caller. In
the peer-plane handshake context that surfaces the decrypt
result to peers, this is a textbook Vaudenay padding-oracle.

Resolution (Stage 9 review response): the dnode peer-client
driver consumes the decrypt result through
`net::dnode_client::decrypt_dnode_payload`, which collapses any
failure into a single opaque `NetError::Dnode("dnode payload
decrypt failed")` before the loop can write a response frame.
The detail-level variant is dropped at the boundary - no
`tracing` event is emitted in the failure arm so an attacker
cannot use log timing or content to distinguish bad-padding
from decryption-failed either. See the Stage 9 deviation entry
in `docs/parity.md`.

### Crypto PEM-loader panic-free proptest

The Stage 6 review recommended a one-line `proptest!` case in
`crypto::pem::tests` asserting that
`load_rsa_private_key_from_bytes` does not panic on arbitrary
`Vec<u8>` inputs of length 0..4096. Low-cost coverage for the
panic-free contract; track alongside the `crypto_aes_decrypt`
fuzz target above so the additions land together.

### Stage 9: QUIC + crypto coexistence (resolved)

The original `quic` feature pulled in `quiche` which bundles its
own BoringSSL while the original Stage 6 used the `openssl` crate
with the `vendored` feature. The two static archives both
exported the OpenSSL ABI symbols and produced multi-definition
linker errors when the QUIC integration test attempted to link
both into one artifact.

Resolution: Stage 6 crypto migrated to the pure-Rust RustCrypto
stack (`aes`/`cbc`/`rsa`/`sha1`/`rand`). With no C-binding crypto
in the workspace, `cargo build --workspace --all-features` and
`cargo nextest run --workspace --all-features` both succeed.
The QUIC end-to-end test now runs alongside the AES/RSA tests in
a single `--all-features` artifact.

Follow-up: see the RUSTSEC-2023-0071 entry below.

### Stage 9: cluster-side dispatcher seam

The per-role FSMs (CLIENT, SERVER, DNODE_PEER_*) hand parsed
requests to a `Dispatcher` trait. Stage 10 implements the
cluster-aware dispatcher (DC/rack routing, quorum, fragment
bookkeeping); Stage 9 ships the seam plus a `NoopDispatcher`
so integration tests can exercise the framing without the
cluster stack. The C-equivalent functions deferred here are
listed in the per-file parity rows under Stage 9 with the
phrase `deferred to Stage 10 (cluster routing)`.

### Stage 6 follow-up: RSA timing sidechannel (RUSTSEC-2023-0071)

The Stage 6 crypto migration from the `openssl` C-binding crate to
the pure-Rust RustCrypto stack (`aes`/`cbc`/`rsa`/`sha1`/`rand`) was
required to resolve the Stage 9 link-time symbol clash between the
openssl-vendored static archive and quiche's bundled BoringSSL. The
two static archives both export the OpenSSL ABI symbols
(`EVP_rc2_40_cbc`, `EVP_rc4`, `EVP_BytesToKey`, etc.), causing
`ld: multiple definition` errors when the `quic` feature is on.

The migration leaves us on `rsa` 0.9.10 which carries
RUSTSEC-2023-0071 (Marvin Attack: potential key recovery through
timing sidechannels). Upstream has not yet released a constant-time
implementation; the issue is tracked at
https://github.com/RustCrypto/RSA/issues/626.

Mitigation status:
* The advisory targets PKCS#1 v1.5 padding-oracle attacks
  specifically. The dynomite::crypto module uses OAEP, matching the
  `RSA_PKCS1_OAEP_PADDING` choice in dyn_crypto.c lines 521-538.
* The non-constant-time underlying arithmetic is still a theoretical
  concern for adversaries that can observe inter-peer DNODE
  handshake timing.
* deny.toml ignores RUSTSEC-2023-0071 with a written justification.
* scripts/check.sh passes `--ignore RUSTSEC-2023-0071` to `cargo audit`.
* The advisory is recorded as a Stage 15 hardening item: when
  RustCrypto ships a fix or we add an HSM/KMS adapter via the
  CryptoProvider trait (Stage 13 embedding API), the override goes
  away.

## QUIC PBC round-trip test flake (transport-poll readiness race) -- RESOLVED

Date noted: 2026-07-08. Resolved: 2026-07-08.

Root cause (found by driver-level tracing): the QUIC listener shared a
single UDP socket across the accept path and every connection driver,
each calling `recv_from` on it. The dyniak/dnode QUIC servers loop-accept
(`serve_pbc_quic_inner` calls `accept()` again after spawning a handler),
so the re-entered accept loop competed with the live connection's driver
for inbound datagrams and discarded the connection's later packets (it
only keeps Initial packets). The connection then stalled on its second
request and timed out. Not a timing race in the adapter -- a
socket-ownership defect.

Fix (crates/dynomite/src/net/quic.rs): the listener now spawns one demux
task that owns the socket read side, routes each datagram to the right
connection by peer address, opens a connection on a fresh Initial
packet, and hands accepted transports to `accept()` over a channel.
Connection drivers no longer read the shared socket; the server driver
receives datagrams from its demux-fed channel, the client driver reads
its own connected socket. The driver select also gained an explicit
app-bytes branch so queued writes flush without waiting for a timer tick.

Verification (deterministic reproduction, AGENTS.md 6.5): new regression
test `quic_many_sequential_round_trips_on_one_connection` in
`crates/dynomite/tests/stage_09_quic.rs` drives ten sequential
round-trips against a loop-accepting server. Teeth confirmed: 12/12
failures on the pre-fix code, 15/15 passes on the fix; the original
`quic_pbc_ping_put_get_round_trip` passed 25/25 after the fix (it failed
~1-in-3 before). The nextest retry override has been removed.
