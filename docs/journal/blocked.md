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

### Crypto padding-oracle surface

The Stage 6 review flagged that `crypto::aes::decrypt_to_vec`
returns either `CryptoError::BadPadding` (from
`Crypter::finalize` failure) or `CryptoError::DecryptionFailed`
(from `Crypter::update` failure or length validation). The two
are distinguishable to a caller. In the DNODE handshake context
that surfaces the decrypt result to peers, this is a textbook
Vaudenay padding-oracle. Stage 9 wires that surface; resolve
there by mapping all decrypt errors to a single externally-
visible variant before returning to the caller, then expose the
detail only via `tracing` events scoped to the local process.

### Crypto PEM-loader panic-free proptest

The Stage 6 review recommended a one-line `proptest!` case in
`crypto::pem::tests` asserting that
`load_rsa_private_key_from_bytes` does not panic on arbitrary
`Vec<u8>` inputs of length 0..4096. Low-cost coverage for the
panic-free contract; track alongside the `crypto_aes_decrypt`
fuzz target above so the additions land together.

### Stage 9: QUIC + openssl-vendored link conflict

The `quic` cargo feature pulls in `quiche`, which bundles its
own BoringSSL. The `openssl` workspace dependency is configured
with the `vendored` feature, which statically links a copy of
OpenSSL's libcrypto. Binaries that link both static archives
(test artifacts built with `--all-features`) fail with
`multiple definition` linker errors on every shared symbol
(`EVP_*`, `BIO_*`, `RSA_*`, ...).

The Stage 9 QUIC code compiles cleanly as a library (`cargo
build -p dynomite --features quic`) but the integration test
that exercises the QUIC end-to-end loopback cannot link in
the same artifact as the Stage 6 crypto code. Stage 9 ships
the transport behind `#[cfg(feature = "quic")]`; the
end-to-end test is documented as deferred until either the
`openssl-vendored` feature is replaced with system
`openssl-sys` (Stage 12 binary-wiring decision) or the Stage 6
crypto module is migrated to a non-OpenSSL backend
(`aws-lc-rs` or `boring`).

Non-QUIC Stage 9 surfaces (TCP loopback echo, `ConnPool`,
`AutoEject`, IPv6 dual-stack, role-specific FSMs) ship and
test cleanly without the conflict.

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
