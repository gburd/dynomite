# Stage 6 - Crypto

Date: 2026-05-19
Branch: `stage/6-crypto`
Worker: stage-6 sub-agent

## Summary

Implements `dynomite::crypto`: AES-256-CBC payload crypto, RSA wrap
and unwrap, base64 helpers, and PEM key loading. Wires the module
into `crates/dynomite/src/lib.rs` behind the existing
`#![forbid(unsafe_code)]` guard. Tests cover unit-level invariants,
property tests for round-trip stability, and a frozen-output
fixture under `crates/dynomite/tests/fixtures/crypto/`.

## Files touched

- `crates/dynomite/Cargo.toml` - made `openssl` an unconditional
  dependency. The `tls` feature is retained as a placeholder for
  future client-TLS code paths.
- `crates/dynomite/src/lib.rs` - added `pub mod crypto;`.
- `crates/dynomite/src/crypto/mod.rs` (new) - `Crypto` bundle and
  `CryptoError`.
- `crates/dynomite/src/crypto/aes.rs` (new) - AES-256-CBC primitives
  in three flavors: `Vec<u8>` <-> `Vec<u8>`, slice -> chain, chain
  -> chain.
- `crates/dynomite/src/crypto/base64.rs` (new) - thin wrapper around
  the workspace `base64` crate.
- `crates/dynomite/src/crypto/pem.rs` (new) - PEM loader that sniffs
  PKCS#1 vs PKCS#8 framing and returns `Rsa<Private>`.
- `crates/dynomite/src/crypto/rsa.rs` (new) - RSA wrap and unwrap
  with PKCS#1 v1.5 padding.
- `crates/dynomite/examples/gen_crypto_fixture.rs` (new) - one-shot
  utility that regenerates the frozen-output fixture.
- `crates/dynomite/tests/stage_06_crypto.rs` (new) - integration
  tests.
- `crates/dynomite/tests/fixtures/crypto/` (new) - bundled
  `dynomite.pem` (copied from the C tree), `aes_key.bin`,
  `plaintext.bin`, `cipher.bin`.
- `docs/parity.md` - added the `dyn_crypto.{c,h}` row block plus
  four Stage 6 deviation entries.

## Parity rows added

24 rows under the new `### dyn_crypto.{c,h}` subsection, plus four
new entries under `## Deviations`. Net `PARITY_DELTA = 24` C
symbols mapped (counted as one per C identifier), with four
explicit Deviation entries.

## Test count

- Unit tests under `crates/dynomite/src/crypto/`: 22 (including
  proptest-driven sub-cases).
- Integration tests in `tests/stage_06_crypto.rs`: 11.
- Stage 6 doctest count contribution: 32 runnable doctests.

Workspace totals after this stage:

- `cargo nextest run --workspace`: 293 tests, all passing (up from
  261, +32).
- `cargo test --doc --workspace`: 321 tests, all passing (up from
  267, +54).

## Deviations from the C reference

Recorded in `docs/parity.md` under `## Deviations`. Summary:

1. **AES-128-CBC -> AES-256-CBC with random per-message IV
   prepended.** The C source uses AES-128 and reuses the key
   bytes as the IV, which makes the cipher deterministic for a
   given (key, plaintext) pair. The Stage 6 brief directs the
   Rust port to upgrade to AES-256 with a random 16-byte IV
   prepended to the output. This is not wire-compatible with a C
   peer; Stage 9 will revisit if cross-version handshakes are
   required.
2. **Long-lived static `EVP_CIPHER_CTX *` replaced by per-call
   `openssl::symm::Crypter`.** The C globals are not safe under
   concurrent worker tasks; the Rust port allocates a fresh
   crypter per call. Functionally equivalent.
3. **RSA padding.** The C source actually uses
   `RSA_PKCS1_OAEP_PADDING` (`dyn_crypto.c::dyn_rsa_encrypt` and
   `dyn_rsa_decrypt`), but the Stage 6 brief directs the Rust
   port to use PKCS#1 v1.5 (citing C parity). The brief is the
   authoritative task spec; the port follows the brief and uses
   PKCS#1 v1.5. Stage 9 will revisit when wire compatibility
   with a C peer becomes a constraint.
4. **`base64_encode` is unpadded.** Every in-tree consumer parses
   back through `base64_decode`, which accepts both padded and
   unpadded inputs. A future external consumer can switch to the
   padded engine without touching the decode side.

## Ambiguities

- The Stage 6 brief described the AES output format as
  "matches the C layout exactly" but the C layout actually
  differs (no IV prepending, AES-128, key-as-IV). The Rust port
  follows the brief's explicit layout description and records
  the wire-format mismatch as deviation #1.
- The Stage 6 brief described the RSA padding choice as PKCS#1
  v1.5 "to match C" but the C actually uses OAEP. Resolved as
  deviation #3.
- The dyn_aes_encrypt brief signature accepts a destination
  `MbufPool` rather than mutating a single caller-supplied
  `Mbuf`. We chose the pool-based form because the prepended
  IV plus PKCS#7 padding can exceed the 16-byte trailing extra
  region the C `mbuf->end_extra` reservation provides.

## Cross-impl test

The Stage 6 brief's preferred path was a hand-traced fixture
captured against the C `dyn_aes_encrypt`. Hand-tracing is not
useful here because the Rust wire format intentionally differs
from the C wire format (deviation #1), so a C-side capture would
not decrypt with the Rust impl. The brief allows a Rust-side
round-trip fixture as the fallback; that is what is committed.
The fixture (`tests/fixtures/crypto/{aes_key,plaintext,cipher}.bin`)
is regenerated by `examples/gen_crypto_fixture.rs` and pinned by
`tests/stage_06_crypto.rs::cross_version_cipher_round_trip`,
which decrypts the committed ciphertext, re-encrypts the
plaintext (producing a different IV), and decrypts again to
confirm round-trip stability.

## Verification

```
cargo fmt --all -- --check          # clean
cargo clippy --workspace --all-targets --all-features -- -D warnings  # clean
cargo build --workspace --all-targets --locked   # clean
cargo nextest run --workspace        # 293 / 293 passed
cargo test --doc --workspace         # 321 / 321 passed
scripts/check_no_port_comments.sh    # clean
scripts/check_no_todos.sh            # clean
scripts/check_ascii.sh               # clean
```

## Open questions for the lead

- The RSA padding deviation (PKCS#1 v1.5 vs the C source's actual
  OAEP) is worth a re-confirmation before Stage 9. If wire
  compatibility with the C reference is a hard requirement, the
  Stage 6 RSA implementation will need to switch to OAEP.
- Same question for the AES wire format. The current Rust output
  is a strict superset of information (random IV prepended) and
  is not wire-compatible with a C peer.
