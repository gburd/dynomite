//! Stage 6 (crypto) end-to-end coverage.
//!
//! Includes property tests for AES round-tripping, RSA wrap/unwrap
//! against the bundled PEM fixture, base64 round-tripping, an
//! `mbuf`-piped AES round-trip, and a frozen-output regression test
//! that pins the wire format.

#![allow(
    clippy::format_push_string,
    clippy::needless_continue,
    clippy::unnecessary_debug_formatting,
    clippy::match_same_arms,
    clippy::similar_names,
    clippy::cast_possible_truncation
)]
use std::fs;
use std::path::{Path, PathBuf};

use dynomite::crypto::aes::{
    decrypt_chain_to_chain, decrypt_to_vec, encrypt_to_chain, encrypt_to_vec, AES_BLOCK_SIZE,
    AES_KEYLEN,
};
use dynomite::crypto::pem::load_rsa_private_key_from_bytes;
use dynomite::crypto::{base64_decode, base64_encode, Crypto};
use dynomite::io::mbuf::MbufPool;
use proptest::prelude::*;

fn fixture_dir() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("fixtures");
    p.push("crypto");
    p
}

fn read_fixture(name: &str) -> Vec<u8> {
    let p: &Path = &fixture_dir().join(name);
    fs::read(p).unwrap_or_else(|e| panic!("cannot read {p:?}: {e}"))
}

#[test]
fn aes_round_trip_known_lengths() {
    let key = Crypto::generate_aes_key().unwrap();
    for n in [0usize, 1, 15, 16, 17, 31, 32, 33, 4096] {
        let msg: Vec<u8> = (0..n)
            .map(|i| {
                u8::try_from(i & 0xff)
                    .expect("masked to u8")
                    .wrapping_mul(31)
            })
            .collect();
        let cipher = Crypto::aes_encrypt(&msg, &key).unwrap();
        assert_eq!(cipher.len() % AES_BLOCK_SIZE, 0);
        let plain = Crypto::aes_decrypt(&cipher, &key).unwrap();
        assert_eq!(plain, msg, "round trip failed at len {n}");
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn aes_round_trip_property(msg in prop::collection::vec(any::<u8>(), 0..4096)) {
        let key = Crypto::generate_aes_key().unwrap();
        let cipher = Crypto::aes_encrypt(&msg, &key).unwrap();
        prop_assert!(cipher.len() >= AES_BLOCK_SIZE);
        let plain = Crypto::aes_decrypt(&cipher, &key).unwrap();
        prop_assert_eq!(plain, msg);
    }

    #[test]
    fn aes_chain_round_trip_property(msg in prop::collection::vec(any::<u8>(), 0..4096)) {
        let pool = MbufPool::default();
        let key = Crypto::generate_aes_key().unwrap();
        let mut chain = encrypt_to_chain(&msg, &key, &pool).unwrap();
        let plain = Crypto::dyn_aes_decrypt_to_vec(&mut chain, &key).unwrap();
        prop_assert_eq!(plain, msg);
    }

    #[test]
    fn aes_chain_to_chain_property(msg in prop::collection::vec(any::<u8>(), 0..4096)) {
        let pool = MbufPool::default();
        let key = Crypto::generate_aes_key().unwrap();
        let mut chain = encrypt_to_chain(&msg, &key, &pool).unwrap();
        let mut plain_chain = decrypt_chain_to_chain(&mut chain, &key, &pool).unwrap();
        let bytes: Vec<u8> = plain_chain
            .iter()
            .flat_map(|m| m.readable().to_vec())
            .collect();
        prop_assert_eq!(bytes.as_slice(), msg.as_slice());
        plain_chain.recycle(&pool);
    }

    #[test]
    fn base64_round_trip_property(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        let encoded = base64_encode(&bytes);
        let decoded = base64_decode(&encoded).unwrap();
        prop_assert_eq!(decoded, bytes);
    }
}

#[test]
fn rsa_round_trip_with_bundled_pem() {
    let pem_bytes = read_fixture("dynomite.pem");
    let rsa = load_rsa_private_key_from_bytes(&pem_bytes).unwrap();
    let crypto = Crypto::from_parts(rsa, [0u8; AES_KEYLEN]);
    assert_eq!(crypto.rsa_size(), 128, "bundled key is 1024 bits");

    let key = Crypto::generate_aes_key().unwrap();
    let wrapped = crypto.rsa_encrypt(&key).unwrap();
    assert_eq!(wrapped.len(), crypto.rsa_size());
    let unwrapped = crypto.rsa_decrypt(&wrapped).unwrap();
    assert_eq!(unwrapped, key);
}

#[test]
fn crypto_from_pem_smoke_test() {
    let path = fixture_dir().join("dynomite.pem");
    let crypto = Crypto::from_pem(&path).expect("load bundled PEM");
    assert_eq!(crypto.aes_key().len(), AES_KEYLEN);
    assert_eq!(crypto.rsa_size(), 128);
}

#[test]
fn dyn_aes_encrypt_msg_round_trip() {
    let pool = MbufPool::default();
    let key = Crypto::generate_aes_key().unwrap();
    let mut buf = pool.get();
    let payload = b"the quick brown fox jumps over the lazy dog";
    buf.recv(payload);

    let (mut chain, n) = Crypto::dyn_aes_encrypt_msg(&buf, &key, &pool).unwrap();
    assert!(n >= AES_BLOCK_SIZE);
    assert_eq!(n, chain.total_len());

    let plain = Crypto::dyn_aes_decrypt_to_vec(&mut chain, &key).unwrap();
    assert_eq!(plain.as_slice(), payload);
}

#[test]
fn dyn_aes_chain_decrypts_back_to_chain() {
    let pool = MbufPool::default();
    let key = Crypto::generate_aes_key().unwrap();
    let mut chain = encrypt_to_chain(b"chain test payload", &key, &pool).unwrap();
    let mut plain = Crypto::dyn_aes_decrypt(&mut chain, &key, &pool).unwrap();
    assert!(chain.is_empty());
    let bytes: Vec<u8> = plain.iter().flat_map(|m| m.readable().to_vec()).collect();
    assert_eq!(bytes, b"chain test payload");
    plain.recycle(&pool);
}

/// Frozen-output wire-format pin.
///
/// The fixture under `tests/fixtures/crypto/` is generated by
/// `examples/gen_crypto_fixture.rs` and verified out-of-band against
/// the `openssl enc -aes-128-cbc -K HEX -iv HEX` command (see the
/// example header for the exact invocation). The test asserts that
/// the committed ciphertext still decrypts to the committed
/// plaintext under the committed key, and that the deterministic
/// encryption produces the same bytes byte-for-byte. This pins the
/// Rust wire format against future drift; because the algorithm is
/// AES-128-CBC with the key reused as the IV, two compliant
/// implementations (Rust, the C reference, OpenSSL CLI) all produce
/// the same bytes for the same inputs.
#[test]
fn wire_format_pin_round_trip() {
    let key_bytes = read_fixture("aes_key.bin");
    assert_eq!(key_bytes.len(), AES_KEYLEN);
    let mut key = [0u8; AES_KEYLEN];
    key.copy_from_slice(&key_bytes);

    let plaintext = read_fixture("plaintext.bin");
    let cipher = read_fixture("cipher.bin");

    let recovered = decrypt_to_vec(&cipher, &key).unwrap();
    assert_eq!(recovered, plaintext, "frozen ciphertext must decrypt");

    // The cipher is deterministic for fixed inputs (key reused as
    // IV), so re-encrypting must reproduce the committed bytes
    // byte-for-byte.
    let fresh_cipher = encrypt_to_vec(&plaintext, &key).unwrap();
    assert_eq!(fresh_cipher, cipher);
}

#[test]
fn invalid_pem_is_rejected() {
    let bad = b"-----BEGIN COWBELL-----\nMORE\n-----END COWBELL-----\n";
    let err = load_rsa_private_key_from_bytes(bad).unwrap_err();
    assert!(format!("{err}").contains("PEM"));
}
