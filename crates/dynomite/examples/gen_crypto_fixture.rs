//! Helper that generates the frozen crypto fixture under
//! `crates/dynomite/tests/fixtures/crypto/`. Run with:
//!
//! ```text
//! cargo run --example gen_crypto_fixture -p dynomite
//! ```
//!
//! The fixture pins the AES wire format we implemented in Stage 6.

use std::fs;
use std::path::PathBuf;

use dynomite::crypto::Crypto;

fn main() {
    let aes_key = [
        0x10u8, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd,
        0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0xef, 0xcd, 0xab, 0x89, 0x67, 0x45,
        0x23, 0x01,
    ];
    let plaintext = b"dynomite stage 6 crypto fixture: round-trip me";

    let cipher = Crypto::aes_encrypt(plaintext, &aes_key).unwrap();
    let recovered = Crypto::aes_decrypt(&cipher, &aes_key).unwrap();
    assert_eq!(recovered, plaintext);

    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("crypto");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("aes_key.bin"), aes_key).unwrap();
    fs::write(dir.join("plaintext.bin"), plaintext).unwrap();
    fs::write(dir.join("cipher.bin"), &cipher).unwrap();

    println!("wrote {} bytes of cipher", cipher.len());
}
