//! Helper that regenerates the frozen crypto fixture under
//! `crates/dynomite/tests/fixtures/crypto/`. Run with:
//!
//! ```text
//! cargo run --example gen_crypto_fixture -p dynomited
//! ```
//!
//! The fixture pins the AES wire format implemented in Stage 6.
//! Because the cipher is AES-128-CBC with the key reused as the IV,
//! the ciphertext is deterministic and any compliant implementation
//! produces the same bytes for the same inputs. The C-reference
//! mapping is recorded in `docs/parity.md` only.
//!
//! The committed `cipher.bin` was independently verified against
//! the OpenSSL CLI:
//!
//! ```text
//! printf 'dynomite stage 6 crypto fixture: round-trip me' | \
//!   openssl enc -aes-128-cbc \
//!     -K  1032547698badcfe0123456789abcdef \
//!     -iv 1032547698badcfe0123456789abcdef
//! ```
//!
//! produces the same 48 bytes that `Crypto::aes_encrypt` produces
//! for the bundled key + plaintext.

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
        .join("..")
        .join("dynomite")
        .join("tests")
        .join("fixtures")
        .join("crypto");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("aes_key.bin"), aes_key).unwrap();
    fs::write(dir.join("plaintext.bin"), plaintext).unwrap();
    fs::write(dir.join("cipher.bin"), &cipher).unwrap();

    println!("wrote {} bytes of cipher", cipher.len());
}
