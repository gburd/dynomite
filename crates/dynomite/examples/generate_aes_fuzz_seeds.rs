//! Generator for the `crypto_aes_decrypt` fuzz seed corpus.
//!
//! Encrypts a fixed list of representative plaintexts with the same
//! key the fuzz harness uses and writes the ciphertexts to
//! `crates/fuzz/seeds/crypto_aes_decrypt/`. Re-run after rotating
//! the harness key.

use dynomite::crypto::aes::{encrypt_to_vec, AES_KEYLEN};
use std::fs;
use std::path::PathBuf;

fn main() {
    let key: [u8; AES_KEYLEN] = *b"dynomite-fuzz-key-buffer-32byte!";
    let plaintexts: Vec<&[u8]> = vec![
        b"",
        b"a",
        b"hello",
        b"sixteen-byte-msg",
        b"this is a longer plaintext that spans many blocks of aes",
        b"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f",
        b"DNODE peer payload",
        b"$2014$ 1 3 0 1 1 *1 d *0\r\n",
        b"the quick brown fox jumps over the lazy dog",
    ];
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("fuzz")
        .join("seeds")
        .join("crypto_aes_decrypt");
    fs::create_dir_all(&dir).unwrap();
    for (i, p) in plaintexts.iter().enumerate() {
        let cipher = encrypt_to_vec(p, &key).unwrap();
        let path = dir.join(format!("{:02}_seed.bin", i + 1));
        fs::write(&path, &cipher).unwrap();
        eprintln!("wrote {} ({} bytes)", path.display(), cipher.len());
    }
}
