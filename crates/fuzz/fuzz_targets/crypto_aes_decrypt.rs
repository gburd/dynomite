#![no_main]
//! Fuzz harness for `dynomite::crypto::aes::decrypt_to_vec`.
//!
//! The harness uses a fixed AES key buffer (the C reference's
//! 32-byte `aes_key[AES_KEYLEN]`; the cipher itself is AES-128 and
//! consumes the first 16 bytes). Arbitrary bytes are handed to the
//! decrypt path; the function must return either `Ok(Vec<u8>)` or
//! `Err(CryptoError)`. Anything else (panic, UB) is a finding.
use libfuzzer_sys::fuzz_target;

use dynomite::crypto::aes::{decrypt_to_vec, AES_KEYLEN};

const KEY: [u8; AES_KEYLEN] = *b"dynomite-fuzz-key-buffer-32byte!";

fuzz_target!(|data: &[u8]| {
    let _ = decrypt_to_vec(data, &KEY);
});
