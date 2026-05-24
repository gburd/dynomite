//! AES-128-CBC primitives used by the DNODE peer protocol.
//!
//! The wire format produced by [`encrypt_to_vec`] (and consumed by
//! [`decrypt_to_vec`]) is:
//!
//! ```text
//! [PKCS#7-padded ciphertext (multiple of 16 bytes)]
//! ```
//!
//! There is no separate IV in the output. The cipher is AES-128-CBC
//! and the IV is the same 16 bytes that serve as the key. PKCS#7
//! padding is mandatory: an empty plaintext encrypts to a single
//! 16-byte block; a 16-byte plaintext encrypts to 32 bytes.
//!
//! The public surface still takes a 32-byte key (the C reference's
//! `aes_key[AES_KEYLEN]` buffer). AES-128-CBC consumes only the
//! first 16 bytes; the remaining 16 are unused by the cipher and are
//! kept solely for byte-for-byte compatibility with the C
//! `aes_key` buffer layout.
//!
//! `encrypt_to_chain` and `decrypt_chain_to_chain` provide the same
//! primitives wrapped in the [`MbufQueue`] flow used by the rest of
//! the engine.
//!
//! # Security
//!
//! The C reference reuses the AES key as the IV
//! (`EVP_EncryptInit_ex(ctx, cipher, NULL, key, key)`), which makes
//! the cipher deterministic for a given (key, plaintext) pair: two
//! encryptions of the same plaintext produce identical ciphertext.
//! This is a known weakness. The Rust port faithfully reproduces it
//! for wire compatibility with C peers; embedders should treat the
//! resulting channel as transport-layer encryption only, not as an
//! authenticated channel. A future hardening pass may layer an AEAD
//! on top.

use aes::Aes128;
use cbc::cipher::block_padding::Pkcs7;
use cbc::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};

use crate::crypto::CryptoError;
use crate::io::mbuf::{Mbuf, MbufPool, MbufQueue};

type Aes128CbcEnc = cbc::Encryptor<Aes128>;
type Aes128CbcDec = cbc::Decryptor<Aes128>;

/// AES key buffer length in bytes (the `AES_KEYLEN` constant from
/// `dyn_crypto.h`). The cipher itself is AES-128, which uses only
/// the first 16 bytes; the remaining 16 are unused.
pub const AES_KEYLEN: usize = 32;

/// AES block size in bytes.
pub const AES_BLOCK_SIZE: usize = 16;

/// AES-128 key length in bytes (the prefix of the [`AES_KEYLEN`]
/// buffer that the cipher actually consumes).
pub const AES_128_KEY_LEN: usize = 16;

fn key_iv(aes_key: &[u8; AES_KEYLEN]) -> &[u8; AES_128_KEY_LEN] {
    aes_key
        .first_chunk::<AES_128_KEY_LEN>()
        .expect("AES_KEYLEN >= AES_128_KEY_LEN by construction")
}

/// Encrypt `msg` with AES-128-CBC and PKCS#7 padding using the first
/// 16 bytes of `aes_key` as both the key and the IV. The output is
/// the raw ciphertext (no IV prefix) and is therefore deterministic
/// for a given (key, plaintext) pair.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::Crypto;
/// use dynomite::crypto::aes::{encrypt_to_vec, AES_BLOCK_SIZE};
///
/// let key = Crypto::generate_aes_key().unwrap();
/// let cipher = encrypt_to_vec(b"alpha", &key).unwrap();
/// assert_eq!(cipher.len() % AES_BLOCK_SIZE, 0);
/// assert!(cipher.len() >= AES_BLOCK_SIZE);
/// ```
pub fn encrypt_to_vec(msg: &[u8], aes_key: &[u8; AES_KEYLEN]) -> Result<Vec<u8>, CryptoError> {
    let kiv = key_iv(aes_key);
    let cipher = Aes128CbcEnc::new(kiv.into(), kiv.into());
    Ok(cipher.encrypt_padded_vec_mut::<Pkcs7>(msg))
}

/// Decrypt the output of [`encrypt_to_vec`].
///
/// `enc` must be a non-empty integral number of 16-byte ciphertext
/// blocks. PKCS#7 padding is removed automatically. The first 16
/// bytes of `aes_key` are used as both the key and the IV, matching
/// the encryption side.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::Crypto;
/// use dynomite::crypto::aes::{decrypt_to_vec, encrypt_to_vec};
///
/// let key = Crypto::generate_aes_key().unwrap();
/// let cipher = encrypt_to_vec(b"alpha", &key).unwrap();
/// let plain = decrypt_to_vec(&cipher, &key).unwrap();
/// assert_eq!(plain, b"alpha");
/// ```
pub fn decrypt_to_vec(enc: &[u8], aes_key: &[u8; AES_KEYLEN]) -> Result<Vec<u8>, CryptoError> {
    if enc.is_empty() || !enc.len().is_multiple_of(AES_BLOCK_SIZE) {
        return Err(CryptoError::DecryptionFailed);
    }
    let kiv = key_iv(aes_key);
    let cipher = Aes128CbcDec::new(kiv.into(), kiv.into());
    cipher
        .decrypt_padded_vec_mut::<Pkcs7>(enc)
        .map_err(|_| CryptoError::BadPadding)
}

/// Encrypt `msg` and write the ciphertext into a fresh chain of
/// pool-backed [`Mbuf`] chunks.
///
/// The chain is filled chunk-by-chunk; a new chunk is allocated only
/// when the previous one runs out of writable space. The output
/// chain holds the raw ciphertext only; there is no separate IV
/// prefix, matching the C `dyn_aes_encrypt` path.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::Crypto;
/// use dynomite::crypto::aes::{encrypt_to_chain, AES_BLOCK_SIZE};
/// use dynomite::io::mbuf::MbufPool;
///
/// let pool = MbufPool::default();
/// let key = Crypto::generate_aes_key().unwrap();
/// let chain = encrypt_to_chain(b"hello", &key, &pool).unwrap();
/// assert!(chain.total_len() >= AES_BLOCK_SIZE);
/// ```
pub fn encrypt_to_chain(
    msg: &[u8],
    aes_key: &[u8; AES_KEYLEN],
    pool: &MbufPool,
) -> Result<MbufQueue, CryptoError> {
    let cipher_bytes = encrypt_to_vec(msg, aes_key)?;
    let mut queue = MbufQueue::new();
    let mut remaining = cipher_bytes.as_slice();
    while !remaining.is_empty() {
        let mut buf: Mbuf = pool.get();
        let n = buf.recv(remaining);
        debug_assert!(n > 0, "fresh mbuf cannot accept any bytes");
        if n == 0 {
            return Err(CryptoError::EncryptionFailed);
        }
        remaining = &remaining[n..];
        queue.push_back(buf);
    }
    Ok(queue)
}

/// Decrypt the ciphertext stored in `enc` (consumed front-to-back)
/// and return a chain of plaintext chunks drawn from `pool`.
///
/// All readable bytes across all chunks of `enc` are concatenated and
/// then decrypted as a single ciphertext. The total length must be a
/// non-zero multiple of [`AES_BLOCK_SIZE`].
///
/// # Examples
///
/// ```
/// use dynomite::crypto::Crypto;
/// use dynomite::crypto::aes::{decrypt_chain_to_chain, encrypt_to_chain};
/// use dynomite::io::mbuf::MbufPool;
///
/// let pool = MbufPool::default();
/// let key = Crypto::generate_aes_key().unwrap();
/// let mut chain = encrypt_to_chain(b"hello", &key, &pool).unwrap();
/// let plain = decrypt_chain_to_chain(&mut chain, &key, &pool).unwrap();
/// assert_eq!(plain.total_len(), 5);
/// ```
pub fn decrypt_chain_to_chain(
    enc: &mut MbufQueue,
    aes_key: &[u8; AES_KEYLEN],
    pool: &MbufPool,
) -> Result<MbufQueue, CryptoError> {
    let mut bytes = Vec::with_capacity(enc.total_len());
    while let Some(buf) = enc.pop_front() {
        bytes.extend_from_slice(buf.readable());
    }
    let plain = decrypt_to_vec(&bytes, aes_key)?;
    let mut queue = MbufQueue::new();
    let mut remaining = plain.as_slice();
    while !remaining.is_empty() {
        let mut buf = pool.get();
        let n = buf.recv(remaining);
        debug_assert!(n > 0, "fresh mbuf cannot accept any bytes");
        if n == 0 {
            return Err(CryptoError::DecryptionFailed);
        }
        remaining = &remaining[n..];
        queue.push_back(buf);
    }
    Ok(queue)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::Crypto;

    #[test]
    fn empty_plaintext_round_trips() {
        let key = Crypto::generate_aes_key().unwrap();
        let cipher = encrypt_to_vec(b"", &key).unwrap();
        assert_eq!(cipher.len(), AES_BLOCK_SIZE);
        let plain = decrypt_to_vec(&cipher, &key).unwrap();
        assert!(plain.is_empty());
    }

    #[test]
    fn block_aligned_plaintext_pads_full_block() {
        let key = Crypto::generate_aes_key().unwrap();
        let msg = vec![0xab; AES_BLOCK_SIZE];
        let cipher = encrypt_to_vec(&msg, &key).unwrap();
        assert_eq!(cipher.len(), 2 * AES_BLOCK_SIZE);
        let plain = decrypt_to_vec(&cipher, &key).unwrap();
        assert_eq!(plain, msg);
    }

    #[test]
    fn encryption_is_deterministic() {
        let key = Crypto::generate_aes_key().unwrap();
        let a = encrypt_to_vec(b"same", &key).unwrap();
        let b = encrypt_to_vec(b"same", &key).unwrap();
        assert_eq!(a, b, "key-as-IV makes the cipher deterministic");
    }

    #[test]
    fn known_vector_pin() {
        // Fixed-key, fixed-plaintext byte pin. Reproduces the C
        // `dyn_aes_encrypt` path: AES-128-CBC, PKCS#7 padding, IV
        // equal to the first 16 bytes of the key, no IV prefix.
        let key: [u8; AES_KEYLEN] = [
            0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
            0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0xef, 0xcd, 0xab, 0x89,
            0x67, 0x45, 0x23, 0x01,
        ];
        let plaintext = b"";
        let cipher = encrypt_to_vec(plaintext, &key).unwrap();
        assert_eq!(cipher.len(), AES_BLOCK_SIZE);

        // Independently verifiable with:
        //   openssl enc -aes-128-cbc \
        //     -K  10325476 98badcfe 01234567 89abcdef \
        //     -iv 10325476 98badcfe 01234567 89abcdef \
        //     -in /dev/null
        let expected: [u8; AES_BLOCK_SIZE] = [
            0x98, 0xe1, 0x44, 0x32, 0xf6, 0x65, 0x78, 0xb9, 0x45, 0xd6, 0x4f, 0xc4, 0x60, 0x27,
            0x1b, 0xab,
        ];
        assert_eq!(cipher, expected);

        let round = decrypt_to_vec(&cipher, &key).unwrap();
        assert_eq!(round.as_slice(), plaintext);
    }

    #[test]
    fn truncated_ciphertext_is_rejected() {
        let key = Crypto::generate_aes_key().unwrap();
        let cipher = encrypt_to_vec(b"abc", &key).unwrap();
        let truncated = &cipher[..cipher.len() - 1];
        assert!(decrypt_to_vec(truncated, &key).is_err());
    }

    #[test]
    fn empty_ciphertext_is_rejected() {
        let key = Crypto::generate_aes_key().unwrap();
        assert!(decrypt_to_vec(&[], &key).is_err());
    }

    #[test]
    fn wrong_key_fails_padding_check() {
        // For a single 16-byte ciphertext block, decrypting with a
        // random wrong key produces 16 effectively random bytes.
        // PKCS#7 padding accepts roughly one in 256 of those
        // accidentally; the remaining ~99.6% surface
        // `CryptoError::Padding`. A single random-key pair would
        // therefore false-pass about 0.4% of the time and was the
        // root cause of the load-correlated flake recorded as F9 in
        // `docs/journal/2026-05-23-audit.md`. Iterate over
        // independently-generated key pairs until at least one
        // surfaces the padding-rejection path. The probability of
        // 32 consecutive random-key decryptions all yielding valid
        // padding is bounded above by `(1 / 255)^32`, well under
        // 1e-77, which is comfortably below any reasonable CI flake
        // threshold.
        const TRIALS: usize = 32;
        let mut observed_rejection = false;
        for _ in 0..TRIALS {
            let key_a = Crypto::generate_aes_key().unwrap();
            let key_b = Crypto::generate_aes_key().unwrap();
            let cipher = encrypt_to_vec(b"abc", &key_a).unwrap();
            if decrypt_to_vec(&cipher, &key_b).is_err() {
                observed_rejection = true;
                break;
            }
        }
        assert!(
            observed_rejection,
            "expected at least one wrong-key decryption in {TRIALS} trials to fail PKCS#7 padding"
        );
    }

    #[test]
    fn chain_round_trip() {
        let pool = MbufPool::default();
        let key = Crypto::generate_aes_key().unwrap();
        let mut chain = encrypt_to_chain(b"hello world", &key, &pool).unwrap();
        let mut plain = decrypt_chain_to_chain(&mut chain, &key, &pool).unwrap();
        assert!(chain.is_empty());
        let bytes: Vec<u8> = plain.iter().flat_map(|m| m.readable().to_vec()).collect();
        assert_eq!(bytes, b"hello world");
        plain.recycle(&pool);
    }
}
