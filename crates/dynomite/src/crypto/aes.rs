//! AES-256-CBC primitives used by the DNODE peer protocol.
//!
//! The wire format produced by [`encrypt_to_vec`] (and consumed by
//! [`decrypt_to_vec`]) is the concatenation of:
//!
//! ```text
//! [16-byte random IV] [PKCS#7-padded ciphertext (multiple of 16)]
//! ```
//!
//! The IV is generated from the system CSPRNG on every encryption.
//! PKCS#7 padding is mandatory: an empty plaintext encrypts to a
//! single 16-byte block; a 16-byte plaintext encrypts to 32 bytes.
//!
//! `encrypt_to_chain` and `decrypt_chain_to_chain` provide the same
//! primitives wrapped in [`MbufQueue`] flow used by the rest of the
//! engine.

use openssl::symm::{Cipher, Crypter, Mode};

use crate::crypto::CryptoError;
use crate::io::mbuf::{Mbuf, MbufPool, MbufQueue};

/// AES-256 key length in bytes.
pub const AES_KEYLEN: usize = 32;

/// AES block size in bytes.
pub const AES_BLOCK_SIZE: usize = 16;

/// AES-CBC IV length in bytes.
pub const AES_IV_LEN: usize = 16;

fn cipher() -> Cipher {
    Cipher::aes_256_cbc()
}

fn random_iv() -> Result<[u8; AES_IV_LEN], CryptoError> {
    let mut iv = [0u8; AES_IV_LEN];
    openssl::rand::rand_bytes(&mut iv)?;
    Ok(iv)
}

/// Encrypt `msg` with AES-256-CBC and PKCS#7 padding using
/// `aes_key`. The output is a fresh 16-byte IV concatenated with the
/// ciphertext.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::Crypto;
/// use dynomite::crypto::aes::encrypt_to_vec;
///
/// let key = Crypto::generate_aes_key().unwrap();
/// let cipher = encrypt_to_vec(b"alpha", &key).unwrap();
/// assert!(cipher.len() >= 32);
/// ```
pub fn encrypt_to_vec(msg: &[u8], aes_key: &[u8; AES_KEYLEN]) -> Result<Vec<u8>, CryptoError> {
    let iv = random_iv()?;
    let cipher = cipher();
    let mut crypter = Crypter::new(cipher, Mode::Encrypt, aes_key, Some(&iv))?;
    crypter.pad(true);

    let mut out = Vec::with_capacity(AES_IV_LEN + msg.len() + AES_BLOCK_SIZE);
    out.extend_from_slice(&iv);

    let prev_len = out.len();
    out.resize(prev_len + msg.len() + AES_BLOCK_SIZE, 0);
    let written = crypter
        .update(msg, &mut out[prev_len..])
        .map_err(|_| CryptoError::EncryptionFailed)?;
    let final_written = crypter
        .finalize(&mut out[prev_len + written..])
        .map_err(|_| CryptoError::EncryptionFailed)?;
    out.truncate(prev_len + written + final_written);
    Ok(out)
}

/// Decrypt the output of [`encrypt_to_vec`].
///
/// `enc` must begin with a 16-byte IV followed by an integral number
/// of 16-byte ciphertext blocks. PKCS#7 padding is removed
/// automatically.
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
    if enc.len() < AES_IV_LEN + AES_BLOCK_SIZE {
        return Err(CryptoError::DecryptionFailed);
    }
    if (enc.len() - AES_IV_LEN) % AES_BLOCK_SIZE != 0 {
        return Err(CryptoError::DecryptionFailed);
    }
    let (iv, body) = enc.split_at(AES_IV_LEN);
    let cipher = cipher();
    let mut crypter = Crypter::new(cipher, Mode::Decrypt, aes_key, Some(iv))?;
    crypter.pad(true);

    let mut out = vec![0u8; body.len() + AES_BLOCK_SIZE];
    let written = crypter
        .update(body, &mut out)
        .map_err(|_| CryptoError::DecryptionFailed)?;
    let final_written = crypter
        .finalize(&mut out[written..])
        .map_err(|_| CryptoError::BadPadding)?;
    out.truncate(written + final_written);
    Ok(out)
}

/// Encrypt `msg` and write the IV + ciphertext into a fresh chain of
/// pool-backed [`Mbuf`] chunks.
///
/// The first chunk holds the 16-byte IV; subsequent chunks hold
/// ciphertext blocks. The chain is filled chunk-by-chunk; a new chunk
/// is allocated only when the previous one runs out of writable
/// space. The chain is suitable for direct submission to the reactor.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::Crypto;
/// use dynomite::crypto::aes::encrypt_to_chain;
/// use dynomite::io::mbuf::MbufPool;
///
/// let pool = MbufPool::default();
/// let key = Crypto::generate_aes_key().unwrap();
/// let chain = encrypt_to_chain(b"hello", &key, &pool).unwrap();
/// assert!(chain.total_len() >= 16);
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
/// then decrypted as a single ciphertext. The first 16 bytes are
/// interpreted as the IV.
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
/// let mut plain = decrypt_chain_to_chain(&mut chain, &key, &pool).unwrap();
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
        assert_eq!(cipher.len(), AES_IV_LEN + AES_BLOCK_SIZE);
        let plain = decrypt_to_vec(&cipher, &key).unwrap();
        assert!(plain.is_empty());
    }

    #[test]
    fn block_aligned_plaintext_pads_full_block() {
        let key = Crypto::generate_aes_key().unwrap();
        let msg = vec![0xab; AES_BLOCK_SIZE];
        let cipher = encrypt_to_vec(&msg, &key).unwrap();
        assert_eq!(cipher.len(), AES_IV_LEN + 2 * AES_BLOCK_SIZE);
        let plain = decrypt_to_vec(&cipher, &key).unwrap();
        assert_eq!(plain, msg);
    }

    #[test]
    fn iv_is_random_per_call() {
        let key = Crypto::generate_aes_key().unwrap();
        let a = encrypt_to_vec(b"same", &key).unwrap();
        let b = encrypt_to_vec(b"same", &key).unwrap();
        assert_ne!(a, b, "two encryptions should produce distinct IVs");
        assert_ne!(&a[..AES_IV_LEN], &b[..AES_IV_LEN]);
    }

    #[test]
    fn truncated_ciphertext_is_rejected() {
        let key = Crypto::generate_aes_key().unwrap();
        let cipher = encrypt_to_vec(b"abc", &key).unwrap();
        let truncated = &cipher[..cipher.len() - 1];
        assert!(decrypt_to_vec(truncated, &key).is_err());
    }

    #[test]
    fn wrong_key_fails_padding_check() {
        let key_a = Crypto::generate_aes_key().unwrap();
        let key_b = Crypto::generate_aes_key().unwrap();
        let cipher = encrypt_to_vec(b"abc", &key_a).unwrap();
        assert!(decrypt_to_vec(&cipher, &key_b).is_err());
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
