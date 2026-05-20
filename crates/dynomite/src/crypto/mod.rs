//! Cryptographic primitives used by the DNODE peer protocol.
//!
//! The engine encrypts inter-node payloads with a per-pool symmetric
//! AES key. The key itself is wrapped with the recipient's RSA public
//! key and exchanged during the DNODE handshake. This module exposes:
//!
//! * [`Crypto`] - bundle of an RSA key pair (loaded from PEM) and a
//!   freshly generated AES-256 key. Construct it with
//!   [`Crypto::from_pem`] at process startup.
//! * AES-256-CBC encryption and decryption, including helpers that
//!   pipe through the [`MbufQueue`](crate::io::mbuf::MbufQueue) chain
//!   the rest of the engine uses.
//! * RSA wrap and unwrap of the symmetric key, using PKCS#1 v1.5
//!   padding.
//! * Base64 encoding and decoding wrappers around the workspace
//!   `base64` crate.
//! * PEM key loading for both PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`)
//!   and PKCS#8 (`-----BEGIN PRIVATE KEY-----`) framings.
//!
//! # Examples
//!
//! ```
//! use dynomite::crypto::Crypto;
//!
//! let key = Crypto::generate_aes_key().unwrap();
//! let plain = b"hello dnode";
//! let cipher = Crypto::aes_encrypt(plain, &key).unwrap();
//! assert_ne!(cipher.as_slice(), plain);
//! let round = Crypto::aes_decrypt(&cipher, &key).unwrap();
//! assert_eq!(round, plain);
//! ```

use std::io;
use std::path::Path;

use openssl::error::ErrorStack;
use openssl::pkey::Private;
use openssl::rsa::Rsa;
use thiserror::Error;

pub mod aes;
pub mod base64;
pub mod pem;
pub mod rsa;

pub use self::aes::{AES_BLOCK_SIZE, AES_IV_LEN, AES_KEYLEN};
pub use self::base64::{base64_decode, base64_encode};

/// Errors produced by the crypto module.
///
/// Variants enumerate the small fixed set of failure modes the engine
/// reports up to its callers. The opaque OpenSSL error stack is
/// preserved when relevant so operators can correlate failures with
/// the underlying library log.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// A symmetric or asymmetric key was malformed or had the wrong
    /// length.
    #[error("invalid key material")]
    InvalidKey,

    /// A PEM file did not contain a recognisable RSA private key.
    #[error("invalid PEM input: {0}")]
    InvalidPem(String),

    /// Symmetric or asymmetric encryption failed.
    #[error("encryption failed")]
    EncryptionFailed,

    /// Symmetric or asymmetric decryption failed.
    #[error("decryption failed")]
    DecryptionFailed,

    /// PKCS#7 padding on a decrypted block was malformed.
    #[error("bad PKCS#7 padding")]
    BadPadding,

    /// A base64 input was malformed.
    #[error("base64 decode failed: {0}")]
    Base64(String),

    /// Underlying I/O failure (file open, read, write).
    #[error(transparent)]
    Io(#[from] io::Error),

    /// Underlying OpenSSL failure with the original error stack.
    #[error("openssl: {0}")]
    OpenSsl(#[from] ErrorStack),
}

/// Bundle of crypto state used by a Dynomite peer instance.
///
/// Holds an RSA key pair loaded from a PEM file and a fresh AES-256
/// session key generated when the bundle is constructed. The session
/// key is used for symmetric encryption of DNODE payloads, while the
/// RSA pair is used to wrap/unwrap session keys during the handshake.
///
/// # Examples
///
/// ```no_run
/// use dynomite::crypto::Crypto;
///
/// let crypto = Crypto::from_pem("conf/dynomite.pem").unwrap();
/// let payload = b"sample";
/// let cipher = Crypto::aes_encrypt(payload, crypto.aes_key()).unwrap();
/// let plain = Crypto::aes_decrypt(&cipher, crypto.aes_key()).unwrap();
/// assert_eq!(plain, payload);
/// ```
pub struct Crypto {
    aes_key: [u8; AES_KEYLEN],
    rsa: Rsa<Private>,
}

impl Crypto {
    /// Construct a new bundle by loading an RSA private key from the
    /// given PEM file and generating a fresh AES-256 key from the
    /// system CSPRNG.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::crypto::Crypto;
    /// let crypto = Crypto::from_pem("conf/dynomite.pem").unwrap();
    /// assert_eq!(crypto.aes_key().len(), 32);
    /// ```
    pub fn from_pem<P: AsRef<Path>>(path: P) -> Result<Self, CryptoError> {
        let rsa = pem::load_rsa_private_key(path.as_ref())?;
        let aes_key = Self::generate_aes_key()?;
        Ok(Self { aes_key, rsa })
    }

    /// Construct a bundle from an already-loaded RSA private key and
    /// a caller-supplied AES key. Used by tests and embedders that
    /// want to exercise the bundle without touching the filesystem.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    ///
    /// let aes_key = Crypto::generate_aes_key().unwrap();
    /// let rsa = openssl::rsa::Rsa::generate(2048).unwrap();
    /// let crypto = Crypto::from_parts(rsa, aes_key);
    /// assert_eq!(crypto.aes_key().len(), 32);
    /// ```
    pub fn from_parts(rsa: Rsa<Private>, aes_key: [u8; AES_KEYLEN]) -> Self {
        Self { aes_key, rsa }
    }

    /// Generate a fresh AES-256 key from the system CSPRNG.
    ///
    /// The returned slice is 32 random bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    ///
    /// let a = Crypto::generate_aes_key().unwrap();
    /// let b = Crypto::generate_aes_key().unwrap();
    /// assert_ne!(a, b);
    /// ```
    pub fn generate_aes_key() -> Result<[u8; AES_KEYLEN], CryptoError> {
        let mut key = [0u8; AES_KEYLEN];
        openssl::rand::rand_bytes(&mut key)?;
        Ok(key)
    }

    /// Borrow the bundle's AES session key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::crypto::Crypto;
    /// let crypto = Crypto::from_pem("conf/dynomite.pem").unwrap();
    /// assert_eq!(crypto.aes_key().len(), 32);
    /// ```
    pub fn aes_key(&self) -> &[u8; AES_KEYLEN] {
        &self.aes_key
    }

    /// Borrow the bundle's RSA private key.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::crypto::Crypto;
    /// let crypto = Crypto::from_pem("conf/dynomite.pem").unwrap();
    /// assert!(crypto.rsa_size() > 0);
    /// ```
    pub fn rsa_private_key(&self) -> &Rsa<Private> {
        &self.rsa
    }

    /// Modulus size of the loaded RSA key in bytes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::crypto::Crypto;
    /// let crypto = Crypto::from_pem("conf/dynomite.pem").unwrap();
    /// assert!(crypto.rsa_size() >= 128);
    /// ```
    pub fn rsa_size(&self) -> usize {
        self.rsa.size() as usize
    }

    /// AES-256-CBC encrypt `msg` with `aes_key`. The output is a fresh
    /// 16-byte IV followed by the PKCS#7-padded ciphertext.
    ///
    /// # Security
    ///
    /// * AES-256 in CBC mode with PKCS#7 padding.
    /// * A fresh 16-byte IV is generated from the system CSPRNG for
    ///   every call and prepended to the ciphertext. The output is not
    ///   authenticated; integrity is provided by the surrounding DNODE
    ///   message framing. Embedders that need authenticated payloads
    ///   should layer an AEAD on top.
    /// * The IV is prepended (not derived from the key) so two
    ///   encryptions of the same plaintext produce different
    ///   ciphertexts.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let cipher = Crypto::aes_encrypt(b"hi", &key).unwrap();
    /// let plain = Crypto::aes_decrypt(&cipher, &key).unwrap();
    /// assert_eq!(plain, b"hi");
    /// ```
    pub fn aes_encrypt(msg: &[u8], aes_key: &[u8; AES_KEYLEN]) -> Result<Vec<u8>, CryptoError> {
        aes::encrypt_to_vec(msg, aes_key)
    }

    /// AES-256-CBC decrypt the output of [`Crypto::aes_encrypt`].
    ///
    /// `enc` must begin with a 16-byte IV followed by an integral
    /// number of 16-byte ciphertext blocks.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let cipher = Crypto::aes_encrypt(b"hello", &key).unwrap();
    /// let plain = Crypto::aes_decrypt(&cipher, &key).unwrap();
    /// assert_eq!(plain, b"hello");
    /// ```
    pub fn aes_decrypt(enc: &[u8], aes_key: &[u8; AES_KEYLEN]) -> Result<Vec<u8>, CryptoError> {
        aes::decrypt_to_vec(enc, aes_key)
    }

    /// AES-256-CBC encrypt `msg`, writing the result into a fresh
    /// `MbufQueue` drawn from `pool`. The first chunk of the output
    /// chain begins with the 16-byte IV, followed by the ciphertext.
    /// Output spans as many chunks as needed; each chunk is filled up
    /// to the writable region before allocating the next one.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    /// use dynomite::io::mbuf::MbufPool;
    ///
    /// let pool = MbufPool::default();
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let mut chain = Crypto::dyn_aes_encrypt(b"hello", &key, &pool).unwrap();
    /// let plain = Crypto::dyn_aes_decrypt_to_vec(&mut chain, &key).unwrap();
    /// assert_eq!(plain, b"hello");
    /// ```
    pub fn dyn_aes_encrypt(
        msg: &[u8],
        aes_key: &[u8; AES_KEYLEN],
        pool: &crate::io::mbuf::MbufPool,
    ) -> Result<crate::io::mbuf::MbufQueue, CryptoError> {
        aes::encrypt_to_chain(msg, aes_key, pool)
    }

    /// AES-256-CBC decrypt a ciphertext chain produced by
    /// [`Crypto::dyn_aes_encrypt`], appending the recovered plaintext
    /// to a fresh `MbufQueue` drawn from `pool`.
    ///
    /// `enc` is consumed: chunks are popped off the front and pushed
    /// to the pool free list as they are drained.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    /// use dynomite::io::mbuf::MbufPool;
    ///
    /// let pool = MbufPool::default();
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let mut chain = Crypto::dyn_aes_encrypt(b"abc", &key, &pool).unwrap();
    /// let mut plain_chain = Crypto::dyn_aes_decrypt(&mut chain, &key, &pool).unwrap();
    /// assert_eq!(plain_chain.total_len(), 3);
    /// ```
    pub fn dyn_aes_decrypt(
        enc: &mut crate::io::mbuf::MbufQueue,
        aes_key: &[u8; AES_KEYLEN],
        pool: &crate::io::mbuf::MbufPool,
    ) -> Result<crate::io::mbuf::MbufQueue, CryptoError> {
        aes::decrypt_chain_to_chain(enc, aes_key, pool)
    }

    /// Convenience wrapper that decrypts a ciphertext chain into a
    /// flat `Vec<u8>`. Useful for tests and protocol code that needs
    /// the cleartext as a single buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    /// use dynomite::io::mbuf::MbufPool;
    ///
    /// let pool = MbufPool::default();
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let mut chain = Crypto::dyn_aes_encrypt(b"hello", &key, &pool).unwrap();
    /// let plain = Crypto::dyn_aes_decrypt_to_vec(&mut chain, &key).unwrap();
    /// assert_eq!(plain, b"hello");
    /// ```
    pub fn dyn_aes_decrypt_to_vec(
        enc: &mut crate::io::mbuf::MbufQueue,
        aes_key: &[u8; AES_KEYLEN],
    ) -> Result<Vec<u8>, CryptoError> {
        let mut bytes = Vec::with_capacity(enc.total_len());
        while let Some(buf) = enc.pop_front() {
            bytes.extend_from_slice(buf.readable());
        }
        Self::aes_decrypt(&bytes, aes_key)
    }

    /// AES-256-CBC encrypt the readable region of `msg`, returning a
    /// new chain holding the ciphertext along with the total number
    /// of ciphertext bytes written.
    ///
    /// The handshake encodes its own framing on top of the returned
    /// chain, so the output count is reported separately rather than
    /// derived from the queue.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::crypto::Crypto;
    /// use dynomite::io::mbuf::{Mbuf, MbufPool};
    ///
    /// let pool = MbufPool::default();
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let mut buf = pool.get();
    /// buf.recv(b"payload");
    /// let (mut chain, n) = Crypto::dyn_aes_encrypt_msg(&buf, &key, &pool).unwrap();
    /// assert!(n > 0);
    /// let plain = Crypto::dyn_aes_decrypt_to_vec(&mut chain, &key).unwrap();
    /// assert_eq!(plain, b"payload");
    /// ```
    pub fn dyn_aes_encrypt_msg(
        msg: &crate::io::mbuf::Mbuf,
        aes_key: &[u8; AES_KEYLEN],
        pool: &crate::io::mbuf::MbufPool,
    ) -> Result<(crate::io::mbuf::MbufQueue, usize), CryptoError> {
        let chain = aes::encrypt_to_chain(msg.readable(), aes_key, pool)?;
        let n = chain.total_len();
        Ok((chain, n))
    }

    /// RSA encrypt `msg` with the bundle's public key using PKCS#1
    /// v1.5 padding. The output length is the RSA modulus size in
    /// bytes (typically 128 for 1024-bit keys, 256 for 2048-bit).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::crypto::Crypto;
    /// let crypto = Crypto::from_pem("conf/dynomite.pem").unwrap();
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let wrapped = crypto.rsa_encrypt(&key).unwrap();
    /// let unwrapped = crypto.rsa_decrypt(&wrapped).unwrap();
    /// assert_eq!(unwrapped, key);
    /// ```
    pub fn rsa_encrypt(&self, msg: &[u8]) -> Result<Vec<u8>, CryptoError> {
        rsa::encrypt(&self.rsa, msg)
    }

    /// RSA decrypt `enc` with the bundle's private key using PKCS#1
    /// v1.5 padding.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::crypto::Crypto;
    /// let crypto = Crypto::from_pem("conf/dynomite.pem").unwrap();
    /// let key = Crypto::generate_aes_key().unwrap();
    /// let wrapped = crypto.rsa_encrypt(&key).unwrap();
    /// let unwrapped = crypto.rsa_decrypt(&wrapped).unwrap();
    /// assert_eq!(unwrapped, key);
    /// ```
    pub fn rsa_decrypt(&self, enc: &[u8]) -> Result<Vec<u8>, CryptoError> {
        rsa::decrypt(&self.rsa, enc)
    }
}

impl std::fmt::Debug for Crypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crypto")
            .field("aes_key_len", &self.aes_key.len())
            .field("rsa_size", &self.rsa_size())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_aes_key_returns_distinct_keys() {
        let a = Crypto::generate_aes_key().unwrap();
        let b = Crypto::generate_aes_key().unwrap();
        assert_eq!(a.len(), AES_KEYLEN);
        assert_ne!(a, b);
    }

    #[test]
    fn aes_round_trip_short() {
        let key = Crypto::generate_aes_key().unwrap();
        for plain in &[&b""[..], b"a", b"abcdefghij", b"this is a test"] {
            let cipher = Crypto::aes_encrypt(plain, &key).unwrap();
            assert!(cipher.len() >= AES_IV_LEN + AES_BLOCK_SIZE);
            let round = Crypto::aes_decrypt(&cipher, &key).unwrap();
            assert_eq!(round.as_slice(), *plain);
        }
    }

    #[test]
    fn debug_does_not_leak_key() {
        let aes = [0u8; AES_KEYLEN];
        let rsa = openssl::rsa::Rsa::generate(2048).unwrap();
        let c = Crypto::from_parts(rsa, aes);
        let s = format!("{c:?}");
        assert!(s.contains("Crypto"));
        assert!(!s.contains("0, 0, 0, 0"));
    }
}
