//! RSA wrap and unwrap for AES session keys.
//!
//! Uses PKCS#1 v1.5 padding for both directions. The handshake wraps
//! a 32-byte AES key with the recipient's public key; the recipient
//! unwraps with its private key and uses the result as the session
//! key for AES-CBC traffic.
//!
//! PKCS#1 v1.5 is not constant-time-safe against Bleichenbacher
//! attacks. Embedders that need stronger guarantees should layer
//! their own AEAD on top of the resulting AES session, or replace
//! [`encrypt`] / [`decrypt`] with OAEP equivalents.

use openssl::pkey::Private;
use openssl::rsa::{Padding, Rsa};

use crate::crypto::CryptoError;

/// Encrypt `msg` with the public half of `rsa` using PKCS#1 v1.5
/// padding. The output length equals the RSA modulus size in bytes.
///
/// `msg.len()` must be at most `rsa.size() - 11` (the PKCS#1 v1.5
/// constraint).
///
/// # Examples
///
/// ```
/// use dynomite::crypto::rsa::{decrypt, encrypt};
/// use openssl::rsa::Rsa;
///
/// let rsa = Rsa::generate(2048).unwrap();
/// let cipher = encrypt(&rsa, b"hello").unwrap();
/// let plain = decrypt(&rsa, &cipher).unwrap();
/// assert_eq!(plain, b"hello");
/// ```
pub fn encrypt(rsa: &Rsa<Private>, msg: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let n = rsa.size() as usize;
    if msg.len() + 11 > n {
        return Err(CryptoError::EncryptionFailed);
    }
    let mut out = vec![0u8; n];
    let written = rsa
        .public_encrypt(msg, &mut out, Padding::PKCS1)
        .map_err(|_| CryptoError::EncryptionFailed)?;
    out.truncate(written);
    Ok(out)
}

/// Decrypt `enc` with the private half of `rsa` using PKCS#1 v1.5
/// padding.
///
/// `enc.len()` must equal the RSA modulus size in bytes.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::rsa::{decrypt, encrypt};
/// use openssl::rsa::Rsa;
///
/// let rsa = Rsa::generate(2048).unwrap();
/// let cipher = encrypt(&rsa, b"hello").unwrap();
/// let plain = decrypt(&rsa, &cipher).unwrap();
/// assert_eq!(plain, b"hello");
/// ```
pub fn decrypt(rsa: &Rsa<Private>, enc: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let n = rsa.size() as usize;
    if enc.len() != n {
        return Err(CryptoError::DecryptionFailed);
    }
    let mut out = vec![0u8; n];
    let written = rsa
        .private_decrypt(enc, &mut out, Padding::PKCS1)
        .map_err(|_| CryptoError::DecryptionFailed)?;
    out.truncate(written);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_key() -> Rsa<Private> {
        Rsa::generate(2048).unwrap()
    }

    #[test]
    fn round_trip_short() {
        let rsa = fresh_key();
        let cipher = encrypt(&rsa, b"abc").unwrap();
        assert_eq!(cipher.len(), rsa.size() as usize);
        let plain = decrypt(&rsa, &cipher).unwrap();
        assert_eq!(plain, b"abc");
    }

    #[test]
    fn round_trip_aes_keylen() {
        let rsa = fresh_key();
        let key = [0xa5u8; super::super::aes::AES_KEYLEN];
        let cipher = encrypt(&rsa, &key).unwrap();
        let plain = decrypt(&rsa, &cipher).unwrap();
        assert_eq!(plain, key);
    }

    #[test]
    fn oversize_input_rejected() {
        let rsa = fresh_key();
        let big = vec![0u8; rsa.size() as usize];
        assert!(encrypt(&rsa, &big).is_err());
    }

    #[test]
    fn wrong_size_decrypt_rejected() {
        let rsa = fresh_key();
        assert!(decrypt(&rsa, b"too short").is_err());
    }
}
