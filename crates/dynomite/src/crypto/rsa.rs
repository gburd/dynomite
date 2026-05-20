//! RSA wrap and unwrap for AES session keys.
//!
//! Uses PKCS#1 OAEP padding (with the SHA-1 hash and MGF1) for both
//! directions. The handshake wraps a 32-byte AES key with the
//! recipient's public key; the recipient unwraps with its private
//! key and uses the result as the session key for AES-CBC traffic.

use rand::rngs::OsRng;
use rsa::traits::PublicKeyParts;
use rsa::{Oaep, RsaPrivateKey};

use crate::crypto::CryptoError;

/// Maximum plaintext length for OAEP-SHA1 padding given an RSA modulus
/// of `n` bytes is `n - 2 * hash_len - 2 = n - 42`.
const OAEP_SHA1_OVERHEAD: usize = 42;

/// Encrypt `msg` with the public half of `rsa` using PKCS#1 OAEP
/// padding. The output length equals the RSA modulus size in bytes.
///
/// `msg.len()` must be at most `rsa.size() - 42` (the OAEP-SHA1
/// constraint).
///
/// # Examples
///
/// ```
/// use dynomite::crypto::rsa::{decrypt, encrypt};
/// use rsa::RsaPrivateKey;
/// use rand::rngs::OsRng;
///
/// let mut rng = OsRng;
/// let rsa = RsaPrivateKey::new(&mut rng, 2048).unwrap();
/// let cipher = encrypt(&rsa, b"hello").unwrap();
/// let plain = decrypt(&rsa, &cipher).unwrap();
/// assert_eq!(plain, b"hello");
/// ```
pub fn encrypt(rsa: &RsaPrivateKey, msg: &[u8]) -> Result<Vec<u8>, CryptoError> {
    let n = rsa.size();
    if msg.len() + OAEP_SHA1_OVERHEAD > n {
        return Err(CryptoError::EncryptionFailed);
    }
    let mut rng = OsRng;
    let public = rsa.to_public_key();
    let padding = Oaep::new::<sha1::Sha1>();
    public
        .encrypt(&mut rng, padding, msg)
        .map_err(|_| CryptoError::EncryptionFailed)
}

/// Decrypt `enc` with the private half of `rsa` using PKCS#1 OAEP
/// padding.
///
/// `enc.len()` must equal the RSA modulus size in bytes.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::rsa::{decrypt, encrypt};
/// use rsa::RsaPrivateKey;
/// use rand::rngs::OsRng;
///
/// let mut rng = OsRng;
/// let rsa = RsaPrivateKey::new(&mut rng, 2048).unwrap();
/// let cipher = encrypt(&rsa, b"hello").unwrap();
/// let plain = decrypt(&rsa, &cipher).unwrap();
/// assert_eq!(plain, b"hello");
/// ```
pub fn decrypt(rsa: &RsaPrivateKey, enc: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if enc.len() != rsa.size() {
        return Err(CryptoError::DecryptionFailed);
    }
    let padding = Oaep::new::<sha1::Sha1>();
    rsa.decrypt(padding, enc)
        .map_err(|_| CryptoError::DecryptionFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::aes::AES_KEYLEN;

    fn fresh_key() -> RsaPrivateKey {
        let mut rng = OsRng;
        RsaPrivateKey::new(&mut rng, 2048).unwrap()
    }

    #[test]
    fn round_trip_short() {
        let rsa = fresh_key();
        let cipher = encrypt(&rsa, b"abc").unwrap();
        assert_eq!(cipher.len(), rsa.size());
        let plain = decrypt(&rsa, &cipher).unwrap();
        assert_eq!(plain, b"abc");
    }

    #[test]
    fn round_trip_aes_keylen() {
        let rsa = fresh_key();
        let key = [0xa5u8; AES_KEYLEN];
        let cipher = encrypt(&rsa, &key).unwrap();
        let plain = decrypt(&rsa, &cipher).unwrap();
        assert_eq!(plain, key);
    }

    #[test]
    fn oversize_input_rejected() {
        let rsa = fresh_key();
        let big = vec![0u8; rsa.size()];
        assert!(encrypt(&rsa, &big).is_err());
    }

    #[test]
    fn wrong_size_decrypt_rejected() {
        let rsa = fresh_key();
        assert!(decrypt(&rsa, b"too short").is_err());
    }
}
