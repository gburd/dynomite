//! PEM key file loading.
//!
//! Accepts both PKCS#1 framed RSA keys (`-----BEGIN RSA PRIVATE KEY-----`)
//! and PKCS#8 framed private keys (`-----BEGIN PRIVATE KEY-----`).
//! PKCS#8 support lets new deployments use the formats produced by
//! modern OpenSSL and `ssh-keygen`.

use std::fs;
use std::path::Path;

use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::RsaPrivateKey;

use crate::crypto::CryptoError;

/// Load an RSA private key from a PEM file at `path`.
///
/// The file content is sniffed: PKCS#1 framing is decoded directly,
/// PKCS#8 framing is decoded as a `PrivateKeyInfo` and then
/// downgraded to `RsaPrivateKey`. Anything else is rejected with
/// [`CryptoError::InvalidPem`].
///
/// # Examples
///
/// ```no_run
/// use dynomite::crypto::pem::load_rsa_private_key;
/// use rsa::traits::PublicKeyParts;
///
/// let key = load_rsa_private_key("conf/dynomite.pem".as_ref()).unwrap();
/// assert!(key.size() > 0);
/// ```
pub fn load_rsa_private_key(path: &Path) -> Result<RsaPrivateKey, CryptoError> {
    let bytes = fs::read(path)?;
    load_rsa_private_key_from_bytes(&bytes)
}

/// Parse an RSA private key from raw PEM bytes.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::pem::load_rsa_private_key_from_bytes;
/// use rsa::pkcs1::EncodeRsaPrivateKey;
/// use rsa::traits::PublicKeyParts;
/// use rsa::RsaPrivateKey;
/// use rand::rngs::OsRng;
///
/// let mut rng = OsRng;
/// let rsa = RsaPrivateKey::new(&mut rng, 2048).unwrap();
/// let pem = rsa.to_pkcs1_pem(rsa::pkcs8::LineEnding::LF).unwrap();
/// let parsed = load_rsa_private_key_from_bytes(pem.as_bytes()).unwrap();
/// assert_eq!(parsed.size(), rsa.size());
/// ```
pub fn load_rsa_private_key_from_bytes(bytes: &[u8]) -> Result<RsaPrivateKey, CryptoError> {
    let pem = std::str::from_utf8(bytes)
        .map_err(|e| CryptoError::InvalidPem(format!("PEM is not UTF-8: {e}")))?;
    if pem.contains("-----BEGIN RSA PRIVATE KEY-----") {
        return RsaPrivateKey::from_pkcs1_pem(pem)
            .map_err(|e| CryptoError::InvalidPem(format!("PKCS#1 parse failed: {e}")));
    }
    if pem.contains("-----BEGIN PRIVATE KEY-----") {
        return RsaPrivateKey::from_pkcs8_pem(pem)
            .map_err(|e| CryptoError::InvalidPem(format!("PKCS#8 parse failed: {e}")));
    }
    Err(CryptoError::InvalidPem(
        "no RSA private key marker found".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::OsRng;
    use rsa::pkcs1::EncodeRsaPrivateKey;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use rsa::traits::PublicKeyParts;

    fn fresh_key() -> RsaPrivateKey {
        let mut rng = OsRng;
        RsaPrivateKey::new(&mut rng, 2048).unwrap()
    }

    #[test]
    fn pkcs1_round_trips() {
        let rsa = fresh_key();
        let pem = rsa.to_pkcs1_pem(LineEnding::LF).unwrap();
        let loaded = load_rsa_private_key_from_bytes(pem.as_bytes()).unwrap();
        assert_eq!(loaded.size(), rsa.size());
    }

    #[test]
    fn pkcs8_round_trips() {
        let rsa = fresh_key();
        let pem = rsa.to_pkcs8_pem(LineEnding::LF).unwrap();
        let loaded = load_rsa_private_key_from_bytes(pem.as_bytes()).unwrap();
        assert!(loaded.size() > 0);
    }

    #[test]
    fn unknown_marker_is_rejected() {
        let bad = b"-----BEGIN GARBAGE-----\nabcd\n-----END GARBAGE-----\n";
        let err = load_rsa_private_key_from_bytes(bad).unwrap_err();
        assert!(matches!(err, CryptoError::InvalidPem(_)));
    }

    #[test]
    fn malformed_pkcs1_returns_invalid_pem() {
        let bad = b"-----BEGIN RSA PRIVATE KEY-----\nNOTBASE64\n-----END RSA PRIVATE KEY-----\n";
        let err = load_rsa_private_key_from_bytes(bad).unwrap_err();
        assert!(matches!(err, CryptoError::InvalidPem(_)));
    }
}
