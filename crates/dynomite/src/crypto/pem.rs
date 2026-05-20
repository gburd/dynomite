//! PEM key file loading.
//!
//! Accepts both PKCS#1 framed RSA keys (`-----BEGIN RSA PRIVATE KEY-----`)
//! and PKCS#8 framed private keys (`-----BEGIN PRIVATE KEY-----`). The
//! reference engine ships PKCS#1 fixtures by default; PKCS#8 support
//! lets new deployments use the formats produced by modern OpenSSL
//! and `ssh-keygen`.

use std::fs;
use std::path::Path;

use openssl::pkey::Private;
use openssl::rsa::Rsa;

use crate::crypto::CryptoError;

/// Load an RSA private key from a PEM file at `path`.
///
/// The file content is sniffed: PKCS#1 framing is decoded directly,
/// PKCS#8 framing is decoded via [`openssl::pkey::PKey`] and then
/// extracted as RSA. Anything else is rejected with
/// [`CryptoError::InvalidPem`].
///
/// # Examples
///
/// ```no_run
/// use dynomite::crypto::pem::load_rsa_private_key;
/// let key = load_rsa_private_key("conf/dynomite.pem".as_ref()).unwrap();
/// assert!(key.size() > 0);
/// ```
pub fn load_rsa_private_key(path: &Path) -> Result<Rsa<Private>, CryptoError> {
    let bytes = fs::read(path)?;
    load_rsa_private_key_from_bytes(&bytes)
}

/// Parse an RSA private key from raw PEM bytes.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::pem::load_rsa_private_key_from_bytes;
///
/// let rsa = openssl::rsa::Rsa::generate(2048).unwrap();
/// let pem = rsa.private_key_to_pem().unwrap();
/// let parsed = load_rsa_private_key_from_bytes(&pem).unwrap();
/// assert_eq!(parsed.size(), rsa.size());
/// ```
pub fn load_rsa_private_key_from_bytes(bytes: &[u8]) -> Result<Rsa<Private>, CryptoError> {
    if find_marker(bytes, b"-----BEGIN RSA PRIVATE KEY-----") {
        return Rsa::private_key_from_pem(bytes)
            .map_err(|e| CryptoError::InvalidPem(format!("PKCS#1 parse failed: {e}")));
    }
    if find_marker(bytes, b"-----BEGIN PRIVATE KEY-----") {
        let pkey = openssl::pkey::PKey::private_key_from_pem(bytes)
            .map_err(|e| CryptoError::InvalidPem(format!("PKCS#8 parse failed: {e}")))?;
        return pkey
            .rsa()
            .map_err(|e| CryptoError::InvalidPem(format!("PKCS#8 not RSA: {e}")));
    }
    Err(CryptoError::InvalidPem(
        "no RSA private key marker found".to_string(),
    ))
}

fn find_marker(haystack: &[u8], needle: &[u8]) -> bool {
    if haystack.len() < needle.len() {
        return false;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkcs1_round_trips() {
        let rsa = Rsa::generate(2048).unwrap();
        let pem = rsa.private_key_to_pem().unwrap();
        let loaded = load_rsa_private_key_from_bytes(&pem).unwrap();
        assert_eq!(loaded.size(), rsa.size());
    }

    #[test]
    fn pkcs8_round_trips() {
        let rsa = Rsa::generate(2048).unwrap();
        let pkey = openssl::pkey::PKey::from_rsa(rsa).unwrap();
        let pem = pkey.private_key_to_pem_pkcs8().unwrap();
        let loaded = load_rsa_private_key_from_bytes(&pem).unwrap();
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
