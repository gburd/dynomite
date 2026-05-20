//! Base64 encoding helpers.
//!
//! Wraps the workspace `base64` crate with the standard alphabet and
//! no padding stripping. The reference engine emits unpadded output
//! suitable for embedding in DNODE handshake fields and decodes
//! padded or unpadded inputs equivalently; this module preserves that
//! behavior.

use base64::engine::general_purpose::STANDARD;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;

use crate::crypto::CryptoError;

/// Encode `bytes` to a base64 string using the standard alphabet
/// without trailing padding characters.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::base64_encode;
/// assert_eq!(base64_encode(b"hi"), "aGk");
/// assert_eq!(base64_encode(b""), "");
/// ```
pub fn base64_encode(bytes: &[u8]) -> String {
    STANDARD_NO_PAD.encode(bytes)
}

/// Decode a base64 string. Accepts both padded and unpadded inputs
/// using the standard alphabet.
///
/// # Examples
///
/// ```
/// use dynomite::crypto::base64_decode;
/// assert_eq!(base64_decode("aGk").unwrap(), b"hi");
/// assert_eq!(base64_decode("aGk=").unwrap(), b"hi");
/// assert!(base64_decode("not base64!@#").is_err());
/// ```
pub fn base64_decode(s: &str) -> Result<Vec<u8>, CryptoError> {
    if s.contains('=') {
        STANDARD
            .decode(s.as_bytes())
            .map_err(|e| CryptoError::Base64(e.to_string()))
    } else {
        STANDARD_NO_PAD
            .decode(s.as_bytes())
            .map_err(|e| CryptoError::Base64(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_round_trip() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_decode("").unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn standard_vectors() {
        assert_eq!(base64_encode(b"f"), "Zg");
        assert_eq!(base64_encode(b"fo"), "Zm8");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn padded_decodes_too() {
        assert_eq!(base64_decode("Zg==").unwrap(), b"f");
        assert_eq!(base64_decode("Zm8=").unwrap(), b"fo");
    }

    #[test]
    fn invalid_input_errors() {
        assert!(base64_decode("@@@").is_err());
        assert!(base64_decode("####").is_err());
    }
}
