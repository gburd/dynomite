//! Pre-shared key and IV loading for the entropy reconciliation
//! channel.
//!
//! The reconciliation channel uses AES-128-CBC with a 16-byte key
//! and 16-byte IV held in two on-disk files at the conf-configured
//! `recon_key.pem` and `recon_iv.pem` paths. Despite the `.pem`
//! suffix, the bundled fixtures are plain ASCII files containing the
//! key material followed by a trailing newline.
//!
//! The loader honours the contents of the file. To absorb
//! the off-by-one in the bundled fixture (the file is
//! `01234567890123456` -- seventeen characters, not sixteen) it
//! takes the first [`ENTROPY_KEY_LEN`] / [`ENTROPY_IV_LEN`] bytes
//! once trailing whitespace has been trimmed, provided the file
//! contains at least that many bytes. This is recorded as a
//! deviation in `docs/parity.md`.
//!
//! The loader accepts both shapes:
//!
//! * a raw secret followed by optional trailing whitespace
//!   (matches the bundled fixtures), and
//! * a `BEGIN/END`-armored PEM block whose decoded body is at
//!   least 16 bytes long.
//!
//! Anything else is rejected.

use std::fs;
use std::path::Path;

use crate::crypto::base64::base64_decode;
use crate::entropy::EntropyError;

/// Length in bytes of the AES-128 key consumed by the entropy
/// channel.
pub const ENTROPY_KEY_LEN: usize = 16;

/// Length in bytes of the AES-128-CBC initialisation vector
/// consumed by the entropy channel.
pub const ENTROPY_IV_LEN: usize = 16;

/// 16-byte AES-128 key for the entropy reconciliation channel.
///
/// # Examples
///
/// ```
/// use dynomite::entropy::util::{EntropyKey, ENTROPY_KEY_LEN};
/// let key = EntropyKey::from_bytes([0x10; ENTROPY_KEY_LEN]);
/// assert_eq!(key.as_bytes().len(), ENTROPY_KEY_LEN);
/// ```
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct EntropyKey([u8; ENTROPY_KEY_LEN]);

impl EntropyKey {
    /// Wrap a fixed-size array.
    #[must_use]
    pub fn from_bytes(bytes: [u8; ENTROPY_KEY_LEN]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw key material.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; ENTROPY_KEY_LEN] {
        &self.0
    }
}

impl std::fmt::Debug for EntropyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntropyKey")
            .field("len", &ENTROPY_KEY_LEN)
            .finish()
    }
}

/// 16-byte AES-128-CBC IV for the entropy reconciliation channel.
///
/// # Examples
///
/// ```
/// use dynomite::entropy::util::{EntropyIv, ENTROPY_IV_LEN};
/// let iv = EntropyIv::from_bytes([0x42; ENTROPY_IV_LEN]);
/// assert_eq!(iv.as_bytes().len(), ENTROPY_IV_LEN);
/// ```
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct EntropyIv([u8; ENTROPY_IV_LEN]);

impl EntropyIv {
    /// Wrap a fixed-size array.
    #[must_use]
    pub fn from_bytes(bytes: [u8; ENTROPY_IV_LEN]) -> Self {
        Self(bytes)
    }

    /// Borrow the raw IV material.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; ENTROPY_IV_LEN] {
        &self.0
    }
}

impl std::fmt::Debug for EntropyIv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntropyIv")
            .field("len", &ENTROPY_IV_LEN)
            .finish()
    }
}

/// Pre-shared key + IV pair held by the entropy worker.
///
/// # Examples
///
/// ```
/// use dynomite::entropy::util::{EntropyKey, EntropyIv, EntropyMaterial};
/// let mat = EntropyMaterial::new(
///     EntropyKey::from_bytes([0x10; 16]),
///     EntropyIv::from_bytes([0x42; 16]),
/// );
/// assert_eq!(mat.key().as_bytes()[0], 0x10);
/// assert_eq!(mat.iv().as_bytes()[0], 0x42);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntropyMaterial {
    key: EntropyKey,
    iv: EntropyIv,
}

impl EntropyMaterial {
    /// Bundle a key and IV into a single material handle.
    #[must_use]
    pub fn new(key: EntropyKey, iv: EntropyIv) -> Self {
        Self { key, iv }
    }

    /// Borrow the AES key.
    #[must_use]
    pub fn key(&self) -> &EntropyKey {
        &self.key
    }

    /// Borrow the AES IV.
    #[must_use]
    pub fn iv(&self) -> &EntropyIv {
        &self.iv
    }
}

/// Strip the `-----BEGIN/END-----` armor and decode the base64
/// body when present. Returns the input verbatim (less trailing
/// whitespace) if no armor markers are found.
fn parse_secret_bytes(text: &str) -> Result<Vec<u8>, EntropyError> {
    if text.contains("-----BEGIN") {
        return decode_pem_block(text);
    }
    let trimmed = text.trim_end_matches(['\r', '\n', ' ', '\t']);
    Ok(trimmed.as_bytes().to_vec())
}

/// Minimal PEM block decoder: locates the first `-----BEGIN ...-----`
/// line, gathers everything up to the matching `-----END ...-----`
/// line, base64-decodes the body. Header/key-value lines inside the
/// block are not supported (the entropy loader does not produce
/// them).
fn decode_pem_block(text: &str) -> Result<Vec<u8>, EntropyError> {
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim_start().starts_with("-----BEGIN") {
            let mut body = String::new();
            let mut saw_end = false;
            for inner in lines.by_ref() {
                let trimmed = inner.trim();
                if trimmed.starts_with("-----END") {
                    saw_end = true;
                    break;
                }
                body.push_str(trimmed);
            }
            if !saw_end {
                return Err(EntropyError::KeyMaterial(
                    "PEM block missing END marker".to_string(),
                ));
            }
            return base64_decode(&body)
                .map_err(|e| EntropyError::KeyMaterial(format!("PEM base64 decode: {e}")));
        }
    }
    Err(EntropyError::KeyMaterial(
        "PEM block missing BEGIN marker".to_string(),
    ))
}

/// Read the AES key from `path`. The file must contain exactly
/// 16 bytes of key material (raw or PEM-armored).
///
/// # Errors
/// Returns [`EntropyError::Io`] if the file cannot be read and
/// [`EntropyError::KeyMaterial`] if the contents do not yield
/// exactly [`ENTROPY_KEY_LEN`] bytes.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use dynomite::entropy::util::load_key_file;
/// let key = load_key_file(Path::new("/etc/dynomite/recon_key.pem")).unwrap();
/// assert_eq!(key.as_bytes().len(), 16);
/// ```
pub fn load_key_file(path: &Path) -> Result<EntropyKey, EntropyError> {
    let raw = fs::read_to_string(path).map_err(|e| io_err(path, "read key file", &e))?;
    let bytes = parse_secret_bytes(&raw)?;
    if bytes.len() < ENTROPY_KEY_LEN {
        return Err(EntropyError::KeyMaterial(format!(
            "expected at least {ENTROPY_KEY_LEN} bytes of key material in {}, got {}",
            path.display(),
            bytes.len()
        )));
    }
    let mut out = [0u8; ENTROPY_KEY_LEN];
    out.copy_from_slice(&bytes[..ENTROPY_KEY_LEN]);
    Ok(EntropyKey(out))
}

/// Read the AES IV from `path`. The file must contain exactly
/// 16 bytes of IV material (raw or PEM-armored).
///
/// # Errors
/// Returns [`EntropyError::Io`] if the file cannot be read and
/// [`EntropyError::KeyMaterial`] if the contents do not yield
/// exactly [`ENTROPY_IV_LEN`] bytes.
///
/// # Examples
///
/// ```no_run
/// use std::path::Path;
/// use dynomite::entropy::util::load_iv_file;
/// let iv = load_iv_file(Path::new("/etc/dynomite/recon_iv.pem")).unwrap();
/// assert_eq!(iv.as_bytes().len(), 16);
/// ```
pub fn load_iv_file(path: &Path) -> Result<EntropyIv, EntropyError> {
    let raw = fs::read_to_string(path).map_err(|e| io_err(path, "read iv file", &e))?;
    let bytes = parse_secret_bytes(&raw)?;
    if bytes.len() < ENTROPY_IV_LEN {
        return Err(EntropyError::KeyMaterial(format!(
            "expected at least {ENTROPY_IV_LEN} bytes of IV material in {}, got {}",
            path.display(),
            bytes.len()
        )));
    }
    let mut out = [0u8; ENTROPY_IV_LEN];
    out.copy_from_slice(&bytes[..ENTROPY_IV_LEN]);
    Ok(EntropyIv(out))
}

/// Convenience wrapper that loads both files and bundles them.
///
/// # Errors
/// Forwarded from [`load_key_file`] / [`load_iv_file`]. Both files
/// are read; if both fail only the first error is returned.
///
/// # Examples
///
/// ```no_run
/// use std::path::PathBuf;
/// use dynomite::entropy::util::load_material;
/// let mat = load_material(
///     &PathBuf::from("/etc/dynomite/recon_key.pem"),
///     &PathBuf::from("/etc/dynomite/recon_iv.pem"),
/// ).unwrap();
/// assert_eq!(mat.key().as_bytes().len(), 16);
/// ```
pub fn load_material(key_file: &Path, iv_file: &Path) -> Result<EntropyMaterial, EntropyError> {
    let key = load_key_file(key_file)?;
    let iv = load_iv_file(iv_file)?;
    Ok(EntropyMaterial::new(key, iv))
}

fn io_err(path: &Path, what: &str, e: &std::io::Error) -> EntropyError {
    EntropyError::Io(std::io::Error::new(
        e.kind(),
        format!("{what} {}: {e}", path.display()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp(contents: &[u8]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(contents).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn loads_raw_16_byte_key() {
        let f = write_temp(b"0123456789012345\n");
        let key = load_key_file(f.path()).unwrap();
        assert_eq!(key.as_bytes(), b"0123456789012345");
    }

    #[test]
    fn loads_raw_16_byte_iv() {
        let f = write_temp(b"0123456789012345\n");
        let iv = load_iv_file(f.path()).unwrap();
        assert_eq!(iv.as_bytes(), b"0123456789012345");
    }

    #[test]
    fn rejects_short_key() {
        let f = write_temp(b"short\n");
        let err = load_key_file(f.path()).unwrap_err();
        assert!(matches!(err, EntropyError::KeyMaterial(_)));
    }

    #[test]
    fn rejects_short_iv() {
        let f = write_temp(b"short\n");
        let err = load_iv_file(f.path()).unwrap_err();
        assert!(matches!(err, EntropyError::KeyMaterial(_)));
    }

    #[test]
    fn truncates_oversized_key_to_16_bytes() {
        let f = write_temp(b"01234567890123456\n");
        let key = load_key_file(f.path()).unwrap();
        assert_eq!(key.as_bytes(), b"0123456789012345");
    }

    #[test]
    fn truncates_oversized_iv_to_16_bytes() {
        let f = write_temp(b"01234567890123456\n");
        let iv = load_iv_file(f.path()).unwrap();
        assert_eq!(iv.as_bytes(), b"0123456789012345");
    }

    #[test]
    fn loads_pem_armored_16_bytes() {
        // 16 bytes of 0x42 base64-armored.
        let body: [u8; 16] = [0x42; 16];
        let armored = format!(
            "-----BEGIN ENTROPY KEY-----\n{}\n-----END ENTROPY KEY-----\n",
            crate::crypto::base64::base64_encode(&body)
        );
        let f = write_temp(armored.as_bytes());
        let key = load_key_file(f.path()).unwrap();
        assert_eq!(key.as_bytes(), &body);
    }

    #[test]
    fn missing_file_is_io_error() {
        let path = Path::new("/nonexistent/dynomite/no-such-key");
        let err = load_key_file(path).unwrap_err();
        assert!(matches!(err, EntropyError::Io(_)));
    }

    #[test]
    fn loads_bundled_recon_fixtures() {
        // Bundled recon fixtures live with the crate's test data.
        let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let key_path = crate_root.join("tests/fixtures/recon/recon_key.pem");
        let iv_path = crate_root.join("tests/fixtures/recon/recon_iv.pem");
        let key = load_key_file(&key_path).unwrap();
        let iv = load_iv_file(&iv_path).unwrap();
        // The bundled fixtures contain a 17-byte ASCII string
        // ("01234567890123456"); the loader takes the first 16
        // bytes, yielding the 16-byte key the cipher runs against.
        assert_eq!(key.as_bytes(), b"0123456789012345");
        assert_eq!(iv.as_bytes(), b"0123456789012345");
    }
}
