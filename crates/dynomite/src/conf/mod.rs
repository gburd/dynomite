//! YAML configuration: schema, parsing, defaulting, validation.
//!
//! The top-level YAML document is a single-key mapping from a pool name
//! to a [`ConfPool`]. [`Config`] wraps both. The typical lifecycle is:
//!
//! 1. [`Config::parse_str`] (or [`Config::parse_file`]) - parse YAML and
//!    apply structural checks.
//! 2. [`Config::finalize`] - apply defaults to fields that were left
//!    unset.
//! 3. [`Config::validate`] - run the full set of cross-field checks.
//!
//! [`Config::test_conf`] is the convenience used by the `-t` flag of the
//! server binary and runs `finalize` + `validate` and returns a short
//! status string.
//!
//! # Examples
//!
//! ```
//! use dynomite::conf::Config;
//!
//! let yaml = r#"
//! dyn_o_mite:
//!   listen: 127.0.0.1:8102
//!   dyn_listen: 127.0.0.1:8101
//!   tokens: '101134286'
//!   servers:
//!   - 127.0.0.1:22122:1
//!   data_store: 0
//!   mbuf_size: 16384
//!   max_msgs: 300000
//! "#;
//!
//! let mut cfg = Config::parse_str(yaml).unwrap();
//! cfg.finalize();
//! cfg.validate().unwrap();
//! assert_eq!(cfg.pool_name(), "dyn_o_mite");
//! ```

mod endpoint;
mod enums;
mod error;
mod pool;
mod server;
mod tokens;

pub use endpoint::{ConfListen, EndpointKind};
pub use enums::{ConsistencyLevel, DataStore, HashType, SecureServerOption};
pub use error::ConfError;
pub use pool::{ConfPool, Servers};
pub use server::{ConfDynSeed, ConfServer};
pub use tokens::{TokenComponent, TokenList};

use std::collections::BTreeMap;
use std::path::Path;

/// Top-level configuration value: a single named [`ConfPool`].
///
/// The YAML document mirrors the C reference: a top-level mapping with
/// exactly one key, the pool name, whose value is the pool body.
#[derive(Debug, Clone)]
pub struct Config {
    pool_name: String,
    pool: ConfPool,
}

impl Config {
    /// Parse a YAML configuration document from a string.
    ///
    /// Performs structural validation (exactly one pool, no unknown
    /// keys) but does not apply defaults. Call [`Config::finalize`]
    /// before [`Config::validate`] to fully prepare the config.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// let yaml = "p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n";
    /// let cfg = Config::parse_str(yaml).unwrap();
    /// assert_eq!(cfg.pool_name(), "p");
    /// assert!(Config::parse_str("").is_err());
    /// ```
    pub fn parse_str(input: &str) -> Result<Self, ConfError> {
        let raw: BTreeMap<String, ConfPool> =
            serde_yaml::from_str(input).map_err(|e| ConfError::from_yaml(&e))?;
        if raw.is_empty() {
            return Err(ConfError::EmptyDocument);
        }
        if raw.len() != 1 {
            return Err(ConfError::TooManyPools(raw.len()));
        }
        let (pool_name, pool) = raw
            .into_iter()
            .next()
            .expect("invariant: raw.len() == 1, checked above");
        if pool_name.is_empty() {
            return Err(ConfError::EmptyPoolName);
        }
        Ok(Self { pool_name, pool })
    }

    /// Parse a YAML configuration document from a filesystem path.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Write;
    /// use dynomite::conf::Config;
    /// let mut f = tempfile::NamedTempFile::new().unwrap();
    /// writeln!(f, "p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n").unwrap();
    /// let cfg = Config::parse_file(f.path()).unwrap();
    /// assert_eq!(cfg.pool_name(), "p");
    /// ```
    pub fn parse_file(path: &Path) -> Result<Self, ConfError> {
        let bytes = std::fs::read_to_string(path).map_err(|e| ConfError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        Self::parse_str(&bytes)
    }

    /// The configured pool name (the single top-level YAML key).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// let cfg = Config::parse_str("my_pool:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n").unwrap();
    /// assert_eq!(cfg.pool_name(), "my_pool");
    /// ```
    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    /// Borrow the inner [`ConfPool`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// let cfg = Config::parse_str("p:\n  listen: 127.0.0.1:8102\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n").unwrap();
    /// assert_eq!(cfg.pool().listen.as_ref().unwrap().port(), 8102);
    /// ```
    pub fn pool(&self) -> &ConfPool {
        &self.pool
    }

    /// Mutably borrow the inner [`ConfPool`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// let mut cfg = Config::parse_str("p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n").unwrap();
    /// cfg.pool_mut().preconnect = Some(true);
    /// assert_eq!(cfg.pool().preconnect, Some(true));
    /// ```
    pub fn pool_mut(&mut self) -> &mut ConfPool {
        &mut self.pool
    }

    /// Apply default values to any field left unset by the YAML.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// let mut cfg = Config::parse_str("p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n").unwrap();
    /// assert!(cfg.pool().rack.is_none());
    /// cfg.finalize();
    /// assert!(cfg.pool().rack.is_some());
    /// ```
    pub fn finalize(&mut self) {
        self.pool.apply_defaults();
    }

    /// Run the full validation pass.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// let mut cfg = Config::parse_str("p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n").unwrap();
    /// cfg.finalize();
    /// cfg.validate().unwrap();
    /// ```
    pub fn validate(&self) -> Result<(), ConfError> {
        self.pool.validate(&self.pool_name)
    }

    /// Equivalent of `dynomite -t -c <file>`: finalize, validate, and
    /// produce a short status string.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// let cfg = Config::parse_str("p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n").unwrap();
    /// assert!(cfg.test_conf().unwrap().contains("is valid"));
    /// ```
    pub fn test_conf(&self) -> Result<String, ConfError> {
        let mut owned = self.clone();
        owned.finalize();
        owned.validate()?;
        Ok(format!(
            "configuration file with pool '{}' is valid",
            owned.pool_name
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r"
dyn_o_mite:
  listen: 127.0.0.1:8102
  dyn_listen: 127.0.0.1:8101
  tokens: '101134286'
  servers:
  - 127.0.0.1:22122:1
  data_store: 0
";

    #[test]
    fn parse_minimal() {
        let cfg = Config::parse_str(MINIMAL).unwrap();
        assert_eq!(cfg.pool_name(), "dyn_o_mite");
        assert_eq!(cfg.pool().listen.as_ref().unwrap().port(), 8102);
    }

    #[test]
    fn finalize_sets_defaults() {
        let mut cfg = Config::parse_str(MINIMAL).unwrap();
        cfg.finalize();
        assert_eq!(cfg.pool().rack.as_deref(), Some("localrack"));
        assert_eq!(cfg.pool().datacenter.as_deref(), Some("localdc"));
        assert_eq!(cfg.pool().timeout, Some(5000));
    }

    #[test]
    fn validate_minimal() {
        let mut cfg = Config::parse_str(MINIMAL).unwrap();
        cfg.finalize();
        cfg.validate().unwrap();
    }

    #[test]
    fn empty_document_rejected() {
        let err = Config::parse_str("").unwrap_err();
        assert!(matches!(
            err,
            ConfError::Yaml { .. } | ConfError::EmptyDocument
        ));
    }

    #[test]
    fn too_many_pools_rejected() {
        let yaml = "a:\n  listen: 1.2.3.4:80\nb:\n  listen: 1.2.3.4:81\n";
        let err = Config::parse_str(yaml).unwrap_err();
        assert!(matches!(err, ConfError::TooManyPools(2)));
    }

    #[test]
    fn unknown_key_rejected() {
        let yaml = "p:\n  listen: 127.0.0.1:1\n  bogus_key: 42\n";
        let err = Config::parse_str(yaml).unwrap_err();
        match err {
            ConfError::UnknownKey { name } => assert_eq!(name, "bogus_key"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_conf_reports_pool_name() {
        let cfg = Config::parse_str(MINIMAL).unwrap();
        let report = cfg.test_conf().unwrap();
        assert!(report.contains("dyn_o_mite"));
    }
}
