//! Typed errors emitted by configuration parsing and validation.

use std::path::PathBuf;

use thiserror::Error;

/// Errors that can occur while loading or validating a [`Config`].
///
/// [`Config`]: crate::conf::Config
///
/// # Examples
///
/// ```
/// use dynomite::conf::{Config, ConfError};
/// let err = Config::parse_str("").unwrap_err();
/// assert!(matches!(err, ConfError::Yaml { .. } | ConfError::EmptyDocument));
/// ```
#[derive(Debug, Error)]
pub enum ConfError {
    /// I/O error while reading a configuration file.
    #[error("conf: failed to read configuration file '{path}': {source}")]
    Io {
        /// The path that triggered the failure.
        path: PathBuf,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// The YAML document was empty or missing the top-level pool.
    #[error("conf: configuration document is empty")]
    EmptyDocument,

    /// The top-level mapping had more than one pool.
    #[error("conf: configuration must contain exactly one pool, found {0}")]
    TooManyPools(usize),

    /// The top-level pool name was the empty string.
    #[error("conf: pool name must not be empty")]
    EmptyPoolName,

    /// A directive name in the YAML is not recognized.
    #[error("conf: directive '{name}' is unknown")]
    UnknownKey {
        /// The unrecognized YAML key.
        name: String,
    },

    /// A required directive is absent from the configuration.
    #[error("conf: directive '{0}' is missing")]
    MissingRequired(&'static str),

    /// `listen` / `dyn_listen` / `stats_listen` address could not be parsed.
    #[error("conf: '{field}' has an invalid address '{value}': {reason}")]
    BadAddr {
        /// The directive whose value failed to parse.
        field: &'static str,
        /// The string that failed to parse.
        value: String,
        /// Human-readable parse failure reason.
        reason: String,
    },

    /// A token list could not be parsed as comma-separated big-ints.
    #[error("conf: token list '{value}' is not valid: {reason}")]
    BadToken {
        /// The token-list string.
        value: String,
        /// Human-readable parse failure reason.
        reason: String,
    },

    /// `read_consistency` / `write_consistency` value is not a known level.
    #[error("conf: directive '{field}' must be one of 'DC_ONE', 'DC_QUORUM', 'DC_SAFE_QUORUM', 'DC_EACH_SAFE_QUORUM', got '{value}'")]
    BadConsistency {
        /// The directive: `read_consistency` or `write_consistency`.
        field: &'static str,
        /// The unrecognized value.
        value: String,
    },

    /// `secure_server_option` value is not a known mode.
    #[error("conf: directive 'secure_server_option' must be one of 'none', 'rack', 'datacenter', 'all', got '{0}'")]
    BadSecure(String),

    /// `data_store` value is not 0 (Redis), 1 (Memcache), or 2
    /// (Noxu). The string forms `redis`, `memcache`, and `noxu`
    /// are also accepted on the YAML side and are translated to
    /// these integers before validation.
    #[error("conf: directive 'data_store' must be 0 (redis), 1 (memcache), or 2 (noxu), got {0}")]
    BadDataStore(i64),

    /// `data_store: noxu` was selected but `noxu_path` was not
    /// supplied or `dynomited` was built without `--features
    /// riak`.
    #[error("conf: {0}")]
    BadNoxuConfig(&'static str),

    /// `hash` value is not a recognized hash algorithm name.
    #[error("conf: directive 'hash' is not a valid hash function, got '{0}'")]
    BadHash(String),

    /// `distribution` value is not a recognized distribution
    /// algorithm name.
    #[error("conf: directive 'distribution' is not a valid distribution, got '{0}'")]
    BadDistribution(String),

    /// A numeric directive is out of its allowed range.
    #[error("conf: directive '{field}' value {value} is out of range: {reason}")]
    OutOfRange {
        /// The directive name.
        field: &'static str,
        /// The offending value.
        value: i64,
        /// What range was expected.
        reason: &'static str,
    },

    /// A server / `dyn_seeds` entry is malformed.
    #[error("conf: '{field}' entry '{value}' is invalid: {reason}")]
    BadServer {
        /// The directive (`servers` or `dyn_seeds`).
        field: &'static str,
        /// The malformed entry.
        value: String,
        /// Human-readable failure reason.
        reason: String,
    },

    /// `hash_tag` must be exactly two characters, per the C parser.
    #[error("conf: directive 'hash_tag' must be a string of exactly 2 characters, got '{0}'")]
    BadHashTag(String),

    /// The YAML failed to parse at the document level.
    #[error("conf: yaml parse error: {message}")]
    Yaml {
        /// The YAML error message (with location, if available).
        message: String,
    },
}

impl ConfError {
    pub(crate) fn from_yaml(err: &serde_yaml::Error) -> Self {
        let message = err.to_string();
        // serde_yaml emits unknown-field errors as
        //   `<key>: unknown field \`<name>\`, expected one of ...`.
        // Pull the field name out so callers can pattern-match
        // on the typed `UnknownKey` variant directly.
        if let Some(start) = message.find("unknown field `") {
            let after = &message[start + "unknown field `".len()..];
            if let Some(end) = after.find('`') {
                return ConfError::UnknownKey {
                    name: after[..end].to_string(),
                };
            }
        }
        ConfError::Yaml { message }
    }
}
