//! Pool body schema, default application, and validation.
//!
//! The [`ConfPool`] struct mirrors `struct conf_pool` from the C
//! reference. Every field whose C counterpart starts as
//! `CONF_UNSET_NUM` / `CONF_UNSET_BOOL` / `CONF_UNSET_HASH` is wrapped
//! in [`Option`]; [`ConfPool::apply_defaults`] later fills in the
//! sentinel-driven defaults.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::endpoint::ConfListen;
use super::enums::{ConsistencyLevel, DataStore, HashType, SecureServerOption};
use super::error::ConfError;
use super::server::{ConfDynSeed, ConfServer};
use super::tokens::TokenList;

/// Default configuration constants. Mirrors the `CONF_DEFAULT_*` macros
/// in the C reference.
pub mod defaults {
    /// Default request timeout in milliseconds.
    pub const TIMEOUT_MS: i64 = 5_000;
    /// Default `listen()` backlog.
    pub const LISTEN_BACKLOG: i64 = 512;
    /// Default `client_connections:` value (0 = unlimited).
    pub const CLIENT_CONNECTIONS: i64 = 0;
    /// Default `data_store:` value (0 = redis).
    pub const DATA_STORE: i64 = 0;
    /// Default `preconnect:` value.
    ///
    /// The C reference defaults `preconnect` to false: clients
    /// can connect to dynomited before the local datastore is
    /// reachable. The lazy connect avoids a hard dependency on
    /// boot ordering. We match that default here.
    pub const PRECONNECT: bool = false;
    /// Default `auto_eject_hosts:` value.
    pub const AUTO_EJECT_HOSTS: bool = true;
    /// Default `server_retry_timeout:` (ms).
    pub const SERVER_RETRY_TIMEOUT_MS: i64 = 10 * 1000;
    /// Default `server_failure_limit:`.
    pub const SERVER_FAILURE_LIMIT: i64 = 3;
    /// Default `dyn_read_timeout:` (ms).
    pub const DYN_READ_TIMEOUT_MS: i64 = 10_000;
    /// Default `dyn_write_timeout:` (ms).
    pub const DYN_WRITE_TIMEOUT_MS: i64 = 10_000;
    /// Default `dyn_connections:`.
    pub const DYN_CONNECTIONS: i64 = 100;
    /// Default `gos_interval:` (ms).
    pub const GOS_INTERVAL_MS: i64 = 30_000;
    /// Default per-connection message rate.
    pub const CONN_MSG_RATE: u32 = 50_000;
    /// Default `stats_interval:` (ms).
    pub const STATS_INTERVAL_MS: i64 = 30 * 1000;
    /// Default stats listener address.
    pub const STATS_PNAME: &str = "0.0.0.0:22222";
    /// Default datastore-side connection count.
    pub const DATASTORE_CONNECTIONS: u8 = 1;
    /// Default local-peer connection count.
    pub const LOCAL_PEER_CONNECTIONS: u8 = 1;
    /// Default remote-peer connection count.
    pub const REMOTE_PEER_CONNECTIONS: u8 = 1;
    /// Default rack name.
    pub const RACK: &str = "localrack";
    /// Default datacenter name.
    pub const DC: &str = "localdc";
    /// Default `secure_server_option:` value.
    pub const SECURE_SERVER_OPTION: &str = "none";
    /// Default `read_consistency:` / `write_consistency:`.
    pub const CONSISTENCY: &str = "DC_ONE";
    /// Default `dyn_seed_provider:`.
    pub const SEED_PROVIDER: &str = "simple_provider";
    /// Default `env:` (cloud environment marker).
    pub const ENV: &str = "aws";
    /// Default PEM key file path.
    pub const PEM_KEY_FILE: &str = "conf/dynomite.pem";
    /// Default reconciliation key file path.
    pub const RECON_KEY_FILE: &str = "conf/recon_key.pem";
    /// Default reconciliation IV file path.
    pub const RECON_IV_FILE: &str = "conf/recon_iv.pem";
    /// Smallest valid `mbuf_size:`.
    pub const MBUF_MIN_SIZE: i64 = 512;
    /// Largest valid `mbuf_size:`.
    pub const MBUF_MAX_SIZE: i64 = 512_000;
    /// Smallest valid `max_msgs:`.
    pub const ALLOC_MSGS_MIN: i64 = 100_000;
    /// Largest valid `max_msgs:`.
    pub const ALLOC_MSGS_MAX: i64 = 1_000_000;
}

/// Wrapper for the `servers:` field that enforces the invariant
/// of "exactly one datastore" without losing the YAML list shape.
///
/// # Examples
///
/// ```
/// use dynomite::conf::{ConfServer, Servers};
/// let s = Servers::from_vec(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()]);
/// assert_eq!(s.len(), 1);
/// assert!(!s.is_empty());
/// assert!(s.datastore().is_some());
/// ```
#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Servers(pub(crate) Vec<ConfServer>);

impl Servers {
    /// Construct from an explicit list. Validation enforces a length
    /// of one when called via `Config::validate`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfServer, Servers};
    /// let s = Servers::from_vec(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()]);
    /// assert_eq!(s.len(), 1);
    /// ```
    pub fn from_vec(v: Vec<ConfServer>) -> Self {
        Self(v)
    }
}

impl Servers {
    /// Borrow the entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfServer, Servers};
    /// let s = Servers::from_vec(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()]);
    /// assert_eq!(s.entries().len(), 1);
    /// ```
    pub fn entries(&self) -> &[ConfServer] {
        &self.0
    }
    /// Number of entries.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Servers;
    /// assert_eq!(Servers::default().len(), 0);
    /// ```
    pub fn len(&self) -> usize {
        self.0.len()
    }
    /// Whether the list is empty.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Servers;
    /// assert!(Servers::default().is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// The single datastore (returns the first entry, if any).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfServer, Servers};
    /// let s = Servers::from_vec(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()]);
    /// assert!(s.datastore().is_some());
    /// assert!(Servers::default().datastore().is_none());
    /// ```
    pub fn datastore(&self) -> Option<&ConfServer> {
        self.0.first()
    }
}

/// Pool configuration body. One per top-level YAML pool name.
///
/// # Examples
///
/// ```
/// use dynomite::conf::{ConfPool, ConfListen};
/// let mut p = ConfPool::default();
/// assert!(p.listen.is_none());
/// p.listen = Some(ConfListen::parse("listen", "127.0.0.1:8102").unwrap());
/// p.apply_defaults();
/// assert_eq!(p.timeout, Some(5_000));
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfPool {
    /// `listen:` - client-facing listener address.
    pub listen: Option<ConfListen>,
    /// `dyn_listen:` - peer-facing listener address.
    pub dyn_listen: Option<ConfListen>,
    /// `stats_listen:` - HTTP stats endpoint.
    pub stats_listen: Option<ConfListen>,

    /// `hash:` - hash function name.
    pub hash: Option<HashType>,
    /// `hash_tag:` - two-character delimiter pair.
    pub hash_tag: Option<String>,

    /// `distribution:` - deprecated; recorded for warning but ignored.
    #[serde(default)]
    pub distribution: Option<String>,
    /// `server_connections:` - deprecated; recorded for warning but ignored.
    #[serde(default)]
    pub server_connections: Option<i64>,

    /// `timeout:` - request timeout in milliseconds.
    pub timeout: Option<i64>,
    /// `backlog:` - listen backlog.
    pub backlog: Option<i64>,
    /// `client_connections:` - max client connections.
    pub client_connections: Option<i64>,
    /// `data_store:` - 0 = redis, 1 = memcache.
    pub data_store: Option<i64>,
    /// `preconnect:` - eagerly establish connections at startup.
    pub preconnect: Option<bool>,
    /// `redis_requirepass:` - optional password sent as `AUTH <pw>`
    /// on every backend connection right after the TCP handshake.
    /// Mirrors the Redis server option of the same name. Leave
    /// unset to disable. Memcache backends are not authenticated
    /// (`AUTH` is Redis-specific; memcache binary SASL is not
    /// implemented).
    #[serde(default)]
    pub redis_requirepass: Option<String>,
    /// `auto_eject_hosts:` - automatically eject failing peers.
    pub auto_eject_hosts: Option<bool>,
    /// `server_retry_timeout:` - retry interval for ejected servers (ms).
    pub server_retry_timeout: Option<i64>,
    /// `server_failure_limit:` - consecutive failures before eject.
    pub server_failure_limit: Option<i64>,

    /// `servers:` - the (single-element) datastore list.
    pub servers: Option<Servers>,

    /// `dyn_read_timeout:` - inter-node read timeout (ms).
    pub dyn_read_timeout: Option<i64>,
    /// `dyn_write_timeout:` - inter-node write timeout (ms).
    pub dyn_write_timeout: Option<i64>,
    /// `dyn_seed_provider:` - seeds backend.
    pub dyn_seed_provider: Option<String>,
    /// `dyn_seeds:` - peer dynomite nodes.
    pub dyn_seeds: Option<Vec<ConfDynSeed>>,
    /// `dyn_port:` - default peer port.
    pub dyn_port: Option<i64>,
    /// `dyn_connections:` - per-peer connection count.
    pub dyn_connections: Option<i64>,
    /// `rack:` - this node's rack.
    pub rack: Option<String>,
    /// `tokens:` - this node's tokens.
    pub tokens: Option<TokenList>,
    /// `gos_interval:` - gossip period (ms).
    pub gos_interval: Option<i64>,
    /// `secure_server_option:` - inter-node TLS mode.
    pub secure_server_option: Option<String>,
    /// `pem_key_file:` - path to the PEM private key.
    pub pem_key_file: Option<String>,
    /// `recon_key_file:` - reconciliation key path.
    pub recon_key_file: Option<String>,
    /// `recon_iv_file:` - reconciliation IV path.
    pub recon_iv_file: Option<String>,
    /// `datacenter:` - this node's datacenter.
    pub datacenter: Option<String>,
    /// `env:` - cloud environment marker.
    pub env: Option<String>,
    /// `conn_msg_rate:` - per-connection message rate cap.
    pub conn_msg_rate: Option<u32>,
    /// `read_consistency:` - quorum policy for reads.
    pub read_consistency: Option<String>,
    /// `write_consistency:` - quorum policy for writes.
    pub write_consistency: Option<String>,
    /// `stats_interval:` - stats aggregation period (ms).
    pub stats_interval: Option<i64>,
    /// `enable_gossip:` - enable / disable gossip thread.
    pub enable_gossip: Option<bool>,
    /// `mbuf_size:` - mbuf chunk size in bytes.
    pub mbuf_size: Option<i64>,
    /// `max_msgs:` - allocated message buffer size.
    pub max_msgs: Option<i64>,
    /// `datastore_connections:` - count of connections to the datastore.
    pub datastore_connections: Option<u8>,
    /// `local_peer_connections:` - count of connections to local-DC peers.
    pub local_peer_connections: Option<u8>,
    /// `remote_peer_connections:` - count of connections to remote peers.
    pub remote_peer_connections: Option<u8>,
    /// `read_repairs_enabled:` - enable read-repair on quorum mismatch.
    pub read_repairs_enabled: Option<bool>,
}

impl ConfPool {
    /// Apply defaults to any field still left `None` after parsing.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfPool;
    /// let mut p = ConfPool::default();
    /// p.apply_defaults();
    /// assert_eq!(p.timeout, Some(5_000));
    /// assert_eq!(p.rack.as_deref(), Some("localrack"));
    /// ```
    pub fn apply_defaults(&mut self) {
        if self.dyn_seed_provider.is_none() {
            self.dyn_seed_provider = Some(defaults::SEED_PROVIDER.to_string());
        }
        if self.hash.is_none() {
            self.hash = Some(HashType::Murmur);
        }
        if self.timeout.is_none() {
            self.timeout = Some(defaults::TIMEOUT_MS);
        }
        if self.backlog.is_none() {
            self.backlog = Some(defaults::LISTEN_BACKLOG);
        }
        // client_connections is unconditionally reset to its default
        // by the C validator; we mirror that exactly.
        self.client_connections = Some(defaults::CLIENT_CONNECTIONS);
        if self.data_store.is_none() {
            self.data_store = Some(defaults::DATA_STORE);
        }
        if self.preconnect.is_none() {
            self.preconnect = Some(defaults::PRECONNECT);
        }
        if self.auto_eject_hosts.is_none() {
            self.auto_eject_hosts = Some(defaults::AUTO_EJECT_HOSTS);
        }
        if self.server_retry_timeout.is_none() {
            self.server_retry_timeout = Some(defaults::SERVER_RETRY_TIMEOUT_MS);
        }
        if self.server_failure_limit.is_none() {
            self.server_failure_limit = Some(defaults::SERVER_FAILURE_LIMIT);
        }
        if self.dyn_read_timeout.is_none() {
            self.dyn_read_timeout = Some(defaults::DYN_READ_TIMEOUT_MS);
        }
        if self.dyn_write_timeout.is_none() {
            self.dyn_write_timeout = Some(defaults::DYN_WRITE_TIMEOUT_MS);
        }
        if self.dyn_connections.is_none() {
            self.dyn_connections = Some(defaults::DYN_CONNECTIONS);
        }
        if self.gos_interval.is_none() {
            self.gos_interval = Some(defaults::GOS_INTERVAL_MS);
        }
        if self.conn_msg_rate.is_none() {
            self.conn_msg_rate = Some(defaults::CONN_MSG_RATE);
        }
        if self.rack.is_none() {
            self.rack = Some(defaults::RACK.to_string());
        }
        if self.datacenter.is_none() {
            self.datacenter = Some(defaults::DC.to_string());
        }
        if self.secure_server_option.is_none() {
            self.secure_server_option = Some(defaults::SECURE_SERVER_OPTION.to_string());
        }
        if self.read_consistency.is_none() {
            self.read_consistency = Some(defaults::CONSISTENCY.to_string());
        }
        if self.write_consistency.is_none() {
            self.write_consistency = Some(defaults::CONSISTENCY.to_string());
        }
        if self.stats_interval.is_none() {
            self.stats_interval = Some(defaults::STATS_INTERVAL_MS);
        }
        if self.stats_listen.is_none() {
            // Safe: the constant is a hard-coded valid pname.
            self.stats_listen = Some(
                ConfListen::parse("stats_listen", defaults::STATS_PNAME)
                    .expect("invariant: STATS_PNAME constant is valid"),
            );
        }
        if self.env.is_none() {
            self.env = Some(defaults::ENV.to_string());
        }
        if self.pem_key_file.is_none() {
            self.pem_key_file = Some(defaults::PEM_KEY_FILE.to_string());
        }
        if self.recon_key_file.is_none() {
            self.recon_key_file = Some(defaults::RECON_KEY_FILE.to_string());
        }
        if self.recon_iv_file.is_none() {
            self.recon_iv_file = Some(defaults::RECON_IV_FILE.to_string());
        }
        if self.datastore_connections.is_none() {
            self.datastore_connections = Some(defaults::DATASTORE_CONNECTIONS);
        }
        if self.local_peer_connections.is_none() {
            self.local_peer_connections = Some(defaults::LOCAL_PEER_CONNECTIONS);
        }
        if self.remote_peer_connections.is_none() {
            self.remote_peer_connections = Some(defaults::REMOTE_PEER_CONNECTIONS);
        }
        if self.read_repairs_enabled.is_none() {
            self.read_repairs_enabled = Some(false);
        }
        if self.enable_gossip.is_none() {
            self.enable_gossip = Some(false);
        }
    }

    /// Run the full validation pass against the (presumably finalized)
    /// pool body.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfListen, ConfPool, ConfServer, Servers, TokenList};
    /// let mut p = ConfPool {
    ///     listen: Some(ConfListen::parse("listen", "127.0.0.1:8102").unwrap()),
    ///     servers: Some(Servers::from_vec(vec![ConfServer::parse("127.0.0.1:6379:1").unwrap()])),
    ///     tokens: Some(TokenList::parse("0").unwrap()),
    ///     ..ConfPool::default()
    /// };
    /// p.apply_defaults();
    /// assert!(p.validate("dyn_o_mite").is_ok());
    /// ```
    pub fn validate(&self, pool_name: &str) -> Result<(), ConfError> {
        if pool_name.is_empty() {
            return Err(ConfError::EmptyPoolName);
        }

        if self.listen.is_none() {
            return Err(ConfError::MissingRequired("listen"));
        }

        self.validate_numeric_ranges()?;
        self.validate_mbuf_size()?;
        self.validate_max_msgs()?;

        if let Some(n) = self.data_store {
            DataStore::from_int(n)?;
        }
        if let Some(tag) = &self.hash_tag {
            if tag.chars().count() != 2 {
                return Err(ConfError::BadHashTag(tag.clone()));
            }
        }

        let secure = if let Some(s) = &self.secure_server_option {
            SecureServerOption::parse(s)?
        } else {
            SecureServerOption::None
        };
        if let Some(s) = &self.read_consistency {
            ConsistencyLevel::parse("read_consistency", s)?;
        }
        if let Some(s) = &self.write_consistency {
            ConsistencyLevel::parse("write_consistency", s)?;
        }
        if secure != SecureServerOption::None {
            match &self.pem_key_file {
                Some(s) if !s.is_empty() => {}
                _ => return Err(ConfError::MissingRequired("pem_key_file")),
            }
        }

        match &self.servers {
            None => return Err(ConfError::MissingRequired("servers")),
            Some(s) if s.is_empty() => return Err(ConfError::MissingRequired("servers")),
            Some(s) if s.len() > 1 => {
                return Err(ConfError::BadServer {
                    field: "servers",
                    value: s.len().to_string(),
                    reason: "expected exactly one datastore entry".to_string(),
                });
            }
            Some(_) => {}
        }

        Ok(())
    }

    fn validate_numeric_ranges(&self) -> Result<(), ConfError> {
        check_positive("timeout", self.timeout)?;
        check_positive("backlog", self.backlog)?;
        check_non_negative("client_connections", self.client_connections)?;
        check_positive("server_retry_timeout", self.server_retry_timeout)?;
        check_positive("server_failure_limit", self.server_failure_limit)?;
        check_positive("dyn_read_timeout", self.dyn_read_timeout)?;
        check_positive("dyn_write_timeout", self.dyn_write_timeout)?;
        check_positive("gos_interval", self.gos_interval)?;
        check_positive("stats_interval", self.stats_interval)?;

        if let Some(n) = self.dyn_connections {
            if n <= 0 {
                return Err(ConfError::OutOfRange {
                    field: "dyn_connections",
                    value: n,
                    reason: "must be a positive non-zero number",
                });
            }
        }
        Ok(())
    }

    fn validate_mbuf_size(&self) -> Result<(), ConfError> {
        let Some(n) = self.mbuf_size else {
            return Ok(());
        };
        if n <= 0 {
            return Err(ConfError::OutOfRange {
                field: "mbuf_size",
                value: n,
                reason: "must be a positive number",
            });
        }
        if !(defaults::MBUF_MIN_SIZE..=defaults::MBUF_MAX_SIZE).contains(&n) {
            return Err(ConfError::OutOfRange {
                field: "mbuf_size",
                value: n,
                reason: "must be between 512 and 512000 bytes",
            });
        }
        if n % 16 != 0 {
            return Err(ConfError::OutOfRange {
                field: "mbuf_size",
                value: n,
                reason: "must be a multiple of 16",
            });
        }
        Ok(())
    }

    fn validate_max_msgs(&self) -> Result<(), ConfError> {
        let Some(n) = self.max_msgs else {
            return Ok(());
        };
        if n <= 0 {
            return Err(ConfError::OutOfRange {
                field: "max_msgs",
                value: n,
                reason: "requires a non-zero number",
            });
        }
        if !(defaults::ALLOC_MSGS_MIN..=defaults::ALLOC_MSGS_MAX).contains(&n) {
            return Err(ConfError::OutOfRange {
                field: "max_msgs",
                value: n,
                reason: "must be between 100000 and 1000000 messages",
            });
        }
        Ok(())
    }
}

impl fmt::Display for ConfPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // We render the pool body by re-serializing through serde_yaml
        // so the round-trip is well defined; this is used by `test_conf`
        // and rustdoc examples.
        match serde_yaml::to_string(self) {
            Ok(s) => f.write_str(&s),
            Err(_) => Err(fmt::Error),
        }
    }
}

fn check_positive(field: &'static str, v: Option<i64>) -> Result<(), ConfError> {
    if let Some(n) = v {
        if n <= 0 {
            return Err(ConfError::OutOfRange {
                field,
                value: n,
                reason: "must be a positive number",
            });
        }
    }
    Ok(())
}

fn check_non_negative(field: &'static str, v: Option<i64>) -> Result<(), ConfError> {
    if let Some(n) = v {
        if n < 0 {
            return Err(ConfError::OutOfRange {
                field,
                value: n,
                reason: "must be a non-negative number",
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> ConfPool {
        ConfPool {
            listen: Some(ConfListen::parse("listen", "127.0.0.1:8102").unwrap()),
            servers: Some(Servers::from_vec(vec![ConfServer::parse(
                "127.0.0.1:6379:1",
            )
            .unwrap()])),
            tokens: Some(TokenList::parse("0").unwrap()),
            ..ConfPool::default()
        }
    }

    #[test]
    fn validate_minimal_post_finalize() {
        let mut p = pool();
        p.apply_defaults();
        p.validate("dyn_o_mite").unwrap();
    }

    #[test]
    fn missing_listen_rejected() {
        let mut p = pool();
        p.listen = None;
        p.apply_defaults();
        assert!(matches!(
            p.validate("p"),
            Err(ConfError::MissingRequired("listen"))
        ));
    }

    #[test]
    fn out_of_range_mbuf_rejected() {
        let mut p = pool();
        p.mbuf_size = Some(127);
        p.apply_defaults();
        assert!(matches!(p.validate("p"), Err(ConfError::OutOfRange { .. })));
    }

    #[test]
    fn mbuf_size_not_multiple_of_16_rejected() {
        let mut p = pool();
        p.mbuf_size = Some(513);
        p.apply_defaults();
        assert!(matches!(p.validate("p"), Err(ConfError::OutOfRange { .. })));
    }

    #[test]
    fn pem_required_when_secure() {
        let mut p = pool();
        p.secure_server_option = Some("datacenter".to_string());
        p.pem_key_file = Some(String::new());
        p.apply_defaults();
        // apply_defaults restores pem_key_file because it's `Some("")`,
        // which is non-None; so we expect MissingRequired("pem_key_file").
        assert!(matches!(
            p.validate("p"),
            Err(ConfError::MissingRequired("pem_key_file"))
        ));
    }

    #[test]
    fn data_store_out_of_range_rejected() {
        let mut p = pool();
        p.data_store = Some(7);
        p.apply_defaults();
        assert!(matches!(p.validate("p"), Err(ConfError::BadDataStore(7))));
    }

    #[test]
    fn hash_tag_must_be_two_chars() {
        let mut p = pool();
        p.hash_tag = Some("abc".to_string());
        p.apply_defaults();
        assert!(matches!(p.validate("p"), Err(ConfError::BadHashTag(_))));
    }

    #[test]
    fn empty_servers_rejected() {
        let mut p = pool();
        p.servers = Some(Servers::from_vec(vec![]));
        p.apply_defaults();
        assert!(matches!(
            p.validate("p"),
            Err(ConfError::MissingRequired("servers"))
        ));
    }
}
