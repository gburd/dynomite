//! Pool body schema, default application, and validation.
//!
//! The [`ConfPool`] struct mirrors `struct conf_pool` from the C
//! reference. Every field whose C counterpart starts as
//! `CONF_UNSET_NUM` / `CONF_UNSET_BOOL` / `CONF_UNSET_HASH` is wrapped
//! in [`Option`]; [`ConfPool::apply_defaults`] later fills in the
//! sentinel-driven defaults.

use std::fmt;
use std::path::PathBuf;

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
    /// Default `enable_hinted_handoff:` value. The feature is
    /// off by default until operators opt in.
    pub const ENABLE_HINTED_HANDOFF: bool = false;
    /// Default `hint_ttl_seconds:` (24h). Hints older than this
    /// are dropped during expiry sweeps.
    pub const HINT_TTL_SECONDS: u64 = 86_400;
    /// Default `hint_store_max_bytes:` (64 MiB) cap on the
    /// node-local in-memory hint store.
    pub const HINT_STORE_MAX_BYTES: u64 = 64 * 1024 * 1024;
    /// Default `hint_drain_interval_ms:` between hint drainer
    /// sweeps.
    pub const HINT_DRAIN_INTERVAL_MS: u64 = 30_000;
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
    /// Default cadence (in seconds) of the entropy reconciliation
    /// run loop. Mirrors the brief's five-minute default; ignored
    /// when the entropy task is not enabled.
    pub const RECON_INTERVAL_SECONDS: u64 = 300;
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
    /// `data_store:` - 0 = redis, 1 = memcache, 2 = noxu.
    /// Operators may also write the textual form (`redis`,
    /// `memcache`, `noxu`); the deserializer normalises both
    /// shapes to the integer code that the rest of the engine
    /// consumes.
    #[serde(default, deserialize_with = "deserialize_data_store")]
    pub data_store: Option<i64>,
    /// `noxu_path:` - filesystem directory the in-process Noxu
    /// DB datastore opens its environment at. Required when
    /// `data_store: noxu` is selected; ignored otherwise. The
    /// directory must be writable; an existing environment is
    /// reused, otherwise one is created.
    #[serde(default)]
    pub noxu_path: Option<PathBuf>,
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
    /// `recon_interval_seconds:` - period (in seconds) of the
    /// background entropy reconciliation cycle.
    ///
    /// Ignored when [`Self::recon_key_file`] is unset or when the
    /// configured key file cannot be opened at startup. When the
    /// entropy task is enabled the default cadence is 300 seconds
    /// (five minutes); operators can override it via this YAML
    /// directive.
    #[serde(default)]
    pub recon_interval_seconds: Option<u64>,
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
    /// `peer_tls_cert:` - PEM certificate path for the dnode
    /// listener and outbound dnode connections. When both this
    /// field and [`Self::peer_tls_key`] are set the peer plane
    /// runs over TLS; when both are absent the peer plane runs
    /// in plaintext (the historical behaviour). Setting one
    /// without the other is rejected at validation time.
    #[serde(default)]
    pub peer_tls_cert: Option<PathBuf>,
    /// `peer_tls_key:` - PEM private-key path matching
    /// [`Self::peer_tls_cert`].
    #[serde(default)]
    pub peer_tls_key: Option<PathBuf>,
    /// `peer_tls_ca:` - optional PEM CA bundle. When set, the
    /// dnode listener requires every inbound peer to present a
    /// certificate signed by a CA from this bundle (mutual TLS).
    /// When unset, the listener still terminates TLS but does
    /// not request a client certificate. The outbound side uses
    /// this bundle as its trust anchor; when unset, the bundled
    /// `webpki_roots` Mozilla bundle is used.
    #[serde(default)]
    pub peer_tls_ca: Option<PathBuf>,
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
    /// `enable_hinted_handoff:` - when true, writes whose target
    /// peer is in [`crate::cluster::peer::PeerState::Down`] (or
    /// whose outbound channel is closed / full) are stored in a
    /// node-local hint queue and counted toward the consistency
    /// threshold; a background drainer ships the hints to the
    /// peer once it returns to
    /// [`crate::cluster::peer::PeerState::Normal`]. When false
    /// (the default) the dispatcher behaviour is unchanged: a
    /// Down or unreachable target is silently skipped and the
    /// request fails with `DynomiteNoQuorumAchieved` if the
    /// remaining targets cannot satisfy the consistency level.
    #[serde(default)]
    pub enable_hinted_handoff: Option<bool>,
    /// `hint_ttl_seconds:` - per-hint expiry. Hints older than
    /// this many seconds are dropped during periodic sweeps to
    /// bound the in-memory store. Defaults to 86400 (24 hours).
    /// Ignored when `enable_hinted_handoff` is false.
    #[serde(default)]
    pub hint_ttl_seconds: Option<u64>,
    /// `hint_store_max_bytes:` - upper bound on the in-memory
    /// hint store. Once the store reaches this many bytes,
    /// further enqueues fail with
    /// [`crate::cluster::hints::HintStoreError::OverCapacity`]
    /// and the dispatcher falls back to its non-handoff error
    /// path. Defaults to 64 MiB. Ignored when
    /// `enable_hinted_handoff` is false.
    #[serde(default)]
    pub hint_store_max_bytes: Option<u64>,
    /// `hint_drain_interval_ms:` - period of the background hint
    /// drainer sweep. Defaults to 30000 ms (30 seconds). Ignored
    /// when `enable_hinted_handoff` is false.
    #[serde(default)]
    pub hint_drain_interval_ms: Option<u64>,
    /// `log_format:` - selectable shape for tracing output.
    ///
    /// Accepted values are `default`, `rfc5424`, `rfc3164`, `json`,
    /// and `ndjson` (alias of `json`). When unset, the historical
    /// default text format is used. Parsing is performed at
    /// log-installation time by [`crate::core::log::LogFormat::parse`];
    /// invalid values fail the `dynomited --test-conf` gate.
    pub log_format: Option<String>,

    /// `observability:` - opt-in observability knobs (distributed
    /// tracing OTLP exporter today; the OTLP log appender is
    /// scoped but not yet wired). Absent / null disables every
    /// observability surface, preserving today's silent default.
    /// See [`ObservabilityConfig`].
    #[serde(default)]
    pub observability: Option<ObservabilityConfig>,

    /// `bucket_types:` - per-bucket routing-property bundles.
    ///
    /// Each entry is a [`ConfBucketType`] keyed by name. The
    /// dispatcher extracts the bucket name from the request key
    /// (the prefix before the first `/`; see
    /// [`crate::proto::redis::bucket_name`]) and, when a matching
    /// entry exists, swaps in that type's `read_consistency`,
    /// `write_consistency`, and `n_val` for the lifetime of that
    /// request. Pool-level fields stay the fallback for keys
    /// without a slash, for keys whose bucket prefix is unknown,
    /// and for any field the bucket-type stanza leaves at its
    /// default. Empty list disables the feature; entries must have
    /// unique names.
    #[serde(default)]
    pub bucket_types: Vec<ConfBucketType>,

    /// `default_bucket_type:` - name of the bucket type to apply
    /// when the request key has no slash (or has an empty prefix).
    /// Must reference an entry of `bucket_types` when set; unset
    /// (the default) falls all the way back to pool-level
    /// consistency.
    #[serde(default)]
    pub default_bucket_type: Option<String>,

    /// `riak:` - optional Riak-mode listener / AAE configuration.
    ///
    /// Consumed only when `dynomited` is built with the
    /// `--features riak` Cargo feature: the binary then
    /// instantiates a Protocol Buffers Client (PBC) listener,
    /// an HTTP gateway, and (optionally) the active anti-entropy
    /// scheduler against the supplied addresses. When the field
    /// is absent (or every inner option is unset), the binary
    /// behaves identically to a Redis / Memcache deployment.
    /// The block is parsed unconditionally so YAML files
    /// authored against the Riak-enabled binary still validate
    /// under the default build.
    #[serde(default)]
    pub riak: Option<ConfRiak>,
}

/// Optional Riak-mode listener / AAE knobs.
///
/// Every field is optional. The PBC and HTTP listeners are
/// independent: setting one without the other is supported.
/// When `aae_enabled` is `true` the active anti-entropy
/// scheduler is spawned; the cadence knobs default to the
/// values shipped by `dyn_riak::aae::config`.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ConfRiak;
/// let r = ConfRiak {
///     pbc_listen: Some("127.0.0.1:8087".into()),
///     http_listen: Some("127.0.0.1:8098".into()),
///     aae_enabled: Some(false),
///     aae_full_sweep_interval_seconds: None,
///     aae_segment_interval_seconds: None,
///     tls_cert: None,
///     tls_key: None,
///     tls_ca: None,
///     wasm_modules: None,
/// };
/// assert!(r.validate().is_ok());
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ConfRiak {
    /// Address the Riak Protocol Buffers Client listener binds
    /// to (`host:port`). When unset, the PBC listener is not
    /// started.
    pub pbc_listen: Option<String>,
    /// Address the Riak HTTP gateway listener binds to
    /// (`host:port`). When unset, the HTTP gateway is not
    /// started.
    pub http_listen: Option<String>,
    /// When `true`, the Riak active anti-entropy scheduler is
    /// spawned alongside the listeners. Default: `false`.
    pub aae_enabled: Option<bool>,
    /// Override for the AAE full-sweep cadence, in seconds.
    /// When unset, `dyn_riak::aae::config::DEFAULT_FULL_SWEEP_SECONDS`
    /// (24h) is used.
    pub aae_full_sweep_interval_seconds: Option<u64>,
    /// Override for the AAE per-segment exchange cadence, in
    /// seconds. When unset, `dyn_riak::aae::config::DEFAULT_SEGMENT_SECONDS`
    /// (60s) is used.
    pub aae_segment_interval_seconds: Option<u64>,
    /// `tls_cert:` - PEM certificate path for the Riak PBC and
    /// HTTP listeners. When both `tls_cert` and `tls_key` are
    /// set, both Riak listeners terminate TLS; when both are
    /// absent, both listeners run in plaintext (the historical
    /// behaviour). Setting one without the other is rejected at
    /// validation time.
    #[serde(default)]
    pub tls_cert: Option<PathBuf>,
    /// `tls_key:` - PEM private-key path matching
    /// [`Self::tls_cert`].
    #[serde(default)]
    pub tls_key: Option<PathBuf>,
    /// `tls_ca:` - optional PEM CA bundle for mutual TLS on the
    /// Riak listeners. When set, every inbound client must
    /// present a certificate signed by a CA from this bundle.
    /// When unset, the listeners terminate TLS without
    /// requesting a client certificate.
    #[serde(default)]
    pub tls_ca: Option<PathBuf>,
    /// `wasm_modules:` - optional list of Wasm modules to
    /// register with the MapReduce executor at startup. Each
    /// entry pairs a logical `id` with the on-disk `path` of a
    /// Wasm binary (`.wasm`) or WAT text (`.wat`) file. When
    /// `dynomited` is built with the `wasm` Cargo feature it
    /// loads every entry through
    /// [`dyn_riak::mapreduce::wasm::load_modules_from_config`]
    /// and exposes the resulting store on the executor; without
    /// the feature the field is parsed and validated but the
    /// loader is never called (the runtime returns the typed
    /// `WasmNotImplemented` error if a `Phase::WasmModule` is
    /// submitted).
    ///
    /// Validation: every `id` must be unique and every `path`
    /// must point at an existing file at validation time.
    #[serde(default)]
    pub wasm_modules: Option<Vec<ConfRiakWasmModule>>,
}

/// One Wasm module entry inside a [`ConfRiak::wasm_modules`]
/// list.
///
/// # Examples
///
/// ```
/// use std::path::PathBuf;
/// use dynomite::conf::ConfRiakWasmModule;
/// let m = ConfRiakWasmModule {
///     id: "identity".into(),
///     path: PathBuf::from("/etc/dynomited/wasm/identity.wasm"),
/// };
/// assert_eq!(m.id, "identity");
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfRiakWasmModule {
    /// Logical module identifier referenced from a
    /// `Phase::WasmModule { module_id }` MapReduce phase.
    pub id: String,
    /// Filesystem path to the Wasm binary (`.wasm`) or WAT
    /// text (`.wat`) file. Read once at startup.
    pub path: PathBuf,
}

impl ConfRiak {
    /// Validate the cross-field invariants of the Riak block.
    ///
    /// # Errors
    /// Returns a [`ConfError::BadServer`] when an address fails
    /// to parse as a `host:port` socket address, when an AAE
    /// cadence is zero, or when `aae_segment_interval_seconds`
    /// exceeds `aae_full_sweep_interval_seconds`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::ConfRiak;
    /// let r = ConfRiak {
    ///     pbc_listen: Some("not-a-socket-addr".into()),
    ///     ..ConfRiak::default()
    /// };
    /// assert!(r.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), ConfError> {
        if let Some(addr) = self.pbc_listen.as_deref() {
            validate_riak_addr("pbc_listen", addr)?;
        }
        if let Some(addr) = self.http_listen.as_deref() {
            validate_riak_addr("http_listen", addr)?;
        }
        if let Some(n) = self.aae_full_sweep_interval_seconds {
            if n == 0 {
                return Err(ConfError::BadServer {
                    field: "aae_full_sweep_interval_seconds",
                    value: n.to_string(),
                    reason: "must be > 0".into(),
                });
            }
        }
        if let Some(n) = self.aae_segment_interval_seconds {
            if n == 0 {
                return Err(ConfError::BadServer {
                    field: "aae_segment_interval_seconds",
                    value: n.to_string(),
                    reason: "must be > 0".into(),
                });
            }
        }
        if let (Some(seg), Some(full)) = (
            self.aae_segment_interval_seconds,
            self.aae_full_sweep_interval_seconds,
        ) {
            if seg > full {
                return Err(ConfError::BadServer {
                    field: "aae_segment_interval_seconds",
                    value: seg.to_string(),
                    reason: format!("must be <= aae_full_sweep_interval_seconds ({full})"),
                });
            }
        }
        validate_tls_pair(
            "tls_cert",
            "tls_key",
            self.tls_cert.as_deref(),
            self.tls_key.as_deref(),
        )?;
        if self.tls_ca.is_some() && self.tls_cert.is_none() {
            return Err(ConfError::BadServer {
                field: "tls_ca",
                value: self
                    .tls_ca
                    .as_ref()
                    .map_or_else(String::new, |p| p.display().to_string()),
                reason: "requires tls_cert and tls_key to also be set".into(),
            });
        }
        if let Some(modules) = self.wasm_modules.as_deref() {
            let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
            for m in modules {
                if m.id.is_empty() {
                    return Err(ConfError::BadServer {
                        field: "wasm_modules.id",
                        value: String::new(),
                        reason: "wasm module id must not be empty".into(),
                    });
                }
                if !seen.insert(m.id.as_str()) {
                    return Err(ConfError::BadServer {
                        field: "wasm_modules.id",
                        value: m.id.clone(),
                        reason: "wasm module ids must be unique".into(),
                    });
                }
                if !m.path.is_file() {
                    return Err(ConfError::BadServer {
                        field: "wasm_modules.path",
                        value: m.path.display().to_string(),
                        reason: format!("wasm module file not found for id '{}'", m.id),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Cross-check a `(cert, key)` TLS pair: both must be `Some` or
/// both must be `None`. The `cert_field` and `key_field` static
/// strings name the YAML keys for the error message.
fn validate_tls_pair(
    cert_field: &'static str,
    key_field: &'static str,
    cert: Option<&std::path::Path>,
    key: Option<&std::path::Path>,
) -> Result<(), ConfError> {
    match (cert, key) {
        (Some(_), Some(_)) | (None, None) => Ok(()),
        (Some(c), None) => Err(ConfError::BadServer {
            field: cert_field,
            value: c.display().to_string(),
            reason: format!(
                "{cert_field} is set but {key_field} is not; both must be set together"
            ),
        }),
        (None, Some(k)) => Err(ConfError::BadServer {
            field: key_field,
            value: k.display().to_string(),
            reason: format!(
                "{key_field} is set but {cert_field} is not; both must be set together"
            ),
        }),
    }
}

fn validate_riak_addr(field: &'static str, value: &str) -> Result<(), ConfError> {
    use std::net::ToSocketAddrs;
    if value.is_empty() {
        return Err(ConfError::BadServer {
            field,
            value: value.to_string(),
            reason: "riak listen address must not be empty".into(),
        });
    }
    // Accept anything that resolves; mirrors how the rest of
    // the engine validates `listen:` strings (parse first,
    // resolve as a fallback).
    if value.parse::<std::net::SocketAddr>().is_ok() {
        return Ok(());
    }
    match value.to_socket_addrs() {
        Ok(mut iter) => {
            if iter.next().is_some() {
                Ok(())
            } else {
                Err(ConfError::BadServer {
                    field,
                    value: value.to_string(),
                    reason: "resolved to no addresses".into(),
                })
            }
        }
        Err(e) => Err(ConfError::BadServer {
            field,
            value: value.to_string(),
            reason: format!("could not resolve: {e}"),
        }),
    }
}

/// Routing-property bundle attached to a key bucket.
///
/// Bucket types let operators give different key classes
/// different SLAs without running multiple pools. Cache-style
/// keys can pin to `DC_ONE` while transactional keys sit on
/// `DC_EACH_SAFE_QUORUM`; the same dynomited binary serves both.
///
/// `n_val` caps the replica fan-out for the lifetime of one
/// request: when the topology offers more replicas than `n_val`,
/// the dispatcher takes the first `n_val` (the existing rack /
/// DC ordering puts preferred replicas first). A value of `0`
/// means "no cap" and is treated identically to omitting the
/// field.
///
/// # Examples
///
/// ```
/// use dynomite::conf::{ConfBucketType, ConsistencyLevel};
/// let bt = ConfBucketType {
///     name: "sessions".into(),
///     read_consistency: "DC_QUORUM".into(),
///     write_consistency: "DC_EACH_SAFE_QUORUM".into(),
///     n_val: 3,
/// };
/// assert_eq!(bt.name, "sessions");
/// assert_eq!(
///     ConsistencyLevel::parse("read_consistency", &bt.read_consistency).unwrap(),
///     ConsistencyLevel::DcQuorum,
/// );
/// assert_eq!(bt.n_val, 3);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConfBucketType {
    /// Bucket name. Compared verbatim against the bytes returned
    /// by [`crate::proto::redis::bucket_name`]; no normalisation
    /// is performed. Names must be unique within a pool and must
    /// not be empty.
    pub name: String,
    /// Read-side consistency level for keys in this bucket.
    /// Stored as a string so the YAML round-trip is
    /// human-readable; parsed via
    /// [`ConsistencyLevel::parse`](crate::conf::ConsistencyLevel::parse)
    /// during validation.
    pub read_consistency: String,
    /// Write-side consistency level for keys in this bucket.
    /// See [`ConfBucketType::read_consistency`].
    pub write_consistency: String,
    /// Replication-factor cap. The dispatcher trims its replica
    /// fan-out to at most this many targets. `0` means "no cap";
    /// any positive value caps the fan-out to the leading
    /// `n_val` peers (rack-local first).
    #[serde(default)]
    pub n_val: u8,
}

impl ConfBucketType {
    /// Parse [`Self::read_consistency`] into the typed enum.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfBucketType, ConsistencyLevel};
    /// let bt = ConfBucketType {
    ///     name: "s".into(),
    ///     read_consistency: "DC_QUORUM".into(),
    ///     write_consistency: "DC_ONE".into(),
    ///     n_val: 0,
    /// };
    /// assert_eq!(bt.read_level().unwrap(), ConsistencyLevel::DcQuorum);
    /// ```
    pub fn read_level(&self) -> Result<ConsistencyLevel, ConfError> {
        ConsistencyLevel::parse("read_consistency", &self.read_consistency)
    }

    /// Parse [`Self::write_consistency`] into the typed enum.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::{ConfBucketType, ConsistencyLevel};
    /// let bt = ConfBucketType {
    ///     name: "s".into(),
    ///     read_consistency: "DC_ONE".into(),
    ///     write_consistency: "DC_SAFE_QUORUM".into(),
    ///     n_val: 0,
    /// };
    /// assert_eq!(bt.write_level().unwrap(), ConsistencyLevel::DcSafeQuorum);
    /// ```
    pub fn write_level(&self) -> Result<ConsistencyLevel, ConfError> {
        ConsistencyLevel::parse("write_consistency", &self.write_consistency)
    }
}

/// Opt-in observability configuration.
///
/// Absent or null disables every observability surface. Covers
/// distributed-tracing today; an OTLP log-appender bridge using
/// the same fields is scoped as a follow-up.
///
/// # Examples
///
/// ```
/// use dynomite::conf::ObservabilityConfig;
/// let cfg = ObservabilityConfig {
///     otlp_traces_endpoint: Some("http://collector:4317".into()),
///     otlp_logs_endpoint: None,
///     service_name: Some("dynomited".into()),
///     traces_sampling: Some(0.1),
/// };
/// assert!(cfg.otlp_traces_endpoint.is_some());
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ObservabilityConfig {
    /// OTLP gRPC endpoint for distributed traces (e.g.
    /// `http://localhost:4317`). When `None` the binary skips
    /// the OTel SDK install entirely; tracing keeps using the
    /// configured `tracing-subscriber` log layer only.
    pub otlp_traces_endpoint: Option<String>,
    /// OTLP gRPC endpoint for log records. When set the binary
    /// installs an `opentelemetry-appender-tracing` bridge
    /// alongside the local log writer. The wiring is scoped as
    /// a follow-up; the field is parsed today so YAML files
    /// authored against the eventual implementation validate.
    pub otlp_logs_endpoint: Option<String>,
    /// Service name attached to every emitted span / log record.
    /// Defaults to `"dynomited"` when unset.
    pub service_name: Option<String>,
    /// Trace sampling ratio in `[0.0, 1.0]`. `1.0` records every
    /// trace, `0.0` records none. Defaults to `1.0` when unset.
    pub traces_sampling: Option<f64>,
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
        if self.recon_interval_seconds.is_none() {
            self.recon_interval_seconds = Some(defaults::RECON_INTERVAL_SECONDS);
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
        self.apply_hinted_handoff_defaults();
    }

    /// Fill in the hinted-handoff knobs. Factored out of
    /// [`Self::apply_defaults`] to keep the parent method under
    /// the project's per-function line budget while still
    /// covering every hinted-handoff key with a default.
    fn apply_hinted_handoff_defaults(&mut self) {
        if self.enable_hinted_handoff.is_none() {
            self.enable_hinted_handoff = Some(defaults::ENABLE_HINTED_HANDOFF);
        }
        if self.hint_ttl_seconds.is_none() {
            self.hint_ttl_seconds = Some(defaults::HINT_TTL_SECONDS);
        }
        if self.hint_store_max_bytes.is_none() {
            self.hint_store_max_bytes = Some(defaults::HINT_STORE_MAX_BYTES);
        }
        if self.hint_drain_interval_ms.is_none() {
            self.hint_drain_interval_ms = Some(defaults::HINT_DRAIN_INTERVAL_MS);
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
            let ds = DataStore::from_int(n)?;
            if ds == DataStore::Noxu {
                self.validate_noxu()?;
            }
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

        if let Some(s) = &self.log_format {
            crate::core::log::LogFormat::parse(s).map_err(|e| ConfError::BadServer {
                field: "log_format",
                value: s.clone(),
                reason: e.to_string(),
            })?;
        }

        self.validate_bucket_types()?;
        self.validate_hinted_handoff()?;
        self.validate_peer_tls()?;
        if let Some(r) = &self.riak {
            r.validate()?;
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

    fn validate_bucket_types(&self) -> Result<(), ConfError> {
        use std::collections::BTreeSet;
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for bt in &self.bucket_types {
            if bt.name.is_empty() {
                return Err(ConfError::BadServer {
                    field: "bucket_types",
                    value: String::new(),
                    reason: "bucket-type name must not be empty".to_string(),
                });
            }
            if !seen.insert(bt.name.as_str()) {
                return Err(ConfError::BadServer {
                    field: "bucket_types",
                    value: bt.name.clone(),
                    reason: "duplicate bucket-type name".to_string(),
                });
            }
            ConsistencyLevel::parse("read_consistency", &bt.read_consistency)?;
            ConsistencyLevel::parse("write_consistency", &bt.write_consistency)?;
        }
        if let Some(name) = &self.default_bucket_type {
            if !self.bucket_types.iter().any(|bt| &bt.name == name) {
                return Err(ConfError::BadServer {
                    field: "default_bucket_type",
                    value: name.clone(),
                    reason: "references an undefined bucket-type name".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Validate the cross-field invariants of the Noxu
    /// datastore selection.
    ///
    /// Selecting `data_store: noxu` is permitted only when the
    /// binary was built with `--features riak`; without it,
    /// `dynomited` cannot construct a `NoxuDatastore` because
    /// the `dyn-riak` crate (which owns the type) is not
    /// linked. The check is gated on a `cfg!(feature = ...)`
    /// expression that the parent crate threads through via
    /// the [`crate::conf::set_noxu_supported`] toggle: the
    /// engine ships with the toggle off, the `dynomited` binary
    /// turns it on under `--features riak`. The toggle is
    /// global because `data_store: noxu` is a build-time
    /// configuration constraint, not a per-pool one.
    ///
    /// `noxu_path:` must be set and non-empty.
    fn validate_noxu(&self) -> Result<(), ConfError> {
        if !crate::conf::is_noxu_supported() {
            return Err(ConfError::BadNoxuConfig(
                "noxu data_store requires dynomited built with --features riak",
            ));
        }
        match self.noxu_path.as_deref() {
            Some(p) if !p.as_os_str().is_empty() => Ok(()),
            _ => Err(ConfError::BadNoxuConfig(
                "data_store: noxu requires a non-empty 'noxu_path:' directive",
            )),
        }
    }

    fn validate_hinted_handoff(&self) -> Result<(), ConfError> {
        if self.enable_hinted_handoff != Some(true) {
            return Ok(());
        }
        if let Some(ttl) = self.hint_ttl_seconds {
            if ttl == 0 {
                return Err(ConfError::BadServer {
                    field: "hint_ttl_seconds",
                    value: ttl.to_string(),
                    reason: "must be a positive number when enable_hinted_handoff is true"
                        .to_string(),
                });
            }
        }
        if let Some(cap) = self.hint_store_max_bytes {
            if cap == 0 {
                return Err(ConfError::BadServer {
                    field: "hint_store_max_bytes",
                    value: cap.to_string(),
                    reason: "must be a positive number when enable_hinted_handoff is true"
                        .to_string(),
                });
            }
        }
        if let Some(period) = self.hint_drain_interval_ms {
            if period == 0 {
                return Err(ConfError::BadServer {
                    field: "hint_drain_interval_ms",
                    value: period.to_string(),
                    reason: "must be a positive number when enable_hinted_handoff is true"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    /// Cross-check the peer-plane TLS knobs.
    ///
    /// `peer_tls_cert` and `peer_tls_key` must both be set or
    /// both be unset. `peer_tls_ca` is independent (it controls
    /// optional mutual TLS) but only meaningful when the cert /
    /// key pair is set.
    fn validate_peer_tls(&self) -> Result<(), ConfError> {
        validate_tls_pair(
            "peer_tls_cert",
            "peer_tls_key",
            self.peer_tls_cert.as_deref(),
            self.peer_tls_key.as_deref(),
        )?;
        if self.peer_tls_ca.is_some() && self.peer_tls_cert.is_none() {
            return Err(ConfError::BadServer {
                field: "peer_tls_ca",
                value: self
                    .peer_tls_ca
                    .as_ref()
                    .map_or_else(String::new, |p| p.display().to_string()),
                reason: "requires peer_tls_cert and peer_tls_key to also be set".into(),
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

/// Custom deserializer for `data_store:` that accepts either the
/// historical integer form (`0`, `1`, `2`) or the textual form
/// (`redis`, `memcache`, `noxu`). Both shapes normalise to the
/// integer code that the rest of the engine consumes.
fn deserialize_data_store<'de, D>(de: D) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};
    use std::fmt;

    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<i64>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a data_store value: integer (0, 1, 2) or string (redis, memcache, noxu)")
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D2: serde::Deserializer<'de>>(
            self,
            de: D2,
        ) -> Result<Self::Value, D2::Error> {
            de.deserialize_any(V)
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            DataStore::from_int(v)
                .map(|d| Some(d.as_int()))
                .map_err(|e| E::custom(e.to_string()))
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            let n = i64::try_from(v).map_err(|_| E::custom("data_store integer overflow"))?;
            self.visit_i64(n)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            DataStore::from_name(v)
                .map(|d| Some(d.as_int()))
                .map_err(|_| {
                    E::custom(format!(
                        "data_store: unknown name '{v}'; expected one of: redis, memcache, noxu"
                    ))
                })
        }

        fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }
    }

    de.deserialize_any(V)
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

    /// Lock serialising tests that mutate the process-wide
    /// `NOXU_SUPPORTED` flag. cargo test runs tests on multiple
    /// threads; without serialisation a parallel test can flip
    /// the flag back before the assertion runs.
    static NOXU_FLAG_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn data_store_noxu_requires_riak_feature() {
        let _g = NOXU_FLAG_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Default state: noxu support flag is off; selecting
        // noxu must be rejected with the documented message.
        let prev = crate::conf::is_noxu_supported();
        crate::conf::set_noxu_supported(false);
        let mut p = pool();
        p.data_store = Some(2);
        p.noxu_path = Some("/tmp/test".into());
        p.apply_defaults();
        let err = p.validate("p");
        crate::conf::set_noxu_supported(prev);
        match err {
            Err(ConfError::BadNoxuConfig(msg)) => {
                assert!(msg.contains("--features riak"), "unexpected message: {msg}");
            }
            other => panic!("expected BadNoxuConfig, got {other:?}"),
        }
    }

    #[test]
    fn data_store_noxu_requires_path() {
        let _g = NOXU_FLAG_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = crate::conf::is_noxu_supported();
        crate::conf::set_noxu_supported(true);
        let mut p = pool();
        p.data_store = Some(2);
        p.noxu_path = None;
        p.apply_defaults();
        let err = p.validate("p");
        crate::conf::set_noxu_supported(prev);
        match err {
            Err(ConfError::BadNoxuConfig(msg)) => {
                assert!(msg.contains("noxu_path"), "unexpected message: {msg}");
            }
            other => panic!("expected BadNoxuConfig, got {other:?}"),
        }
    }

    #[test]
    fn data_store_noxu_yaml_round_trip_string_form() {
        // String form `data_store: noxu` and integer form
        // `data_store: 2` both normalise to integer 2 on parse.
        let yaml = r"
listen: 127.0.0.1:8102
servers:
- 127.0.0.1:6379:1
tokens: '0'
data_store: noxu
noxu_path: /tmp/test
";
        let p: ConfPool = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(p.data_store, Some(2));
        assert_eq!(
            p.noxu_path.as_deref(),
            Some(std::path::Path::new("/tmp/test"))
        );
        // Re-emit and re-parse: round-trip is stable.
        let dumped = serde_yaml::to_string(&p).unwrap();
        let p2: ConfPool = serde_yaml::from_str(&dumped).unwrap();
        assert_eq!(p2.data_store, p.data_store);
        assert_eq!(p2.noxu_path, p.noxu_path);
    }

    #[test]
    fn data_store_yaml_int_form_still_works() {
        let yaml = r"
listen: 127.0.0.1:8102
servers:
- 127.0.0.1:6379:1
tokens: '0'
data_store: 2
noxu_path: /tmp/test
";
        let p: ConfPool = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(p.data_store, Some(2));
    }

    #[test]
    fn data_store_string_form_unknown_rejected() {
        let yaml = r"
listen: 127.0.0.1:8102
servers:
- 127.0.0.1:6379:1
tokens: '0'
data_store: postgres
";
        let err = serde_yaml::from_str::<ConfPool>(yaml).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown name") || msg.contains("data_store"),
            "unexpected message: {msg}"
        );
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

    #[test]
    fn log_format_known_values_accepted() {
        for value in ["default", "rfc5424", "rfc3164", "json", "ndjson", "DEFAULT"] {
            let mut p = pool();
            p.log_format = Some(value.to_string());
            p.apply_defaults();
            assert!(p.validate("p").is_ok(), "value {value:?} should validate");
        }
    }

    #[test]
    fn log_format_unknown_rejected() {
        let mut p = pool();
        p.log_format = Some("yaml".to_string());
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(
            matches!(
                err,
                ConfError::BadServer {
                    field: "log_format",
                    ..
                }
            ),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn observability_block_round_trips() {
        let yaml = r"
observability:
  otlp_logs_endpoint: http://collector:4317
  service_name: dynomited
listen: 127.0.0.1:8102
servers:
- 127.0.0.1:6379:1
tokens: '0'
";
        let p: ConfPool = serde_yaml::from_str(yaml).unwrap();
        let obs = p.observability.as_ref().expect("observability set");
        assert_eq!(
            obs.otlp_logs_endpoint.as_deref(),
            Some("http://collector:4317")
        );
        assert_eq!(obs.service_name.as_deref(), Some("dynomited"));
    }

    #[test]
    fn bucket_types_round_trip() {
        let yaml = r"
listen: 127.0.0.1:8102
servers:
- 127.0.0.1:6379:1
tokens: '0'
bucket_types:
- name: hot
  read_consistency: DC_QUORUM
  write_consistency: DC_EACH_SAFE_QUORUM
  n_val: 3
- name: cold
  read_consistency: DC_ONE
  write_consistency: DC_ONE
  n_val: 1
default_bucket_type: cold
";
        let p: ConfPool = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(p.bucket_types.len(), 2);
        assert_eq!(p.bucket_types[0].name, "hot");
        assert_eq!(p.bucket_types[0].n_val, 3);
        assert_eq!(
            p.bucket_types[0].read_level().unwrap(),
            crate::conf::ConsistencyLevel::DcQuorum,
        );
        assert_eq!(p.default_bucket_type.as_deref(), Some("cold"));
        // Re-emit and re-parse: round-trip preserves the data.
        let dumped = serde_yaml::to_string(&p).unwrap();
        let p2: ConfPool = serde_yaml::from_str(&dumped).unwrap();
        assert_eq!(p2.bucket_types, p.bucket_types);
        assert_eq!(p2.default_bucket_type, p.default_bucket_type);
    }

    #[test]
    fn bucket_types_default_is_empty() {
        let mut p = pool();
        p.apply_defaults();
        assert!(p.bucket_types.is_empty());
        assert!(p.default_bucket_type.is_none());
        assert!(p.validate("p").is_ok());
    }

    #[test]
    fn duplicate_bucket_type_name_rejected() {
        let mut p = pool();
        p.bucket_types = vec![
            ConfBucketType {
                name: "a".into(),
                read_consistency: "DC_ONE".into(),
                write_consistency: "DC_ONE".into(),
                n_val: 0,
            },
            ConfBucketType {
                name: "a".into(),
                read_consistency: "DC_ONE".into(),
                write_consistency: "DC_ONE".into(),
                n_val: 0,
            },
        ];
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(
            matches!(
                err,
                ConfError::BadServer {
                    field: "bucket_types",
                    ..
                }
            ),
            "unexpected error: {err:?}",
        );
    }

    #[test]
    fn bucket_type_unknown_consistency_rejected() {
        let mut p = pool();
        p.bucket_types = vec![ConfBucketType {
            name: "a".into(),
            read_consistency: "DC_PURPLE".into(),
            write_consistency: "DC_ONE".into(),
            n_val: 0,
        }];
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(matches!(err, ConfError::BadConsistency { .. }));
    }

    #[test]
    fn unknown_default_bucket_type_rejected() {
        let mut p = pool();
        p.default_bucket_type = Some("missing".into());
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(matches!(
            err,
            ConfError::BadServer {
                field: "default_bucket_type",
                ..
            }
        ));
    }

    #[test]
    fn hinted_handoff_default_off_with_canonical_constants() {
        let mut p = pool();
        p.apply_defaults();
        assert_eq!(p.enable_hinted_handoff, Some(false));
        assert_eq!(p.hint_ttl_seconds, Some(defaults::HINT_TTL_SECONDS));
        assert_eq!(p.hint_store_max_bytes, Some(defaults::HINT_STORE_MAX_BYTES));
        assert_eq!(
            p.hint_drain_interval_ms,
            Some(defaults::HINT_DRAIN_INTERVAL_MS)
        );
        assert!(p.validate("p").is_ok());
    }

    #[test]
    fn hinted_handoff_yaml_round_trip() {
        let yaml = r"
listen: 127.0.0.1:8102
servers:
- 127.0.0.1:6379:1
tokens: '0'
enable_hinted_handoff: true
hint_ttl_seconds: 7200
hint_store_max_bytes: 8388608
hint_drain_interval_ms: 5000
";
        let p: ConfPool = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(p.enable_hinted_handoff, Some(true));
        assert_eq!(p.hint_ttl_seconds, Some(7200));
        assert_eq!(p.hint_store_max_bytes, Some(8_388_608));
        assert_eq!(p.hint_drain_interval_ms, Some(5_000));
        let dumped = serde_yaml::to_string(&p).unwrap();
        let p2: ConfPool = serde_yaml::from_str(&dumped).unwrap();
        assert_eq!(p2.enable_hinted_handoff, p.enable_hinted_handoff);
        assert_eq!(p2.hint_ttl_seconds, p.hint_ttl_seconds);
        assert_eq!(p2.hint_store_max_bytes, p.hint_store_max_bytes);
        assert_eq!(p2.hint_drain_interval_ms, p.hint_drain_interval_ms);
    }

    #[test]
    fn hinted_handoff_zero_ttl_rejected_when_enabled() {
        let mut p = pool();
        p.enable_hinted_handoff = Some(true);
        p.hint_ttl_seconds = Some(0);
        p.apply_defaults();
        // apply_defaults() does NOT overwrite Some(0) with the
        // default; the validator should reject it.
        let err = p.validate("p").unwrap_err();
        assert!(matches!(
            err,
            ConfError::BadServer {
                field: "hint_ttl_seconds",
                ..
            }
        ));
    }

    #[test]
    fn hinted_handoff_zero_max_bytes_rejected_when_enabled() {
        let mut p = pool();
        p.enable_hinted_handoff = Some(true);
        p.hint_store_max_bytes = Some(0);
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(matches!(
            err,
            ConfError::BadServer {
                field: "hint_store_max_bytes",
                ..
            }
        ));
    }

    #[test]
    fn hinted_handoff_zero_values_ignored_when_disabled() {
        // With handoff off, the validator must NOT reject
        // out-of-range values: operators may legitimately leave
        // them at zero with handoff off.
        let mut p = pool();
        p.enable_hinted_handoff = Some(false);
        p.hint_ttl_seconds = Some(0);
        p.hint_store_max_bytes = Some(0);
        p.hint_drain_interval_ms = Some(0);
        p.apply_defaults();
        assert!(p.validate("p").is_ok());
    }

    #[test]
    fn riak_block_validates_when_unset() {
        let mut p = pool();
        p.riak = Some(ConfRiak::default());
        p.apply_defaults();
        assert!(p.validate("p").is_ok());
    }

    #[test]
    fn riak_block_validates_with_addresses() {
        let mut p = pool();
        p.riak = Some(ConfRiak {
            pbc_listen: Some("127.0.0.1:8087".into()),
            http_listen: Some("127.0.0.1:8098".into()),
            ..ConfRiak::default()
        });
        p.apply_defaults();
        assert!(p.validate("p").is_ok());
    }

    #[test]
    fn riak_block_rejects_bad_pbc_addr() {
        let mut p = pool();
        p.riak = Some(ConfRiak {
            pbc_listen: Some(String::new()),
            ..ConfRiak::default()
        });
        p.apply_defaults();
        assert!(matches!(p.validate("p"), Err(ConfError::BadServer { .. })));
    }

    #[test]
    fn riak_block_rejects_segment_above_full_sweep() {
        let mut p = pool();
        p.riak = Some(ConfRiak {
            aae_segment_interval_seconds: Some(120),
            aae_full_sweep_interval_seconds: Some(60),
            ..ConfRiak::default()
        });
        p.apply_defaults();
        assert!(matches!(p.validate("p"), Err(ConfError::BadServer { .. })));
    }

    #[test]
    fn riak_block_round_trips_through_yaml() {
        let yaml = r"
p:
  listen: 127.0.0.1:1
  dyn_listen: 127.0.0.1:2
  tokens: '0'
  servers:
  - 127.0.0.1:3:1
  data_store: 0
  riak:
    pbc_listen: 127.0.0.1:8087
    http_listen: 127.0.0.1:8098
    aae_enabled: true
    aae_full_sweep_interval_seconds: 3600
    aae_segment_interval_seconds: 30
";
        let cfg: std::collections::BTreeMap<String, ConfPool> = serde_yaml::from_str(yaml).unwrap();
        let p = cfg.get("p").unwrap();
        let r = p.riak.as_ref().unwrap();
        assert_eq!(r.pbc_listen.as_deref(), Some("127.0.0.1:8087"));
        assert_eq!(r.http_listen.as_deref(), Some("127.0.0.1:8098"));
        assert_eq!(r.aae_enabled, Some(true));
        assert_eq!(r.aae_full_sweep_interval_seconds, Some(3600));
        assert_eq!(r.aae_segment_interval_seconds, Some(30));
    }

    #[test]
    fn peer_tls_pair_unset_is_ok() {
        let mut p = pool();
        p.apply_defaults();
        assert!(p.validate("p").is_ok(), "plaintext default must validate");
    }

    #[test]
    fn peer_tls_pair_both_set_is_ok() {
        let mut p = pool();
        p.peer_tls_cert = Some(std::path::PathBuf::from("/etc/dynomite/peer.crt"));
        p.peer_tls_key = Some(std::path::PathBuf::from("/etc/dynomite/peer.key"));
        p.apply_defaults();
        assert!(p.validate("p").is_ok());
    }

    #[test]
    fn peer_tls_cert_without_key_rejected() {
        let mut p = pool();
        p.peer_tls_cert = Some(std::path::PathBuf::from("/x.crt"));
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(
            matches!(
                err,
                ConfError::BadServer {
                    field: "peer_tls_cert",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn peer_tls_key_without_cert_rejected() {
        let mut p = pool();
        p.peer_tls_key = Some(std::path::PathBuf::from("/x.key"));
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(
            matches!(
                err,
                ConfError::BadServer {
                    field: "peer_tls_key",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn peer_tls_ca_without_cert_rejected() {
        let mut p = pool();
        p.peer_tls_ca = Some(std::path::PathBuf::from("/x.ca"));
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(
            matches!(
                err,
                ConfError::BadServer {
                    field: "peer_tls_ca",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn riak_tls_cert_without_key_rejected() {
        let mut p = pool();
        p.riak = Some(ConfRiak {
            pbc_listen: Some("127.0.0.1:8087".into()),
            tls_cert: Some(std::path::PathBuf::from("/x.crt")),
            ..ConfRiak::default()
        });
        p.apply_defaults();
        let err = p.validate("p").unwrap_err();
        assert!(
            matches!(
                err,
                ConfError::BadServer {
                    field: "tls_cert",
                    ..
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn riak_tls_pair_both_set_is_ok() {
        let mut p = pool();
        p.riak = Some(ConfRiak {
            pbc_listen: Some("127.0.0.1:8087".into()),
            tls_cert: Some(std::path::PathBuf::from("/x.crt")),
            tls_key: Some(std::path::PathBuf::from("/x.key")),
            ..ConfRiak::default()
        });
        p.apply_defaults();
        assert!(p.validate("p").is_ok());
    }

    #[test]
    fn riak_wasm_modules_yaml_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let m1 = dir.path().join("identity.wasm");
        let m2 = dir.path().join("sum.wasm");
        std::fs::write(&m1, b"\0asm\x01\0\0\0").unwrap();
        std::fs::write(&m2, b"\0asm\x01\0\0\0").unwrap();
        let yaml = format!(
            r"
listen: 127.0.0.1:8102
servers:
- 127.0.0.1:6379:1
tokens: '0'
riak:
  pbc_listen: 127.0.0.1:8087
  wasm_modules:
  - id: identity
    path: {m1}
  - id: sum
    path: {m2}
",
            m1 = m1.display(),
            m2 = m2.display(),
        );
        let p: ConfPool = serde_yaml::from_str(&yaml).unwrap();
        let r = p.riak.as_ref().unwrap();
        let mods = r.wasm_modules.as_ref().unwrap();
        assert_eq!(mods.len(), 2);
        assert_eq!(mods[0].id, "identity");
        assert_eq!(mods[0].path, m1);
        assert_eq!(mods[1].id, "sum");
        assert_eq!(mods[1].path, m2);
        // Round-trip back to YAML and re-parse.
        let dumped = serde_yaml::to_string(&p).unwrap();
        let p2: ConfPool = serde_yaml::from_str(&dumped).unwrap();
        assert_eq!(p2.riak.unwrap().wasm_modules, r.wasm_modules);
    }

    #[test]
    fn riak_wasm_modules_unique_ids_required() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.wasm");
        std::fs::write(&path, b"\0").unwrap();
        let r = ConfRiak {
            wasm_modules: Some(vec![
                ConfRiakWasmModule {
                    id: "m".into(),
                    path: path.clone(),
                },
                ConfRiakWasmModule {
                    id: "m".into(),
                    path: path.clone(),
                },
            ]),
            ..ConfRiak::default()
        };
        let err = r.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfError::BadServer {
                field: "wasm_modules.id",
                ..
            }
        ));
    }

    #[test]
    fn riak_wasm_modules_path_must_exist() {
        let r = ConfRiak {
            wasm_modules: Some(vec![ConfRiakWasmModule {
                id: "missing".into(),
                path: std::path::PathBuf::from("/no/such/path/at/all.wasm"),
            }]),
            ..ConfRiak::default()
        };
        let err = r.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfError::BadServer {
                field: "wasm_modules.path",
                ..
            }
        ));
    }

    #[test]
    fn riak_wasm_modules_empty_id_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.wasm");
        std::fs::write(&path, b"\0").unwrap();
        let r = ConfRiak {
            wasm_modules: Some(vec![ConfRiakWasmModule {
                id: String::new(),
                path,
            }]),
            ..ConfRiak::default()
        };
        let err = r.validate().unwrap_err();
        assert!(matches!(
            err,
            ConfError::BadServer {
                field: "wasm_modules.id",
                ..
            }
        ));
    }
}
