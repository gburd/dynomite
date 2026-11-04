//! Server pool owner.
//!
//! [`ServerPool`] is the cluster-level container that holds the
//! local datastore endpoint, the dnode listener configuration, the
//! datacenter and rack tables, the per-peer connection pools, the
//! consistency settings, and the response-manager constructors used
//! by [`crate::cluster::dispatch::ClusterDispatcher`]. It holds the
//! server-pool data shape and is the entry point that the
//! `dynomited` binary and the embedding API construct from a parsed
//! [`crate::conf::Config`].

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use crate::cluster::datacenter::Datacenter;
use crate::cluster::peer::Peer;
use crate::conf::{
    ConfPool, ConsistencyLevel as ConfConsistencyLevel, DataStore, Distribution, HashType,
};

use crate::msg::ConsistencyLevel;
use crate::net::auto_eject::AutoEject;

/// Convert a YAML-form consistency string from a bucket-type
/// stanza into the runtime [`ConsistencyLevel`] enum.
///
/// Values that fail validation fall back to `DcOne`; the
/// configuration validator runs first, so by the time this is
/// called every string is already known to be one of the four
/// canonical names (case-insensitive).
fn bucket_type_consistency(raw: &str) -> ConsistencyLevel {
    match ConfConsistencyLevel::parse("bucket_type_consistency", raw) {
        Ok(ConfConsistencyLevel::DcQuorum) => ConsistencyLevel::DcQuorum,
        Ok(ConfConsistencyLevel::DcSafeQuorum) => ConsistencyLevel::DcSafeQuorum,
        Ok(ConfConsistencyLevel::DcEachSafeQuorum) => ConsistencyLevel::DcEachSafeQuorum,
        Ok(ConfConsistencyLevel::DcOne) | Err(_) => ConsistencyLevel::DcOne,
    }
}

/// Resolved bucket-type bundle stored on the live
/// [`PoolConfig`].
///
/// Mirrors [`crate::conf::ConfBucketType`] but with the
/// consistency strings already parsed into
/// [`ConsistencyLevel`] and the field-name semantics finalised
/// for the dispatcher's hot path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketType {
    /// Bucket name. Compared verbatim against the prefix
    /// returned by [`crate::proto::redis::bucket_name`].
    pub name: String,
    /// Read consistency override.
    pub read_consistency: ConsistencyLevel,
    /// Write consistency override.
    pub write_consistency: ConsistencyLevel,
    /// Replica fan-out cap. `0` means no cap.
    pub n_val: u8,
}

/// Minimal projection of the YAML pool block consumed by the
/// cluster runtime.
///
/// Carries the fields copied from
/// `ConfPool` into the runtime pool during initialization.
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Pool name.
    pub name: String,
    /// Local datacenter name.
    pub dc: String,
    /// Local rack name.
    pub rack: String,
    /// Backing datastore protocol.
    pub data_store: DataStore,
    /// Hash function used for token ring lookups.
    pub hash: HashType,
    /// Distribution algorithm used to map a hashed key to a
    /// peer. Defaults to [`Distribution::Vnode`].
    pub distribution: Distribution,
    /// Optional shadow distribution. When `Some`, the
    /// dispatcher computes both the live and shadow routes for
    /// every request and bumps a counter when they disagree.
    /// The actual route is the configured
    /// [`Self::distribution`].
    pub distribution_shadow: Option<Distribution>,
    /// Read consistency level.
    pub read_consistency: ConsistencyLevel,
    /// Write consistency level.
    pub write_consistency: ConsistencyLevel,
    /// Operation timeout in milliseconds.
    pub timeout_ms: u64,
    /// Eject window (`server_retry_timeout_ms`).
    pub server_retry_timeout_ms: u64,
    /// Consecutive-failure threshold.
    pub server_failure_limit: u32,
    /// Honor `auto_eject_hosts`.
    pub auto_eject_hosts: bool,
    /// Whether gossip is enabled (`enable_gossip`).
    pub enable_gossip: bool,
    /// Per-bucket routing-property bundles. Empty when the
    /// `bucket_types:` YAML stanza is unset.
    pub bucket_types: Vec<BucketType>,
    /// Name of the bucket type to apply when the request key has
    /// no slash. References an entry of `bucket_types`; `None`
    /// keeps the pool-level defaults.
    pub default_bucket_type: Option<String>,
    /// Hinted-handoff feature flag. When `true`, writes targeted
    /// at peers in [`crate::cluster::peer::PeerState::Down`] (or
    /// at peers whose outbound channel is closed / full) are
    /// stored as hints in the node-local
    /// [`crate::cluster::hints::HintStore`] and counted toward
    /// the request's consistency threshold. The default `false`
    /// preserves the legacy behaviour where Down targets are
    /// silently skipped.
    pub enable_hinted_handoff: bool,
    /// Hint expiry, in seconds. Hints older than this are
    /// dropped during the periodic sweep so the in-memory store
    /// stays bounded.
    pub hint_ttl_seconds: u64,
    /// Upper bound on the in-memory hint store. Once the store
    /// holds this many bytes, further enqueues fail and the
    /// dispatcher falls back to its non-handoff error path.
    pub hint_store_max_bytes: u64,
    /// Hint drainer cadence, in milliseconds. Setting this to
    /// zero is rejected by the configuration validator when
    /// `enable_hinted_handoff` is true.
    pub hint_drain_interval_ms: u64,
    /// Optional directory for the durable hinted-handoff backend.
    /// When `Some` (and `enable_hinted_handoff` is true), the
    /// hint store persists one segment file per peer here and
    /// replays them at startup. `None` keeps the store RAM-only.
    pub hint_dir: Option<std::path::PathBuf>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            name: "p".into(),
            dc: "localdc".into(),
            rack: "localrack".into(),
            data_store: DataStore::Valkey,
            hash: HashType::Murmur,
            distribution: Distribution::Vnode,
            distribution_shadow: None,
            read_consistency: ConsistencyLevel::DcOne,
            write_consistency: ConsistencyLevel::DcOne,
            timeout_ms: 5_000,
            server_retry_timeout_ms: 30_000,
            server_failure_limit: 2,
            auto_eject_hosts: false,
            enable_gossip: false,
            bucket_types: Vec::new(),
            default_bucket_type: None,
            enable_hinted_handoff: false,
            hint_ttl_seconds: 86_400,
            hint_store_max_bytes: 64 * 1024 * 1024,
            hint_drain_interval_ms: 30_000,
            hint_dir: None,
        }
    }
}

impl PoolConfig {
    /// Look up a bucket type by name.
    ///
    /// Returns the matching [`BucketType`] when one is
    /// configured. The dispatcher consults this on every request
    /// to swap in per-bucket consistency / fan-out settings.
    #[must_use]
    pub fn lookup_bucket_type(&self, name: &[u8]) -> Option<&BucketType> {
        self.bucket_types
            .iter()
            .find(|bt| bt.name.as_bytes() == name)
    }

    /// Resolve the bucket type that applies to a request whose
    /// extracted bucket name is `bucket`. `None` falls back to
    /// `default_bucket_type` (also possibly `None`).
    #[must_use]
    pub fn resolve_bucket_type(&self, bucket: Option<&[u8]>) -> Option<&BucketType> {
        if let Some(b) = bucket {
            if let Some(bt) = self.lookup_bucket_type(b) {
                return Some(bt);
            }
        }
        // Either no bucket prefix on the key, or the prefix did
        // not match any configured type. Fall through to the
        // default bucket type when one is named.
        let name = self.default_bucket_type.as_deref()?;
        self.lookup_bucket_type(name.as_bytes())
    }
}

impl PoolConfig {
    /// Construct a `PoolConfig` from a [`ConfPool`] block. Fields
    /// missing from the YAML are filled with defaults (the
    /// caller is expected to have called
    /// [`crate::conf::Config::finalize`] before this point).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::PoolConfig;
    /// use dynomite::conf::Config;
    /// let mut cfg = Config::parse_str(
    ///     "p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n",
    /// ).unwrap();
    /// cfg.finalize();
    /// let pc = PoolConfig::from_conf("p", cfg.pool());
    /// assert_eq!(pc.name, "p");
    /// ```
    #[must_use]
    pub fn from_conf(name: &str, pool: &ConfPool) -> Self {
        let parse_consistency = |s: &Option<String>| {
            s.as_deref()
                .and_then(ConsistencyLevel::from_name)
                .unwrap_or(ConsistencyLevel::DcOne)
        };
        let data_store = match pool.data_store {
            Some(1) => DataStore::Memcache,
            Some(2) => DataStore::Dyniak,
            _ => DataStore::Valkey,
        };
        Self {
            name: name.to_string(),
            dc: pool.datacenter.clone().unwrap_or_else(|| "localdc".into()),
            rack: pool.rack.clone().unwrap_or_else(|| "localrack".into()),
            data_store,
            hash: pool.hash.unwrap_or(HashType::Murmur),
            distribution: pool.resolved_distribution(),
            distribution_shadow: pool.distribution_shadow,
            read_consistency: parse_consistency(&pool.read_consistency),
            write_consistency: parse_consistency(&pool.write_consistency),
            timeout_ms: pool
                .timeout
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or(5_000),
            server_retry_timeout_ms: pool
                .server_retry_timeout
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or(30_000),
            server_failure_limit: pool
                .server_failure_limit
                .and_then(|n| u32::try_from(n).ok())
                .unwrap_or(2),
            auto_eject_hosts: pool.auto_eject_hosts.unwrap_or(false),
            enable_gossip: pool.enable_gossip.unwrap_or(false),
            bucket_types: pool
                .bucket_types
                .iter()
                .map(|bt| BucketType {
                    name: bt.name.clone(),
                    read_consistency: bucket_type_consistency(&bt.read_consistency),
                    write_consistency: bucket_type_consistency(&bt.write_consistency),
                    n_val: bt.n_val,
                })
                .collect(),
            default_bucket_type: pool.default_bucket_type.clone(),
            enable_hinted_handoff: pool.enable_hinted_handoff.unwrap_or(false),
            hint_ttl_seconds: pool.hint_ttl_seconds.unwrap_or(86_400),
            hint_store_max_bytes: pool.hint_store_max_bytes.unwrap_or(64 * 1024 * 1024),
            hint_drain_interval_ms: pool.hint_drain_interval_ms.unwrap_or(30_000),
            hint_dir: pool.hint_dir.clone(),
        }
    }
}

/// Cluster-wide owner.
///
/// Holds the topology (datacenters, racks), the peer list (peer
/// index 0 is always the local node, mirroring the reference
/// engine), and the per-peer auto-eject decision state.
///
/// `peers` and `datacenters` live behind `RwLock`s so the
/// dispatcher can hold a read lock while gossip occasionally
/// upgrades to write.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::pool::{PoolConfig, ServerPool};
/// use dynomite::cluster::peer::{Peer, PeerEndpoint};
/// use dynomite::hashkit::DynToken;
/// let cfg = PoolConfig {
///     dc: "dc1".into(),
///     rack: "r1".into(),
///     ..PoolConfig::default()
/// };
/// let local = Peer::new(
///     0, PeerEndpoint::tcp("127.0.0.1".into(), 8101), "r1".into(), "dc1".into(),
///     vec![DynToken::from_u32(1)], true, true, false,
/// );
/// let pool = ServerPool::new(cfg, vec![local]);
/// assert_eq!(pool.peers().read().len(), 1);
/// ```
#[derive(Debug)]
pub struct ServerPool {
    config: PoolConfig,
    peers: Arc<RwLock<Vec<Peer>>>,
    datacenters: Arc<RwLock<Vec<Datacenter>>>,
    auto_eject: Arc<RwLock<Vec<AutoEject>>>,
    read_consistency: AtomicU8,
    write_consistency: AtomicU8,
    timeout_factor: AtomicU8,
    local_state: AtomicU8,
}

/// Lifecycle state of the local node, as reported by the stats
/// endpoint `/state/get_state` and set by `/state/<state>`.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::pool::DynState;
/// assert_eq!(DynState::WritesOnly.name(), "WRITES_ONLY");
/// assert_eq!(DynState::Unknown.name(), "Unknown");
/// ```
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum DynState {
    /// Process is initialising.
    Init = 0,
    /// Node serves neither reads nor writes.
    Standby = 1,
    /// Node accepts writes but drops reads.
    WritesOnly = 2,
    /// Node is coming back from standby.
    Resuming = 3,
    /// Node serves all traffic.
    Normal = 4,
    /// Node is joining the ring (gossip enabled, no peer update yet).
    Joining = 7,
    /// Node is down.
    Down = 8,
    /// Node connections are being reset.
    Reset = 11,
    /// Unknown state.
    Unknown = 12,
}

impl DynState {
    /// Display name used in the `State: <name>` admin reply.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::DynState;
    /// assert_eq!(DynState::Normal.name(), "NORMAL");
    /// ```
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            DynState::Init => "INIT",
            DynState::Standby => "STANDBY",
            DynState::WritesOnly => "WRITES_ONLY",
            DynState::Resuming => "RESUMING",
            DynState::Normal => "NORMAL",
            DynState::Joining => "JOINING",
            DynState::Down => "DOWN",
            DynState::Reset => "RESET",
            DynState::Unknown => "Unknown",
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => DynState::Init,
            1 => DynState::Standby,
            2 => DynState::WritesOnly,
            3 => DynState::Resuming,
            4 => DynState::Normal,
            7 => DynState::Joining,
            8 => DynState::Down,
            11 => DynState::Reset,
            _ => DynState::Unknown,
        }
    }
}

/// Lower bound of the request timeout multiplier.
///
/// # Examples
///
/// ```
/// assert_eq!(dynomite::cluster::pool::TIMEOUT_FACTOR_MIN, 1);
/// ```
pub const TIMEOUT_FACTOR_MIN: u8 = 1;

/// Upper bound of the request timeout multiplier.
///
/// # Examples
///
/// ```
/// assert_eq!(dynomite::cluster::pool::TIMEOUT_FACTOR_MAX, 10);
/// ```
pub const TIMEOUT_FACTOR_MAX: u8 = 10;

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn consistency_from_u8(v: u8) -> ConsistencyLevel {
    match v {
        1 => ConsistencyLevel::DcQuorum,
        2 => ConsistencyLevel::DcSafeQuorum,
        3 => ConsistencyLevel::DcEachSafeQuorum,
        _ => ConsistencyLevel::DcOne,
    }
}

impl ServerPool {
    /// Build a fresh pool from a [`PoolConfig`] and an initial peer
    /// list (peer index 0 is the local node).
    ///
    /// Datacenters and racks are populated automatically from the
    /// supplied peers; their continuum is rebuilt by
    /// [`ServerPool::rebuild_ring`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::hashkit::DynToken;
    /// # use dynomite::conf::{DataStore, HashType};
    /// # use dynomite::msg::ConsistencyLevel;
    /// # let cfg = PoolConfig::default();
    /// # let local = Peer::new(
    /// #    0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    /// #    vec![DynToken::from_u32(0)], true, true, false,
    /// # );
    /// let pool = ServerPool::new(cfg, vec![local]);
    /// pool.rebuild_ring();
    /// assert_eq!(pool.datacenters().read().len(), 1);
    /// ```
    #[must_use]
    pub fn new(config: PoolConfig, peers: Vec<Peer>) -> Self {
        let mut dcs: Vec<Datacenter> = Vec::new();
        for p in &peers {
            let dc_idx = if let Some(i) = dcs.iter().position(|d| d.name() == p.dc()) {
                i
            } else {
                dcs.push(Datacenter::new(p.dc().to_string()));
                dcs.len() - 1
            };
            dcs[dc_idx].upsert_rack(p.rack().to_string());
        }
        let auto_eject_template = AutoEject::new(
            config.auto_eject_hosts,
            config.server_failure_limit,
            Duration::from_millis(config.server_retry_timeout_ms),
        );
        let mut auto_ejects = Vec::with_capacity(peers.len());
        for _ in &peers {
            auto_ejects.push(auto_eject_template.clone());
        }
        // Without gossip the node is serving as soon as it starts;
        // with gossip it stays JOINING until the first peer update.
        let initial_state = if config.enable_gossip {
            DynState::Joining
        } else {
            DynState::Normal
        };
        let pool = Self {
            read_consistency: AtomicU8::new(config.read_consistency as u8),
            write_consistency: AtomicU8::new(config.write_consistency as u8),
            timeout_factor: AtomicU8::new(TIMEOUT_FACTOR_MIN),
            local_state: AtomicU8::new(initial_state as u8),
            config,
            peers: Arc::new(RwLock::new(peers)),
            datacenters: Arc::new(RwLock::new(dcs)),
            auto_eject: Arc::new(RwLock::new(auto_ejects)),
        };
        pool.rebuild_ring();
        pool
    }

    /// Configuration block.
    #[must_use]
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }

    /// Current pool-wide read consistency. Starts at
    /// `read_consistency:` and changes at runtime via
    /// `/set_consistency/read/<level>`. Bucket-type overrides take
    /// precedence at dispatch time.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// use dynomite::msg::ConsistencyLevel;
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// pool.set_read_consistency(ConsistencyLevel::DcQuorum);
    /// assert_eq!(pool.read_consistency(), ConsistencyLevel::DcQuorum);
    /// ```
    #[must_use]
    pub fn read_consistency(&self) -> ConsistencyLevel {
        consistency_from_u8(self.read_consistency.load(Ordering::Relaxed))
    }

    /// Current pool-wide write consistency; see
    /// [`ServerPool::read_consistency`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// use dynomite::msg::ConsistencyLevel;
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// assert_eq!(pool.write_consistency(), ConsistencyLevel::DcOne);
    /// ```
    #[must_use]
    pub fn write_consistency(&self) -> ConsistencyLevel {
        consistency_from_u8(self.write_consistency.load(Ordering::Relaxed))
    }

    /// Replace the pool-wide read consistency.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// use dynomite::msg::ConsistencyLevel;
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// pool.set_read_consistency(ConsistencyLevel::DcSafeQuorum);
    /// assert_eq!(pool.read_consistency(), ConsistencyLevel::DcSafeQuorum);
    /// ```
    pub fn set_read_consistency(&self, level: ConsistencyLevel) {
        self.read_consistency.store(level as u8, Ordering::Relaxed);
    }

    /// Replace the pool-wide write consistency.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// use dynomite::msg::ConsistencyLevel;
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// pool.set_write_consistency(ConsistencyLevel::DcQuorum);
    /// assert_eq!(pool.write_consistency(), ConsistencyLevel::DcQuorum);
    /// ```
    pub fn set_write_consistency(&self, level: ConsistencyLevel) {
        self.write_consistency.store(level as u8, Ordering::Relaxed);
    }

    /// True when read repairs should run: the process-wide flag
    /// ([`crate::msg::is_read_repairs_enabled`]) is set and both the
    /// read and the write consistency are stronger than `DC_ONE`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// // DC_ONE consistency never repairs, whatever the flag says.
    /// assert!(!pool.is_read_repairs_enabled());
    /// ```
    #[must_use]
    pub fn is_read_repairs_enabled(&self) -> bool {
        crate::msg::is_read_repairs_enabled()
            && self.read_consistency() != ConsistencyLevel::DcOne
            && self.write_consistency() != ConsistencyLevel::DcOne
    }

    /// Current request timeout multiplier, in
    /// `TIMEOUT_FACTOR_MIN..=TIMEOUT_FACTOR_MAX`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// assert_eq!(pool.timeout_factor(), 1);
    /// ```
    #[must_use]
    pub fn timeout_factor(&self) -> u8 {
        self.timeout_factor.load(Ordering::Relaxed)
    }

    /// Set the request timeout multiplier, clamped to
    /// `TIMEOUT_FACTOR_MIN..=TIMEOUT_FACTOR_MAX`. Returns the stored
    /// value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// assert_eq!(pool.set_timeout_factor(99), 10);
    /// assert_eq!(pool.set_timeout_factor(0), 1);
    /// ```
    pub fn set_timeout_factor(&self, factor: u8) -> u8 {
        let f = factor.clamp(TIMEOUT_FACTOR_MIN, TIMEOUT_FACTOR_MAX);
        self.timeout_factor.store(f, Ordering::Relaxed);
        f
    }

    /// Timeout, in milliseconds, of a request written to the local
    /// datastore: the pool's `timeout:`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// let cfg = PoolConfig { timeout_ms: 1000, ..PoolConfig::default() };
    /// assert_eq!(ServerPool::new(cfg, Vec::new()).server_timeout(), 1000);
    /// ```
    #[must_use]
    pub fn server_timeout(&self) -> u64 {
        self.config.timeout_ms
    }

    /// Timeout, in milliseconds, of a request forwarded to a peer: the
    /// pool's `timeout:` plus 200 ms for a peer in this datacenter or
    /// 5000 ms for a remote one, plus 20000 ms more for a write so a
    /// write is almost never dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// let cfg = PoolConfig { timeout_ms: 1000, ..PoolConfig::default() };
    /// let pool = ServerPool::new(cfg, Vec::new());
    /// assert_eq!(pool.dnode_peer_timeout(true, true), 1200);
    /// assert_eq!(pool.dnode_peer_timeout(false, false), 26000);
    /// ```
    #[must_use]
    pub fn dnode_peer_timeout(&self, is_same_dc: bool, is_read: bool) -> u64 {
        let mut additional_timeout = if is_same_dc { 200 } else { 5000 };
        if !is_read {
            additional_timeout += 20000;
        }
        self.config.timeout_ms.saturating_add(additional_timeout)
    }

    /// Deadline of `req` dispatched now: to the local datastore when
    /// `peer_dc` is `None`, else to a peer in datacenter `peer_dc`.
    /// `None` when the request expects no reply or the timeout is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// use dynomite::msg::{Msg, MsgType};
    /// let cfg = PoolConfig { dc: "dc1".into(), timeout_ms: 0, ..PoolConfig::default() };
    /// let pool = ServerPool::new(cfg, Vec::new());
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// assert!(pool.request_deadline(&req, None).is_none());
    /// assert!(pool.request_deadline(&req, Some("dc1")).is_some());
    /// ```
    #[must_use]
    pub fn request_deadline(
        &self,
        req: &crate::msg::Msg,
        peer_dc: Option<&str>,
    ) -> Option<tokio::time::Instant> {
        if !req.flags().expect_datastore_reply {
            return None;
        }
        let timeout = match peer_dc {
            None => self.server_timeout(),
            Some(dc) => self.dnode_peer_timeout(dc == self.config.dc, req.flags().is_read),
        };
        crate::msg::message::msg_tmo_insert(
            timeout,
            self.timeout_factor(),
            tokio::time::Instant::now(),
        )
    }

    /// Request timeout in milliseconds: `timeout:` multiplied by the
    /// current timeout factor. A zero `timeout:` means no timeout and
    /// stays zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// let cfg = PoolConfig { timeout_ms: 100, ..PoolConfig::default() };
    /// let pool = ServerPool::new(cfg, Vec::new());
    /// pool.set_timeout_factor(3);
    /// assert_eq!(pool.request_timeout_ms(), 300);
    /// ```
    #[must_use]
    pub fn request_timeout_ms(&self) -> u64 {
        self.config
            .timeout_ms
            .saturating_mul(u64::from(self.timeout_factor()))
    }

    /// Lifecycle state of the local node.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{DynState, PoolConfig, ServerPool};
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// assert_eq!(pool.local_state(), DynState::Normal);
    /// ```
    #[must_use]
    pub fn local_state(&self) -> DynState {
        DynState::from_u8(self.local_state.load(Ordering::Relaxed))
    }

    /// Set the local node state. The local peer entry (index 0)
    /// follows when the state has a peer-state equivalent
    /// (`NORMAL`, `STANDBY`, `JOINING`, `DOWN`, `RESET`).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::pool::{DynState, PoolConfig, ServerPool};
    /// let pool = ServerPool::new(PoolConfig::default(), Vec::new());
    /// pool.set_local_state(DynState::Standby);
    /// assert_eq!(pool.local_state(), DynState::Standby);
    /// ```
    pub fn set_local_state(&self, state: DynState) {
        use crate::cluster::peer::PeerState;
        self.local_state.store(state as u8, Ordering::Relaxed);
        let peer_state = match state {
            DynState::Normal => Some(PeerState::Normal),
            DynState::Standby => Some(PeerState::Standby),
            DynState::Joining => Some(PeerState::Joining),
            DynState::Down => Some(PeerState::Down),
            DynState::Reset => Some(PeerState::Reset),
            _ => None,
        };
        if let (Some(ps), Some(local)) = (peer_state, self.peers.write().first_mut()) {
            local.set_state(ps, now_secs());
        }
    }

    /// Borrow the peer list (RwLock).
    #[must_use]
    pub fn peers(&self) -> &RwLock<Vec<Peer>> {
        &self.peers
    }

    /// Shared `Arc` to the peer list.
    #[must_use]
    pub fn peers_arc(&self) -> Arc<RwLock<Vec<Peer>>> {
        self.peers.clone()
    }

    /// Borrow the datacenter list.
    #[must_use]
    pub fn datacenters(&self) -> &RwLock<Vec<Datacenter>> {
        &self.datacenters
    }

    /// Borrow the per-peer auto-eject deciders.
    #[must_use]
    pub fn auto_eject(&self) -> &RwLock<Vec<AutoEject>> {
        &self.auto_eject
    }

    /// Rebuild the per-rack token continuum from the current peer
    /// table. Mirrors `vnode_update`. When the configured
    /// distribution is
    /// [`crate::conf::Distribution::RandomSlicing`], a
    /// [`crate::hashkit::random_slicing::RandomSlices`] table is
    /// installed on each rack alongside the continuum so the
    /// shadow-distribution path can still walk the vnode view.
    pub fn rebuild_ring(&self) {
        let peers = self.peers.read();
        let mut dcs = self.datacenters.write();
        // Make sure all (dc, rack) pairs exist.
        for p in peers.iter() {
            let dc_idx = if let Some(i) = dcs.iter().position(|d| d.name() == p.dc()) {
                i
            } else {
                dcs.push(Datacenter::new(p.dc().to_string()));
                dcs.len() - 1
            };
            dcs[dc_idx].upsert_rack(p.rack().to_string());
        }
        let entries: Vec<_> = peers
            .iter()
            .map(|p| crate::cluster::vnode::PeerTokens {
                peer_idx: p.idx(),
                dc: p.dc(),
                rack: p.rack(),
                tokens: p.tokens(),
            })
            .collect();
        crate::cluster::vnode::rebuild_continuums(&mut dcs, &entries);
        let live_or_shadow_uses_rs = matches!(
            self.config.distribution,
            crate::conf::Distribution::RandomSlicing
        ) || matches!(
            self.config.distribution_shadow,
            Some(crate::conf::Distribution::RandomSlicing)
        );
        if live_or_shadow_uses_rs {
            Self::install_random_slices(&peers, &mut dcs);
        }
    }

    fn install_random_slices(peers: &[crate::cluster::peer::Peer], dcs: &mut [Datacenter]) {
        use crate::hashkit::random_slicing::RandomSlices;
        for dc in dcs.iter_mut() {
            let dc_name = dc.name().to_string();
            for rack in dc.racks_mut().iter_mut() {
                let mut names: Vec<String> = peers
                    .iter()
                    .filter(|p| p.dc() == dc_name && p.rack() == rack.name())
                    .map(|p| p.endpoint().pname())
                    .collect();
                names.sort();
                names.dedup();
                if names.is_empty() {
                    continue;
                }
                let refs: Vec<&str> = names.iter().map(String::as_str).collect();
                if let Ok(slices) = RandomSlices::from_uniform(&refs) {
                    rack.set_random_slices(slices);
                } else {
                    tracing::warn!(
                        target: "dynomite::cluster::pool",
                        rack = rack.name(),
                        dc = %dc_name,
                        "random-slicing build failed; falling back to vnode for this rack"
                    );
                }
            }
        }
    }

    /// Walk the datacenters and choose, for each remote DC, a rack
    /// for cross-DC replication. Mirrors
    /// `preselect_remote_rack_for_replication`.
    pub fn preselect_remote_racks(&self) {
        let mut dcs = self.datacenters.write();
        for dc in dcs.iter_mut() {
            dc.sort_racks();
        }
        // Find the index of the local rack in the local DC.
        let mut my_rack_index = 0usize;
        for dc in dcs.iter() {
            if dc.name() == self.config.dc {
                if let Some(idx) = dc.rack_idx(&self.config.rack) {
                    my_rack_index = idx;
                }
                break;
            }
        }
        for dc in dcs.iter_mut() {
            if dc.name() == self.config.dc {
                dc.set_preselected_rack_idx(None);
                continue;
            }
            let rack_count = dc.racks().len();
            if rack_count == 0 {
                dc.set_preselected_rack_idx(None);
            } else {
                dc.set_preselected_rack_idx(Some(my_rack_index % rack_count));
            }
        }
    }

    /// Initialise a per-DC [`crate::msg::ResponseMgr`] vector for
    /// the supplied request. The walker visits every datacenter
    /// and produces one manager per DC sized to the rack count.
    /// Mirrors `init_response_mgr_all_dcs`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::hashkit::DynToken;
    /// # use dynomite::conf::{DataStore, HashType};
    /// # use dynomite::msg::{ConsistencyLevel, Msg, MsgType};
    /// # let cfg = PoolConfig::default();
    /// # let local = Peer::new(
    /// #    0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    /// #    vec![DynToken::from_u32(0)], true, true, false,
    /// # );
    /// let pool = ServerPool::new(cfg, vec![local]);
    /// let req = Msg::new(1, MsgType::ReqRedisGet, true);
    /// let mgrs = pool.init_response_mgrs(&req);
    /// assert_eq!(mgrs.len(), 1);
    /// ```
    #[must_use]
    pub fn init_response_mgrs(&self, req: &crate::msg::Msg) -> Vec<crate::msg::ResponseMgr> {
        use crate::msg::{ResponseMgr, MAX_REPLICAS_PER_DC};
        let dcs = self.datacenters.read();
        let mut out = Vec::with_capacity(dcs.len());
        for dc in dcs.iter() {
            let rack_count = dc.racks().len();
            let max_responses = u8::try_from(rack_count.clamp(1, MAX_REPLICAS_PER_DC))
                .unwrap_or(u8::try_from(MAX_REPLICAS_PER_DC).unwrap_or(1));
            out.push(ResponseMgr::new(
                req,
                max_responses,
                Some(dc.name().to_string()),
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::peer::PeerEndpoint;
    use crate::hashkit::DynToken;

    fn cfg(dc: &str, rack: &str) -> PoolConfig {
        PoolConfig {
            dc: dc.into(),
            rack: rack.into(),
            ..PoolConfig::default()
        }
    }

    fn peer(idx: u32, dc: &str, rack: &str, tok: u32, is_local: bool, is_same: bool) -> Peer {
        Peer::new(
            idx,
            PeerEndpoint::tcp("127.0.0.1".into(), 8101 + u16::try_from(idx).unwrap_or(0)),
            rack.into(),
            dc.into(),
            vec![DynToken::from_u32(tok)],
            is_local,
            is_same,
            false,
        )
    }

    #[test]
    fn request_timeouts_follow_target_dc_and_read_write() {
        let pool = ServerPool::new(
            PoolConfig {
                timeout_ms: 1000,
                ..cfg("dc1", "r1")
            },
            Vec::new(),
        );
        assert_eq!(pool.server_timeout(), 1000);
        assert_eq!(pool.dnode_peer_timeout(true, true), 1200);
        assert_eq!(pool.dnode_peer_timeout(true, false), 21200);
        assert_eq!(pool.dnode_peer_timeout(false, true), 6000);
        assert_eq!(pool.dnode_peer_timeout(false, false), 26000);
        // A zero `timeout:` still leaves the peer allowance.
        let zero = ServerPool::new(
            PoolConfig {
                timeout_ms: 0,
                ..cfg("dc1", "r1")
            },
            Vec::new(),
        );
        assert_eq!(zero.server_timeout(), 0);
        assert_eq!(zero.dnode_peer_timeout(true, true), 200);
    }

    #[test]
    fn build_pool_populates_topology() {
        let pool = ServerPool::new(
            cfg("dc1", "r1"),
            vec![
                peer(0, "dc1", "r1", 10, true, true),
                peer(1, "dc1", "r2", 20, false, true),
                peer(2, "dc2", "r1", 30, false, false),
            ],
        );
        let topology = pool.datacenters().read();
        let dc1 = topology.iter().find(|d| d.name() == "dc1").unwrap();
        assert_eq!(dc1.racks().len(), 2);
    }

    #[test]
    fn preselect_remote_picks_per_dc() {
        let pool = ServerPool::new(
            cfg("dc1", "rA"),
            vec![
                peer(0, "dc1", "rA", 10, true, true),
                peer(1, "dc2", "rA", 20, false, false),
                peer(2, "dc2", "rB", 30, false, false),
            ],
        );
        pool.preselect_remote_racks();
        let topology = pool.datacenters().read();
        let dc2 = topology.iter().find(|d| d.name() == "dc2").unwrap();
        // Local rack "rA" is at sorted index 0, dc2 has 2 racks, so
        // preselected idx is 0 -> "rA".
        assert_eq!(
            dc2.preselected_rack()
                .map(super::super::datacenter::Rack::name),
            Some("rA")
        );
    }

    #[test]
    fn init_response_mgrs_one_per_dc() {
        let pool = ServerPool::new(
            cfg("dc1", "r1"),
            vec![
                peer(0, "dc1", "r1", 10, true, true),
                peer(1, "dc2", "r1", 20, false, false),
            ],
        );
        let req = crate::msg::Msg::new(1, crate::msg::MsgType::ReqRedisGet, true);
        let mgrs = pool.init_response_mgrs(&req);
        assert_eq!(mgrs.len(), 2);
    }
}
