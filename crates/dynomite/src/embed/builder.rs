//! Fluent builder for [`crate::embed::Server`].
//!
//! [`ServerBuilder`] mirrors every YAML-visible field on
//! [`crate::conf::ConfPool`] with a typed setter and adds typed
//! setters for the hook traits that have no YAML form.
//!
//! The builder is owned and chainable; `build()` validates the
//! assembled config (the same [`crate::conf::Config::validate`]
//! used for YAML) and returns a [`crate::embed::Server`].

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use crate::conf::{
    ConfDynSeed, ConfListen, ConfPool, ConfServer, Config, ConsistencyLevel, DataStore, HashType,
    SecureServerOption, Servers, TokenList,
};
use crate::embed::error::EmbedError;
use crate::embed::hooks::{
    CryptoProvider, Datastore, MemoryDatastore, MetricsSink, SeedsProvider, SimpleSeedsProvider,
};
use crate::embed::server::{Server, ServerHooks};

/// Fluent builder for [`Server`].
///
/// Every YAML-visible field on [`ConfPool`] has a setter on
/// `ServerBuilder`; in addition, hook setters accept boxed trait
/// objects to plug custom implementations.
///
/// # Examples
///
/// ```
/// use dynomite::embed::ServerBuilder;
/// use dynomite::conf::DataStore;
/// let server = ServerBuilder::new("dyn_o_mite")
///     .listen("127.0.0.1:18102".parse().unwrap())
///     .dyn_listen("127.0.0.1:18101".parse().unwrap())
///     .data_store(DataStore::Redis)
///     .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
///     .tokens_str("0")
///     .build()
///     .unwrap();
/// drop(server);
/// ```
pub struct ServerBuilder {
    pool_name: String,
    pool: ConfPool,
    hooks: ServerHooks,
    vector_registry: Option<std::sync::Arc<crate::vector::registry::VectorRegistry>>,
}

impl std::fmt::Debug for ServerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerBuilder")
            .field("pool_name", &self.pool_name)
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}

impl Default for ServerBuilder {
    /// Build a `ServerBuilder` for the canonical pool name
    /// `"dyn_o_mite"`. Mirrors the design page's sketch of a
    /// no-arg `Server::builder()` while keeping the typed
    /// constructor (`new(pool_name)`) for ergonomics; the C
    /// reference accepts only one pool per `Config`.
    fn default() -> Self {
        Self::new("dyn_o_mite")
    }
}

impl ServerBuilder {
    /// Build an empty `ServerBuilder` for a pool named `pool_name`.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::embed::ServerBuilder;
    /// let b = ServerBuilder::new("dyn_o_mite");
    /// assert_eq!(b.pool_name(), "dyn_o_mite");
    /// ```
    pub fn new(pool_name: impl Into<String>) -> Self {
        Self {
            pool_name: pool_name.into(),
            pool: ConfPool::default(),
            hooks: ServerHooks::default(),
            vector_registry: None,
        }
    }

    /// Build from an existing parsed [`Config`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::conf::Config;
    /// use dynomite::embed::ServerBuilder;
    /// let yaml = "p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0\n";
    /// let cfg = Config::parse_str(yaml).unwrap();
    /// let b = ServerBuilder::from_config(&cfg);
    /// assert_eq!(b.pool_name(), "p");
    /// ```
    pub fn from_config(cfg: &Config) -> Self {
        let pool_name = cfg.pool_name().to_string();
        Self {
            pool_name,
            pool: cfg.pool().clone(),
            hooks: ServerHooks::default(),
            vector_registry: None,
        }
    }

    /// Build by parsing a YAML file.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Write;
    /// use dynomite::embed::ServerBuilder;
    /// let mut f = tempfile::NamedTempFile::new().unwrap();
    /// writeln!(f, "p:\n  listen: 127.0.0.1:1\n  dyn_listen: 127.0.0.1:2\n  tokens: '1'\n  servers:\n  - 127.0.0.1:3:1\n  data_store: 0").unwrap();
    /// let b = ServerBuilder::from_yaml_file(f.path()).unwrap();
    /// assert_eq!(b.pool_name(), "p");
    /// ```
    pub fn from_yaml_file(path: impl AsRef<Path>) -> Result<Self, EmbedError> {
        let cfg = Config::parse_file(path.as_ref())?;
        Ok(Self::from_config(&cfg))
    }

    /// Currently-configured pool name.
    #[must_use]
    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    /// Override the pool name supplied to [`Self::new`].
    ///
    /// The C reference accepts only one pool per `Config`, so the
    /// pool name is part of the constructor for ergonomics. This
    /// setter exists so callers that build a [`ServerBuilder`]
    /// from a default-named template (`"dyn_o_mite"`) can rename
    /// the pool without rebuilding the chain. Recorded as
    /// Deviation in `docs/parity.md` (the design page sketches
    /// a no-arg `Server::builder()`).
    #[must_use]
    pub fn with_pool_name(mut self, name: impl Into<String>) -> Self {
        self.pool_name = name.into();
        self
    }

    // ---- YAML-mirroring setters ------------------------------------------

    /// `listen:` - client-facing listener address.
    ///
    /// Accepts port `0` (kernel-assigned ephemeral port). The
    /// post-bind address is reported via
    /// [`ServerHandle::listen_addr`](crate::embed::ServerHandle::listen_addr).
    #[must_use]
    pub fn listen(mut self, addr: SocketAddr) -> Self {
        self.pool.listen = Some(ConfListen::from_socket_addr(addr));
        self
    }

    /// `dyn_listen:` - peer-facing listener address.
    ///
    /// Accepts port `0` (kernel-assigned ephemeral port). The
    /// post-bind address is reported via
    /// [`ServerHandle::dyn_listen_addr`](crate::embed::ServerHandle::dyn_listen_addr).
    #[must_use]
    pub fn dyn_listen(mut self, addr: SocketAddr) -> Self {
        self.pool.dyn_listen = Some(ConfListen::from_socket_addr(addr));
        self
    }

    /// `stats_listen:` - HTTP stats listener address.
    ///
    /// Accepts port `0` for ephemeral-port semantics. Note that
    /// the embedded server does not currently bind the stats
    /// listener; the `dynomited` binary does. Embedders that want
    /// a Prometheus-style scrape endpoint should plug a
    /// [`MetricsSink`](crate::embed::MetricsSink) implementation
    /// instead.
    #[must_use]
    pub fn stats_listen(mut self, addr: SocketAddr) -> Self {
        self.pool.stats_listen = Some(ConfListen::from_socket_addr(addr));
        self
    }

    /// `hash:` - hash algorithm.
    #[must_use]
    pub fn hash(mut self, h: HashType) -> Self {
        self.pool.hash = Some(h);
        self
    }

    /// `data_store:` - protocol selector.
    #[must_use]
    pub fn data_store(mut self, d: DataStore) -> Self {
        self.pool.data_store = Some(d.as_int());
        self
    }

    /// `read_consistency:` - quorum policy for reads.
    #[must_use]
    pub fn read_consistency(mut self, c: ConsistencyLevel) -> Self {
        self.pool.read_consistency = Some(c.as_str().to_string());
        self
    }

    /// `write_consistency:` - quorum policy for writes.
    #[must_use]
    pub fn write_consistency(mut self, c: ConsistencyLevel) -> Self {
        self.pool.write_consistency = Some(c.as_str().to_string());
        self
    }

    /// `secure_server_option:` - inter-node TLS mode.
    #[must_use]
    pub fn secure_server_option(mut self, opt: SecureServerOption) -> Self {
        self.pool.secure_server_option = Some(opt.as_str().to_string());
        self
    }

    /// `pem_key_file:` - path to the PEM private key.
    #[must_use]
    pub fn pem_key_file(mut self, path: impl AsRef<Path>) -> Self {
        self.pool.pem_key_file = Some(path.as_ref().to_string_lossy().into_owned());
        self
    }

    /// `servers:` - the (single-element) datastore list.
    #[must_use]
    pub fn servers(mut self, servers: Vec<ConfServer>) -> Self {
        self.pool.servers = Some(Servers::from_vec(servers));
        self
    }

    /// `dyn_seeds:` - peer dynomite nodes.
    #[must_use]
    pub fn dyn_seeds(mut self, seeds: Vec<ConfDynSeed>) -> Self {
        self.pool.dyn_seeds = Some(seeds);
        self
    }

    /// `dyn_seed_provider:` - seeds backend selector.
    #[must_use]
    pub fn dyn_seed_provider(mut self, name: impl Into<String>) -> Self {
        self.pool.dyn_seed_provider = Some(name.into());
        self
    }

    /// `timeout:` - request timeout.
    #[must_use]
    pub fn timeout(mut self, d: Duration) -> Self {
        let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.pool.timeout = Some(ms);
        self
    }

    /// `auto_eject_hosts:` - automatically eject failing peers.
    #[must_use]
    pub fn auto_eject_hosts(mut self, on: bool) -> Self {
        self.pool.auto_eject_hosts = Some(on);
        self
    }

    /// `server_failure_limit:` - consecutive failures before eject.
    #[must_use]
    pub fn server_failure_limit(mut self, n: u32) -> Self {
        self.pool.server_failure_limit = Some(i64::from(n));
        self
    }

    /// `server_retry_timeout:` - retry interval for ejected servers.
    #[must_use]
    pub fn server_retry_timeout(mut self, d: Duration) -> Self {
        let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.pool.server_retry_timeout = Some(ms);
        self
    }

    /// `gos_interval:` - gossip period.
    #[must_use]
    pub fn gossip_interval(mut self, d: Duration) -> Self {
        let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.pool.gos_interval = Some(ms);
        self
    }

    /// `enable_gossip:` - turn the gossip task on or off.
    #[must_use]
    pub fn enable_gossip(mut self, on: bool) -> Self {
        self.pool.enable_gossip = Some(on);
        self
    }

    /// `datacenter:` - this node's datacenter.
    #[must_use]
    pub fn datacenter(mut self, dc: impl Into<String>) -> Self {
        self.pool.datacenter = Some(dc.into());
        self
    }

    /// `rack:` - this node's rack.
    #[must_use]
    pub fn rack(mut self, rack: impl Into<String>) -> Self {
        self.pool.rack = Some(rack.into());
        self
    }

    /// `tokens:` - this node's tokens, parsed from the YAML
    /// representation.
    ///
    /// Returns the builder unchanged when the tokens string fails
    /// to parse so callers can chain. Parse failures emit a
    /// `tracing::warn!` event so a typo in the token string is
    /// observable in logs even though it does not break the
    /// chain. Use [`ServerBuilder::tokens`] to set a pre-parsed
    /// [`TokenList`] when you want a hard error on bad input.
    ///
    /// The silent-ignore-with-log behaviour is recorded as a
    /// SemVer-major candidate in `docs/parity.md`; v0.2 may
    /// switch to a `Result`-returning variant.
    #[must_use]
    pub fn tokens_str(mut self, raw: impl AsRef<str>) -> Self {
        let raw = raw.as_ref();
        match TokenList::parse(raw) {
            Ok(t) => {
                self.pool.tokens = Some(t);
            }
            Err(e) => {
                tracing::warn!(
                    raw = %raw,
                    error = %e,
                    "ServerBuilder::tokens_str: parse failed; leaving tokens unchanged. \
                     Use ServerBuilder::tokens(TokenList) for a hard error on bad input."
                );
            }
        }
        self
    }

    /// `tokens:` - this node's tokens.
    #[must_use]
    pub fn tokens(mut self, tokens: TokenList) -> Self {
        self.pool.tokens = Some(tokens);
        self
    }

    /// `mbuf_size:` - mbuf chunk size in bytes.
    #[must_use]
    pub fn mbuf_size(mut self, n: usize) -> Self {
        self.pool.mbuf_size = Some(i64::try_from(n).unwrap_or(i64::MAX));
        self
    }

    /// `max_msgs:` - maximum allocated messages.
    #[must_use]
    pub fn max_msgs(mut self, n: usize) -> Self {
        self.pool.max_msgs = Some(i64::try_from(n).unwrap_or(i64::MAX));
        self
    }

    /// `read_repairs_enabled:` - enable read-repair on quorum
    /// mismatch.
    #[must_use]
    pub fn read_repairs_enabled(mut self, on: bool) -> Self {
        self.pool.read_repairs_enabled = Some(on);
        self
    }

    /// `client_connections:` - client connection cap.
    #[must_use]
    pub fn client_connections(mut self, n: u32) -> Self {
        self.pool.client_connections = Some(i64::from(n));
        self
    }

    /// `datastore_connections:` - count of datastore-side
    /// connections.
    #[must_use]
    pub fn datastore_connections(mut self, n: u8) -> Self {
        self.pool.datastore_connections = Some(n);
        self
    }

    /// `local_peer_connections:` - count of local-DC peer
    /// connections.
    #[must_use]
    pub fn local_peer_connections(mut self, n: u8) -> Self {
        self.pool.local_peer_connections = Some(n);
        self
    }

    /// `remote_peer_connections:` - count of remote-DC peer
    /// connections.
    #[must_use]
    pub fn remote_peer_connections(mut self, n: u8) -> Self {
        self.pool.remote_peer_connections = Some(n);
        self
    }

    /// `preconnect:` - eagerly establish connections at startup.
    #[must_use]
    pub fn preconnect(mut self, on: bool) -> Self {
        self.pool.preconnect = Some(on);
        self
    }

    /// `stats_interval:` - stats aggregation period.
    #[must_use]
    pub fn stats_interval(mut self, d: Duration) -> Self {
        let ms = i64::try_from(d.as_millis()).unwrap_or(i64::MAX);
        self.pool.stats_interval = Some(ms);
        self
    }

    // ---- Hook setters ---------------------------------------------------

    /// Plug a custom [`Datastore`].
    #[must_use]
    pub fn datastore(mut self, ds: Box<dyn Datastore>) -> Self {
        self.hooks.datastore = Some(ds);
        self
    }

    /// Plug a custom [`SeedsProvider`].
    #[must_use]
    pub fn seeds_provider(mut self, sp: Box<dyn SeedsProvider>) -> Self {
        self.hooks.seeds = Some(sp);
        self
    }

    /// Plug a custom [`CryptoProvider`].
    #[must_use]
    pub fn crypto_provider(mut self, cp: Box<dyn CryptoProvider>) -> Self {
        self.hooks.crypto = Some(cp);
        self
    }

    /// Plug a custom [`MetricsSink`].
    #[must_use]
    pub fn metrics_sink(mut self, ms: Box<dyn MetricsSink>) -> Self {
        self.hooks.metrics = Some(ms);
        self
    }

    /// Plug a shared [`crate::vector::registry::VectorRegistry`].
    ///
    /// Without this setter the builder installs a fresh,
    /// per-server registry on `build()`. Embedders that want to
    /// drive multiple servers against a single shared catalog of
    /// indexes (mirroring, fan-out testing, ...) can pass an
    /// `Arc` they constructed elsewhere; the same registry will
    /// then be reachable through
    /// [`crate::embed::ServerHandle::vector_registry`] on every
    /// server built with this builder.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use dynomite::embed::ServerBuilder;
    /// use dynomite::vector::registry::VectorRegistry;
    /// let registry = Arc::new(VectorRegistry::new());
    /// let _b = ServerBuilder::new("p").with_vector_registry(registry);
    /// ```
    #[must_use]
    pub fn with_vector_registry(
        mut self,
        registry: std::sync::Arc<crate::vector::registry::VectorRegistry>,
    ) -> Self {
        self.vector_registry = Some(registry);
        self
    }

    /// Borrow the configured [`crate::vector::registry::VectorRegistry`],
    /// if one was supplied via
    /// [`Self::with_vector_registry`]. Returns `None` when the
    /// default-allocated registry will be installed by
    /// [`Self::build`].
    #[must_use]
    pub fn vector_registry(
        &self,
    ) -> Option<&std::sync::Arc<crate::vector::registry::VectorRegistry>> {
        self.vector_registry.as_ref()
    }

    // `conf_pool_mut` removed: the escape hatch leaked the
    // entire internal `ConfPool` shape onto the public API.
    // Targeted setters land as the missing fields are
    // identified.

    /// Build the [`Server`].
    ///
    /// Applies defaults to any unset field, runs the validation
    /// pass, fills in any missing hooks with their in-crate
    /// defaults, and constructs the runtime data structures
    /// (cluster pool, dispatcher, stats, event bus). The returned
    /// `Server` is not running yet; call
    /// [`Server::start`](crate::embed::Server::start) to spawn
    /// background tasks.
    pub fn build(mut self) -> Result<Server, EmbedError> {
        if self.pool.servers.is_none() {
            // Provide a sentinel so validation passes for the
            // common in-process case where the embedder plugs a
            // custom Datastore.
            self.pool.servers = Some(Servers::from_vec(vec![ConfServer::parse(
                "127.0.0.1:1:1 stub",
            )
            .map_err(EmbedError::Conf)?]));
        }
        if self.pool.tokens.is_none() {
            self.pool.tokens = Some(TokenList::parse("0").map_err(EmbedError::Conf)?);
        }
        // Apply defaults + validate directly on the pool.
        let mut finalized = self.pool.clone();
        finalized.apply_defaults();
        finalized.validate(&self.pool_name)?;

        let datastore = self
            .hooks
            .datastore
            .unwrap_or_else(|| Box::new(MemoryDatastore::new()));
        let seeds = self.hooks.seeds.unwrap_or_else(|| {
            let raw = finalized
                .dyn_seeds
                .as_deref()
                .map(<[_]>::to_vec)
                .unwrap_or_default();
            Box::new(SimpleSeedsProvider::new(raw))
        });
        let crypto = self.hooks.crypto;
        let metrics = self.hooks.metrics;
        let vector_registry = self
            .vector_registry
            .unwrap_or_else(|| std::sync::Arc::new(crate::vector::registry::VectorRegistry::new()));

        Ok(Server::from_pool(
            self.pool_name,
            finalized,
            ServerHooks {
                datastore: Some(datastore),
                seeds: Some(seeds),
                crypto,
                metrics,
            },
            vector_registry,
        ))
    }
}

// `socket_to_listen` was the YAML-validating helper for the
// `listen:` / `dyn_listen:` / `stats_listen:` setters. The
// builder now calls `ConfListen::from_socket_addr` directly so
// that port `0` (kernel-assigned ephemeral) round-trips
// unchanged through the embed API.

// (synthetic_config helper removed; ConfPool drives validation directly.)

/// Internal convenience: container for hook overrides.
impl Default for ServerHooks {
    fn default() -> Self {
        Self {
            datastore: None,
            seeds: None,
            crypto: None,
            metrics: None,
        }
    }
}

// `SharedDatastore` removed: it was a `#[doc(hidden)]` `pub`
// alias with no in-tree consumer. Embedders that need a shared
// handle build their own `Arc<dyn Datastore>` from a
// `Box<dyn Datastore>` they construct.
