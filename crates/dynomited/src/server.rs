//! Async run loop for `dynomited`.
//!
//! [`Server`] is the top-level orchestrator constructed from a
//! validated [`dynomite::conf::Config`]. It mirrors the reference
//! engine's `dn_run` flow:
//!
//! 1. Build the [`ServerPool`] from the configuration's pool block
//!    plus any `dyn_seeds` peers, populating the local node at
//!    index 0.
//! 2. Bind the client-facing [`Proxy`] listener at `pool.listen`.
//! 3. Bind the peer-facing [`DnodeProxy`] listener at
//!    `pool.dyn_listen` whenever `dyn_listen` is set or
//!    `secure_server_option` requires a TLS-capable peer socket.
//! 4. Bind the [`StatsServer`] when `stats_listen` is set.
//! 5. Wrap the pool in a [`ClusterDispatcher`] used by both
//!    listeners.
//!
//! [`Server::run`] then `tokio::select!`s on the listener tasks,
//! the [`SignalSet`], and an internal shutdown
//! [`tokio::sync::watch`] channel; `SIGINT` / `SIGTERM` / a
//! programmatic [`Server::shutdown`] all converge on the same
//! cancel future. `SIGHUP` reopens the log file
//! ([`dynomite::core::log::reopen_on_sighup`]) and, when the server
//! was built with a config path, re-parses that file and applies the
//! reloadable ("value, not structure") pool knobs through the
//! [`crate::reload`] pipeline.
//!
//! Background tasks the run loop spawns:
//!
//! * The gossip task. It runs the periodic `GossipSyn` broadcast
//!   and the per-peer phi evaluation that toggles `PeerState`
//!   between `Normal` and `Down`. It always spawns (even when
//!   `enable_gossip: false`) because no peer would otherwise leave
//!   the initial `Down` state; `enable_gossip` only controls
//!   whether a startup info log is emitted.
//! * The entropy receiver / sender. The run loop spawns the
//!   periodic reconciliation driver
//!   ([`dynomite::entropy::driver::EntropyDriver`]) when
//!   `recon_key_file:` is set in the YAML pool config and the
//!   on-disk key + IV files load successfully. The cadence
//!   defaults to five minutes (configurable via
//!   `recon_interval_seconds:`). When the directive is unset or
//!   the files cannot be read, the run loop emits a single
//!   warning and otherwise stays silent.

use std::fmt;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use dynomite::cluster::dispatch::ClusterDispatcher;
use dynomite::cluster::hints::HintStore;
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::conf::{ConfDynSeed, ConfListen, ConfPool, Config, EndpointKind, Transport};
use dynomite::core::log::reopen_on_sighup;
use dynomite::hashkit::DynToken;
#[cfg(unix)]
use dynomite::io::reactor::UnixTransport;
use dynomite::io::reactor::{ConnRole, TcpTransport, Transport as IoTransport};
use dynomite::net::server::OutboundRequest;
use dynomite::net::tls::SharedTlsProfiles;
use dynomite::net::{Conn, DnodeProxy, DnodeServerConn, NetError, Proxy};
use dynomite::stats::{Snapshot, StatsServer};

use crate::reload::{reload_from_path, ReloadableSnapshot, ReloadableState};
use crate::signals::{SignalEvent, SignalSet};

/// Address of a RESP datastore backend the supervisor dials.
///
/// A backend is configured either as a `host:port` endpoint
/// (dialed over TCP) or as a filesystem path (dialed over a Unix
/// domain socket). The supervisor branches on this enum at connect
/// time and wraps the resulting stream in the matching
/// [`IoTransport`] before handing it to the run loop.
#[derive(Debug, Clone)]
enum BackendAddr {
    /// TCP endpoint resolved from the datastore `host:port`.
    Tcp(SocketAddr),
    /// Unix-domain-socket filesystem path.
    #[cfg(unix)]
    Unix(PathBuf),
}

impl fmt::Display for BackendAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BackendAddr::Tcp(addr) => write!(f, "{addr}"),
            #[cfg(unix)]
            BackendAddr::Unix(path) => write!(f, "unix:{}", path.display()),
        }
    }
}

impl BackendAddr {
    /// Connect to the backend, returning a boxed transport tagged
    /// with [`ConnRole::Server`]. TCP connects set `TCP_NODELAY`;
    /// the Unix path needs no equivalent.
    async fn connect(&self) -> io::Result<Box<dyn IoTransport>> {
        match self {
            BackendAddr::Tcp(addr) => {
                let stream = tokio::net::TcpStream::connect(addr).await?;
                let _ = stream.set_nodelay(true);
                Ok(Box::new(TcpTransport::new(stream, ConnRole::Server)))
            }
            #[cfg(unix)]
            BackendAddr::Unix(path) => {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok(Box::new(UnixTransport::new(stream, ConnRole::Server)))
            }
        }
    }
}

/// Errors produced while building or running a [`Server`].
#[derive(Debug, Error)]
pub enum ServerError {
    /// The configuration is missing a field the run loop requires.
    #[error("server: missing required configuration field '{0}'")]
    MissingConfig(&'static str),
    /// A configuration value the run loop consumes was unparseable.
    #[error("server: configuration field '{field}' is invalid: {reason}")]
    BadConfig {
        /// Field name (matches the YAML key).
        field: &'static str,
        /// Human-readable reason.
        reason: String,
    },
    /// I/O error binding a listener or driving an accept loop.
    #[error("server: io: {0}")]
    Io(#[from] io::Error),
    /// Networking / framing error from the listener tasks.
    #[error("server: net: {0}")]
    Net(#[from] NetError),
    /// A spawned listener task panicked or was cancelled.
    #[error("server: task '{task}' failed: {reason}")]
    TaskFailed {
        /// Logical name of the failing task.
        task: &'static str,
        /// Underlying message.
        reason: String,
    },
    /// Signal stream installation failed.
    #[error("server: install signal handlers: {0}")]
    Signals(io::Error),
}

/// Either-or wrapper around the proxy listener so the
/// [`Server`] can hold a TCP or QUIC listener under one field.
///
/// The TCP variant is built unconditionally; the QUIC variant
/// is feature-gated and only constructed when the pool's
/// `transport: quic` directive is set and the binary was built
/// with the `quic` Cargo feature.
enum ProxyKind {
    /// Plain TCP proxy (the historical default).
    Tcp(Proxy),
    /// QUIC proxy. Constructed only when the engine's `quic`
    /// feature is on and the pool is configured for it.
    #[cfg(feature = "quic")]
    Quic(dynomite::net::QuicProxy),
}

impl ProxyKind {
    async fn run(
        self,
        cancel: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    ) -> Result<(), NetError> {
        match self {
            ProxyKind::Tcp(p) => p.run(cancel).await,
            #[cfg(feature = "quic")]
            ProxyKind::Quic(p) => p.run(cancel).await,
        }
    }
}

/// Build the [`ProxyKind`] for the given listen address.
///
/// Branches on the pool's `transport:` directive: TCP is the
/// default; QUIC is selected when `transport: quic` is set
/// AND the engine was built with the `quic` Cargo feature.
/// When `transport: quic` is requested but the binary lacks the
/// `quic` feature, [`Server::build`] returns a typed
/// [`ServerError::BadConfig`] so the operator sees a clean
/// error at startup rather than a silent fall-back.
#[allow(
    clippy::unused_async,
    reason = "the QUIC arm at line ~187 awaits QuicProxy::bind; clippy only sees \
              the active feature configuration and flags the no-await TCP-only build"
)]
async fn build_proxy(
    listen_addr: SocketAddr,
    dispatcher: Arc<ClusterDispatcher>,
    data_store: dynomite::conf::DataStore,
    conf_pool: &ConfPool,
) -> Result<ProxyKind, ServerError> {
    let transport = conf_pool.transport.unwrap_or_default();
    match transport {
        Transport::Tcp => {
            let p = Proxy::bind(listen_addr, dispatcher)
                .map_err(ServerError::Net)?
                .with_data_store(data_store);
            Ok(ProxyKind::Tcp(p))
        }
        #[cfg(feature = "quic")]
        Transport::Quic => {
            let cert = conf_pool
                .quic_cert_file
                .as_ref()
                .and_then(|p| p.to_str().map(str::to_owned))
                .ok_or(ServerError::BadConfig {
                    field: "quic_cert_file",
                    reason: "transport: quic requires quic_cert_file (UTF-8 path)".into(),
                })?;
            let key = conf_pool
                .quic_key_file
                .as_ref()
                .and_then(|p| p.to_str().map(str::to_owned))
                .ok_or(ServerError::BadConfig {
                    field: "quic_key_file",
                    reason: "transport: quic requires quic_key_file (UTF-8 path)".into(),
                })?;
            let cfg = dynomite::net::QuicConfig::server_with_cert_paths(cert, key);
            let p = dynomite::net::QuicProxy::bind(listen_addr, dispatcher, cfg)
                .await
                .map_err(ServerError::Net)?
                .with_data_store(data_store);
            Ok(ProxyKind::Quic(p))
        }
        #[cfg(not(feature = "quic"))]
        Transport::Quic => Err(ServerError::BadConfig {
            field: "transport",
            reason: "transport: quic requires the engine's `quic` Cargo feature".into(),
        }),
    }
}

/// Top-level Dynomite server.
///
/// Constructed by [`Server::build`] from a validated
/// [`Config`]. Use [`Server::run`] to drive the accept loops and
/// [`Server::shutdown`] (via a clone of the [`ShutdownHandle`]) to
/// trigger graceful teardown.
///
/// # Examples
///
/// ```no_run
/// # use dynomite::conf::Config;
/// # async fn _example(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
/// let server = dynomited::server::Server::build(cfg).await?;
/// let handle = server.shutdown_handle();
/// tokio::spawn(async move {
///     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
///     handle.shutdown();
/// });
/// server.run().await?;
/// # Ok(()) }
/// ```
pub struct Server {
    pool_name: String,
    pool: Arc<ServerPool>,
    dispatcher: Arc<ClusterDispatcher>,
    proxy: Option<ProxyKind>,
    dnode_proxy: Option<DnodeProxy>,
    stats: Option<StatsServer>,
    backend_handle: Option<JoinHandle<Result<(), NetError>>>,
    peer_handles: Vec<JoinHandle<Result<(), NetError>>>,
    listen_addr: SocketAddr,
    dyn_listen_addr: Option<SocketAddr>,
    stats_listen_addr: Option<SocketAddr>,
    enable_gossip: bool,
    /// Optional entropy driver. `Some` when the pool's
    /// `recon_key_file:` is set and the on-disk key + IV files
    /// load successfully at startup. The driver is moved into
    /// the run loop, where it is spawned as a tokio task and
    /// reaped on shutdown.
    entropy_driver: Option<dynomite::entropy::driver::EntropyDriver>,
    /// Path of the recon key file the entropy driver loaded
    /// from, kept around so the run loop can log the source on
    /// startup. `None` when no driver was built.
    entropy_key_path: Option<std::path::PathBuf>,
    /// Optional Riak listener bundle. `Some` exactly when the
    /// pool's `riak:` block configures a PBC or HTTP address.
    /// Owned here so [`Server::run`] can spawn the listeners
    /// alongside the existing client / dnode / stats tasks.
    #[cfg(feature = "riak")]
    riak_handles: Option<crate::riak::RiakHandles>,
    /// Shared gossip handler (peer-state authority + phi-accrual
    /// failure detector). Built unconditionally so the
    /// `dnode_proxy` factory always has somewhere to deliver
    /// inbound gossip frames; the periodic gossip task is
    /// spawned only when `enable_gossip` is set.
    gossip_handler: Arc<dynomite::cluster::gossip::GossipHandler>,
    /// Optional replica-apply sink for a `data_store: dyniak`
    /// pool. `Some` when the pool serves the dyniak surface with
    /// a local object store; the `dnode_proxy` factory attaches
    /// it to every inbound peer `ClientHandler` so
    /// `DmsgType::RiakReplica` frames apply to the local store
    /// (and are never re-forwarded). `None` for non-dyniak
    /// pools, where the type is unrecognised on the wire.
    #[cfg(feature = "riak")]
    replica_sink: Option<Arc<dyn dynomite::net::ReplicaApplySink>>,
    /// Per-peer gossip TX channel: a clone of each peer's
    /// `peer_supervisor` outbound channel, kept here so
    /// `Server::run` can spawn a single gossip task that
    /// fans out heartbeats / shutdowns without holding the
    /// dispatcher's `peer_backends` map locked.
    gossip_peer_txs: Vec<(u32, String, tokio::sync::mpsc::Sender<OutboundRequest>)>,
    /// Local-node pname (`host:port` of the dyn_listen address)
    /// used as the payload of every outbound gossip frame so
    /// receiving peers know who sent it.
    local_pname: String,
    /// Optional hint store. `Some` exactly when the pool's
    /// `enable_hinted_handoff` flag is true; in that case the
    /// `hint_drainer_task` periodically ships pending hints to
    /// peers that have transitioned back to `Normal`.
    hint_store: Option<Arc<HintStore>>,
    /// Period of the hint drainer sweep when `hint_store` is
    /// set. Mirrors the `hint_drain_interval_ms:` YAML key.
    hint_drain_interval: Duration,
    /// Snapshot of the YAML pool block this server was built
    /// from. Used as the diff baseline by the SIGHUP reload
    /// pipeline so it can classify which fields changed and
    /// which are non-reloadable.
    original_conf_pool: ConfPool,
    /// Path of the YAML configuration file this server was
    /// built from. Re-read on `SIGHUP` to drive the
    /// configuration-reload pipeline. `None` keeps the
    /// historical behaviour where `SIGHUP` only reopens the
    /// log file (used by tests that build a [`Server`] from an
    /// in-memory [`Config`]).
    conf_path: Option<PathBuf>,
    /// Shared, reloadable view of "value, not structure" pool
    /// knobs. Updated by the SIGHUP reload pipeline; consumed
    /// by hot loops that need to see the latest values without
    /// a restart.
    reloadable: ReloadableState,
    /// Shared TLS profile cell. The dnode listener and every
    /// peer supervisor read this on every handshake / reconnect
    /// so a SIGHUP-driven cert rotation lands without rebinding
    /// sockets. `Some` when at least one peer-plane TLS profile
    /// is configured at startup; `None` when the peer plane is
    /// plaintext (in which case the cell stays empty and the
    /// reload pipeline has nothing to swap).
    tls_profiles: Option<SharedTlsProfiles>,
    /// Shared failure-cause metrics accumulator. Plumbed into
    /// the dispatcher (which records per-cause dispatch error
    /// counters) and the gossip handler (which records peer
    /// state transitions and phi-score gauges). The run loop
    /// snapshots these into the stats sink on a periodic
    /// timer so the `/stats` and `/metrics` endpoints expose
    /// them.
    failure_metrics: Arc<dynomite::stats::FailureMetrics>,
    /// Mutex-shared stats snapshot the [`StatsServer`] reads
    /// from. The run loop refreshes the failure block of this
    /// snapshot on a one-second timer.
    stats_sink: Arc<Mutex<Snapshot>>,
    /// Process-wide vector index registry shared by the
    /// [`ClusterDispatcher`] and any out-of-band code that wants
    /// to inspect the live FT.* surface (tests, admin tools).
    /// Built fresh on every [`Server::build`] call (when the
    /// `search` feature is enabled); survives for the lifetime
    /// of the [`Server`].
    #[cfg(feature = "search")]
    vector_registry: std::sync::Arc<dynomite_search::VectorRegistry>,
    /// Suggestion-dictionary registry shared with the FT.*
    /// search extension. Held so the run loop can snapshot it
    /// alongside the vector registry when persistence is
    /// configured. `None` when no `search_index_dir:` is set.
    #[cfg(feature = "search")]
    search_suggestions: Option<std::sync::Arc<dynomite_search::SuggestionRegistry>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
}

/// Cheap clonable handle that triggers [`Server::run`] to return.
///
/// Held by callers (signal handlers, embedding test harnesses,
/// supervisor tasks) that need to ask the server to stop without
/// owning the [`Server`] itself.
#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    tx: watch::Sender<bool>,
}

impl ShutdownHandle {
    /// Request graceful shutdown. Idempotent.
    ///
    /// Returns `true` if this call was the first to flip the flag.
    pub fn shutdown(&self) -> bool {
        // `send` only fails when every receiver has been dropped,
        // which means the run loop has already exited. We treat
        // that as a no-op success.
        self.tx.send(true).is_ok()
    }
}

impl Server {
    /// Build a server from a validated [`Config`].
    ///
    /// The configuration is finalised and validated again here so
    /// callers that hold a parsed but un-finalised config still
    /// reach a known-good state. Listeners bind eagerly; bind
    /// failures surface here rather than from the run loop.
    ///
    /// # Errors
    /// [`ServerError`] for configuration or I/O failures.
    #[allow(
        clippy::too_many_lines,
        reason = "Builder threads config validation, listener bind, backend wiring, peer-supervisor spawn, dispatcher construction, and shutdown channel setup. Splitting hides the assembly order which matters for crash-on-construct semantics."
    )]
    pub async fn build(mut config: Config) -> Result<Self, ServerError> {
        config.finalize();
        config.validate().map_err(|e| ServerError::BadConfig {
            field: "<pool>",
            reason: e.to_string(),
        })?;
        let pool_name = config.pool_name().to_string();
        let conf_pool = config.pool().clone();

        let listen = conf_pool
            .listen
            .as_ref()
            .ok_or(ServerError::MissingConfig("listen"))?;
        let listen_addr = listen_to_socket_addr(listen, "listen")?;

        let dyn_listen_addr = match conf_pool.dyn_listen.as_ref() {
            Some(l) => Some(listen_to_socket_addr(l, "dyn_listen")?),
            None => None,
        };
        // Advertised peer address. A node binds `dyn_listen` (often a
        // wildcard like `0.0.0.0:8101` so it accepts connections on
        // every interface, which is required behind NAT / in the
        // cloud), but its gossip identity and the endpoint peers match
        // it by must be a *routable* address, not the wildcard. When
        // the bind IP is unspecified (`0.0.0.0` / `[::]`), the
        // advertised address is taken from `DYN_ADVERTISE_ADDR`
        // (`host` or `host:port`); the port defaults to the
        // `dyn_listen` port. Without the override a wildcard bind
        // would advertise `0.0.0.0:PORT`, which matches no peer's seed
        // entry, so no peer ever leaves `Down` and routing collapses
        // to the local node.
        let advertise_addr = resolve_advertise_addr(dyn_listen_addr);
        let stats_listen_addr = match conf_pool.stats_listen.as_ref() {
            Some(l) => Some(listen_to_socket_addr(l, "stats_listen")?),
            None => None,
        };

        let pool_config = PoolConfig::from_conf(&pool_name, &conf_pool);
        let local_peer = build_local_peer(&conf_pool, &pool_config, advertise_addr)?;
        let mut peers: Vec<Peer> = vec![local_peer];
        if let Some(seeds) = conf_pool.dyn_seeds.as_ref() {
            for (i, seed) in seeds.iter().enumerate() {
                peers.push(seed_to_peer(
                    u32::try_from(i + 1).map_err(|_| ServerError::BadConfig {
                        field: "dyn_seeds",
                        reason: "too many seeds".into(),
                    })?,
                    seed,
                    &pool_config,
                )?);
            }
        }
        let server_pool = Arc::new(ServerPool::new(pool_config.clone(), peers));
        server_pool.preselect_remote_racks();

        // Open the local-datastore connection. The dispatcher
        // routes `LocalDatastore` plans to this `ServerConn`
        // task, which writes request bytes to the backend (redis
        // or memcache) and feeds parsed responses back to the
        // originating client via the per-request responder
        // channel. Without this wiring the proxy parses requests
        // and then drops them on the floor.
        let datastore = conf_pool
            .servers
            .as_ref()
            .and_then(|s| s.entries().first())
            .ok_or(ServerError::MissingConfig("servers"))?;
        let backend_data_store = pool_config.data_store;
        let is_dyniak = backend_data_store == dynomite::conf::DataStore::Dyniak;
        let preconnect = conf_pool.preconnect.unwrap_or(false);
        let backend_capacity =
            usize::from(conf_pool.datastore_connections.unwrap_or(8)).max(1) * 64;
        let (backend_tx, backend_rx) =
            tokio::sync::mpsc::channel::<OutboundRequest>(backend_capacity);

        // A `data_store: dyniak` pool opens an in-process Noxu
        // environment in transactional mode and serves the dyniak
        // Riak PBC / HTTP surface against it. It does NOT dial an
        // external backend and does NOT run a RESP client proxy,
        // so neither a backend supervisor nor a front-end proxy
        // listener is spawned. The shared handle is also handed to
        // the Riak listener wiring below. Noxu's environment lock
        // is exclusive per directory, so it is opened exactly
        // once.
        #[cfg(feature = "riak")]
        let noxu_shared: Option<Arc<dyniak::datastore::NoxuDatastore>> = if is_dyniak {
            let path = conf_pool.noxu_path.clone().ok_or(ServerError::BadConfig {
                field: "noxu_path",
                reason: "data_store: dyniak requires a non-empty 'noxu_path:' directive".into(),
            })?;
            match dyniak::datastore::NoxuDatastore::open_transactional(&path) {
                Ok(ds) => Some(Arc::new(ds)),
                Err(e) => {
                    return Err(ServerError::BadConfig {
                        field: "noxu_path",
                        reason: format!(
                            "could not open transactional Noxu environment at '{}': {e}",
                            path.display()
                        ),
                    });
                }
            }
        } else {
            None
        };

        // Backend supervisor selection. The `valkey` / `memcache`
        // path dials a remote backend over TCP and keeps one
        // `ServerConn` alive against it. The `dyniak` path has no
        // RESP backend, so no supervisor is spawned: its local
        // datastore lives in-process behind the Riak surface.
        let backend_handle: Option<JoinHandle<Result<(), NetError>>> = if is_dyniak {
            #[cfg(not(feature = "riak"))]
            {
                return Err(ServerError::BadConfig {
                    field: "data_store",
                    reason: "dyniak data_store requires dynomited built with --features riak"
                        .into(),
                });
            }
            #[cfg(feature = "riak")]
            {
                // The transactional Noxu environment opened above
                // backs the dyniak surface; drop the unused RESP
                // backend receiver so the channel closes cleanly.
                drop(backend_rx);
                None
            }
        } else {
            let backend_addr: BackendAddr = if datastore.is_unix() {
                #[cfg(unix)]
                {
                    BackendAddr::Unix(PathBuf::from(datastore.host()))
                }
                #[cfg(not(unix))]
                {
                    return Err(ServerError::BadConfig {
                        field: "servers",
                        reason: "unix-socket datastores require a unix target".into(),
                    });
                }
            } else {
                let resolved: SocketAddr = format!("{}:{}", datastore.host(), datastore.port())
                    .parse()
                    .or_else(|_| -> Result<SocketAddr, ServerError> {
                        let mut iter = (datastore.host(), datastore.port())
                            .to_socket_addrs()
                            .map_err(ServerError::Io)?;
                        iter.next().ok_or(ServerError::BadConfig {
                            field: "servers",
                            reason: format!(
                                "could not resolve datastore endpoint '{}:{}'",
                                datastore.host(),
                                datastore.port()
                            ),
                        })
                    })?;
                BackendAddr::Tcp(resolved)
            };
            // Backend supervisor: keeps a single `ServerConn` alive
            // against the configured datastore. It runs in its own
            // task so `build()` does not block on a slow / refused
            // backend; the `preconnect: true` config option still
            // gets respected by attempting one synchronous connect
            // before returning. The supervisor reconnects with
            // exponential-ish backoff on failure so a transient
            // backend restart does not break the proxy permanently.
            if preconnect {
                match tokio::time::timeout(Duration::from_secs(5), backend_addr.connect()).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        return Err(ServerError::BadConfig {
                            field: "servers",
                            reason: format!(
                                "preconnect=true: could not connect to datastore '{backend_addr}': {e}"
                            ),
                        });
                    }
                    Err(_) => {
                        return Err(ServerError::BadConfig {
                            field: "servers",
                            reason: format!(
                                "preconnect=true: connect to datastore '{backend_addr}' timed out after 5s"
                            ),
                        });
                    }
                }
            }
            let backend_requirepass = conf_pool.redis_requirepass.clone();
            Some(tokio::spawn(async move {
                backend_supervisor(
                    backend_addr,
                    backend_rx,
                    backend_data_store,
                    backend_requirepass,
                )
                .await
            }))
        };

        // Spawn one peer supervisor per non-local peer so the
        // dispatcher can fan `Replicas` plans across the
        // cluster. Each supervisor owns one `mpsc::Receiver`,
        // dials the peer's `dyn_listen` (the `endpoint()`
        // address), and drives a `DnodeServerConn` with bounded
        // reconnect backoff. Failures are non-fatal at startup;
        // the supervisor reports them via `tracing::warn!` and
        // keeps trying.
        //
        // The supervisor no longer publishes peer-state
        // transitions; the gossip handler owns `PeerState`
        // authority. The supervisor only owns the TCP / dnode
        // request-response channel for data-plane traffic.
        // Build the dispatcher with the local backend wired.
        // The hint store and the dispatcher share the same
        // `Arc<HintStore>` so the drainer task and the dispatch
        // path operate against the same in-memory queue.
        let hint_store = if pool_config.enable_hinted_handoff {
            let store =
                match pool_config.hint_dir.as_ref() {
                    Some(dir) => HintStore::open(dir, pool_config.hint_store_max_bytes).map_err(
                        |source| ServerError::BadConfig {
                            field: "hint_dir",
                            reason: source.to_string(),
                        },
                    )?,
                    None => HintStore::new(pool_config.hint_store_max_bytes),
                };
            Some(Arc::new(store))
        } else {
            None
        };
        let mut dispatcher = ClusterDispatcher::new(server_pool.clone()).with_backend(backend_tx);
        let failure_metrics = Arc::new(dynomite::stats::FailureMetrics::new());
        dispatcher = dispatcher.with_failure_metrics(failure_metrics.clone());
        if let Some(store) = hint_store.as_ref() {
            dispatcher = dispatcher.with_hint_store(store.clone());
        }
        // Vector index registry. The dispatcher routes RediSearch
        // FT.* commands and the HSET interception path through
        // the `dynomite-search` extension when the binary was
        // built with the `search` feature; otherwise the
        // dispatcher does not recognise FT.* keywords and
        // forwards them to the backend (which typically rejects
        // them with `-ERR unknown command`). One registry is
        // built per dynomited process; share it across
        // components by holding an `Arc` clone (e.g. the
        // [`crate::server::Server`] accessor exposes it via
        // [`Self::vector_registry`]).
        #[cfg(feature = "search")]
        let (vector_registry, search_suggestions) = {
            let suggestions = std::sync::Arc::new(dynomite_search::SuggestionRegistry::new());
            let registry = match conf_pool.search_index_dir.as_ref() {
                Some(dir) => {
                    // Durable mode: reload any prior snapshot
                    // (index schemas, indexed documents, text
                    // fields, suggestions) so a restart does not
                    // drop the FT.* surface.
                    match dynomite_search::VectorRegistry::open(dir.clone(), &suggestions) {
                        Ok(reg) => {
                            tracing::info!(
                                search_index_dir = %dir.display(),
                                indexes = reg.list().len(),
                                "loaded persistent search index snapshot"
                            );
                            std::sync::Arc::new(reg)
                        }
                        Err(e) => {
                            return Err(ServerError::BadConfig {
                                field: "search_index_dir",
                                reason: format!("{}: {e}", dir.display()),
                            });
                        }
                    }
                }
                // In-memory mode: identical to the historical
                // behaviour. Indexes are lost on restart.
                None => std::sync::Arc::new(dynomite_search::VectorRegistry::new()),
            };
            let extension =
                std::sync::Arc::new(dynomite_search::SearchExtension::with_suggestions(
                    std::sync::Arc::clone(&registry),
                    std::sync::Arc::clone(&suggestions),
                ));
            dispatcher = dispatcher.with_command_extension(extension);
            // Only hold the suggestions handle for snapshotting
            // when persistence is on; the in-memory path never
            // touches disk.
            let persisted_suggestions = registry.is_persistent().then_some(suggestions);
            (registry, persisted_suggestions)
        };
        let mut peer_handles: Vec<JoinHandle<Result<(), NetError>>> = Vec::new();
        let mut gossip_peer_txs: Vec<(u32, String, tokio::sync::mpsc::Sender<OutboundRequest>)> =
            Vec::new();
        let peer_channel_capacity = 256usize;
        // Resolve peer-plane TLS knobs once at startup. When
        // both `peer_tls_cert` and `peer_tls_key` are set we build
        // a `TlsAcceptor` for the inbound listener and a
        // `TlsConnector` for outbound peer dials; when both are
        // unset every peer link runs in plaintext, matching the
        // behaviour before this slice.
        let peer_tls = build_peer_tls_runtime(&conf_pool)?;
        for peer in server_pool.peers().read().iter() {
            if peer.is_local() {
                continue;
            }
            let peer_idx = peer.idx();
            let host = peer.endpoint().host().to_string();
            let port = peer.endpoint().port();
            let peer_dc = peer.dc().to_string();
            let peer_pname = peer.endpoint().pname();
            // Resolve once. We log and continue on failure - the
            // supervisor will then sit on the channel and silently
            // discard any forwarded requests until the operator
            // fixes the seed entry.
            let resolved = match (host.as_str(), port).to_socket_addrs() {
                Ok(mut iter) => iter.next(),
                Err(e) => {
                    tracing::warn!(
                        peer = %format!("{host}:{port}"),
                        error = %e,
                        "could not resolve peer endpoint; skipping"
                    );
                    continue;
                }
            };
            let Some(peer_addr) = resolved else {
                tracing::warn!(
                    peer = %format!("{host}:{port}"),
                    "peer endpoint resolved to empty address list; skipping"
                );
                continue;
            };
            let (peer_tx, peer_rx) =
                tokio::sync::mpsc::channel::<OutboundRequest>(peer_channel_capacity);
            dispatcher = dispatcher.with_peer_backend(peer_idx, peer_tx.clone());
            gossip_peer_txs.push((peer_idx, peer_pname.clone(), peer_tx));
            let peer_tls_for_supervisor = peer_tls.clone();
            let peer_dc_for_supervisor = peer_dc.clone();
            let handle: JoinHandle<Result<(), NetError>> = {
                use tracing::Instrument as _;
                let span = tracing::info_span!(
                    "peer_supervisor.spawn",
                    peer_idx,
                    peer = %peer_addr,
                );
                tokio::spawn(
                    async move {
                        peer_supervisor(
                            peer_addr,
                            peer_idx,
                            peer_rx,
                            peer_tls_for_supervisor,
                            peer_dc_for_supervisor,
                        )
                        .await
                    }
                    .instrument(span),
                )
            };
            peer_handles.push(handle);
            tracing::info!(
                peer_idx,
                peer_addr = %peer_addr,
                "spawned peer supervisor"
            );
        }

        let dispatcher = Arc::new(dispatcher);

        // A dyniak pool serves only the Riak PBC / HTTP surface;
        // it does not run a RESP client proxy, so the front-end
        // listener is left unbound.
        let proxy = if is_dyniak {
            None
        } else {
            Some(
                build_proxy(
                    listen_addr,
                    dispatcher.clone(),
                    pool_config.data_store,
                    &conf_pool,
                )
                .await?,
            )
        };

        // Resolve peer-plane TLS knobs once at startup. When
        // both `peer_tls_cert` and `peer_tls_key` are set we build
        // a `TlsAcceptor` for the inbound listener and a
        // `TlsConnector` for outbound peer dials; when both are
        // unset every peer link runs in plaintext, matching the
        // behaviour before this slice.
        // (peer_tls is computed earlier so the supervisor spawn
        // loop can borrow it; we re-attach to the listener here.)

        let dnode_proxy = match dyn_listen_addr {
            Some(addr) => {
                let mut p = DnodeProxy::bind(addr).map_err(ServerError::Net)?;
                if let Some(rt) = peer_tls.as_ref() {
                    p = p.with_tls(rt.acceptor.clone());
                }
                Some(p)
            }
            None => None,
        };

        let stats_sink = Arc::new(Mutex::new(Snapshot::default()));
        let stats = match stats_listen_addr {
            Some(addr) => {
                let ring_pool = server_pool.clone();
                let ring_provider: dynomite::stats::RingProvider = Arc::new(move || {
                    dynomite::admin::cluster_info::gather_ring_from_pool(&ring_pool)
                });
                Some(
                    StatsServer::bind(addr, stats_sink.clone())
                        .map_err(ServerError::Io)?
                        .with_ring_provider(ring_provider),
                )
            }
            None => None,
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Build the gossip handler. The handler is the single
        // mutator of `PeerState` once gossip is wired; it lives
        // for the lifetime of the server so the dnode_proxy
        // factory and the periodic gossip task share the same
        // failure-detector view.
        let gossip_interval_ms = u64::try_from(
            conf_pool
                .gos_interval
                .unwrap_or(default_gossip_interval_ms_i64()),
        )
        .unwrap_or(dynomite::cluster::gossip::DEFAULT_GOSSIP_INTERVAL_MS);
        let gossip_handler = Arc::new(
            dynomite::cluster::gossip::GossipHandler::new(server_pool.clone())
                .with_interval(Duration::from_millis(gossip_interval_ms))
                .with_failure_metrics(failure_metrics.clone()),
        );
        // Membership backend selection. `gossip` (the default) uses
        // the phi-accrual handler constructed above. `swim` is the
        // opt-in SWIM + Lifeguard backend: its pure state machine
        // and I/O shell live in `dynomite::cluster::swim`. The
        // handler produces the same `(peer_idx, PeerState)`
        // transition shape as the gossip handler, so downstream
        // routing is unchanged. Wiring the SWIM probe loop's socket
        // traffic into the run loop is the documented production
        // cutover gap (see docs/journal/2026-07-05-swim-prototype.md);
        // until then the phi-accrual loop remains the live mutator
        // even when `membership: swim` is selected, and the SWIM
        // handler is constructed here so the selection is honoured
        // and observable.
        match conf_pool.membership.unwrap_or_default() {
            dynomite::conf::Membership::Gossip => {}
            dynomite::conf::Membership::Swim => {
                let peer_count = server_pool.peers().read().len();
                let _swim_handler = Arc::new(dynomite::cluster::swim::SwimHandler::new(
                    server_pool.clone(),
                    0,
                    peer_count.max(1),
                    dynomite::cluster::swim::SwimConfig::default(),
                    Duration::from_millis(gossip_interval_ms),
                ));
                tracing::info!(
                    peers = peer_count,
                    period_ms = gossip_interval_ms,
                    "membership backend: SWIM + Lifeguard (prototype; probe loop not yet wired, phi-accrual remains the live detector)"
                );
            }
        }
        let local_pname = advertise_addr.map_or_else(
            || "127.0.0.1:0".to_string(),
            |a| format!("{}:{}", a.ip(), a.port()),
        );

        #[cfg(feature = "riak")]
        let riak_handles = match conf_pool.riak.as_ref() {
            Some(r) => {
                let ds: Arc<dyn dynomite::embed::Datastore> = match noxu_shared.as_ref() {
                    Some(noxu) => noxu.clone(),
                    None => Arc::new(dynomite::embed::MemoryDatastore::new()),
                };
                let handles = crate::riak::build_handles(r, ds).await.map_err(|e| {
                    ServerError::BadConfig {
                        field: "riak",
                        reason: e.to_string(),
                    }
                })?;
                // Wire the shared vector-index registry into the Riak
                // HTTP gateway for `data_store: dyniak` pools so the
                // bucket index-management and search routes resolve
                // against the same registry the RediSearch FT.*
                // surface uses. Only meaningful when the binary is
                // built with the `search` feature; the dyniak object
                // store (noxu) feeds the indexes on write.
                #[cfg(feature = "search")]
                let handles = handles.map(|mut h| {
                    if is_dyniak {
                        h.search_registry = Some(std::sync::Arc::clone(&vector_registry));
                    }
                    h
                });
                // Wire the cross-node replica router for a
                // `data_store: dyniak` pool. The router builds a
                // RingView from the live server pool and fans
                // object writes out over the same per-peer
                // outbound channels the RESP data plane uses
                // (`gossip_peer_txs`), so a PBC put replicates
                // across the ring instead of storing node-local.
                // Skipped when the pool has no non-local peers
                // (a single-node pool needs no fan-out).
                handles.map(|mut h| {
                    if is_dyniak && !gossip_peer_txs.is_empty() {
                        h.hooks = Some(crate::riak::build_routing_hooks(
                            &server_pool,
                            pool_config.hash,
                            &gossip_peer_txs,
                        ));
                    }
                    h
                })
            }
            None => None,
        };

        let (entropy_driver, entropy_key_path) =
            build_entropy_driver(&conf_pool, &server_pool, &pool_name);

        // Build the replica-apply sink for a `data_store: dyniak`
        // pool: inbound `DmsgType::RiakReplica` frames apply to
        // this node's local object store. Built whenever a local
        // noxu store exists (even for a single-node pool) so a
        // node that starts solo and later gains peers already
        // applies forwarded replicas; a non-dyniak pool leaves it
        // `None` and the type is unrecognised on the wire.
        #[cfg(feature = "riak")]
        let replica_sink: Option<Arc<dyn dynomite::net::ReplicaApplySink>> =
            noxu_shared.as_ref().map(|noxu| {
                let ds: Arc<dyn dynomite::embed::Datastore> = noxu.clone();
                Arc::new(dyniak::ReplicaApplier::new(ds))
                    as Arc<dyn dynomite::net::ReplicaApplySink>
            });

        let reloadable = ReloadableState::new(ReloadableSnapshot::from_pool(&conf_pool));
        let tls_profiles = peer_tls.as_ref().map(|rt| rt.profiles.clone());

        Ok(Self {
            pool_name,
            pool: server_pool,
            dispatcher,
            proxy,
            dnode_proxy,
            stats,
            backend_handle,
            peer_handles,
            listen_addr,
            dyn_listen_addr,
            stats_listen_addr,
            enable_gossip: conf_pool.enable_gossip.unwrap_or(false),
            entropy_driver,
            entropy_key_path,
            #[cfg(feature = "riak")]
            riak_handles,
            gossip_handler,
            #[cfg(feature = "riak")]
            replica_sink,
            gossip_peer_txs,
            local_pname,
            hint_store,
            hint_drain_interval: Duration::from_millis(pool_config.hint_drain_interval_ms.max(1)),
            original_conf_pool: conf_pool,
            conf_path: None,
            reloadable,
            tls_profiles,
            failure_metrics,
            stats_sink,
            #[cfg(feature = "search")]
            vector_registry,
            #[cfg(feature = "search")]
            search_suggestions,
            shutdown_tx,
            shutdown_rx,
        })
    }

    /// Borrow the [`dynomite_search::VectorRegistry`] the
    /// dispatcher routes RediSearch FT.* commands and the HSET
    /// interception path through. The handle is cheap to clone;
    /// embedders that drive admin paths out-of-band can hold
    /// one alongside the `Server`.
    ///
    /// Only available when the binary was built with the
    /// `search` Cargo feature (default-on).
    #[cfg(feature = "search")]
    #[must_use]
    pub fn vector_registry(&self) -> std::sync::Arc<dynomite_search::VectorRegistry> {
        std::sync::Arc::clone(&self.vector_registry)
    }

    /// Attach the original YAML configuration file path so the
    /// run loop's `SIGHUP` arm can re-parse and reload it. When
    /// unset (the default), `SIGHUP` reopens the log file but
    /// does not attempt a configuration reload, matching the
    /// pre-reload behaviour used by tests that build a
    /// [`Server`] from an in-memory [`Config`].
    #[must_use]
    pub fn with_conf_path(mut self, path: PathBuf) -> Self {
        self.conf_path = Some(path);
        self
    }

    /// Cheap clonable handle to the runtime's reloadable
    /// configuration cell. Consumers (the dispatcher, the
    /// gossip task, the entropy driver) re-read the cell on
    /// every cycle so a `SIGHUP`-driven update lands without a
    /// restart.
    #[must_use]
    pub fn reloadable(&self) -> ReloadableState {
        self.reloadable.clone()
    }

    /// Cheap clonable handle to the runtime's shared TLS
    /// profile cell. `None` when the peer plane is plaintext;
    /// `Some(_)` when at least one TLS profile is configured at
    /// startup. The dnode listener and every peer supervisor
    /// read this on every handshake / reconnect so a SIGHUP
    /// reload of the cert / key / CA bundle takes effect on the
    /// next new connection.
    #[must_use]
    pub fn tls_profiles(&self) -> Option<SharedTlsProfiles> {
        self.tls_profiles.clone()
    }

    /// Borrow the pool.
    #[must_use]
    pub fn pool(&self) -> &Arc<ServerPool> {
        &self.pool
    }

    /// Borrow the dispatcher.
    #[must_use]
    pub fn dispatcher(&self) -> &Arc<ClusterDispatcher> {
        &self.dispatcher
    }

    /// Local address the client-facing proxy is bound to.
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Local address of the peer-facing proxy when bound.
    #[must_use]
    pub fn dyn_listen_addr(&self) -> Option<SocketAddr> {
        self.dyn_listen_addr
    }

    /// Local address of the stats listener when bound.
    #[must_use]
    pub fn stats_listen_addr(&self) -> Option<SocketAddr> {
        self.stats_listen_addr
    }

    /// `true` when the entropy reconciliation driver was built
    /// at construction time and will be spawned by
    /// [`Server::run`]. `false` when `recon_key_file:` is
    /// unset, the file does not exist on disk, or the AES
    /// material could not be parsed.
    #[must_use]
    pub fn entropy_enabled(&self) -> bool {
        self.entropy_driver.is_some()
    }

    /// Cadence the entropy driver will tick at, when one was
    /// built. `None` when [`Self::entropy_enabled`] is `false`.
    #[must_use]
    pub fn entropy_cadence(&self) -> Option<Duration> {
        self.entropy_driver
            .as_ref()
            .map(dynomite::entropy::driver::EntropyDriver::cadence)
    }

    /// Address the optional Riak PBC listener bound to.
    /// Available only with the `riak` Cargo feature.
    #[cfg(feature = "riak")]
    #[must_use]
    pub fn riak_pbc_addr(&self) -> Option<SocketAddr> {
        self.riak_handles.as_ref().and_then(|h| h.pbc_addr)
    }

    /// Address the optional Riak HTTP gateway bound to.
    /// Available only with the `riak` Cargo feature.
    #[cfg(feature = "riak")]
    #[must_use]
    pub fn riak_http_addr(&self) -> Option<SocketAddr> {
        self.riak_handles.as_ref().and_then(|h| h.http_addr)
    }

    /// Address the optional Riak QUIC PBC listener bound to.
    /// Available only with the `riak` and `quic` Cargo features.
    #[cfg(all(feature = "riak", feature = "quic"))]
    #[must_use]
    pub fn riak_quic_addr(&self) -> Option<SocketAddr> {
        self.riak_handles.as_ref().and_then(|h| h.quic_addr)
    }

    /// Cheap clonable shutdown handle. Surviving copies can request
    /// a stop after [`Server::run`] has been called.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            tx: self.shutdown_tx.clone(),
        }
    }

    /// Trigger graceful shutdown via the same path the signal
    /// handlers use. Idempotent.
    ///
    /// # Errors
    /// Currently infallible; the `Result` is reserved for future
    /// failure modes (e.g. timed-out child tasks).
    pub fn shutdown(&self) -> Result<(), ServerError> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }

    /// Drive the accept loops until shutdown.
    ///
    /// Spawns one tokio task per listener, installs the
    /// [`SignalSet`], and selects on the supervisor loop. Returns
    /// `Ok(())` when every listener has stopped cleanly after a
    /// graceful shutdown; returns [`ServerError::TaskFailed`] when
    /// any listener panics or errors out before shutdown is
    /// requested.
    ///
    /// # Errors
    /// [`ServerError`] for signal-installation, listener, or
    /// task-join failures.
    #[tracing::instrument(
        name = "server.run",
        skip_all,
        fields(
            pool = %self.pool_name,
            listen = %self.listen_addr,
            peers = self.pool.peers().read().len(),
        ),
    )]
    #[allow(
        clippy::too_many_lines,
        reason = "single supervise loop must enumerate every shutdown source (signal, ctrl-c, dispatcher) plus join every spawned task; splitting hides the shutdown ordering invariants"
    )]
    pub async fn run(self) -> Result<(), ServerError> {
        let Self {
            pool_name,
            pool,
            dispatcher,
            proxy,
            dnode_proxy,
            stats,
            backend_handle,
            peer_handles,
            listen_addr,
            dyn_listen_addr,
            stats_listen_addr,
            enable_gossip,
            entropy_driver,
            entropy_key_path,
            #[cfg(feature = "riak")]
            mut riak_handles,
            gossip_handler,
            #[cfg(feature = "riak")]
            replica_sink,
            gossip_peer_txs,
            local_pname,
            hint_store,
            hint_drain_interval,
            original_conf_pool,
            conf_path,
            reloadable,
            tls_profiles,
            failure_metrics,
            stats_sink,
            #[cfg(feature = "search")]
            vector_registry,
            #[cfg(feature = "search")]
            search_suggestions,
            shutdown_tx,
            mut shutdown_rx,
        } = self;

        // Periodic stats sink refresher: every second copies
        // the latest failure-metrics snapshot into the shared
        // sink the StatsServer reads from. The non-failure
        // fields stay at their default values until the
        // engine grows a real `Stats` aggregator (currently
        // out of scope for the binary).
        let stats_refresher: JoinHandle<()> = {
            let metrics = failure_metrics.clone();
            let sink = stats_sink.clone();
            let mut cancel_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(1));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_rx.changed() => {
                            if *cancel_rx.borrow() { return; }
                        }
                        _ = ticker.tick() => {
                            let mut guard = sink.lock();
                            guard.failure = metrics.snapshot();
                        }
                    }
                }
            })
        };

        // Periodic search-index snapshot task. Only spawned
        // when `search_index_dir:` is configured. A full
        // snapshot is written every few seconds and once more
        // on shutdown so a clean stop never loses the latest
        // delta. A SIGKILL between ticks loses at most the
        // un-snapshotted delta; the client/workload tolerates
        // re-creation, and the prior good snapshot survives
        // because writes are atomic (write-temp + rename).
        #[cfg(feature = "search")]
        let search_snapshotter: Option<JoinHandle<()>> = match search_suggestions {
            Some(suggestions) => {
                let registry = vector_registry.clone();
                let mut cancel_rx = shutdown_rx.clone();
                Some(tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(Duration::from_secs(5));
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tokio::select! {
                            biased;
                            _ = cancel_rx.changed() => {
                                if *cancel_rx.borrow() {
                                    // Final save on clean shutdown.
                                    if let Err(e) = registry.save(&suggestions) {
                                        tracing::warn!(error = %e, "search snapshot on shutdown failed");
                                    }
                                    return;
                                }
                            }
                            _ = ticker.tick() => {
                                if let Err(e) = registry.save(&suggestions) {
                                    tracing::warn!(error = %e, "periodic search snapshot failed");
                                }
                            }
                        }
                    }
                }))
            }
            None => None,
        };
        #[cfg(not(feature = "search"))]
        let search_snapshotter: Option<JoinHandle<()>> = None;

        let entropy_handle: Option<JoinHandle<()>> = match entropy_driver {
            Some(driver) => {
                let cadence = driver.cadence();
                let key_path_disp = entropy_key_path
                    .as_deref()
                    .map_or_else(|| "<unknown>".to_string(), |p| p.display().to_string());
                tracing::info!(
                    pool = %pool_name,
                    key_file = %key_path_disp,
                    cadence_seconds = cadence.as_secs(),
                    "entropy reconciliation task started"
                );
                let cancel_rx = shutdown_rx.clone();
                Some(tokio::spawn(async move {
                    driver.run_until_shutdown(cancel_rx).await;
                }))
            }
            None => None,
        };

        tracing::info!(
            pool = %pool_name,
            listen = %listen_addr,
            ?dyn_listen_addr,
            ?stats_listen_addr,
            peers = pool.peers().read().len(),
            "dynomited run loop starting"
        );

        let proxy_handle: Option<JoinHandle<Result<(), NetError>>> = proxy.map(|proxy| {
            let proxy_cancel = cancel_future(shutdown_rx.clone());
            tokio::spawn(async move { proxy.run(proxy_cancel).await })
        });

        let dnode_handle = dnode_proxy.map(|dnode| {
            let dispatcher = dispatcher.clone();
            let cancel = cancel_future(shutdown_rx.clone());
            let gossip_for_factory = gossip_handler.clone();
            #[cfg(feature = "riak")]
            let replica_sink_for_factory = replica_sink.clone();
            tokio::spawn(async move {
                dnode
                    .run(cancel, move |tx| {
                        // The factory is invoked once per accepted
                        // peer; build a fresh `ClientHandler`
                        // bound to the cluster dispatcher and the
                        // per-peer response channel. The gossip
                        // handler is attached so inbound
                        // gossip-class dnode frames feed the
                        // sender peer's failure detector instead
                        // of being routed into the datastore
                        // parser. For a dyniak pool a replica-apply
                        // sink is also attached so inbound
                        // `RiakReplica` frames apply to the local
                        // object store (and never re-forward).
                        let handler = dynomite::net::ClientHandler::new(
                            dispatcher.clone(),
                            tx,
                            dynomite::conf::DataStore::Valkey,
                        )
                        .with_read_timeout(Some(Duration::from_mins(1)))
                        .with_gossip(gossip_for_factory.clone());
                        #[cfg(feature = "riak")]
                        let handler = match replica_sink_for_factory.clone() {
                            Some(sink) => handler.with_replica_sink(sink),
                            None => handler,
                        };
                        handler
                    })
                    .await
            })
        });

        // Spawn the gossip task. The task ticks at the configured
        // interval and: (a) sends a `GossipSyn` on every per-peer
        // outbound channel carrying the local-node pname so the
        // remote can identify the sender, (b) re-evaluates phi
        // for every non-local peer and toggles `PeerState`
        // between `Normal` and `Down`. The task always spawns,
        // even when `enable_gossip` is false in the YAML, because
        // the supervisor no longer publishes peer-state on its
        // own; without the periodic evaluation no peer would
        // ever leave the initial `Down` state.
        let gossip_cancel = cancel_future(shutdown_rx.clone());
        let gossip_task_handle: JoinHandle<()> = {
            let handler = gossip_handler.clone();
            let pname = local_pname.clone();
            let txs = gossip_peer_txs.clone();
            tokio::spawn(async move {
                gossip_task(handler, pname, txs, gossip_cancel).await;
            })
        };
        if enable_gossip {
            tracing::info!(pool = %pool_name, "gossip task spawned");
        }

        // Spawn the hint drainer when hinted handoff is
        // configured. The drainer ticks at
        // `hint_drain_interval`: each tick (a) drops expired
        // hints, and (b) for every peer in `Normal` state ships
        // the pending hints via the same per-peer outbound
        // channels the dispatcher uses.
        let hint_drainer_handle: Option<JoinHandle<()>> = hint_store.as_ref().map(|store| {
            let store = store.clone();
            let pool_for_drainer = pool.clone();
            let txs = gossip_peer_txs.clone();
            let cancel = cancel_future(shutdown_rx.clone());
            let interval = hint_drain_interval;
            tracing::info!(
                pool = %pool_name,
                interval_ms = u64::try_from(interval.as_millis()).unwrap_or(u64::MAX),
                "hint drainer task spawned"
            );
            tokio::spawn(async move {
                hint_drainer_task(store, pool_for_drainer, txs, interval, cancel).await;
            })
        });

        let stats_handle: Option<JoinHandle<io::Result<()>>> = stats.map(|s| {
            let mut cancel_rx = shutdown_rx.clone();
            tokio::spawn(async move {
                tokio::select! {
                    res = s.run() => res,
                    () = wait_for_flag(&mut cancel_rx) => Ok(()),
                }
            })
        });

        // Optional Riak surface (PBC + HTTP + AAE). The handles
        // are bound by `Server::build`; here we just spawn the
        // listener tasks and the AAE scheduler. They share the
        // same shutdown signal as the rest of the run loop.
        #[cfg(feature = "riak")]
        let (riak_pbc_handle, riak_http_handle, riak_quic_handle, riak_aae_handle) =
            match riak_handles.as_mut() {
                Some(h) => {
                    let aae_cfg = h.aae.clone();
                    let (p, http, quic) = crate::riak::spawn_listeners(h, &shutdown_rx);
                    let aae = aae_cfg.map(|cfg| {
                        let txs = gossip_peer_txs.clone();
                        let cancel = shutdown_rx.clone();
                        tracing::info!(
                            pool = %pool_name,
                            full_sweep_seconds = cfg.full_sweep_interval_seconds,
                            segment_seconds = cfg.segment_interval_seconds,
                            "riak aae scheduler spawned"
                        );
                        crate::riak::spawn_aae(cfg, txs, cancel)
                    });
                    if let Some(a) = h.pbc_addr {
                        tracing::info!(pool = %pool_name, addr = %a, "riak pbc listener spawned");
                    }
                    if let Some(a) = h.http_addr {
                        tracing::info!(pool = %pool_name, addr = %a, "riak http gateway spawned");
                    }
                    #[cfg(feature = "quic")]
                    if let Some(a) = h.quic_addr {
                        tracing::info!(pool = %pool_name, addr = %a, "riak pbc quic listener spawned");
                    }
                    (p, http, quic, aae)
                }
                None => (None, None, None, None),
            };

        let mut signals = SignalSet::install().map_err(ServerError::Signals)?;
        let reload_ctx = ReloadContext {
            conf_path: conf_path.as_deref(),
            original_pool: &original_conf_pool,
            reloadable: &reloadable,
            tls_profiles: tls_profiles.as_ref(),
        };
        let supervise_result = supervise(
            &shutdown_tx,
            &mut shutdown_rx,
            &mut signals,
            proxy_handle.as_ref(),
            dnode_handle.as_ref(),
            stats_handle.as_ref(),
            &reload_ctx,
        )
        .await;

        // Whatever the supervisor returns, make sure the listeners
        // see the cancel and the join handles drain.
        let _ = shutdown_tx.send(true);

        // Send GossipShutdown to every peer so they can
        // transition us to Down promptly without waiting for the
        // phi threshold to climb.
        send_gossip_shutdown(&local_pname, &gossip_peer_txs);

        // The backend driver listens to its request-channel sender;
        // dropping the dispatcher (which holds the only sender)
        // when the proxy and dnode listeners exit will close the
        // channel and the driver will return Ok. We still abort
        // here as a belt-and-braces against a stuck backend.
        if let Some(h) = backend_handle {
            h.abort();
            let _ = h.await;
        }
        for h in peer_handles {
            h.abort();
            let _ = h.await;
        }
        gossip_task_handle.abort();
        let _ = gossip_task_handle.await;
        if let Some(h) = hint_drainer_handle {
            h.abort();
            let _ = h.await;
        }
        // Drain the entropy run loop. The driver observes the
        // shutdown flag at the next per-peer boundary; aborting
        // here is belt-and-braces against a stuck transport.
        if let Some(h) = entropy_handle {
            h.abort();
            let _ = h.await;
        }
        // Stop the periodic stats refresher.
        stats_refresher.abort();
        let _ = stats_refresher.await;
        // Let the search snapshotter run its final save, then
        // join it. It observes the shutdown flag, writes one
        // last snapshot, and returns on its own; await rather
        // than abort so the final save is not cut short.
        if let Some(h) = search_snapshotter {
            let _ = h.await;
        }
        #[cfg(feature = "riak")]
        {
            for h in [
                riak_pbc_handle,
                riak_http_handle,
                riak_quic_handle,
                riak_aae_handle,
            ]
            .into_iter()
            .flatten()
            {
                h.abort();
                let _ = h.await;
            }
        }
        // Now that the gossip task is gone, drop the explicit
        // gossip channel handles so reference counts settle.
        drop(gossip_peer_txs);
        drop(gossip_handler);

        let proxy_outcome = if let Some(h) = proxy_handle {
            await_listener("proxy", h).await
        } else {
            Ok(())
        };
        let dnode_outcome = if let Some(h) = dnode_handle {
            await_listener("dnode_proxy", h).await
        } else {
            Ok(())
        };
        let stats_outcome = if let Some(h) = stats_handle {
            await_stats("stats", h).await
        } else {
            Ok(())
        };

        tracing::info!(pool = %pool_name, "dynomited run loop stopped");

        // Surface the first failure from any of the supervisor /
        // listener / stats arms, but always drain every join
        // handle first so we never leak tasks.
        supervise_result?;
        proxy_outcome?;
        dnode_outcome?;
        stats_outcome?;
        Ok(())
    }
}

fn cancel_future(
    rx: watch::Receiver<bool>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    let mut rx = rx;
    Box::pin(async move {
        wait_for_flag(&mut rx).await;
    })
}

async fn wait_for_flag(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Bundle of references the run loop's `SIGHUP` arm needs to
/// drive a configuration reload. Built once on entry to
/// [`Server::run`] and passed to [`supervise`] by reference so
/// nothing inside the supervise loop has to clone.
struct ReloadContext<'a> {
    /// YAML configuration path captured at startup. `None`
    /// disables the reload pipeline; `SIGHUP` then only reopens
    /// the log file.
    conf_path: Option<&'a std::path::Path>,
    /// Pool snapshot taken at startup; the diff baseline.
    original_pool: &'a ConfPool,
    /// Shared reloadable-state cell to write into.
    reloadable: &'a ReloadableState,
    /// Shared TLS profile cell. `None` when the peer plane is
    /// plaintext at startup.
    tls_profiles: Option<&'a SharedTlsProfiles>,
}

async fn supervise(
    shutdown_tx: &watch::Sender<bool>,
    shutdown_rx: &mut watch::Receiver<bool>,
    signals: &mut SignalSet,
    proxy: Option<&JoinHandle<Result<(), NetError>>>,
    dnode: Option<&JoinHandle<Result<(), NetError>>>,
    stats: Option<&JoinHandle<io::Result<()>>>,
    reload: &ReloadContext<'_>,
) -> Result<(), ServerError> {
    loop {
        tokio::select! {
            biased;
            res = shutdown_rx.changed() => {
                if res.is_err() || *shutdown_rx.borrow() {
                    return Ok(());
                }
            }
            ev = signals.recv() => {
                match ev {
                    Some(SignalEvent::Interrupt | SignalEvent::Terminate) => {
                        tracing::info!("shutdown signal received");
                        let _ = shutdown_tx.send(true);
                        return Ok(());
                    }
                    Some(SignalEvent::Hangup) => {
                        // Reopen the log file FIRST so the
                        // reload's INFO line lands in the new
                        // file.
                        if let Err(e) = reopen_on_sighup() {
                            tracing::warn!(error = %e, "log reopen failed");
                        } else {
                            tracing::info!("log reopened on SIGHUP");
                        }
                        handle_sighup_reload(reload);
                    }
                    None => {
                        tracing::warn!("signal stream closed; treating as shutdown");
                        let _ = shutdown_tx.send(true);
                        return Ok(());
                    }
                }
            }
            () = wait_finished_opt(proxy) => {
                tracing::error!("proxy listener exited unexpectedly");
                let _ = shutdown_tx.send(true);
                return Err(ServerError::TaskFailed {
                    task: "proxy",
                    reason: "listener returned before shutdown".into(),
                });
            }
            () = wait_finished_opt(dnode) => {
                tracing::error!("dnode_proxy listener exited unexpectedly");
                let _ = shutdown_tx.send(true);
                return Err(ServerError::TaskFailed {
                    task: "dnode_proxy",
                    reason: "listener returned before shutdown".into(),
                });
            }
            () = wait_finished_io_opt(stats) => {
                tracing::error!("stats listener exited unexpectedly");
                let _ = shutdown_tx.send(true);
                return Err(ServerError::TaskFailed {
                    task: "stats",
                    reason: "listener returned before shutdown".into(),
                });
            }
        }
    }
}

/// Run the SIGHUP-driven configuration reload pipeline.
///
/// Failures are non-fatal: a malformed reload leaves the live
/// configuration intact and surfaces a `tracing::error!` line so
/// the operator can fix the YAML and re-send `SIGHUP`.
fn handle_sighup_reload(reload: &ReloadContext<'_>) {
    let Some(path) = reload.conf_path else {
        tracing::debug!("SIGHUP: no conf_path captured at startup; skipping config reload");
        return;
    };
    // The TLS swap path requires a live `SharedTlsProfiles` cell
    // even when the peer plane started plaintext: in that case
    // the cell is empty and the reload may populate it. Build
    // an empty fallback cell when none was wired so the reload
    // pipeline always sees a target to write into; the result
    // is dropped immediately afterwards.
    let local_fallback;
    let tls = if let Some(t) = reload.tls_profiles {
        t
    } else {
        local_fallback = SharedTlsProfiles::default();
        &local_fallback
    };
    match reload_from_path(path, reload.original_pool, reload.reloadable, tls) {
        Ok(outcome) => {
            tracing::info!(
                reloaded = ?outcome.reloaded,
                non_reloadable = ?outcome.non_reloadable,
                tls_swapped = outcome.tls_swapped,
                "config reloaded; {} reloadable fields updated, {} non-reloadable fields ignored",
                outcome.reloaded.len(),
                outcome.non_reloadable.len(),
            );
        }
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %path.display(),
                "config reload failed; keeping previous configuration",
            );
        }
    }
}

async fn wait_finished(handle: &JoinHandle<Result<(), NetError>>) {
    // Poll until the join handle is finished without consuming
    // it. We need the borrow because the supervisor still wants
    // to await the handle later for its outcome.
    while !handle.is_finished() {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Send `AUTH <password>` to a freshly-connected Redis backend
/// and read the first reply line. Returns `Ok(())` on `+OK`,
/// `Err(NetError)` on any of: I/O failure, connection close,
/// timeout, or a non-`+OK` reply (e.g. `-ERR invalid password`).
///
/// The handshake is intentionally tiny: we read byte-by-byte
/// until the first CRLF so we never over-consume into the
/// run-loop's own read buffer. Bounded by `timeout`.
async fn redis_auth_handshake<S>(
    stream: &mut S,
    password: &str,
    timeout: Duration,
) -> Result<(), NetError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + ?Sized,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cmd = format!(
        "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
        password.len(),
        password
    );
    let write = async { stream.write_all(cmd.as_bytes()).await.map_err(NetError::Io) };
    tokio::time::timeout(timeout, write)
        .await
        .map_err(|_| NetError::Parse("AUTH write timeout".into()))??;

    // Read the reply: first byte indicates the type.
    // `+OK\r\n` is the only success; anything else (including
    // `-...\r\n` errors) is a failure.
    let mut buf = Vec::with_capacity(64);
    let mut byte = [0u8; 1];
    let read = async {
        loop {
            match stream.read(&mut byte).await {
                Ok(0) => return Err(NetError::Closed),
                Ok(_) => {
                    buf.push(byte[0]);
                    if buf.len() >= 2 && buf.ends_with(b"\r\n") {
                        return Ok(());
                    }
                    if buf.len() > 1024 {
                        return Err(NetError::Parse(
                            "AUTH reply exceeded 1KiB without CRLF".into(),
                        ));
                    }
                }
                Err(e) => return Err(NetError::Io(e)),
            }
        }
    };
    tokio::time::timeout(timeout, read)
        .await
        .map_err(|_| NetError::Parse("AUTH read timeout".into()))??;
    if buf.starts_with(b"+OK\r\n") {
        Ok(())
    } else {
        let msg = String::from_utf8_lossy(buf.trim_ascii_end()).to_string();
        Err(NetError::Auth(msg))
    }
}

/// Long-running supervisor that owns the request channel for the
/// local datastore. Reconnects to the backend with bounded
/// backoff whenever a `ServerConn` driver returns. Exits when
/// the receiver half is closed (the dispatcher is dropped).
///
/// Backoff is exponential with multiplicative jitter:
///
/// * Initial delay: 50 ms.
/// * Cap: 5 000 ms.
/// * Doubling factor on every failure.
/// * Per-sleep jitter uniform in `[0.5x, 1.5x]` so concurrent
///   supervisors do not synchronise on the same retry instant.
///
/// The backoff resets to its initial value only after a run
/// during which the inner driver successfully parsed at least
/// one frame. A connection that opens, fails its first parse,
/// and disconnects therefore does not earn a free reset: the
/// pass-6 chaos run caught the previous flat-50 ms reconnect
/// loop spinning at ~18 cores against a misconfigured backend
/// that returned a parse error on every probe (see
/// `docs/journal/2026-05-27-busy-loop-investigation.md`).
///
/// Every reconnect attempt increments
/// `backend_reconnect_total{backend, reason}` so the same shape
/// is detectable from metrics scrapes alone, even when the
/// per-attempt `WARN` line is suppressed by tracing rate limiting.
///
/// When `requirepass` is set, every freshly-opened TCP connection
/// performs a Redis `AUTH <password>` handshake before being
/// handed to `run_one_backend_conn`. A rejected AUTH is treated
/// as a connection failure: the supervisor logs and reconnects.
#[tracing::instrument(
    name = "backend_supervisor",
    skip_all,
    fields(
        backend = %addr,
        ds = ?data_store,
    ),
)]
async fn backend_supervisor(
    addr: BackendAddr,
    mut rx: tokio::sync::mpsc::Receiver<OutboundRequest>,
    data_store: dynomite::conf::DataStore,
    requirepass: Option<String>,
) -> Result<(), NetError> {
    let backend_label = addr.to_string();
    let mut backoff_ms: u64 = BACKEND_BACKOFF_INIT_MS;
    let mut consecutive_failures: u64 = 0;
    loop {
        // Bail out if the channel is empty AND the sender side has
        // been dropped (proxy/dispatcher gone). `is_closed` is the
        // cleanest signal; an empty open channel just means we are
        // idle and should connect anyway.
        if rx.is_closed() && rx.is_empty() {
            return Ok(());
        }
        let connect = tokio::time::timeout(Duration::from_secs(5), addr.connect()).await;
        let mut transport = match connect {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => {
                record_reconnect_and_back_off(
                    &backend_label,
                    "connect_refused",
                    Some(&e.to_string()),
                    "backend connect failed; retrying",
                    &mut consecutive_failures,
                    &mut backoff_ms,
                )
                .await;
                continue;
            }
            Err(_) => {
                record_reconnect_and_back_off(
                    &backend_label,
                    "connect_timeout",
                    None,
                    "backend connect timed out; retrying",
                    &mut consecutive_failures,
                    &mut backoff_ms,
                )
                .await;
                continue;
            }
        };

        // Optional Redis AUTH handshake before the supervisor
        // hands the stream to the run loop. Memcache backends
        // skip this entirely (binary SASL is not implemented).
        if data_store == dynomite::conf::DataStore::Valkey {
            if let Some(pw) = requirepass.as_deref() {
                if let Err(e) =
                    redis_auth_handshake(transport.as_mut(), pw, Duration::from_secs(5)).await
                {
                    drop(transport);
                    record_reconnect_and_back_off(
                        &backend_label,
                        "auth_failed",
                        Some(&e.to_string()),
                        "backend AUTH failed; reconnecting after backoff",
                        &mut consecutive_failures,
                        &mut backoff_ms,
                    )
                    .await;
                    continue;
                }
            }
        }

        let conn = Conn::new(transport, ConnRole::Server);
        // The ServerConn takes the receiver by ownership; on its
        // exit we get the receiver back via the channel-half
        // pattern below. tokio's mpsc cannot move a Receiver in
        // and out of an owned struct cleanly, so we drive the
        // ServerConn loop manually here, owning the receiver
        // ourselves and forwarding requests / responses.
        let mut frames_ok: u64 = 0;
        let driver_res = run_one_backend_conn(conn, &mut rx, data_store, &mut frames_ok).await;
        match driver_res {
            Ok(()) => return Ok(()),
            Err(e) => {
                if frames_ok > 0 {
                    // The connection produced real work before it
                    // tore down; treat the next attempt as a fresh
                    // start so a long-lived but eventually-dropped
                    // link does not pay the previous run's backoff.
                    backoff_ms = BACKEND_BACKOFF_INIT_MS;
                    consecutive_failures = 0;
                }
                let reason = classify_reconnect_reason(&e);
                record_reconnect_and_back_off(
                    &backend_label,
                    reason,
                    Some(&e.to_string()),
                    "backend connection ended; reconnecting",
                    &mut consecutive_failures,
                    &mut backoff_ms,
                )
                .await;
            }
        }
    }
}

/// Increment `backend_reconnect_total{backend, reason}`, log
/// the supplied message subject to the throttle returned by
/// [`should_log_reconnect`], and `tokio::time::sleep` for the
/// next jittered backoff slot.
///
/// Mutates `consecutive_failures` (incremented, saturating) and
/// `backoff_ms` (advanced through [`next_backoff_ms`]) in place
/// so callers see the same schedule the supervisor itself
/// observes on the next iteration.
async fn record_reconnect_and_back_off(
    backend_label: &str,
    reason: &str,
    error: Option<&str>,
    message: &'static str,
    consecutive_failures: &mut u64,
    backoff_ms: &mut u64,
) {
    *consecutive_failures = consecutive_failures.saturating_add(1);
    crate::metrics::backend_reconnect()
        .with_label_values(&[backend_label, reason])
        .inc();
    if should_log_reconnect(*consecutive_failures) {
        if let Some(err) = error {
            tracing::warn!(
                backend = %backend_label,
                reason,
                error = %err,
                consecutive_failures = *consecutive_failures,
                "{}", message,
            );
        } else {
            tracing::warn!(
                backend = %backend_label,
                reason,
                consecutive_failures = *consecutive_failures,
                "{}", message,
            );
        }
    }
    tokio::time::sleep(jittered_backoff(*backoff_ms)).await;
    *backoff_ms = next_backoff_ms(*backoff_ms);
}

/// Initial reconnect delay (50 ms) and ceiling (5 000 ms) used
/// by the `backend_supervisor` backoff schedule. Exposed at
/// module scope so the regression test in
/// `tests/regression_busy_loop.rs` can reason about the
/// expected wall-clock spread.
pub const BACKEND_BACKOFF_INIT_MS: u64 = 50;
/// Ceiling on the per-attempt sleep imposed by the
/// `backend_supervisor` backoff schedule.
pub const BACKEND_BACKOFF_MAX_MS: u64 = 5_000;

/// Compute the next non-jittered backoff value: double the
/// previous one, saturating at the ceiling. The jittered sleep
/// uses [`jittered_backoff`].
#[must_use]
pub fn next_backoff_ms(prev: u64) -> u64 {
    prev.saturating_mul(2).min(BACKEND_BACKOFF_MAX_MS)
}

/// Apply uniform multiplicative jitter in `[0.5, 1.5]` to the
/// supplied backoff in ms and return a [`Duration`]. Jitter is
/// computed in integer arithmetic so the function never panics
/// and never depends on floating-point determinism.
///
/// The minimum sleep is 1 ms: a zero-duration sleep would
/// degrade to a yield, which is precisely what we are guarding
/// against here.
#[must_use]
pub fn jittered_backoff(ms: u64) -> Duration {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    // Multiplicative jitter in [50, 150] / 100 == [0.5, 1.5].
    let scale: u64 = rng.gen_range(50..=150);
    let scaled = ms.saturating_mul(scale) / 100;
    Duration::from_millis(scaled.max(1))
}

/// Translate a [`NetError`] into the `reason` label for the
/// `backend_reconnect_total` counter.
#[must_use]
pub fn classify_reconnect_reason(err: &NetError) -> &'static str {
    match err {
        NetError::Parse(_) => "parse",
        NetError::Io(_) => "io",
        NetError::Closed => "closed",
        NetError::Tls(_) => "tls",
        NetError::Auth(_) => "auth_failed",
        NetError::Dnode(_) => "dnode",
        NetError::Ejected | NetError::PoolExhausted | NetError::PoolShutdown => "other",
    }
}

/// Decide whether to emit the per-reconnect `WARN` line.
///
/// The first three reconnects after a healthy run are always
/// logged so operators see the start of the storm; after that
/// the supervisor logs every tenth attempt to keep journald and
/// disk usage bounded under sustained failure. The Prometheus
/// counter is incremented unconditionally.
#[must_use]
pub fn should_log_reconnect(consecutive_failures: u64) -> bool {
    consecutive_failures <= 3 || consecutive_failures.is_multiple_of(10)
}

/// Spawn a [`backend_supervisor`] task wired to the supplied
/// outbound-request channel and backend address.
///
/// This is the supervisor entry point used by [`Server::run`] and
/// re-exported here so the regression suite can drive it directly
/// against a synthetic backend. The returned [`tokio::task::JoinHandle`]
/// resolves once the channel is closed and drained.
#[doc(hidden)]
pub fn spawn_backend_supervisor_for_testing(
    addr: SocketAddr,
    rx: tokio::sync::mpsc::Receiver<OutboundRequest>,
    data_store: dynomite::conf::DataStore,
    requirepass: Option<String>,
) -> tokio::task::JoinHandle<Result<(), NetError>> {
    tokio::spawn(backend_supervisor(
        BackendAddr::Tcp(addr),
        rx,
        data_store,
        requirepass,
    ))
}

/// Spawn a [`backend_supervisor`] task wired to a Unix-domain
/// datastore socket at `path`. The unix sibling of
/// [`spawn_backend_supervisor_for_testing`]; used by the
/// integration suite to drive the supervisor against a fake
/// `UnixListener` backend.
#[cfg(unix)]
#[doc(hidden)]
pub fn spawn_backend_supervisor_unix_for_testing(
    path: PathBuf,
    rx: tokio::sync::mpsc::Receiver<OutboundRequest>,
    data_store: dynomite::conf::DataStore,
    requirepass: Option<String>,
) -> tokio::task::JoinHandle<Result<(), NetError>> {
    tokio::spawn(backend_supervisor(
        BackendAddr::Unix(path),
        rx,
        data_store,
        requirepass,
    ))
}

/// Drive one TCP connection to the backend. Reads requests from
/// `rx`, writes them, parses responses, and pushes responses
/// onto each request's per-connection responder channel. Returns
/// `Ok(())` when `rx` closes naturally; returns an error on
/// transport failure so the supervisor reconnects.
#[tracing::instrument(
    name = "run_one_backend_conn",
    skip_all,
    fields(ds = ?data_store),
)]
#[allow(
    clippy::too_many_lines,
    reason = "single backend conn driver enumerates every state of a request/response interleave; splitting hides the borrow-checker boundaries on `accumulated` and `pending`"
)]
async fn run_one_backend_conn(
    mut conn: Conn,
    rx: &mut tokio::sync::mpsc::Receiver<OutboundRequest>,
    data_store: dynomite::conf::DataStore,
    frames_ok: &mut u64,
) -> Result<(), NetError> {
    use dynomite::msg::{Msg, MsgParseResult, MsgType};
    use dynomite::net::OutboundEnvelope;
    use tracing::Instrument as _;
    let mut accumulated: Vec<u8> = Vec::new();
    let mut read_buf = vec![0u8; 4096];
    let mut pending: std::collections::VecDeque<(
        u64,
        tokio::sync::mpsc::Sender<OutboundEnvelope>,
        tracing::Span,
    )> = std::collections::VecDeque::new();
    loop {
        tokio::select! {
            req = rx.recv() => {
                let Some(req) = req else { return Ok(()); };
                // Build the `backend.send` span as a child of
                // the originating client request span and use
                // `.instrument()` to attach it to the write
                // future. This avoids crossing an `.await` with
                // a non-`Send` `EnteredSpan` guard.
                let send_span = tracing::info_span!(
                    parent: &req.span,
                    "backend.send",
                    req_id = req.req_id,
                    bytes = req.bytes.len(),
                );
                let req_span = req.span.clone();
                let req_bytes = req.bytes;
                let transport = conn.transport_mut().ok_or(NetError::Closed)?;
                let write_res = async { transport.write_all(&req_bytes).await }
                    .instrument(send_span)
                    .await;
                if let Err(e) = write_res {
                    return Err(NetError::Io(e));
                }
                conn.record_send(req_bytes.len());
                pending.push_back((req.req_id, req.responder, req_span));
            }
            res = async {
                if let Some(t) = conn.transport_mut() {
                    t.read(&mut read_buf).await
                } else {
                    Ok(0)
                }
            } => {
                let n = match res {
                    Ok(n) => n,
                    Err(e) => return Err(NetError::Io(e)),
                };
                if n == 0 {
                    return Err(NetError::Closed);
                }
                conn.record_recv(n);
                accumulated.extend_from_slice(&read_buf[..n]);
                while !accumulated.is_empty() {
                    let head_id = pending.front().map_or(0, |p| p.0);
                    let mut msg = Msg::new(head_id, MsgType::Unknown, false);
                    let result = match data_store {
                        dynomite::conf::DataStore::Valkey | dynomite::conf::DataStore::Dyniak => {
                            dynomite::proto::redis::redis_parse_rsp(&mut msg, &accumulated)
                        }
                        dynomite::conf::DataStore::Memcache => {
                            dynomite::proto::memcache::memcache_parse_rsp(&mut msg, &accumulated)
                        }
                    };
                    match result {
                        MsgParseResult::Ok => {
                            let consumed = msg.parser_pos();
                            if consumed == 0 {
                                return Err(NetError::Parse("backend parser stalled".into()));
                            }
                            let bytes = accumulated[..consumed].to_vec();
                            accumulated.drain(0..consumed);
                            *frames_ok = frames_ok.saturating_add(1);
                            if let Some((req_id, responder, req_span)) = pending.pop_front() {
                                let parse_span = tracing::info_span!(
                                    parent: &req_span,
                                    "backend.parse",
                                    req_id,
                                    bytes = consumed,
                                );
                                let env = parse_span.in_scope(|| {
                                    let pool = conn.mbuf_pool().clone();
                                    let mut buf = pool.get();
                                    buf.recv(&bytes);
                                    msg.mbufs_mut().push_back(buf);
                                    msg.recompute_mlen();
                                    OutboundEnvelope {
                                        req_id,
                                        rsp: msg,
                                        span: req_span,
                                        source_peer_idx: None,
                                    }
                                });
                                let _ = responder.send(env).await;
                            }
                        }
                        MsgParseResult::Again => {
                            break;
                        }
                        MsgParseResult::Repair
                        | MsgParseResult::Noop
                        | MsgParseResult::Fragment => {
                            let consumed = msg.parser_pos();
                            if consumed > 0 {
                                accumulated.drain(0..consumed);
                            } else {
                                break;
                            }
                        }
                        MsgParseResult::Error
                        | MsgParseResult::OomError
                        | MsgParseResult::DynoConfig => {
                            return Err(NetError::Parse(format!("{result:?}")));
                        }
                    }
                }
            }
        }
    }
}

/// Long-running supervisor for one outbound peer connection.
/// Maintains a `DnodeServerConn` against the peer's `dyn_listen`
/// address, reconnecting with capped exponential backoff when
/// the underlying TCP / dnode driver returns an error. Owns the
/// receiver half of the dispatcher's per-peer outbound channel
/// and exits when that channel is closed (the dispatcher dropped).
///
/// The supervisor does NOT publish [`PeerState`] transitions:
/// once gossip is wired the peer-state field is owned by
/// [`dynomite::cluster::gossip::GossipHandler`] and driven by
/// the phi-accrual failure detector. The supervisor only owns
/// the TCP / dnode framing for data-plane traffic; treating the
/// link state as the peer-state signal would force every remote
/// peer to flap whenever the connection bounced.
#[tracing::instrument(
    name = "peer_supervisor",
    skip_all,
    fields(peer = %addr),
)]
async fn peer_supervisor(
    addr: SocketAddr,
    peer_idx: u32,
    mut rx: tokio::sync::mpsc::Receiver<OutboundRequest>,
    tls: Option<PeerTlsRuntime>,
    peer_dc: String,
) -> Result<(), NetError> {
    // Resolve the per-peer client-side TLS handles dynamically:
    // the supervisor re-reads `tls.profiles` on every reconnect
    // attempt so a SIGHUP-driven cert rotation lands on the
    // next dial without restarting the supervisor. When the
    // shared profile map carries no entry for `peer_dc` and no
    // default is set, the supervisor falls back to plaintext
    // for this peer (when neither a per-DC profile nor a default
    // is set, the connection is plaintext).
    let peer_tls_profiles = tls.as_ref().map(|rt| rt.profiles.clone());
    let sni_hostname = dynomite::net::tls::dc_sni_hostname(peer_dc.as_str());
    let mut backoff_ms: u64 = 100;
    let backoff_max_ms: u64 = 5_000;
    loop {
        if rx.is_closed() && rx.is_empty() {
            return Ok(());
        }
        let connect =
            tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
                .await;
        let stream = match connect {
            Ok(Ok(s)) => {
                backoff_ms = 100;
                s
            }
            Ok(Err(e)) => {
                tracing::warn!(peer = %addr, error = %e, "peer connect failed; retrying");
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms.saturating_mul(2)).min(backoff_max_ms);
                continue;
            }
            Err(_) => {
                tracing::warn!(peer = %addr, "peer connect timed out; retrying");
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms.saturating_mul(2)).min(backoff_max_ms);
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        tracing::debug!(
            peer = %addr,
            peer_idx,
            "peer TCP link up; gossip drives PeerState authoritatively"
        );
        // Wrap with TLS when configured. On handshake failure we
        // log and re-enter the reconnect loop with backoff so a
        // mis-trusted CA does not saturate the CPU. The connector
        // is rebuilt from the shared TLS profile map on every
        // reconnect attempt so a SIGHUP-driven cert rotation
        // lands on the next dial.
        let connector = peer_tls_profiles.as_ref().and_then(|p| {
            p.client_config_for_dc(peer_dc.as_str())
                .map(dynomite::net::tls::connector_from)
        });
        let transport: Box<dyn dynomite::io::reactor::Transport> = if let Some(connector) =
            connector.as_ref()
        {
            let server_name = match dynomite::net::tls::server_name_owned(sni_hostname.as_str()) {
                Ok(n) => n,
                Err(e) => {
                    tracing::warn!(
                        peer = %addr,
                        error = %e,
                        "peer TLS server-name parse failed; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(backoff_max_ms);
                    continue;
                }
            };
            match connector.connect(server_name, stream).await {
                Ok(tls_stream) => Box::new(dynomite::net::tls::TlsClientTransport::new(
                    tls_stream,
                    ConnRole::DnodePeerServer,
                )),
                Err(e) => {
                    tracing::warn!(
                        peer = %addr,
                        error = %e,
                        "peer TLS handshake failed; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(backoff_max_ms);
                    continue;
                }
            }
        } else {
            Box::new(TcpTransport::new(stream, ConnRole::DnodePeerServer))
        };
        let conn = Conn::new(transport, ConnRole::DnodePeerServer);
        // Build a fresh DnodeServerConn each iteration. The
        // borrowed-receiver `run_with` keeps `rx` owned by us so
        // we can reconnect without losing pending requests.
        let mut driver = DnodeServerConn::new(
            conn,
            // Placeholder receiver; `run_with` ignores
            // `self.requests` and reads from the borrowed one.
            tokio::sync::mpsc::channel::<OutboundRequest>(1).1,
        );
        let driver_res = driver.run_with(&mut rx).await;
        if let Err(e) = driver_res {
            tracing::warn!(
                peer = %addr,
                error = %e,
                "peer connection ended; reconnecting"
            );
        } else {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Bundle of TLS handles shared across the dnode listener and
/// every outbound peer supervisor.
///
/// The struct is `Clone`-cheap (each field is an `Arc` under the
/// hood) so the `Server::build` closure can hand a fresh copy to
/// every spawned task without a global mutex.
///
/// `acceptor` is the SNI-routed listener acceptor whose
/// resolver reads the [`SharedTlsProfiles`] cell on every
/// handshake; `profiles` is that same cell, used by the
/// per-peer outbound supervisors to pick a TLS connector
/// matching the target peer's DC at reconnect time.
#[derive(Clone)]
struct PeerTlsRuntime {
    acceptor: tokio_rustls::TlsAcceptor,
    profiles: dynomite::net::tls::SharedTlsProfiles,
}

/// Resolve the peer-plane TLS knobs into a `PeerTlsRuntime`.
///
/// Returns `Ok(None)` when neither the legacy `peer_tls_*`
/// triple nor the per-DC `peer_tls_profiles` map is populated;
/// returns `Ok(Some(_))` when at least one profile is configured.
/// Mismatched `(cert, key)` is already rejected by
/// [`dynomite::conf::ConfPool::validate`], but we keep the
/// defensive branches so direct callers (tests) also see a
/// clear error.
fn build_peer_tls_runtime(conf_pool: &ConfPool) -> Result<Option<PeerTlsRuntime>, ServerError> {
    let default_spec = match (
        conf_pool.peer_tls_cert.as_deref(),
        conf_pool.peer_tls_key.as_deref(),
    ) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some(dynomite::net::tls::TlsProfileSpec {
            cert: cert.to_path_buf(),
            key: key.to_path_buf(),
            ca: conf_pool.peer_tls_ca.clone(),
        }),
        (Some(_), None) => {
            return Err(ServerError::BadConfig {
                field: "peer_tls_key",
                reason: "peer_tls_cert is set but peer_tls_key is not".into(),
            });
        }
        (None, Some(_)) => {
            return Err(ServerError::BadConfig {
                field: "peer_tls_cert",
                reason: "peer_tls_key is set but peer_tls_cert is not".into(),
            });
        }
    };

    let mut per_dc: std::collections::BTreeMap<String, dynomite::net::tls::TlsProfileSpec> =
        std::collections::BTreeMap::new();
    for (dc, profile) in &conf_pool.peer_tls_profiles {
        match (profile.cert.as_deref(), profile.key.as_deref()) {
            (Some(cert), Some(key)) => {
                per_dc.insert(
                    dc.clone(),
                    dynomite::net::tls::TlsProfileSpec {
                        cert: cert.to_path_buf(),
                        key: key.to_path_buf(),
                        ca: profile.ca.clone(),
                    },
                );
            }
            (None, None) => {
                // Empty entry is meaningless; treat as a config
                // error to avoid silently downgrading the DC to
                // plaintext when the operator clearly meant to
                // configure something.
                return Err(ServerError::BadConfig {
                    field: "peer_tls_profiles",
                    reason: format!("peer_tls_profiles[{dc}] is empty"),
                });
            }
            (Some(_), None) => {
                return Err(ServerError::BadConfig {
                    field: "peer_tls_profiles.key",
                    reason: format!("peer_tls_profiles[{dc}].cert is set but .key is not"),
                });
            }
            (None, Some(_)) => {
                return Err(ServerError::BadConfig {
                    field: "peer_tls_profiles.cert",
                    reason: format!("peer_tls_profiles[{dc}].key is set but .cert is not"),
                });
            }
        }
    }

    if default_spec.is_none() && per_dc.is_empty() {
        return Ok(None);
    }

    let profiles = dynomite::net::tls::TlsProfileMap::build(default_spec, per_dc).map_err(|e| {
        ServerError::BadConfig {
            field: "peer_tls_profiles",
            reason: e.to_string(),
        }
    })?;
    let shared = dynomite::net::tls::SharedTlsProfiles::from_map(profiles);
    let acceptor = shared
        .build_sni_acceptor()
        .map_err(|e| ServerError::BadConfig {
            field: "peer_tls_profiles",
            reason: e.to_string(),
        })?
        .ok_or_else(|| ServerError::BadConfig {
            field: "peer_tls_profiles",
            reason: "profile map built but produced no acceptor (internal invariant)".into(),
        })?;
    Ok(Some(PeerTlsRuntime {
        acceptor,
        profiles: shared,
    }))
}

fn default_gossip_interval_ms_i64() -> i64 {
    i64::try_from(dynomite::cluster::gossip::DEFAULT_GOSSIP_INTERVAL_MS).unwrap_or(1_000)
}

/// Construct the entropy reconciliation driver when the pool's
/// `recon_key_file:` resolves to a readable file.
///
/// Returns `(None, None)` when the directive is unset, points
/// at an empty string, or the on-disk key / IV files cannot be
/// loaded. In every skip path a tracing event explains why;
/// behavior is unchanged for
/// pools that do not opt into entropy reconciliation.
fn build_entropy_driver(
    conf_pool: &ConfPool,
    server_pool: &Arc<ServerPool>,
    pool_name: &str,
) -> (
    Option<dynomite::entropy::driver::EntropyDriver>,
    Option<std::path::PathBuf>,
) {
    let key_path = match conf_pool.recon_key_file.as_deref() {
        Some(p) if !p.is_empty() => std::path::PathBuf::from(p),
        _ => return (None, None),
    };
    let iv_path = match conf_pool.recon_iv_file.as_deref() {
        Some(p) if !p.is_empty() => std::path::PathBuf::from(p),
        _ => {
            tracing::warn!(
                pool = %pool_name,
                "recon_key_file is set but recon_iv_file is not; entropy task disabled"
            );
            return (None, None);
        }
    };
    if !key_path.exists() || !iv_path.exists() {
        tracing::warn!(
            pool = %pool_name,
            key_file = %key_path.display(),
            iv_file = %iv_path.display(),
            "recon key / iv file is missing on disk; entropy task disabled"
        );
        return (None, None);
    }
    let material = match dynomite::entropy::util::load_material(&key_path, &iv_path) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(
                pool = %pool_name,
                error = %e,
                key_file = %key_path.display(),
                iv_file = %iv_path.display(),
                "failed to load recon key/iv material; entropy task disabled"
            );
            return (None, None);
        }
    };
    let cadence_secs = conf_pool
        .recon_interval_seconds
        .filter(|s| *s > 0)
        .unwrap_or_else(|| dynomite::entropy::driver::DEFAULT_RECON_INTERVAL.as_secs());
    let cadence = Duration::from_secs(cadence_secs);
    // The default snapshot source is empty: a future stage
    // wires the embedded data store's snapshot here. The
    // periodic header exchange still verifies peer
    // reachability and surfaces the per-cycle counters at INFO.
    let source: dynomite::entropy::BoxedSnapshotSource =
        std::sync::Arc::new(dynomite::entropy::send::StaticSnapshot::new(Vec::new()));
    let driver = dynomite::entropy::driver::EntropyDriver::new(
        material,
        source,
        server_pool.peers_arc(),
        cadence,
    );
    (Some(driver), Some(key_path))
}

/// Single periodic gossip task per pool.
///
/// Ticks at the handler's configured interval and: (1) sends a
/// `GossipSyn` to every wired peer with the local-node pname as
/// the payload, (2) re-evaluates phi for every non-local peer
/// and toggles `PeerState`. The task fires the first heartbeat
/// immediately rather than waiting a full interval so the
/// receiver records a heartbeat as soon as the TCP link is up.
async fn gossip_task(
    handler: Arc<dynomite::cluster::gossip::GossipHandler>,
    local_pname: String,
    peer_txs: Vec<(u32, String, tokio::sync::mpsc::Sender<OutboundRequest>)>,
    cancel: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) {
    use dynomite::proto::dnode::DmsgType;
    if peer_txs.is_empty() {
        // No remote peers; just block until cancelled so the
        // join handle behaves consistently with multi-peer
        // pools.
        let () = cancel.await;
        return;
    }
    let mut cancel = cancel;
    let mut next_msg_id: u64 = 1;
    let interval = handler.interval();
    // Fire once immediately, then on a fixed cadence.
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = &mut cancel => return,
            _ = ticker.tick() => {
                let now = std::time::Instant::now();
                for (peer_idx, _peer_pname, tx) in &peer_txs {
                    let payload = local_pname.as_bytes().to_vec();
                    let msg_id = next_msg_id;
                    next_msg_id = next_msg_id.wrapping_add(1).max(1);
                    // Disposable responder: gossip frames are
                    // fire-and-forget, the matching response is
                    // never produced.
                    let (rsp_tx, _rsp_rx) =
                        tokio::sync::mpsc::channel::<dynomite::net::dispatcher::OutboundEnvelope>(1);
                    let req = OutboundRequest {
                        bytes: payload,
                        req_id: msg_id,
                        responder: rsp_tx,
                        span: tracing::Span::current(),
                        ty: DmsgType::GossipSyn,
                        target_peer_idx: None,
                    };
                    if let Err(e) = tx.try_send(req) {
                        tracing::trace!(
                            peer_idx,
                            error = %e,
                            "gossip_syn channel push failed"
                        );
                    }
                }
                let transitions = handler.evaluate(now);
                for (idx, state) in transitions {
                    tracing::info!(
                        peer_idx = idx,
                        ?state,
                        "gossip transitioned peer state"
                    );
                }
            }
        }
    }
}

/// Best-effort `GossipShutdown` broadcast on graceful exit.
///
/// Each peer receives a single fire-and-forget control frame so
/// it can mark us [`PeerState::Down`] immediately rather than
/// waiting for phi to cross the threshold. Failures (closed
/// channel, full queue) are silent on purpose: we are already
/// shutting down and the receiving side will detect us via
/// gossip silence within `~threshold * gossip_interval`.
fn send_gossip_shutdown(
    local_pname: &str,
    peer_txs: &[(u32, String, tokio::sync::mpsc::Sender<OutboundRequest>)],
) {
    use dynomite::proto::dnode::DmsgType;
    for (peer_idx, _, tx) in peer_txs {
        let (rsp_tx, _rsp_rx) =
            tokio::sync::mpsc::channel::<dynomite::net::dispatcher::OutboundEnvelope>(1);
        let req = OutboundRequest {
            bytes: local_pname.as_bytes().to_vec(),
            req_id: 0,
            responder: rsp_tx,
            span: tracing::Span::current(),
            ty: DmsgType::GossipShutdown,
            target_peer_idx: None,
        };
        if let Err(e) = tx.try_send(req) {
            tracing::trace!(
                peer_idx,
                error = %e,
                "gossip_shutdown channel push failed"
            );
        }
    }
}

/// Periodic drainer for the node-local hint store.
///
/// Each tick performs two passes:
///
/// 1. Drop hints whose deadline has passed
///    ([`HintStore::expire_now`]). Bounds the in-memory
///    store so a long-running peer outage does not consume
///    unbounded memory.
/// 2. For each peer in [`PeerState::Normal`], take that
///    peer's pending hints and ship them via the wired
///    per-peer outbound channel as `DmsgType::ReqForward`
///    so the receiving peer's `dnode_client_loop` rewrites
///    the parsed request's routing tag to `LocalNodeOnly`
///    (no recursive fan-out at the destination). The hint
///    is fire-and-forget: the responder is a disposable
///    sink so the receiving peer's reply is dropped.
///
/// Runs until `cancel` resolves.
async fn hint_drainer_task(
    store: Arc<HintStore>,
    pool: Arc<ServerPool>,
    peer_txs: Vec<(u32, String, tokio::sync::mpsc::Sender<OutboundRequest>)>,
    interval: Duration,
    cancel: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) {
    use dynomite::net::dispatcher::OutboundEnvelope;
    use dynomite::proto::dnode::DmsgType;
    let mut cancel = cancel;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Map peer_idx -> (channel, pname) for O(1) lookup.
    let txs_by_idx: std::collections::HashMap<
        u32,
        (String, tokio::sync::mpsc::Sender<OutboundRequest>),
    > = peer_txs
        .into_iter()
        .map(|(idx, pname, tx)| (idx, (pname, tx)))
        .collect();
    loop {
        tokio::select! {
            biased;
            () = &mut cancel => return,
            _ = ticker.tick() => {
                let dropped = store.expire_now(std::time::Instant::now());
                if dropped > 0 {
                    tracing::debug!(
                        target: "dynomite::hints",
                        dropped,
                        "hint drainer expired entries"
                    );
                }
                // Snapshot the per-peer state so we never hold
                // the peer-table read lock while shipping hints.
                let live_peers: Vec<u32> = {
                    let peers = pool.peers().read();
                    peers
                        .iter()
                        .filter(|p| !p.is_local() && matches!(p.state(), PeerState::Normal))
                        .map(dynomite::cluster::Peer::idx)
                        .collect()
                };
                for peer_idx in live_peers {
                    let Some((_pname, tx)) = txs_by_idx.get(&peer_idx) else {
                        continue;
                    };
                    let hints = store.take_for(peer_idx);
                    if hints.is_empty() {
                        continue;
                    }
                    let count = hints.len();
                    let mut shipped = 0usize;
                    let mut requeued = 0usize;
                    for h in hints {
                        let payload_len = h.payload.len();
                        let (rsp_tx, mut rsp_rx) =
                            tokio::sync::mpsc::channel::<OutboundEnvelope>(1);
                        // Drop the disposable response on the
                        // floor so the channel does not back
                        // up under high replay rates.
                        tokio::spawn(async move {
                            while rsp_rx.recv().await.is_some() {}
                        });
                        let req = OutboundRequest {
                            bytes: h.payload.clone(),
                            req_id: 0,
                            responder: rsp_tx,
                            span: tracing::Span::none(),
                            ty: DmsgType::ReqForward,
                            target_peer_idx: Some(peer_idx),
                        };
                        if tx.try_send(req).is_ok() {
                            shipped += 1;
                        } else {
                            // Channel full or closed: put the
                            // hint back so the next sweep can
                            // retry. We only re-enqueue when
                            // the deadline has not already
                            // passed.
                            let ttl_remaining = h
                                .deadline
                                .checked_duration_since(std::time::Instant::now())
                                .unwrap_or_else(|| Duration::from_secs(1));
                            if ttl_remaining > Duration::from_secs(0)
                                && store
                                    .enqueue(peer_idx, h.payload, ttl_remaining)
                                    .is_ok()
                            {
                                requeued += 1;
                            }
                            let _ = payload_len;
                        }
                    }
                    tracing::debug!(
                        target: "dynomite::hints",
                        peer_idx,
                        count,
                        shipped,
                        requeued,
                        "hint drainer pass"
                    );
                }
            }
        }
    }
}

async fn wait_finished_opt(handle: Option<&JoinHandle<Result<(), NetError>>>) {
    match handle {
        Some(h) => wait_finished(h).await,
        None => std::future::pending::<()>().await,
    }
}

async fn wait_finished_io_opt(handle: Option<&JoinHandle<io::Result<()>>>) {
    match handle {
        Some(h) => {
            while !h.is_finished() {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        None => std::future::pending::<()>().await,
    }
}

async fn await_listener(
    task: &'static str,
    handle: JoinHandle<Result<(), NetError>>,
) -> Result<(), ServerError> {
    match handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(ServerError::TaskFailed {
            task,
            reason: e.to_string(),
        }),
        Err(e) => Err(ServerError::TaskFailed {
            task,
            reason: e.to_string(),
        }),
    }
}

async fn await_stats(
    task: &'static str,
    handle: JoinHandle<io::Result<()>>,
) -> Result<(), ServerError> {
    match handle.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(ServerError::TaskFailed {
            task,
            reason: e.to_string(),
        }),
        Err(e) => Err(ServerError::TaskFailed {
            task,
            reason: e.to_string(),
        }),
    }
}

fn listen_to_socket_addr(l: &ConfListen, field: &'static str) -> Result<SocketAddr, ServerError> {
    match l.kind() {
        EndpointKind::V4 | EndpointKind::V6 => {
            let pname = l.pname();
            pname
                .parse::<SocketAddr>()
                .map_err(|e| ServerError::BadConfig {
                    field,
                    reason: format!("could not parse '{pname}' as a socket address: {e}"),
                })
        }
        EndpointKind::UnixPath => Err(ServerError::BadConfig {
            field,
            reason: "unix-domain sockets are not supported by the run loop yet".into(),
        }),
        EndpointKind::Hostname => {
            // Resolve via the std resolver, aborting on failure.
            let pname = l.pname();
            pname
                .to_socket_addrs()
                .map_err(|e| ServerError::BadConfig {
                    field,
                    reason: format!("could not resolve '{pname}': {e}"),
                })?
                .next()
                .ok_or_else(|| ServerError::BadConfig {
                    field,
                    reason: format!("'{pname}' resolved to no addresses"),
                })
        }
    }
}

/// Resolve the address this node advertises to peers (its gossip
/// identity and the endpoint peers match it by), given the bind
/// address it derived from `dyn_listen`.
///
/// When the bind IP is a wildcard (`0.0.0.0` or `[::]`) the node
/// accepts connections on every interface but must not advertise the
/// wildcard: peers store it in their seed list by a routable address,
/// and gossip matches by that pname. The routable address is read
/// from the `DYN_ADVERTISE_ADDR` environment variable, which may be
/// either a bare host (`203.0.113.7`) -- the `dyn_listen` port is kept
/// -- or a full `host:port`. When the bind IP is already concrete the
/// bind address is advertised unchanged. A wildcard bind with no
/// `DYN_ADVERTISE_ADDR` is left as-is and logged, because the node
/// cannot discover its own routable address unaided.
fn resolve_advertise_addr(dyn_listen: Option<SocketAddr>) -> Option<SocketAddr> {
    let env = std::env::var("DYN_ADVERTISE_ADDR").ok();
    resolve_advertise_addr_with(dyn_listen, env.as_deref())
}

/// Core of [`resolve_advertise_addr`] with the `DYN_ADVERTISE_ADDR`
/// value injected, so it is testable without mutating process env.
fn resolve_advertise_addr_with(
    dyn_listen: Option<SocketAddr>,
    advertise_env: Option<&str>,
) -> Option<SocketAddr> {
    let bind = dyn_listen?;
    if !bind.ip().is_unspecified() {
        return Some(bind);
    }
    let Some(val) = advertise_env else {
        tracing::warn!(
            bind = %bind,
            "dyn_listen binds a wildcard address and DYN_ADVERTISE_ADDR is unset; peers will not be able to match this node's gossip identity and will keep it Down"
        );
        return Some(bind);
    };
    let trimmed = val.trim();
    // Accept `host:port` or bare `host` (keep the bind port).
    let candidate = if trimmed.parse::<SocketAddr>().is_ok() {
        trimmed.to_string()
    } else {
        format!("{trimmed}:{}", bind.port())
    };
    match candidate.parse::<SocketAddr>() {
        Ok(addr) => {
            tracing::info!(
                bind = %bind,
                advertise = %addr,
                "advertising routable dnode address (bind is wildcard)"
            );
            Some(addr)
        }
        Err(e) => {
            tracing::warn!(
                value = %val,
                error = %e,
                "DYN_ADVERTISE_ADDR is not a valid address; advertising the wildcard bind address, which peers cannot match"
            );
            Some(bind)
        }
    }
}

fn build_local_peer(
    conf_pool: &ConfPool,
    pool_config: &PoolConfig,
    dyn_listen: Option<SocketAddr>,
) -> Result<Peer, ServerError> {
    let tokens = conf_pool
        .tokens
        .as_ref()
        .ok_or(ServerError::MissingConfig("tokens"))?;
    let dyn_tokens: Vec<DynToken> = tokens
        .components()
        .iter()
        .map(token_component_to_dyn)
        .collect::<Result<Vec<_>, ServerError>>()?;
    if dyn_tokens.is_empty() {
        return Err(ServerError::BadConfig {
            field: "tokens",
            reason: "expected at least one token".into(),
        });
    }
    // Reuse the configured peer-listen address as this node's
    // own peer endpoint; with no dyn_listen we still need an
    // endpoint for the topology tables, so fall back to the
    // configured listen address.
    let endpoint = dyn_listen.map_or_else(
        || PeerEndpoint::tcp("127.0.0.1".into(), 0),
        |a| PeerEndpoint::tcp(a.ip().to_string(), a.port()),
    );
    Ok(Peer::new(
        0,
        endpoint,
        pool_config.rack.clone(),
        pool_config.dc.clone(),
        dyn_tokens,
        true,
        true,
        false,
    ))
}

fn seed_to_peer(
    idx: u32,
    seed: &ConfDynSeed,
    pool_config: &PoolConfig,
) -> Result<Peer, ServerError> {
    let dyn_tokens: Vec<DynToken> = seed
        .tokens()
        .components()
        .iter()
        .map(token_component_to_dyn)
        .collect::<Result<Vec<_>, ServerError>>()?;
    let is_same_dc = seed.dc() == pool_config.dc;
    let endpoint = PeerEndpoint::tcp(seed.host().to_string(), seed.port());
    Ok(Peer::new(
        idx,
        endpoint,
        seed.rack().to_string(),
        seed.dc().to_string(),
        dyn_tokens,
        false,
        is_same_dc,
        false,
    ))
}

fn token_component_to_dyn(
    component: &dynomite::conf::TokenComponent,
) -> Result<DynToken, ServerError> {
    // `DynToken` carries a
    // four-byte big-endian integer that fits the common case
    // (single-rack ring with sub-`u32::MAX` tokens). Parse the
    // decimal digits into u32 with saturation when oversized so
    // operators with values beyond `u32::MAX` still get a valid,
    // clearly-marked-up token rather than a panic. The behaviour
    // is documented in `docs/parity.md`.
    let digits = &component.digits;
    let value = digits.parse::<u128>().map_err(|e| ServerError::BadConfig {
        field: "tokens",
        reason: format!("'{digits}': {e}"),
    })?;
    let trimmed = u32::try_from(value).unwrap_or(u32::MAX);
    Ok(DynToken::from_u32(trimmed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn yaml(listen_port: u16, dyn_port: u16, stats_port: u16) -> String {
        format!(
            "p:\n  listen: 127.0.0.1:{listen_port}\n  dyn_listen: 127.0.0.1:{dyn_port}\n  stats_listen: 127.0.0.1:{stats_port}\n  tokens: '101134286'\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n",
        )
    }

    fn free_port() -> u16 {
        // Static lock so concurrent unit tests do not race the
        // kernel into handing the same ephemeral port out twice.
        static GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = GUARD
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        port
    }

    /// Serialises the tests that build and `run()` a full
    /// [`Server`]. `free_port` hands back a port but drops its
    /// probe listener before [`Server::build`] rebinds it, so two
    /// server-building tests running concurrently can race into
    /// the same ephemeral port. The loser's stats / proxy / dnode
    /// listener then fails to bind (or its first connection is
    /// reset) and the listener task exits before the shutdown
    /// signal arrives, surfacing as `TaskFailed { task: "stats",
    /// reason: "listener returned before shutdown" }`. Holding
    /// this async mutex for the lifetime of each server-building
    /// test keeps them strictly serial without serialising the
    /// whole unit-test binary. The flake only manifested under
    /// `--all-features` in CI, where the extra feature-gated
    /// tasks widen the bind-race window.
    fn server_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn build_runs_and_shuts_down() {
        let _serial = server_test_lock().lock().await;
        // Two free ephemeral ports. We bind the picker on a v4
        // loopback with SO_REUSEADDR so a second test running in
        // parallel does not race us into the same port.
        let listen_port = free_port();
        let dyn_port = free_port_distinct(listen_port);
        let stats_port = free_port_distinct(listen_port);
        let cfg = Config::parse_str(&yaml(listen_port, dyn_port, stats_port)).unwrap();
        let server = match Server::build(cfg).await {
            Ok(s) => s,
            Err(e) => panic!("build failed (lp={listen_port}, dp={dyn_port}): {e}"),
        };
        let listen_addr = server.listen_addr();
        let dyn_listen_addr = server.dyn_listen_addr();
        assert_eq!(listen_addr.port(), listen_port);
        assert_eq!(dyn_listen_addr.map(|a| a.port()), Some(dyn_port));
        let handle = server.shutdown_handle();
        let supervisor = tokio::spawn(async move { server.run().await });
        tokio::time::sleep(Duration::from_millis(100)).await;
        handle.shutdown();
        let res = tokio::time::timeout(Duration::from_secs(5), supervisor)
            .await
            .expect("run loop did not shut down within 5s")
            .expect("join");
        assert!(res.is_ok(), "run returned error: {res:?}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_method_triggers_stop() {
        let _serial = server_test_lock().lock().await;
        let listen_port = free_port();
        let dyn_port = free_port_distinct(listen_port);
        let stats_port = free_port_distinct(listen_port);
        let cfg = Config::parse_str(&yaml(listen_port, dyn_port, stats_port)).unwrap();
        let server = Server::build(cfg).await.expect("build");
        let handle = server.shutdown_handle();
        let supervisor = tokio::spawn(async move { server.run().await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Calling `shutdown` on the handle is equivalent to the
        // signal arms.
        assert!(handle.shutdown());
        let res = tokio::time::timeout(Duration::from_secs(5), supervisor)
            .await
            .expect("supervisor stuck")
            .expect("join");
        assert!(res.is_ok(), "run returned error: {res:?}");
    }

    fn free_port_distinct(other: u16) -> u16 {
        for _ in 0..32 {
            let p = free_port();
            if p != other {
                return p;
            }
        }
        panic!("could not find a free port distinct from {other}");
    }

    #[test]
    fn token_component_round_trip() {
        let cmp = dynomite::conf::TokenComponent {
            signum: 1,
            digits: "42".to_string(),
        };
        let tok = token_component_to_dyn(&cmp).unwrap();
        assert_eq!(tok.get_int(), 42);
    }

    #[test]
    fn token_component_saturates_above_u32() {
        let cmp = dynomite::conf::TokenComponent {
            signum: 1,
            digits: "99999999999".to_string(),
        };
        let tok = token_component_to_dyn(&cmp).unwrap();
        assert_eq!(tok.get_int(), u32::MAX);
    }

    /// Build a tiny RESP server that:
    ///   1. accepts a single TCP connection,
    ///   2. reads the first command (the AUTH),
    ///   3. replies with the operator-supplied bytes,
    ///   4. records what was read into a shared buffer.
    async fn auth_stub_backend(
        reply: &'static [u8],
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<tokio::sync::Mutex<Vec<u8>>>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let recorded = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::<u8>::new()));
        let recorded_inner = recorded.clone();
        let h = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            // Read until we have at least one CRLF-terminated
            // command (AUTH always ends with two CRLFs in the
            // RESP framing). Bound the read so the test cannot
            // hang on a misbehaving client.
            for _ in 0..32 {
                match tokio::time::timeout(Duration::from_secs(1), sock.read(&mut buf)).await {
                    Ok(Ok(n)) if n > 0 => {
                        let mut g = recorded_inner.lock().await;
                        g.extend_from_slice(&buf[..n]);
                        // RESP `*2\r\n$4\r\nAUTH\r\n$N\r\n<pw>\r\n`
                        // ends with at least 4 CRLFs.
                        if g.windows(2).filter(|w| *w == b"\r\n").count() >= 4 {
                            break;
                        }
                    }
                    // Eof, timeout, or read error - all terminal.
                    _ => break,
                }
            }
            let _ = sock.write_all(reply).await;
            // Hold the socket briefly so the client can read.
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = sock.shutdown().await;
        });
        (addr, recorded, h)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redis_auth_handshake_sends_auth_and_succeeds_on_ok() {
        let (addr, recorded, h) = auth_stub_backend(b"+OK\r\n").await;
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        redis_auth_handshake(&mut stream, "hunter2", Duration::from_secs(2))
            .await
            .expect("AUTH should succeed");
        h.await.unwrap();
        let bytes = recorded.lock().await.clone();
        let s = String::from_utf8_lossy(&bytes).to_string();
        assert!(s.contains("AUTH"), "client did not send AUTH: {s:?}");
        assert!(s.contains("hunter2"), "client did not send password: {s:?}");
        assert!(
            s.starts_with("*2\r\n$4\r\nAUTH\r\n"),
            "AUTH not framed as a RESP array: {s:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redis_auth_handshake_rejects_on_err_reply() {
        let (addr, _recorded, h) = auth_stub_backend(b"-ERR invalid password\r\n").await;
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let err = redis_auth_handshake(&mut stream, "wrong", Duration::from_secs(2))
            .await
            .expect_err("AUTH should fail on -ERR");
        assert!(
            matches!(err, NetError::Auth(_)),
            "expected NetError::Auth, got {err:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("authentication rejected"),
            "unexpected error: {msg}"
        );
        assert!(
            msg.contains("invalid password"),
            "error did not propagate server message: {msg}"
        );
        h.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn redis_auth_handshake_handles_peer_close() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            // Accept and immediately drop.
            let _ = listener.accept().await;
        });
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let err = redis_auth_handshake(&mut stream, "x", Duration::from_secs(2))
            .await
            .expect_err("AUTH should fail when peer closes");
        let _ = err; // any error variant is fine; we just want non-Ok
        h.await.unwrap();
    }

    // ---- listen_to_socket_addr: one test per EndpointKind arm ----

    #[test]
    fn listen_to_socket_addr_parses_v4() {
        let l = ConfListen::parse("listen", "127.0.0.1:8102").unwrap();
        let addr = listen_to_socket_addr(&l, "listen").unwrap();
        assert_eq!(addr, "127.0.0.1:8102".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn listen_to_socket_addr_parses_v6() {
        let l = ConfListen::parse("listen", "[::1]:8103").unwrap();
        assert_eq!(l.kind(), EndpointKind::V6);
        let addr = listen_to_socket_addr(&l, "listen").unwrap();
        assert!(addr.is_ipv6());
        assert_eq!(addr.port(), 8103);
    }

    #[test]
    fn listen_to_socket_addr_resolves_hostname() {
        // `localhost` resolves via the std resolver to at least
        // one loopback address: the Hostname arm.
        let l = ConfListen::parse("listen", "localhost:8104").unwrap();
        assert_eq!(l.kind(), EndpointKind::Hostname);
        let addr = listen_to_socket_addr(&l, "listen").unwrap();
        assert!(addr.ip().is_loopback());
        assert_eq!(addr.port(), 8104);
    }

    #[test]
    fn listen_to_socket_addr_rejects_unresolvable_hostname() {
        // RFC 6761 reserves `.invalid` so this never resolves.
        let l = ConfListen::parse("listen", "nonexistent.invalid:8105").unwrap();
        assert_eq!(l.kind(), EndpointKind::Hostname);
        let err = listen_to_socket_addr(&l, "dyn_listen").unwrap_err();
        match err {
            ServerError::BadConfig { field, .. } => assert_eq!(field, "dyn_listen"),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn listen_to_socket_addr_rejects_unix_path() {
        // A leading slash classifies as a Unix path, which the run
        // loop does not yet bind: the UnixPath arm returns BadConfig.
        let l = ConfListen::parse("listen", "/tmp/dynomited.sock").unwrap();
        assert_eq!(l.kind(), EndpointKind::UnixPath);
        let err = listen_to_socket_addr(&l, "listen").unwrap_err();
        assert!(matches!(
            err,
            ServerError::BadConfig {
                field: "listen",
                ..
            }
        ));
    }

    // ---- resolve_advertise_addr ----

    #[test]
    fn advertise_concrete_bind_is_unchanged() {
        let bind: SocketAddr = "10.0.0.5:8101".parse().unwrap();
        assert_eq!(
            resolve_advertise_addr_with(Some(bind), Some("203.0.113.9")),
            Some(bind),
            "a concrete bind IP is advertised as-is, ignoring the env"
        );
    }

    #[test]
    fn advertise_wildcard_with_bare_host_keeps_bind_port() {
        let bind: SocketAddr = "0.0.0.0:8101".parse().unwrap();
        assert_eq!(
            resolve_advertise_addr_with(Some(bind), Some("203.0.113.9")),
            Some("203.0.113.9:8101".parse().unwrap()),
            "a wildcard bind advertises the env host with the bind port"
        );
    }

    #[test]
    fn advertise_wildcard_with_host_port_uses_both() {
        let bind: SocketAddr = "0.0.0.0:8101".parse().unwrap();
        assert_eq!(
            resolve_advertise_addr_with(Some(bind), Some("203.0.113.9:9999")),
            Some("203.0.113.9:9999".parse().unwrap()),
        );
    }

    #[test]
    fn advertise_wildcard_without_env_falls_back_to_bind() {
        let bind: SocketAddr = "0.0.0.0:8101".parse().unwrap();
        assert_eq!(
            resolve_advertise_addr_with(Some(bind), None),
            Some(bind),
            "no override leaves the wildcard bind (logged); the operator must set DYN_ADVERTISE_ADDR"
        );
    }

    #[test]
    fn advertise_ipv6_wildcard_with_bare_host() {
        let bind: SocketAddr = "[::]:8101".parse().unwrap();
        assert_eq!(
            resolve_advertise_addr_with(Some(bind), Some("203.0.113.9")),
            Some("203.0.113.9:8101".parse().unwrap()),
        );
    }

    #[test]
    fn advertise_invalid_env_falls_back_to_bind() {
        let bind: SocketAddr = "0.0.0.0:8101".parse().unwrap();
        assert_eq!(
            resolve_advertise_addr_with(Some(bind), Some("not an address")),
            Some(bind),
        );
    }

    // ---- build_local_peer ----

    #[test]
    fn build_local_peer_uses_dyn_listen_endpoint() {
        let cfg = Config::parse_str(&yaml(7000, 7001, 7002)).unwrap();
        let conf_pool = cfg.pool();
        let pool_config = PoolConfig::default();
        let dyn_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let peer = build_local_peer(conf_pool, &pool_config, Some(dyn_addr)).unwrap();
        // The local peer is index 0 and carries the dyn_listen port.
        assert_eq!(peer.idx(), 0);
        assert_eq!(peer.endpoint().port(), 7001);
    }

    #[test]
    fn build_local_peer_falls_back_when_dyn_listen_absent() {
        let cfg = Config::parse_str(&yaml(7010, 7011, 7012)).unwrap();
        let conf_pool = cfg.pool();
        let pool_config = PoolConfig::default();
        // With no dyn_listen the endpoint defaults to 127.0.0.1:0.
        let peer = build_local_peer(conf_pool, &pool_config, None).unwrap();
        assert_eq!(peer.endpoint().port(), 0);
    }

    #[test]
    fn build_local_peer_errors_when_tokens_missing() {
        // A pool YAML without a `tokens:` directive: the
        // MissingConfig("tokens") arm fires.
        let yaml =
            "p:\n  listen: 127.0.0.1:7020\n  servers:\n  - 127.0.0.1:22122:1\n  data_store: 0\n";
        let cfg = Config::parse_str(yaml).unwrap();
        let conf_pool = cfg.pool();
        let pool_config = PoolConfig::default();
        let err = build_local_peer(conf_pool, &pool_config, None).unwrap_err();
        assert!(matches!(err, ServerError::MissingConfig("tokens")));
    }

    // ---- seed_to_peer: same-dc vs cross-dc ----

    #[test]
    fn seed_to_peer_same_dc_flag() {
        let seed = dynomite::conf::ConfDynSeed::parse("10.0.0.2:8101:rack-b:localdc:200").unwrap();
        let pool_config = PoolConfig::default(); // dc == "localdc"
        let peer = seed_to_peer(3, &seed, &pool_config).unwrap();
        assert_eq!(peer.idx(), 3);
        assert!(peer.is_same_dc());
        assert_eq!(peer.endpoint().port(), 8101);
    }

    #[test]
    fn seed_to_peer_cross_dc_flag() {
        let seed = dynomite::conf::ConfDynSeed::parse("10.0.1.2:8101:rack-c:remotedc:300").unwrap();
        let pool_config = PoolConfig::default(); // dc == "localdc"
        let peer = seed_to_peer(4, &seed, &pool_config).unwrap();
        assert!(!peer.is_same_dc());
    }

    // ---- BackendAddr Display + connect ----

    #[test]
    fn backend_addr_tcp_display() {
        let addr = BackendAddr::Tcp("127.0.0.1:6379".parse().unwrap());
        assert_eq!(format!("{addr}"), "127.0.0.1:6379");
    }

    #[cfg(unix)]
    #[test]
    fn backend_addr_unix_display() {
        let addr = BackendAddr::Unix(PathBuf::from("/run/redis.sock"));
        assert_eq!(format!("{addr}"), "unix:/run/redis.sock");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_addr_tcp_connect_succeeds() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let backend = BackendAddr::Tcp(addr);
        let transport = backend.connect().await.expect("connect should succeed");
        // The transport is tagged Server-role.
        assert_eq!(transport.role(), ConnRole::Server);
        drop(transport);
        h.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn backend_addr_tcp_connect_refused() {
        // Bind then drop to free a port nothing is listening on.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap();
        drop(probe);
        let backend = BackendAddr::Tcp(addr);
        assert!(backend.connect().await.is_err());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn backend_addr_unix_connect_succeeds() {
        let dir = std::env::temp_dir().join(format!("dynomited-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("be.sock");
        let _ = std::fs::remove_file(&path);
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let accept_path = path.clone();
        let h = tokio::spawn(async move {
            let _ = listener.accept().await;
            let _ = accept_path; // keep the binding alive for the connect
        });
        let backend = BackendAddr::Unix(path.clone());
        let transport = backend
            .connect()
            .await
            .expect("unix connect should succeed");
        assert_eq!(transport.role(), ConnRole::Server);
        drop(transport);
        h.await.unwrap();
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    // ---- backoff schedule helpers ----

    #[test]
    fn next_backoff_doubles_then_saturates() {
        assert_eq!(next_backoff_ms(BACKEND_BACKOFF_INIT_MS), 100);
        assert_eq!(next_backoff_ms(100), 200);
        // Saturates at the ceiling rather than overflowing.
        assert_eq!(
            next_backoff_ms(BACKEND_BACKOFF_MAX_MS),
            BACKEND_BACKOFF_MAX_MS
        );
        assert_eq!(next_backoff_ms(u64::MAX), BACKEND_BACKOFF_MAX_MS);
    }

    #[test]
    fn jittered_backoff_stays_in_band_and_never_zero() {
        // Multiplicative jitter is in [0.5, 1.5]; the floor is 1ms.
        for _ in 0..256 {
            let d = jittered_backoff(1000);
            assert!(d >= Duration::from_millis(500));
            assert!(d <= Duration::from_millis(1500));
        }
        // A zero input still yields the 1ms floor, never a 0-sleep.
        assert_eq!(jittered_backoff(0), Duration::from_millis(1));
    }

    #[test]
    fn classify_reconnect_reason_maps_every_variant() {
        use std::io::{Error, ErrorKind};
        assert_eq!(
            classify_reconnect_reason(&NetError::Io(Error::from(ErrorKind::BrokenPipe))),
            "io"
        );
        assert_eq!(classify_reconnect_reason(&NetError::Closed), "closed");
        assert_eq!(classify_reconnect_reason(&NetError::Ejected), "other");
        assert_eq!(classify_reconnect_reason(&NetError::PoolExhausted), "other");
        assert_eq!(classify_reconnect_reason(&NetError::PoolShutdown), "other");
    }

    #[test]
    fn should_log_reconnect_first_three_then_every_tenth() {
        assert!(should_log_reconnect(1));
        assert!(should_log_reconnect(2));
        assert!(should_log_reconnect(3));
        assert!(!should_log_reconnect(4));
        assert!(!should_log_reconnect(9));
        assert!(should_log_reconnect(10));
        assert!(!should_log_reconnect(11));
        assert!(should_log_reconnect(20));
    }

    #[test]
    fn default_gossip_interval_is_positive() {
        // The constant fits an i64, so the unwrap_or fallback path
        // is never taken; the returned cadence is positive.
        assert!(default_gossip_interval_ms_i64() > 0);
    }

    // ---- cancel_future / wait_for_flag ----

    #[tokio::test]
    async fn cancel_future_resolves_when_flag_already_set() {
        let (tx, rx) = watch::channel(true);
        // Flag is already true: the future returns on the first borrow.
        cancel_future(rx).await;
        drop(tx);
    }

    #[tokio::test]
    async fn cancel_future_resolves_on_flag_flip() {
        let (tx, rx) = watch::channel(false);
        let fut = cancel_future(rx);
        let waiter = tokio::spawn(fut);
        tokio::time::sleep(Duration::from_millis(20)).await;
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("cancel_future stuck")
            .unwrap();
    }

    #[tokio::test]
    async fn wait_for_flag_returns_when_sender_dropped() {
        let (tx, mut rx) = watch::channel(false);
        // Dropping the sender closes the channel; wait_for_flag
        // returns on the changed() error rather than hanging.
        drop(tx);
        tokio::time::timeout(Duration::from_secs(2), wait_for_flag(&mut rx))
            .await
            .expect("wait_for_flag stuck");
    }

    // ---- build_proxy: TCP arm ----

    #[tokio::test(flavor = "multi_thread")]
    async fn build_proxy_tcp_binds() {
        let cfg = Config::parse_str(&yaml(free_port(), free_port(), free_port())).unwrap();
        let conf_pool = cfg.pool().clone();
        let pool = Arc::new(ServerPool::new(PoolConfig::default(), Vec::new()));
        let dispatcher = Arc::new(ClusterDispatcher::new(pool));
        let listen_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let kind = build_proxy(
            listen_addr,
            dispatcher,
            dynomite::conf::DataStore::Valkey,
            &conf_pool,
        )
        .await
        .expect("TCP proxy should bind");
        // Default transport is TCP.
        assert!(matches!(kind, ProxyKind::Tcp(_)));
    }
}
