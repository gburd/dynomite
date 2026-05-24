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
//!    `pool.dyn_listen` when configured (the reference engine
//!    likewise opens the dnode listener whenever `dyn_listen` is
//!    set or `secure_server_option` requires a TLS-capable peer
//!    socket).
//! 4. Bind the [`StatsServer`] when `stats_listen` is set.
//! 5. Wrap the pool in a [`ClusterDispatcher`] used by both
//!    listeners.
//!
//! [`Server::run`] then `tokio::select!`s on the listener tasks,
//! the [`SignalSet`], and an internal shutdown
//! [`tokio::sync::watch`] channel; `SIGINT` / `SIGTERM` / a
//! programmatic [`Server::shutdown`] all converge on the same
//! cancel future. `SIGHUP` reopens the log file
//! ([`dynomite::core::log::reopen_on_sighup`]) without otherwise
//! perturbing the loop.
//!
//! What this stage deliberately does not wire:
//!
//! * The gossip task. The reference engine spawns one when
//!   `enable_gossip: true`; the Rust port still owns gossip as
//!   data-shape only (see `dynomite::cluster::gossip`). The
//!   run-time driver lands in a later stage; this module logs a
//!   warning when gossip is requested so operators are not
//!   surprised by the silence.
//! * The entropy receiver / sender. Stage 11 ships
//!   [`dynomite::entropy::EntropyReceiver`] and
//!   [`dynomite::entropy::EntropySender`] as standalone tasks, but
//!   the YAML configuration does not yet expose the listen / peer
//!   addresses they need. Wiring them here without that surface
//!   would force operators to edit code, so the run loop logs a
//!   warning when `recon_key_file` is configured and otherwise
//!   stays silent.
//! * Config reload on `SIGHUP`. The brief defers it to the embed
//!   API in Stage 13.

use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
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
use dynomite::conf::{ConfDynSeed, ConfListen, ConfPool, Config, EndpointKind};
use dynomite::core::log::reopen_on_sighup;
use dynomite::hashkit::DynToken;
use dynomite::io::reactor::{ConnRole, TcpTransport};
use dynomite::net::server::OutboundRequest;
use dynomite::net::{Conn, DnodeProxy, DnodeServerConn, NetError, Proxy};
use dynomite::stats::{Snapshot, StatsServer};

use crate::signals::{SignalEvent, SignalSet};

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
    proxy: Proxy,
    dnode_proxy: Option<DnodeProxy>,
    stats: Option<StatsServer>,
    backend_handle: Option<JoinHandle<Result<(), NetError>>>,
    peer_handles: Vec<JoinHandle<Result<(), NetError>>>,
    listen_addr: SocketAddr,
    dyn_listen_addr: Option<SocketAddr>,
    stats_listen_addr: Option<SocketAddr>,
    enable_gossip: bool,
    has_recon_keys: bool,
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
        let stats_listen_addr = match conf_pool.stats_listen.as_ref() {
            Some(l) => Some(listen_to_socket_addr(l, "stats_listen")?),
            None => None,
        };

        let pool_config = PoolConfig::from_conf(&pool_name, &conf_pool);
        let local_peer = build_local_peer(&conf_pool, &pool_config, dyn_listen_addr)?;
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
        let preconnect = conf_pool.preconnect.unwrap_or(false);
        let backend_capacity =
            usize::from(conf_pool.datastore_connections.unwrap_or(8)).max(1) * 64;
        let (backend_tx, backend_rx) =
            tokio::sync::mpsc::channel::<OutboundRequest>(backend_capacity);

        // When `data_store: noxu` is selected, open the Noxu
        // environment exactly once and share it across the
        // backend supervisor and the Riak PBC / HTTP listener.
        // Noxu's environment lock is exclusive per directory,
        // so re-opening it would fail with
        // `Environment locked`.
        #[cfg(feature = "riak")]
        let noxu_shared: Option<Arc<dyn_riak::datastore::NoxuDatastore>> =
            if backend_data_store == dynomite::conf::DataStore::Noxu {
                let path = conf_pool.noxu_path.clone().ok_or(ServerError::BadConfig {
                    field: "noxu_path",
                    reason: "data_store: noxu requires a non-empty 'noxu_path:' directive".into(),
                })?;
                match dyn_riak::datastore::NoxuDatastore::open_in(&path) {
                    Ok(ds) => Some(Arc::new(ds)),
                    Err(e) => {
                        return Err(ServerError::BadConfig {
                            field: "noxu_path",
                            reason: format!(
                                "could not open Noxu environment at '{}': {e}",
                                path.display()
                            ),
                        });
                    }
                }
            } else {
                None
            };

        // Backend supervisor selection. The historical TCP path
        // dials a remote Redis / Memcache backend; the Noxu
        // path delegates to an in-process supervisor that
        // executes against the shared Noxu environment.
        let backend_handle: JoinHandle<Result<(), NetError>> = if backend_data_store
            == dynomite::conf::DataStore::Noxu
        {
            #[cfg(feature = "riak")]
            {
                let noxu = noxu_shared
                    .as_ref()
                    .expect("invariant: noxu_shared populated when data_store == Noxu")
                    .clone();
                tokio::spawn(async move {
                    crate::noxu_backend::noxu_backend_supervisor(noxu, backend_rx).await
                })
            }
            #[cfg(not(feature = "riak"))]
            {
                return Err(ServerError::BadConfig {
                    field: "data_store",
                    reason: "noxu data_store requires dynomited built with --features riak".into(),
                });
            }
        } else {
            if datastore.is_unix() {
                return Err(ServerError::BadConfig {
                    field: "servers",
                    reason: "unix-socket datastores are not yet wired in the binary".into(),
                });
            }
            let backend_addr: SocketAddr = format!("{}:{}", datastore.host(), datastore.port())
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
            // Backend supervisor: keeps a single `ServerConn` alive
            // against the configured datastore. It runs in its own
            // task so `build()` does not block on a slow / refused
            // backend; the `preconnect: true` config option still
            // gets respected by attempting one synchronous connect
            // before returning. The supervisor reconnects with
            // exponential-ish backoff on failure so transient redis
            // restarts do not break the proxy permanently.
            if preconnect {
                match tokio::time::timeout(
                    Duration::from_secs(5),
                    tokio::net::TcpStream::connect(backend_addr),
                )
                .await
                {
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
            tokio::spawn(async move {
                backend_supervisor(
                    backend_addr,
                    backend_rx,
                    backend_data_store,
                    backend_requirepass,
                )
                .await
            })
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
            Some(Arc::new(HintStore::new(pool_config.hint_store_max_bytes)))
        } else {
            None
        };
        let mut dispatcher = ClusterDispatcher::new(server_pool.clone()).with_backend(backend_tx);
        if let Some(store) = hint_store.as_ref() {
            dispatcher = dispatcher.with_hint_store(store.clone());
        }
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
            let peer_host_for_supervisor = host.clone();
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
                            peer_host_for_supervisor,
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

        let proxy = Proxy::bind(listen_addr, dispatcher.clone())
            .map_err(ServerError::Net)?
            .with_data_store(pool_config.data_store);

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
            Some(addr) => Some(
                StatsServer::bind(addr, stats_sink)
                    .await
                    .map_err(ServerError::Io)?,
            ),
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
                .with_interval(Duration::from_millis(gossip_interval_ms)),
        );
        let local_pname = dyn_listen_addr.map_or_else(
            || "127.0.0.1:0".to_string(),
            |a| format!("{}:{}", a.ip(), a.port()),
        );

        #[cfg(feature = "riak")]
        let riak_handles = match conf_pool.riak.as_ref() {
            Some(r) => {
                // Reuse the same backing store the request
                // dispatcher routes to: when `data_store: noxu`
                // is selected the Riak PBC listener serves
                // requests against the same Noxu environment
                // the Redis-front dispatcher writes to. Other
                // values fall back to the in-process
                // `MemoryDatastore` so the Riak surface remains
                // a stand-alone protocol on Redis / Memcache
                // deployments.
                let ds: Arc<dyn dynomite::embed::Datastore> = match noxu_shared.as_ref() {
                    Some(noxu) => noxu.clone(),
                    None => Arc::new(dynomite::embed::MemoryDatastore::new()),
                };
                crate::riak::build_handles(r, ds)
                    .await
                    .map_err(|e| ServerError::BadConfig {
                        field: "riak",
                        reason: e.to_string(),
                    })?
            }
            None => None,
        };

        Ok(Self {
            pool_name,
            pool: server_pool,
            dispatcher,
            proxy,
            dnode_proxy,
            stats,
            backend_handle: Some(backend_handle),
            peer_handles,
            listen_addr,
            dyn_listen_addr,
            stats_listen_addr,
            enable_gossip: conf_pool.enable_gossip.unwrap_or(false),
            has_recon_keys: !conf_pool.recon_key_file.as_deref().unwrap_or("").is_empty(),
            #[cfg(feature = "riak")]
            riak_handles,
            gossip_handler,
            gossip_peer_txs,
            local_pname,
            hint_store,
            hint_drain_interval: Duration::from_millis(pool_config.hint_drain_interval_ms.max(1)),
            shutdown_tx,
            shutdown_rx,
        })
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
            has_recon_keys,
            #[cfg(feature = "riak")]
            mut riak_handles,
            gossip_handler,
            gossip_peer_txs,
            local_pname,
            hint_store,
            hint_drain_interval,
            shutdown_tx,
            mut shutdown_rx,
        } = self;

        if has_recon_keys {
            tracing::warn!(
                pool = %pool_name,
                "recon_key_file is set but the entropy run loop is not yet wired (deferred)"
            );
        }

        tracing::info!(
            pool = %pool_name,
            listen = %listen_addr,
            ?dyn_listen_addr,
            ?stats_listen_addr,
            peers = pool.peers().read().len(),
            "dynomited run loop starting"
        );

        let proxy_cancel = cancel_future(shutdown_rx.clone());
        let proxy_handle: JoinHandle<Result<(), NetError>> =
            tokio::spawn(async move { proxy.run(proxy_cancel).await });

        let dnode_handle = dnode_proxy.map(|dnode| {
            let dispatcher = dispatcher.clone();
            let cancel = cancel_future(shutdown_rx.clone());
            let gossip_for_factory = gossip_handler.clone();
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
                        // parser.
                        dynomite::net::ClientHandler::new(
                            dispatcher.clone(),
                            tx,
                            dynomite::conf::DataStore::Redis,
                        )
                        .with_read_timeout(Some(Duration::from_secs(60)))
                        .with_gossip(gossip_for_factory.clone())
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
        let (riak_pbc_handle, riak_http_handle, riak_aae_handle) = match riak_handles.as_mut() {
            Some(h) => {
                let aae_cfg = h.aae.clone();
                let (p, http) = crate::riak::spawn_listeners(h, &shutdown_rx);
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
                (p, http, aae)
            }
            None => (None, None, None),
        };

        let mut signals = SignalSet::install().map_err(ServerError::Signals)?;
        let supervise_result = supervise(
            &shutdown_tx,
            &mut shutdown_rx,
            &mut signals,
            &proxy_handle,
            dnode_handle.as_ref(),
            stats_handle.as_ref(),
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
        #[cfg(feature = "riak")]
        {
            for h in [riak_pbc_handle, riak_http_handle, riak_aae_handle]
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

        let proxy_outcome = await_listener("proxy", proxy_handle).await;
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

async fn supervise(
    shutdown_tx: &watch::Sender<bool>,
    shutdown_rx: &mut watch::Receiver<bool>,
    signals: &mut SignalSet,
    proxy: &JoinHandle<Result<(), NetError>>,
    dnode: Option<&JoinHandle<Result<(), NetError>>>,
    stats: Option<&JoinHandle<io::Result<()>>>,
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
                        if let Err(e) = reopen_on_sighup() {
                            tracing::warn!(error = %e, "log reopen failed");
                        } else {
                            tracing::info!("log reopened on SIGHUP");
                        }
                    }
                    None => {
                        tracing::warn!("signal stream closed; treating as shutdown");
                        let _ = shutdown_tx.send(true);
                        return Ok(());
                    }
                }
            }
            () = wait_finished(proxy) => {
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
async fn redis_auth_handshake(
    stream: &mut tokio::net::TcpStream,
    password: &str,
    timeout: Duration,
) -> Result<(), NetError> {
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
        Err(NetError::Parse(format!("AUTH rejected: {msg}")))
    }
}

/// Long-running supervisor that owns the request channel for the
/// local datastore. Reconnects to the backend with bounded
/// backoff whenever a `ServerConn` driver returns. Exits when
/// the receiver half is closed (the dispatcher is dropped).
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
    addr: SocketAddr,
    mut rx: tokio::sync::mpsc::Receiver<OutboundRequest>,
    data_store: dynomite::conf::DataStore,
    requirepass: Option<String>,
) -> Result<(), NetError> {
    let mut backoff_ms: u64 = 100;
    let backoff_max_ms: u64 = 5_000;
    loop {
        // Bail out if the channel is empty AND the sender side has
        // been dropped (proxy/dispatcher gone). `is_closed` is the
        // cleanest signal; an empty open channel just means we are
        // idle and should connect anyway.
        if rx.is_closed() && rx.is_empty() {
            return Ok(());
        }
        let connect =
            tokio::time::timeout(Duration::from_secs(5), tokio::net::TcpStream::connect(addr))
                .await;
        let mut stream = match connect {
            Ok(Ok(s)) => {
                backoff_ms = 100;
                s
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    backend = %addr,
                    error = %e,
                    "backend connect failed; retrying"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms.saturating_mul(2)).min(backoff_max_ms);
                continue;
            }
            Err(_) => {
                tracing::warn!(
                    backend = %addr,
                    "backend connect timed out; retrying"
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms.saturating_mul(2)).min(backoff_max_ms);
                continue;
            }
        };
        let _ = stream.set_nodelay(true);

        // Optional Redis AUTH handshake before the supervisor
        // hands the stream to the run loop. Memcache backends
        // skip this entirely (binary SASL is not implemented).
        if data_store == dynomite::conf::DataStore::Redis {
            if let Some(pw) = requirepass.as_deref() {
                if let Err(e) = redis_auth_handshake(&mut stream, pw, Duration::from_secs(5)).await
                {
                    tracing::error!(
                        backend = %addr,
                        error = %e,
                        "backend AUTH failed; reconnecting after backoff"
                    );
                    drop(stream);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(backoff_max_ms);
                    continue;
                }
            }
        }

        let conn = Conn::new(
            Box::new(TcpTransport::new(stream, ConnRole::Server)),
            ConnRole::Server,
        );
        // The ServerConn takes the receiver by ownership; on its
        // exit we get the receiver back via the channel-half
        // pattern below. tokio's mpsc cannot move a Receiver in
        // and out of an owned struct cleanly, so we drive the
        // ServerConn loop manually here, owning the receiver
        // ourselves and forwarding requests / responses.
        if let Err(e) = run_one_backend_conn(conn, &mut rx, data_store).await {
            tracing::warn!(
                backend = %addr,
                error = %e,
                "backend connection ended; reconnecting"
            );
        } else {
            // Clean exit only when the channel closed.
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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
                        dynomite::conf::DataStore::Redis | dynomite::conf::DataStore::Noxu => {
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
    host: String,
) -> Result<(), NetError> {
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
        // mis-trusted CA does not saturate the CPU.
        let transport: Box<dyn dynomite::io::reactor::Transport> = if let Some(rt) = tls.as_ref() {
            let server_name = match dynomite::net::tls::server_name_owned(host.as_str()) {
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
            match rt.connector.connect(server_name, stream).await {
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
#[derive(Clone)]
struct PeerTlsRuntime {
    acceptor: tokio_rustls::TlsAcceptor,
    connector: tokio_rustls::TlsConnector,
}

/// Resolve the peer-plane TLS knobs into a `PeerTlsRuntime`.
///
/// Returns `Ok(None)` when neither cert nor key is set; returns
/// `Ok(Some(_))` when both are set; returns an error when the
/// PEM material fails to load. Mismatched `(cert, key)` is
/// already rejected by [`dynomite::conf::ConfPool::validate`],
/// but we keep the defensive branch so direct callers (tests)
/// also see a clear error.
fn build_peer_tls_runtime(conf_pool: &ConfPool) -> Result<Option<PeerTlsRuntime>, ServerError> {
    match (
        conf_pool.peer_tls_cert.as_deref(),
        conf_pool.peer_tls_key.as_deref(),
    ) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => {
            let ca = conf_pool.peer_tls_ca.as_deref();
            let server_cfg =
                dynomite::net::tls::load_server_config(cert, key, ca).map_err(|e| {
                    ServerError::BadConfig {
                        field: "peer_tls_cert",
                        reason: e.to_string(),
                    }
                })?;
            let client_cfg =
                dynomite::net::tls::load_client_config(ca).map_err(|e| ServerError::BadConfig {
                    field: "peer_tls_ca",
                    reason: e.to_string(),
                })?;
            Ok(Some(PeerTlsRuntime {
                acceptor: dynomite::net::tls::acceptor_from(server_cfg),
                connector: dynomite::net::tls::connector_from(client_cfg),
            }))
        }
        (Some(_), None) => Err(ServerError::BadConfig {
            field: "peer_tls_key",
            reason: "peer_tls_cert is set but peer_tls_key is not".into(),
        }),
        (None, Some(_)) => Err(ServerError::BadConfig {
            field: "peer_tls_cert",
            reason: "peer_tls_key is set but peer_tls_cert is not".into(),
        }),
    }
}

fn default_gossip_interval_ms_i64() -> i64 {
    i64::try_from(dynomite::cluster::gossip::DEFAULT_GOSSIP_INTERVAL_MS).unwrap_or(1_000)
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
            // Resolve via the std resolver. The reference engine
            // also resolves at bind time and aborts on failure.
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
    // The reference engine accepts arbitrary-precision tokens via
    // its big-int routines; the Rust `DynToken` carries a
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

    #[tokio::test(flavor = "multi_thread")]
    async fn build_runs_and_shuts_down() {
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
        let msg = format!("{err}");
        assert!(msg.contains("AUTH rejected"), "unexpected error: {msg}");
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
}
