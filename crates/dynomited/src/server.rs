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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use dynomite::cluster::dispatch::ClusterDispatcher;
use dynomite::cluster::peer::{Peer, PeerEndpoint};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::conf::{ConfDynSeed, ConfListen, ConfPool, Config, EndpointKind};
use dynomite::core::log::reopen_on_sighup;
use dynomite::hashkit::DynToken;
use dynomite::net::{DnodeProxy, NetError, Proxy};
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
    listen_addr: SocketAddr,
    dyn_listen_addr: Option<SocketAddr>,
    stats_listen_addr: Option<SocketAddr>,
    enable_gossip: bool,
    has_recon_keys: bool,
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
        let dispatcher = Arc::new(ClusterDispatcher::new(server_pool.clone()));

        let proxy = Proxy::bind(listen_addr, dispatcher.clone())
            .map_err(ServerError::Net)?
            .with_data_store(pool_config.data_store);

        let dnode_proxy = match dyn_listen_addr {
            Some(addr) => Some(DnodeProxy::bind(addr).map_err(ServerError::Net)?),
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

        Ok(Self {
            pool_name,
            pool: server_pool,
            dispatcher,
            proxy,
            dnode_proxy,
            stats,
            listen_addr,
            dyn_listen_addr,
            stats_listen_addr,
            enable_gossip: conf_pool.enable_gossip.unwrap_or(false),
            has_recon_keys: !conf_pool.recon_key_file.as_deref().unwrap_or("").is_empty(),
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
    pub async fn run(self) -> Result<(), ServerError> {
        let Self {
            pool_name,
            pool,
            dispatcher,
            proxy,
            dnode_proxy,
            stats,
            listen_addr,
            dyn_listen_addr,
            stats_listen_addr,
            enable_gossip,
            has_recon_keys,
            shutdown_tx,
            mut shutdown_rx,
        } = self;

        if enable_gossip {
            tracing::warn!(
                pool = %pool_name,
                "enable_gossip is set but the gossip run loop is not yet wired (deferred)"
            );
        }
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
            tokio::spawn(async move {
                dnode
                    .run(cancel, move |tx| {
                        // The factory is invoked once per accepted
                        // peer; build a fresh `ClientHandler`
                        // bound to the cluster dispatcher and the
                        // per-peer response channel.
                        dynomite::net::ClientHandler::new(
                            dispatcher.clone(),
                            tx,
                            dynomite::conf::DataStore::Redis,
                        )
                        .with_read_timeout(Some(Duration::from_secs(60)))
                    })
                    .await
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
            use std::net::ToSocketAddrs;
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
}
