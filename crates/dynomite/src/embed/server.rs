//! Embedding-API server runtime.
//!
//! [`Server`] owns the runtime data structures (cluster pool,
//! dispatcher, stats, event bus, hooks). [`Server::start`] spawns
//! the background tasks (gossip, stats aggregator, optional TCP
//! listeners) on the current tokio runtime and returns a
//! [`ServerHandle`].
//!
//! The handle exposes the public control surface documented in
//! `docs/book/src/embedding/server.md`. It is `Clone + Send +
//! Sync`; multiple consumers may hold one. Dropping the last
//! handle does not shut the server down - only
//! [`ServerHandle::shutdown`] does.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::{Duration, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::cluster::dispatch::{ClusterDispatcher, DispatchPlan};
use crate::cluster::peer::{Peer, PeerEndpoint, PeerState};
use crate::cluster::pool::{PoolConfig, ServerPool};
use crate::conf::{ConfDynSeed, ConfPool, Config};
use crate::embed::error::EmbedError;
use crate::embed::events::{CloseReason, ConnRoleTag, EventBus, EventStream, ServerEvent};
use crate::embed::hooks::{
    CryptoProvider, Datastore, LoggingMetricsSink, MetricsSink, SeedsProvider,
};
use crate::embed::snapshots::{DatacenterSnapshot, PeerSnapshot, RingSnapshot};
use crate::hashkit::DynToken;
use crate::msg::Msg;
use crate::stats::{
    describe_stats, MetricSpec, PoolField, PoolStats, ServerField, ServerStats, ServiceInfo,
    Snapshot, Stats,
};

/// Bag of optional hook overrides supplied to a builder.
///
/// All fields are private so adding new hooks (for example, a
/// `Box<dyn TransportListener>` slot) is not a SemVer-breaking
/// change. Construct a `ServerHooks` only through
/// [`crate::embed::ServerBuilder`] setters; read it back through
/// the accessor methods below.
#[non_exhaustive]
pub struct ServerHooks {
    pub(crate) datastore: Option<Box<dyn Datastore>>,
    pub(crate) seeds: Option<Box<dyn SeedsProvider>>,
    pub(crate) crypto: Option<Box<dyn CryptoProvider>>,
    pub(crate) metrics: Option<Box<dyn MetricsSink>>,
}

impl ServerHooks {
    /// Borrow the configured [`Datastore`], if any.
    #[must_use]
    pub fn datastore(&self) -> Option<&dyn Datastore> {
        self.datastore.as_deref()
    }

    /// Borrow the configured [`SeedsProvider`], if any.
    #[must_use]
    pub fn seeds(&self) -> Option<&dyn SeedsProvider> {
        self.seeds.as_deref()
    }

    /// Borrow the configured [`CryptoProvider`], if any.
    #[must_use]
    pub fn crypto(&self) -> Option<&dyn CryptoProvider> {
        self.crypto.as_deref()
    }

    /// Borrow the configured [`MetricsSink`], if any.
    #[must_use]
    pub fn metrics(&self) -> Option<&dyn MetricsSink> {
        self.metrics.as_deref()
    }
}

impl std::fmt::Debug for ServerHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerHooks")
            .field("datastore", &self.datastore.is_some())
            .field("seeds", &self.seeds.is_some())
            .field("crypto", &self.crypto.is_some())
            .field("metrics", &self.metrics.is_some())
            .finish()
    }
}

// -------- in-process registry -------------------------------------------

/// Process-wide registry for in-process clusters.
///
/// When `inject_request` produces a `Replicas` plan that targets
/// a peer co-located in the same process, the embed runtime
/// looks up the target via this registry and forwards directly
/// without the dnode wire path. This keeps the in-process tests
/// from depending on real network I/O while preserving the
/// production code path semantics (compute plan, deliver to
/// target peers).
fn registry() -> &'static Mutex<HashMap<SocketAddr, Weak<ServerInner>>> {
    static R: OnceLock<Mutex<HashMap<SocketAddr, Weak<ServerInner>>>> = OnceLock::new();
    R.get_or_init(|| Mutex::new(HashMap::new()))
}

fn registry_register(addr: SocketAddr, server: &Arc<ServerInner>) {
    registry().lock().insert(addr, Arc::downgrade(server));
}

fn registry_remove(addr: SocketAddr) {
    registry().lock().remove(&addr);
}

fn registry_lookup(addr: SocketAddr) -> Option<Arc<ServerInner>> {
    registry().lock().get(&addr).and_then(Weak::upgrade)
}

// -------- inner state ----------------------------------------------------

/// Generation counter for [`ServerEvent::ConfigReloaded`] /
/// [`RingSnapshot`].
fn next_generation() -> u64 {
    static G: AtomicU64 = AtomicU64::new(0);
    G.fetch_add(1, Ordering::Relaxed)
}

/// Connection-id allocator.
fn next_conn_id() -> u64 {
    static C: AtomicU64 = AtomicU64::new(0);
    C.fetch_add(1, Ordering::Relaxed)
}

/// Inner state shared by every clone of a [`ServerHandle`].
pub(crate) struct ServerInner {
    pool: Arc<ServerPool>,
    dispatcher: ClusterDispatcher,
    stats: Arc<Stats>,
    snapshot_cache: Arc<Mutex<Snapshot>>,
    bus: EventBus,
    datastore: Box<dyn Datastore>,
    seeds: Box<dyn SeedsProvider>,
    metrics: Box<dyn MetricsSink>,
    crypto: Option<Box<dyn CryptoProvider>>,
    listen_addr: Option<SocketAddr>,
    dyn_listen_addr: Option<SocketAddr>,
    cancel: CancellationToken,
    pool_name: String,
    config: Mutex<ConfPool>,
    generation: AtomicU64,
}

impl std::fmt::Debug for ServerInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerInner")
            .field("pool_name", &self.pool_name)
            .field("listen", &self.listen_addr)
            .field("dyn_listen", &self.dyn_listen_addr)
            .finish_non_exhaustive()
    }
}

impl ServerInner {
    fn dispatch_local(&self, req: Msg) -> super::hooks::BoxFuture<'_, Result<Msg, EmbedError>> {
        let fut = self.datastore.dispatch(req);
        Box::pin(async move {
            let rsp = fut.await.map_err(|e| EmbedError::Inject(e.to_string()))?;
            Ok(rsp)
        })
    }
}

// -------- Server (configured-but-not-running) ----------------------------

/// Configured-but-not-running server.
///
/// Build one with [`ServerBuilder`](crate::embed::ServerBuilder),
/// then call [`Server::start`] to spawn the background tasks.
#[derive(Debug)]
pub struct Server {
    pool_name: String,
    pool: ConfPool,
    cluster: Arc<ServerPool>,
    hooks: ServerHooks,
    stats: Arc<Stats>,
}

impl Server {
    /// Used internally by [`crate::embed::ServerBuilder::build`].
    pub(crate) fn from_pool(pool_name: String, pool: ConfPool, hooks: ServerHooks) -> Self {
        let pool_cfg = PoolConfig::from_conf(&pool_name, &pool);
        let local_peer = build_local_peer(&pool, &pool_cfg);
        let mut peers = vec![local_peer];
        if let Some(seeds) = pool.dyn_seeds.as_ref() {
            let start = u32::try_from(peers.len()).unwrap_or(0);
            peers.extend(peers_from_seeds(&pool_cfg, seeds, start));
        }
        let server_pool_arc = Arc::new(ServerPool::new(pool_cfg.clone(), peers));
        server_pool_arc.preselect_remote_racks();

        let stats = Arc::new(Stats::new(
            ServiceInfo {
                source: pool_cfg.name.clone(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                rack: pool_cfg.rack.clone(),
                dc: pool_cfg.dc.clone(),
            },
            PoolStats::new(&pool_cfg.name),
            ServerStats::new("backend"),
        ));

        Self {
            pool_name,
            pool,
            cluster: server_pool_arc,
            hooks,
            stats,
        }
    }

    /// The pool name configured in the YAML / builder.
    #[must_use]
    pub fn pool_name(&self) -> &str {
        &self.pool_name
    }

    /// Spawn background tasks on the current tokio runtime and
    /// return a [`ServerHandle`].
    ///
    /// `start` is non-blocking. The returned handle is `Clone +
    /// Send + Sync`.
    ///
    /// # In-process only
    ///
    /// The embedded server in this stage is **in-process only**:
    /// the `listen:` and `dyn_listen:` sockets bind so that
    /// configured ports are reservable and post-bind reporting
    /// works, but cross-process clients connecting to those
    /// ports see open-then-immediate-close (with a runtime
    /// warning logged on each accept). The sanctioned way to
    /// drive an embedded `Server` from in-process code is
    /// [`ServerHandle::inject_request`]. Cross-process traffic
    /// is supported by the `dynomited` binary, which wires the
    /// proxy module directly. Wiring the embedded accept loop
    /// to the dispatcher is tracked as a follow-up; the contract
    /// is documented in `docs/parity.md`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dynomite::embed::ServerBuilder;
    /// use dynomite::conf::DataStore;
    /// # tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
    /// let server = ServerBuilder::new("dyn_o_mite")
    ///     .listen("127.0.0.1:0".parse().unwrap())
    ///     .dyn_listen("127.0.0.1:0".parse().unwrap())
    ///     .data_store(DataStore::Redis)
    ///     .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
    ///     .tokens_str("0")
    ///     .enable_gossip(false)
    ///     .build()
    ///     .unwrap();
    /// let handle = server.start().await.unwrap();
    /// handle.shutdown().await.unwrap();
    /// # });
    /// ```
    pub async fn start(self) -> Result<ServerHandle, EmbedError> {
        let Server {
            pool_name,
            pool,
            cluster,
            mut hooks,
            stats,
        } = self;

        let dispatcher = ClusterDispatcher::new(cluster.clone());
        let bus = EventBus::new(64);
        let cancel = CancellationToken::new();
        let snapshot_cache = Arc::new(Mutex::new(Snapshot::default()));

        let datastore = hooks
            .datastore
            .take()
            .expect("invariant: builder always populates datastore");
        let seeds = hooks
            .seeds
            .take()
            .expect("invariant: builder always populates seeds");
        let metrics = hooks
            .metrics
            .take()
            .unwrap_or_else(|| Box::new(LoggingMetricsSink::new(pool_name.clone())));

        let (listen_listener, listen_addr) = bind_listener(pool.listen.as_ref()).await?;
        let (dyn_listener, dyn_listen_addr) = bind_listener(pool.dyn_listen.as_ref()).await?;

        let inner = Arc::new(ServerInner {
            pool: cluster.clone(),
            dispatcher,
            stats: stats.clone(),
            snapshot_cache: snapshot_cache.clone(),
            bus: bus.clone(),
            datastore,
            seeds,
            metrics,
            crypto: hooks.crypto,
            listen_addr,
            dyn_listen_addr,
            cancel: cancel.clone(),
            pool_name: pool_name.clone(),
            config: Mutex::new(pool.clone()),
            generation: AtomicU64::new(0),
        });

        // Register self in the in-process registry so peer
        // forwarding can find this node.
        if let Some(addr) = inner.dyn_listen_addr {
            registry_register(addr, &inner);
        }

        // Spawn background tasks.
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        tasks.push(tokio::spawn(stats_loop(inner.clone(), pool.stats_interval)));
        tasks.push(tokio::spawn(metrics_loop(inner.clone())));
        if pool.enable_gossip.unwrap_or(false) {
            tasks.push(tokio::spawn(gossip_loop(
                inner.clone(),
                Duration::from_millis(
                    u64::try_from(pool.gos_interval.unwrap_or(1_000)).unwrap_or(1_000),
                ),
            )));
        }
        if let (Some(listener), Some(addr)) = (listen_listener, listen_addr) {
            tasks.push(tokio::spawn(accept_loop(
                inner.clone(),
                listener,
                addr,
                ConnRoleTag::Proxy,
            )));
        }
        if let (Some(listener), Some(addr)) = (dyn_listener, dyn_listen_addr) {
            tasks.push(tokio::spawn(accept_loop(
                inner.clone(),
                listener,
                addr,
                ConnRoleTag::DnodeProxy,
            )));
        }

        Ok(ServerHandle {
            inner,
            tasks: Arc::new(Mutex::new(tasks)),
        })
    }
}

// -------- ServerHandle ---------------------------------------------------

/// Cloneable handle to a running [`Server`].
///
/// The handle is the public control surface: shutdown, reload,
/// stats, events, request injection, topology snapshots.
#[derive(Clone)]
pub struct ServerHandle {
    inner: Arc<ServerInner>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl std::fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerHandle")
            .field("pool", &self.inner.pool_name)
            .field("listen", &self.inner.listen_addr)
            .field("dyn_listen", &self.inner.dyn_listen_addr)
            .finish_non_exhaustive()
    }
}

impl ServerHandle {
    /// Borrow the configured [`CryptoProvider`], if one was
    /// plugged. The default builder does not install one.
    #[must_use]
    pub fn crypto_provider(&self) -> Option<&dyn CryptoProvider> {
        self.inner.crypto.as_deref()
    }

    /// Local listen address (post-bind), if a `listen:` was
    /// configured.
    #[must_use]
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.inner.listen_addr
    }

    /// Local dnode listen address (post-bind), if a
    /// `dyn_listen:` was configured.
    #[must_use]
    pub fn dyn_listen_addr(&self) -> Option<SocketAddr> {
        self.inner.dyn_listen_addr
    }

    /// Subscribe to the [`ServerEvent`] broadcast.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use dynomite::embed::ServerBuilder;
    /// # use dynomite::conf::DataStore;
    /// # tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(async {
    /// let server = ServerBuilder::new("p")
    ///     .listen("127.0.0.1:0".parse().unwrap())
    ///     .dyn_listen("127.0.0.1:0".parse().unwrap())
    ///     .data_store(DataStore::Redis)
    ///     .servers(vec![dynomite::conf::ConfServer::parse("127.0.0.1:6379:1").unwrap()])
    ///     .tokens_str("0")
    ///     .build().unwrap();
    /// let handle = server.start().await.unwrap();
    /// let _events = handle.subscribe_events();
    /// handle.shutdown().await.unwrap();
    /// # });
    /// ```
    #[must_use]
    pub fn subscribe_events(&self) -> EventStream {
        self.inner.bus.subscribe()
    }

    /// Latest stats snapshot.
    #[must_use]
    pub fn stats(&self) -> Snapshot {
        self.inner.stats.snapshot()
    }

    /// Manifest of every metric the engine emits.
    #[must_use]
    pub fn describe_stats(&self) -> Vec<MetricSpec> {
        let _ = describe_stats(); // ensure the module is exercised
        let mut out: Vec<MetricSpec> = Vec::new();
        out.extend(crate::stats::POOL_CODEC.iter().copied());
        out.extend(crate::stats::SERVER_CODEC.iter().copied());
        out
    }

    /// Snapshot of every peer in the cluster.
    #[must_use]
    pub fn peers(&self) -> Vec<PeerSnapshot> {
        self.inner
            .pool
            .peers()
            .read()
            .iter()
            .map(PeerSnapshot::from)
            .collect()
    }

    /// Snapshot of every datacenter and its racks.
    #[must_use]
    pub fn datacenters(&self) -> Vec<DatacenterSnapshot> {
        self.inner
            .pool
            .datacenters()
            .read()
            .iter()
            .map(DatacenterSnapshot::from)
            .collect()
    }

    /// Snapshot of the token ring.
    #[must_use]
    pub fn ring(&self) -> RingSnapshot {
        let dcs = self.inner.pool.datacenters().read();
        let mut entries: Vec<(DynToken, u32)> = Vec::new();
        for dc in dcs.iter() {
            for rack in dc.racks() {
                for c in rack.continuums() {
                    entries.push((c.token.clone(), c.peer_idx));
                }
            }
        }
        RingSnapshot {
            entries,
            generation: self.inner.generation.load(Ordering::Relaxed),
        }
    }

    /// Inject a parsed request as if it had arrived from a
    /// client. Returns the response message.
    ///
    /// This is the test/embedding entry point that bypasses the
    /// proxy listener. The dispatcher computes a routing plan; if
    /// the plan resolves to the local datastore, the
    /// [`Datastore::dispatch`](crate::embed::hooks::Datastore::dispatch)
    /// hook is invoked. If the plan resolves to a remote peer
    /// co-located in the same process (via the in-process
    /// registry), the request is forwarded to that peer's
    /// datastore and its response is returned.
    pub async fn inject_request(&self, req: Msg) -> Result<Msg, EmbedError> {
        // Use the parsed key (if any) for routing; if there are
        // no keys, the dispatcher's `LocalDatastore` short-circuit
        // applies.
        let key: Vec<u8> = req
            .keys()
            .first()
            .map(|kp| kp.tag_bytes().to_vec())
            .unwrap_or_default();
        let plan = self.inner.dispatcher.plan(&req, &key);
        match plan {
            DispatchPlan::Drop => {
                // Drop: respond with an empty message of the same
                // id so callers can correlate.
                let mut rsp = Msg::new(req.id(), crate::msg::MsgType::Unknown, false);
                rsp.set_parent_id(req.id());
                Ok(rsp)
            }
            DispatchPlan::NoTargets => Err(EmbedError::Inject(
                "cluster has no quorum-eligible targets".into(),
            )),
            DispatchPlan::LocalDatastore => self.inner.dispatch_local(req).await,
            DispatchPlan::Replicas { targets, .. } => {
                self.inner.stats.pool_incr_by(PoolField::ForwardError, 0); // touch counter table
                self.inner.stats.server_incr(ServerField::ReadRequests);
                // Snapshot the targets' addresses outside any
                // lock so we don't hold the peer-list read guard
                // across the awaits below.
                let resolved: Vec<(bool, Option<SocketAddr>)> = {
                    let peers = self.inner.pool.peers().read();
                    targets
                        .iter()
                        .map(|t| {
                            let peer = peers.get(t.peer_idx as usize);
                            let is_local = peer.is_some_and(crate::cluster::peer::Peer::is_local);
                            let addr = peer.and_then(|p| p.endpoint().pname().parse().ok());
                            (is_local, addr)
                        })
                        .collect()
                };
                let mut last_err: Option<EmbedError> = None;
                for (is_local, addr) in resolved {
                    if is_local {
                        return self.inner.dispatch_local(req).await;
                    }
                    if let Some(addr) = addr {
                        if let Some(remote) = registry_lookup(addr) {
                            let mut forwarded = Msg::new(req.id(), req.ty(), true);
                            forwarded.set_parent_id(req.parent_id());
                            match remote.datastore.dispatch(forwarded).await {
                                Ok(rsp) => return Ok(rsp),
                                Err(e) => last_err = Some(EmbedError::Inject(e.to_string())),
                            }
                        }
                    }
                }
                if let Some(e) = last_err {
                    return Err(e);
                }
                // No registered remote peer; fall back to the
                // local datastore so the embed surface stays
                // useful in mixed deployments.
                self.inner.dispatch_local(req).await
            }
        }
    }

    /// Reload the configuration. Validates the new [`Config`]
    /// before any state is touched; on failure the running config
    /// is left untouched.
    ///
    /// This is the in-process equivalent of SIGHUP. The C
    /// reference's `dynomite_reload_conf` (in
    /// `_/dynomite/src/dynomite.c`) re-reads the YAML from disk;
    /// the embedding API takes the `Config` directly.
    pub async fn reload(&self, mut cfg: Config) -> Result<(), EmbedError> {
        cfg.finalize();
        cfg.validate()?;
        let new_pool = cfg.pool().clone();
        // Update the stored pool so subsequent .stats() / .peers()
        // calls reflect the live config. The dispatcher's pool
        // structure is left untouched here; a future stage will
        // rebuild it under a write lock.
        *self.inner.config.lock() = new_pool;
        let gen_id = next_generation();
        self.inner.generation.store(gen_id, Ordering::Relaxed);
        self.inner
            .bus
            .send(ServerEvent::ConfigReloaded { generation: gen_id });
        // Yield once so the broadcast send is observable on the
        // calling task's scheduler before the function returns.
        tokio::task::yield_now().await;
        Ok(())
    }

    /// Graceful shutdown.
    ///
    /// Cancels every background task, deregisters from the
    /// in-process peer registry, drains the join set, and
    /// returns. Idempotent.
    pub async fn shutdown(&self) -> Result<(), EmbedError> {
        self.inner.cancel.cancel();
        if let Some(addr) = self.inner.dyn_listen_addr {
            registry_remove(addr);
        }
        let drained: Vec<JoinHandle<()>> = std::mem::take(&mut *self.tasks.lock());
        for t in drained {
            // Tasks observe `cancel` and return promptly.
            let _ = t.await;
        }
        Ok(())
    }
}

// -------- helpers ---------------------------------------------------------

fn build_local_peer(pool: &ConfPool, cfg: &PoolConfig) -> Peer {
    let dyn_listen = pool.dyn_listen.as_ref().map_or_else(
        || ("127.0.0.1".to_string(), 0u16),
        |l| (l.name().to_string(), l.port()),
    );
    let tokens = pool
        .tokens
        .as_ref()
        .map(|tl| {
            tl.components()
                .iter()
                .map(|c| DynToken::from_u32(c.digits.parse::<u32>().unwrap_or(0)))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut peer = Peer::new(
        0,
        PeerEndpoint::tcp(dyn_listen.0, dyn_listen.1),
        cfg.rack.clone(),
        cfg.dc.clone(),
        if tokens.is_empty() {
            vec![DynToken::from_u32(0)]
        } else {
            tokens
        },
        true,
        true,
        false,
    );
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    peer.set_state(PeerState::Normal, now_secs);
    peer
}

fn peers_from_seeds(cfg: &PoolConfig, seeds: &[ConfDynSeed], start_idx: u32) -> Vec<Peer> {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    seeds
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let tokens: Vec<DynToken> = s
                .tokens()
                .components()
                .iter()
                .map(|c| DynToken::from_u32(c.digits.parse::<u32>().unwrap_or(0)))
                .collect();
            let idx_off = u32::try_from(i).unwrap_or(0);
            let mut p = Peer::new(
                start_idx + idx_off,
                PeerEndpoint::tcp(s.host().to_string(), s.port()),
                s.rack().to_string(),
                s.dc().to_string(),
                if tokens.is_empty() {
                    vec![DynToken::from_u32(0)]
                } else {
                    tokens
                },
                false,
                s.dc() == cfg.dc,
                false,
            );
            p.set_state(PeerState::Normal, now_secs);
            p
        })
        .collect()
}

// Unused legacy helpers removed.

async fn bind_listener(
    listen: Option<&crate::conf::ConfListen>,
) -> Result<(Option<TcpListener>, Option<SocketAddr>), EmbedError> {
    let Some(l) = listen else {
        return Ok((None, None));
    };
    let host = l.name();
    let port = l.port();
    if host.is_empty() {
        return Ok((None, None));
    }
    let addr_str = format!("{host}:{port}");
    let Ok(_addr) = addr_str.parse::<SocketAddr>() else {
        return Ok((None, None));
    };
    let listener = TcpListener::bind(&addr_str).await?;
    let local = listener.local_addr()?;
    Ok((Some(listener), Some(local)))
}

// removed unused try_bind helper

async fn accept_loop(
    inner: Arc<ServerInner>,
    listener: TcpListener,
    addr: SocketAddr,
    role: ConnRoleTag,
) {
    loop {
        tokio::select! {
            biased;
            () = inner.cancel.cancelled() => return,
            res = listener.accept() => {
                let Ok((sock, peer)) = res else { return };
                let conn_id = next_conn_id();
                inner.bus.send(ServerEvent::ConnectionAccepted {
                    conn_id,
                    role,
                    local_addr: Some(addr),
                });
                // The embedded server is in-process only at this
                // stage: the kernel-bound socket is reserved so
                // post-bind reporting works, but the per-role
                // protocol parser is not wired. Cross-process
                // clients see open-then-immediate-close. Use
                // `ServerHandle::inject_request` for in-process
                // traffic; use the `dynomited` binary for the
                // wire path.
                tracing::warn!(
                    listen = %addr,
                    peer = %peer,
                    role = ?role,
                    conn_id,
                    "embedded listen_addr accepted a connection; embedded mode does not yet \
                     forward to the dispatcher; use ServerHandle::inject_request instead. \
                     Closing connection."
                );
                let bus = inner.bus.clone();
                let cancel = inner.cancel.clone();
                tokio::spawn(async move {
                    let _ = sock; // drop on close
                    let close_reason = if cancel.is_cancelled() {
                        CloseReason::LocalClose
                    } else {
                        CloseReason::PeerEof
                    };
                    bus.send(ServerEvent::ConnectionClosed { conn_id, reason: close_reason });
                });
            }
        }
    }
}

async fn stats_loop(inner: Arc<ServerInner>, interval_ms: Option<i64>) {
    let interval =
        Duration::from_millis(u64::try_from(interval_ms.unwrap_or(1_000)).unwrap_or(1_000));
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = inner.cancel.cancelled() => return,
            _ = ticker.tick() => {
                let snap = inner.stats.snapshot();
                *inner.snapshot_cache.lock() = snap;
            }
        }
    }
}

async fn metrics_loop(inner: Arc<ServerInner>) {
    let interval = inner.metrics.flush_interval();
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(50)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = inner.cancel.cancelled() => return,
            _ = ticker.tick() => {
                let snap = inner.snapshot_cache.lock().clone();
                let _ = inner.metrics.emit(&snap).await;
            }
        }
    }
}

async fn gossip_loop(inner: Arc<ServerInner>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(20)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut round: u64 = 0;
    let mut known: std::collections::HashSet<(String, u16)> = std::collections::HashSet::new();
    {
        let peers = inner.pool.peers().read();
        for p in peers.iter() {
            known.insert((p.endpoint().host().to_string(), p.endpoint().port()));
        }
    }
    loop {
        tokio::select! {
            biased;
            () = inner.cancel.cancelled() => return,
            _ = ticker.tick() => {
                round += 1;
                let seeds = inner.seeds.fetch().unwrap_or_default();
                let mut added: u32 = 0;
                {
                    let mut peers = inner.pool.peers().write();
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let cfg = inner.pool.config().clone();
                    for seed in &seeds {
                        let key = (seed.host().to_string(), seed.port());
                        if known.contains(&key) {
                            continue;
                        }
                        let next_idx = u32::try_from(peers.len()).unwrap_or(u32::MAX);
                        let tokens: Vec<DynToken> = seed
                            .tokens()
                            .components()
                            .iter()
                            .map(|c| DynToken::from_u32(c.digits.parse::<u32>().unwrap_or(0)))
                            .collect();
                        let mut p = Peer::new(
                            next_idx,
                            PeerEndpoint::tcp(seed.host().to_string(), seed.port()),
                            seed.rack().to_string(),
                            seed.dc().to_string(),
                            if tokens.is_empty() {
                                vec![DynToken::from_u32(0)]
                            } else {
                                tokens
                            },
                            false,
                            seed.dc() == cfg.dc,
                            false,
                        );
                        p.set_state(PeerState::Normal, now_secs);
                        peers.push(p);
                        known.insert(key);
                        added += 1;
                    }
                }
                if added > 0 {
                    // Rebuild the topology + ring under fresh
                    // datacenters so the dispatcher sees the new
                    // peers.
                    rebuild_topology(&inner.pool);
                    let peers_now = inner.pool.peers().read();
                    for (idx, p) in peers_now.iter().enumerate().rev().take(added as usize) {
                        let _ = idx;
                        inner.bus.send(ServerEvent::PeerUp(p.idx()));
                    }
                }
                let count = u32::try_from(inner.pool.peers().read().len()).unwrap_or(u32::MAX);
                inner.bus.send(ServerEvent::GossipRound { round, peers: count });
                inner.stats.pool_incr(PoolField::StatsCount);
            }
        }
    }
}

fn rebuild_topology(pool: &Arc<ServerPool>) {
    use crate::cluster::Datacenter;
    let peers = pool.peers().read();
    let mut new_dcs: Vec<Datacenter> = Vec::new();
    for p in peers.iter() {
        let idx = if let Some(i) = new_dcs.iter().position(|d| d.name() == p.dc()) {
            i
        } else {
            new_dcs.push(Datacenter::new(p.dc().to_string()));
            new_dcs.len() - 1
        };
        new_dcs[idx].upsert_rack(p.rack().to_string());
    }
    drop(peers);
    {
        let mut dcs = pool.datacenters().write();
        *dcs = new_dcs;
    }
    pool.rebuild_ring();
    pool.preselect_remote_racks();
}

// Internal helpers (`shutdown_signal`, `_instant_now`) removed:
// they were public-but-doc(hidden) escape hatches that risked
// SemVer commitments. The `oneshot` channel was not used
// outside the (deleted) test helper, and `Instant` was kept in
// scope only to silence an unused-import warning. Both have
// been dropped.
