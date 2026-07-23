//! Optional Riak protocol surface wiring for `dynomited`.
//!
//! This module is compiled only when `dynomited` is built with the
//! `riak` Cargo feature. It hosts:
//!
//! * [`RiakHandles`] -- the per-server bundle of bound listeners and
//!   join handles that [`crate::server::Server::run`] joins on alongside
//!   the existing client / dnode / stats listeners.
//! * [`build_handles`] -- the eager-bind constructor invoked from
//!   [`crate::server::Server::build`] when a `riak:` block is set.
//! * [`PeerChannelRepairSink`] -- a [`dyniak::aae::RepairSink`]
//!   adapter that trampolines repair tasks onto the dispatcher's
//!   per-peer outbound channels (the same `mpsc::Sender<OutboundRequest>`
//!   map the data-plane uses).
//!
//! No code in this module is reachable without the feature.
//! Without the feature the YAML `riak:` block is parsed and
//! validated but ignored at runtime, preserving the legacy
//! Redis / Memcache behaviour bit-identically.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use dynomite::conf::ConfRiak;
use dynomite::embed::Datastore;
use dynomite::net::server::OutboundRequest;
use dynomite::proto::dnode::DmsgType;

use dyniak::aae::config::{ConfAae, DEFAULT_FULL_SWEEP_SECONDS, DEFAULT_SEGMENT_SECONDS};
use dyniak::aae::repair::{RepairSink, RepairTask};
use dyniak::aae::scheduler::{Scheduler, SweepPlan, SystemClock};
use dyniak::proto::replica_wire::encode_peer_op;
use dyniak::replication::{RingPoint, RingView};
use dyniak::router::{BucketRouter, PeerOp, PeerOutbound, RoutingHooks};
use dyniak::{serve_http, serve_http_tls, serve_pbc, serve_pbc_tls, serve_pbc_with_routing};
use dyniak::{serve_http_tls_with_routing, serve_http_with_routing};
#[cfg(feature = "search")]
use dyniak::{serve_http_tls_with_search, serve_http_with_search};
#[cfg(all(feature = "search", feature = "wasm"))]
use dyniak::{serve_http_tls_with_search_and_wasm, serve_http_with_search_and_wasm};
#[cfg(feature = "wasm")]
use dyniak::{serve_http_tls_with_wasm, serve_http_with_wasm};

#[cfg(feature = "quic")]
use dyniak::serve_pbc_quic;
#[cfg(feature = "quic")]
use dynomite::net::quic::{QuicConfig, QuicListener};

#[cfg(feature = "wasm")]
use dyniak::mapreduce::wasm::{load_modules_from_config, WasmLimits, WasmModuleStore};

/// Errors produced while binding the Riak listeners.
#[derive(Debug, thiserror::Error)]
pub enum RiakWireError {
    /// Failed to parse a `host:port` listen address.
    #[error("riak: invalid '{field}' address '{value}': {reason}")]
    BadAddr {
        /// YAML field name.
        field: &'static str,
        /// Address as configured.
        value: String,
        /// Underlying reason.
        reason: String,
    },
    /// I/O failure binding a listener.
    #[error("riak: io binding listener: {0}")]
    Io(#[from] std::io::Error),
    /// A `quic_listen` address was configured but `dynomited`
    /// was built without the `quic` Cargo feature. Surfaced at
    /// startup so the operator sees a clean error rather than a
    /// silently ignored directive, mirroring how the engine's
    /// QUIC client-plane rejects `transport: quic` without the
    /// feature.
    #[error("riak: quic_listen '{0}' requires dynomited built with the `quic` Cargo feature")]
    QuicUnsupported(String),
    /// Loading a Wasm module from disk or compiling it failed.
    #[error("riak: wasm module load failed: {0}")]
    Wasm(String),
}

/// Bound listeners and tasks owned by [`crate::server::Server`].
///
/// Constructed by [`build_handles`] when at least one of the Riak
/// addresses is configured. The listener fields are consumed by
/// [`spawn_listeners`] when the run loop starts; once consumed,
/// only the resolved-address fields and the AAE config remain
/// for inspection.
pub struct RiakHandles {
    /// Bound TCP listener for the PBC accept loop. Taken by
    /// [`spawn_listeners`].
    pub pbc_listener: Option<TcpListener>,
    /// Bound TCP listener for the HTTP gateway. Taken by
    /// [`spawn_listeners`].
    pub http_listener: Option<TcpListener>,
    /// Resolved address of the PBC listener. Stays populated
    /// across the [`spawn_listeners`] handoff.
    pub pbc_addr: Option<SocketAddr>,
    /// Resolved address of the HTTP gateway. Stays populated
    /// across the [`spawn_listeners`] handoff.
    pub http_addr: Option<SocketAddr>,
    /// Bound QUIC listener for the PBC accept loop. Taken by
    /// [`spawn_listeners`]. Present only when the binary is
    /// built with the `quic` Cargo feature and a `quic_listen`
    /// address was configured.
    #[cfg(feature = "quic")]
    pub quic_listener: Option<QuicListener>,
    /// Resolved address of the QUIC PBC listener. Stays
    /// populated across the [`spawn_listeners`] handoff.
    #[cfg(feature = "quic")]
    pub quic_addr: Option<SocketAddr>,
    /// Datastore the listeners delegate to. Shared between the
    /// PBC and HTTP loops so request accounting accumulates in
    /// one place.
    pub datastore: Arc<dyn Datastore>,
    /// Routing hooks for a `data_store: dyniak` pool. When
    /// present, the PBC accept loop serves
    /// [`serve_pbc_with_routing`] so `RpbPutReq` / `RpbDelReq`
    /// frames fan out to the object's replica peers over the
    /// dnode plane before the local store call. `None` for a
    /// single-node pool (no peers) or a non-dyniak backend; the
    /// PBC loop then serves the plain [`serve_pbc`] path.
    ///
    /// HTTP-gateway replica routing is a scoped follow-up; the
    /// HTTP object/transaction handlers still write node-local
    /// (see the wire-up journal entry).
    pub hooks: Option<RoutingHooks>,
    /// Optional vector-index registry shared with the dispatcher's
    /// RediSearch FT.* surface. When present, the HTTP gateway
    /// serves the bucket index-management and search routes against
    /// it via [`dyniak::serve_http_with_search`]; when absent those
    /// routes reply `501 Not Implemented`. Populated by
    /// [`crate::server`] only when the binary is built with the
    /// `search` Cargo feature and a `data_store: dyniak` pool is
    /// configured.
    #[cfg(feature = "search")]
    pub search_registry: Option<Arc<dynomite_search::VectorRegistry>>,
    /// Materialised AAE configuration. `None` when the YAML did
    /// not enable AAE.
    pub aae: Option<ConfAae>,
    /// Optional TLS acceptor; when `Some`, both the PBC and HTTP
    /// listeners terminate TLS for every accepted connection.
    /// Computed by [`build_handles`] from
    /// [`ConfRiak::tls_cert`] / [`ConfRiak::tls_key`] /
    /// [`ConfRiak::tls_ca`].
    pub tls: Option<tokio_rustls::TlsAcceptor>,
    /// Optional Wasm phase store loaded from
    /// [`ConfRiak::wasm_modules`]. Populated when `dynomited`
    /// is built with the `wasm` Cargo feature and the YAML
    /// list is non-empty; otherwise `None`. The MapReduce
    /// executor uses the store via
    /// [`dyniak::mapreduce::run_job_with_wasm`] when a job
    /// references a `Phase::WasmModule`.
    #[cfg(feature = "wasm")]
    pub wasm: Option<Arc<WasmModuleStore>>,
}

/// Eagerly bind the Riak listeners described by `riak`.
///
/// Returns `Ok(None)` when none of `pbc_listen`, `http_listen`,
/// or `quic_listen` is set; otherwise returns a fully-populated
/// [`RiakHandles`]. Bind failures surface here rather than from
/// the run loop, so `Server::build` can crash-on-construct in
/// the same way the existing `Proxy::bind` calls do.
///
/// When `quic_listen` is set but the binary was built without
/// the `quic` Cargo feature, this returns
/// [`RiakWireError::QuicUnsupported`] so the operator sees a
/// clean configuration error at startup.
///
/// # Errors
///
/// [`RiakWireError`] when an address fails to parse or a
/// `bind()` returns an I/O error.
pub async fn build_handles(
    riak: &ConfRiak,
    datastore: Arc<dyn Datastore>,
) -> Result<Option<RiakHandles>, RiakWireError> {
    // Reject `quic_listen` at build time when the feature is
    // absent so the operator sees a clean error rather than a
    // silently ignored directive. Mirrors how `build_proxy`
    // rejects `transport: quic` without the engine feature.
    #[cfg(not(feature = "quic"))]
    if let Some(addr) = riak.quic_listen.as_deref() {
        return Err(RiakWireError::QuicUnsupported(addr.to_string()));
    }

    if riak.pbc_listen.is_none() && riak.http_listen.is_none() && riak.quic_listen.is_none() {
        return Ok(None);
    }

    let (pbc_listener, pbc_addr) = match riak.pbc_listen.as_deref() {
        Some(s) => {
            let (a, l) = bind("pbc_listen", s).await?;
            (Some(l), Some(a))
        }
        None => (None, None),
    };
    let (http_listener, http_addr) = match riak.http_listen.as_deref() {
        Some(s) => {
            let (a, l) = bind("http_listen", s).await?;
            (Some(l), Some(a))
        }
        None => (None, None),
    };
    #[cfg(feature = "quic")]
    let (quic_listener, quic_addr) = bind_quic_listener(riak).await?;

    let aae = if riak.aae_enabled.unwrap_or(false) {
        let cfg = ConfAae {
            enabled: true,
            full_sweep_interval_seconds: riak
                .aae_full_sweep_interval_seconds
                .unwrap_or(DEFAULT_FULL_SWEEP_SECONDS),
            segment_interval_seconds: riak
                .aae_segment_interval_seconds
                .unwrap_or(DEFAULT_SEGMENT_SECONDS),
            ..ConfAae::default()
        };
        cfg.validate().map_err(|e| RiakWireError::BadAddr {
            field: "aae",
            value: format!(
                "full={} segment={}",
                cfg.full_sweep_interval_seconds, cfg.segment_interval_seconds
            ),
            reason: e,
        })?;
        Some(cfg)
    } else {
        None
    };

    Ok(Some(RiakHandles {
        pbc_listener,
        http_listener,
        pbc_addr,
        http_addr,
        #[cfg(feature = "quic")]
        quic_listener,
        #[cfg(feature = "quic")]
        quic_addr,
        datastore,
        hooks: None,
        #[cfg(feature = "search")]
        search_registry: None,
        aae,
        tls: build_riak_tls_acceptor(riak)?,
        #[cfg(feature = "wasm")]
        wasm: build_wasm_store_from_config(riak)?,
    }))
}

/// Build the optional `TlsAcceptor` for the Riak listeners from
/// the YAML knobs. Returns `Ok(None)` when neither cert nor key
/// is set; returns an error when the PEM material fails to load
/// or the (cert, key) pair is mismatched.
fn build_riak_tls_acceptor(
    riak: &ConfRiak,
) -> Result<Option<tokio_rustls::TlsAcceptor>, RiakWireError> {
    match (riak.tls_cert.as_deref(), riak.tls_key.as_deref()) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => {
            let server_cfg =
                dynomite::net::tls::load_server_config(cert, key, riak.tls_ca.as_deref()).map_err(
                    |e| RiakWireError::BadAddr {
                        field: "tls_cert",
                        value: cert.display().to_string(),
                        reason: e.to_string(),
                    },
                )?;
            Ok(Some(dynomite::net::tls::acceptor_from(server_cfg)))
        }
        (Some(_), None) => Err(RiakWireError::BadAddr {
            field: "tls_key",
            value: String::new(),
            reason: "tls_cert is set but tls_key is not".into(),
        }),
        (None, Some(_)) => Err(RiakWireError::BadAddr {
            field: "tls_cert",
            value: String::new(),
            reason: "tls_key is set but tls_cert is not".into(),
        }),
    }
}

/// Bind the optional QUIC PBC listener described by
/// [`ConfRiak::quic_listen`].
///
/// Returns `(None, None)` when `quic_listen` is unset. When set,
/// the listener reuses the [`ConfRiak::tls_cert`] /
/// [`ConfRiak::tls_key`] pair (QUIC mandates TLS); both must be
/// present and UTF-8, otherwise a [`RiakWireError::BadAddr`] is
/// returned. Bind failures (bad address, UDP socket error)
/// surface here so `Server::build` crashes on construct in the
/// same way the TCP `bind()` calls do.
///
/// Available only when the binary is built with the `quic` Cargo
/// feature.
#[cfg(feature = "quic")]
async fn bind_quic_listener(
    riak: &ConfRiak,
) -> Result<(Option<QuicListener>, Option<SocketAddr>), RiakWireError> {
    let Some(addr_str) = riak.quic_listen.as_deref() else {
        return Ok((None, None));
    };
    let addr = addr_str
        .parse::<SocketAddr>()
        .map_err(|e| RiakWireError::BadAddr {
            field: "quic_listen",
            value: addr_str.to_string(),
            reason: e.to_string(),
        })?;
    let (Some(cert), Some(key)) = (riak.tls_cert.as_deref(), riak.tls_key.as_deref()) else {
        return Err(RiakWireError::BadAddr {
            field: "quic_listen",
            value: addr_str.to_string(),
            reason: "quic_listen requires tls_cert and tls_key to also be set".into(),
        });
    };
    let cert = cert.to_str().ok_or_else(|| RiakWireError::BadAddr {
        field: "tls_cert",
        value: cert.display().to_string(),
        reason: "quic_listen requires a UTF-8 tls_cert path".into(),
    })?;
    let key = key.to_str().ok_or_else(|| RiakWireError::BadAddr {
        field: "tls_key",
        value: key.display().to_string(),
        reason: "quic_listen requires a UTF-8 tls_key path".into(),
    })?;
    let cfg = QuicConfig::server_with_cert_paths(cert, key);
    let listener = QuicListener::bind(addr, cfg).await?;
    let local = listener.local_addr();
    Ok((Some(listener), Some(local)))
}

/// Build the optional [`WasmModuleStore`] for the Riak
/// MapReduce executor from [`ConfRiak::wasm_modules`].
///
/// Returns `Ok(None)` when the YAML list is absent or empty;
/// otherwise reads each path from disk and registers it on a
/// fresh [`WasmModuleStore`] under the configured `id`.
///
/// # Errors
///
/// [`RiakWireError::Wasm`] when a path cannot be read or the
/// bytes fail to compile as a Wasm module.
#[cfg(feature = "wasm")]
pub fn build_wasm_store_from_config(
    riak: &ConfRiak,
) -> Result<Option<Arc<WasmModuleStore>>, RiakWireError> {
    let Some(modules) = riak.wasm_modules.as_deref() else {
        return Ok(None);
    };
    if modules.is_empty() {
        return Ok(None);
    }
    let pairs: Vec<(String, std::path::PathBuf)> = modules
        .iter()
        .map(|m| (m.id.clone(), m.path.clone()))
        .collect();
    let store = load_modules_from_config(&pairs, WasmLimits::default())
        .map_err(|e| RiakWireError::Wasm(e.to_string()))?;
    Ok(Some(Arc::new(store)))
}

/// The join handles spawned for the Riak listener loops, in
/// `(pbc, http, quic)` order. Any element is `None` when the
/// corresponding listener was not configured; `quic` is always
/// `None` unless the binary is built with the `quic` Cargo
/// feature.
type ListenerHandles = (
    Option<JoinHandle<()>>,
    Option<JoinHandle<()>>,
    Option<JoinHandle<()>>,
);

/// Spawn the Riak listener tasks.
///
/// Consumes the bound listeners and returns join handles whose
/// lifetimes the caller (the run loop) is responsible for.
/// `cancel_rx` resolves when graceful shutdown is requested;
/// all listener loops exit shortly after.
///
/// # Returns
///
/// A `ListenerHandles` tuple of `(pbc_handle, http_handle,
/// quic_handle)`; any may be `None` when the corresponding
/// address was not configured. `quic_handle` is always `None`
/// unless the binary is built with the `quic` Cargo feature.
pub fn spawn_listeners(
    handles: &mut RiakHandles,
    cancel_rx: &watch::Receiver<bool>,
) -> ListenerHandles {
    let pbc = handles.pbc_listener.take().map(|listener| {
        let ds = Arc::clone(&handles.datastore);
        let mut cancel = cancel_rx.clone();
        let tls = handles.tls.clone();
        let hooks = handles.hooks.clone();
        tokio::spawn(async move {
            let serve = async {
                match (tls, hooks) {
                    // Routing hooks and TLS are independent knobs;
                    // the routing-enabled PBC serve does not have a
                    // TLS sibling yet, so a dyniak pool with TLS on
                    // the Riak listener falls back to the plain TLS
                    // path (replica routing is then scoped to the
                    // plaintext peer plane). This mirrors how the
                    // QUIC path also skips routing.
                    (Some(acc), _) => serve_pbc_tls(listener, ds, acc).await,
                    (None, Some(hooks)) => {
                        let admin: Arc<dyn dynomite::cluster::ClusterAdmin> =
                            Arc::new(dynomite::cluster::NoopClusterAdmin);
                        serve_pbc_with_routing(listener, ds, admin, hooks).await
                    }
                    (None, None) => serve_pbc(listener, ds).await,
                }
            };
            tokio::select! {
                res = serve => {
                    if let Err(e) = res {
                        tracing::warn!(error = %e, "riak pbc listener exited with error");
                    }
                }
                () = wait_flag(&mut cancel) => {}
            }
        })
    });
    let http = handles.http_listener.take().map(|listener| {
        let ds = Arc::clone(&handles.datastore);
        let mut cancel = cancel_rx.clone();
        let tls = handles.tls.clone();
        let http_hooks = handles.hooks.clone();
        #[cfg(feature = "search")]
        let registry = handles.search_registry.clone();
        #[cfg(feature = "wasm")]
        let wasm = handles.wasm.clone();
        tokio::spawn(async move {
            let serve = async {
                // Cross-node replica routing takes priority: when the
                // pool is a distributed `data_store: dyniak` pool the
                // HTTP object PUT / DELETE must fan out to the key's
                // replicas, mirroring the PBC routing path. The
                // routing entry does not layer search / wasm; those
                // are orthogonal read-side surfaces served by the
                // non-routing variants below when no hooks are set.
                if let Some(hooks) = http_hooks {
                    return if let Some(acc) = tls {
                        serve_http_tls_with_routing(listener, ds, hooks, acc).await
                    } else {
                        serve_http_with_routing(listener, ds, hooks).await
                    };
                }
                #[cfg(all(feature = "search", feature = "wasm"))]
                if let (Some(registry), Some(wasm)) = (registry.clone(), wasm.clone()) {
                    return if let Some(acc) = tls {
                        serve_http_tls_with_search_and_wasm(listener, ds, registry, wasm, acc).await
                    } else {
                        serve_http_with_search_and_wasm(listener, ds, registry, wasm).await
                    };
                }
                #[cfg(feature = "wasm")]
                if let Some(wasm) = wasm {
                    return if let Some(acc) = tls {
                        serve_http_tls_with_wasm(listener, ds, wasm, acc).await
                    } else {
                        serve_http_with_wasm(listener, ds, wasm).await
                    };
                }
                #[cfg(feature = "search")]
                if let Some(registry) = registry {
                    return if let Some(acc) = tls {
                        serve_http_tls_with_search(listener, ds, registry, acc).await
                    } else {
                        serve_http_with_search(listener, ds, registry).await
                    };
                }
                if let Some(acc) = tls {
                    serve_http_tls(listener, ds, acc).await
                } else {
                    serve_http(listener, ds).await
                }
            };
            tokio::select! {
                res = serve => {
                    if let Err(e) = res {
                        tracing::warn!(error = %e, "riak http gateway exited with error");
                    }
                }
                () = wait_flag(&mut cancel) => {}
            }
        })
    });
    let quic = spawn_quic_listener(handles, cancel_rx);
    (pbc, http, quic)
}

/// Spawn the QUIC PBC listener task, if one is bound.
///
/// The accept loop drives [`serve_pbc_quic`], which reuses the
/// same per-connection handler the TCP and TLS-over-TCP paths
/// use. Returns `None` when no QUIC listener was bound.
///
/// Available only when the binary is built with the `quic` Cargo
/// feature.
#[cfg(feature = "quic")]
fn spawn_quic_listener(
    handles: &mut RiakHandles,
    cancel_rx: &watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    handles.quic_listener.take().map(|listener| {
        let ds = Arc::clone(&handles.datastore);
        let mut cancel = cancel_rx.clone();
        tokio::spawn(async move {
            let serve = serve_pbc_quic(listener, ds);
            tokio::select! {
                res = serve => {
                    if let Err(e) = res {
                        tracing::warn!(error = %e, "riak pbc quic listener exited with error");
                    }
                }
                () = wait_flag(&mut cancel) => {}
            }
        })
    })
}

/// QUIC support is compiled out without the `quic` feature; the
/// listener is never bound, so the spawn is a no-op.
#[cfg(not(feature = "quic"))]
fn spawn_quic_listener(
    _handles: &mut RiakHandles,
    _cancel_rx: &watch::Receiver<bool>,
) -> Option<JoinHandle<()>> {
    None
}

/// Spawn the Riak active anti-entropy scheduler task.
///
/// The scheduler ticks at the configured `segment_interval`
/// cadence; each tick is logged at `debug` level. This task does
/// not yet perform divergence detection: it exists so the
/// configured cadence is observable and the [`PeerChannelRepairSink`]
/// is referenced (so the dispatcher's per-peer outbound channels
/// remain the canonical repair sink wiring).
///
/// `peer_txs` is the same `(peer_idx, pname, sender)` triple
/// the gossip task and the hint drainer use; the AAE task
/// holds a reference so real `RepairTask`s can later be routed
/// without re-plumbing the supervisor.
pub fn spawn_aae(
    cfg: ConfAae,
    peer_txs: Vec<(u32, String, mpsc::Sender<OutboundRequest>)>,
    mut cancel_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let clock = Arc::new(SystemClock);
        let scheduler: Scheduler<SystemClock> = Scheduler::new(cfg.clone(), clock);
        let peer_idxs: Vec<u32> = peer_txs.iter().map(|(idx, _, _)| *idx).collect();
        scheduler.install_plan(SweepPlan::new(&peer_idxs, cfg.n_time_buckets, &cfg));
        let _sink = Arc::new(PeerChannelRepairSink::new(peer_txs));
        let interval = cfg.segment_interval();
        let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = wait_flag(&mut cancel_rx) => return,
                _ = ticker.tick() => {
                    if let Some(tick) = scheduler.poll() {
                        tracing::debug!(
                            target: "dynomite::riak::aae",
                            peer_idx = tick.peer_idx,
                            time_bucket = tick.time_bucket,
                            "riak aae tick"
                        );
                    }
                }
            }
        }
    })
}

/// [`RepairSink`] adapter that routes [`RepairTask`]s onto the
/// dispatcher's per-peer outbound channel map.
///
/// The map mirrors the data-plane channels used by the gossip
/// and hint-drainer subsystems, so an enqueued repair travels
/// the same code path as a normal forwarded request.
///
/// # Examples
///
/// ```
/// use dynomite::net::server::OutboundRequest;
/// use dyniak::aae::repair::{RepairDirection, RepairSink, RepairTask};
/// use dynomited::riak::PeerChannelRepairSink;
///
/// let rt = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()
///     .unwrap();
/// rt.block_on(async {
///     let (tx, _rx) = tokio::sync::mpsc::channel::<OutboundRequest>(8);
///     let sink = PeerChannelRepairSink::new(vec![(7, "peer-7".into(), tx)]);
///     let task = RepairTask {
///         peer_idx: 7,
///         bucket: b"users".to_vec(),
///         key: b"alice".to_vec(),
///         vclock: b"vc1".to_vec(),
///         direction: RepairDirection::PushToRemote,
///     };
///     sink.submit(task).unwrap();
/// });
/// ```
pub struct PeerChannelRepairSink {
    by_peer: std::collections::HashMap<u32, mpsc::Sender<OutboundRequest>>,
}

impl PeerChannelRepairSink {
    /// Wrap the dispatcher's per-peer outbound channel map.
    #[must_use]
    pub fn new(peer_txs: Vec<(u32, String, mpsc::Sender<OutboundRequest>)>) -> Self {
        let mut by_peer = std::collections::HashMap::new();
        for (idx, _, tx) in peer_txs {
            by_peer.insert(idx, tx);
        }
        Self { by_peer }
    }
}

impl RepairSink for PeerChannelRepairSink {
    fn submit(&self, task: RepairTask) -> Result<(), RepairTask> {
        let Some(tx) = self.by_peer.get(&task.peer_idx) else {
            return Err(task);
        };
        // Encode the repair as a forwarded request payload.
        // The shape mirrors the hint drainer: a single binary
        // blob carrying the bucket, key, and vclock; the
        // receiving peer interprets it via the AAE-specific
        // dnode framer landing in a follow-up slice.
        let mut payload =
            Vec::with_capacity(task.bucket.len() + task.key.len() + task.vclock.len() + 6);
        payload.extend_from_slice(b"AAE:");
        payload.extend_from_slice(&task.bucket);
        payload.push(b'/');
        payload.extend_from_slice(&task.key);
        payload.push(b':');
        payload.extend_from_slice(&task.vclock);
        let (rsp_tx, mut rsp_rx) =
            tokio::sync::mpsc::channel::<dynomite::net::dispatcher::OutboundEnvelope>(1);
        // Drop replies on the floor; the next sweep retries on
        // mismatch.
        tokio::spawn(async move { while rsp_rx.recv().await.is_some() {} });
        let req = OutboundRequest {
            bytes: payload,
            req_id: 0,
            responder: rsp_tx,
            span: tracing::Span::none(),
            ty: dynomite::proto::dnode::DmsgType::ReqForward,
            target_peer_idx: Some(task.peer_idx),
        };
        match tx.try_send(req) {
            Ok(()) => Ok(()),
            Err(_) => Err(task),
        }
    }
}

/// [`PeerOutbound`] that forwards replica ops onto the dispatcher's
/// per-peer outbound channels.
///
/// This reuses the same `mpsc::Sender<OutboundRequest>` map the
/// gossip task, the hint drainer, and the RESP data plane use
/// (`ClusterDispatcher::peer_backends`). Each replica op is encoded
/// with [`encode_peer_op`] and shipped as an [`OutboundRequest`]
/// tagged [`DmsgType::RiakReplica`]; the receiving peer's
/// `dnode_client_loop` recognises that type, applies the op to its
/// local store via the [`dyniak::ReplicaApplier`] sink, and does
/// not re-forward it.
///
/// The dispatch is fire-and-forget: a missing peer, a closed
/// channel, or a full channel logs at `debug` and drops the op
/// rather than blocking or panicking. The write still lands
/// locally and on every reachable replica; anti-entropy and
/// read-repair reconcile a peer that missed it, matching Riak's
/// eventual-consistency model.
#[derive(Debug)]
pub struct PeerChannelOutbound {
    by_peer: std::collections::HashMap<u32, mpsc::Sender<OutboundRequest>>,
}

impl PeerChannelOutbound {
    /// Wrap the dispatcher's per-peer outbound channel map.
    #[must_use]
    pub fn new(peer_txs: &[(u32, String, mpsc::Sender<OutboundRequest>)]) -> Self {
        let mut by_peer = std::collections::HashMap::new();
        for (idx, _, tx) in peer_txs {
            by_peer.insert(*idx, tx.clone());
        }
        Self { by_peer }
    }

    /// Ship one encoded replica op to `peer_idx`, dropping it on
    /// any delivery failure.
    fn deliver(&self, peer_idx: u32, op: &PeerOp) {
        let Some(tx) = self.by_peer.get(&peer_idx) else {
            tracing::debug!(peer_idx, "riak replica: no outbound channel; dropped");
            return;
        };
        // A replica op is fire-and-forget: the responder is never
        // signalled (RiakReplica is not a data-plane request type),
        // so drain any stray reply on a throwaway task.
        let (rsp_tx, mut rsp_rx) =
            tokio::sync::mpsc::channel::<dynomite::net::dispatcher::OutboundEnvelope>(1);
        tokio::spawn(async move { while rsp_rx.recv().await.is_some() {} });
        let req = OutboundRequest {
            bytes: encode_peer_op(op),
            req_id: 0,
            responder: rsp_tx,
            span: tracing::Span::none(),
            ty: DmsgType::RiakReplica,
            target_peer_idx: Some(peer_idx),
        };
        if tx.try_send(req).is_err() {
            tracing::debug!(
                peer_idx,
                "riak replica: peer channel full or closed; dropped"
            );
        }
    }
}

impl PeerOutbound for PeerChannelOutbound {
    fn dispatch(&self, peer_idx: u32, op: PeerOp) -> dynomite::embed::hooks::BoxFuture<'_, ()> {
        // Deliver synchronously (a bounded `try_send`) and return a
        // ready future so the request handler never awaits a peer.
        self.deliver(peer_idx, &op);
        Box::pin(async {})
    }
}

/// Build the [`RoutingHooks`] for a `data_store: dyniak` pool.
///
/// Constructs a [`RingView`] with one [`RingPoint`] per
/// `(peer, token)` in the pool (skipping the local peer -- a write
/// always lands locally, so the local node is never a forwarding
/// target), a Riak-defaults [`dyniak::BucketPropsRegistry`], and a
/// [`BucketRouter`] over the pool's [`HashType`]. The router's
/// replica plan then drives a fan-out through a
/// [`PeerChannelOutbound`] over `peer_txs`.
///
/// Ring tokens are the engine's 32-bit continuum: each
/// [`dynomite::hashkit::DynToken`] is projected to `u64` via
/// `get_int()`, matching the reaper's `ring_token_to_dyntoken`
/// convention so routing and reaping agree on partition bounds.
#[must_use]
pub fn build_routing_hooks(
    pool: &Arc<dynomite::cluster::ServerPool>,
    hash: dynomite::conf::HashType,
    peer_txs: &[(u32, String, mpsc::Sender<OutboundRequest>)],
) -> RoutingHooks {
    let mut points: Vec<RingPoint> = Vec::new();
    let mut local_actor = dyniak::datatypes::ActorId::new("local", "local");
    for peer in pool.peers().read().iter() {
        let idx = peer.idx();
        let dc = peer.dc();
        let rack = peer.rack();
        if peer.is_local() {
            // This node's CRDT actor identity: (datacenter, unique peer
            // name). Attributing each node's CRDT contribution to a
            // distinct actor is what lets concurrent increments on
            // partitioned replicas sum on merge instead of overwrite.
            local_actor = dyniak::datatypes::ActorId::new(dc.to_string(), peer.endpoint().pname());
        }
        for token in peer.tokens() {
            points.push(RingPoint::new(u64::from(token.get_int()), idx, dc, rack));
        }
    }
    let ring = Arc::new(RingView::new(points));
    let registry = Arc::new(dyniak::BucketPropsRegistry::new_riak_defaults());
    let router = Arc::new(BucketRouter::new(
        registry,
        ring,
        dynomite::cluster::map_hash(hash),
    ));
    let outbound = Arc::new(PeerChannelOutbound::new(peer_txs));
    RoutingHooks {
        router,
        outbound,
        local_actor,
    }
}

async fn bind(field: &'static str, addr: &str) -> Result<(SocketAddr, TcpListener), RiakWireError> {
    let parsed = match addr.parse::<SocketAddr>() {
        Ok(a) => a,
        Err(e) => {
            return Err(RiakWireError::BadAddr {
                field,
                value: addr.to_string(),
                reason: e.to_string(),
            });
        }
    };
    let listener = TcpListener::bind(parsed).await?;
    let local = listener.local_addr()?;
    Ok((local, listener))
}

async fn wait_flag(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dynomite::embed::MemoryDatastore;

    #[tokio::test]
    async fn build_handles_returns_none_when_unset() {
        let cfg = ConfRiak::default();
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let h = build_handles(&cfg, ds).await.unwrap();
        assert!(h.is_none(), "empty config should not bind anything");
    }

    #[tokio::test]
    async fn build_handles_binds_pbc_only() {
        let cfg = ConfRiak {
            pbc_listen: Some("127.0.0.1:0".into()),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let h = build_handles(&cfg, ds).await.unwrap().unwrap();
        assert!(h.pbc_addr.is_some());
        assert!(h.http_addr.is_none());
        assert!(h.aae.is_none());
        assert!(h.tls.is_none(), "plaintext default keeps TLS unset");
    }

    #[tokio::test]
    async fn build_handles_loads_tls_when_cert_pair_set() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
        let cfg = ConfRiak {
            pbc_listen: Some("127.0.0.1:0".into()),
            tls_cert: Some(cert_path),
            tls_key: Some(key_path),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let h = build_handles(&cfg, ds).await.unwrap().unwrap();
        assert!(h.tls.is_some(), "loader must produce a TlsAcceptor");
    }

    #[tokio::test]
    async fn build_handles_rejects_tls_cert_without_key() {
        let cfg = ConfRiak {
            pbc_listen: Some("127.0.0.1:0".into()),
            tls_cert: Some(std::path::PathBuf::from("/no/such/cert")),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let Err(err) = build_handles(&cfg, ds).await else {
            panic!("expected mismatched cert/key to fail");
        };
        assert!(matches!(
            err,
            RiakWireError::BadAddr {
                field: "tls_key",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn build_handles_rejects_bad_addr() {
        let cfg = ConfRiak {
            pbc_listen: Some("not-an-addr".into()),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let err = build_handles(&cfg, ds).await;
        assert!(matches!(err, Err(RiakWireError::BadAddr { .. })));
    }

    #[tokio::test]
    async fn aae_enabled_materialises_config() {
        let cfg = ConfRiak {
            pbc_listen: Some("127.0.0.1:0".into()),
            aae_enabled: Some(true),
            aae_segment_interval_seconds: Some(5),
            aae_full_sweep_interval_seconds: Some(60),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let h = build_handles(&cfg, ds).await.unwrap().unwrap();
        let aae = h.aae.expect("aae present");
        assert!(aae.enabled);
        assert_eq!(aae.segment_interval_seconds, 5);
        assert_eq!(aae.full_sweep_interval_seconds, 60);
    }

    #[tokio::test]
    async fn repair_sink_unknown_peer_is_rejected() {
        let sink = PeerChannelRepairSink::new(Vec::new());
        let task = RepairTask {
            peer_idx: 9,
            bucket: b"b".to_vec(),
            key: b"k".to_vec(),
            vclock: b"vc".to_vec(),
            direction: dyniak::aae::repair::RepairDirection::PushToRemote,
        };
        let res = sink.submit(task);
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn repair_sink_routes_to_known_peer() {
        let (tx, mut rx) = mpsc::channel::<OutboundRequest>(8);
        let sink = PeerChannelRepairSink::new(vec![(3, "peer-3".into(), tx)]);
        sink.submit(RepairTask {
            peer_idx: 3,
            bucket: b"users".to_vec(),
            key: b"alice".to_vec(),
            vclock: b"vc1".to_vec(),
            direction: dyniak::aae::repair::RepairDirection::PushToRemote,
        })
        .unwrap();
        let req = rx.recv().await.expect("one outbound");
        assert!(req.bytes.starts_with(b"AAE:users/alice:"));
        assert_eq!(req.target_peer_idx, Some(3));
    }

    /// When the binary is built without the `quic` feature, a
    /// configured `quic_listen` address must be rejected at
    /// build time with [`RiakWireError::QuicUnsupported`] rather
    /// than silently ignored.
    #[cfg(not(feature = "quic"))]
    #[tokio::test]
    async fn build_handles_rejects_quic_without_feature() {
        let cfg = ConfRiak {
            quic_listen: Some("127.0.0.1:0".into()),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let Err(err) = build_handles(&cfg, ds).await else {
            panic!("quic_listen without the quic feature must fail");
        };
        assert!(matches!(err, RiakWireError::QuicUnsupported(_)));
    }

    /// With the `quic` feature on, a `quic_listen` address plus a
    /// matching `tls_cert` / `tls_key` pair binds a QUIC
    /// listener and reports its resolved address.
    #[cfg(feature = "quic")]
    #[tokio::test]
    async fn build_handles_binds_quic_with_tls_pair() {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
        let cfg = ConfRiak {
            quic_listen: Some("127.0.0.1:0".into()),
            tls_cert: Some(cert_path),
            tls_key: Some(key_path),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let h = build_handles(&cfg, ds).await.unwrap().unwrap();
        assert!(h.quic_addr.is_some(), "quic listener must bind");
        assert!(h.quic_listener.is_some());
    }

    /// With the `quic` feature on, a `quic_listen` address with
    /// no TLS material is rejected (QUIC mandates TLS).
    #[cfg(feature = "quic")]
    #[tokio::test]
    async fn build_handles_rejects_quic_without_tls() {
        let cfg = ConfRiak {
            quic_listen: Some("127.0.0.1:0".into()),
            ..ConfRiak::default()
        };
        let ds: Arc<dyn Datastore> = Arc::new(MemoryDatastore::new());
        let Err(err) = build_handles(&cfg, ds).await else {
            panic!("quic_listen without tls_cert/tls_key must fail");
        };
        assert!(matches!(
            err,
            RiakWireError::BadAddr {
                field: "quic_listen",
                ..
            }
        ));
    }
}
