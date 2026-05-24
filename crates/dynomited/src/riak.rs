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
//! * [`PeerChannelRepairSink`] -- a [`dyn_riak::aae::RepairSink`]
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

use dyn_riak::aae::config::{ConfAae, DEFAULT_FULL_SWEEP_SECONDS, DEFAULT_SEGMENT_SECONDS};
use dyn_riak::aae::repair::{RepairSink, RepairTask};
use dyn_riak::aae::scheduler::{Scheduler, SweepPlan, SystemClock};
use dyn_riak::{serve_http, serve_pbc};

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
    /// Datastore the listeners delegate to. Shared between the
    /// PBC and HTTP loops so request accounting accumulates in
    /// one place.
    pub datastore: Arc<dyn Datastore>,
    /// Materialised AAE configuration. `None` when the YAML did
    /// not enable AAE.
    pub aae: Option<ConfAae>,
}

/// Eagerly bind the Riak listeners described by `riak`.
///
/// Returns `Ok(None)` when neither `pbc_listen` nor `http_listen`
/// is set; otherwise returns a fully-populated [`RiakHandles`].
/// Bind failures surface here rather than from the run loop, so
/// `Server::build` can crash-on-construct in the same way the
/// existing `Proxy::bind` calls do.
///
/// # Errors
///
/// [`RiakWireError`] when an address fails to parse or a TCP
/// `bind()` returns an I/O error.
pub async fn build_handles(
    riak: &ConfRiak,
    datastore: Arc<dyn Datastore>,
) -> Result<Option<RiakHandles>, RiakWireError> {
    if riak.pbc_listen.is_none() && riak.http_listen.is_none() {
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
        datastore,
        aae,
    }))
}

/// Spawn the Riak listener tasks.
///
/// Consumes the bound listeners and returns join handles whose
/// lifetimes the caller (the run loop) is responsible for.
/// `cancel_rx` resolves when graceful shutdown is requested;
/// both listener loops exit shortly after.
///
/// # Returns
///
/// A tuple of `(pbc_handle, http_handle)`; either may be `None`
/// when the corresponding address was not configured.
pub fn spawn_listeners(
    handles: &mut RiakHandles,
    cancel_rx: &watch::Receiver<bool>,
) -> (Option<JoinHandle<()>>, Option<JoinHandle<()>>) {
    let pbc = handles.pbc_listener.take().map(|listener| {
        let ds = Arc::clone(&handles.datastore);
        let mut cancel = cancel_rx.clone();
        tokio::spawn(async move {
            tokio::select! {
                res = serve_pbc(listener, ds) => {
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
        tokio::spawn(async move {
            tokio::select! {
                res = serve_http(listener, ds) => {
                    if let Err(e) = res {
                        tracing::warn!(error = %e, "riak http gateway exited with error");
                    }
                }
                () = wait_flag(&mut cancel) => {}
            }
        })
    });
    (pbc, http)
}

/// Spawn the Riak active anti-entropy scheduler task.
///
/// The scheduler ticks at the configured `segment_interval`
/// cadence; each tick is logged at `debug` level. Real divergence
/// detection is wired by future slices when the per-peer tree
/// exchange protocol lands; until then the task exists so the
/// configured cadence is observable and the [`PeerChannelRepairSink`]
/// is referenced (so the dispatcher's per-peer outbound channels
/// remain the canonical repair sink wiring).
///
/// `peer_txs` is the same `(peer_idx, pname, sender)` triple
/// the gossip task and the hint drainer use; the AAE task
/// holds a reference so the next slice can route real
/// `RepairTask`s without re-plumbing the supervisor.
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
/// use dyn_riak::aae::repair::{RepairDirection, RepairSink, RepairTask};
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
            direction: dyn_riak::aae::repair::RepairDirection::PushToRemote,
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
            direction: dyn_riak::aae::repair::RepairDirection::PushToRemote,
        })
        .unwrap();
        let req = rx.recv().await.expect("one outbound");
        assert!(req.bytes.starts_with(b"AAE:users/alice:"));
        assert_eq!(req.target_peer_idx, Some(3));
    }
}
