//! Cluster-aware [`Dispatcher`].
//!
//! Routes parsed [`Msg`]s based on the configured consistency level
//! and the [`crate::cluster::pool::ServerPool`] topology:
//!
//! * `DC_ONE` reads pick the rack-local replica via the snitch.
//! * `DC_ONE` writes fan out to every replica in the local DC.
//! * `DC_QUORUM` / `DC_SAFE_QUORUM` reads fan out to every replica
//!   in the local DC.
//! * `DC_EACH_SAFE_QUORUM` writes fan out per-DC, walking the
//!   per-DC racks via the preselected rack from
//!   [`crate::cluster::pool::ServerPool::preselect_remote_racks`].
//!
//! The actual outbound delivery happens through the per-peer
//! [`crate::net::ConnPool`]s; this module produces a
//! [`DispatchPlan`] (the list of replica peers a request must be
//! routed to) and exposes the planning logic so it can be tested
//! independently of the runtime fan-out.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::dispatch::{ClusterDispatcher, DispatchPlan};
//! use dynomite::cluster::pool::{PoolConfig, ServerPool};
//! use dynomite::cluster::peer::{Peer, PeerEndpoint};
//! use dynomite::hashkit::DynToken;
//! use dynomite::msg::{Msg, MsgType};
//! use std::sync::Arc;
//!
//! let cfg = PoolConfig {
//!     dc: "d".into(), rack: "r".into(),
//!     ..PoolConfig::default()
//! };
//! let local = Peer::new(
//!     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
//!     vec![DynToken::from_u32(0)], true, true, false,
//! );
//! let pool = Arc::new(ServerPool::new(cfg, vec![local]));
//! let disp = ClusterDispatcher::new(pool);
//! let req = Msg::new(1, MsgType::ReqRedisGet, true);
//! let plan = disp.plan(&req, b"foo");
//! assert!(matches!(plan, DispatchPlan::LocalDatastore));
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;

use crate::cluster::pool::ServerPool;
use crate::cluster::snitch::{rack_distance, RackDistance};
use crate::cluster::vnode;
use crate::conf::HashType as ConfHashType;
use crate::hashkit::{self, HashType};
use crate::io::mbuf::MbufPool;
use crate::msg::{ConsistencyLevel, Msg, MsgRouting, MsgType};
use crate::net::dispatcher::{DispatchOutcome, Dispatcher, OutboundEnvelope, ServerSink};
use crate::net::server::OutboundRequest;
use crate::proto::redis::repair::make::{find_most_updated, repair_request, TsReply};
use crate::proto::redis::repair::request::{cmd_info, Script, ScriptRequest};

/// Process-global Prometheus-friendly counter of
/// shadow-distribution disagreements. The dispatcher bumps this
/// counter every time the configured `distribution` and the
/// configured `distribution_shadow` choose different peers for
/// the same key. Exposed for both the stats endpoint and the
/// integration test that exercises shadow mode.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::dispatch::distribution_shadow_disagreement_total;
/// let _seen = distribution_shadow_disagreement_total();
/// ```
#[must_use]
pub fn distribution_shadow_disagreement_total() -> u64 {
    SHADOW_DISAGREEMENTS.load(Ordering::Relaxed)
}

/// Reset the shadow-disagreement counter. Used by integration
/// tests that need a clean baseline; never called from
/// production code.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::dispatch::reset_distribution_shadow_disagreement_total;
/// reset_distribution_shadow_disagreement_total();
/// ```
pub fn reset_distribution_shadow_disagreement_total() {
    SHADOW_DISAGREEMENTS.store(0, Ordering::Relaxed);
}

static SHADOW_DISAGREEMENTS: AtomicU64 = AtomicU64::new(0);

fn bump_shadow_disagreement() {
    SHADOW_DISAGREEMENTS.fetch_add(1, Ordering::Relaxed);
}

/// Build the `dispatch.plan` info span and enter it. Returns the
/// originating client request span (captured before the plan
/// span was entered) plus the entered plan-span guard. Factored
/// out so [`ClusterDispatcher::dispatch`] stays inside the
/// project's per-function line budget.
fn enter_plan_span(
    req_id: u64,
    plan: &DispatchPlan,
) -> (tracing::Span, tracing::span::EnteredSpan) {
    let req_span = tracing::Span::current();
    let kind: &'static str = match plan {
        DispatchPlan::Drop => "drop",
        DispatchPlan::NoTargets => "no_targets",
        DispatchPlan::LocalDatastore => "local_datastore",
        DispatchPlan::Replicas { .. } => "replicas",
    };
    let targets = match plan {
        DispatchPlan::Replicas { targets, .. } => targets.len(),
        _ => 0,
    };
    let span = tracing::info_span!("dispatch.plan", req_id, plan = kind, targets,).entered();
    (req_span, span)
}

/// Map a configuration-layer [`ConfHashType`] to the runtime
/// [`HashType`] used by the ring hash. The two enums are distinct
/// (one is parsed from YAML, the other drives
/// [`crate::hashkit::hash64`]); this is the single conversion seam
/// so the dispatcher, the reaper, and the dyniak replica router all
/// agree on which hash a pool uses.
#[must_use]
pub fn map_hash(h: ConfHashType) -> HashType {
    match h {
        ConfHashType::OneAtATime => HashType::OneAtATime,
        ConfHashType::Md5 => HashType::Md5,
        ConfHashType::Crc16 => HashType::Crc16,
        ConfHashType::Crc32 => HashType::Crc32,
        ConfHashType::Crc32a => HashType::Crc32a,
        ConfHashType::Fnv1_64 => HashType::Fnv1_64,
        ConfHashType::Fnv1a64 => HashType::Fnv1a_64,
        ConfHashType::Fnv1_32 => HashType::Fnv1_32,
        ConfHashType::Fnv1a32 => HashType::Fnv1a_32,
        ConfHashType::Hsieh => HashType::Hsieh,
        ConfHashType::Murmur => HashType::Murmur,
        ConfHashType::Jenkins => HashType::Jenkins,
        ConfHashType::Murmur3 => HashType::Murmur3,
        ConfHashType::Murmur3X64_64 => HashType::Murmur3X64_64,
    }
}

/// One replica target produced by [`ClusterDispatcher::plan`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaTarget {
    /// Index of the target peer in the pool's peer array.
    pub peer_idx: u32,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// True when the target is the local node.
    pub is_local: bool,
    /// True when this target stands in for a primary owner that is
    /// known down (Riak's sloppy quorum / hinted-handoff fallback).
    /// The topology dispatch path has no notion of a preference-list
    /// fallback -- every entry it produces is a primary owner, so this
    /// is always `false` here. The Riak walk-N-successors planner
    /// ([`dyniak`](https://docs.rs/dyniak)'s `replication::plan_successors`)
    /// is the only producer that can set this to `true`.
    pub is_fallback: bool,
}

/// Dispatch plan produced by the cluster dispatcher.
///
/// `LocalDatastore` is the early-return branch the reference
/// engine takes when the routing tag is `ROUTING_LOCAL_NODE_ONLY`
/// (or when the request is destined for the local node and the
/// topology has only one peer); the per-connection driver then
/// hands the request off to its server-side connection pool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatchPlan {
    /// Hand the request straight to the local datastore.
    LocalDatastore,
    /// Forward to one or more peer replicas. The carried
    /// consistency level is the one the planner resolved for
    /// this request (after applying any bucket-type override),
    /// so the dispatcher's reply coalescer does not have to
    /// re-resolve it.
    Replicas {
        /// Replica peers the request must be routed to.
        targets: Vec<ReplicaTarget>,
        /// Resolved consistency level.
        consistency: ConsistencyLevel,
    },
    /// Reply with an error: the cluster has no quorum-eligible
    /// targets.
    NoTargets,
    /// Drop the request (`QUIT`-style swallow).
    Drop,
}

/// Cluster-aware dispatcher.
#[derive(Clone)]
pub struct ClusterDispatcher {
    pool: Arc<ServerPool>,
    /// Outbound channel feeding the local datastore driver. When
    /// `None`, `LocalDatastore` plans short-circuit to `Pending`
    /// without forwarding (used by tests that do not need a real
    /// backend). When set, requests for the local node are
    /// encoded onto the wire and shipped to the [`crate::net::ServerConn`]
    /// task that drives the redis / memcache backend.
    backend: Option<LocalBackend>,
    /// Per-peer outbound channel for cross-DC fan-out. Keyed by
    /// `Peer::idx`. When a `DispatchPlan::Replicas` plan names a
    /// non-local peer, the dispatcher forwards via the matching
    /// channel to a `DnodeServerConn` task. Peers without a
    /// wired channel are skipped (`Pending`); when no replica
    /// is reachable for the consistency level the dispatcher
    /// falls back to a `DynomiteNoQuorumAchieved` error response.
    peer_backends: std::collections::HashMap<u32, mpsc::Sender<OutboundRequest>>,
    /// Mbuf pool used to render synthetic error payloads.
    /// `MbufPool` already wraps an `Arc`, so cloning the
    /// dispatcher (and the pool with it) shares the same free
    /// list across every cluster handle.
    mbuf_pool: MbufPool,
    /// Optional node-local hint store. When set AND the pool's
    /// `enable_hinted_handoff` flag is true, write requests
    /// targeted at peers in [`crate::cluster::peer::PeerState::Down`]
    /// (or at peers whose outbound channel is closed / full) are
    /// recorded as hints and counted toward the consistency
    /// threshold, instead of being silently skipped.
    hint_store: Option<Arc<crate::cluster::hints::HintStore>>,
    /// Optional failure-cause metrics handle. When wired, every
    /// error-producing branch in the dispatcher increments the
    /// matching counter via the [`crate::stats::FailureMetrics`]
    /// accumulator. When `None`, the dispatcher's behaviour is
    /// unchanged.
    failure_metrics: Option<Arc<crate::stats::FailureMetrics>>,
    /// Optional command-dispatch extension. When set, the
    /// dispatcher offers FT.* / HSET requests to the extension
    /// before the routing planner runs; the extension may
    /// short-circuit with a synthesised reply
    /// ([`crate::net::DispatchOutcome::Inline`]), reject the
    /// request with a structured error
    /// ([`crate::net::DispatchOutcome::Error`]), or fall
    /// through to the standard storage path. When `None`, the
    /// dispatcher's behaviour is unchanged. See
    /// [`crate::embed::CommandExtension`] for the trait shape.
    command_extension: Option<Arc<dyn crate::embed::CommandExtension>>,
    /// Optional in-process local datastore hook. When set, a
    /// [`DispatchPlan::LocalDatastore`] request is handed to this
    /// [`crate::embed::Datastore`] (the parsed request `Msg`,
    /// asynchronously) instead of being relayed over the
    /// [`Self::backend`] byte channel. The embedded server wires
    /// this so a connection accepted on its `listen:` socket is
    /// served through the same `Datastore` hook that
    /// `ServerHandle::inject_request` uses. When `None`, the
    /// backend byte channel is used (the standalone proxy path).
    local_datastore: Option<Arc<dyn crate::embed::hooks::Datastore>>,
}

/// What the node's own state allows for a request bound for its local
/// datastore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalGate {
    /// Send it, and everything held ahead of it, now.
    Send,
    /// Send it only when nothing is held ahead of it; otherwise hold it
    /// behind the earlier requests.
    SendIfIdle,
    /// Accept it but hold it unsent until a request arrives under
    /// [`DynState::Resuming`](crate::cluster::pool::DynState::Resuming).
    Hold,
    /// Refuse it with `DynomiteInvalidState`.
    Refuse,
}

/// The local-datastore gate for a request in node state `state`: NORMAL
/// sends when nothing is held ahead; STANDBY refuses everything;
/// WRITES_ONLY refuses reads and holds writes; RESUMING refuses reads and
/// sends writes together with whatever was held; any other state
/// (JOINING, INIT, ...) holds the request.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::dispatch::{local_gate, LocalGate};
/// use dynomite::cluster::pool::DynState;
/// assert_eq!(local_gate(DynState::Normal, true), LocalGate::SendIfIdle);
/// assert_eq!(local_gate(DynState::Standby, false), LocalGate::Refuse);
/// assert_eq!(local_gate(DynState::WritesOnly, true), LocalGate::Refuse);
/// assert_eq!(local_gate(DynState::WritesOnly, false), LocalGate::Hold);
/// assert_eq!(local_gate(DynState::Resuming, false), LocalGate::Send);
/// assert_eq!(local_gate(DynState::Joining, true), LocalGate::Hold);
/// ```
#[must_use]
pub fn local_gate(state: crate::cluster::pool::DynState, is_read: bool) -> LocalGate {
    use crate::cluster::pool::DynState;
    match state {
        DynState::Normal => LocalGate::SendIfIdle,
        DynState::Standby => LocalGate::Refuse,
        DynState::WritesOnly | DynState::Resuming if is_read => LocalGate::Refuse,
        DynState::Resuming => LocalGate::Send,
        _ => LocalGate::Hold,
    }
}

/// Why [`LocalBackend::send`] did not hand a request on.
#[derive(Debug)]
enum LocalSendError {
    /// The node's state refuses it ([`LocalGate::Refuse`]).
    Refused,
    /// The backend channel is full or closed.
    Channel(tokio::sync::mpsc::error::TrySendError<OutboundRequest>),
}

/// The local datastore's request channel behind the node-state gate.
/// Requests the gate holds wait here, oldest first, until a request
/// arrives under RESUMING; every clone shares the queue, so it is per
/// node.
///
/// A held request keeps its deadline: when the oldest one passes, every
/// held request is answered `Storage: Datastore refused connection`, as
/// the datastore connection they are queued on times out and closes.
#[derive(Clone)]
struct LocalBackend {
    tx: mpsc::Sender<OutboundRequest>,
    held: Arc<parking_lot::Mutex<std::collections::VecDeque<OutboundRequest>>>,
    /// Wakes the expiry task when a request is held.
    wake: Arc<tokio::sync::Notify>,
}

impl LocalBackend {
    fn new(tx: mpsc::Sender<OutboundRequest>, data_store: crate::conf::DataStore) -> Self {
        let this = Self {
            tx,
            held: Arc::default(),
            wake: Arc::default(),
        };
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(expire_held(
                Arc::downgrade(&this.held),
                this.wake.clone(),
                data_store,
            ));
        }
        this
    }

    /// Hand `env` on as `pool`'s state allows ([`local_gate`]). A send
    /// under RESUMING first flushes the held requests in order, as far as
    /// the channel has room; whatever does not fit stays held, ahead of
    /// `env`.
    fn send(
        &self,
        pool: &ServerPool,
        env: OutboundRequest,
        is_read: bool,
    ) -> Result<(), LocalSendError> {
        use tokio::sync::mpsc::error::TrySendError;
        let mut held = self.held.lock();
        match local_gate(pool.local_state(), is_read) {
            LocalGate::Refuse => Err(LocalSendError::Refused),
            LocalGate::SendIfIdle if held.is_empty() => {
                self.tx.try_send(env).map_err(LocalSendError::Channel)
            }
            LocalGate::Hold | LocalGate::SendIfIdle => {
                held.push_back(env);
                self.wake.notify_one();
                Ok(())
            }
            LocalGate::Send => {
                while let Some(h) = held.pop_front() {
                    if let Err(TrySendError::Full(h) | TrySendError::Closed(h)) =
                        self.tx.try_send(h)
                    {
                        held.push_front(h);
                        held.push_back(env);
                        return Ok(());
                    }
                }
                self.tx.try_send(env).map_err(LocalSendError::Channel)
            }
        }
    }
}

/// Answer every held request with the storage error once the earliest
/// held deadline passes. Ends when the dispatcher (the queue's owner) is
/// gone.
async fn expire_held(
    held: std::sync::Weak<parking_lot::Mutex<std::collections::VecDeque<OutboundRequest>>>,
    wake: Arc<tokio::sync::Notify>,
    data_store: crate::conf::DataStore,
) {
    loop {
        // Register for a wake-up before reading the queue so a request
        // held in between is not missed.
        let notified = wake.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let next = {
            let Some(q) = held.upgrade() else { return };
            let next = q.lock().iter().filter_map(|r| r.deadline).min();
            next
        };
        tokio::select! {
            () = crate::net::server::request_timer(next) => {}
            () = &mut notified => continue,
        }
        let expired: Vec<OutboundRequest> = {
            let Some(q) = held.upgrade() else { return };
            let mut q = q.lock();
            // The earliest deadline passed: the queue's connection closes
            // and takes every request on it.
            if !q
                .iter()
                .filter_map(|r| r.deadline)
                .any(|d| d <= tokio::time::Instant::now())
            {
                continue;
            }
            q.drain(..).collect()
        };
        let pool = MbufPool::default();
        for req in expired {
            let anchor = Msg::new(req.req_id, MsgType::Unknown, true);
            let rsp = crate::msg::response::make_error(
                &anchor,
                error_rsp_type(data_store),
                0,
                crate::msg::DynErrorCode::StorageConnectionRefuse,
                &pool,
            );
            let _ = req
                .responder
                .send(OutboundEnvelope {
                    req_id: req.req_id,
                    rsp,
                    span: req.span,
                    source_peer_idx: req.target_peer_idx,
                })
                .await;
        }
    }
}

impl std::fmt::Debug for ClusterDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `Datastore` is not `Debug` (it is a public embedding
        // trait and must stay object-safe without a Debug bound),
        // so the local-datastore hook is reported as a presence
        // flag rather than its contents.
        f.debug_struct("ClusterDispatcher")
            .field("backend", &self.backend.is_some())
            .field("peer_backends", &self.peer_backends.len())
            .field("hint_store", &self.hint_store.is_some())
            .field("failure_metrics", &self.failure_metrics.is_some())
            .field("command_extension", &self.command_extension)
            .field("local_datastore", &self.local_datastore.is_some())
            .finish_non_exhaustive()
    }
}

impl ClusterDispatcher {
    /// Wrap a [`ServerPool`] in a dispatcher.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dynomite::cluster::dispatch::ClusterDispatcher;
    /// # use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::hashkit::DynToken;
    /// # use std::sync::Arc;
    /// # let cfg = PoolConfig::default();
    /// # let local = Peer::new(
    /// #    0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    /// #    vec![DynToken::from_u32(0)], true, true, false,
    /// # );
    /// let pool = Arc::new(ServerPool::new(cfg, vec![local]));
    /// let disp = ClusterDispatcher::new(pool);
    /// let _ = disp.pool();
    /// ```
    #[must_use]
    pub fn new(pool: Arc<ServerPool>) -> Self {
        Self {
            pool,
            backend: None,
            peer_backends: std::collections::HashMap::new(),
            mbuf_pool: MbufPool::default(),
            hint_store: None,
            failure_metrics: None,
            command_extension: None,
            local_datastore: None,
        }
    }

    /// Override the dispatcher's mbuf pool. Useful when the
    /// embedding wants every synthetic error payload to come from
    /// the same recycled buffers as the rest of the engine.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dynomite::cluster::dispatch::ClusterDispatcher;
    /// # use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::hashkit::DynToken;
    /// # use dynomite::io::mbuf::MbufPool;
    /// # use std::sync::Arc;
    /// # let cfg = PoolConfig::default();
    /// # let local = Peer::new(
    /// #    0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    /// #    vec![DynToken::from_u32(0)], true, true, false,
    /// # );
    /// let pool = Arc::new(ServerPool::new(cfg, vec![local]));
    /// let _disp = ClusterDispatcher::new(pool).with_mbuf_pool(MbufPool::default());
    /// ```
    #[must_use]
    pub fn with_mbuf_pool(mut self, pool: MbufPool) -> Self {
        self.mbuf_pool = pool;
        self
    }

    /// Borrow the dispatcher's mbuf pool. Exposed so embedders
    /// can reuse the same pool when building synthetic responses
    /// outside the dispatcher's own code paths.
    #[must_use]
    pub fn mbuf_pool(&self) -> &MbufPool {
        &self.mbuf_pool
    }

    /// Attach a backend request channel. Calls to [`Self::dispatch`]
    /// that produce a [`DispatchPlan::LocalDatastore`] plan will
    /// forward the request bytes onto this channel for the local
    /// datastore driver to write to the backend.
    ///
    /// The channel sender must be the request side of a
    /// [`crate::net::ServerConn`] task; multiple senders cloned from
    /// the same channel are fine.
    #[must_use]
    pub fn with_backend(mut self, backend: mpsc::Sender<OutboundRequest>) -> Self {
        self.backend = Some(LocalBackend::new(backend, self.pool.config().data_store));
        self
    }

    /// Attach an in-process local datastore hook. A
    /// [`DispatchPlan::LocalDatastore`] request is then handed to
    /// the supplied [`crate::embed::hooks::Datastore`] (the parsed
    /// request `Msg`) and its reply is delivered on the
    /// connection's responder, instead of being relayed over the
    /// [`Self::with_backend`] byte channel. The embedded server
    /// wires this so a client accepted on its `listen:` socket is
    /// served through the same `Datastore` hook that
    /// `ServerHandle::inject_request` uses. Takes precedence over
    /// the backend channel for local requests.
    #[must_use]
    pub fn with_local_datastore(
        mut self,
        datastore: Arc<dyn crate::embed::hooks::Datastore>,
    ) -> Self {
        self.local_datastore = Some(datastore);
        self
    }

    /// Attach an outbound channel for a single peer (by
    /// `Peer::idx`). The supplied sender feeds a
    /// [`crate::net::DnodeServerConn`] task that writes
    /// dnode-framed requests to the peer's `dyn_listen` and
    /// routes the response back through the per-request
    /// responder channel.
    ///
    /// Wiring is additive: call this once per non-local peer.
    /// Calling it again with the same `peer_idx` replaces the
    /// previous sender (used by reconnect supervisors that
    /// rebuild channels on restart).
    #[must_use]
    pub fn with_peer_backend(
        mut self,
        peer_idx: u32,
        sender: mpsc::Sender<OutboundRequest>,
    ) -> Self {
        self.peer_backends.insert(peer_idx, sender);
        self
    }

    /// Whether a backend channel is wired.
    #[must_use]
    pub fn has_backend(&self) -> bool {
        self.backend.is_some()
    }

    /// Number of peer-backend channels wired.
    #[must_use]
    pub fn peer_backend_count(&self) -> usize {
        self.peer_backends.len()
    }

    /// Borrow the underlying pool.
    #[must_use]
    pub fn pool(&self) -> &Arc<ServerPool> {
        &self.pool
    }

    /// Attach a [`crate::cluster::hints::HintStore`].
    ///
    /// When set AND the pool's `enable_hinted_handoff` flag is
    /// `true`, write requests targeted at peers in
    /// [`crate::cluster::peer::PeerState::Down`] (or at peers
    /// whose outbound channel is closed / full) are stored as
    /// hints and counted toward the consistency threshold. The
    /// background drainer task in `dynomited` is responsible
    /// for shipping the hints back to the peer once it returns
    /// to [`crate::cluster::peer::PeerState::Normal`]. Without
    /// this builder call (or with `enable_hinted_handoff: false`)
    /// the dispatcher behaviour is unchanged: a Down or
    /// unreachable target is silently skipped.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use dynomite::cluster::dispatch::ClusterDispatcher;
    /// # use dynomite::cluster::hints::HintStore;
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::hashkit::DynToken;
    /// let cfg = PoolConfig { enable_hinted_handoff: true, ..PoolConfig::default() };
    /// let local = Peer::new(
    ///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    ///     vec![DynToken::from_u32(0)], true, true, false,
    /// );
    /// let pool = Arc::new(ServerPool::new(cfg, vec![local]));
    /// let store = Arc::new(HintStore::new(64 * 1024 * 1024));
    /// let _disp = ClusterDispatcher::new(pool).with_hint_store(store);
    /// ```
    #[must_use]
    pub fn with_hint_store(mut self, store: Arc<crate::cluster::hints::HintStore>) -> Self {
        self.hint_store = Some(store);
        self
    }

    /// Borrow the wired hint store, if any.
    #[must_use]
    pub fn hint_store(&self) -> Option<&Arc<crate::cluster::hints::HintStore>> {
        self.hint_store.as_ref()
    }

    /// Attach a [`crate::stats::FailureMetrics`] handle.
    ///
    /// When wired, each error-producing branch in the
    /// dispatcher (no-targets, peer-channel-full,
    /// peer-channel-closed, backend-channel-full,
    /// backend-channel-closed, response-timeout) increments
    /// the matching counter so an operator can pull the
    /// per-cause histogram off the `/stats` and `/metrics`
    /// endpoints. The default behaviour is unchanged when no
    /// metrics handle is supplied.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use dynomite::cluster::dispatch::ClusterDispatcher;
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::hashkit::DynToken;
    /// # use dynomite::stats::FailureMetrics;
    /// let cfg = PoolConfig::default();
    /// let local = Peer::new(
    ///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    ///     vec![DynToken::from_u32(0)], true, true, false,
    /// );
    /// let pool = Arc::new(ServerPool::new(cfg, vec![local]));
    /// let metrics = Arc::new(FailureMetrics::new());
    /// let _disp = ClusterDispatcher::new(pool).with_failure_metrics(metrics);
    /// ```
    #[must_use]
    pub fn with_failure_metrics(mut self, metrics: Arc<crate::stats::FailureMetrics>) -> Self {
        self.failure_metrics = Some(metrics);
        self
    }

    /// Borrow the wired failure-metrics handle, if any.
    #[must_use]
    pub fn failure_metrics(&self) -> Option<&Arc<crate::stats::FailureMetrics>> {
        self.failure_metrics.as_ref()
    }

    /// Attach a [`crate::embed::CommandExtension`].
    ///
    /// When wired, parsed FT.* requests and HSET requests are
    /// offered to the extension before the routing planner
    /// runs. The extension may produce a synthesised RESP
    /// reply (returned to the client as
    /// [`crate::net::DispatchOutcome::Inline`]), surface a
    /// structured `-ERR ...` reply
    /// ([`crate::net::DispatchOutcome::Error`]), or fall
    /// through. Without this builder call the dispatcher's
    /// behaviour is unchanged: FT.* keywords are forwarded to
    /// the local datastore (which typically rejects them with
    /// `-ERR unknown command`) and HSETs proceed unmodified.
    ///
    /// The standard RediSearch implementation lives in the
    /// `dynomite-search` crate; the trait itself is part of
    /// the engine so embedders can plug their own.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use dynomite::cluster::dispatch::ClusterDispatcher;
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::embed::{CommandExtension, HsetOutcome};
    /// # use dynomite::hashkit::DynToken;
    /// # use dynomite::msg::MsgType;
    /// #[derive(Debug)]
    /// struct NoOp;
    /// impl CommandExtension for NoOp {
    ///     fn handles_msg_type(&self, _: MsgType) -> bool { false }
    ///     fn try_dispatch(&self, _: &[&[u8]]) -> Option<Vec<u8>> { None }
    /// }
    /// let cfg = PoolConfig::default();
    /// let local = Peer::new(
    ///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    ///     vec![DynToken::from_u32(0)], true, true, false,
    /// );
    /// let pool = Arc::new(ServerPool::new(cfg, vec![local]));
    /// let _disp = ClusterDispatcher::new(pool).with_command_extension(Arc::new(NoOp));
    /// ```
    #[must_use]
    pub fn with_command_extension(mut self, ext: Arc<dyn crate::embed::CommandExtension>) -> Self {
        self.command_extension = Some(ext);
        self
    }

    /// Borrow the wired command extension, if any.
    #[must_use]
    pub fn command_extension(&self) -> Option<&Arc<dyn crate::embed::CommandExtension>> {
        self.command_extension.as_ref()
    }

    /// True when both the hint store is wired AND the pool has
    /// `enable_hinted_handoff: true`. Hot-path predicate.
    #[must_use]
    pub fn hinted_handoff_active(&self) -> bool {
        self.hint_store.is_some() && self.pool.config().enable_hinted_handoff
    }

    /// Compute the routing plan for `req` with the supplied key.
    ///
    /// `key` is the primary key of the request (the first key
    /// returned by [`Msg::keys`] for parsed redis / memcache
    /// commands, or an empty slice for argument-less commands).
    ///
    /// The function never panics; it consults the live peer table
    /// behind the pool's `RwLock` and returns
    /// [`DispatchPlan::NoTargets`] when the topology cannot
    /// satisfy the request.
    ///
    /// # Examples
    ///
    /// See the module-level example.
    #[must_use]
    pub fn plan(&self, req: &Msg, key: &[u8]) -> DispatchPlan {
        let cfg = self.pool.config();
        let peers = self.pool.peers().read();
        if peers.is_empty() {
            self.record_no_targets_metric(cfg, ConsistencyLevel::default());
            return DispatchPlan::NoTargets;
        }
        if matches!(req.routing(), MsgRouting::LocalNodeOnly) {
            return DispatchPlan::LocalDatastore;
        }
        if matches!(req.routing(), MsgRouting::AllNodesAllRacksAllDcs) {
            // SCRIPT LOAD / FLUSH / KILL / EXISTS: every node in every rack
            // and DC, regardless of key or token. Quorum makes no sense
            // here, so the level is DC_ONE: the first reply answers and
            // the rest are swallowed (C `req_forward` for
            // ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS).
            let dcs = self.pool.datacenters().read();
            let mut targets = Vec::with_capacity(peers.len());
            for p in peers.iter() {
                let Some(dc_idx) = dcs.iter().position(|d| d.name() == p.dc()) else {
                    continue;
                };
                let Some(rack_idx) = dcs[dc_idx]
                    .racks()
                    .iter()
                    .position(|r| r.name() == p.rack())
                else {
                    continue;
                };
                targets.push(build_target(&dcs, &peers, dc_idx, rack_idx, p.idx()));
            }
            if targets.is_empty() {
                return DispatchPlan::NoTargets;
            }
            return DispatchPlan::Replicas {
                targets,
                consistency: ConsistencyLevel::DcOne,
            };
        }
        if key.is_empty() {
            return DispatchPlan::LocalDatastore;
        }
        let token = hashkit::hash(map_hash(cfg.hash), key);
        let key_hash64 = hashkit::hash64(map_hash(cfg.hash), key);
        let bucket = crate::proto::redis::bucket_name(key);
        let bucket_type = cfg.resolve_bucket_type(bucket);
        let is_read = matches!(req.ty(), MsgType::Unknown) || req.flags().is_read;
        let consistency = match (bucket_type, is_read) {
            (Some(bt), true) => bt.read_consistency,
            (Some(bt), false) => bt.write_consistency,
            (None, true) => self.pool.read_consistency(),
            (None, false) => self.pool.write_consistency(),
        };
        let n_val_cap = bucket_type.map_or(0, |bt| bt.n_val);
        let dcs = self.pool.datacenters().read();
        // When hinted handoff is active and the request is a
        // write, peers in `Down` are kept in the routable set
        // so the dispatcher can hint them at fan-out time. The
        // hint counts toward the consistency threshold; the
        // background drainer ships the hint once the peer
        // returns to `Normal`. For reads (and for writes when
        // handoff is off), Down peers are filtered out as
        // before.
        let include_down = self.hinted_handoff_active() && !is_read;
        let routable = collect_routable(
            &dcs,
            &peers,
            &token,
            key_hash64,
            cfg.distribution,
            include_down,
        );
        if let Some(shadow) = cfg.distribution_shadow {
            if shadow != cfg.distribution {
                let shadow_routable =
                    collect_routable(&dcs, &peers, &token, key_hash64, shadow, include_down);
                if !plans_agree(&routable, &shadow_routable) {
                    bump_shadow_disagreement();
                    tracing::debug!(
                        target: "dynomite::dispatch::shadow",
                        live = cfg.distribution.as_str(),
                        shadow = shadow.as_str(),
                        "shadow distribution disagreed on key route"
                    );
                }
            }
        }
        if routable.is_empty() {
            self.record_no_targets_metric(cfg, consistency);
            return DispatchPlan::NoTargets;
        }
        let (local, remote): (Vec<_>, Vec<_>) = routable
            .into_iter()
            .partition(|(dc_idx, _, _)| dcs[*dc_idx].name() == cfg.dc);
        let plan = plan_with_consistency(
            cfg,
            &dcs,
            &peers,
            consistency,
            req.routing(),
            is_read,
            RoutablePartition { local, remote },
        );
        let plan = cap_replicas(plan, n_val_cap);
        if matches!(plan, DispatchPlan::NoTargets) {
            self.record_no_targets_metric(cfg, consistency);
        }
        plan
    }

    /// Record a `dispatch_no_targets_total` metric tick using
    /// the local-DC labels, when a metrics handle is wired.
    fn record_no_targets_metric(
        &self,
        cfg: &crate::cluster::pool::PoolConfig,
        consistency: ConsistencyLevel,
    ) {
        if let Some(m) = self.failure_metrics.as_ref() {
            m.record_no_targets(&cfg.dc, &cfg.rack, consistency);
        }
    }

    /// Resolve the destination DC of a peer (for per-peer
    /// failure metrics). Local peers and unknown indexes both
    /// fall back to the configured local DC.
    fn peer_dc_label(&self, peer_idx: u32) -> String {
        let peers = self.pool.peers().read();
        peers
            .get(peer_idx as usize)
            .map_or_else(|| self.pool.config().dc.clone(), |p| p.dc().to_string())
    }
}

/// Apply the bucket-type `n_val` fan-out cap to a freshly
/// computed plan. Only `DispatchPlan::Replicas` is affected; the
/// other variants pass through unchanged. `cap == 0` means "no
/// cap" and is the no-op used for keys without a matching bucket
/// type.
fn cap_replicas(plan: DispatchPlan, cap: u8) -> DispatchPlan {
    if cap == 0 {
        return plan;
    }
    let cap = cap as usize;
    match plan {
        DispatchPlan::Replicas {
            mut targets,
            consistency,
        } if targets.len() > cap => {
            targets.truncate(cap);
            DispatchPlan::Replicas {
                targets,
                consistency,
            }
        }
        other => other,
    }
}

fn plans_agree(a: &[(usize, usize, u32)], b: &[(usize, usize, u32)]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_idx: Vec<u32> = a.iter().map(|t| t.2).collect();
    let mut b_idx: Vec<u32> = b.iter().map(|t| t.2).collect();
    a_idx.sort_unstable();
    b_idx.sort_unstable();
    a_idx == b_idx
}

/// The peer that owns a key on one rack: the vnode continuum lookup, or
/// the random-slice claimant when that distribution is active.
fn rack_candidate(
    dc: &crate::cluster::Datacenter,
    rack: &crate::cluster::Rack,
    peers: &[crate::cluster::peer::Peer],
    token: &crate::hashkit::DynToken,
    hash64: u64,
    distribution: crate::conf::Distribution,
) -> Option<u32> {
    match (distribution, rack.random_slices()) {
        (crate::conf::Distribution::RandomSlicing, Some(slices)) => {
            // The slice table holds peer pnames (host:port); resolve the
            // claimant to a peer index within this rack.
            slices.claimant_for(hash64).and_then(|name| {
                peers.iter().find_map(|p| {
                    if p.dc() == dc.name()
                        && p.rack() == rack.name()
                        && p.endpoint().pname() == name
                    {
                        Some(p.idx())
                    } else {
                        None
                    }
                })
            })
        }
        _ => vnode::dispatch(rack.continuums(), token),
    }
}

fn collect_routable(
    dcs: &[crate::cluster::Datacenter],
    peers: &[crate::cluster::peer::Peer],
    token: &crate::hashkit::DynToken,
    hash64: u64,
    distribution: crate::conf::Distribution,
    include_down: bool,
) -> Vec<(usize, usize, u32)> {
    let mut routable: Vec<(usize, usize, u32)> = Vec::new();
    for (dc_idx, dc) in dcs.iter().enumerate() {
        for (rack_idx, rack) in dc.racks().iter().enumerate() {
            let candidate = rack_candidate(dc, rack, peers, token, hash64, distribution);
            if let Some(peer_idx) = candidate {
                if let Some(peer) = peers.get(peer_idx as usize) {
                    let state = peer.state();
                    // A local node in STANDBY still owns its keys: the
                    // state gates the local datastore send ([`local_gate`]),
                    // not routing.
                    let local_standby = peer.is_local()
                        && matches!(state, crate::cluster::peer::PeerState::Standby);
                    let accept = local_standby
                        || state.is_routable()
                        || (include_down && matches!(state, crate::cluster::peer::PeerState::Down));
                    if accept {
                        routable.push((dc_idx, rack_idx, peer_idx));
                    }
                }
            }
        }
    }
    routable
}

fn build_target(
    dcs: &[crate::cluster::Datacenter],
    peers: &[crate::cluster::peer::Peer],
    dc_idx: usize,
    rack_idx: usize,
    peer_idx: u32,
) -> ReplicaTarget {
    let dc_name = dcs[dc_idx].name().to_string();
    let rack_name = dcs[dc_idx].racks()[rack_idx].name().to_string();
    let is_local = peers
        .get(peer_idx as usize)
        .is_some_and(crate::cluster::peer::Peer::is_local);
    ReplicaTarget {
        peer_idx,
        dc: dc_name,
        rack: rack_name,
        is_local,
        // The topology dispatch preference list has no fallback
        // concept: every routable target it names is a primary owner.
        is_fallback: false,
    }
}

/// The routable replica set for a key, partitioned into local-DC and
/// remote-DC candidates by [`ClusterDispatcher::plan`].
struct RoutablePartition {
    local: Vec<(usize, usize, u32)>,
    remote: Vec<(usize, usize, u32)>,
}

fn plan_with_consistency(
    cfg: &crate::cluster::pool::PoolConfig,
    dcs: &[crate::cluster::Datacenter],
    peers: &[crate::cluster::peer::Peer],
    consistency: ConsistencyLevel,
    routing: MsgRouting,
    is_read: bool,
    partition: RoutablePartition,
) -> DispatchPlan {
    let RoutablePartition { local, remote } = partition;
    let want_per_dc_fanout = matches!(consistency, ConsistencyLevel::DcEachSafeQuorum)
        || matches!(routing, MsgRouting::AllNodesAllRacksAllDcs);
    let mut targets: Vec<ReplicaTarget> = Vec::new();
    match consistency {
        ConsistencyLevel::DcOne => {
            if local.is_empty() {
                return DispatchPlan::NoTargets;
            }
            if is_read {
                // DC_ONE READ: pick the single rack-closest local-DC
                // replica (lowest latency); the coalescer returns on
                // its first reply.
                let mut best: Option<(RackDistance, (usize, usize, u32))> = None;
                for (dc_idx, rack_idx, peer_idx) in local {
                    let rack_name = dcs[dc_idx].racks()[rack_idx].name();
                    let d = rack_distance(&cfg.dc, &cfg.rack, &cfg.dc, rack_name);
                    let take = match best {
                        None => true,
                        Some((bd, _)) => d.cost() < bd.cost(),
                    };
                    if take {
                        best = Some((d, (dc_idx, rack_idx, peer_idx)));
                    }
                }
                if let Some((_, (dc_idx, rack_idx, peer_idx))) = best {
                    let is_local_node = peers
                        .get(peer_idx as usize)
                        .is_some_and(crate::cluster::peer::Peer::is_local);
                    if is_local_node {
                        return DispatchPlan::LocalDatastore;
                    }
                    targets.push(build_target(dcs, peers, dc_idx, rack_idx, peer_idx));
                }
            } else {
                // DC_ONE WRITE: fan out to EVERY local-DC replica for
                // durability (each rack is a replica). The coalescer
                // acks on the first successful reply, but the write is
                // delivered to all replicas. This matches C Dynomite,
                // where a DC_ONE write replicates within the local DC.
                for (dc_idx, rack_idx, peer_idx) in local {
                    targets.push(build_target(dcs, peers, dc_idx, rack_idx, peer_idx));
                }
                // A lone local replica (single-node pool, or any key
                // whose only local-DC replica is this node) is served
                // through the local datastore directly. Fanning it out
                // as a one-element `Replicas` plan would require a wired
                // backend channel; the `LocalDatastore` path is the one
                // both the wire proxy and `inject_request` already
                // serve via the local-datastore hook. Multi-replica
                // fan-out (more than one local target) is unaffected.
                if targets.len() == 1 && targets[0].is_local {
                    return DispatchPlan::LocalDatastore;
                }
            }
        }
        ConsistencyLevel::DcQuorum | ConsistencyLevel::DcSafeQuorum => {
            if local.is_empty() {
                return DispatchPlan::NoTargets;
            }
            for (dc_idx, rack_idx, peer_idx) in local {
                targets.push(build_target(dcs, peers, dc_idx, rack_idx, peer_idx));
            }
        }
        ConsistencyLevel::DcEachSafeQuorum => {
            if local.is_empty() && remote.is_empty() {
                return DispatchPlan::NoTargets;
            }
            for (dc_idx, rack_idx, peer_idx) in local.iter().chain(remote.iter()) {
                targets.push(build_target(dcs, peers, *dc_idx, *rack_idx, *peer_idx));
            }
        }
    }
    if want_per_dc_fanout && !remote.is_empty() {
        for (dc_idx, rack_idx, peer_idx) in remote {
            if !targets.iter().any(|t| t.peer_idx == peer_idx) {
                targets.push(build_target(dcs, peers, dc_idx, rack_idx, peer_idx));
            }
        }
    }
    if targets.is_empty() {
        return DispatchPlan::LocalDatastore;
    }
    DispatchPlan::Replicas {
        targets,
        consistency,
    }
}

impl ClusterDispatcher {
    /// The peer that owns `key_tag` on the local node's rack, with the
    /// same placement [`ClusterDispatcher::plan`] uses. `None` when the
    /// local rack is not in the topology or has no continuum.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dynomite::cluster::dispatch::ClusterDispatcher;
    /// # use dynomite::cluster::pool::{PoolConfig, ServerPool};
    /// # use dynomite::cluster::peer::{Peer, PeerEndpoint};
    /// # use dynomite::hashkit::DynToken;
    /// # use std::sync::Arc;
    /// let cfg = PoolConfig { dc: "d".into(), rack: "r".into(), ..PoolConfig::default() };
    /// let local = Peer::new(
    ///     0, PeerEndpoint::tcp("h".into(), 1), "r".into(), "d".into(),
    ///     vec![DynToken::from_u32(0)], true, true, false,
    /// );
    /// let pool = Arc::new(ServerPool::new(cfg, vec![local]));
    /// pool.rebuild_ring();
    /// assert_eq!(ClusterDispatcher::new(pool).rack_owner(b"k"), Some(0));
    /// ```
    #[must_use]
    pub fn rack_owner(&self, key_tag: &[u8]) -> Option<u32> {
        let cfg = self.pool.config();
        let peers = self.pool.peers().read();
        let dcs = self.pool.datacenters().read();
        let dc = dcs.iter().find(|d| d.name() == cfg.dc)?;
        let rack = dc.racks().iter().find(|r| r.name() == cfg.rack)?;
        let token = hashkit::hash(map_hash(cfg.hash), key_tag);
        let hash64 = hashkit::hash64(map_hash(cfg.hash), key_tag);
        rack_candidate(dc, rack, &peers, &token, hash64, cfg.distribution)
    }
}

/// Multi-key requests split by the owning peer on the local rack.
impl crate::proto::redis::FragmentDispatcher for ClusterDispatcher {
    fn shard_for(&self, key_tag: &[u8]) -> u32 {
        self.rack_owner(key_tag).unwrap_or(0)
    }

    fn shard_count(&self) -> u32 {
        u32::try_from(self.pool.peers().read().len()).unwrap_or(u32::MAX)
    }
}

impl ClusterDispatcher {
    /// Split a client's multi-key `MGET` / `DEL` / `EXISTS` / `MSET` by
    /// the owning peer on the local rack, dispatch each fragment through
    /// the ordinary single-request path (so per-fragment replication and
    /// quorum are unchanged), and answer the client with the merged reply
    /// in its key order. `None` when the request is not one that splits.
    fn dispatch_fragmented(
        &self,
        req: &mut Msg,
        responder: &ServerSink,
    ) -> Option<DispatchOutcome> {
        let splits = matches!(
            req.ty(),
            MsgType::ReqRedisMget
                | MsgType::ReqRedisDel
                | MsgType::ReqRedisExists
                | MsgType::ReqRedisMset
        );
        // Only client requests split: a fragment (frag_id set) or a
        // request relayed by a peer (dnode header) is served as is.
        if !splits
            || req.keys().len() < 2
            || req.frag_id() != 0
            || req.dmsg().is_some()
            || !matches!(req.routing(), MsgRouting::Normal)
            || !matches!(
                self.pool.config().data_store,
                crate::conf::DataStore::Valkey
            )
        {
            return None;
        }
        let values: Vec<Vec<u8>> = if matches!(req.ty(), MsgType::ReqRedisMset) {
            let bytes: Vec<u8> = req
                .mbufs()
                .iter()
                .flat_map(|b| b.readable().to_vec())
                .collect();
            resp_array_tokens(&bytes)?
                .into_iter()
                .skip(2)
                .step_by(2)
                .collect()
        } else {
            Vec::new()
        };
        let outcome =
            crate::proto::redis::redis_fragment(req, self, &values, &self.mbuf_pool).ok()??;
        // Fragments are created in first-seen shard order.
        let mut shards: Vec<u32> = Vec::new();
        let frag_for_key: Vec<usize> = outcome
            .shard_for_key
            .iter()
            .map(|s| {
                shards.iter().position(|x| x == s).unwrap_or_else(|| {
                    shards.push(*s);
                    shards.len() - 1
                })
            })
            .collect();
        let mut replies: Vec<Option<Vec<u8>>> = vec![None; outcome.fragments.len()];
        let mut waiting: Vec<(usize, mpsc::Receiver<OutboundEnvelope>)> = Vec::new();
        for (f, frag) in outcome.fragments.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel::<OutboundEnvelope>(2);
            match self.dispatch(frag, tx) {
                DispatchOutcome::Pending => waiting.push((f, rx)),
                DispatchOutcome::Inline(rsp) | DispatchOutcome::Error(rsp) => {
                    replies[f] = Some(msg_bytes(&rsp));
                }
                DispatchOutcome::Drop => {}
            }
        }
        let req_id = req.id();
        let req_ty = req.ty();
        let pool = self.mbuf_pool.clone();
        let responder = responder.clone();
        tokio::spawn(async move {
            for (f, mut rx) in waiting {
                replies[f] = rx.recv().await.map(|env| msg_bytes(&env.rsp));
            }
            let merged =
                crate::proto::redis::redis_coalesce_fragments(req_ty, &frag_for_key, &replies);
            let mut rsp = Msg::new(req_id, MsgType::Unknown, false);
            rsp.set_parent_id(req_id);
            rsp.append_bytes(&pool, &merged);
            let _ = responder
                .send(OutboundEnvelope {
                    req_id,
                    rsp,
                    span: tracing::Span::none(),
                    source_peer_idx: None,
                })
                .await;
        });
        Some(DispatchOutcome::Pending)
    }
}

/// The wire bytes of a message's mbuf chain.
fn msg_bytes(m: &Msg) -> Vec<u8> {
    m.mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect()
}

/// The bulk tokens of a RESP array request (`*n` then `n` bulks).
fn resp_array_tokens(wire: &[u8]) -> Option<Vec<Vec<u8>>> {
    fn line(wire: &[u8], at: usize) -> Option<(&[u8], usize)> {
        let end = at + wire.get(at..)?.windows(2).position(|w| w == b"\r\n")?;
        Some((&wire[at..end], end + 2))
    }
    let (head, mut p) = line(wire, 0)?;
    let n: usize = std::str::from_utf8(head.strip_prefix(b"*")?)
        .ok()?
        .parse()
        .ok()?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let (len, body) = line(wire, p)?;
        let len: usize = std::str::from_utf8(len.strip_prefix(b"$")?)
            .ok()?
            .parse()
            .ok()?;
        out.push(wire.get(body..body + len)?.to_vec());
        p = body + len + 2;
    }
    Some(out)
}

impl Dispatcher for ClusterDispatcher {
    #[allow(
        clippy::too_many_lines,
        reason = "single dispatch fn must enumerate every plan; splitting hides the planner-to-effect mapping"
    )]
    fn dispatch(&self, mut req: Msg, responder: ServerSink) -> DispatchOutcome {
        if req.flags().quit {
            return DispatchOutcome::Drop;
        }
        if let Some(outcome) = self.dispatch_fragmented(&mut req, &responder) {
            return outcome;
        }
        // FT.* / HSET interception. Runs before the routing
        // planner so vector-index commands never visit the
        // backend, and HSETs against an indexed prefix get the
        // vector mirrored into the in-process registry before
        // the standard storage write fans out.
        if let Some(ext) = self.command_extension.as_ref() {
            if let Some(outcome) = self.intercept_command(ext.as_ref(), &req) {
                return outcome;
            }
        }
        // Inspect the request without consuming it: pull the routing
        // bytes from the first parsed key. `KeyPos::tag_bytes` returns
        // the hash-tag-aware sub-range when one was parsed and the full
        // key otherwise, which is the slice shape `plan` expects.
        // Requests with no parsed keys (e.g. PING, INFO) fall through
        // with an empty slice; `plan` handles that by routing to the
        // local datastore.
        let key: Vec<u8> = req
            .keys()
            .first()
            .map(|kp| kp.tag_bytes().to_vec())
            .unwrap_or_default();
        let plan = self.plan(&req, &key);
        let (req_span, _plan_span) = enter_plan_span(req.id(), &plan);
        let script = self.repair_script(&req);
        match plan {
            DispatchPlan::Drop => DispatchOutcome::Drop,
            DispatchPlan::NoTargets => {
                let err_type = error_rsp_type(self.pool.config().data_store);
                let rsp = crate::msg::response::make_error(
                    &req,
                    err_type,
                    0,
                    crate::msg::DynErrorCode::DynomiteNoQuorumAchieved,
                    &self.mbuf_pool,
                );
                DispatchOutcome::Error(rsp)
            }
            DispatchPlan::LocalDatastore => {
                // In-process datastore hook takes precedence: hand
                // the parsed request to the embedder's `Datastore`
                // and deliver its reply on the connection's
                // responder. The hook is async and `dispatch` is
                // sync, so spawn the await and return `Pending`
                // (the same contract the backend-channel and
                // replica paths use).
                if let Some(ds) = self.local_datastore.as_ref() {
                    let ds = Arc::clone(ds);
                    let req_id = req.id();
                    let span = req_span.clone();
                    tokio::spawn(async move {
                        if let Ok(rsp) = ds.dispatch(req).await {
                            let _ = responder
                                .send(OutboundEnvelope {
                                    req_id,
                                    rsp,
                                    span,
                                    source_peer_idx: None,
                                })
                                .await;
                        }
                    });
                    return DispatchOutcome::Pending;
                }
                if let Some(tx) = self.backend.as_ref() {
                    // Snapshot the wire bytes from the parsed mbuf
                    // chain. The chain is the original on-the-wire
                    // sequence the parser walked, so this is a
                    // faithful relay rather than a re-encode.
                    let bytes = script
                        .as_ref()
                        .map_or_else(|| msg_bytes(&req), ScriptRequest::encode);
                    let responder = self.client_sink(script.as_ref(), req.ty(), responder);
                    if bytes.is_empty() {
                        // Parsed request with no replayable bytes
                        // (e.g. a synthetic `Msg`) - drop rather
                        // than enqueue a no-op on the backend.
                        return DispatchOutcome::Drop;
                    }
                    let env = OutboundRequest {
                        bytes,
                        req_id: req.id(),
                        responder,
                        span: req_span.clone(),
                        ty: crate::proto::dnode::DmsgType::Req,
                        target_peer_idx: None,
                        deadline: self.pool.request_deadline(&req, None),
                    };
                    let is_read = matches!(req.ty(), MsgType::Unknown) || req.flags().is_read;
                    if let Err(err) = tx.send(&self.pool, env, is_read) {
                        let LocalSendError::Channel(err) = err else {
                            return DispatchOutcome::Error(self.invalid_state_error(&req));
                        };
                        // Backend channel full or closed: surface
                        // an error to the client immediately.
                        if let Some(m) = self.failure_metrics.as_ref() {
                            match err {
                                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                                    m.record_backend_send_full();
                                }
                                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                                    m.record_backend_send_closed();
                                }
                            }
                        }
                        let err_type = error_rsp_type(self.pool.config().data_store);
                        let rsp = crate::msg::response::make_error(
                            &req,
                            err_type,
                            0,
                            crate::msg::DynErrorCode::DynomiteNoQuorumAchieved,
                            &self.mbuf_pool,
                        );
                        return DispatchOutcome::Error(rsp);
                    }
                }
                DispatchOutcome::Pending
            }
            DispatchPlan::Replicas {
                targets,
                consistency,
            } => self.dispatch_replicas(
                &req,
                &req_span,
                &targets,
                consistency,
                responder,
                script.as_ref(),
            ),
        }
    }
}

impl ClusterDispatcher {
    /// Fan a request out across replicas and install the per-
    /// request reply coalescer.
    ///
    /// The single-target case short-circuits to a direct
    /// forward (no coalescer needed). The multi-target case
    /// spawns a coalescer task on the ambient tokio runtime; the
    /// task drains the per-target replies, picks one according
    /// to the consistency level, and forwards the chosen reply
    /// to the original `responder`. Under read repair
    /// ([`ServerPool::is_read_repairs_enabled`]) `script` is the
    /// repair-script form every target receives; replicas a
    /// repairable read finds stale get a repair write via the same
    /// channels.
    ///
    /// When [`hinted_handoff_active`](Self::hinted_handoff_active)
    /// reports true and the request is a write, targets that
    /// are currently in [`crate::cluster::peer::PeerState::Down`]
    /// are recorded in the hint store instead of being sent.
    /// A synthetic `+OK\r\n` reply is fed to the coalescer on
    /// the hinted target's behalf so the consistency threshold
    /// can be met by the surviving replicas plus the hint(s).
    /// On `try_send` failure (channel closed or full) for a
    /// non-Down peer, the dispatcher likewise falls back to
    /// hinting before declaring the target lost.
    fn dispatch_replicas(
        &self,
        req: &Msg,
        req_span: &tracing::Span,
        targets: &[ReplicaTarget],
        consistency: ConsistencyLevel,
        responder: ServerSink,
        script: Option<&ScriptRequest>,
    ) -> DispatchOutcome {
        if targets.is_empty() {
            return DispatchOutcome::Drop;
        }
        // Snapshot the wire bytes once. Each target gets its
        // own clone (the ServerConn / DnodeServerConn takes
        // ownership of `bytes`).
        let bytes = script.map_or_else(|| msg_bytes(req), ScriptRequest::encode);
        if bytes.is_empty() {
            return DispatchOutcome::Drop;
        }
        // Snapshot the per-target current state so we do not
        // re-acquire the peer-table lock inside the per-target
        // loop. Local peers are always treated as Normal.
        let peer_states = self.snapshot_peer_states(targets);
        let is_read = matches!(req.ty(), MsgType::Unknown) || req.flags().is_read;
        let is_write = !is_read;
        let handoff_active = self.hinted_handoff_active() && is_write;
        // Single-target path: no coalescing needed; forward
        // directly to the original responder.
        if targets.len() == 1 {
            let responder = self.client_sink(script, req.ty(), responder);
            return self.dispatch_replicas_direct(
                req,
                req_span,
                targets,
                &bytes,
                &responder,
                &HandoffCtx {
                    handoff_active,
                    peer_states: &peer_states,
                },
            );
        }
        // Multi-target path: install the coalescer.
        let cfg = self.pool.config();
        let local_dc = cfg.dc.clone();
        // Channel each replica's reply lands on. Sized to
        // `targets.len() + 1`: every target produces at most one
        // envelope (real or hint-synthesised) and we leave a
        // spare so a late repair reply never blocks the actor.
        let (intermediate_tx, intermediate_rx) =
            mpsc::channel::<OutboundEnvelope>(targets.len() + 1);
        // Build the tracker's target list and capture the
        // per-target dispatch state.
        let target_pairs: Vec<(u32, String)> =
            targets.iter().map(|t| (t.peer_idx, t.dc.clone())).collect();
        // Read repair context, for requests rewritten into a repair
        // script.
        let repair_ctx = script.map(|script| ReadRepairContext {
            req_id: req.id(),
            req_ty: req.ty(),
            script: script.clone(),
            peer_backends: self.peer_backends.clone(),
            local_backend: self.backend.clone(),
            pool: self.pool.clone(),
            target_is_local: targets.iter().map(|t| (t.peer_idx, t.is_local)).collect(),
        });
        // Fan out: each per-target outbound feeds the coalescer
        // channel, NOT the client's responder.
        let mut sent = 0usize;
        let mut hinted = 0usize;
        for target in targets {
            let action = Self::choose_target_action(target, handoff_active, &peer_states);
            match action {
                TargetAction::Send => {
                    if self.fanout_send(target, req, req_span, &bytes, &intermediate_tx) {
                        sent += 1;
                    } else if handoff_active
                        && self.hint_target(target, &bytes, req, req_span, &intermediate_tx)
                    {
                        hinted += 1;
                    }
                }
                TargetAction::Hint => {
                    if self.hint_target(target, &bytes, req, req_span, &intermediate_tx) {
                        hinted += 1;
                    }
                }
            }
        }
        // Drop the local clone of the intermediate sender so the
        // coalescer task observes RX close once every per-target
        // sender has been dropped. (`OutboundRequest` owns one
        // sender each; once they finish they drop it.)
        drop(intermediate_tx);
        if sent + hinted == 0 {
            return DispatchOutcome::Error(self.no_quorum_error(req));
        }
        let req_id = req.id();
        let req_ty = req.ty();
        let mbuf_pool = self.mbuf_pool.clone();
        let failure_metrics = self.failure_metrics.clone();
        let data_store = cfg.data_store;
        tokio::spawn(coalesce_actor(
            req_id,
            req_ty,
            data_store,
            consistency,
            target_pairs,
            local_dc,
            intermediate_rx,
            responder,
            mbuf_pool,
            repair_ctx,
            failure_metrics,
        ));
        DispatchOutcome::Pending
    }

    /// The repair script a client request is rewritten into: when read
    /// repairs are on ([`ServerPool::is_read_repairs_enabled`]) and the
    /// command has repair support, stamped with the current time in
    /// milliseconds. Requests relayed by a peer are served as they came.
    fn repair_script(&self, req: &Msg) -> Option<ScriptRequest> {
        if req.dmsg().is_some()
            || matches!(
                self.pool.config().data_store,
                crate::conf::DataStore::Memcache
            )
            || !self.pool.is_read_repairs_enabled()
        {
            return None;
        }
        cmd_info(req.ty())?;
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
        ScriptRequest::from_request(req, ts)
    }

    /// The sink a single replica's reply goes to: the client's, with the
    /// repair metadata stripped from a rewritten repairable read.
    fn client_sink(
        &self,
        script: Option<&ScriptRequest>,
        ty: MsgType,
        client: ServerSink,
    ) -> ServerSink {
        if script.is_none() || !cmd_info(ty).is_some_and(|i| i.is_repairable) {
            return client;
        }
        let pool = self.mbuf_pool.clone();
        let (tx, mut rx) = mpsc::channel::<OutboundEnvelope>(1);
        tokio::spawn(async move {
            while let Some(mut env) = rx.recv().await {
                if let Some(r) = TsReply::parse(&msg_bytes(&env.rsp)) {
                    env.rsp = plain_rsp(env.req_id, &r.client_reply(), &pool);
                }
                if client.send(env).await.is_err() {
                    break;
                }
            }
        });
        tx
    }

    /// Capture the current `PeerState` for each target so the
    /// per-target loop does not re-acquire the read lock on
    /// every iteration. Local-node targets are reported as
    /// `Normal` regardless of the on-disk peer entry.
    fn snapshot_peer_states(
        &self,
        targets: &[ReplicaTarget],
    ) -> std::collections::HashMap<u32, crate::cluster::peer::PeerState> {
        use crate::cluster::peer::PeerState;
        let peers = self.pool.peers().read();
        let mut out = std::collections::HashMap::with_capacity(targets.len());
        for t in targets {
            let state = if t.is_local {
                PeerState::Normal
            } else {
                peers
                    .get(t.peer_idx as usize)
                    .map_or(PeerState::Unknown, crate::cluster::peer::Peer::state)
            };
            out.insert(t.peer_idx, state);
        }
        out
    }

    fn choose_target_action(
        target: &ReplicaTarget,
        handoff_active: bool,
        peer_states: &std::collections::HashMap<u32, crate::cluster::peer::PeerState>,
    ) -> TargetAction {
        use crate::cluster::peer::PeerState;
        if !handoff_active {
            return TargetAction::Send;
        }
        let state = peer_states
            .get(&target.peer_idx)
            .copied()
            .unwrap_or(PeerState::Unknown);
        match state {
            PeerState::Down => TargetAction::Hint,
            _ => TargetAction::Send,
        }
    }

    /// Forward one target via its outbound channel. Returns
    /// `true` on a successful `try_send`.
    fn fanout_send(
        &self,
        target: &ReplicaTarget,
        req: &Msg,
        req_span: &tracing::Span,
        bytes: &[u8],
        intermediate_tx: &mpsc::Sender<OutboundEnvelope>,
    ) -> bool {
        // A request forwarded to a REMOTE peer is tagged
        // `ReqForward` so the receiver hands it straight to its
        // local datastore instead of re-routing it (which would
        // re-hash, re-plan, and in the worst case bounce the
        // request back). The local-backend path keeps `Req`.
        let ty = if target.is_local {
            crate::proto::dnode::DmsgType::Req
        } else {
            crate::proto::dnode::DmsgType::ReqForward
        };
        let env = OutboundRequest {
            bytes: bytes.to_vec(),
            req_id: req.id(),
            responder: intermediate_tx.clone(),
            span: req_span.clone(),
            ty,
            target_peer_idx: Some(target.peer_idx),
            deadline: self
                .pool
                .request_deadline(req, (!target.is_local).then_some(target.dc.as_str())),
        };
        let send_result = if target.is_local {
            let is_read = matches!(req.ty(), MsgType::Unknown) || req.flags().is_read;
            match self
                .backend
                .as_ref()
                .map(|tx| tx.send(&self.pool, env, is_read))
            {
                Some(Err(LocalSendError::Refused)) => {
                    // The node's state refuses its own replica: that
                    // replica answers with the state error, which the
                    // coalescer counts like any other reply.
                    let rsp = self.invalid_state_error(req);
                    return intermediate_tx
                        .try_send(OutboundEnvelope {
                            req_id: req.id(),
                            rsp,
                            span: req_span.clone(),
                            source_peer_idx: Some(target.peer_idx),
                        })
                        .is_ok();
                }
                Some(Err(LocalSendError::Channel(e))) => Some(Err(e)),
                Some(Ok(())) => Some(Ok(())),
                None => None,
            }
        } else {
            self.peer_backends
                .get(&target.peer_idx)
                .map(|tx| tx.try_send(env))
        };
        match send_result {
            Some(Ok(())) => true,
            Some(Err(err)) => {
                self.observe_send_error(target, &err);
                false
            }
            None => false,
        }
    }

    /// Convert a `tokio::sync::mpsc::error::TrySendError` into a
    /// failure-metrics observation. Local targets bump the
    /// backend counters; peer targets bump the per-peer
    /// counters labelled with the peer's DC.
    fn observe_send_error(
        &self,
        target: &ReplicaTarget,
        err: &tokio::sync::mpsc::error::TrySendError<OutboundRequest>,
    ) {
        let Some(m) = self.failure_metrics.as_ref() else {
            return;
        };
        if target.is_local {
            match err {
                tokio::sync::mpsc::error::TrySendError::Full(_) => m.record_backend_send_full(),
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    m.record_backend_send_closed();
                }
            }
        } else {
            let peer_dc = self.peer_dc_label(target.peer_idx);
            match err {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    m.record_peer_send_full(target.peer_idx, &peer_dc);
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    m.record_peer_send_closed(target.peer_idx, &peer_dc);
                }
            }
        }
    }

    /// Record `bytes` as a hint for `target`'s peer and feed the
    /// coalescer a synthetic success reply. Returns `true` when
    /// both the enqueue and the synth-push succeeded.
    fn hint_target(
        &self,
        target: &ReplicaTarget,
        bytes: &[u8],
        req: &Msg,
        req_span: &tracing::Span,
        intermediate_tx: &mpsc::Sender<OutboundEnvelope>,
    ) -> bool {
        let Some(store) = self.hint_store.as_ref() else {
            return false;
        };
        let cfg = self.pool.config();
        let ttl = std::time::Duration::from_secs(cfg.hint_ttl_seconds.max(1));
        match store.enqueue(target.peer_idx, bytes.to_vec(), ttl) {
            Ok(()) => {}
            Err(e) => {
                tracing::debug!(
                    target: "dynomite::hints",
                    peer_idx = target.peer_idx,
                    error = %e,
                    "hint enqueue failed"
                );
                return false;
            }
        }
        let synth = synth_hint_reply(req, &self.mbuf_pool);
        let env = OutboundEnvelope {
            req_id: req.id(),
            rsp: synth,
            span: req_span.clone(),
            source_peer_idx: Some(target.peer_idx),
        };
        if intermediate_tx.try_send(env).is_err() {
            // The channel is sized for one envelope per target;
            // an immediate full means the coalescer task has
            // exited. Still report success so the hint sits in
            // the store for the drainer.
            tracing::debug!(
                target: "dynomite::hints",
                peer_idx = target.peer_idx,
                "hint synth-reply could not be queued; coalescer absent"
            );
        }
        tracing::debug!(
            target: "dynomite::hints",
            peer_idx = target.peer_idx,
            bytes = bytes.len(),
            "stored hint for down peer"
        );
        true
    }

    fn dispatch_replicas_direct(
        &self,
        req: &Msg,
        req_span: &tracing::Span,
        targets: &[ReplicaTarget],
        bytes: &[u8],
        responder: &ServerSink,
        ctx: &HandoffCtx<'_>,
    ) -> DispatchOutcome {
        debug_assert_eq!(targets.len(), 1);
        let target = &targets[0];
        // If the target is Down and handoff is active, hint it
        // and feed the responder a synth `+OK\r\n` so the
        // client sees the write as having succeeded.
        if let TargetAction::Hint =
            Self::choose_target_action(target, ctx.handoff_active, ctx.peer_states)
        {
            if self.hint_single_target_direct(target, bytes, req, req_span, responder) {
                return DispatchOutcome::Pending;
            }
            return DispatchOutcome::Error(self.no_quorum_error(req));
        }
        let env = OutboundRequest {
            bytes: bytes.to_vec(),
            req_id: req.id(),
            responder: responder.clone(),
            span: req_span.clone(),
            // A forward to a remote peer is `ReqForward` so the
            // receiver serves it locally rather than re-routing it;
            // the local-backend path uses `Req`.
            ty: if target.is_local {
                crate::proto::dnode::DmsgType::Req
            } else {
                crate::proto::dnode::DmsgType::ReqForward
            },
            target_peer_idx: Some(target.peer_idx),
            deadline: self
                .pool
                .request_deadline(req, (!target.is_local).then_some(target.dc.as_str())),
        };
        let send_result = if target.is_local {
            let is_read = matches!(req.ty(), MsgType::Unknown) || req.flags().is_read;
            match self
                .backend
                .as_ref()
                .map(|tx| tx.send(&self.pool, env, is_read))
            {
                Some(Err(LocalSendError::Refused)) => {
                    return DispatchOutcome::Error(self.invalid_state_error(req));
                }
                Some(Err(LocalSendError::Channel(e))) => Some(Err(e)),
                Some(Ok(())) => Some(Ok(())),
                None => None,
            }
        } else {
            self.peer_backends
                .get(&target.peer_idx)
                .map(|tx| tx.try_send(env))
        };
        let sent = match send_result {
            Some(Ok(())) => true,
            Some(Err(ref err)) => {
                self.observe_send_error(target, err);
                false
            }
            None => false,
        };
        if sent {
            return DispatchOutcome::Pending;
        }
        if ctx.handoff_active
            && self.hint_single_target_direct(target, bytes, req, req_span, responder)
        {
            return DispatchOutcome::Pending;
        }
        DispatchOutcome::Error(self.no_quorum_error(req))
    }

    /// Single-target hint path. Records the hint in the store
    /// and pushes a `+OK\r\n` envelope onto the responder. The
    /// client sees the write as having succeeded; the drainer
    /// will replay the request to the peer when it returns.
    fn hint_single_target_direct(
        &self,
        target: &ReplicaTarget,
        bytes: &[u8],
        req: &Msg,
        req_span: &tracing::Span,
        responder: &ServerSink,
    ) -> bool {
        let Some(store) = self.hint_store.as_ref() else {
            return false;
        };
        let cfg = self.pool.config();
        let ttl = std::time::Duration::from_secs(cfg.hint_ttl_seconds.max(1));
        if let Err(e) = store.enqueue(target.peer_idx, bytes.to_vec(), ttl) {
            tracing::debug!(
                target: "dynomite::hints",
                peer_idx = target.peer_idx,
                error = %e,
                "hint enqueue failed (single-target)"
            );
            return false;
        }
        let synth = synth_hint_reply(req, &self.mbuf_pool);
        let env = OutboundEnvelope {
            req_id: req.id(),
            rsp: synth,
            span: req_span.clone(),
            source_peer_idx: Some(target.peer_idx),
        };
        let _ = responder.try_send(env);
        true
    }

    /// The `Dynomite's current state does not allow this request` reply
    /// for a request the node's state refuses.
    fn invalid_state_error(&self, req: &Msg) -> Msg {
        crate::msg::response::make_error(
            req,
            error_rsp_type(self.pool.config().data_store),
            0,
            crate::msg::DynErrorCode::DynomiteInvalidState,
            &self.mbuf_pool,
        )
    }

    fn no_quorum_error(&self, req: &Msg) -> Msg {
        let err_type = error_rsp_type(self.pool.config().data_store);
        crate::msg::response::make_error(
            req,
            err_type,
            0,
            crate::msg::DynErrorCode::DynomiteNoQuorumAchieved,
            &self.mbuf_pool,
        )
    }

    /// FT.* / HSET interception. Returns `Some(outcome)` when the
    /// command was fully handled (FT.* keyword, or an HSET that
    /// references an indexed prefix with a malformed payload);
    /// returns `None` to let the caller fall through to the
    /// standard dispatch path. The HSET success path returns
    /// `None` after the extension records the side-effect so
    /// the standard storage write still goes to the backend.
    fn intercept_command(
        &self,
        ext: &dyn crate::embed::CommandExtension,
        req: &Msg,
    ) -> Option<DispatchOutcome> {
        if ext.handles_msg_type(req.ty()) {
            return Some(self.run_extension_command(ext, req));
        }
        if matches!(req.ty(), MsgType::ReqRedisHset) {
            return self.intercept_extension_hset(ext, req);
        }
        None
    }

    /// Drive an FT.* command through the registered extension
    /// and wrap the RESP bytes in a [`DispatchOutcome::Inline`].
    /// When the extension declines (`try_dispatch` returns
    /// `None`) the outcome is rendered as a `-ERR not
    /// supported in this build` reply so the dispatcher does
    /// not silently drop the request.
    fn run_extension_command(
        &self,
        ext: &dyn crate::embed::CommandExtension,
        req: &Msg,
    ) -> DispatchOutcome {
        // For typed FT.* variants the keyword is unambiguous; for
        // [`MsgType::ReqRedisFtUnknown`] we recover the original
        // wire keyword from the request's mbuf chain (the parser
        // case-folds for table lookup but the wire bytes are
        // preserved verbatim) so the extension can decide
        // whether we recognise the keyword and either execute
        // it or surface a structured `-ERR ...` reply.
        let recovered_kw: Vec<u8>;
        let keyword: &[u8] = match req.ty() {
            MsgType::ReqRedisFtCreate => b"FT.CREATE",
            MsgType::ReqRedisFtSearch => b"FT.SEARCH",
            MsgType::ReqRedisFtInfo => b"FT.INFO",
            MsgType::ReqRedisFtList => b"FT.LIST",
            MsgType::ReqRedisFtDropindex => b"FT.DROPINDEX",
            MsgType::ReqRedisFtRegex => b"FT.REGEX",
            MsgType::ReqRedisFtSugadd => b"FT.SUGADD",
            MsgType::ReqRedisFtSugget => b"FT.SUGGET",
            MsgType::ReqRedisFtSugdel => b"FT.SUGDEL",
            MsgType::ReqRedisFtSuglen => b"FT.SUGLEN",
            MsgType::ReqRedisFtUnknown => {
                recovered_kw = first_bulk_token(req).unwrap_or_else(|| b"FT.UNKNOWN".to_vec());
                recovered_kw.as_slice()
            }
            // The dispatcher only enters this branch from the
            // FT.* arm in [`Self::intercept_command`], so the
            // catch-all is unreachable unless the MsgType set
            // drifts out of sync with that arm.
            _ => return DispatchOutcome::Drop,
        };
        let mut args: Vec<&[u8]> = Vec::with_capacity(1 + req.keys().len() + req.args().len());
        args.push(keyword);
        for k in req.keys() {
            args.push(k.key());
        }
        for a in req.args() {
            args.push(a.bytes());
        }
        let bytes = ext.try_dispatch(&args).unwrap_or_else(|| {
            let kw = String::from_utf8_lossy(keyword);
            format!("-ERR not supported in this build: {kw}\r\n").into_bytes()
        });
        DispatchOutcome::Inline(synthetic_redis_reply(req, &self.mbuf_pool, &bytes))
    }

    /// HSET interception via the registered extension.
    /// Returns `Some(Error(...))` when the extension reports a
    /// malformed payload against a registered prefix; returns
    /// `None` (so the dispatcher falls through to the backend)
    /// on success or when no registered prefix matches.
    fn intercept_extension_hset(
        &self,
        ext: &dyn crate::embed::CommandExtension,
        req: &Msg,
    ) -> Option<DispatchOutcome> {
        let mut args: Vec<&[u8]> = Vec::with_capacity(req.keys().len() + req.args().len());
        for k in req.keys() {
            args.push(k.key());
        }
        for a in req.args() {
            args.push(a.bytes());
        }
        match ext.try_intercept_hset(&args) {
            crate::embed::HsetOutcome::Absorbed | crate::embed::HsetOutcome::NotIndexed => None,
            crate::embed::HsetOutcome::Error(message) => {
                let payload = format!("-ERR {message}\r\n");
                Some(DispatchOutcome::Error(synthetic_redis_reply(
                    req,
                    &self.mbuf_pool,
                    payload.as_bytes(),
                )))
            }
        }
    }
}

/// Wrap an arbitrary RESP byte sequence as a synthetic Redis
/// response [`Msg`]. The response inherits the request's id (so
/// the FSM can pair it with the originating request), is marked
/// `is_request = false`, and the supplied bytes are copied into
/// one or more mbufs drawn from `pool`.
fn synthetic_redis_reply(req: &Msg, pool: &MbufPool, payload: &[u8]) -> Msg {
    let mut rsp = Msg::new(req.id(), MsgType::RspRedisStatus, false);
    rsp.set_parent_id(req.id());
    let mut written = 0usize;
    while written < payload.len() {
        let mut buf = pool.get();
        let n = buf.recv(&payload[written..]);
        debug_assert!(
            n > 0,
            "MbufPool returned a buffer with zero writable capacity"
        );
        rsp.mbufs_mut().push_back(buf);
        written += n;
    }
    rsp.recompute_mlen();
    rsp
}

/// Recover the first bulk-string token from a parsed RESP
/// request's mbuf chain. The parser case-folds the keyword for
/// the command-table lookup but stores the original wire bytes
/// in the mbufs; this helper reads them back so the dispatcher
/// can render structured error replies that quote the actual
/// keyword the client sent.
///
/// Returns `None` when the mbuf chain does not begin with a
/// well-formed `*N\r\n$M\r\n<token>\r\n` sequence.
fn first_bulk_token(req: &Msg) -> Option<Vec<u8>> {
    let mut wire: Vec<u8> = Vec::new();
    for buf in req.mbufs() {
        wire.extend_from_slice(buf.readable());
        if wire.len() > 256 {
            break;
        }
    }
    let mut p = 0usize;
    if wire.first() == Some(&b'*') {
        let cr = wire.iter().position(|&b| b == b'\r')?;
        if wire.get(cr + 1) != Some(&b'\n') {
            return None;
        }
        p = cr + 2;
    }
    if wire.get(p) != Some(&b'$') {
        return None;
    }
    let header_start = p + 1;
    let header_cr = wire[header_start..]
        .iter()
        .position(|&b| b == b'\r')
        .map(|i| header_start + i)?;
    if wire.get(header_cr + 1) != Some(&b'\n') {
        return None;
    }
    let len_str = std::str::from_utf8(&wire[header_start..header_cr]).ok()?;
    let len: usize = len_str.parse().ok()?;
    let body_start = header_cr + 2;
    let body_end = body_start.checked_add(len)?;
    if wire.len() < body_end + 2 {
        return None;
    }
    Some(wire[body_start..body_end].to_vec())
}

/// Context for a request rewritten into a repair script: the coalescer
/// repairs stale replicas of a read and clears delete markers after a
/// delete.
#[derive(Clone)]
struct ReadRepairContext {
    req_id: crate::core::types::MsgId,
    req_ty: MsgType,
    /// The repair script the request was rewritten into.
    script: ScriptRequest,
    peer_backends: std::collections::HashMap<u32, mpsc::Sender<OutboundRequest>>,
    local_backend: Option<LocalBackend>,
    pool: Arc<ServerPool>,
    target_is_local: std::collections::HashMap<u32, bool>,
}

impl ReadRepairContext {
    fn is_repairable_read(&self) -> bool {
        cmd_info(self.req_ty).is_some_and(|i| i.is_repairable)
    }

    /// Repair the replicas whose reply is older than the freshest one
    /// and return the client bytes of the first reply that is not stale.
    /// `None` when nothing needed a repair or no repair could be built,
    /// so the replies are compared as they are.
    fn repair(&self, replies: &[(u32, TsReply)], span: &tracing::Span) -> Option<Vec<u8>> {
        let ts: Vec<TsReply> = replies.iter().map(|(_, r)| r.clone()).collect();
        let m = find_most_updated(&ts)?;
        let fix = repair_request(&self.script, self.req_ty, &ts[m.winner], m.repair_by_add)?;
        let bytes = fix.encode();
        for ((peer_idx, _), stale) in replies.iter().zip(&m.needs_repair) {
            if *stale {
                self.send(*peer_idx, &bytes, span);
            }
        }
        replies
            .iter()
            .zip(&m.needs_repair)
            .find(|(_, stale)| !**stale)
            .map(|((_, r), _)| r.client_reply())
    }

    /// Once every replica has answered a delete, drop its delete marker
    /// on all of them.
    fn cleanup(&self, span: &tracing::Span) {
        if !cmd_info(self.req_ty).is_some_and(|i| i.is_delete) {
            return;
        }
        let req = if self.req_ty == MsgType::ReqRedisDel {
            ScriptRequest::cleanup(Script::CleanupDel, &self.script.key, None, self.script.ts)
        } else {
            let Some(field) = self.script.fields.first() else {
                return;
            };
            ScriptRequest::cleanup(
                Script::CleanupHdel,
                &self.script.key,
                Some(field),
                self.script.ts,
            )
        };
        let bytes = req.encode();
        for &peer_idx in self.target_is_local.keys() {
            self.send(peer_idx, &bytes, span);
        }
    }

    /// Best-effort send to one replica; the reply is discarded. Tagged
    /// `ReqForward` so a peer serves it on its own datastore.
    fn send(&self, peer_idx: u32, bytes: &[u8], span: &tracing::Span) {
        let env = OutboundRequest {
            bytes: bytes.to_vec(),
            req_id: self.req_id,
            responder: repair_sink(),
            span: span.clone(),
            ty: crate::proto::dnode::DmsgType::ReqForward,
            target_peer_idx: Some(peer_idx),
            deadline: None,
        };
        let is_local = self
            .target_is_local
            .get(&peer_idx)
            .copied()
            .unwrap_or(false);
        let sent = if is_local {
            // Repair writes pass the node-state gate like any other
            // request bound for the local datastore.
            self.local_backend
                .as_ref()
                .is_some_and(|tx| tx.send(&self.pool, env, false).is_ok())
        } else {
            self.peer_backends
                .get(&peer_idx)
                .is_some_and(|tx| tx.try_send(env).is_ok())
        };
        tracing::debug!(
            target: "dynomite::read_repair",
            req_id = self.req_id,
            peer_idx,
            sent,
            "repair-script write",
        );
    }
}

/// The quorum-unreachable error reply for a request.
fn no_quorum_rsp(
    req_id: crate::core::types::MsgId,
    req_ty: MsgType,
    data_store: crate::conf::DataStore,
    pool: &MbufPool,
) -> Msg {
    let anchor = Msg::new(req_id, req_ty, true);
    crate::msg::response::make_error(
        &anchor,
        error_rsp_type(data_store),
        0,
        crate::msg::DynErrorCode::DynomiteNoQuorumAchieved,
        pool,
    )
}

/// A reply message holding `bytes`, typed by the response parser.
fn plain_rsp(req_id: crate::core::types::MsgId, bytes: &[u8], pool: &MbufPool) -> Msg {
    let mut rsp = Msg::new(req_id, MsgType::Unknown, false);
    let _ = crate::proto::redis::redis_parse_rsp(&mut rsp, bytes);
    rsp.set_parent_id(req_id);
    rsp.append_bytes(pool, bytes);
    rsp
}

/// Give a replica reply its type and error flag before it votes.
///
/// The local datastore's reply arrives already parsed; a peer's arrives as
/// the raw payload of a dnode frame (`MsgType::Unknown`, `is_error`
/// unset). The coalescer's equivalence key includes `is_error`, so without
/// this, identical error replies (e.g. `-NOSCRIPT` from every replica)
/// looked divergent and a `DC_SAFE_QUORUM` request failed. In C every
/// response, local or peer, passes through the datastore's response
/// parser before `rspmgr_submit_response` reads `rsp->is_error`.
fn classify_reply(rsp: &mut Msg, data_store: crate::conf::DataStore) {
    if !matches!(rsp.ty(), MsgType::Unknown) {
        return;
    }
    let bytes: Vec<u8> = rsp
        .mbufs()
        .iter()
        .flat_map(|b| b.readable().to_vec())
        .collect();
    let mut probe = Msg::new(rsp.id(), MsgType::Unknown, false);
    let parsed = match data_store {
        crate::conf::DataStore::Memcache => {
            crate::proto::memcache::memcache_parse_rsp(&mut probe, &bytes)
        }
        _ => crate::proto::redis::redis_parse_rsp(&mut probe, &bytes),
    };
    if matches!(parsed, crate::msg::MsgParseResult::Ok) {
        rsp.set_type(probe.ty());
        rsp.set_is_error(probe.flags().is_error);
    }
}

/// The error reply type for the pool's datastore: a Redis error for
/// Redis-protocol pools, `SERVER_ERROR` for memcache. The protocol is the
/// pool's, not the command's: C `msg_get_error` keys on `g_data_store`.
/// Keying on the command (as an earlier version did, Redis only for GET
/// and SET) sent memcache-style `SERVER_ERROR` text to Redis clients for
/// every other command, which they cannot parse.
fn error_rsp_type(data_store: crate::conf::DataStore) -> MsgType {
    match data_store {
        crate::conf::DataStore::Memcache => MsgType::RspMcServerError,
        _ => MsgType::RspRedisError,
    }
}

/// Per-fan-out coalescer task body.
#[allow(
    clippy::too_many_arguments,
    reason = "actor task captures the entire dispatch context; bundling into a struct adds churn for no callsite gain"
)]
async fn coalesce_actor(
    req_id: crate::core::types::MsgId,
    req_ty: MsgType,
    data_store: crate::conf::DataStore,
    consistency: ConsistencyLevel,
    targets: Vec<(u32, String)>,
    local_dc: String,
    mut intermediate_rx: mpsc::Receiver<OutboundEnvelope>,
    client_tx: ServerSink,
    mbuf_pool: MbufPool,
    repair_ctx: Option<ReadRepairContext>,
    failure_metrics: Option<Arc<crate::stats::FailureMetrics>>,
) {
    use crate::proto::redis::{CoalesceOutcome, CoalesceTracker};
    let total = targets.len();
    let mut tracker = CoalesceTracker::new(req_id, consistency, targets, &local_dc);
    let mut emitted = false;
    let ts_read = repair_ctx
        .as_ref()
        .is_some_and(ReadRepairContext::is_repairable_read);
    // Replies to a rewritten repairable read, in arrival order; each
    // votes with its metadata stripped.
    let mut ts_replies: Vec<(u32, TsReply)> = Vec::new();
    let mut replied = 0usize;
    let mut good = 0usize;
    while let Some(env) = intermediate_rx.recv().await {
        let source = env.source_peer_idx.unwrap_or(u32::MAX);
        let span = env.span.clone();
        let mut rsp = env.rsp;
        replied += 1;
        if ts_read {
            if let Some(r) = TsReply::parse(&msg_bytes(&rsp)) {
                rsp = plain_rsp(req_id, &r.client_reply(), &mbuf_pool);
                ts_replies.push((source, r));
            }
        }
        classify_reply(&mut rsp, data_store);
        if !rsp.flags().is_error {
            good += 1;
        }
        let outcome = tracker.record_reply(source, rsp);
        let fresh = match (&outcome, repair_ctx.as_ref()) {
            (CoalesceOutcome::Ready { .. } | CoalesceOutcome::Error(_), Some(ctx))
                if ts_read && !emitted =>
            {
                ctx.repair(&ts_replies, &span)
            }
            _ => None,
        };
        // Delete markers are dropped only once every replica deleted.
        if replied == total && good == total {
            if let Some(ctx) = repair_ctx.as_ref() {
                ctx.cleanup(&span);
            }
        }
        let to_client = match outcome {
            CoalesceOutcome::Pending => None,
            CoalesceOutcome::Ready { winner, .. } => Some(*winner),
            CoalesceOutcome::Error(reason) => {
                tracing::debug!(target: "dynomite::coalesce", req_id, reason = %reason, "coalesce error");
                Some(no_quorum_rsp(req_id, req_ty, data_store, &mbuf_pool))
            }
        };
        if let (Some(rsp), false) = (to_client, emitted) {
            // The freshest timestamped reply answers a repaired read.
            let rsp = fresh.map_or(rsp, |b| plain_rsp(req_id, &b, &mbuf_pool));
            let _ = client_tx
                .send(OutboundEnvelope {
                    req_id,
                    rsp,
                    span: span.clone(),
                    source_peer_idx: None,
                })
                .await;
            emitted = true;
        }
    }
    if !emitted {
        // No reply was emitted and the channel closed (every
        // per-target sender dropped without producing a reply).
        // From the dispatcher's perspective the request has
        // timed out at the coalescer layer; surface a
        // quorum-unreachable error so the client does not hang
        // and bump the response-timeout counter.
        if let Some(m) = failure_metrics.as_ref() {
            m.record_response_timeout(consistency);
        }
        let rsp = no_quorum_rsp(req_id, req_ty, data_store, &mbuf_pool);
        let _ = client_tx
            .send(OutboundEnvelope {
                req_id,
                rsp,
                span: tracing::Span::none(),
                source_peer_idx: None,
            })
            .await;
    }
}

/// Build a sink the read-repair task can drop replies into. The
/// scheduler is fire-and-forget: every reply is discarded.
fn repair_sink() -> ServerSink {
    let (tx, mut rx) = mpsc::channel::<OutboundEnvelope>(8);
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            // Drop the envelope; the original client already
            // received its reply on the main responder.
        }
    });
    tx
}

/// Bundle of handoff state passed into
/// [`ClusterDispatcher::dispatch_replicas_direct`]. Avoids a
/// noisy parameter list while keeping the per-call decisions
/// ("is handoff on?", "what state is each target in?") in one
/// place.
struct HandoffCtx<'a> {
    handoff_active: bool,
    peer_states: &'a std::collections::HashMap<u32, crate::cluster::peer::PeerState>,
}

/// Per-target dispatch action chosen by
/// [`ClusterDispatcher::choose_target_action`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TargetAction {
    /// Forward the request via the per-peer outbound channel.
    Send,
    /// Record a hint and feed the coalescer a synth success
    /// reply.
    Hint,
}

/// Synthetic reply pushed into the coalescer on behalf of a
/// hinted target. Always a `+OK\r\n` Redis status reply: the
/// dispatcher decouples the hint from the eventual peer reply
/// (the drainer will fire-and-forget the hint when the peer
/// returns), so the synth shape only needs to satisfy the
/// coalescer for SET-style writes which is the dominant case
/// for hinted handoff. Mismatched-shape writes (DEL with one
/// hinted target) coalesce to the surviving real reply via the
/// plurality / quorum branch and the divergence is harmless
/// because read repair acts only on timestamped read replies.
fn synth_hint_reply(req: &Msg, pool: &MbufPool) -> Msg {
    crate::msg::response::make_simple_redis(req, pool, b"+OK\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::peer::{Peer, PeerEndpoint, PeerState};
    use crate::conf::DataStore;
    use crate::hashkit::DynToken;

    fn cfg(read: ConsistencyLevel, write: ConsistencyLevel) -> crate::cluster::PoolConfig {
        crate::cluster::PoolConfig {
            read_consistency: read,
            write_consistency: write,
            dc: "dc1".into(),
            rack: "rA".into(),
            ..crate::cluster::PoolConfig::default()
        }
    }

    fn peer(idx: u32, dc: &str, rack: &str, tok: u32, is_local: bool, is_same: bool) -> Peer {
        let mut p = Peer::new(
            idx,
            PeerEndpoint::tcp("h".into(), 8101 + u16::try_from(idx).unwrap_or(0)),
            rack.into(),
            dc.into(),
            vec![DynToken::from_u32(tok)],
            is_local,
            is_same,
            false,
        );
        p.set_state(PeerState::Normal, 0);
        p
    }

    fn pool(read: ConsistencyLevel, write: ConsistencyLevel, peers: Vec<Peer>) -> Arc<ServerPool> {
        let pool = ServerPool::new(cfg(read, write), peers);
        pool.preselect_remote_racks();
        Arc::new(pool)
    }

    #[test]
    fn local_node_only_short_circuits() {
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![peer(0, "dc1", "rA", 10, true, true)],
        );
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        req.set_routing(MsgRouting::LocalNodeOnly);
        assert_eq!(
            ClusterDispatcher::new(p).plan(&req, b"k"),
            DispatchPlan::LocalDatastore,
        );
    }

    #[test]
    fn dc_one_read_targets_local_rack_when_present() {
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![
                peer(0, "dc1", "rA", 10, true, true),
                peer(1, "dc1", "rB", 20, false, true),
                peer(2, "dc2", "rA", 30, false, false),
            ],
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        // Any key resolves to peer 0 in rack rA (single-token continuum).
        let plan = ClusterDispatcher::new(p).plan(&req, b"hello");
        assert!(matches!(plan, DispatchPlan::LocalDatastore));
    }

    /// Regression: a DC_ONE WRITE must fan out to every local-DC
    /// replica (each rack is a replica), not just the rack-local node.
    /// C Dynomite replicates a DC_ONE write within the local DC; the
    /// Rust port previously picked one target for both reads and
    /// writes, so a write landed on a single backend and never
    /// replicated -- reads from other replicas then missed it (found
    /// by the EC2 differential vs C).
    #[test]
    fn dc_one_write_fans_out_to_all_local_replicas() {
        // Three local-DC replicas, one per rack (full-replica layout,
        // n_val = 3). Every node owns the whole ring (token 0), so a
        // write must reach all three.
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![
                peer(0, "dc1", "rA", 0, true, true),
                peer(1, "dc1", "rB", 0, false, true),
                peer(2, "dc1", "rC", 0, false, true),
            ],
        );
        // A write (is_read = false): ReqRedisSet.
        let mut req = Msg::new(1, MsgType::ReqRedisSet, false);
        req.flags_mut().is_read = false;
        req.push_key(crate::msg::keypos::KeyPos::without_tag(b"k".to_vec()));
        match ClusterDispatcher::new(p).plan(&req, b"k") {
            DispatchPlan::Replicas { targets, .. } => {
                assert_eq!(
                    targets.len(),
                    3,
                    "DC_ONE write fans out to all 3 local-DC replicas"
                );
                for t in &targets {
                    assert_eq!(t.dc, "dc1");
                }
            }
            other => panic!("expected a 3-replica fan-out for a DC_ONE write, got {other:?}"),
        }
    }

    /// Regression: a DC_ONE write on a single-node pool (the only
    /// local-DC replica is this node) must resolve to `LocalDatastore`,
    /// not a one-element `Replicas` fan-out. The fan-out form requires a
    /// wired backend channel; the embedded wire server serves local
    /// traffic through the local-datastore hook via `LocalDatastore`, so
    /// a lone-replica write that planned as `Replicas` returned a
    /// no-quorum error over the wire. The multi-replica fan-out above is
    /// unaffected. See commit 45ba694 (the DC_ONE write-fan-out change
    /// that introduced the regression) and the embed_api wire test
    /// `embedded_listen_socket_serves_a_wire_client`.
    #[test]
    fn dc_one_write_on_lone_replica_serves_local() {
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![peer(0, "dc1", "rA", 0, true, true)],
        );
        let mut req = Msg::new(1, MsgType::ReqRedisSet, false);
        req.flags_mut().is_read = false;
        req.push_key(crate::msg::keypos::KeyPos::without_tag(b"k".to_vec()));
        assert!(
            matches!(
                ClusterDispatcher::new(p).plan(&req, b"k"),
                DispatchPlan::LocalDatastore
            ),
            "a DC_ONE write whose only local replica is this node must \
             serve LocalDatastore, not fan out as a 1-element Replicas plan"
        );
    }

    /// A DC_ONE READ still picks the single rack-local replica
    /// (LocalDatastore here since the local node is a replica).
    #[test]
    fn dc_one_read_picks_one_replica() {
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![
                peer(0, "dc1", "rA", 0, true, true),
                peer(1, "dc1", "rB", 0, false, true),
                peer(2, "dc1", "rC", 0, false, true),
            ],
        );
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        req.push_key(crate::msg::keypos::KeyPos::without_tag(b"k".to_vec()));
        // Local node is a replica -> served locally, not fanned out.
        assert!(matches!(
            ClusterDispatcher::new(p).plan(&req, b"k"),
            DispatchPlan::LocalDatastore
        ));
    }

    #[test]
    fn dc_one_partitions_the_ring_within_a_shared_rack() {
        // The deployment topology that makes DC_ONE route by key: all
        // same-DC nodes share ONE rack (rA) and their tokens partition
        // the ring. A key whose token falls in a REMOTE node's range
        // must route to that node, not collapse to the local node.
        // This is the multi-region EC2 model (per-DC token partition
        // in a shared rack); one-node-per-rack instead makes every
        // rack own the whole ring and every DC_ONE read stay local.
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![
                peer(0, "dc1", "rA", 0, true, true),
                peer(1, "dc1", "rA", 1_431_655_765, false, true),
                peer(2, "dc1", "rA", 2_863_311_530, false, true),
            ],
        );
        let disp = ClusterDispatcher::new(p);
        // Probe many keys; at least one must route to a REMOTE peer
        // (not LocalDatastore), proving the ring is partitioned across
        // the shared-rack nodes rather than always-local.
        let mut saw_remote = false;
        let mut saw_local = false;
        for i in 0..200u32 {
            let key = format!("key-{i}");
            let req = Msg::new(1, MsgType::ReqRedisGet, true);
            match disp.plan(&req, key.as_bytes()) {
                DispatchPlan::LocalDatastore => saw_local = true,
                DispatchPlan::Replicas { targets, .. } => {
                    assert_eq!(targets.len(), 1, "DC_ONE picks one owner");
                    assert_eq!(targets[0].dc, "dc1");
                    saw_remote = true;
                }
                other => panic!("unexpected plan {other:?}"),
            }
        }
        assert!(
            saw_remote,
            "keys owned by a remote same-rack node must route to it, not stay local"
        );
        assert!(saw_local, "keys owned by the local node stay local");
    }

    /// Regression: a request forwarded to a REMOTE peer must be
    /// tagged `DmsgType::ReqForward`, not `Req`. The receiver only
    /// hands a `ReqForward` straight to its local datastore; a plain
    /// `Req` makes it re-route (re-hash / re-plan), so a write owned
    /// by a remote node never lands in that node's backend and is
    /// lost. Found on the EC2 cluster: the token owner received zero
    /// forwarded writes.
    #[tokio::test]
    async fn remote_forward_is_tagged_req_forward() {
        // Two same-rack peers with u32-spread tokens; peer 0 is
        // local. Find a key that routes to the remote peer 1 and
        // assert the captured OutboundRequest carries ReqForward.
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![
                peer(0, "dc1", "rA", 0, true, true),
                peer(1, "dc1", "rA", 2_147_483_648, false, true),
            ],
        );
        let (tx, mut rx) = mpsc::channel::<crate::net::server::OutboundRequest>(64);
        let disp = ClusterDispatcher::new(p).with_peer_backend(1, tx);
        // Drive keys until one routes to the remote peer.
        let pool_buf = crate::io::mbuf::MbufPool::default();
        let mut forwarded_ty = None;
        for i in 0..500u32 {
            let key = format!("k{i}");
            let mut req = Msg::new(1, MsgType::ReqRedisSet, false);
            req.push_key(crate::msg::keypos::KeyPos::without_tag(key.into_bytes()));
            let mut buf = pool_buf.get();
            buf.copy_from_slice(b"*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$1\r\ny\r\n");
            req.mbufs_mut().push_back(buf);
            let (resp_tx, _resp_rx) = mpsc::channel(1);
            let _ = disp.dispatch(req, resp_tx);
            if let Ok(env) = rx.try_recv() {
                forwarded_ty = Some(env.ty);
                break;
            }
        }
        assert_eq!(
            forwarded_ty,
            Some(crate::proto::dnode::DmsgType::ReqForward),
            "a forward to a remote peer must be ReqForward so the target serves it locally"
        );
    }

    #[test]
    fn dc_quorum_fans_out_local_dc() {
        let p = pool(
            ConsistencyLevel::DcQuorum,
            ConsistencyLevel::DcQuorum,
            vec![
                peer(0, "dc1", "rA", 10, true, true),
                peer(1, "dc1", "rB", 20, false, true),
                peer(2, "dc2", "rA", 30, false, false),
            ],
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"k");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => {
                assert_eq!(rs.len(), 2);
                for r in rs {
                    assert_eq!(r.dc, "dc1");
                }
            }
            _ => panic!("expected replicas"),
        }
    }

    #[test]
    fn dc_each_safe_quorum_fans_out_per_dc() {
        let p = pool(
            ConsistencyLevel::DcEachSafeQuorum,
            ConsistencyLevel::DcEachSafeQuorum,
            vec![
                peer(0, "dc1", "rA", 10, true, true),
                peer(1, "dc2", "rA", 20, false, false),
            ],
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"k");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => {
                assert_eq!(rs.len(), 2);
                let dcs: Vec<&str> = rs.iter().map(|r| r.dc.as_str()).collect();
                assert!(dcs.contains(&"dc1"));
                assert!(dcs.contains(&"dc2"));
            }
            _ => panic!("expected replicas"),
        }
    }

    #[test]
    fn no_routable_peers_returns_no_targets() {
        let mut p0 = peer(0, "dc1", "rA", 10, true, true);
        p0.set_state(PeerState::Down, 0);
        let p = pool(
            ConsistencyLevel::DcQuorum,
            ConsistencyLevel::DcQuorum,
            vec![p0],
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"k");
        assert_eq!(plan, DispatchPlan::NoTargets);
    }

    /// Regression: any code path that returns
    /// `DispatchOutcome::Error` used to send 0 wire bytes to the
    /// client because [`crate::msg::response::make_error`] never
    /// attached the wire-format error string. The client then
    /// hung until its read timeout. After the fix, the error
    /// response carries a parseable `-ERR Dynomite: ...` reply that
    /// the client can render as an error.
    #[test]
    fn no_targets_error_response_carries_dynomite_wire_bytes() {
        let mut p0 = peer(0, "dc1", "rA", 10, true, true);
        p0.set_state(PeerState::Down, 0);
        let p = pool(
            ConsistencyLevel::DcQuorum,
            ConsistencyLevel::DcQuorum,
            vec![p0],
        );
        let disp = ClusterDispatcher::new(p);
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        req.push_key(crate::msg::keypos::KeyPos::without_tag(b"k".to_vec()));
        let (tx, _rx) = mpsc::channel(1);
        let outcome = disp.dispatch(req, tx);
        match outcome {
            DispatchOutcome::Error(rsp) => {
                assert_eq!(rsp.ty(), MsgType::RspRedisError);
                assert!(rsp.flags().is_error);
                let bytes: Vec<u8> = rsp
                    .mbufs()
                    .iter()
                    .flat_map(|b| b.readable().to_vec())
                    .collect();
                assert!(
                    !bytes.is_empty(),
                    "NoTargets must produce on-wire bytes, not a 0-byte hang"
                );
                assert!(bytes.starts_with(b"-ERR Dynomite: "));
                assert!(bytes.ends_with(b"\r\n"));
                assert_eq!(rsp.mlen() as usize, bytes.len());
            }
            other => panic!("expected DispatchOutcome::Error, got {other:?}"),
        }
    }

    /// Memcache traffic with no quorum-eligible target must
    /// surface a `SERVER_ERROR ...\r\n` reply rather than
    /// hanging the client.
    #[test]
    fn no_targets_error_response_memcache_wire_bytes() {
        // Build a memcache pool so the dispatcher's err_type
        // selection lands on `RspMcServerError` (the dispatcher
        // currently keys off the request `MsgType`, so a
        // memcache request flows through the memcache wire
        // shape).
        let mut cfg = cfg(ConsistencyLevel::DcQuorum, ConsistencyLevel::DcQuorum);
        cfg.data_store = DataStore::Memcache;
        let mut p0 = peer(0, "dc1", "rA", 10, true, true);
        p0.set_state(PeerState::Down, 0);
        let pool_arc = ServerPool::new(cfg, vec![p0]);
        pool_arc.preselect_remote_racks();
        let disp = ClusterDispatcher::new(Arc::new(pool_arc));
        let mut req = Msg::new(1, MsgType::ReqMcGet, true);
        req.push_key(crate::msg::keypos::KeyPos::without_tag(b"k".to_vec()));
        let (tx, _rx) = mpsc::channel(1);
        let outcome = disp.dispatch(req, tx);
        match outcome {
            DispatchOutcome::Error(rsp) => {
                assert_eq!(rsp.ty(), MsgType::RspMcServerError);
                let bytes: Vec<u8> = rsp
                    .mbufs()
                    .iter()
                    .flat_map(|b| b.readable().to_vec())
                    .collect();
                assert!(
                    !bytes.is_empty(),
                    "NoTargets must produce on-wire bytes, not a 0-byte hang"
                );
                assert!(bytes.starts_with(b"SERVER_ERROR "));
                assert!(bytes.ends_with(b"\r\n"));
            }
            other => panic!("expected DispatchOutcome::Error, got {other:?}"),
        }
    }

    use crate::cluster::pool::{BucketType, PoolConfig};

    fn pool_with_bucket_types(
        pool_read: ConsistencyLevel,
        pool_write: ConsistencyLevel,
        bucket_types: Vec<BucketType>,
        default_bucket_type: Option<&str>,
        peers: Vec<Peer>,
    ) -> Arc<ServerPool> {
        let cfg = PoolConfig {
            read_consistency: pool_read,
            write_consistency: pool_write,
            dc: "dc1".into(),
            rack: "rA".into(),
            bucket_types,
            default_bucket_type: default_bucket_type.map(str::to_string),
            ..PoolConfig::default()
        };
        let pool = ServerPool::new(cfg, peers);
        pool.preselect_remote_racks();
        Arc::new(pool)
    }

    fn three_local_peers() -> Vec<Peer> {
        vec![
            peer(0, "dc1", "rA", 10, true, true),
            peer(1, "dc1", "rB", 20, false, true),
            peer(2, "dc1", "rC", 30, false, true),
        ]
    }

    #[test]
    fn bucket_type_overrides_pool_consistency() {
        // Pool default is DC_ONE, the bucket forces DC_QUORUM.
        let bts = vec![BucketType {
            name: "hot".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 0,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            None,
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"hot/key1");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => assert_eq!(rs.len(), 3),
            other => panic!("expected DC_QUORUM fan-out, got {other:?}"),
        }
    }

    #[test]
    fn slashless_key_falls_back_to_pool_default() {
        let bts = vec![BucketType {
            name: "hot".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 0,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            None,
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"plain-key");
        // No slash and no default_bucket_type -> pool DC_ONE.
        // The local rack hosts peer 0 so the plan short-circuits.
        assert!(matches!(plan, DispatchPlan::LocalDatastore));
    }

    #[test]
    fn unknown_bucket_uses_default_bucket_type_when_set() {
        let bts = vec![BucketType {
            name: "safe".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 0,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            Some("safe"),
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        // Slashless key: bucket is None, default_bucket_type=safe applies
        // so we get the bucket-type's DC_QUORUM fan-out.
        let plan = ClusterDispatcher::new(p.clone()).plan(&req, b"plain-key");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => assert_eq!(rs.len(), 3),
            other => panic!("expected DC_QUORUM via default bucket, got {other:?}"),
        }
        // Slashed key with an unknown bucket prefix also falls
        // through to the default bucket type.
        let plan = ClusterDispatcher::new(p).plan(&req, b"unknown-bucket/key");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => assert_eq!(rs.len(), 3),
            other => panic!("expected DC_QUORUM via default bucket, got {other:?}"),
        }
    }

    #[test]
    fn unknown_bucket_with_no_default_uses_pool_default() {
        let bts = vec![BucketType {
            name: "safe".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 0,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            None,
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"unknown-bucket/key");
        assert!(matches!(plan, DispatchPlan::LocalDatastore));
    }

    #[test]
    fn n_val_one_caps_replicas_to_first_target() {
        let bts = vec![BucketType {
            name: "thin".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 1,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            None,
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"thin/key");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => assert_eq!(rs.len(), 1),
            other => panic!("expected single-target plan, got {other:?}"),
        }
    }

    #[test]
    fn n_val_two_caps_replicas_to_first_two_targets() {
        let bts = vec![BucketType {
            name: "medium".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 2,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            None,
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"medium/key");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => assert_eq!(rs.len(), 2),
            other => panic!("expected two-target plan, got {other:?}"),
        }
    }

    #[test]
    fn n_val_zero_does_not_cap() {
        let bts = vec![BucketType {
            name: "any".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 0,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            None,
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"any/key");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => assert_eq!(rs.len(), 3),
            other => panic!("expected uncapped plan, got {other:?}"),
        }
    }

    #[test]
    fn n_val_larger_than_replicas_is_a_no_op() {
        let bts = vec![BucketType {
            name: "big".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            n_val: 7,
        }];
        let p = pool_with_bucket_types(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            bts,
            None,
            three_local_peers(),
        );
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        let plan = ClusterDispatcher::new(p).plan(&req, b"big/key");
        match plan {
            DispatchPlan::Replicas { targets: rs, .. } => assert_eq!(rs.len(), 3),
            other => panic!("expected uncapped plan, got {other:?}"),
        }
    }

    /// Smoke test for the failure-cause counter wiring: a
    /// `NoTargets` plan increments the labelled counter
    /// exactly once.
    #[test]
    fn no_targets_records_failure_metric() {
        let mut p0 = peer(0, "dc1", "rA", 10, true, true);
        p0.set_state(PeerState::Down, 0);
        let p = pool(
            ConsistencyLevel::DcQuorum,
            ConsistencyLevel::DcQuorum,
            vec![p0],
        );
        let metrics = Arc::new(crate::stats::FailureMetrics::new());
        let disp = ClusterDispatcher::new(p).with_failure_metrics(metrics.clone());
        let req = Msg::new(1, MsgType::ReqRedisGet, true);
        assert_eq!(disp.plan(&req, b"k"), DispatchPlan::NoTargets);
        let snap = metrics.snapshot();
        assert_eq!(snap.no_targets.len(), 1);
        let entry = &snap.no_targets[0];
        assert_eq!(entry.dc, "dc1");
        assert_eq!(entry.rack, "rA");
        assert_eq!(entry.consistency, ConsistencyLevel::DcQuorum);
        assert_eq!(entry.count, 1);
    }

    /// A closed mpsc channel returns `Closed` from
    /// `try_send`. Wire the dispatcher's local-datastore path
    /// to such a channel, fire one request, and assert the
    /// `dispatch_backend_send_closed_total` counter ticks by
    /// exactly one.
    #[tokio::test]
    async fn closed_backend_channel_records_closed_metric() {
        let p = pool(
            ConsistencyLevel::DcOne,
            ConsistencyLevel::DcOne,
            vec![peer(0, "dc1", "rA", 10, true, true)],
        );
        let (tx, rx) = mpsc::channel::<crate::net::server::OutboundRequest>(4);
        drop(rx);
        let metrics = Arc::new(crate::stats::FailureMetrics::new());
        let disp = ClusterDispatcher::new(p)
            .with_backend(tx)
            .with_failure_metrics(metrics.clone());
        let mut req = Msg::new(1, MsgType::ReqRedisGet, true);
        // Attach a non-empty mbuf so the dispatcher actually
        // attempts the try_send (empty bytes short-circuit
        // to Drop before the channel is touched).
        let pool_buf = crate::io::mbuf::MbufPool::default();
        let mut buf = pool_buf.get();
        buf.copy_from_slice(b"PING\r\n");
        req.mbufs_mut().push_back(buf);
        let (resp_tx, _resp_rx) = mpsc::channel(1);
        let outcome = disp.dispatch(req, resp_tx);
        assert!(matches!(outcome, DispatchOutcome::Error(_)));
        let snap = metrics.snapshot();
        assert_eq!(snap.backend_send_closed, 1);
        assert_eq!(snap.backend_send_full, 0);
    }

    /// Integration-style unit test: build a two-peer pool,
    /// mark one peer Down, drive 100 dispatches across the
    /// ring, and assert the `dispatch_no_targets_total`
    /// counter reflects every observed `NoTargets` plan.
    #[test]
    fn two_peer_pool_with_one_down_records_per_key_no_targets() {
        let cfg = crate::cluster::PoolConfig {
            dc: "dc1".into(),
            rack: "rA".into(),
            read_consistency: ConsistencyLevel::DcQuorum,
            write_consistency: ConsistencyLevel::DcQuorum,
            ..crate::cluster::PoolConfig::default()
        };
        // Single-rack two-peer ring: peer 0 owns the upper
        // half via wrap-around (token 2_147_483_648), peer 1
        // owns the lower half (token 0 plus the boundary up to
        // 2_147_483_648). With both peers in rack `rA` the
        // continuum has two entries, so each key maps to
        // exactly one peer; marking peer 1 Down causes its
        // arc to produce `NoTargets`.
        let p0 = peer(0, "dc1", "rA", 2_147_483_648, true, true);
        let mut p1 = peer(1, "dc1", "rA", 0, false, true);
        p1.set_state(PeerState::Down, 0);
        let pool_arc = ServerPool::new(cfg, vec![p0, p1]);
        pool_arc.preselect_remote_racks();
        let metrics = Arc::new(crate::stats::FailureMetrics::new());
        let disp = ClusterDispatcher::new(Arc::new(pool_arc)).with_failure_metrics(metrics.clone());
        let mut planned_no_targets = 0u64;
        let mut planned_routable = 0u64;
        for i in 0..100u32 {
            let key = format!("k{i:03}");
            let req = Msg::new(u64::from(i), MsgType::ReqRedisGet, true);
            match disp.plan(&req, key.as_bytes()) {
                DispatchPlan::NoTargets => planned_no_targets += 1,
                DispatchPlan::Replicas { .. } | DispatchPlan::LocalDatastore => {
                    planned_routable += 1;
                }
                DispatchPlan::Drop => panic!("unexpected Drop in plan"),
            }
        }
        assert!(planned_no_targets > 0, "expected some NoTargets dispatches");
        assert!(planned_routable > 0, "expected some routable dispatches");
        let snap = metrics.snapshot();
        let counter_total: u64 = snap.no_targets.iter().map(|e| e.count).sum();
        assert_eq!(
            counter_total, planned_no_targets,
            "dispatch_no_targets_total must match observed NoTargets count",
        );
        // No `Closed`/`Full` channel errors expected: we did
        // not wire any backends.
        assert_eq!(snap.backend_send_full, 0);
        assert_eq!(snap.backend_send_closed, 0);
        assert!(snap.peer_send_full.is_empty());
        assert!(snap.peer_send_closed.is_empty());
    }
}
