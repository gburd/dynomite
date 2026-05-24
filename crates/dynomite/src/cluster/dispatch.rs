//! Cluster-aware [`Dispatcher`](crate::net::Dispatcher).
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

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::cluster::pool::ServerPool;
use crate::cluster::snitch::{rack_distance, RackDistance};
use crate::cluster::vnode;
use crate::conf::HashType as ConfHashType;
use crate::hashkit::{self, HashType};
use crate::io::mbuf::MbufPool;
use crate::msg::{ConsistencyLevel, Msg, MsgRouting, MsgType};
use crate::net::dispatcher::{DispatchOutcome, Dispatcher, ServerSink};
use crate::net::server::OutboundRequest;

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
        DispatchPlan::Replicas(_) => "replicas",
    };
    let targets = match plan {
        DispatchPlan::Replicas(t) => t.len(),
        _ => 0,
    };
    let span = tracing::info_span!("dispatch.plan", req_id, plan = kind, targets,).entered();
    (req_span, span)
}

fn map_hash(h: ConfHashType) -> HashType {
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
    /// Forward to one or more peer replicas.
    Replicas(Vec<ReplicaTarget>),
    /// Reply with an error: the cluster has no quorum-eligible
    /// targets.
    NoTargets,
    /// Drop the request (`QUIT`-style swallow).
    Drop,
}

/// Cluster-aware dispatcher.
#[derive(Debug, Clone)]
pub struct ClusterDispatcher {
    pool: Arc<ServerPool>,
    /// Outbound channel feeding the local datastore driver. When
    /// `None`, `LocalDatastore` plans short-circuit to `Pending`
    /// without forwarding (used by tests that do not need a real
    /// backend). When set, requests for the local node are
    /// encoded onto the wire and shipped to the [`crate::net::ServerConn`]
    /// task that drives the redis / memcache backend.
    backend: Option<mpsc::Sender<OutboundRequest>>,
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
    /// # use dynomite::conf::{DataStore, HashType};
    /// # use dynomite::msg::ConsistencyLevel;
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
    /// # use dynomite::conf::{DataStore, HashType};
    /// # use dynomite::msg::ConsistencyLevel;
    /// # use dynomite::io::mbuf::MbufPool;
    /// # use std::sync::Arc;
    /// # let cfg = PoolConfig {
    /// #    name: "p".into(), dc: "d".into(), rack: "r".into(),
    /// #    data_store: DataStore::Redis, hash: HashType::Murmur,
    /// #    read_consistency: ConsistencyLevel::DcOne,
    /// #    write_consistency: ConsistencyLevel::DcOne,
    /// #    timeout_ms: 5_000, server_retry_timeout_ms: 30_000,
    /// #    server_failure_limit: 2, auto_eject_hosts: false,
    /// #    enable_gossip: false,
    /// #    bucket_types: Vec::new(), default_bucket_type: None,
    /// # };
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
        self.backend = Some(backend);
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
            return DispatchPlan::NoTargets;
        }
        if matches!(req.routing(), MsgRouting::LocalNodeOnly) {
            return DispatchPlan::LocalDatastore;
        }
        if key.is_empty() {
            return DispatchPlan::LocalDatastore;
        }
        let token = hashkit::hash(map_hash(cfg.hash), key);
        let bucket = crate::proto::redis::bucket_name(key);
        let bucket_type = cfg.resolve_bucket_type(bucket);
        let is_read = matches!(req.ty(), MsgType::Unknown) || req.flags().is_read;
        let consistency = match (bucket_type, is_read) {
            (Some(bt), true) => bt.read_consistency,
            (Some(bt), false) => bt.write_consistency,
            (None, true) => cfg.read_consistency,
            (None, false) => cfg.write_consistency,
        };
        let n_val_cap = bucket_type.map_or(0, |bt| bt.n_val);
        let dcs = self.pool.datacenters().read();
        let routable = collect_routable(&dcs, &peers, &token);
        if routable.is_empty() {
            return DispatchPlan::NoTargets;
        }
        let (local, remote): (Vec<_>, Vec<_>) = routable
            .into_iter()
            .partition(|(dc_idx, _, _)| dcs[*dc_idx].name() == cfg.dc);
        let plan =
            plan_with_consistency(cfg, &dcs, &peers, consistency, req.routing(), local, remote);
        cap_replicas(plan, n_val_cap)
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
        DispatchPlan::Replicas(mut targets) if targets.len() > cap => {
            targets.truncate(cap);
            DispatchPlan::Replicas(targets)
        }
        other => other,
    }
}

fn collect_routable(
    dcs: &[crate::cluster::Datacenter],
    peers: &[crate::cluster::peer::Peer],
    token: &crate::hashkit::DynToken,
) -> Vec<(usize, usize, u32)> {
    let mut routable: Vec<(usize, usize, u32)> = Vec::new();
    for (dc_idx, dc) in dcs.iter().enumerate() {
        for (rack_idx, rack) in dc.racks().iter().enumerate() {
            if let Some(peer_idx) = vnode::dispatch(rack.continuums(), token) {
                if let Some(peer) = peers.get(peer_idx as usize) {
                    if peer.state().is_routable() {
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
    }
}

fn plan_with_consistency(
    cfg: &crate::cluster::pool::PoolConfig,
    dcs: &[crate::cluster::Datacenter],
    peers: &[crate::cluster::peer::Peer],
    consistency: ConsistencyLevel,
    routing: MsgRouting,
    local: Vec<(usize, usize, u32)>,
    remote: Vec<(usize, usize, u32)>,
) -> DispatchPlan {
    let want_per_dc_fanout = matches!(consistency, ConsistencyLevel::DcEachSafeQuorum)
        || matches!(routing, MsgRouting::AllNodesAllRacksAllDcs);
    let mut targets: Vec<ReplicaTarget> = Vec::new();
    match consistency {
        ConsistencyLevel::DcOne => {
            if local.is_empty() {
                return DispatchPlan::NoTargets;
            }
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
    DispatchPlan::Replicas(targets)
}

impl Dispatcher for ClusterDispatcher {
    #[allow(
        clippy::too_many_lines,
        reason = "single dispatch fn must enumerate every plan; splitting hides the planner-to-effect mapping"
    )]
    fn dispatch(&self, req: Msg, responder: ServerSink) -> DispatchOutcome {
        if req.flags().quit {
            return DispatchOutcome::Drop;
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
        match plan {
            DispatchPlan::Drop => DispatchOutcome::Drop,
            DispatchPlan::NoTargets => {
                let err_type = if matches!(req.ty(), MsgType::ReqRedisGet | MsgType::ReqRedisSet) {
                    MsgType::RspRedisError
                } else {
                    MsgType::RspMcServerError
                };
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
                if let Some(tx) = self.backend.as_ref() {
                    // Snapshot the wire bytes from the parsed mbuf
                    // chain. The chain is the original on-the-wire
                    // sequence the parser walked, so this is a
                    // faithful relay rather than a re-encode.
                    let bytes: Vec<u8> = req
                        .mbufs()
                        .iter()
                        .flat_map(|b| b.readable().to_vec())
                        .collect();
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
                    };
                    if tx.try_send(env).is_err() {
                        // Backend channel full or closed: surface
                        // an error to the client immediately.
                        let err_type =
                            if matches!(req.ty(), MsgType::ReqRedisGet | MsgType::ReqRedisSet) {
                                MsgType::RspRedisError
                            } else {
                                MsgType::RspMcServerError
                            };
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
            DispatchPlan::Replicas(targets) => {
                if targets.is_empty() {
                    return DispatchOutcome::Drop;
                }
                // Snapshot the wire bytes once. Each target gets
                // its own clone (the ServerConn / DnodeServerConn
                // takes ownership of `bytes`).
                let bytes: Vec<u8> = req
                    .mbufs()
                    .iter()
                    .flat_map(|b| b.readable().to_vec())
                    .collect();
                if bytes.is_empty() {
                    return DispatchOutcome::Drop;
                }
                let mut sent = 0usize;
                for target in &targets {
                    if target.is_local {
                        if let Some(tx) = self.backend.as_ref() {
                            let env = OutboundRequest {
                                bytes: bytes.clone(),
                                req_id: req.id(),
                                responder: responder.clone(),
                                span: req_span.clone(),
                                ty: crate::proto::dnode::DmsgType::Req,
                            };
                            if tx.try_send(env).is_ok() {
                                sent += 1;
                            }
                        }
                    } else if let Some(tx) = self.peer_backends.get(&target.peer_idx) {
                        let env = OutboundRequest {
                            bytes: bytes.clone(),
                            req_id: req.id(),
                            responder: responder.clone(),
                            span: req_span.clone(),
                            ty: crate::proto::dnode::DmsgType::Req,
                        };
                        if tx.try_send(env).is_ok() {
                            sent += 1;
                        }
                    }
                }
                if sent == 0 {
                    let err_type =
                        if matches!(req.ty(), MsgType::ReqRedisGet | MsgType::ReqRedisSet) {
                            MsgType::RspRedisError
                        } else {
                            MsgType::RspMcServerError
                        };
                    let rsp = crate::msg::response::make_error(
                        &req,
                        err_type,
                        0,
                        crate::msg::DynErrorCode::DynomiteNoQuorumAchieved,
                        &self.mbuf_pool,
                    );
                    return DispatchOutcome::Error(rsp);
                }
                DispatchOutcome::Pending
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::peer::{Peer, PeerEndpoint, PeerState};
    use crate::conf::{DataStore, HashType};
    use crate::hashkit::DynToken;

    fn cfg(read: ConsistencyLevel, write: ConsistencyLevel) -> crate::cluster::PoolConfig {
        crate::cluster::PoolConfig {
            name: "p".into(),
            dc: "dc1".into(),
            rack: "rA".into(),
            data_store: DataStore::Redis,
            hash: HashType::Murmur,
            read_consistency: read,
            write_consistency: write,
            timeout_ms: 5_000,
            server_retry_timeout_ms: 30_000,
            server_failure_limit: 2,
            auto_eject_hosts: false,
            enable_gossip: false,
            bucket_types: Vec::new(),
            default_bucket_type: None,
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
            DispatchPlan::Replicas(rs) => {
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
            DispatchPlan::Replicas(rs) => {
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
    /// response carries a parseable `-Dynomite: ...` reply that
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
                assert!(bytes.starts_with(b"-Dynomite: "));
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
            DispatchPlan::Replicas(rs) => assert_eq!(rs.len(), 3),
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
            DispatchPlan::Replicas(rs) => assert_eq!(rs.len(), 3),
            other => panic!("expected DC_QUORUM via default bucket, got {other:?}"),
        }
        // Slashed key with an unknown bucket prefix also falls
        // through to the default bucket type.
        let plan = ClusterDispatcher::new(p).plan(&req, b"unknown-bucket/key");
        match plan {
            DispatchPlan::Replicas(rs) => assert_eq!(rs.len(), 3),
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
            DispatchPlan::Replicas(rs) => assert_eq!(rs.len(), 1),
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
            DispatchPlan::Replicas(rs) => assert_eq!(rs.len(), 2),
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
            DispatchPlan::Replicas(rs) => assert_eq!(rs.len(), 3),
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
            DispatchPlan::Replicas(rs) => assert_eq!(rs.len(), 3),
            other => panic!("expected uncapped plan, got {other:?}"),
        }
    }
}
