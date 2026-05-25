//! Cluster admin RPC surface.
//!
//! This module defines the trait [`ClusterAdmin`] that the
//! `dyn-riak` PBC server invokes when an operator drives one of
//! the `cluster-list`, `cluster-join`, `cluster-leave`,
//! `cluster-plan`, or `cluster-commit` admin commands. The
//! reference engine pulls these mutations off a dedicated
//! gossip-state DNODE message family; the Rust port surfaces
//! them through this trait so the same staging-then-commit
//! semantics can be exercised from tests and from the CLI
//! without forcing a real DNODE round-trip.
//!
//! The default in-process implementation is
//! [`PoolClusterAdmin`]: it owns an `Arc<ServerPool>` and a
//! lock-protected list of staged [`ClusterChange`] entries.
//! `cluster_join` and `cluster_leave` push a change onto the
//! staging list and return the [`JoinPlan`] so the caller can
//! print it; `cluster_commit` applies every staged change in
//! order under the pool's existing peer / auto-eject
//! `RwLock`s and rebuilds the token continuum on the way out.
//!
//! # Examples
//!
//! ```
//! use std::net::SocketAddr;
//! use std::sync::Arc;
//!
//! use dynomite::cluster::admin_rpc::{ClusterAdmin, PoolClusterAdmin};
//! use dynomite::cluster::peer::{Peer, PeerEndpoint};
//! use dynomite::cluster::pool::{PoolConfig, ServerPool};
//! use dynomite::hashkit::DynToken;
//!
//! let cfg = PoolConfig {
//!     dc: "dc1".into(),
//!     rack: "r1".into(),
//!     ..PoolConfig::default()
//! };
//! let local = Peer::new(
//!     0, PeerEndpoint::tcp("127.0.0.1".into(), 8101), "r1".into(), "dc1".into(),
//!     vec![DynToken::from_u32(0)], true, true, false,
//! );
//! let pool = Arc::new(ServerPool::new(cfg, vec![local]));
//! let admin = PoolClusterAdmin::new(pool);
//! assert_eq!(admin.list_peers().len(), 1);
//! let target: SocketAddr = "127.0.0.1:8102".parse().unwrap();
//! let plan = admin.cluster_join(target).unwrap();
//! assert!(plan.change.peer.is_some());
//! assert_eq!(admin.cluster_plan_pending().len(), 1);
//! admin.cluster_commit().unwrap();
//! assert_eq!(admin.list_peers().len(), 2);
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::cluster::peer::{Peer, PeerEndpoint, PeerState};
use crate::cluster::pool::ServerPool;
use crate::hashkit::DynToken;
use crate::net::auto_eject::AutoEject;

/// Snapshot of one peer the admin layer reports back over the
/// PBC. The shape is intentionally small and self-describing so
/// the wire format can serialise every field with no further
/// lookups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSnapshot {
    /// Peer index in the pool's peer array. Stable for the
    /// lifetime of the peer.
    pub idx: u32,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// Hostname or numeric IP.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Token list rendered as the same `u32` integers the engine
    /// uses for ring lookups.
    pub tokens: Vec<u32>,
    /// Lifecycle state.
    pub state: PeerState,
    /// True for the local peer (peer index 0 in conforming
    /// configurations).
    pub is_local: bool,
}

/// Description of a peer to add to the cluster.
///
/// This is the inverse of [`PeerSnapshot`]: a snapshot describes
/// a peer that already exists, a `PeerSpec` describes one that
/// the admin caller wants to join.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSpec {
    /// Hostname or numeric IP.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
    /// Token list as `u32`s.
    pub tokens: Vec<u32>,
    /// True when the peer expects an encrypted dnode link.
    pub is_secure: bool,
}

/// Direction of a staged [`ClusterChange`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ClusterChangeKind {
    /// Add a peer specified by [`ClusterChange::peer`].
    Add,
    /// Remove the peer at [`ClusterChange::peer_idx`].
    Remove,
}

/// One pending cluster-membership mutation.
///
/// A staged mutation is a description; it is not applied until
/// [`ClusterAdmin::cluster_commit`] is called.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterChange {
    /// Add or Remove.
    pub kind: ClusterChangeKind,
    /// Peer index for Remove; ignored for Add.
    pub peer_idx: Option<u32>,
    /// Peer description for Add; ignored for Remove.
    pub peer: Option<PeerSpec>,
}

/// Plan returned by [`ClusterAdmin::cluster_join`] and
/// [`ClusterAdmin::cluster_leave`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinPlan {
    /// The change that was staged.
    pub change: ClusterChange,
}

/// Errors produced by the cluster admin layer.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClusterError {
    /// No peer in the pool has the requested index.
    #[error("peer not found: idx={idx}")]
    PeerNotFound {
        /// Index that was not found.
        idx: u32,
    },
    /// The local peer cannot leave the cluster through the admin
    /// surface; the operator should stop the node instead.
    #[error("cannot remove the local peer")]
    CannotRemoveLocal,
    /// The supplied join target already exists in the peer list.
    #[error("peer with endpoint {addr} already exists")]
    PeerAlreadyExists {
        /// `host:port` of the duplicate target.
        addr: String,
    },
    /// Generic precondition failure (an Add change with no
    /// `peer` field, a Remove change with no `peer_idx`, ...).
    #[error("invalid request: {0}")]
    Invalid(String),
}

/// Cluster-membership admin surface.
///
/// The PBC server consults a `&dyn ClusterAdmin` for the
/// `DynRpb*` admin RPCs. The default implementation is
/// [`PoolClusterAdmin`]; a [`NoopClusterAdmin`] is provided for
/// servers that do not expose admin operations.
pub trait ClusterAdmin: Send + Sync + std::fmt::Debug {
    /// Snapshot of every peer the gossip layer has seen.
    fn list_peers(&self) -> Vec<PeerSnapshot>;
    /// Stage a Add change for `target` and return the plan.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::PeerAlreadyExists`] when an
    /// existing peer already advertises the same host:port.
    fn cluster_join(&self, target: SocketAddr) -> Result<JoinPlan, ClusterError>;
    /// Stage a Remove change for `peer_idx` and return the plan.
    ///
    /// # Errors
    ///
    /// Returns [`ClusterError::PeerNotFound`] when no peer has
    /// the requested index, or
    /// [`ClusterError::CannotRemoveLocal`] when the index
    /// points at the local node.
    fn cluster_leave(&self, peer_idx: u32) -> Result<JoinPlan, ClusterError>;
    /// Snapshot of every staged-but-uncommitted change.
    fn cluster_plan_pending(&self) -> Vec<ClusterChange>;
    /// Apply every staged change and clear the staging list.
    ///
    /// # Errors
    ///
    /// Returns the first staging error encountered. Already-
    /// applied changes remain applied.
    fn cluster_commit(&self) -> Result<(), ClusterError>;
}

/// `ClusterAdmin` that always reports an empty cluster and
/// rejects mutations. Used by callers that have not wired the
/// real admin surface.
#[derive(Debug, Default)]
pub struct NoopClusterAdmin;

impl ClusterAdmin for NoopClusterAdmin {
    fn list_peers(&self) -> Vec<PeerSnapshot> {
        Vec::new()
    }
    fn cluster_join(&self, _target: SocketAddr) -> Result<JoinPlan, ClusterError> {
        Err(ClusterError::Invalid(
            "cluster admin RPC not configured on this node".into(),
        ))
    }
    fn cluster_leave(&self, _peer_idx: u32) -> Result<JoinPlan, ClusterError> {
        Err(ClusterError::Invalid(
            "cluster admin RPC not configured on this node".into(),
        ))
    }
    fn cluster_plan_pending(&self) -> Vec<ClusterChange> {
        Vec::new()
    }
    fn cluster_commit(&self) -> Result<(), ClusterError> {
        Ok(())
    }
}

/// `ClusterAdmin` backed by an [`Arc<ServerPool>`].
///
/// Mutations stage into an internal [`Mutex<Vec<ClusterChange>>`];
/// `cluster_commit` applies every staged entry under the pool's
/// existing `RwLock`s and rebuilds the token continuum once on
/// the way out so dispatch sees the new ring.
#[derive(Debug)]
pub struct PoolClusterAdmin {
    pool: Arc<ServerPool>,
    staged: Mutex<Vec<ClusterChange>>,
}

impl PoolClusterAdmin {
    /// Build a fresh admin handle wrapping `pool`.
    #[must_use]
    pub fn new(pool: Arc<ServerPool>) -> Self {
        Self {
            pool,
            staged: Mutex::new(Vec::new()),
        }
    }

    /// Borrow the pool the handle was built over. Useful when a
    /// caller needs to consult the topology directly (e.g. the
    /// dispatcher) without going through the admin RPC.
    #[must_use]
    pub fn pool(&self) -> &Arc<ServerPool> {
        &self.pool
    }
}

impl ClusterAdmin for PoolClusterAdmin {
    fn list_peers(&self) -> Vec<PeerSnapshot> {
        let peers = self.pool.peers().read();
        peers
            .iter()
            .map(|p| PeerSnapshot {
                idx: p.idx(),
                dc: p.dc().to_string(),
                rack: p.rack().to_string(),
                host: p.endpoint().host().to_string(),
                port: p.endpoint().port(),
                tokens: p.tokens().iter().map(DynToken::get_int).collect(),
                state: p.state(),
                is_local: p.is_local(),
            })
            .collect()
    }

    fn cluster_join(&self, target: SocketAddr) -> Result<JoinPlan, ClusterError> {
        let host = target.ip().to_string();
        let port = target.port();
        let peers = self.pool.peers().read();
        if peers
            .iter()
            .any(|p| p.endpoint().host() == host && p.endpoint().port() == port)
        {
            return Err(ClusterError::PeerAlreadyExists {
                addr: target.to_string(),
            });
        }
        let staged = self.staged.lock();
        if staged
            .iter()
            .any(|c| matches!(&c.peer, Some(s) if s.host == host && s.port == port))
        {
            return Err(ClusterError::PeerAlreadyExists {
                addr: target.to_string(),
            });
        }
        // Inherit dc / rack from the local peer (or pool config).
        let (dc, rack) = peers.iter().find(|p| p.is_local()).map_or_else(
            || {
                (
                    self.pool.config().dc.clone(),
                    self.pool.config().rack.clone(),
                )
            },
            |p| (p.dc().to_string(), p.rack().to_string()),
        );
        drop(staged);
        drop(peers);
        let token_val = derive_token(&host, port);
        let spec = PeerSpec {
            host,
            port,
            dc,
            rack,
            tokens: vec![token_val],
            is_secure: false,
        };
        let change = ClusterChange {
            kind: ClusterChangeKind::Add,
            peer_idx: None,
            peer: Some(spec),
        };
        let plan = JoinPlan {
            change: change.clone(),
        };
        self.staged.lock().push(change);
        Ok(plan)
    }

    fn cluster_leave(&self, peer_idx: u32) -> Result<JoinPlan, ClusterError> {
        let peers = self.pool.peers().read();
        let target = peers
            .iter()
            .find(|p| p.idx() == peer_idx)
            .ok_or(ClusterError::PeerNotFound { idx: peer_idx })?;
        if target.is_local() {
            return Err(ClusterError::CannotRemoveLocal);
        }
        drop(peers);
        let change = ClusterChange {
            kind: ClusterChangeKind::Remove,
            peer_idx: Some(peer_idx),
            peer: None,
        };
        let plan = JoinPlan {
            change: change.clone(),
        };
        self.staged.lock().push(change);
        Ok(plan)
    }

    fn cluster_plan_pending(&self) -> Vec<ClusterChange> {
        self.staged.lock().clone()
    }

    fn cluster_commit(&self) -> Result<(), ClusterError> {
        let mut staged = self.staged.lock();
        if staged.is_empty() {
            return Ok(());
        }
        let mut peers = self.pool.peers().write();
        let mut auto_ejects = self.pool.auto_eject().write();
        let local_dc = self.pool.config().dc.clone();
        for change in staged.iter() {
            match change.kind {
                ClusterChangeKind::Add => {
                    let spec = change
                        .peer
                        .as_ref()
                        .ok_or_else(|| ClusterError::Invalid("Add change missing peer".into()))?;
                    if peers.iter().any(|p| {
                        p.endpoint().host() == spec.host && p.endpoint().port() == spec.port
                    }) {
                        return Err(ClusterError::PeerAlreadyExists {
                            addr: format!("{}:{}", spec.host, spec.port),
                        });
                    }
                    let new_idx = u32::try_from(peers.len()).unwrap_or(u32::MAX);
                    let is_same_dc = spec.dc == local_dc;
                    let new_peer = Peer::new(
                        new_idx,
                        PeerEndpoint::tcp(spec.host.clone(), spec.port),
                        spec.rack.clone(),
                        spec.dc.clone(),
                        spec.tokens
                            .iter()
                            .copied()
                            .map(DynToken::from_u32)
                            .collect(),
                        false,
                        is_same_dc,
                        spec.is_secure,
                    );
                    peers.push(new_peer);
                    let template = AutoEject::new(
                        self.pool.config().auto_eject_hosts,
                        self.pool.config().server_failure_limit,
                        Duration::from_millis(self.pool.config().server_retry_timeout_ms),
                    );
                    auto_ejects.push(template);
                }
                ClusterChangeKind::Remove => {
                    let idx = change
                        .peer_idx
                        .ok_or_else(|| ClusterError::Invalid("Remove change missing idx".into()))?;
                    let pos = peers
                        .iter()
                        .position(|p| p.idx() == idx)
                        .ok_or(ClusterError::PeerNotFound { idx })?;
                    if peers[pos].is_local() {
                        return Err(ClusterError::CannotRemoveLocal);
                    }
                    peers.remove(pos);
                    if pos < auto_ejects.len() {
                        auto_ejects.remove(pos);
                    }
                }
            }
        }
        staged.clear();
        drop(peers);
        drop(auto_ejects);
        // The continuum has to be re-derived once after the
        // batch so the dispatcher routes against the new ring.
        self.pool.rebuild_ring();
        Ok(())
    }
}

/// Stable per-(host, port) token used by [`PoolClusterAdmin::cluster_join`]
/// when the caller does not provide an explicit token list. The
/// hash is FNV-1a over the host bytes and port; collisions with
/// existing tokens are not avoided here because the v0 admin
/// surface does not yet drive a rebalance pass.
fn derive_token(host: &str, port: u16) -> u32 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in host.as_bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for byte in port.to_be_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    u32::try_from(hash & 0xffff_ffff).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::peer::PeerEndpoint;
    use crate::cluster::pool::PoolConfig;

    fn small_pool() -> Arc<ServerPool> {
        let cfg = PoolConfig {
            dc: "dc1".into(),
            rack: "r1".into(),
            ..PoolConfig::default()
        };
        let local = Peer::new(
            0,
            PeerEndpoint::tcp("127.0.0.1".into(), 8101),
            "r1".into(),
            "dc1".into(),
            vec![DynToken::from_u32(0)],
            true,
            true,
            false,
        );
        let remote = Peer::new(
            1,
            PeerEndpoint::tcp("127.0.0.1".into(), 8102),
            "r1".into(),
            "dc1".into(),
            vec![DynToken::from_u32(2_147_483_648)],
            false,
            true,
            false,
        );
        Arc::new(ServerPool::new(cfg, vec![local, remote]))
    }

    #[test]
    fn list_peers_reports_every_peer() {
        let admin = PoolClusterAdmin::new(small_pool());
        let snaps = admin.list_peers();
        assert_eq!(snaps.len(), 2);
        let local = snaps.iter().find(|s| s.is_local).expect("local snapshot");
        assert_eq!(local.idx, 0);
        assert_eq!(local.port, 8101);
        let remote = snaps.iter().find(|s| !s.is_local).expect("remote snapshot");
        assert_eq!(remote.idx, 1);
        assert_eq!(remote.tokens, vec![2_147_483_648]);
    }

    #[test]
    fn join_stages_and_commit_appends_peer() {
        let admin = PoolClusterAdmin::new(small_pool());
        let target: SocketAddr = "127.0.0.1:8103".parse().unwrap();
        let plan = admin.cluster_join(target).expect("plan");
        assert_eq!(plan.change.kind, ClusterChangeKind::Add);
        assert_eq!(admin.cluster_plan_pending().len(), 1);
        // Peer list does not include the new peer until commit.
        assert_eq!(admin.list_peers().len(), 2);
        admin.cluster_commit().expect("commit");
        assert_eq!(admin.cluster_plan_pending().len(), 0);
        let snaps = admin.list_peers();
        assert_eq!(snaps.len(), 3);
        assert!(snaps.iter().any(|s| s.port == 8103));
    }

    #[test]
    fn join_rejects_duplicate_endpoint() {
        let admin = PoolClusterAdmin::new(small_pool());
        let target: SocketAddr = "127.0.0.1:8102".parse().unwrap();
        let err = admin.cluster_join(target).expect_err("duplicate");
        assert!(matches!(err, ClusterError::PeerAlreadyExists { .. }));
    }

    #[test]
    fn join_rejects_duplicate_in_staging() {
        let admin = PoolClusterAdmin::new(small_pool());
        let target: SocketAddr = "127.0.0.1:8200".parse().unwrap();
        admin.cluster_join(target).expect("first");
        let err = admin.cluster_join(target).expect_err("staged dup");
        assert!(matches!(err, ClusterError::PeerAlreadyExists { .. }));
    }

    #[test]
    fn leave_stages_and_commit_removes_peer() {
        let admin = PoolClusterAdmin::new(small_pool());
        let plan = admin.cluster_leave(1).expect("plan");
        assert_eq!(plan.change.kind, ClusterChangeKind::Remove);
        assert_eq!(plan.change.peer_idx, Some(1));
        // Pre-commit: pool unchanged.
        assert_eq!(admin.list_peers().len(), 2);
        admin.cluster_commit().expect("commit");
        let snaps = admin.list_peers();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].idx, 0);
    }

    #[test]
    fn leave_rejects_unknown_idx() {
        let admin = PoolClusterAdmin::new(small_pool());
        let err = admin.cluster_leave(99).expect_err("unknown");
        assert!(matches!(err, ClusterError::PeerNotFound { idx: 99 }));
    }

    #[test]
    fn leave_rejects_local_peer() {
        let admin = PoolClusterAdmin::new(small_pool());
        let err = admin.cluster_leave(0).expect_err("local");
        assert!(matches!(err, ClusterError::CannotRemoveLocal));
    }

    #[test]
    fn commit_with_empty_staging_is_ok() {
        let admin = PoolClusterAdmin::new(small_pool());
        admin.cluster_commit().expect("noop commit");
        assert_eq!(admin.list_peers().len(), 2);
    }

    #[test]
    fn commit_applies_mixed_batch_in_order() {
        let admin = PoolClusterAdmin::new(small_pool());
        admin.cluster_leave(1).expect("stage leave");
        let target: SocketAddr = "10.0.0.1:8101".parse().unwrap();
        admin.cluster_join(target).expect("stage join");
        assert_eq!(admin.cluster_plan_pending().len(), 2);
        admin.cluster_commit().expect("commit");
        let snaps = admin.list_peers();
        assert_eq!(snaps.len(), 2);
        // The local peer is preserved; the new peer takes idx 1
        // (the slot vacated by the removed peer is not reused;
        // the new peer's idx is chosen as the post-removal len).
        let new = snaps.iter().find(|s| s.host == "10.0.0.1").expect("added");
        assert_eq!(new.port, 8101);
        // Old remote peer is gone.
        assert!(!snaps
            .iter()
            .any(|s| s.host == "127.0.0.1" && s.port == 8102));
    }

    #[test]
    fn noop_admin_returns_empty_and_errors_on_mutations() {
        let admin = NoopClusterAdmin;
        assert!(admin.list_peers().is_empty());
        assert!(admin.cluster_plan_pending().is_empty());
        admin.cluster_commit().expect("noop commit");
        let target: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(matches!(
            admin.cluster_join(target),
            Err(ClusterError::Invalid(_))
        ));
        assert!(matches!(
            admin.cluster_leave(0),
            Err(ClusterError::Invalid(_))
        ));
    }

    #[test]
    fn derive_token_is_stable_per_endpoint() {
        let a = derive_token("10.0.0.1", 8101);
        let b = derive_token("10.0.0.1", 8101);
        let c = derive_token("10.0.0.2", 8101);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn join_rebuilds_ring() {
        // After commit the per-rack continuum should include the
        // freshly added peer.
        let admin = PoolClusterAdmin::new(small_pool());
        let target: SocketAddr = "10.0.0.5:8101".parse().unwrap();
        admin.cluster_join(target).expect("plan");
        admin.cluster_commit().expect("commit");
        let pool = admin.pool();
        let topology = pool.datacenters().read();
        let dc1 = topology.iter().find(|d| d.name() == "dc1").expect("dc1");
        let r1 = dc1.racks().iter().find(|r| r.name() == "r1").expect("r1");
        // The newly-added peer joins rack r1 (inherited from local)
        // so the rack now holds the local + the previous remote +
        // the freshly added peer = 3 distinct peer indices.
        let entries = r1.continuums();
        let mut idxs: Vec<u32> = entries.iter().map(|e| e.peer_idx).collect();
        idxs.sort_unstable();
        idxs.dedup();
        assert_eq!(idxs.len(), 3);
    }
}
