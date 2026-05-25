//! Walk-N-successors replication planning for Riak buckets.
//!
//! The default Dynomite replication strategy
//! ([`ReplicationStrategy::Topology`]) fans a write out across
//! datacenters and racks per the configured consistency level.
//! Riak instead replicates a key to the primary partition plus
//! the next `n_val - 1` peers reached by walking forward on the
//! token ring, skipping a peer that has already been added so a
//! peer with multiple ring slots cannot count more than once.
//!
//! This module provides the [`plan_replicas`] entry point and
//! the supporting [`RingView`] accessor. The dispatcher consults
//! a per-bucket-property
//! [`crate::proto::pb::RpbBucketProps::replication_strategy`]
//! field to decide which path to take. When the field is unset
//! (or set to [`REPLICATION_STRATEGY_TOPOLOGY`]), the existing
//! topology code stays in charge; when it is
//! [`REPLICATION_STRATEGY_SUCCESSORS`], this module returns the
//! replica list.
//!
//! [`REPLICATION_STRATEGY_TOPOLOGY`]: crate::proto::pb::REPLICATION_STRATEGY_TOPOLOGY
//! [`REPLICATION_STRATEGY_SUCCESSORS`]: crate::proto::pb::REPLICATION_STRATEGY_SUCCESSORS
//!
//! # Edge cases
//!
//! * Fewer peers than `n_val` -- the plan returns whatever peers
//!   are available; a `tracing::warn!` is emitted at config
//!   validation time so operators see the under-provision early.
//! * Down peers -- NOT skipped during planning. They are returned
//!   as targets so the runtime
//!   [`dynomite::cluster::peer::PeerState`]-aware filter in the
//!   dispatcher decides whether to send. This matches the
//!   topology mode's behaviour.
//!
//! # Examples
//!
//! ```
//! use dyn_riak::replication::{plan_replicas, ReplicationPlan, ReplicationStrategy, RingPoint, RingView};
//! use dynomite::msg::ConsistencyLevel;
//!
//! // 5 peers at evenly spaced tokens.
//! let span = u64::from(u32::MAX);
//! let points = (0..5u32)
//!     .map(|i| RingPoint::new(u64::from(i) * span / 5, i, "dc1", "r1"))
//!     .collect();
//! let ring = RingView::new(points);
//!
//! // Key hashes to peer 1's slot (next slot above token 0).
//! let plan = plan_replicas(&ring, 1, 3, ReplicationStrategy::Successors, ConsistencyLevel::DcOne);
//! match plan {
//!     ReplicationPlan::Successors { successors, primary, .. } => {
//!         let mut idxs = vec![primary.peer_idx];
//!         idxs.extend(successors.iter().map(|r| r.peer_idx));
//!         assert_eq!(idxs, vec![1, 2, 3]);
//!     }
//!     other @ ReplicationPlan::Topology(_) => panic!("expected successors plan, got {other:?}"),
//! }
//! ```

use dynomite::cluster::ReplicaTarget;
use dynomite::msg::ConsistencyLevel;

use crate::proto::pb::{REPLICATION_STRATEGY_SUCCESSORS, REPLICATION_STRATEGY_TOPOLOGY};

/// Strategy used to choose the replica target list for a key.
///
/// Riak-mode pools default to [`Self::Successors`]; non-Riak
/// pools default to [`Self::Topology`] and never expose this
/// knob. The wire encoding lives at
/// [`crate::proto::pb::RpbBucketProps::replication_strategy`].
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum ReplicationStrategy {
    /// Dynomite's classic per-DC, per-rack quorum fan-out. The
    /// dispatcher uses the existing
    /// [`dynomite::cluster::dispatch::ClusterDispatcher::plan`]
    /// pipeline.
    #[default]
    Topology,
    /// Riak-style walk-N-successors. The primary peer plus the
    /// next `n_val - 1` peers reached by walking forward on the
    /// ring, deduplicated.
    Successors,
}

/// Errors produced when decoding a wire `replication_strategy`
/// selector.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReplicationStrategyError {
    /// The wire value was outside the documented enum range.
    #[error("replication_strategy: unknown selector {0}")]
    Unknown(u32),
}

impl ReplicationStrategy {
    /// Convert a wire selector to the in-memory enum. `None`
    /// means the field is unset on the wire and the caller's
    /// default applies (mode-aware: Riak buckets default to
    /// Successors).
    ///
    /// # Errors
    ///
    /// Returns [`ReplicationStrategyError::Unknown`] when the
    /// wire value is outside the documented enum range.
    pub fn from_wire(value: u32) -> Result<Self, ReplicationStrategyError> {
        match value {
            REPLICATION_STRATEGY_TOPOLOGY => Ok(Self::Topology),
            REPLICATION_STRATEGY_SUCCESSORS => Ok(Self::Successors),
            other => Err(ReplicationStrategyError::Unknown(other)),
        }
    }

    /// Convert the in-memory enum to the wire selector.
    #[must_use]
    pub fn to_wire(self) -> u32 {
        match self {
            Self::Topology => REPLICATION_STRATEGY_TOPOLOGY,
            Self::Successors => REPLICATION_STRATEGY_SUCCESSORS,
        }
    }
}

/// One ring entry: a `(token, peer_idx)` mapping plus the
/// peer's `(dc, rack)` labels so the planner can build a
/// [`ReplicaTarget`] without a second pool look-up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RingPoint {
    /// Token at this ring position.
    pub token: u64,
    /// Index into the pool's peer array.
    pub peer_idx: u32,
    /// Datacenter name.
    pub dc: String,
    /// Rack name.
    pub rack: String,
}

impl RingPoint {
    /// Build a ring point. Convenience constructor for tests and
    /// integrations that have ASCII-friendly DC / rack names.
    pub fn new(token: u64, peer_idx: u32, dc: impl Into<String>, rack: impl Into<String>) -> Self {
        Self {
            token,
            peer_idx,
            dc: dc.into(),
            rack: rack.into(),
        }
    }
}

/// Read-only view onto the cluster ring used by
/// [`plan_replicas`]. Holds the ordered list of `(token,
/// peer_idx)` pairs across all datacenters and racks. The
/// dispatcher builds one of these from the live
/// [`dynomite::cluster::ServerPool`] when it needs to invoke the
/// successors strategy; tests construct one directly from a
/// fixture vector.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RingView {
    points: Vec<RingPoint>,
}

impl RingView {
    /// Wrap a set of [`RingPoint`]s in a ring view, sorted by
    /// token ascending so the walk produces a deterministic
    /// result.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyn_riak::replication::{RingPoint, RingView};
    /// let pts = vec![
    ///     RingPoint::new(20, 1, "dc1", "r1"),
    ///     RingPoint::new(10, 0, "dc1", "r1"),
    /// ];
    /// let ring = RingView::new(pts);
    /// assert_eq!(ring.points().len(), 2);
    /// assert_eq!(ring.points()[0].peer_idx, 0);
    /// ```
    #[must_use]
    pub fn new(mut points: Vec<RingPoint>) -> Self {
        points.sort_by_key(|p| p.token);
        Self { points }
    }

    /// Borrow the ordered ring points.
    #[must_use]
    pub fn points(&self) -> &[RingPoint] {
        &self.points
    }

    /// Count of distinct peer indices in the ring.
    #[must_use]
    pub fn peer_count(&self) -> usize {
        let mut idxs: Vec<u32> = self.points.iter().map(|p| p.peer_idx).collect();
        idxs.sort_unstable();
        idxs.dedup();
        idxs.len()
    }

    /// Find the index into [`Self::points`] that owns
    /// `key_hash`: the smallest entry whose token is greater
    /// than or equal to `key_hash`, wrapping to entry 0 when
    /// the hash is greater than every token.
    ///
    /// Returns `None` when the ring is empty.
    #[must_use]
    pub fn primary_index(&self, key_hash: u64) -> Option<usize> {
        if self.points.is_empty() {
            return None;
        }
        match self.points.binary_search_by_key(&key_hash, |p| p.token) {
            Ok(i) => Some(i),
            Err(i) => {
                if i >= self.points.len() {
                    Some(0)
                } else {
                    Some(i)
                }
            }
        }
    }
}

/// Plan produced by [`plan_replicas`].
///
/// `Topology` carries the targets the existing topology pipeline
/// produced; the variant exists so a caller can return a uniform
/// type from both branches. `Successors` carries the primary
/// plus the walk-forward successors; `n` echoes the requested
/// `n_val` so callers can spot-check under-provisioned rings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicationPlan {
    /// Topology fan-out targets. The dispatcher's existing
    /// pipeline is the source of truth; this variant is the
    /// pass-through container [`plan_replicas`] returns when
    /// the strategy is [`ReplicationStrategy::Topology`].
    Topology(Vec<ReplicaTarget>),
    /// Walk-N-successors replica list.
    Successors {
        /// Primary peer for the key.
        primary: ReplicaTarget,
        /// Requested replication factor (`n_val`). May be larger
        /// than `successors.len() + 1` when the ring has fewer
        /// distinct peers than `n_val`.
        n: u8,
        /// `n - 1` (or fewer) successor peers, in walk order.
        /// Excludes the primary; the primary is carried in
        /// `Self::Successors::primary`.
        successors: Vec<ReplicaTarget>,
    },
}

impl ReplicationPlan {
    /// Flatten the plan into a single replica list (primary
    /// first, then successors, in walk order). Convenience for
    /// callers that hand the targets straight to the per-peer
    /// outbound channels.
    #[must_use]
    pub fn into_replica_list(self) -> Vec<ReplicaTarget> {
        match self {
            Self::Topology(targets) => targets,
            Self::Successors {
                primary,
                mut successors,
                ..
            } => {
                let mut out = Vec::with_capacity(1 + successors.len());
                out.push(primary);
                out.append(&mut successors);
                out
            }
        }
    }
}

/// Compute the replica plan for a key according to `strategy`.
///
/// `consistency` is propagated by the dispatcher into the
/// returned plan; this function does not consult it (the
/// successors walk is consistency-level agnostic), but the
/// signature carries it so a future strategy that does care has
/// the value at hand.
///
/// # Behaviour
///
/// * [`ReplicationStrategy::Topology`] -- returns
///   `ReplicationPlan::Topology(Vec::new())`. The caller is
///   expected to fall back to the existing topology pipeline
///   for the actual targets; the empty vector is the sentinel.
/// * [`ReplicationStrategy::Successors`] -- walks the ring
///   forward from the primary slot, deduplicating peer indices,
///   and returns up to `n_val` distinct peers. When the ring
///   has fewer peers than `n_val`, the result has
///   `successors.len() + 1 < n_val`; the caller is expected to
///   surface the under-provision via `tracing::warn!` at config
///   validation time.
///
/// Returns `ReplicationPlan::Successors` with an empty
/// `successors` vector when the ring has exactly one distinct
/// peer; `n_val == 0` is treated identically to `n_val == 1`
/// for safety so the dispatcher always has a primary.
///
/// # Examples
///
/// See the module-level example.
#[must_use]
pub fn plan_replicas(
    distribution: &RingView,
    key_hash: u64,
    n_val: u8,
    strategy: ReplicationStrategy,
    consistency: ConsistencyLevel,
) -> ReplicationPlan {
    let _ = consistency;
    if matches!(strategy, ReplicationStrategy::Topology) {
        return ReplicationPlan::Topology(Vec::new());
    }
    plan_successors(distribution, key_hash, n_val)
}

fn plan_successors(distribution: &RingView, key_hash: u64, n_val: u8) -> ReplicationPlan {
    let target_count = (n_val as usize).max(1);
    let points = distribution.points();
    let Some(start) = distribution.primary_index(key_hash) else {
        // Empty ring: synthesise a placeholder primary so the
        // dispatcher's downstream code never has to handle the
        // "no primary" branch under successors mode. The local
        // node's `is_routable()` filter will subsequently reject
        // it.
        return ReplicationPlan::Successors {
            primary: ReplicaTarget {
                peer_idx: 0,
                dc: String::new(),
                rack: String::new(),
                is_local: false,
            },
            n: n_val,
            successors: Vec::new(),
        };
    };
    let mut chosen: Vec<ReplicaTarget> = Vec::with_capacity(target_count);
    let len = points.len();
    for step in 0..len {
        if chosen.len() >= target_count {
            break;
        }
        let idx = (start + step) % len;
        let pt = &points[idx];
        if chosen.iter().any(|t| t.peer_idx == pt.peer_idx) {
            continue;
        }
        chosen.push(ReplicaTarget {
            peer_idx: pt.peer_idx,
            dc: pt.dc.clone(),
            rack: pt.rack.clone(),
            is_local: false,
        });
    }
    let mut iter = chosen.into_iter();
    let primary = iter
        .next()
        .expect("primary_index returned Some so len >= 1");
    let successors: Vec<ReplicaTarget> = iter.collect();
    ReplicationPlan::Successors {
        primary,
        n: n_val,
        successors,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn five_peer_ring() -> RingView {
        // 5 peers at tokens 0/0.4G/0.8G/1.2G/1.6G of u32::MAX
        // (rounded to integer steps).
        let span = u64::from(u32::MAX);
        let mut pts: Vec<RingPoint> = Vec::with_capacity(5);
        for i in 0..5u32 {
            let token = u64::from(i) * span / 5;
            pts.push(RingPoint::new(token, i, "dc1", "r1"));
        }
        RingView::new(pts)
    }

    fn primary_idx(plan: &ReplicationPlan) -> u32 {
        match plan {
            ReplicationPlan::Successors { primary, .. } => primary.peer_idx,
            other @ ReplicationPlan::Topology(_) => {
                panic!("expected successors plan, got {other:?}")
            }
        }
    }

    fn successor_idxs(plan: &ReplicationPlan) -> Vec<u32> {
        match plan {
            ReplicationPlan::Successors { successors, .. } => {
                successors.iter().map(|t| t.peer_idx).collect()
            }
            other @ ReplicationPlan::Topology(_) => {
                panic!("expected successors plan, got {other:?}")
            }
        }
    }

    #[test]
    fn key_hashing_into_peer1_slot_returns_1_2_3() {
        let ring = five_peer_ring();
        // Hash 1 lands strictly above peer 0's token (0); the
        // owning slot is peer 1.
        let key_hash = 1;
        let plan = plan_replicas(
            &ring,
            key_hash,
            3,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        assert_eq!(primary_idx(&plan), 1);
        assert_eq!(successor_idxs(&plan), vec![2, 3]);
    }

    #[test]
    fn key_hashing_into_peer0_slot_returns_0_1_2() {
        let ring = five_peer_ring();
        // Token 0 itself is owned by peer 0 (binary search hits
        // exact match). This exercises the prompt's "hashes into
        // peer-0's slot; n_val=3 returns peers 0, 1, 2".
        let key_hash = 0;
        let plan = plan_replicas(
            &ring,
            key_hash,
            3,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        assert_eq!(primary_idx(&plan), 0);
        assert_eq!(successor_idxs(&plan), vec![1, 2]);
    }

    #[test]
    fn key_hashing_into_peer3_slot_wraps_around() {
        let ring = five_peer_ring();
        let span = u64::from(u32::MAX);
        // Choose a hash strictly above peer 2's token but at or
        // below peer 3's token. The owning slot is peer 3 and
        // the walk goes peer 4, then wraps to peer 0.
        let key_hash = 2 * span / 5 + 1;
        let plan = plan_replicas(
            &ring,
            key_hash,
            3,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        assert_eq!(primary_idx(&plan), 3);
        assert_eq!(successor_idxs(&plan), vec![4, 0]);
    }

    #[test]
    fn n_val_two_returns_two_peers() {
        let ring = five_peer_ring();
        let plan = plan_replicas(
            &ring,
            1,
            2,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        let flat = plan.into_replica_list();
        assert_eq!(flat.len(), 2);
        assert_eq!(flat[0].peer_idx, 1);
        assert_eq!(flat[1].peer_idx, 2);
    }

    #[test]
    fn n_val_larger_than_ring_caps_at_ring_size() {
        let ring = five_peer_ring();
        let plan = plan_replicas(
            &ring,
            1,
            10,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        let flat = plan.into_replica_list();
        assert_eq!(flat.len(), 5);
        let idxs: Vec<u32> = flat.iter().map(|t| t.peer_idx).collect();
        assert_eq!(idxs, vec![1, 2, 3, 4, 0]);
    }

    #[test]
    fn topology_strategy_returns_empty_passthrough() {
        let ring = five_peer_ring();
        let plan = plan_replicas(
            &ring,
            1,
            3,
            ReplicationStrategy::Topology,
            ConsistencyLevel::DcOne,
        );
        assert!(matches!(plan, ReplicationPlan::Topology(ref v) if v.is_empty()));
    }

    #[test]
    fn duplicate_peer_slots_are_deduplicated() {
        // Peer 0 has two ring points so a 3-replica walk skips
        // its second point in favour of the next distinct peer.
        let pts = vec![
            RingPoint::new(0, 0, "dc1", "r1"),
            RingPoint::new(100, 0, "dc1", "r1"),
            RingPoint::new(200, 1, "dc1", "r1"),
            RingPoint::new(300, 2, "dc1", "r1"),
        ];
        let ring = RingView::new(pts);
        let plan = plan_replicas(
            &ring,
            1,
            3,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        let flat = plan.into_replica_list();
        let idxs: Vec<u32> = flat.iter().map(|t| t.peer_idx).collect();
        assert_eq!(idxs, vec![0, 1, 2]);
    }

    #[test]
    fn empty_ring_yields_synthetic_primary() {
        let ring = RingView::new(Vec::new());
        let plan = plan_replicas(
            &ring,
            42,
            3,
            ReplicationStrategy::Successors,
            ConsistencyLevel::DcOne,
        );
        match plan {
            ReplicationPlan::Successors {
                primary,
                successors,
                ..
            } => {
                assert!(successors.is_empty());
                assert_eq!(primary.dc, "");
            }
            other @ ReplicationPlan::Topology(_) => {
                panic!("expected successors plan, got {other:?}")
            }
        }
    }

    #[test]
    fn from_wire_round_trips() {
        for s in [
            ReplicationStrategy::Topology,
            ReplicationStrategy::Successors,
        ] {
            assert_eq!(ReplicationStrategy::from_wire(s.to_wire()).unwrap(), s);
        }
        assert_eq!(
            ReplicationStrategy::from_wire(7),
            Err(ReplicationStrategyError::Unknown(7))
        );
    }

    #[test]
    fn primary_index_wraps_when_hash_exceeds_largest_token() {
        let ring = five_peer_ring();
        let huge = u64::from(u32::MAX) + 1_000_000;
        assert_eq!(ring.primary_index(huge), Some(0));
    }

    #[test]
    fn ring_view_counts_distinct_peers() {
        let pts = vec![
            RingPoint::new(0, 0, "dc1", "r1"),
            RingPoint::new(10, 0, "dc1", "r1"),
            RingPoint::new(20, 1, "dc1", "r1"),
        ];
        let r = RingView::new(pts);
        assert_eq!(r.peer_count(), 2);
    }
}
