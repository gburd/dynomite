//! Active preference list (APL) annotations for the vnode walker.
//!
//! Riak's `riak_core_apl:get_apl_ann/3` returns a list of
//! `{Index, Node, Type}` triples where `Type` is `primary` (the
//! canonical owner of the partition) or `fallback` (a node that
//! has stepped in for a downed primary). The annotation is what
//! lets a get / put coordinator distinguish "I have R primary
//! responses" (the happy path) from "I have R responses but two
//! of them came from fallbacks" (which should trigger more
//! aggressive read-repair / handoff).
//!
//! This module ports that idea on top of the existing token ring.
//! It walks a [`ClusterState`] forward from the key token,
//! collects the canonical N successors (the *preflist*), then
//! annotates each slot:
//!
//! * if the canonical owner is alive, the slot is a
//!   [`NodeRole::Primary`];
//! * otherwise the next alive distinct peer further in the walk
//!   takes the slot as a [`NodeRole::Fallback`].
//!
//! When the cluster has fewer alive distinct peers than `n`, the
//! walker returns a partial list rather than blocking; the
//! coordinator decides what to do with an under-provisioned APL.
//!
//! The module is deliberately data-only: it operates on a
//! [`ClusterState`] view that production wiring constructs from
//! the live [`crate::cluster::pool::ServerPool`] and the phi-accrual
//! [`crate::cluster::failure_detector`] state. Tests can build a
//! `ClusterState` directly without spinning up a pool.
//!
//! # Examples
//!
//! ```
//! use dynomite::cluster::apl::{get_apl_ann, ClusterState, NodeRole, RingPoint};
//!
//! // 4-peer ring at tokens 100/200/300/400.
//! let ring = vec![
//!     RingPoint::new(100, 0),
//!     RingPoint::new(200, 1),
//!     RingPoint::new(300, 2),
//!     RingPoint::new(400, 3),
//! ];
//! let cluster = ClusterState::new(ring, [0u32, 1, 2, 3].into_iter().collect());
//! let apl = get_apl_ann(&cluster, 50, 3);
//! assert_eq!(apl.len(), 3);
//! assert!(apl.iter().all(|p| p.role == NodeRole::Primary));
//! ```

use std::collections::HashSet;

use crate::embed::events::PeerId;

/// Identifier for a vnode slot on the ring.
///
/// In this engine the ring is a flat continuum of `(token, peer)`
/// points; a vnode is identified by its index into that
/// continuum. The annotated walker reports the index of the entry
/// from which a peer was selected so the coordinator can correlate
/// fallbacks back to the primary slot they cover.
pub type VnodeId = u32;

/// Whether an annotated peer is the canonical owner of its slot
/// (`Primary`) or a stand-in selected because the canonical owner
/// is currently down (`Fallback`).
///
/// # Examples
///
/// ```
/// use dynomite::cluster::apl::NodeRole;
/// assert_ne!(NodeRole::Primary, NodeRole::Fallback);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeRole {
    /// Canonical ring owner of the slot.
    Primary,
    /// Stand-in chosen because the canonical owner is down.
    Fallback,
}

/// One annotated entry in the active preference list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnotatedPeer {
    /// Peer that will receive the operation.
    pub peer_id: PeerId,
    /// Vnode (continuum index) the peer covers in this slot.
    /// For a `Primary` this is the canonical owner's continuum
    /// position. For a `Fallback` it is the position of the
    /// fallback peer's first ring entry encountered during the
    /// walk (which is always at or beyond the slot the fallback
    /// is covering).
    pub vnode: VnodeId,
    /// Whether this is a canonical owner (`Primary`) or a
    /// stand-in (`Fallback`).
    pub role: NodeRole,
}

/// One token-ring point: a `(token, peer)` mapping.
///
/// The walker uses a `u64` token to keep the ring math
/// transport-agnostic; bridge code converts the engine's
/// [`crate::hashkit::DynToken`] continuum to this shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingPoint {
    /// Token at this ring position.
    pub token: u64,
    /// Peer that owns the token.
    pub peer_id: PeerId,
}

impl RingPoint {
    /// Construct a ring point.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::apl::RingPoint;
    /// let p = RingPoint::new(100, 7);
    /// assert_eq!(p.token, 100);
    /// assert_eq!(p.peer_id, 7);
    /// ```
    #[must_use]
    pub fn new(token: u64, peer_id: PeerId) -> Self {
        Self { token, peer_id }
    }
}

/// Decoupled view of cluster topology + liveness.
///
/// Carries enough state for the APL walker to compute an
/// annotated preference list without consulting the live pool:
///
/// * a sorted-by-token continuum of `(token, peer_id)` ring
///   points (one entry per vnode, may have several entries per
///   peer);
/// * the set of peers currently alive according to the failure
///   detector.
///
/// The ring is held as-passed; the constructor sorts it by token.
#[derive(Clone, Debug)]
pub struct ClusterState {
    ring: Vec<RingPoint>,
    alive: HashSet<PeerId>,
}

impl ClusterState {
    /// Build a [`ClusterState`] from a ring and a liveness set.
    ///
    /// The ring is sorted by token (ties broken by peer id) so
    /// callers can pass any ordering without thinking about it.
    /// Empty rings are accepted; `get_apl_ann` will return an
    /// empty vector for them.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::apl::{ClusterState, RingPoint};
    /// let cs = ClusterState::new(
    ///     vec![RingPoint::new(2, 1), RingPoint::new(1, 0)],
    ///     [0u32, 1].into_iter().collect(),
    /// );
    /// assert_eq!(cs.ring()[0].token, 1);
    /// ```
    #[must_use]
    pub fn new(mut ring: Vec<RingPoint>, alive: HashSet<PeerId>) -> Self {
        ring.sort_by(|a, b| a.token.cmp(&b.token).then(a.peer_id.cmp(&b.peer_id)));
        Self { ring, alive }
    }

    /// Read-only view of the sorted ring.
    #[must_use]
    pub fn ring(&self) -> &[RingPoint] {
        &self.ring
    }

    /// True when `peer_id` is in the alive set.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::cluster::apl::{ClusterState, RingPoint};
    /// let cs = ClusterState::new(
    ///     vec![RingPoint::new(1, 0)],
    ///     [0u32].into_iter().collect(),
    /// );
    /// assert!(cs.is_alive(0));
    /// assert!(!cs.is_alive(99));
    /// ```
    #[must_use]
    pub fn is_alive(&self, peer_id: PeerId) -> bool {
        self.alive.contains(&peer_id)
    }
}

/// Walk N successors of `key_token` on the ring, deduplicating
/// by peer id.
///
/// Returns up to `n` `(vnode_index, peer_id)` pairs in walk order.
/// This is the canonical preflist: every entry is the first ring
/// occurrence of a distinct peer encountered while walking
/// forward from `key_token`. Liveness is *not* consulted; this is
/// the input the APL annotator works on.
///
/// Returns an empty vector when the ring is empty or `n == 0`.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::apl::{walk_n_successors, ClusterState, RingPoint};
/// let cs = ClusterState::new(
///     vec![RingPoint::new(10, 0), RingPoint::new(20, 1), RingPoint::new(30, 2)],
///     [0u32, 1, 2].into_iter().collect(),
/// );
/// let pre = walk_n_successors(&cs, 15, 2);
/// assert_eq!(pre.iter().map(|p| p.1).collect::<Vec<_>>(), vec![1, 2]);
/// ```
#[must_use]
pub fn walk_n_successors(
    cluster: &ClusterState,
    key_token: u64,
    n: usize,
) -> Vec<(VnodeId, PeerId)> {
    let ring = cluster.ring();
    if ring.is_empty() || n == 0 {
        return Vec::new();
    }
    let start = primary_index(ring, key_token);
    let mut out: Vec<(VnodeId, PeerId)> = Vec::with_capacity(n);
    let len = ring.len();
    for step in 0..len {
        if out.len() >= n {
            break;
        }
        let idx = (start + step) % len;
        let pt = &ring[idx];
        if out.iter().any(|(_, pid)| *pid == pt.peer_id) {
            continue;
        }
        let vnode = u32::try_from(idx).unwrap_or(u32::MAX);
        out.push((vnode, pt.peer_id));
    }
    out
}

/// Compute the annotated active preference list for `key_token`.
///
/// Returns up to `n` [`AnnotatedPeer`] entries in slot order:
///
/// * For each canonical primary in the first-`n` walk (see
///   [`walk_n_successors`]): if the peer is alive, the slot is
///   filled by that peer with [`NodeRole::Primary`].
/// * If the canonical primary is down, the walker continues
///   beyond the canonical slice and picks the next alive peer
///   that is not already in the result; the slot is filled with
///   [`NodeRole::Fallback`].
/// * If the walker runs out of alive distinct peers, the result
///   is shorter than `n`; the caller decides whether that is
///   acceptable for the requested consistency level.
///
/// # Properties
///
/// * `primaries(&apl).len() + fallbacks(&apl).len() == apl.len()`.
/// * `primaries(&apl)` is a subset (by peer id) of the canonical
///   `walk_n_successors(cluster, key_token, n)` preflist.
/// * `apl.len() <= n` and `apl.len() <= alive_distinct_peers`.
/// * Every peer id in `apl` is unique.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::apl::{get_apl_ann, ClusterState, NodeRole, RingPoint};
/// let cs = ClusterState::new(
///     vec![
///         RingPoint::new(100, 0),
///         RingPoint::new(200, 1),
///         RingPoint::new(300, 2),
///         RingPoint::new(400, 3),
///     ],
///     // Peer 1 (the canonical second) is down.
///     [0u32, 2, 3].into_iter().collect(),
/// );
/// let apl = get_apl_ann(&cs, 50, 3);
/// assert_eq!(apl.len(), 3);
/// assert_eq!(apl[0].peer_id, 0);
/// assert_eq!(apl[0].role, NodeRole::Primary);
/// assert_eq!(apl[1].peer_id, 3);
/// assert_eq!(apl[1].role, NodeRole::Fallback);
/// assert_eq!(apl[2].peer_id, 2);
/// assert_eq!(apl[2].role, NodeRole::Primary);
/// ```
#[must_use]
pub fn get_apl_ann(cluster: &ClusterState, key_token: u64, n: usize) -> Vec<AnnotatedPeer> {
    let ring = cluster.ring();
    if ring.is_empty() || n == 0 {
        return Vec::new();
    }

    // Build the full distinct-peer walk starting at key_token.
    let start = primary_index(ring, key_token);
    let len = ring.len();
    let mut walk: Vec<(VnodeId, PeerId)> = Vec::new();
    for step in 0..len {
        let idx = (start + step) % len;
        let pid = ring[idx].peer_id;
        if walk.iter().any(|(_, p)| *p == pid) {
            continue;
        }
        let vnode = u32::try_from(idx).unwrap_or(u32::MAX);
        walk.push((vnode, pid));
    }

    let canonical_len = walk.len().min(n);
    let mut result: Vec<AnnotatedPeer> = Vec::with_capacity(canonical_len);
    let mut next_fallback = canonical_len;

    for slot in 0..canonical_len {
        let (vnode, peer_id) = walk[slot];
        if cluster.is_alive(peer_id) {
            result.push(AnnotatedPeer {
                peer_id,
                vnode,
                role: NodeRole::Primary,
            });
            continue;
        }
        // Canonical peer is down: pull the next alive peer from
        // the tail of the walk. Each fallback is consumed at most
        // once; if the tail runs dry the slot is dropped, but we
        // keep iterating so a later canonical that *is* alive
        // can still take its slot as Primary.
        while next_fallback < walk.len() {
            let (fb_vnode, fb_peer) = walk[next_fallback];
            next_fallback += 1;
            if !cluster.is_alive(fb_peer) {
                continue;
            }
            // The walk is already deduplicated, so `fb_peer` is
            // not in `result` as long as we have not previously
            // promoted the same canonical id. The dedup invariant
            // makes the explicit not-in-result check redundant,
            // but we keep it as a defensive guard for future
            // refactors.
            if result.iter().any(|p| p.peer_id == fb_peer) {
                continue;
            }
            result.push(AnnotatedPeer {
                peer_id: fb_peer,
                vnode: fb_vnode,
                role: NodeRole::Fallback,
            });
            break;
        }
    }

    result
}

/// Filter `annotated` to just the primary slots.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::apl::{primaries, AnnotatedPeer, NodeRole};
/// let apl = vec![
///     AnnotatedPeer { peer_id: 0, vnode: 0, role: NodeRole::Primary },
///     AnnotatedPeer { peer_id: 1, vnode: 1, role: NodeRole::Fallback },
/// ];
/// assert_eq!(primaries(&apl).len(), 1);
/// ```
#[must_use]
pub fn primaries(annotated: &[AnnotatedPeer]) -> Vec<&AnnotatedPeer> {
    annotated
        .iter()
        .filter(|p| p.role == NodeRole::Primary)
        .collect()
}

/// Filter `annotated` to just the fallback slots.
///
/// # Examples
///
/// ```
/// use dynomite::cluster::apl::{fallbacks, AnnotatedPeer, NodeRole};
/// let apl = vec![
///     AnnotatedPeer { peer_id: 0, vnode: 0, role: NodeRole::Primary },
///     AnnotatedPeer { peer_id: 1, vnode: 1, role: NodeRole::Fallback },
/// ];
/// assert_eq!(fallbacks(&apl).len(), 1);
/// ```
#[must_use]
pub fn fallbacks(annotated: &[AnnotatedPeer]) -> Vec<&AnnotatedPeer> {
    annotated
        .iter()
        .filter(|p| p.role == NodeRole::Fallback)
        .collect()
}

/// Find the index into `ring` that owns `key_token`: the smallest
/// entry whose token is greater than or equal to `key_token`,
/// wrapping to entry 0 when the key is greater than every token.
///
/// Mirrors the upper-bound wraparound semantics of
/// [`crate::cluster::vnode::dispatch`] but operates on a `u64`
/// token slice for transport-agnostic ring math.
fn primary_index(ring: &[RingPoint], key_token: u64) -> usize {
    debug_assert!(!ring.is_empty(), "primary_index requires a non-empty ring");
    match ring.binary_search_by_key(&key_token, |p| p.token) {
        Ok(i) => i,
        Err(i) => {
            if i >= ring.len() {
                0
            } else {
                i
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(pairs: &[(u64, PeerId)]) -> Vec<RingPoint> {
        pairs.iter().map(|&(t, p)| RingPoint::new(t, p)).collect()
    }

    #[test]
    fn empty_ring_returns_empty_apl() {
        let cs = ClusterState::new(Vec::new(), HashSet::new());
        assert!(get_apl_ann(&cs, 100, 3).is_empty());
        assert!(walk_n_successors(&cs, 100, 3).is_empty());
    }

    #[test]
    fn n_zero_returns_empty_apl() {
        let cs = ClusterState::new(ring(&[(10, 0)]), [0u32].into_iter().collect());
        assert!(get_apl_ann(&cs, 10, 0).is_empty());
        assert!(walk_n_successors(&cs, 10, 0).is_empty());
    }

    #[test]
    fn walk_wraps_on_overflow() {
        let cs = ClusterState::new(
            ring(&[(10, 0), (20, 1), (30, 2)]),
            [0u32, 1, 2].into_iter().collect(),
        );
        // Key past the last token wraps to peer 0.
        let pre = walk_n_successors(&cs, 50, 3);
        assert_eq!(pre.iter().map(|p| p.1).collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    #[test]
    fn walk_dedups_peers_with_multiple_vnodes() {
        // Peer 0 has two ring entries; peer 1 has one. With n=2
        // we should still get two distinct peers.
        let cs = ClusterState::new(
            ring(&[(10, 0), (20, 0), (30, 1)]),
            [0u32, 1].into_iter().collect(),
        );
        let pre = walk_n_successors(&cs, 0, 2);
        assert_eq!(pre.iter().map(|p| p.1).collect::<Vec<_>>(), vec![0, 1]);
    }

    #[test]
    fn primaries_subset_of_canonical_walk() {
        let cs = ClusterState::new(
            ring(&[(10, 0), (20, 1), (30, 2), (40, 3)]),
            [0u32, 1, 2, 3].into_iter().collect(),
        );
        let canonical: Vec<PeerId> = walk_n_successors(&cs, 0, 3)
            .into_iter()
            .map(|p| p.1)
            .collect();
        let apl = get_apl_ann(&cs, 0, 3);
        for entry in primaries(&apl) {
            assert!(canonical.contains(&entry.peer_id));
        }
    }
}
