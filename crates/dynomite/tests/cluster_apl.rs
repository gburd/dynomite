//! Active preference list (APL) annotation tests.
//!
//! See [`dynomite::cluster::apl`] for the algorithm.

use std::collections::HashSet;

use dynomite::cluster::apl::{
    fallbacks, get_apl_ann, primaries, walk_n_successors, AnnotatedPeer, ClusterState, NodeRole,
    RingPoint,
};
use dynomite::embed::events::PeerId;
use hegel::generators as gs;
use hegel::TestCase;

fn ring(pairs: &[(u64, PeerId)]) -> Vec<RingPoint> {
    pairs.iter().map(|&(t, p)| RingPoint::new(t, p)).collect()
}

fn alive_set(peers: &[PeerId]) -> HashSet<PeerId> {
    peers.iter().copied().collect()
}

fn peer_ids(apl: &[AnnotatedPeer]) -> Vec<PeerId> {
    apl.iter().map(|p| p.peer_id).collect()
}

#[test]
fn all_primaries_alive_returns_no_fallbacks() {
    let cs = ClusterState::new(
        ring(&[(100, 0), (200, 1), (300, 2), (400, 3), (500, 4)]),
        alive_set(&[0, 1, 2, 3, 4]),
    );
    // key=50 < 100 -> primary slot is peer 0.
    let apl = get_apl_ann(&cs, 50, 3);
    assert_eq!(apl.len(), 3);
    assert!(apl.iter().all(|p| p.role == NodeRole::Primary));
    assert_eq!(peer_ids(&apl), vec![0, 1, 2]);
    assert_eq!(primaries(&apl).len(), 3);
    assert!(fallbacks(&apl).is_empty());
}

#[test]
fn one_primary_down_promotes_one_fallback() {
    let cs = ClusterState::new(
        ring(&[(100, 0), (200, 1), (300, 2), (400, 3), (500, 4)]),
        // Peer 1 is down.
        alive_set(&[0, 2, 3, 4]),
    );
    let apl = get_apl_ann(&cs, 50, 3);
    assert_eq!(apl.len(), 3);
    // Slot 0: canonical peer 0, alive -> Primary.
    assert_eq!(apl[0].peer_id, 0);
    assert_eq!(apl[0].role, NodeRole::Primary);
    // Slot 1: canonical peer 1 down -> next alive in walk is peer 3.
    assert_eq!(apl[1].peer_id, 3);
    assert_eq!(apl[1].role, NodeRole::Fallback);
    // Slot 2: canonical peer 2, alive -> Primary.
    assert_eq!(apl[2].peer_id, 2);
    assert_eq!(apl[2].role, NodeRole::Primary);
    assert_eq!(primaries(&apl).len(), 2);
    assert_eq!(fallbacks(&apl).len(), 1);
}

#[test]
fn all_primaries_down_returns_n_fallbacks_if_available() {
    let cs = ClusterState::new(
        ring(&[(100, 0), (200, 1), (300, 2), (400, 3), (500, 4), (600, 5)]),
        // Canonical primaries for key=50 are peers 0/1/2; all
        // down. Fallbacks should be the next three alive peers
        // in walk order: 3, 4, 5.
        alive_set(&[3, 4, 5]),
    );
    let apl = get_apl_ann(&cs, 50, 3);
    assert_eq!(apl.len(), 3);
    assert!(apl.iter().all(|p| p.role == NodeRole::Fallback));
    assert_eq!(peer_ids(&apl), vec![3, 4, 5]);
    assert!(primaries(&apl).is_empty());
    assert_eq!(fallbacks(&apl).len(), 3);
}

#[test]
fn cluster_smaller_than_n_returns_what_exists() {
    // Two-peer ring asked for n=5: cap at 2.
    let cs = ClusterState::new(ring(&[(100, 0), (200, 1)]), alive_set(&[0, 1]));
    let apl = get_apl_ann(&cs, 50, 5);
    assert_eq!(apl.len(), 2);
    assert!(apl.iter().all(|p| p.role == NodeRole::Primary));

    // Three-peer ring with only one alive, n=3: cap at 1.
    let cs2 = ClusterState::new(ring(&[(100, 0), (200, 1), (300, 2)]), alive_set(&[2]));
    let apl2 = get_apl_ann(&cs2, 50, 3);
    assert_eq!(apl2.len(), 1);
    // Slots 0 and 1 are dropped (canonical peers 0/1 down with
    // no fallback available). Slot 2 keeps its canonical owner.
    assert_eq!(apl2[0].peer_id, 2);
    assert_eq!(apl2[0].role, NodeRole::Primary);
}

#[test]
fn apl_does_not_double_count_a_node_as_both_primary_and_fallback() {
    // Peer 0 has two ring entries (two vnodes); a naive walker
    // could pick it as Primary in slot 0 and then as Fallback
    // for the down peer 1 because peer 0 reappears in the walk.
    // The dedup pass must prevent that.
    let cs = ClusterState::new(
        ring(&[(100, 0), (200, 1), (300, 0), (400, 2), (500, 3)]),
        // Peer 1 is down.
        alive_set(&[0, 2, 3]),
    );
    let apl = get_apl_ann(&cs, 50, 3);
    let ids = peer_ids(&apl);
    let mut unique: Vec<PeerId> = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), ids.len(), "duplicate peer in apl: {ids:?}");
    // Specifically: peer 0 must appear exactly once.
    assert_eq!(ids.iter().filter(|&&p| p == 0).count(), 1);
}

#[test]
fn apl_skips_self_when_self_is_down() {
    // Local node is peer 0 ("self"). Self is down. The walker
    // should not return self in any slot, primary or fallback.
    const SELF: PeerId = 0;
    let cs = ClusterState::new(
        ring(&[(100, SELF), (200, 1), (300, 2), (400, 3)]),
        // Self is down.
        alive_set(&[1, 2, 3]),
    );
    let apl = get_apl_ann(&cs, 50, 3);
    assert_eq!(apl.len(), 3);
    assert!(
        apl.iter().all(|p| p.peer_id != SELF),
        "self ({SELF}) appeared in apl: {apl:?}"
    );
    // Slot 0 canonical was self; substituted by the next alive
    // peer past the canonical slice. Canonical for n=3 is
    // [self, 1, 2]; next-alive past that is peer 3.
    assert_eq!(apl[0].peer_id, 3);
    assert_eq!(apl[0].role, NodeRole::Fallback);
    assert_eq!(apl[1].peer_id, 1);
    assert_eq!(apl[1].role, NodeRole::Primary);
    assert_eq!(apl[2].peer_id, 2);
    assert_eq!(apl[2].role, NodeRole::Primary);
}

#[test]
fn empty_ring_returns_empty_apl() {
    let cs = ClusterState::new(Vec::new(), HashSet::new());
    assert!(get_apl_ann(&cs, 0, 3).is_empty());
}

#[test]
fn all_peers_down_returns_empty() {
    let cs = ClusterState::new(ring(&[(100, 0), (200, 1), (300, 2)]), HashSet::new());
    assert!(get_apl_ann(&cs, 50, 3).is_empty());
}

#[test]
fn vnode_id_for_primary_is_canonical_position() {
    // Ring: peer 0 at vnode 0, peer 1 at vnode 1, peer 2 at vnode 2.
    // key=50 wraps to vnode 0.
    let cs = ClusterState::new(ring(&[(100, 0), (200, 1), (300, 2)]), alive_set(&[0, 1, 2]));
    let apl = get_apl_ann(&cs, 50, 3);
    assert_eq!(apl[0].vnode, 0);
    assert_eq!(apl[1].vnode, 1);
    assert_eq!(apl[2].vnode, 2);
}

// --- Property test --------------------------------------------------------

/// Helper for the property test: build a deterministic ring and
/// liveness set from drawn primitives.
fn build_cluster(
    ring_size: usize,
    distinct_peers: usize,
    down_mask: u64,
) -> (ClusterState, Vec<PeerId>, HashSet<PeerId>) {
    // Each peer gets one vnode at token = idx * 1000.
    let peers: Vec<PeerId> = (0..u32::try_from(distinct_peers).unwrap_or(0)).collect();
    let pts: Vec<RingPoint> = (0..ring_size)
        .map(|i| {
            let pid = peers[i % peers.len()];
            RingPoint::new(
                u64::try_from(i).unwrap_or(0).saturating_mul(1000) + 100,
                pid,
            )
        })
        .collect();
    let mut alive = HashSet::new();
    for &p in &peers {
        if (down_mask >> u64::from(p)) & 1 == 0 {
            alive.insert(p);
        }
    }
    (ClusterState::new(pts, alive.clone()), peers, alive)
}

#[hegel::test(test_cases = 256)]
fn apl_invariants_hold_for_arbitrary_clusters(tc: TestCase) {
    let distinct_peers = tc.draw(gs::integers::<usize>().min_value(1).max_value(12));
    let n = tc.draw(
        gs::integers::<usize>()
            .min_value(1)
            .max_value(distinct_peers),
    );
    // Pick a key token uniformly. Token-space is 0..=u32::MAX
    // for testing; the walker handles wraparound below the
    // first ring point and above the last.
    let key_token = u64::from(tc.draw(gs::integers::<u32>()));
    // Random down mask: any subset of the peers may be down.
    let down_mask = if distinct_peers >= 64 {
        tc.draw(gs::integers::<u64>())
    } else {
        let max = (1u64 << distinct_peers) - 1;
        tc.draw(gs::integers::<u64>().min_value(0).max_value(max))
    };

    let (cluster, _peers, alive_set) = build_cluster(distinct_peers, distinct_peers, down_mask);

    let canonical: Vec<PeerId> = walk_n_successors(&cluster, key_token, n)
        .into_iter()
        .map(|(_, pid)| pid)
        .collect();
    let apl = get_apl_ann(&cluster, key_token, n);

    // 1) primaries.len() + fallbacks.len() == apl.len() (trivial,
    //    but enforce that no slot is unaccounted for).
    let prim = primaries(&apl);
    let fb = fallbacks(&apl);
    assert_eq!(prim.len() + fb.len(), apl.len());

    // 2) Bound: total annotated <= n, and <= alive distinct peers.
    assert!(apl.len() <= n);
    assert!(apl.len() <= alive_set.len());

    // 3) Every entry has a unique peer id.
    let mut ids: Vec<PeerId> = apl.iter().map(|p| p.peer_id).collect();
    ids.sort_unstable();
    let unique_count = {
        let mut copy = ids.clone();
        copy.dedup();
        copy.len()
    };
    assert_eq!(unique_count, ids.len(), "duplicate peer in apl");

    // 4) Every annotated peer is alive.
    for entry in &apl {
        assert!(
            alive_set.contains(&entry.peer_id),
            "down peer {} appeared in apl",
            entry.peer_id
        );
    }

    // 5) Primaries are a subset (by peer id) of the canonical
    //    walk-N-successors list.
    for entry in &prim {
        assert!(
            canonical.contains(&entry.peer_id),
            "primary {} not in canonical {canonical:?}",
            entry.peer_id
        );
    }

    // 6) When all canonical primaries are alive, fallbacks is empty.
    if canonical.iter().all(|p| alive_set.contains(p)) {
        assert!(fb.is_empty(), "expected no fallbacks, got {fb:?}");
        assert_eq!(prim.len(), canonical.len());
    }

    // 7) When the cluster has at least n alive distinct peers,
    //    the apl is exactly n long.
    if alive_set.len() >= n {
        assert_eq!(apl.len(), n);
    }
}
