//! Integration tests for the random-slicing distribution path.
//!
//! Walks the same fixtures the dispatcher uses (no real
//! sockets) and asserts:
//!
//! * Random-slicing routes every key to exactly one peer.
//! * The distribution is within 5% of uniform across 10k keys
//!   and 4 peers.
//! * Pass-3 narrative: when one peer is `Down`, every key still
//!   routes to a `Normal` peer (or fails consistently).
//! * The shadow distribution counter increments when the live
//!   and shadow modes pick different peers.

use std::sync::Arc;

use dynomite::cluster::dispatch::{
    distribution_shadow_disagreement_total, reset_distribution_shadow_disagreement_total,
    ClusterDispatcher, DispatchPlan,
};
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::conf::{Distribution, HashType};
use dynomite::hashkit::DynToken;
use dynomite::msg::{Msg, MsgType};

fn build_pool(distribution: Distribution, shadow: Option<Distribution>) -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        hash: HashType::Murmur3X64_64,
        distribution,
        distribution_shadow: shadow,
        ..PoolConfig::default()
    };
    // Four local peers, all in the same rack.
    let peers: Vec<Peer> = (0..4u32)
        .map(|i| {
            Peer::new(
                i,
                PeerEndpoint::tcp("127.0.0.1".into(), 8101 + u16::try_from(i).unwrap_or(0)),
                "r1".into(),
                "dc1".into(),
                vec![DynToken::from_u32(i.wrapping_mul(0x4000_0000))],
                i == 0,
                true,
                false,
            )
        })
        .collect();
    let pool = Arc::new(ServerPool::new(cfg, peers));
    // Promote every peer to Normal.
    {
        let mut peers = pool.peers().write();
        for p in peers.iter_mut() {
            p.set_state(PeerState::Normal, 0);
        }
    }
    pool.rebuild_ring();
    pool
}

fn drive_keys(disp: &ClusterDispatcher, n: usize) -> Vec<DispatchPlan> {
    (0..n)
        .map(|i| {
            let req = Msg::new(u64::try_from(i).unwrap_or(0), MsgType::ReqRedisGet, true);
            let key = format!("key-{i:08x}");
            disp.plan(&req, key.as_bytes())
        })
        .collect()
}

#[test]
fn random_slicing_covers_all_peers_within_5_percent() {
    let pool = build_pool(Distribution::RandomSlicing, None);
    let disp = ClusterDispatcher::new(pool);
    let plans = drive_keys(&disp, 10_000);
    let mut counts = [0u32; 4];
    let mut routed = 0u32;
    for plan in &plans {
        match plan {
            DispatchPlan::Replicas { targets, .. } => {
                assert!(!targets.is_empty());
                let idx = targets[0].peer_idx as usize;
                counts[idx] += 1;
                routed += 1;
            }
            DispatchPlan::LocalDatastore => {
                counts[0] += 1;
                routed += 1;
            }
            other => panic!("unexpected plan {other:?}"),
        }
    }
    assert_eq!(routed, 10_000);
    let expected = 2_500u32;
    for (i, &c) in counts.iter().enumerate() {
        let lo = expected * 95 / 100;
        let hi = expected * 105 / 100;
        assert!(
            c >= lo && c <= hi,
            "claimant {i}: count {c} outside 5% band [{lo}, {hi}]"
        );
    }
}

#[test]
fn random_slicing_skips_down_peers_via_state_filter() {
    // Pass-3 narrative: marking a peer Down keeps every other
    // key on a Normal peer. The slice table is structurally
    // unchanged; the dispatcher's `is_routable()` filter rejects
    // the Down peer at fan-out time.
    let pool = build_pool(Distribution::RandomSlicing, None);
    {
        let mut peers = pool.peers().write();
        peers[1].set_state(PeerState::Down, 0);
    }
    let disp = ClusterDispatcher::new(pool.clone());
    let plans = drive_keys(&disp, 1024);
    for plan in &plans {
        if let DispatchPlan::Replicas { targets, .. } = plan {
            for t in targets {
                assert_ne!(
                    t.peer_idx, 1,
                    "Down peer must not appear in any plan target list"
                );
            }
        }
    }
}

#[test]
fn shadow_disagreement_counter_increments_with_different_distributions() {
    reset_distribution_shadow_disagreement_total();
    let baseline = distribution_shadow_disagreement_total();
    let pool = build_pool(Distribution::Vnode, Some(Distribution::RandomSlicing));
    let disp = ClusterDispatcher::new(pool);
    let _plans = drive_keys(&disp, 1024);
    let after = distribution_shadow_disagreement_total();
    let delta = after - baseline;
    // The two algorithms have completely independent peer
    // assignment functions; near-100% disagreement is expected.
    assert!(
        delta > 256,
        "expected many shadow disagreements over 1024 keys, got {delta}"
    );
}

#[test]
fn vnode_default_unchanged_when_distribution_unset() {
    // Distribution::Vnode is the default; routing must not
    // change shape.
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        ..PoolConfig::default()
    };
    assert_eq!(cfg.distribution, Distribution::Vnode);
    assert!(cfg.distribution_shadow.is_none());
}
