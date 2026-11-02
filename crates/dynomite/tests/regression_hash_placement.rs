//! Every configurable hash spreads keys across every node under both
//! distributions.
//!
//! Regression: `hash: murmur3` produced a four-word token and
//! `murmur3_x64_64` a two-word token. Token order compares length before
//! magnitude, so on the vnode ring of one-word node tokens every key sorted
//! above every node and routed to the first one. `random_slicing` received
//! 32-bit hashes in the low half of a `u64` and put every key in the first
//! slice. Both sent 100% of keys to one node with no error.

use std::sync::Arc;

use dynomite::cluster::dispatch::{ClusterDispatcher, DispatchPlan};
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::conf::{Distribution, HashType};
use dynomite::hashkit::DynToken;
use dynomite::msg::{Msg, MsgType};

const PEERS: u32 = 4;
const KEYS: u32 = 4_000;

fn pool(hash: HashType, distribution: Distribution) -> Arc<ServerPool> {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        hash,
        distribution,
        ..PoolConfig::default()
    };
    let peers: Vec<Peer> = (0..PEERS)
        .map(|i| {
            Peer::new(
                i,
                PeerEndpoint::tcp("127.0.0.1".into(), 8101 + u16::try_from(i).unwrap()),
                "r1".into(),
                "dc1".into(),
                vec![DynToken::from_u32(i * (u32::MAX / PEERS))],
                i == 0,
                true,
                false,
            )
        })
        .collect();
    let pool = Arc::new(ServerPool::new(cfg, peers));
    for p in pool.peers().write().iter_mut() {
        p.set_state(PeerState::Normal, 0);
    }
    pool.rebuild_ring();
    pool
}

fn per_peer(hash: HashType, distribution: Distribution) -> [u32; PEERS as usize] {
    let disp = ClusterDispatcher::new(pool(hash, distribution));
    let mut counts = [0u32; PEERS as usize];
    for i in 0..KEYS {
        let req = Msg::new(u64::from(i), MsgType::ReqRedisGet, true);
        let idx = match disp.plan(&req, format!("key:{i}").as_bytes()) {
            DispatchPlan::Replicas { targets, .. } => targets[0].peer_idx,
            DispatchPlan::LocalDatastore => 0,
            other => panic!("{hash:?}/{distribution:?}: unexpected plan {other:?}"),
        };
        counts[idx as usize] += 1;
    }
    counts
}

/// No hash may send every key to one node (the regression). `crc32` is
/// excluded: the libmemcached variant is a 15-bit hash by definition
/// (`(~crc >> 16) & 0x7fff`), so on a 32-bit ring it reaches only the first
/// node, in C as here.
#[test]
fn regression_no_hash_collapses_placement_onto_one_node() {
    for hash in ALL {
        if hash == HashType::Crc32 {
            continue;
        }
        for distribution in [Distribution::Vnode, Distribution::RandomSlicing] {
            let counts = per_peer(hash, distribution);
            assert!(
                counts.iter().filter(|&&c| c > 0).count() > 1,
                "{hash:?}/{distribution:?} sent every key to one node: {counts:?}"
            );
        }
    }
}

/// The well-mixed hashes, including the default and both MurmurHash3
/// variants, spread keys evenly. The FNV-1 family is left out: on keys that
/// differ only in their last byte it clusters (the final byte only XORs
/// into the low 8 bits), which is the algorithm and matches C exactly.
#[test]
fn well_mixed_hashes_spread_evenly() {
    for hash in [
        HashType::Murmur,
        HashType::Murmur3,
        HashType::Murmur3X64_64,
        HashType::Md5,
    ] {
        for distribution in [Distribution::Vnode, Distribution::RandomSlicing] {
            let counts = per_peer(hash, distribution);
            for &c in &counts {
                // 25% expected per node; allow 18%..32%.
                assert!(
                    (KEYS * 18 / 100..=KEYS * 32 / 100).contains(&c),
                    "{hash:?}/{distribution:?} uneven: {counts:?}"
                );
            }
        }
    }
}

const ALL: [HashType; 14] = [
    HashType::OneAtATime,
    HashType::Md5,
    HashType::Crc16,
    HashType::Crc32,
    HashType::Crc32a,
    HashType::Fnv1_64,
    HashType::Fnv1a64,
    HashType::Fnv1_32,
    HashType::Fnv1a32,
    HashType::Hsieh,
    HashType::Murmur,
    HashType::Jenkins,
    HashType::Murmur3,
    HashType::Murmur3X64_64,
];
