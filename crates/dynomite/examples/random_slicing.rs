//! Embedded random-slicing example.
//!
//! Builds a four-peer in-memory pool that uses the random
//! slicing distribution mode, drives a small synthetic
//! workload, and reports per-peer ownership counts. Mirrors
//! the operator-side check `dyn-admin distribution-dump`
//! produces.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p dynomite --example random_slicing
//! ```

use std::sync::Arc;

use dynomite::cluster::dispatch::{ClusterDispatcher, DispatchPlan};
use dynomite::cluster::peer::{Peer, PeerEndpoint, PeerState};
use dynomite::cluster::pool::{PoolConfig, ServerPool};
use dynomite::conf::{Distribution, HashType};
use dynomite::hashkit::DynToken;
use dynomite::msg::{Msg, MsgType};

fn main() {
    let cfg = PoolConfig {
        dc: "dc1".into(),
        rack: "r1".into(),
        hash: HashType::Murmur3X64_64,
        distribution: Distribution::RandomSlicing,
        ..PoolConfig::default()
    };
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
    {
        let mut peers = pool.peers().write();
        for p in peers.iter_mut() {
            p.set_state(PeerState::Normal, 0);
        }
    }
    pool.rebuild_ring();
    let disp = ClusterDispatcher::new(pool);

    let mut counts = [0u32; 4];
    let total = 10_000usize;
    for i in 0..total {
        let req = Msg::new(u64::try_from(i).unwrap_or(0), MsgType::ReqRedisGet, true);
        let key = format!("key-{i:08x}");
        let plan = disp.plan(&req, key.as_bytes());
        match plan {
            DispatchPlan::Replicas { targets, .. } => {
                let idx = targets[0].peer_idx as usize;
                if idx < counts.len() {
                    counts[idx] += 1;
                }
            }
            DispatchPlan::LocalDatastore => counts[0] += 1,
            DispatchPlan::NoTargets | DispatchPlan::Drop => {}
        }
    }

    println!("Per-peer ownership over {total} synthetic keys:");
    for (i, &c) in counts.iter().enumerate() {
        println!("  peer-{i:02} : {c}");
    }
}
