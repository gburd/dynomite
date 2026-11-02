//! Oracle: C `vnode_dispatch` (`src/dyn_vnode.c`) against the Rust ring
//! lookup `dynomite::cluster::vnode::dispatch`.
//!
//! For a sorted ring of `(token, peer)` points and a key token, both must
//! pick the same peer, including the wraparound cases (key token above the
//! last point or at/below the first) and exact token hits.

#![cfg(not(c_oracle_absent))]

use c_oracle::hashkit::CToken;
use c_oracle::vnode;
use dynomite::cluster::datacenter::Continuum;
use dynomite::cluster::vnode::dispatch;
use dynomite::hashkit::DynToken;
use hegel::generators as gs;
use hegel::TestCase;

fn c_tok(v: u32) -> CToken {
    CToken {
        signum: u32::from(v != 0),
        mag: [v, 0, 0, 0],
        len: 1,
    }
}

/// Draw a ring of 1..=64 distinct u32 tokens (one token per peer, the C
/// contract; see DEV-H4), sorted ascending, peers numbered in draw order.
fn draw_ring(tc: &TestCase) -> Vec<(u32, u32)> {
    let mut toks: Vec<u32> = tc.draw(
        gs::vecs(gs::integers::<u32>())
            .min_size(1)
            .max_size(64)
            .unique(true),
    );
    let ring: Vec<(u32, u32)> = toks
        .drain(..)
        .enumerate()
        .map(|(i, t)| (t, u32::try_from(i).unwrap()))
        .collect();
    let mut ring = ring;
    ring.sort_by_key(|(t, _)| *t);
    ring
}

fn check(ring: &[(u32, u32)], key: u32) {
    let c_ring: Vec<(CToken, u32)> = ring.iter().map(|(t, p)| (c_tok(*t), *p)).collect();
    let c = vnode::dispatch(&c_ring, &c_tok(key));
    let r_ring: Vec<Continuum> = ring
        .iter()
        .map(|(t, p)| Continuum::new(DynToken::from_u32(*t), *p))
        .collect();
    let r = dispatch(&r_ring, &DynToken::from_u32(key)).expect("non-empty ring");
    assert_eq!(c, r, "ring {ring:?} key {key}");
}

#[hegel::test(test_cases = 3000)]
fn vnode_dispatch_matches_c_random_key(tc: TestCase) {
    let ring = draw_ring(&tc);
    let key: u32 = tc.draw(gs::integers::<u32>());
    check(&ring, key);
}

/// Keys placed exactly on, just below, and just above every ring point,
/// plus the extremes, so every branch of the binary search and both
/// wraparound conditions are hit.
#[hegel::test(test_cases = 500)]
fn vnode_dispatch_matches_c_boundary_keys(tc: TestCase) {
    let ring = draw_ring(&tc);
    let mut keys = vec![0, 1, u32::MAX];
    for (t, _) in &ring {
        keys.push(*t);
        keys.push(t.wrapping_sub(1));
        keys.push(t.wrapping_add(1));
    }
    for key in keys {
        check(&ring, key);
    }
}
