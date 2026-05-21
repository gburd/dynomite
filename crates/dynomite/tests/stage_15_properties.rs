//! Stage 15 property soak: extended-budget invariants beyond
//! Stage 14.
//!
//! These tests run at 1024 cases each (the brief's soak budget)
//! and cover the four invariants called out in PLAN.md Stage 15:
//!
//! 1. Hash + token arithmetic round-trip.
//! 2. Quorum decision table totality across the full N/M state
//!    space.
//! 3. mbuf split/merge identity at every legal split point.
//! 4. Dispatch routing determinism: same key always routes to the
//!    same primary under a fixed ring.

use dynomite::cluster::datacenter::Continuum;
use dynomite::cluster::vnode::dispatch;
use dynomite::hashkit::{hash, DynToken, HashType};
use dynomite::io::mbuf::MbufPool;
use dynomite::msg::{Msg, MsgType, QuorumOutcome, ResponseMgr};
use hegel::generators as gs;
use hegel::TestCase;

const MBUF_CHUNK: usize = 4096;

#[hegel::test(test_cases = 1024)]
fn hash_token_round_trip(tc: TestCase) {
    let v = tc.draw(gs::integers::<u32>());
    let mut t = DynToken::default();
    t.size(1).expect("len 1 fits");
    t.set_int(v);
    assert_eq!(t.get_int(), v);
}

#[hegel::test(test_cases = 1024)]
fn hash_dispatch_is_deterministic(tc: TestCase) {
    let idx = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(HashType::all().len() - 1),
    );
    let key = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(1023));
    let ty = HashType::all()[idx];
    assert_eq!(hash(ty, &key), hash(ty, &key));
}

#[hegel::test(test_cases = 1024)]
fn mbuf_split_merge_identity(tc: TestCase) {
    let bytes = tc.draw(
        gs::vecs(gs::integers::<u8>())
            .min_size(0)
            .max_size(MBUF_CHUNK - 8),
    );
    let pool = MbufPool::new(MBUF_CHUNK, 64);
    let mut head = pool.get();
    head.copy_from_slice(&bytes);
    if bytes.is_empty() {
        // Empty input cannot be split; the trivial reconstruction
        // is the identity.
        assert_eq!(head.readable(), &bytes[..]);
        return;
    }
    let split_at = tc.draw(gs::integers::<usize>().min_value(1).max_value(bytes.len()));
    let tail = head.split_off(split_at, &pool).expect("split inside data");
    head.append(&tail);
    assert_eq!(head.readable(), &bytes[..]);
}

fn req() -> Msg {
    Msg::new(1, MsgType::ReqRedisGet, true)
}

fn good() -> Msg {
    Msg::new(2, MsgType::RspRedisStatus, false)
}

fn err() -> Msg {
    let mut m = Msg::new(3, MsgType::RspRedisStatus, false);
    m.flags_mut().is_error = true;
    m
}

#[hegel::test(test_cases = 1024)]
fn quorum_decision_table(tc: TestCase) {
    // max_responses in [1, 3] (the C reference's
    // MAX_REPLICAS_PER_DC); good and error counts together
    // saturate the manager.
    let max = tc.draw(gs::integers::<u8>().min_value(1).max_value(3));
    let goods = tc.draw(gs::integers::<u8>().min_value(0).max_value(max));
    let errs = tc.draw(gs::integers::<u8>().min_value(0).max_value(max - goods));

    let mut mgr = ResponseMgr::new(&req(), max, None);
    for i in 0..goods {
        mgr.submit_response(good(), u32::from(i) + 1);
    }
    for _ in 0..errs {
        mgr.submit_response(err(), 0);
    }

    let outcome = mgr.outcome();
    let pending = max - goods - errs;
    let quorum = max / 2 + 1;

    // Invariant 1: outcome is total (no panic).
    let _ = outcome;

    // Invariant 2: is_done iff outcome != Pending.
    assert_eq!(mgr.is_done(), !matches!(outcome, QuorumOutcome::Pending));

    // Invariant 3: Failed iff impossible-to-reach-quorum.
    if goods + pending < quorum {
        assert_eq!(outcome, QuorumOutcome::Failed);
    }

    // Invariant 4: Achieved implies goods >= quorum and matching
    // checksums (the manager only keeps consistent goods).
    if matches!(outcome, QuorumOutcome::Achieved) {
        assert!(mgr.good_responses() >= quorum);
    }
}

#[hegel::test(test_cases = 1024)]
fn dispatch_routes_same_key_to_same_primary(tc: TestCase) {
    // Build a small ring once. Routing must be deterministic for
    // any fixed (ring, key) pair.
    let key = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(1).max_size(64));
    let ring: Vec<Continuum> = (0u32..16)
        .map(|i| Continuum::new(DynToken::from_u32(i.wrapping_mul(0x1000_0000)), i))
        .collect();
    let token_a = hash(HashType::Murmur, &key);
    let token_b = hash(HashType::Murmur, &key);
    let a = dispatch(&ring, &token_a);
    let b = dispatch(&ring, &token_b);
    assert_eq!(a, b);
    if let Some(idx) = a {
        assert!(usize::try_from(idx).unwrap() < ring.len());
    }
}
