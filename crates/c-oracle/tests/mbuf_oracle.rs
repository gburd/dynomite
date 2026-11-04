//! Oracle: C `mbuf_copy` / `mbuf_length` / `mbuf_empty` / `mbuf_rewind` /
//! `mbuf_split`, the `dyn_string.h` search helpers, and the `dyn_util.c`
//! keypos / clock helpers against the Rust engine.

#![cfg(not(c_oracle_absent))]

use c_oracle::{mbuf as c, util::GLOBALS};
use dynomite::io::mbuf::{MbufPool, MBUF_ESIZE, MBUF_SIZE};
use dynomite::msg::keypos::{ArgPos, KeyPos};
use dynomite::util::{dyn_string, time};
use hegel::generators as gs;
use hegel::TestCase;

/// Copy chunks into one C mbuf and one Rust mbuf, then split both at the
/// same offset: lengths, contents, emptiness and the split tail agree.
#[hegel::test(test_cases = 300)]
fn mbuf_copy_split_matches_c(tc: TestCase) {
    let _g = GLOBALS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pool = MbufPool::default();
    let mut r = pool.get();
    c::reset();
    assert_eq!(r.remaining(), MBUF_SIZE - MBUF_ESIZE);
    let chunks: Vec<Vec<u8>> =
        tc.draw(gs::vecs(gs::vecs(gs::integers::<u8>()).max_size(64)).max_size(8));
    for ch in &chunks {
        assert!(c::copy(ch));
        r.copy_from_slice(ch);
        assert_eq!(r.len(), c::length() as usize);
        assert_eq!(r.is_empty(), c::empty());
    }
    assert_eq!(r.readable(), c::read().as_slice());
    let at: u32 = tc.draw(gs::integers::<u32>().max_value(c::length() + 4));
    let rt = r.split_off(at as usize, &pool);
    let ct = c::split(at);
    assert_eq!(
        rt.as_ref().map(|m| m.readable().to_vec()),
        ct,
        "split at {at}"
    );
    assert_eq!(r.readable(), c::read().as_slice());
    r.rewind();
    c::rewind();
    assert_eq!((r.len(), r.is_empty()), (c::length() as usize, c::empty()));
}

/// Finding: one C mbuf holds `MBUF_SIZE - sizeof(struct mbuf)` data
/// bytes (`mbuf_get` sets `end = start + mbuf_offset - MBUF_ESIZE`,
/// `mbuf_offset = MBUF_SIZE + MBUF_ESIZE - MBUF_HSIZE`); the Rust mbuf
/// holds `MBUF_SIZE - MBUF_ESIZE`. Pinned here until the Rust capacity
/// is aligned (ledger rows mbuf_get / mbuf_remaining_space /
/// mbuf_data_size stay todo).
#[test]
fn c_mbuf_data_capacity_is_size_minus_header() {
    let _g = GLOBALS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let c_cap = MBUF_SIZE - 64;
    c::reset();
    assert!(c::copy(&vec![7u8; c_cap]));
    assert!(!c::copy(b"x"));
    assert_eq!(
        MbufPool::default().get().remaining(),
        MBUF_SIZE - MBUF_ESIZE
    );
}

#[hegel::test(test_cases = 500)]
fn strchr_strrchr_match_c(tc: TestCase) {
    let hay: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>().max_value(4)).max_size(32));
    let needle: u8 = tc.draw(gs::integers::<u8>().max_value(4));
    assert_eq!(dyn_string::strchr(&hay, needle), c::strchr(&hay, needle));
    assert_eq!(dyn_string::strrchr(&hay, needle), c::strrchr(&hay, needle));
}

#[hegel::test(test_cases = 300)]
fn keypos_argpos_len_match_c(tc: TestCase) {
    let key: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).max_size(64));
    let a: usize = tc.draw(gs::integers::<usize>().max_value(key.len()));
    let b: usize = tc.draw(gs::integers::<usize>().min_value(a).max_value(key.len()));
    let kp = KeyPos::new(key.clone(), a..b);
    assert_eq!(kp.tag_len(), c::keypos_len(&key, a..b) as usize);
    assert_eq!(ArgPos::new(key.clone()).len(), c::argpos_len(&key) as usize);
}

/// C `dn_usec_now` and `current_timestamp_in_millis` read the wall clock;
/// the Rust readings bracketed by two C readings fall between them.
#[test]
fn wall_clock_matches_c() {
    let c0 = c::usec_now();
    let r = time::usec_now();
    let c1 = c::usec_now();
    assert!(c0 <= r && r <= c1, "{c0} {r} {c1}");
    let m0 = c::millis_now();
    let rm = time::msec_now();
    let m1 = c::millis_now();
    assert!(m0 <= rm && rm <= m1, "{m0} {rm} {m1}");
}
