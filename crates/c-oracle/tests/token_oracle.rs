//! Oracle: C token arithmetic (`src/hashkit/dyn_token.c`) against Rust.
//!
//! `parse_dyn_token` converts the decimal token text from configuration
//! (`tokens:`), gossip, and DNODE messages into a `struct dyn_token`;
//! `cmp_dyn_token` orders tokens on the ring. Both must agree with the Rust
//! transliteration for every input the C accepts.

#![cfg(not(c_oracle_absent))]

use std::cmp::Ordering;

use c_oracle::hashkit::{self, CToken};
use dynomite::hashkit::token::{parse_token, Sign};
use dynomite::hashkit::DynToken;
use hegel::generators as gs;
use hegel::TestCase;

fn c_signum(t: &DynToken) -> u32 {
    match t.sign() {
        Sign::Zero => 0,
        Sign::Positive => 1,
        Sign::Negative => u32::MAX,
    }
}

fn to_c(t: &DynToken) -> CToken {
    let mut mag = [0u32; 4];
    mag[..t.mag().len()].copy_from_slice(t.mag());
    CToken {
        signum: c_signum(t),
        mag,
        len: u32::try_from(t.len()).expect("token len fits u32"),
    }
}

fn digit_text(tc: &TestCase, max_len: usize) -> Vec<u8> {
    tc.draw(
        gs::vecs(gs::integers::<u8>().min_value(b'0').max_value(b'9'))
            .min_size(1)
            .max_size(max_len),
    )
}

/// Every decimal string (optionally signed) parses to the same token in C
/// and Rust. Lengths run past one 10-digit group so the multi-group carry
/// path (`add_next_word`) is exercised.
#[hegel::test(test_cases = 3000)]
fn parse_dyn_token_matches_c(tc: TestCase) {
    let mut text = digit_text(&tc, 40);
    if tc.draw(gs::booleans()) {
        text.insert(0, b'-');
    }
    let c = hashkit::parse_token(&text).expect("C accepts a signed digit string");
    let r = parse_token(&text).expect("Rust accepts a signed digit string");
    assert_eq!(
        (c.signum, c.words()),
        (c_signum(&r), r.mag()),
        "text {:?}: C {c:?}, Rust {r:?}",
        String::from_utf8_lossy(&text)
    );
}

/// The token values operators actually configure: every u32, rendered as
/// decimal, round-trips identically through both parsers.
#[hegel::test(test_cases = 3000)]
fn parse_dyn_token_matches_c_on_u32(tc: TestCase) {
    let v: u32 = tc.draw(gs::integers::<u32>());
    let text = v.to_string().into_bytes();
    let c = hashkit::parse_token(&text).expect("C accepts");
    let r = parse_token(&text).expect("Rust accepts");
    assert_eq!((c.signum, c.words()), (c_signum(&r), r.mag()), "value {v}");
}

/// `cmp_dyn_token` and the Rust `Ord` agree on every pair of parsed tokens.
#[hegel::test(test_cases = 3000)]
fn cmp_dyn_token_matches_c(tc: TestCase) {
    let a = parse_token(&digit_text(&tc, 24)).expect("parse a");
    let b = parse_token(&digit_text(&tc, 24)).expect("parse b");
    let c = hashkit::cmp_tokens(&to_c(&a), &to_c(&b));
    let r = match a.cmp(&b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };
    assert_eq!(c, r, "a {a:?} b {b:?}");
}
