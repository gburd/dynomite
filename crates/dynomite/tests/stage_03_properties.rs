//! Property-based tests for [`dynomite::hashkit`] invariants.

use dynomite::hashkit::token::parse_token;
use dynomite::hashkit::{hash, DynToken, HashType};
use hegel::generators as gs;
use hegel::TestCase;

#[hegel::test]
fn set_int_get_int_round_trip(tc: TestCase) {
    let x = tc.draw(gs::integers::<u32>());
    let mut t = DynToken::default();
    t.size(1).expect("len 1 fits");
    t.set_int(x);
    assert_eq!(t.get_int(), x);
}

#[hegel::test]
fn dispatch_is_deterministic(tc: TestCase) {
    let idx = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(HashType::all().len() - 1),
    );
    // Bumped from 0..256 to exercise the 1024-byte boundary in
    // the jenkins/murmur3 main loops. Runs in well under 5s at
    // the default budget.
    let key = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(1023));
    let ty = HashType::all()[idx];
    let a = hash(ty, &key);
    let b = hash(ty, &key);
    assert_eq!(a, b);
}

#[hegel::test]
fn cmp_is_total_order(tc: TestCase) {
    let a = tc.draw(gs::integers::<u32>());
    let b = tc.draw(gs::integers::<u32>());
    let c = tc.draw(gs::integers::<u32>());
    let ta = DynToken::from_u32(a);
    let tb = DynToken::from_u32(b);
    let tc_tok = DynToken::from_u32(c);

    // Reflexivity.
    assert_eq!(ta.cmp(&ta), std::cmp::Ordering::Equal);

    // Antisymmetry.
    let ab = ta.cmp(&tb);
    let ba = tb.cmp(&ta);
    assert_eq!(ab, ba.reverse());

    // Transitivity (only when ta <= tb <= tc).
    if ta <= tb && tb <= tc_tok {
        assert!(ta <= tc_tok);
    }
}

#[hegel::test]
fn parse_round_trips_short_decimal(tc: TestCase) {
    let n = tc.draw(gs::integers::<u32>().min_value(0).max_value(999_999_999));
    let s = n.to_string();
    let t = parse_token(s.as_bytes()).expect("parse decimal");
    assert_eq!(t.get_int(), n);
}

#[hegel::test]
fn parse_total_on_arbitrary_bytes(tc: TestCase) {
    let bytes = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(0).max_size(31));
    // The parser must not panic on any input. Either it returns
    // Ok(token) or Err.
    let _ = parse_token(&bytes);
}
