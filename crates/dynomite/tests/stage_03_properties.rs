//! Property-based tests for [`dynomite::hashkit`] invariants.

use dynomite::hashkit::token::parse_token;
use dynomite::hashkit::{hash, DynToken, HashType};
use proptest::prelude::*;

proptest! {
    #[test]
    fn set_int_get_int_round_trip(x in any::<u32>()) {
        let mut t = DynToken::default();
        t.size(1).expect("len 1 fits");
        t.set_int(x);
        prop_assert_eq!(t.get_int(), x);
    }

    #[test]
    fn dispatch_is_deterministic(
        idx in 0usize..HashType::all().len(),
        // Bumped from 0..256 to exercise the 1024-byte boundary in
        // the jenkins/murmur3 main loops. Runs in well under 5s at
        // the default proptest budget.
        key in proptest::collection::vec(any::<u8>(), 0..1024),
    ) {
        let ty = HashType::all()[idx];
        let a = hash(ty, &key);
        let b = hash(ty, &key);
        prop_assert_eq!(a, b);
    }

    #[test]
    fn cmp_is_total_order(
        a in any::<u32>(),
        b in any::<u32>(),
        c in any::<u32>(),
    ) {
        let ta = DynToken::from_u32(a);
        let tb = DynToken::from_u32(b);
        let tc = DynToken::from_u32(c);

        // Reflexivity.
        prop_assert_eq!(ta.cmp(&ta), std::cmp::Ordering::Equal);

        // Antisymmetry.
        let ab = ta.cmp(&tb);
        let ba = tb.cmp(&ta);
        prop_assert_eq!(ab, ba.reverse());

        // Transitivity (only when ta <= tb <= tc).
        if ta <= tb && tb <= tc {
            prop_assert!(ta <= tc);
        }
    }

    #[test]
    fn parse_round_trips_short_decimal(n in 0u32..1_000_000_000u32) {
        let s = n.to_string();
        let t = parse_token(s.as_bytes()).expect("parse decimal");
        prop_assert_eq!(t.get_int(), n);
    }

    #[test]
    fn parse_total_on_arbitrary_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..32)) {
        // The parser must not panic on any input. Either it returns
        // Ok(token) or Err.
        let _ = parse_token(&bytes);
    }
}
