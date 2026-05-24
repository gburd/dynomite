//! CRDT property tests.
//!
//! Exercises the three CRDT laws -- associativity, commutativity,
//! idempotence -- on every datatype this crate ships.
//! Each `#[hegel::test]` runs at least 256 generated cases under
//! the default profile.

use dyn_riak::datatypes::{ActorId, Crdt, EwFlag, LwwRegister, OrSet, PnCounter};
use hegel::generators as gs;
use hegel::TestCase;

fn arb_actor(tc: &TestCase) -> ActorId {
    let dc = tc.draw(gs::sampled_from(&["dc1".to_string(), "dc2".to_string()]));
    let peer = tc.draw(gs::sampled_from(&[
        "p1".to_string(),
        "p2".to_string(),
        "p3".to_string(),
    ]));
    ActorId::new(dc, peer)
}

fn arb_pncounter(tc: &TestCase) -> PnCounter {
    let mut c = PnCounter::new();
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    for _ in 0..n {
        let a = arb_actor(tc);
        let delta = tc.draw(gs::integers::<i32>().min_value(-50).max_value(50));
        c.apply(&a, i64::from(delta));
    }
    c
}

fn arb_orset(tc: &TestCase) -> OrSet {
    let mut s = OrSet::new();
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    let domain: [&[u8]; 4] = [b"x", b"y", b"z", b"w"];
    for _ in 0..n {
        let a = arb_actor(tc);
        let pick = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(domain.len() - 1),
        );
        let do_remove = tc.draw(gs::booleans());
        if do_remove {
            s.remove(domain[pick]);
        } else {
            s.add(&a, domain[pick].to_vec());
        }
    }
    s
}

fn arb_register(tc: &TestCase) -> LwwRegister {
    let mut r = LwwRegister::new();
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(6));
    for _ in 0..n {
        let a = arb_actor(tc);
        let ts = tc.draw(gs::integers::<u64>().min_value(0).max_value(1_000));
        let v = tc.draw(gs::integers::<u8>());
        r.assign(&a, ts, vec![v]);
    }
    r
}

fn arb_flag(tc: &TestCase) -> EwFlag {
    let mut f = EwFlag::new();
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    for _ in 0..n {
        let a = arb_actor(tc);
        let enable = tc.draw(gs::booleans());
        if enable {
            f.enable(&a);
        } else {
            f.disable();
        }
    }
    f
}

// ---- PnCounter laws --------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn pncounter_merge_is_commutative(tc: TestCase) {
    let a = arb_pncounter(&tc);
    let b = arb_pncounter(&tc);
    let mut left = a.clone();
    left.merge(&b);
    let mut right = b.clone();
    right.merge(&a);
    assert_eq!(left, right);
}

#[hegel::test(test_cases = 256)]
fn pncounter_merge_is_associative(tc: TestCase) {
    let a = arb_pncounter(&tc);
    let b = arb_pncounter(&tc);
    let c = arb_pncounter(&tc);
    let mut left = a.clone();
    left.merge(&b);
    left.merge(&c);
    let mut right = b.clone();
    right.merge(&c);
    let mut acc = a.clone();
    acc.merge(&right);
    assert_eq!(left, acc);
}

#[hegel::test(test_cases = 256)]
fn pncounter_merge_is_idempotent(tc: TestCase) {
    let a = arb_pncounter(&tc);
    let mut x = a.clone();
    x.merge(&a);
    assert_eq!(x, a);
}

// ---- OrSet laws ------------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn orset_merge_is_commutative(tc: TestCase) {
    let a = arb_orset(&tc);
    let b = arb_orset(&tc);
    let mut left = a.clone();
    left.merge(&b);
    let mut right = b.clone();
    right.merge(&a);
    assert_eq!(left.value(), right.value());
}

#[hegel::test(test_cases = 256)]
fn orset_merge_is_associative(tc: TestCase) {
    let a = arb_orset(&tc);
    let b = arb_orset(&tc);
    let c = arb_orset(&tc);
    let mut left = a.clone();
    left.merge(&b);
    left.merge(&c);
    let mut right = b.clone();
    right.merge(&c);
    let mut acc = a.clone();
    acc.merge(&right);
    assert_eq!(left.value(), acc.value());
}

#[hegel::test(test_cases = 256)]
fn orset_merge_is_idempotent(tc: TestCase) {
    let a = arb_orset(&tc);
    let mut x = a.clone();
    x.merge(&a);
    assert_eq!(x.value(), a.value());
}

// ---- LwwRegister laws ------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn register_merge_is_commutative(tc: TestCase) {
    let a = arb_register(&tc);
    let b = arb_register(&tc);
    let mut left = a.clone();
    left.merge(&b);
    let mut right = b.clone();
    right.merge(&a);
    assert_eq!(left, right);
}

#[hegel::test(test_cases = 256)]
fn register_merge_is_associative(tc: TestCase) {
    let a = arb_register(&tc);
    let b = arb_register(&tc);
    let c = arb_register(&tc);
    let mut left = a.clone();
    left.merge(&b);
    left.merge(&c);
    let mut right = b.clone();
    right.merge(&c);
    let mut acc = a.clone();
    acc.merge(&right);
    assert_eq!(left, acc);
}

#[hegel::test(test_cases = 256)]
fn register_merge_is_idempotent(tc: TestCase) {
    let a = arb_register(&tc);
    let mut x = a.clone();
    x.merge(&a);
    assert_eq!(x, a);
}

// ---- EwFlag laws -----------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn flag_merge_is_commutative(tc: TestCase) {
    let a = arb_flag(&tc);
    let b = arb_flag(&tc);
    let mut left = a.clone();
    left.merge(&b);
    let mut right = b.clone();
    right.merge(&a);
    assert_eq!(left.value(), right.value());
}

#[hegel::test(test_cases = 256)]
fn flag_merge_is_associative(tc: TestCase) {
    let a = arb_flag(&tc);
    let b = arb_flag(&tc);
    let c = arb_flag(&tc);
    let mut left = a.clone();
    left.merge(&b);
    left.merge(&c);
    let mut right = b.clone();
    right.merge(&c);
    let mut acc = a.clone();
    acc.merge(&right);
    assert_eq!(left.value(), acc.value());
}

#[hegel::test(test_cases = 256)]
fn flag_merge_is_idempotent(tc: TestCase) {
    let a = arb_flag(&tc);
    let mut x = a.clone();
    x.merge(&a);
    assert_eq!(x.value(), a.value());
}
