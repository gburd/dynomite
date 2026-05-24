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

// ---- Map and HyperLogLog laws (second CRDT slice) ---------------------------

use dyn_riak::datatypes::{
    map::{FieldKey, FieldType, Map, MapOp, NestedOp},
    HyperLogLog,
};

fn arb_field_type(tc: &TestCase) -> FieldType {
    let pick = tc.draw(gs::integers::<u8>().min_value(0).max_value(4));
    match pick {
        0 => FieldType::Counter,
        1 => FieldType::OrSet,
        2 => FieldType::LwwRegister,
        3 => FieldType::EwFlag,
        _ => FieldType::NestedMap,
    }
}

fn arb_field_key(tc: &TestCase) -> FieldKey {
    let name = tc.draw(gs::sampled_from(&[
        "alpha".to_string(),
        "beta".to_string(),
        "gamma".to_string(),
    ]));
    let ty = arb_field_type(tc);
    FieldKey::new(name.into_bytes(), ty)
}

fn arb_nested_op_for(tc: &TestCase, ty: FieldType) -> NestedOp {
    match ty {
        FieldType::Counter => {
            let d = tc.draw(gs::integers::<i32>().min_value(-20).max_value(20));
            NestedOp::Counter(i64::from(d))
        }
        FieldType::OrSet => {
            let elt = tc.draw(gs::sampled_from(&[
                b"x".to_vec(),
                b"y".to_vec(),
                b"z".to_vec(),
            ]));
            if tc.draw(gs::booleans()) {
                NestedOp::SetAdd(elt)
            } else {
                NestedOp::SetRemove(elt)
            }
        }
        FieldType::LwwRegister => {
            let v = tc.draw(gs::integers::<u8>());
            let ts = tc.draw(gs::integers::<u64>().min_value(0).max_value(1_000));
            NestedOp::RegisterAssign {
                value: vec![v],
                ts_micros: ts,
            }
        }
        FieldType::EwFlag => NestedOp::Flag(tc.draw(gs::booleans())),
        FieldType::NestedMap => {
            // Avoid unbounded recursion: nested maps embed a single
            // counter update.
            let inner = NestedOp::Counter(i64::from(
                tc.draw(gs::integers::<i32>().min_value(-5).max_value(5)),
            ));
            NestedOp::Map(Box::new(MapOp::Update {
                field: FieldKey::new(b"i".to_vec(), FieldType::Counter),
                op: inner,
            }))
        }
    }
}

fn arb_map(tc: &TestCase) -> Map {
    let mut m = Map::new();
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(6));
    for _ in 0..n {
        let actor = arb_actor(tc);
        let field = arb_field_key(tc);
        let do_remove = tc.draw(gs::booleans());
        if do_remove {
            m.apply(&actor, &MapOp::Remove { field });
        } else {
            let op = arb_nested_op_for(tc, field.field_type);
            m.apply(&actor, &MapOp::Update { field, op });
        }
    }
    m
}

#[hegel::test(test_cases = 256)]
fn map_merge_is_commutative(tc: TestCase) {
    let a = arb_map(&tc);
    let b = arb_map(&tc);
    let mut left = a.clone();
    left.merge(&b);
    let mut right = b.clone();
    right.merge(&a);
    assert_eq!(left.value(), right.value());
}

#[hegel::test(test_cases = 256)]
fn map_merge_is_associative(tc: TestCase) {
    let a = arb_map(&tc);
    let b = arb_map(&tc);
    let c = arb_map(&tc);
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
fn map_merge_is_idempotent(tc: TestCase) {
    let a = arb_map(&tc);
    let mut x = a.clone();
    x.merge(&a);
    assert_eq!(x.value(), a.value());
}

// ---- HyperLogLog laws ------------------------------------------------------

fn arb_hll(tc: &TestCase) -> HyperLogLog {
    let mut h = HyperLogLog::new();
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(64));
    for _ in 0..n {
        let v = tc.draw(gs::integers::<u32>().min_value(0).max_value(2000));
        h.add(v.to_be_bytes());
    }
    h
}

/// Merge approximate equality: register arrays equal -> values
/// equal. We assert exact register equality because that is the
/// definition of HLL merge (element-wise max), and register
/// equality implies cardinality equality up to the f64 rounding
/// that `value()` performs.
#[hegel::test(test_cases = 256)]
fn hll_merge_is_commutative(tc: TestCase) {
    let a = arb_hll(&tc);
    let b = arb_hll(&tc);
    let mut left = a.clone();
    left.merge(&b);
    let mut right = b.clone();
    right.merge(&a);
    assert_eq!(left.registers(), right.registers());
    assert_eq!(left.value(), right.value());
}

#[hegel::test(test_cases = 256)]
fn hll_merge_is_associative(tc: TestCase) {
    let a = arb_hll(&tc);
    let b = arb_hll(&tc);
    let c = arb_hll(&tc);
    let mut left = a.clone();
    left.merge(&b);
    left.merge(&c);
    let mut right = b.clone();
    right.merge(&c);
    let mut acc = a.clone();
    acc.merge(&right);
    assert_eq!(left.registers(), acc.registers());
    assert_eq!(left.value(), acc.value());
}

#[hegel::test(test_cases = 256)]
fn hll_merge_is_idempotent(tc: TestCase) {
    let a = arb_hll(&tc);
    let mut x = a.clone();
    x.merge(&a);
    assert_eq!(x.registers(), a.registers());
    assert_eq!(x.value(), a.value());
}
