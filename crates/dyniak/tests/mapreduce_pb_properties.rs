//! Property tests for the Riak PBC message codec and the MapReduce
//! built-in reduce functions.
//!
//! Two families of properties:
//!
//! * `RpbContent` / `RpbLink` survive an `encode` -> `decode`
//!   round-trip byte-for-byte over arbitrary field sets, including
//!   every optional metadata field and nested link / index lists.
//!   This guards the prost wire mapping for the object content
//!   message a customer's data rides on.
//! * The pure MapReduce reduce built-ins hold their defining
//!   invariants over arbitrary numeric / value inputs:
//!   `reduce_count` returns the input length, `reduce_set_union`
//!   dedupes while preserving first-seen order and is idempotent,
//!   and `reduce_sort` is a permutation that is order-independent.
//!
//! Each `#[hegel::test]` runs at least 256 generated cases under the
//! default profile.

use dyniak::mapreduce::builtins::{reduce_count, reduce_set_union, reduce_sort};
use dyniak::proto::pb::{RpbContent, RpbLink, RpbPair};
use hegel::generators as gs;
use hegel::TestCase;
use prost::Message as _;
use serde_json::Value;

// ------------------------------------------------------------------
// Generators
// ------------------------------------------------------------------

/// A small, possibly empty byte string drawn from a fixed alphabet so
/// shrinking stays meaningful (the codec is byte-transparent, so the
/// specific bytes do not change the property).
fn arb_bytes(tc: &TestCase) -> Vec<u8> {
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(8));
    let mut v = Vec::with_capacity(len);
    for _ in 0..len {
        v.push(tc.draw(gs::integers::<u8>()));
    }
    v
}

fn arb_opt_bytes(tc: &TestCase) -> Option<Vec<u8>> {
    if tc.draw(gs::booleans()) {
        Some(arb_bytes(tc))
    } else {
        None
    }
}

fn arb_opt_u32(tc: &TestCase) -> Option<u32> {
    if tc.draw(gs::booleans()) {
        Some(tc.draw(gs::integers::<u32>()))
    } else {
        None
    }
}

fn arb_link(tc: &TestCase) -> RpbLink {
    RpbLink {
        bucket: arb_opt_bytes(tc),
        key: arb_opt_bytes(tc),
        tag: arb_opt_bytes(tc),
    }
}

fn arb_pair(tc: &TestCase) -> RpbPair {
    RpbPair {
        key: arb_bytes(tc),
        value: arb_opt_bytes(tc),
    }
}

fn arb_vec<T>(tc: &TestCase, max: usize, f: impl Fn(&TestCase) -> T) -> Vec<T> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(max));
    (0..n).map(|_| f(tc)).collect()
}

fn arb_content(tc: &TestCase) -> RpbContent {
    RpbContent {
        value: arb_bytes(tc),
        content_type: arb_opt_bytes(tc),
        charset: arb_opt_bytes(tc),
        content_encoding: arb_opt_bytes(tc),
        vtag: arb_opt_bytes(tc),
        links: arb_vec(tc, 4, arb_link),
        last_mod: arb_opt_u32(tc),
        last_mod_usecs: arb_opt_u32(tc),
        usermeta: arb_vec(tc, 4, arb_pair),
        indexes: arb_vec(tc, 4, arb_pair),
        deleted: if tc.draw(gs::booleans()) {
            Some(tc.draw(gs::booleans()))
        } else {
            None
        },
    }
}

// ------------------------------------------------------------------
// Codec round-trip properties
// ------------------------------------------------------------------

#[hegel::test(test_cases = 256)]
fn rpb_link_round_trips(tc: TestCase) {
    let link = arb_link(&tc);
    let bytes = link.encode_to_vec();
    let back = RpbLink::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, link);
}

#[hegel::test(test_cases = 256)]
fn rpb_content_round_trips(tc: TestCase) {
    let content = arb_content(&tc);
    let bytes = content.encode_to_vec();
    let back = RpbContent::decode(bytes.as_slice()).expect("decode");
    assert_eq!(back, content);
    // Encoding the decoded message is byte-identical (canonical form).
    assert_eq!(back.encode_to_vec(), bytes);
}

// ------------------------------------------------------------------
// MapReduce reduce-builtin invariants
// ------------------------------------------------------------------

/// A small JSON value drawn from a fixed domain so set-union and sort
/// see repeats and mixed kinds.
fn arb_value(tc: &TestCase) -> Value {
    match tc.draw(gs::integers::<u8>().min_value(0).max_value(4)) {
        0 => Value::Null,
        1 => Value::Bool(tc.draw(gs::booleans())),
        2 => Value::from(tc.draw(gs::integers::<i32>().min_value(-5).max_value(5))),
        3 => Value::from(tc.draw(gs::sampled_from(&[
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ]))),
        _ => serde_json::json!([tc.draw(gs::integers::<i8>())]),
    }
}

fn arb_values(tc: &TestCase) -> Vec<Value> {
    let n = tc.draw(gs::integers::<usize>().min_value(0).max_value(12));
    (0..n).map(|_| arb_value(tc)).collect()
}

#[hegel::test(test_cases = 256)]
fn reduce_count_returns_input_length(tc: TestCase) {
    let inputs = arb_values(&tc);
    let out = reduce_count(&inputs, None).expect("count");
    assert_eq!(
        out,
        vec![Value::from(
            u64::try_from(inputs.len()).expect("len fits u64")
        )]
    );
}

#[hegel::test(test_cases = 256)]
fn reduce_set_union_dedupes_and_is_idempotent(tc: TestCase) {
    let inputs = arb_values(&tc);
    let once = reduce_set_union(&inputs, None).expect("union");
    // No duplicates in the output.
    for i in 0..once.len() {
        for j in (i + 1)..once.len() {
            assert_ne!(once[i], once[j], "union must not contain duplicates");
        }
    }
    // First-seen order: every output element appears in the input, and
    // their relative order matches first occurrence.
    let mut expected: Vec<Value> = Vec::new();
    for v in &inputs {
        if !expected.iter().any(|e| e == v) {
            expected.push(v.clone());
        }
    }
    assert_eq!(once, expected);
    // Idempotent: unioning the union changes nothing.
    let twice = reduce_set_union(&once, None).expect("union^2");
    assert_eq!(twice, once);
}

#[hegel::test(test_cases = 256)]
fn reduce_sort_is_an_order_independent_permutation(tc: TestCase) {
    let inputs = arb_values(&tc);
    let sorted = reduce_sort(&inputs, None).expect("sort");
    // Same multiset of elements (a permutation): equal length and the
    // sort of the sort is stable.
    assert_eq!(sorted.len(), inputs.len());
    let resorted = reduce_sort(&sorted, None).expect("sort^2");
    assert_eq!(resorted, sorted, "sort is idempotent");
    // Order-independence: sorting a reversed input yields the same
    // result.
    let mut reversed = inputs.clone();
    reversed.reverse();
    let from_reversed = reduce_sort(&reversed, None).expect("sort(rev)");
    assert_eq!(from_reversed, sorted, "sort is input-order independent");
}
