//! Oracle: the C estimated histogram (`src/dyn_histogram.c`) against the
//! Rust `dynomite::stats::Histogram`.
//!
//! Every property drives the C struct and the Rust histogram with the same
//! sequence of `histo_add` / `histo_compute` / `histo_reset` operations and
//! requires every observable field to match after each step: the bucket
//! counts, `val_max`, and the `mean`, `val_95th`, `val_99th`, `val_999th`
//! values `histo_compute` publishes.

#![cfg(not(c_oracle_absent))]

use c_oracle::histogram::{CHistogram, BUCKET_SIZE};
use dynomite::stats::{bucket_offsets, Histogram, BUCKET_COUNT};
use hegel::generators as gs;
use hegel::TestCase;

fn assert_same(c: &CHistogram, r: &Histogram, step: &str) {
    assert_eq!(&c.buckets, r.buckets(), "buckets diverge after {step}");
    assert_eq!(c.val_max, r.max(), "val_max diverges after {step}");
    assert_eq!(c.mean, r.mean(), "mean diverges after {step}");
    assert_eq!(c.val_95th, r.val_95th(), "val_95th diverges after {step}");
    assert_eq!(c.val_99th, r.val_99th(), "val_99th diverges after {step}");
    assert_eq!(
        c.val_999th,
        r.val_999th(),
        "val_999th diverges after {step}"
    );
    let c_count = c.buckets.iter().copied().fold(0u64, u64::wrapping_add);
    assert_eq!(c_count, r.count(), "count diverges after {step}");
}

/// A value biased toward the interesting regions: small latencies, the
/// span of the bucket table, the exact bucket boundaries, and anything.
fn draw_value(tc: &TestCase) -> u64 {
    match tc.draw(gs::integers::<u8>().max_value(3)) {
        0 => tc.draw(gs::integers::<u64>().max_value(1_000)),
        1 => tc.draw(gs::integers::<u64>().max_value(40_000_000)),
        2 => {
            let i = tc.draw(gs::integers::<usize>().max_value(BUCKET_COUNT - 1));
            let delta = tc.draw(gs::integers::<i8>().min_value(-1).max_value(1));
            bucket_offsets()[i].wrapping_add_signed(i64::from(delta))
        }
        _ => tc.draw(gs::integers::<u64>()),
    }
}

#[test]
fn layout_constants_match_c() {
    assert_eq!(BUCKET_SIZE, BUCKET_COUNT);
}

/// Random interleavings of add, compute, and reset agree field for field.
#[hegel::test(test_cases = 256)]
fn histogram_ops_match_c(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().max_value(500));
    let mut c = CHistogram::new();
    let mut r = Histogram::new();
    assert_same(&c, &r, "init");
    for step in 0..n {
        match tc.draw(gs::integers::<u8>().max_value(19)) {
            0..=15 => {
                let v = draw_value(&tc);
                c.add(v);
                r.record(v);
                assert_same(&c, &r, &format!("step {step}: add {v}"));
            }
            16..=18 => {
                c.compute();
                r.compute();
                assert_same(&c, &r, &format!("step {step}: compute"));
            }
            _ => {
                c.reset();
                r.reset();
                assert_same(&c, &r, &format!("step {step}: reset"));
            }
        }
    }
    c.compute();
    r.compute();
    assert_same(&c, &r, "final compute");
}

/// A batch of values then one compute, the shape `stats_swap` produces.
#[hegel::test(test_cases = 256)]
fn histogram_batch_compute_matches_c(tc: TestCase) {
    let values = tc.draw(gs::vecs(gs::integers::<u64>().max_value(50_000_000)).max_size(500));
    let mut c = CHistogram::new();
    let mut r = Histogram::new();
    for &v in &values {
        c.add(v);
        r.record(v);
    }
    c.compute();
    r.compute();
    assert_same(&c, &r, &format!("compute over {values:?}"));
}

/// Every bucket boundary the C code can observe (offsets 0..=92; C never
/// reads offset 93) routes the same way and publishes the same offset.
#[test]
fn bucket_boundaries_match_c() {
    for (i, &offset) in bucket_offsets()[..BUCKET_COUNT - 1].iter().enumerate() {
        for v in [offset - 1, offset, offset + 1] {
            let mut c = CHistogram::new();
            let mut r = Histogram::new();
            for _ in 0..1_000 {
                c.add(v);
                r.record(v);
            }
            c.compute();
            r.compute();
            assert_same(&c, &r, &format!("1000 x add {v} (bucket {i})"));
        }
    }
}

/// Values past the last offset, including `u64::MAX`, never reach the
/// final bucket in C; compute therefore never takes its overflow branch.
#[test]
fn huge_values_match_c() {
    let last = bucket_offsets()[BUCKET_COUNT - 1];
    let mut c = CHistogram::new();
    let mut r = Histogram::new();
    for v in [last - 1, last, last + 1, u64::MAX / 2, u64::MAX] {
        c.add(v);
        r.record(v);
        assert_same(&c, &r, &format!("add {v}"));
    }
    assert_eq!(c.buckets[BUCKET_SIZE - 1], 0);
    c.compute();
    r.compute();
    assert_same(&c, &r, "compute");
}

/// Compute on an empty histogram, and on one or two observations where
/// every percentile threshold floors to zero.
#[test]
fn small_counts_match_c() {
    for values in [&[][..], &[0][..], &[500][..], &[7, 9_000][..]] {
        let mut c = CHistogram::new();
        let mut r = Histogram::new();
        for &v in values {
            c.add(v);
            r.record(v);
        }
        c.compute();
        r.compute();
        assert_same(&c, &r, &format!("compute over {values:?}"));
    }
}
