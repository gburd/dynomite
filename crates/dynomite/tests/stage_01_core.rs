//! Property-style coverage for Stage 1 utilities.
//!
//! These tests live outside `mod tests` because they exercise public
//! API only and are intentionally heavier than the unit tests.

use std::time::Duration;

use dynomite::core::ring_queue::RingChannels;
use dynomite::stats::Histogram;
use hegel::generators as gs;
use hegel::TestCase;

#[hegel::test]
fn histogram_percentiles_are_ordered(tc: TestCase) {
    let samples = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(1).max_value(999_999))
            .min_size(1)
            .max_size(255),
    );
    let mut h = Histogram::new();
    for v in &samples {
        h.record(*v);
    }
    h.compute();
    assert!(h.val_95th() <= h.val_99th(), "{samples:?}");
    assert!(h.val_99th() <= h.val_999th(), "{samples:?}");
}

#[hegel::test]
fn histogram_percentile_within_observed_range(tc: TestCase) {
    let samples = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(1).max_value(999))
            .min_size(32)
            .max_size(255),
    );
    let mut h = Histogram::new();
    for v in &samples {
        h.record(*v);
    }
    h.compute();
    // A bucket's offset never exceeds the values counted in it.
    let max = samples.iter().copied().max().unwrap_or(0);
    assert!(h.val_99th() <= max, "p99 {} above max {max}", h.val_99th());
}

#[hegel::test]
fn ring_queue_preserves_order(tc: TestCase) {
    let values = tc.draw(
        gs::vecs(gs::integers::<u32>().min_value(0).max_value(999))
            .min_size(0)
            .max_size(199),
    );
    let chans: RingChannels<u32, ()> = RingChannels::with_capacities(256, 1);
    for v in &values {
        chans.in_tx.send(*v).unwrap();
    }
    let mut received = Vec::with_capacity(values.len());
    while let Ok(v) = chans.in_rx.recv_timeout(Duration::from_millis(10)) {
        received.push(v);
    }
    assert_eq!(received, values);
}
