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
fn histogram_percentile_is_monotone(tc: TestCase) {
    let samples = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(1).max_value(999_999))
            .min_size(1)
            .max_size(255),
    );
    let mut h = Histogram::new();
    for v in &samples {
        h.record(*v);
    }
    let probes = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1.0];
    let mut prev = 0u64;
    for p in probes {
        let cur = h.percentile(p);
        assert!(cur >= prev, "p={p} cur={cur} prev={prev}");
        prev = cur;
    }
}

#[hegel::test]
fn histogram_percentile_within_observed_range(tc: TestCase) {
    let samples = tc.draw(
        gs::vecs(gs::integers::<u64>().min_value(1).max_value(999))
            .min_size(32)
            .max_size(255),
    );
    let mut h = Histogram::new();
    let mut max = 0u64;
    for v in &samples {
        h.record(*v);
        if *v > max {
            max = *v;
        }
    }
    let p = h.percentile(0.99);
    assert!(
        p == 0 || p <= max * 2,
        "percentile {p} not within bound 2*max={}",
        max * 2
    );
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
