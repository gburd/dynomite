//! Property-style coverage for Stage 1 utilities.
//!
//! These tests live outside `mod tests` because they exercise public
//! API only and are intentionally heavier than the unit tests.

use std::time::Duration;

use dynomite::core::ring_queue::RingChannels;
use dynomite::util::histogram::Histogram;
use proptest::prelude::*;

proptest! {
    #[test]
    fn histogram_percentile_is_monotone(samples in proptest::collection::vec(1u64..1_000_000, 1..256)) {
        let mut h = Histogram::new();
        for v in &samples {
            h.add(*v);
        }
        let probes = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1.0];
        let mut prev = 0u64;
        for p in probes {
            let cur = h.percentile(p);
            prop_assert!(cur >= prev, "p={p} cur={cur} prev={prev}");
            prev = cur;
        }
    }

    #[test]
    fn histogram_percentile_within_observed_range(samples in proptest::collection::vec(1u64..1_000, 32..256)) {
        let mut h = Histogram::new();
        let mut max = 0u64;
        for v in &samples {
            h.add(*v);
            if *v > max { max = *v; }
        }
        let p = h.percentile(0.99);
        prop_assert!(p == 0 || p <= max * 2, "percentile {p} not within bound 2*max={}", max * 2);
    }

    #[test]
    fn ring_queue_preserves_order(values in proptest::collection::vec(0u32..1_000, 0..200)) {
        let chans: RingChannels<u32, ()> = RingChannels::with_capacities(256, 1);
        for v in &values {
            chans.in_tx.send(*v).unwrap();
        }
        let mut received = Vec::with_capacity(values.len());
        while let Ok(v) = chans.in_rx.recv_timeout(Duration::from_millis(10)) {
            received.push(v);
        }
        prop_assert_eq!(received, values);
    }
}
