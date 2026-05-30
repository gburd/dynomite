//! Integration tests for `dyntext::bloom`.

use dyntext::bloom::BloomFilter;

#[test]
fn bloom_no_false_negatives_on_inserted_keys() {
    let mut bf = BloomFilter::with_size_and_fp_rate(500, 0.01);
    let keys: Vec<Vec<u8>> = (0..100_u32)
        .map(|i| format!("k-{i}").into_bytes())
        .collect();
    for k in &keys {
        bf.insert(k);
    }
    for k in &keys {
        assert!(bf.contains(k), "false negative on {k:?}");
    }
}

#[test]
fn bloom_false_positive_rate_under_5pct_at_design_load() {
    let n = 5_000_u32;
    let mut bf = BloomFilter::with_size_and_fp_rate(n as usize, 0.01);
    for i in 0..n {
        bf.insert(format!("inserted-{i}").as_bytes());
    }
    let probes = 5_000_u32;
    let mut fps = 0_u32;
    for i in 0..probes {
        if bf.contains(format!("query-{i}-not-in-filter").as_bytes()) {
            fps += 1;
        }
    }
    let observed = f64::from(fps) / f64::from(probes);
    assert!(observed < 0.05, "observed fp = {observed}");
}

#[test]
fn bloom_zero_keys_returns_false_for_anything() {
    let bf = BloomFilter::with_size_and_fp_rate(100, 0.01);
    assert!(!bf.contains(b"never inserted"));
    assert!(!bf.contains(b""));
}
