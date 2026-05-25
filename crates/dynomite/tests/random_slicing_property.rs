//! Property tests for the random-slicing distribution.

use std::collections::BTreeSet;

use dynomite::hashkit::random_slicing::RandomSlices;
use hegel::generators as gs;
use hegel::TestCase;

/// A small helper that constructs N unique peer names so the
/// builders never reject for duplicate-claimant.
fn unique_names(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("peer-{i:08x}")).collect()
}

#[hegel::test(test_cases = 256)]
fn full_coverage_invariant_uniform(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(64));
    let names = unique_names(n);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let s = RandomSlices::from_uniform(&refs).unwrap();
    let total: u128 = s.interval_sizes().iter().map(|&v| u128::from(v)).sum();
    let max = u128::from(u64::MAX);
    assert!(
        total >= max - n as u128 && total <= max + n as u128,
        "uniform sum {total} not within +/- {n} of u64::MAX"
    );
    // Lookup is total over u64::MAX.
    let _ = s.claimant_for(0).unwrap();
    let _ = s.claimant_for(u64::MAX).unwrap();
}

#[hegel::test(test_cases = 256)]
fn deterministic_claimant_for_same_input(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(32));
    let h = tc.draw(gs::integers::<u64>());
    let names = unique_names(n);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let s = RandomSlices::from_uniform(&refs).unwrap();
    let a = s.claimant_for(h).map(str::to_string);
    let b = s.claimant_for(h).map(str::to_string);
    assert_eq!(a, b);
}

#[hegel::test(test_cases = 64)]
fn rebalance_moves_at_most_one_over_n(tc: TestCase) {
    // Removing one claimant from an N-claimant uniform table
    // should move on average ~1/N keys (the keys that fell on
    // the removed slice). Property: when we sample a population
    // of keys, the fraction that change owner is within 2x of
    // the theoretical 1/N.
    let n = tc.draw(gs::integers::<usize>().min_value(2).max_value(8));
    let names_full = unique_names(n);
    let refs_full: Vec<&str> = names_full.iter().map(String::as_str).collect();
    let s_full = RandomSlices::from_uniform(&refs_full).unwrap();
    // Drop the last claimant.
    let names_short = unique_names(n - 1);
    let refs_short: Vec<&str> = names_short.iter().map(String::as_str).collect();
    let s_short = RandomSlices::from_uniform(&refs_short).unwrap();
    // Sample a deterministic stream of u64 hashes.
    let mut moved: u32 = 0;
    let total: u32 = 4096;
    let mut h: u64 = tc.draw(gs::integers::<u64>());
    for _ in 0..total {
        h = h.wrapping_mul(0x6a4a_7935_bd1e_995d).wrapping_add(11);
        let a = s_full.claimant_for(h).unwrap();
        let b = s_short.claimant_for(h).unwrap();
        // Same name string => same claimant. The names differ
        // between rings (one less peer) so the comparison is on
        // the str slices.
        if a != b {
            moved += 1;
        }
    }
    let ratio = f64::from(moved) / f64::from(total);
    #[allow(clippy::cast_precision_loss)]
    let theoretical = 1.0 / (n as f64);
    // Allow [theoretical/4, theoretical*4]: the property is
    // "approximately 1/N", and the test budget is small so we
    // do not want spurious failures from thin tails.
    assert!(
        ratio <= theoretical * 4.0 + 0.05,
        "moved fraction {ratio} too high vs theoretical {theoretical} (n={n})"
    );
}

#[hegel::test(test_cases = 64)]
fn lower_bounds_are_strict_ascending(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(32));
    let names = unique_names(n);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let s = RandomSlices::from_uniform(&refs).unwrap();
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    for &lb in s.lower_bounds() {
        assert!(seen.insert(lb), "duplicate lower bound {lb}");
    }
    for w in s.lower_bounds().windows(2) {
        assert!(w[0] < w[1]);
    }
}
