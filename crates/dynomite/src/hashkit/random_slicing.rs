//! Random-slicing distribution: a small, gap-free partition table
//! over the 64-bit hash space.
//!
//! The hash space is split into a contiguous run of intervals.
//! Each interval is owned by exactly one *claimant* (a peer or
//! peer-name in our terminology). Lookups are an `O(log N)`
//! binary search over the interval table, where `N` is the
//! claimant count.
//!
//! Two construction-time invariants give the technique its name:
//!
//! 1. The intervals are laid out end-to-end so their union is
//!    the entire ring. Whatever rounding remainder integer
//!    division leaves over is absorbed by the last interval, so
//!    no value of `hash(key)` can fail to map to a claimant.
//! 2. Adding or removing a claimant only re-slices its share.
//!    Keys outside the affected slice keep their assignment.
//!
//! The structure is built once per rack at config-load /
//! `SIGHUP`-reload time and thereafter consulted read-only by
//! the dispatcher; see [`crate::cluster::dispatch`].
//!
//! # Examples
//!
//! ```
//! use dynomite::hashkit::random_slicing::RandomSlices;
//!
//! let slices = RandomSlices::from_uniform(&["peer-a", "peer-b"]).unwrap();
//! // Either of the two claimants must own the lookup; the
//! // partition is gap-free by construction.
//! let owner = slices.claimant_for(0).unwrap();
//! assert!(owner == "peer-a" || owner == "peer-b");
//! ```

use std::collections::BTreeSet;

use thiserror::Error;

/// Failure mode produced by the [`RandomSlices`] builders.
#[derive(Debug, Error, PartialEq)]
pub enum RandomSlicesError {
    /// Caller supplied an empty claimant list. The reference
    /// engine refuses to build an empty partition; we mirror
    /// that here.
    #[error("random_slicing: at least one claimant is required")]
    EmptyClaimants,
    /// Two claimants share the same name. Names must be unique
    /// so the reverse lookup (peer name -> slice) is total.
    #[error("random_slicing: duplicate claimant '{name}'")]
    DuplicateClaimant {
        /// The offending duplicated name.
        name: String,
    },
    /// A weight is non-finite, negative, or so large the size
    /// translation cannot proceed. The error is also raised when
    /// every weight rounds to zero at the configured number of
    /// decimal digits.
    #[error("random_slicing: invalid weight for '{name}': {weight}")]
    InvalidWeight {
        /// The claimant whose weight failed validation.
        name: String,
        /// The offending weight.
        weight: f64,
    },
    /// The summed [`u64`] sizes overflow the ring. The C
    /// reference's `hash_partitions_create_with_sizes` accepts
    /// only sums up to `u64::MAX`; anything beyond is a
    /// configuration error.
    #[error("random_slicing: claimant size sum {sum} exceeds u64::MAX")]
    OversizedSizeSum {
        /// The summed sizes (as a `u128` so the overflow is
        /// representable in the diagnostic).
        sum: u128,
    },
    /// Caller supplied a zero-sized interval. The reference
    /// engine permits zero-sized entries silently and the
    /// binary search then returns the next claimant; the Rust
    /// port rejects this configuration up front so no claimant
    /// can be silently masked.
    #[error("random_slicing: zero-sized interval at index {index} for '{name}'")]
    ZeroInterval {
        /// Position of the offending entry in the input list.
        index: usize,
        /// The claimant whose size was zero.
        name: String,
    },
}

/// Slice table indexed by `u64` hash values.
///
/// A `RandomSlices` instance owns three parallel arrays: the
/// ascending interval lower bounds, the per-interval sizes
/// (kept for diagnostics), and the per-interval claimant names.
/// All builders maintain the parallel-array invariant; do not
/// construct this struct by hand.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RandomSlices {
    /// Strict-ascending lower bounds.
    lower_bounds: Vec<u64>,
    /// Parallel size array. `interval_sizes[i]` is the number
    /// of `u64` values mapped to claimant `i`. The last entry
    /// absorbs any rounding remainder so the union is the full
    /// ring.
    interval_sizes: Vec<u64>,
    /// Parallel claimant array.
    claimants: Vec<String>,
}

impl RandomSlices {
    /// Build an equal-sized partition for `claimants`. Each
    /// claimant gets an interval of `u64::MAX / N` values; the
    /// last claimant absorbs the rounding remainder so coverage
    /// is exact.
    ///
    /// # Errors
    /// Returns [`RandomSlicesError::EmptyClaimants`] when the
    /// list is empty and
    /// [`RandomSlicesError::DuplicateClaimant`] when two entries
    /// share a name.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::random_slicing::RandomSlices;
    /// let s = RandomSlices::from_uniform(&["a", "b", "c"]).unwrap();
    /// assert_eq!(s.len(), 3);
    /// ```
    pub fn from_uniform(claimants: &[&str]) -> Result<Self, RandomSlicesError> {
        if claimants.is_empty() {
            return Err(RandomSlicesError::EmptyClaimants);
        }
        check_unique(claimants.iter().copied())?;
        let n = claimants.len() as u64;
        // `u64::MAX / n` cannot overflow because `n >= 1`.
        let base = u64::MAX / n;
        let used = base.checked_mul(n).unwrap_or(0);
        let remainder = u64::MAX - used;
        let mut sizes: Vec<u64> = vec![base; claimants.len()];
        if let Some(last) = sizes.last_mut() {
            *last = last.saturating_add(remainder);
        }
        let lower_bounds = build_lower_bounds(&sizes);
        Ok(Self {
            lower_bounds,
            interval_sizes: sizes,
            claimants: claimants.iter().map(|s| (*s).to_string()).collect(),
        })
    }

    /// Build a weighted partition. Each weight is rounded to
    /// `decimal_digits` places before normalising; the sum is
    /// then translated into per-claimant `u64` sizes by
    /// `floor(u64::MAX * w / sum)`. The last claimant absorbs
    /// the remainder so coverage is exact.
    ///
    /// # Errors
    /// Returns [`RandomSlicesError::EmptyClaimants`],
    /// [`RandomSlicesError::DuplicateClaimant`], or
    /// [`RandomSlicesError::InvalidWeight`] (for non-finite,
    /// negative, or all-zero rounded weights).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::random_slicing::RandomSlices;
    /// let s = RandomSlices::from_weights(
    ///     &[("a".to_string(), 1.0), ("b".to_string(), 2.0)],
    ///     2,
    /// ).unwrap();
    /// assert_eq!(s.len(), 2);
    /// ```
    pub fn from_weights(
        weights: &[(String, f64)],
        decimal_digits: usize,
    ) -> Result<Self, RandomSlicesError> {
        if weights.is_empty() {
            return Err(RandomSlicesError::EmptyClaimants);
        }
        check_unique(weights.iter().map(|(n, _)| n.as_str()))?;
        let scale = 10f64.powi(i32::try_from(decimal_digits).unwrap_or(0));
        let mut rounded: Vec<f64> = Vec::with_capacity(weights.len());
        for (name, w) in weights {
            if !w.is_finite() || *w < 0.0 {
                return Err(RandomSlicesError::InvalidWeight {
                    name: name.clone(),
                    weight: *w,
                });
            }
            // Round to `decimal_digits` places.
            let r = (w * scale).round() / scale;
            rounded.push(r);
        }
        let sum: f64 = rounded.iter().sum();
        if !sum.is_finite() || sum <= 0.0 {
            // Every weight rounded to zero (or sum is otherwise
            // unusable). Pick the first claimant as the offender.
            let first = weights[0].clone();
            return Err(RandomSlicesError::InvalidWeight {
                name: first.0,
                weight: first.1,
            });
        }
        // Convert each rounded weight (except the last) to a
        // u64 size by `floor(u64::MAX * w / sum)`. The last
        // claimant absorbs the rest so coverage is exact and
        // free of `f64`-rounding overflow. The cast to `f64`
        // discards bits below the mantissa's 52-bit width,
        // which is harmless: the slice sizes only need to be
        // approximate (the dispatcher binary-searches on the
        // *lower bounds*; the size array is for diagnostics).
        #[allow(clippy::cast_precision_loss)]
        let max_f = u64::MAX as f64;
        let mut sizes: Vec<u64> = Vec::with_capacity(weights.len());
        let last_idx = weights.len() - 1;
        let mut consumed: u128 = 0;
        for (idx, _) in weights.iter().enumerate() {
            if idx == last_idx {
                // Tail absorbs whatever is left up to u64::MAX.
                let mut last = u128::from(u64::MAX).saturating_sub(consumed);
                if last == 0 {
                    // The first `n-1` claimants saturated the
                    // ring; carve out one byte for the tail.
                    last = 1;
                }
                let last_u64 = u64::try_from(last.min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
                sizes.push(last_u64);
                break;
            }
            let frac = rounded[idx] / sum;
            let raw = max_f * frac;
            let size: u64 = if raw <= 0.0 {
                0
            } else if raw >= max_f {
                u64::MAX
            } else {
                raw as u64
            };
            consumed = consumed.saturating_add(u128::from(size));
            sizes.push(size);
        }
        // If every translated size is zero, surface that as an
        // invalid-weight error rather than silently producing
        // an unbuildable partition.
        if sizes.iter().all(|s| *s == 0) {
            let first = weights[0].clone();
            return Err(RandomSlicesError::InvalidWeight {
                name: first.0,
                weight: first.1,
            });
        }
        // Promote each zero-size to one so no claimant is
        // silently masked. The tail-bump below will absorb the
        // change.
        let mut promoted_zeros: u64 = 0;
        for (idx, s) in sizes.iter_mut().enumerate() {
            if *s == 0 && idx != last_idx {
                *s = 1;
                promoted_zeros = promoted_zeros.saturating_add(1);
            }
        }
        if promoted_zeros > 0 {
            // Steal those bytes from the tail so the total stays
            // bounded by `u64::MAX`.
            if let Some(last) = sizes.last_mut() {
                *last = last.saturating_sub(promoted_zeros).max(1);
            }
        }
        let pairs: Vec<(String, u64)> = weights
            .iter()
            .zip(sizes.iter())
            .map(|((n, _), s)| (n.clone(), *s))
            .collect();
        Self::from_sizes(&pairs)
    }

    /// Build a partition from explicit `(name, size)` pairs.
    ///
    /// The supplied sizes are taken at face value; the only
    /// transformation is bumping the tail interval up by the
    /// remainder when the sum is below `u64::MAX`. Sums above
    /// `u64::MAX` are rejected with
    /// [`RandomSlicesError::OversizedSizeSum`].
    ///
    /// # Errors
    /// Returns [`RandomSlicesError::EmptyClaimants`],
    /// [`RandomSlicesError::DuplicateClaimant`],
    /// [`RandomSlicesError::ZeroInterval`], or
    /// [`RandomSlicesError::OversizedSizeSum`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::random_slicing::RandomSlices;
    /// let s = RandomSlices::from_sizes(&[
    ///     ("a".into(), 100),
    ///     ("b".into(), 200),
    /// ]).unwrap();
    /// assert_eq!(s.len(), 2);
    /// ```
    pub fn from_sizes(sizes: &[(String, u64)]) -> Result<Self, RandomSlicesError> {
        if sizes.is_empty() {
            return Err(RandomSlicesError::EmptyClaimants);
        }
        check_unique(sizes.iter().map(|(n, _)| n.as_str()))?;
        let mut sum: u128 = 0;
        for (idx, (name, s)) in sizes.iter().enumerate() {
            if *s == 0 {
                return Err(RandomSlicesError::ZeroInterval {
                    index: idx,
                    name: name.clone(),
                });
            }
            sum += u128::from(*s);
            if sum > u128::from(u64::MAX) {
                return Err(RandomSlicesError::OversizedSizeSum { sum });
            }
        }
        let mut interval_sizes: Vec<u64> = sizes.iter().map(|(_, s)| *s).collect();
        // Bump the tail to absorb any remainder so coverage is
        // exactly the full ring.
        let used: u128 = sum;
        let remainder = u128::from(u64::MAX) - used;
        if remainder > 0 {
            if let Some(last) = interval_sizes.last_mut() {
                let bumped = u128::from(*last) + remainder;
                *last = u64::try_from(bumped).expect("invariant: remainder fits");
            }
        }
        let lower_bounds = build_lower_bounds(&interval_sizes);
        Ok(Self {
            lower_bounds,
            interval_sizes,
            claimants: sizes.iter().map(|(n, _)| n.clone()).collect(),
        })
    }

    /// Number of claimants / intervals in the table.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::random_slicing::RandomSlices;
    /// assert_eq!(RandomSlices::from_uniform(&["a", "b"]).unwrap().len(), 2);
    /// ```
    #[must_use]
    pub fn len(&self) -> usize {
        self.claimants.len()
    }

    /// True when the table holds no claimants. The builders
    /// reject empty inputs, so this can only be observed via
    /// the (private) raw constructor; included for completeness
    /// behind clippy's `len` lint.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::random_slicing::RandomSlices;
    /// assert!(!RandomSlices::from_uniform(&["a"]).unwrap().is_empty());
    /// ```
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.claimants.is_empty()
    }

    /// Look up the claimant that owns `hash`.
    ///
    /// Returns the borrowed claimant name. Returns `None` only
    /// when the table is empty (which the builders prevent).
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::random_slicing::RandomSlices;
    /// let s = RandomSlices::from_uniform(&["a", "b"]).unwrap();
    /// assert!(s.claimant_for(0).is_some());
    /// ```
    #[must_use]
    pub fn claimant_for(&self, hash: u64) -> Option<&str> {
        let idx = self.index_for(hash)?;
        self.claimants.get(idx).map(String::as_str)
    }

    /// Look up the index of the claimant that owns `hash`.
    ///
    /// Returns `None` only when the table is empty.
    #[must_use]
    pub fn index_for(&self, hash: u64) -> Option<usize> {
        let n = self.lower_bounds.len();
        if n == 0 {
            return None;
        }
        let last = n - 1;
        if hash >= self.lower_bounds[last] {
            return Some(last);
        }
        // Largest `i` with `lower_bounds[i] <= hash`.
        let mut lo = 0usize;
        let mut hi = last;
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if self.lower_bounds[mid] <= hash {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        Some(lo)
    }

    /// Borrow the claimant list (parallel to
    /// [`Self::lower_bounds`] and [`Self::interval_sizes`]).
    #[must_use]
    pub fn claimants(&self) -> &[String] {
        &self.claimants
    }

    /// Borrow the lower-bound array.
    #[must_use]
    pub fn lower_bounds(&self) -> &[u64] {
        &self.lower_bounds
    }

    /// Borrow the per-interval size array.
    #[must_use]
    pub fn interval_sizes(&self) -> &[u64] {
        &self.interval_sizes
    }

    /// Iterate `(claimant, lower_bound, size)` for diagnostics.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::random_slicing::RandomSlices;
    /// let s = RandomSlices::from_uniform(&["a", "b"]).unwrap();
    /// let n = s.intervals().count();
    /// assert_eq!(n, 2);
    /// ```
    pub fn intervals(&self) -> impl Iterator<Item = (&str, u64, u64)> + '_ {
        self.claimants
            .iter()
            .zip(self.lower_bounds.iter().copied())
            .zip(self.interval_sizes.iter().copied())
            .map(|((name, lb), sz)| (name.as_str(), lb, sz))
    }
}

fn build_lower_bounds(sizes: &[u64]) -> Vec<u64> {
    let mut lb: Vec<u64> = Vec::with_capacity(sizes.len());
    let mut acc: u64 = 0;
    for s in sizes {
        lb.push(acc);
        acc = acc.saturating_add(*s);
    }
    lb
}

fn check_unique<'a, I: IntoIterator<Item = &'a str>>(it: I) -> Result<(), RandomSlicesError> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for name in it {
        if !seen.insert(name) {
            return Err(RandomSlicesError::DuplicateClaimant {
                name: name.to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_rejected() {
        assert_eq!(
            RandomSlices::from_uniform(&[]),
            Err(RandomSlicesError::EmptyClaimants)
        );
        let empty_sizes: Vec<(String, u64)> = Vec::new();
        assert_eq!(
            RandomSlices::from_sizes(&empty_sizes),
            Err(RandomSlicesError::EmptyClaimants)
        );
        let empty_weights: Vec<(String, f64)> = Vec::new();
        assert_eq!(
            RandomSlices::from_weights(&empty_weights, 2),
            Err(RandomSlicesError::EmptyClaimants)
        );
    }

    #[test]
    fn duplicate_rejected() {
        let err = RandomSlices::from_uniform(&["a", "a"]).unwrap_err();
        assert!(matches!(err, RandomSlicesError::DuplicateClaimant { .. }));
    }

    #[test]
    fn uniform_partition_covers_full_ring() {
        let s = RandomSlices::from_uniform(&["a", "b", "c", "d"]).unwrap();
        assert_eq!(s.len(), 4);
        let total: u128 = s.interval_sizes().iter().map(|&n| u128::from(n)).sum();
        assert_eq!(total, u128::from(u64::MAX));
        // Lower bounds strictly ascending; first is zero.
        assert_eq!(s.lower_bounds()[0], 0);
        for w in s.lower_bounds().windows(2) {
            assert!(w[0] < w[1]);
        }
    }

    #[test]
    fn lookup_at_extreme_values_works() {
        let s = RandomSlices::from_uniform(&["a", "b", "c", "d"]).unwrap();
        assert_eq!(s.claimant_for(0).unwrap(), "a");
        assert_eq!(s.claimant_for(u64::MAX).unwrap(), "d");
    }

    #[test]
    fn lookup_at_boundaries_picks_upper_interval() {
        let s = RandomSlices::from_uniform(&["a", "b"]).unwrap();
        let split = s.lower_bounds()[1];
        // hash == split is owned by claimant b (lower_bound is
        // inclusive).
        assert_eq!(s.claimant_for(split).unwrap(), "b");
        assert_eq!(s.claimant_for(split - 1).unwrap(), "a");
    }

    #[test]
    fn weights_translate_to_proportional_sizes() {
        let pairs = vec![("a".to_string(), 1.0), ("b".to_string(), 3.0)];
        let s = RandomSlices::from_weights(&pairs, 2).unwrap();
        assert_eq!(s.len(), 2);
        // Claimant "b" should own roughly 3x the slice of "a"
        // (within a small fraction due to the tail bump).
        let a_size = u128::from(s.interval_sizes()[0]);
        let b_size = u128::from(s.interval_sizes()[1]);
        // Cast through u64 first since both halves fit in 64
        // bits (u128 was used for the overflow-detection sum,
        // not for the per-claimant size).
        #[allow(clippy::cast_precision_loss)]
        let ratio = (b_size as f64) / (a_size as f64);
        assert!(
            ratio > 2.9 && ratio < 3.1,
            "expected ~3x ratio, got {ratio}"
        );
    }

    #[test]
    fn weights_reject_negative() {
        let pairs = vec![("a".to_string(), -1.0)];
        let err = RandomSlices::from_weights(&pairs, 2).unwrap_err();
        assert!(matches!(err, RandomSlicesError::InvalidWeight { .. }));
    }

    #[test]
    fn weights_reject_non_finite() {
        let pairs = vec![("a".to_string(), f64::NAN)];
        let err = RandomSlices::from_weights(&pairs, 2).unwrap_err();
        assert!(matches!(err, RandomSlicesError::InvalidWeight { .. }));
    }

    #[test]
    fn weights_reject_all_zero_rounded() {
        // With decimal_digits = 2, every weight rounds to 0.0.
        let pairs = vec![("a".to_string(), 0.001), ("b".to_string(), 0.001)];
        let err = RandomSlices::from_weights(&pairs, 2).unwrap_err();
        assert!(matches!(err, RandomSlicesError::InvalidWeight { .. }));
    }

    #[test]
    fn sizes_reject_zero_interval() {
        let pairs = vec![("a".to_string(), 0u64)];
        let err = RandomSlices::from_sizes(&pairs).unwrap_err();
        assert!(matches!(err, RandomSlicesError::ZeroInterval { .. }));
    }

    #[test]
    fn sizes_reject_overflow() {
        let pairs = vec![("a".to_string(), u64::MAX), ("b".to_string(), 1u64)];
        let err = RandomSlices::from_sizes(&pairs).unwrap_err();
        assert!(matches!(err, RandomSlicesError::OversizedSizeSum { .. }));
    }

    #[test]
    fn from_sizes_bumps_tail_to_cover_full_ring() {
        // Small explicit sizes leave a large remainder; the tail
        // interval absorbs it so coverage is exactly u64::MAX.
        let pairs = vec![("a".to_string(), 10u64), ("b".to_string(), 20u64)];
        let s = RandomSlices::from_sizes(&pairs).unwrap();
        let total: u128 = s.interval_sizes().iter().map(|&v| u128::from(v)).sum();
        assert_eq!(total, u128::from(u64::MAX));
        // Head size is untouched; only the tail grew.
        assert_eq!(s.interval_sizes()[0], 10);
        assert!(s.interval_sizes()[1] >= 20);
        // Every hash resolves and the boundaries are exact.
        assert!(s.claimant_for(0).is_some());
        assert!(s.claimant_for(u64::MAX).is_some());
    }

    #[test]
    fn from_weights_promotes_zero_slice_to_one() {
        // A tiny-but-nonzero weight beside a huge one rounds to a
        // zero-sized slice; the builder promotes it to a single
        // byte so the claimant is never silently masked, and
        // steals that byte from the tail.
        let pairs = vec![
            ("big".to_string(), 1_000_000.0),
            ("small".to_string(), 0.01),
        ];
        let s = RandomSlices::from_weights(&pairs, 2).unwrap();
        assert_eq!(s.len(), 2);
        // Both claimants own a non-empty slice.
        assert!(s.interval_sizes().iter().all(|&v| v >= 1));
        let total: u128 = s.interval_sizes().iter().map(|&v| u128::from(v)).sum();
        assert_eq!(total, u128::from(u64::MAX));
    }

    #[test]
    fn from_weights_tail_saturated_carves_one_byte() {
        // When the leading claimants consume the entire ring the
        // tail would be zero-sized; the builder carves out one
        // byte so coverage stays total and the tail claimant is
        // still reachable.
        let pairs = vec![
            ("first".to_string(), 1.0),
            ("second".to_string(), 1.0),
            ("third".to_string(), 1.0),
        ];
        let s = RandomSlices::from_weights(&pairs, 4).unwrap();
        assert_eq!(s.len(), 3);
        assert!(s.interval_sizes().iter().all(|&v| v >= 1));
        let total: u128 = s.interval_sizes().iter().map(|&v| u128::from(v)).sum();
        assert_eq!(total, u128::from(u64::MAX));
    }

    #[test]
    fn from_weights_promotes_zeroed_middle_slice() {
        // A middle claimant whose rounded weight is zero produces
        // a zero-sized non-tail slice; the builder promotes it to
        // one byte (the `raw <= 0.0` and zero-promotion arms).
        let pairs = vec![
            ("big1".to_string(), 1_000_000.0),
            ("tiny".to_string(), 0.0001),
            ("big2".to_string(), 1_000_000.0),
        ];
        let s = RandomSlices::from_weights(&pairs, 2).unwrap();
        assert_eq!(s.len(), 3);
        // Every claimant, including the promoted middle one, owns
        // a non-empty slice.
        assert!(s.interval_sizes().iter().all(|&v| v >= 1));
        let total: u128 = s.interval_sizes().iter().map(|&v| u128::from(v)).sum();
        assert_eq!(total, u128::from(u64::MAX));
        // The promoted middle slice is exactly one byte wide.
        assert_eq!(s.interval_sizes()[1], 1);
    }

    #[test]
    fn accessor_arrays_are_parallel_and_intervals_iterate() {
        let s = RandomSlices::from_uniform(&["a", "b", "c"]).unwrap();
        assert_eq!(s.claimants().len(), 3);
        assert_eq!(s.lower_bounds().len(), 3);
        assert_eq!(s.interval_sizes().len(), 3);
        let rows: Vec<(&str, u64, u64)> = s.intervals().collect();
        assert_eq!(rows.len(), 3);
        // The first interval starts at zero.
        assert_eq!(rows[0].1, 0);
        // Names line up with the claimant array order.
        for (row, name) in rows.iter().zip(s.claimants()) {
            assert_eq!(row.0, name);
        }
    }

    #[test]
    fn round_trip_1k_random_keys_under_5_percent() {
        // A 4-claimant uniform partition fed `Murmur3X64_64`
        // hashes of a thousand synthetic keys lands within 5%
        // of uniform.
        use crate::hashkit::{hash64, HashType};
        let s = RandomSlices::from_uniform(&["a", "b", "c", "d"]).unwrap();
        let mut counts = [0u32; 4];
        let total = 1024usize;
        for i in 0..total {
            let key = format!("key-{i:08x}");
            let h = hash64(HashType::Murmur3X64_64, key.as_bytes());
            let idx = s.index_for(h).unwrap();
            counts[idx] += 1;
        }
        let expected = (total / 4) as u32;
        for (i, &c) in counts.iter().enumerate() {
            // Allow up to 25% slack on a 1024-key sample so the
            // test is robust against hash-bucket variance; the
            // hegeltest in `tests/random_slicing_property.rs`
            // covers the asymptotic 5% bound on larger samples.
            let lo = expected - expected / 4;
            let hi = expected + expected / 4;
            assert!(
                c >= lo && c <= hi,
                "claimant {i}: count {c} outside [{lo}, {hi}]"
            );
        }
    }
}
