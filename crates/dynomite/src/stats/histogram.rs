//! Estimated histogram with logarithmic bucket layout.
//!
//! The bucket layout follows Cassandra's `EstimatedHistogram`: 94 buckets
//! whose offsets grow geometrically by a factor of 1.2, starting at 1.
//! Recording a value finds the bucket whose offset is the largest value
//! still less than or equal to the input.
//!
//! # Examples
//!
//! ```
//! use dynomite::stats::Histogram;
//!
//! let mut h = Histogram::new();
//! for v in 0..=100 {
//!     h.record(v);
//! }
//! assert_eq!(h.count(), 101);
//! assert!(h.percentile(0.5) <= h.percentile(0.95));
//! ```

use std::sync::OnceLock;

use crate::stats::numeric::{floor_p_times_u64, u64_to_f64};

/// Number of buckets in the estimated histogram.
///
/// # Examples
///
/// ```
/// use dynomite::stats::BUCKET_COUNT;
/// assert_eq!(BUCKET_COUNT, 94);
/// ```
pub const BUCKET_COUNT: usize = 94;

/// Returns the cached bucket offset table.
///
/// The first bucket starts at `1`. Each subsequent offset is
/// `floor(prev * 6 / 5)`, bumped by one if the multiplication did not
/// advance. This is the integer-equivalent of the original `* 1.2`
/// factor used by Cassandra's estimated histogram.
fn bucket_offsets() -> &'static [u64; BUCKET_COUNT] {
    static OFFSETS: OnceLock<[u64; BUCKET_COUNT]> = OnceLock::new();
    OFFSETS.get_or_init(|| {
        let mut offsets = [0u64; BUCKET_COUNT];
        let mut last: u64 = 1;
        offsets[0] = 1;
        for slot in offsets.iter_mut().skip(1) {
            let mut next = last.saturating_mul(6) / 5;
            if next == last {
                next += 1;
            }
            *slot = next;
            last = next;
        }
        offsets
    })
}

/// A fixed-bucket histogram for tracking latencies and payload sizes.
///
/// All operations are O(`BUCKET_COUNT`) and allocation free.
#[derive(Clone, Debug)]
pub struct Histogram {
    buckets: [u64; BUCKET_COUNT],
    val_max: u64,
}

impl Histogram {
    /// Construct a fresh histogram with all bucket counts set to zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let h = Histogram::new();
    /// assert_eq!(h.count(), 0);
    /// ```
    pub fn new() -> Self {
        Self {
            buckets: [0; BUCKET_COUNT],
            val_max: 0,
        }
    }

    /// Record a single observation, placing it in the appropriate bucket.
    ///
    /// Values larger than the largest bucket offset land in the final
    /// bucket and signal histogram overflow. Once the overflow bucket is
    /// non-empty, [`Histogram::percentile`], [`Histogram::mean`], and
    /// [`Histogram::max`] all return [`Histogram::OVERFLOW_SENTINEL`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(42);
    /// assert_eq!(h.count(), 1);
    /// assert_eq!(h.max(), 42);
    /// ```
    pub fn record(&mut self, val: u64) {
        let offsets = bucket_offsets();
        let next = offsets.partition_point(|&o| o <= val);
        let bucket = next.saturating_sub(1).min(BUCKET_COUNT - 1);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        if val > self.val_max {
            self.val_max = val;
        }
    }

    /// Sentinel value returned by [`Histogram::percentile`],
    /// [`Histogram::mean`], and [`Histogram::max`] when the overflow
    /// bucket is non-empty.
    pub const OVERFLOW_SENTINEL: u64 = u64::MAX;

    /// Returns `true` when the final (overflow) bucket is non-empty.
    ///
    /// The reference implementation logs an error and refuses to publish
    /// quantiles in this case; the Rust port surfaces the same signal
    /// through this method and through the [`Histogram::OVERFLOW_SENTINEL`]
    /// returned from quantile accessors.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(10);
    /// assert!(!h.is_overflowing());
    /// ```
    pub fn is_overflowing(&self) -> bool {
        self.buckets[BUCKET_COUNT - 1] > 0
    }

    /// Total number of observations recorded.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(1);
    /// h.record(2);
    /// assert_eq!(h.count(), 2);
    /// ```
    pub fn count(&self) -> u64 {
        self.buckets.iter().copied().fold(0u64, u64::saturating_add)
    }

    /// Approximate percentile, where `p` is in the closed interval
    /// `[0.0, 1.0]`. Inputs outside the range or NaN return `0`.
    ///
    /// The result is the offset of the first bucket whose cumulative
    /// count meets or exceeds `floor(count * p)`. Returns
    /// [`Histogram::OVERFLOW_SENTINEL`] if the histogram is
    /// [`Histogram::is_overflowing`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// for v in 0..1_000 { h.record(v); }
    /// assert!(h.percentile(0.95) >= h.percentile(0.5));
    /// ```
    pub fn percentile(&self, p: f64) -> u64 {
        if !p.is_finite() || !(0.0..=1.0).contains(&p) {
            return 0;
        }
        if self.is_overflowing() {
            return Self::OVERFLOW_SENTINEL;
        }
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let pcount = floor_p_times_u64(p, total);
        if pcount == 0 {
            return 0;
        }
        let offsets = bucket_offsets();
        let mut elements: u64 = 0;
        for (i, &offset) in offsets.iter().enumerate().take(BUCKET_COUNT - 1) {
            elements = elements.saturating_add(self.buckets[i]);
            if elements >= pcount {
                return offset;
            }
        }
        0
    }

    /// Arithmetic mean of all observations using bucket offsets as
    /// representative values. Returns `0.0` for an empty histogram and
    /// `f64::INFINITY` when the histogram is [`Histogram::is_overflowing`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(10);
    /// assert!(h.mean() > 0.0);
    /// ```
    pub fn mean(&self) -> f64 {
        if self.is_overflowing() {
            return f64::INFINITY;
        }
        let offsets = bucket_offsets();
        let mut sum: u64 = 0;
        let mut elements: u64 = 0;
        for (i, &offset) in offsets.iter().enumerate().take(BUCKET_COUNT - 1) {
            elements = elements.saturating_add(self.buckets[i]);
            sum = sum.saturating_add(self.buckets[i].saturating_mul(offset));
        }
        if elements == 0 {
            return 0.0;
        }
        let quotient = sum / elements;
        let remainder = sum % elements;
        u64_to_f64(quotient) + u64_to_f64(remainder) / u64_to_f64(elements)
    }

    /// Maximum observation seen since the last [`Histogram::reset`].
    /// Returns [`Histogram::OVERFLOW_SENTINEL`] when the histogram is
    /// [`Histogram::is_overflowing`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(99);
    /// assert_eq!(h.max(), 99);
    /// ```
    pub fn max(&self) -> u64 {
        if self.is_overflowing() {
            return Self::OVERFLOW_SENTINEL;
        }
        self.val_max
    }

    /// Reset all bucket counts and the recorded maximum to zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(7);
    /// h.reset();
    /// assert_eq!(h.count(), 0);
    /// ```
    pub fn reset(&mut self) {
        self.buckets = [0; BUCKET_COUNT];
        self.val_max = 0;
    }

    /// Merge bucket counts from another histogram into this one.
    ///
    /// The maximum is updated to the larger of the two recorded maxima.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut a = Histogram::new();
    /// let mut b = Histogram::new();
    /// a.record(10);
    /// b.record(20);
    /// a.merge(&b);
    /// assert_eq!(a.count(), 2);
    /// assert_eq!(a.max(), 20);
    /// ```
    pub fn merge(&mut self, other: &Self) {
        for i in 0..BUCKET_COUNT {
            self.buckets[i] = self.buckets[i].saturating_add(other.buckets[i]);
        }
        if other.val_max > self.val_max {
            self.val_max = other.val_max;
        }
    }

    /// Borrow the raw bucket counts.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::{Histogram, BUCKET_COUNT};
    /// let h = Histogram::new();
    /// assert_eq!(h.buckets().len(), BUCKET_COUNT);
    /// ```
    pub fn buckets(&self) -> &[u64; BUCKET_COUNT] {
        &self.buckets
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_are_strictly_increasing() {
        let off = bucket_offsets();
        for window in off.windows(2) {
            assert!(window[0] < window[1], "offsets must be strictly monotonic");
        }
        assert_eq!(off[0], 1);
    }

    #[test]
    fn empty_histogram_returns_zeros() {
        let h = Histogram::new();
        assert_eq!(h.count(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.percentile(0.5), 0);
        assert!(h.mean().abs() < f64::EPSILON);
    }

    #[test]
    fn record_updates_count_and_max() {
        let mut h = Histogram::new();
        h.record(0);
        h.record(5);
        h.record(1_000);
        assert_eq!(h.count(), 3);
        assert_eq!(h.max(), 1_000);
    }

    #[test]
    fn percentile_is_monotone_non_decreasing() {
        let mut h = Histogram::new();
        for v in 0..1_000 {
            h.record(v);
        }
        let mut last = 0u64;
        let mut p = 0.0f64;
        while p <= 1.0 {
            let v = h.percentile(p);
            assert!(v >= last, "percentile decreased at p={p}: {v} < {last}");
            last = v;
            p += 0.05;
        }
    }

    #[test]
    fn merge_sums_counts() {
        let mut a = Histogram::new();
        let mut b = Histogram::new();
        for v in 0..100 {
            a.record(v);
            b.record(v + 50);
        }
        let total = a.count() + b.count();
        a.merge(&b);
        assert_eq!(a.count(), total);
    }

    #[test]
    fn reset_clears_state() {
        let mut h = Histogram::new();
        h.record(123);
        h.reset();
        assert_eq!(h.count(), 0);
        assert_eq!(h.max(), 0);
    }

    #[test]
    fn percentile_outside_range_returns_zero() {
        let mut h = Histogram::new();
        h.record(10);
        assert_eq!(h.percentile(-0.1), 0);
        assert_eq!(h.percentile(1.1), 0);
        assert_eq!(h.percentile(f64::NAN), 0);
    }

    #[test]
    fn overflow_signals_quantile_callers() {
        let mut h = Histogram::new();
        // Record a value larger than every bucket offset.
        let last_offset = *bucket_offsets().last().expect("non-empty offsets");
        h.record(last_offset.saturating_add(1));
        assert!(h.is_overflowing(), "expected overflow signal");
        assert_eq!(h.percentile(0.5), Histogram::OVERFLOW_SENTINEL);
        assert_eq!(h.max(), Histogram::OVERFLOW_SENTINEL);
        assert!(h.mean().is_infinite());
        // After reset the overflow signal clears.
        h.reset();
        assert!(!h.is_overflowing());
    }

    #[test]
    fn mean_rough_match_for_uniform() {
        let mut h = Histogram::new();
        for v in 0..1000u64 {
            h.record(v);
        }
        // True mean is 499.5; bucket-quantization makes the result
        // approximate but within an order of magnitude.
        let m = h.mean();
        assert!((100.0..=1500.0).contains(&m), "mean was {m}");
    }
}
