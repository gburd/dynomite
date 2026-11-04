//! Estimated histogram with logarithmic bucket layout.
//!
//! The bucket layout follows Cassandra's `EstimatedHistogram`: 94 buckets
//! whose offsets grow geometrically by a factor of 1.2, starting at 1.
//! [`Histogram::record`] counts an observation in the bucket a bisection
//! over the offsets selects; [`Histogram::compute`] publishes the mean and
//! the 95th, 99th, and 99.9th percentiles from the bucket counts, and the
//! accessors return what the last compute published.
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
//! h.compute();
//! assert_eq!(h.count(), 101);
//! assert!(h.val_95th() <= h.val_99th());
//! ```

use std::sync::OnceLock;

use crate::stats::numeric::{ceil_f64_to_u64, floor_p_times_u64, u64_to_f64};

/// Number of buckets in the estimated histogram.
///
/// # Examples
///
/// ```
/// use dynomite::stats::BUCKET_COUNT;
/// assert_eq!(BUCKET_COUNT, 94);
/// ```
pub const BUCKET_COUNT: usize = 94;

/// The bucket offset table shared by every histogram.
///
/// The first offset is `1`. Each later offset is `floor(prev * 1.2)`
/// evaluated in `f64`, bumped by one when the product does not advance.
///
/// # Examples
///
/// ```
/// use dynomite::stats::bucket_offsets;
/// let off = bucket_offsets();
/// assert_eq!(&off[..12], &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14]);
/// assert_eq!(off[93], 36_157_192);
/// ```
pub fn bucket_offsets() -> &'static [u64; BUCKET_COUNT] {
    static OFFSETS: OnceLock<[u64; BUCKET_COUNT]> = OnceLock::new();
    OFFSETS.get_or_init(|| {
        let mut offsets = [0u64; BUCKET_COUNT];
        let mut last: u64 = 1;
        offsets[0] = last;
        for slot in offsets.iter_mut().skip(1) {
            let mut next = floor_p_times_u64(1.2, last);
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
    mean: u64,
    val_95th: u64,
    val_99th: u64,
    val_999th: u64,
    val_max: u64,
}

impl Histogram {
    /// Construct a fresh histogram with every count and published value
    /// set to zero.
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
            mean: 0,
            val_95th: 0,
            val_99th: 0,
            val_999th: 0,
            val_max: 0,
        }
    }

    /// Record a single observation.
    ///
    /// The bucket is chosen by bisecting the offset table. Values below
    /// the second offset land in bucket 0; values at or beyond the
    /// second-to-last offset land in bucket `BUCKET_COUNT - 2`, so the
    /// final bucket never fills. The recorded maximum tracks the raw
    /// value.
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
        let mut left = 0usize;
        let mut right = BUCKET_COUNT - 1;
        let mut index = 0usize;

        while left < right {
            let middle = left + (right - left) / 2;
            match val.cmp(&offsets[middle]) {
                std::cmp::Ordering::Equal => {
                    index = middle;
                    break;
                }
                std::cmp::Ordering::Less => right = middle,
                std::cmp::Ordering::Greater => left = middle,
            }
            if left == right - 1 {
                index = left;
                break;
            }
        }

        if left == right - 1 {
            index = left;
        }

        self.buckets[index] = self.buckets[index].wrapping_add(1);
        self.val_max = self.val_max.max(val);
    }

    /// Publish the mean and the 95th, 99th, and 99.9th percentiles from
    /// the current bucket counts.
    ///
    /// Each percentile is the offset of the first bucket whose cumulative
    /// count reaches `floor(count * p)`; with fewer observations than one
    /// percentile step that threshold is zero, so the first offset (`1`)
    /// is published. The mean is `ceil(sum / count)` over bucket offsets,
    /// left unchanged when there are no observations. A non-empty final
    /// bucket logs an error and publishes nothing.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.compute();
    /// assert_eq!((h.mean(), h.val_99th()), (0, 1));
    /// for _ in 0..990 { h.record(10); }
    /// for _ in 0..10 { h.record(1_000); }
    /// h.compute();
    /// assert_eq!((h.val_99th(), h.val_999th()), (10, 926));
    /// assert_eq!(h.mean(), 20);
    /// ```
    pub fn compute(&mut self) {
        let last_bucket = BUCKET_COUNT - 1;
        if self.buckets[last_bucket] > 0 {
            tracing::error!("histogram overflowed!");
            return;
        }

        // 95th, 99th, and 99.9th: the threshold count and the offset
        // published for each.
        let pcounts = [0.95, 0.99, 0.999].map(|p| floor_p_times_u64(p, self.count()));
        let mut vals = [0u64; 3];

        let mut elements: u64 = 0;
        let mut sum: u64 = 0;
        let offsets = bucket_offsets();
        for (&n, &offset) in self.buckets.iter().zip(offsets).take(last_bucket) {
            elements = elements.wrapping_add(n);
            for (val, &pcount) in vals.iter_mut().zip(&pcounts) {
                if elements >= pcount && *val == 0 {
                    *val = offset;
                }
            }
            sum = sum.wrapping_add(n.wrapping_mul(offset));
        }

        if elements != 0 {
            self.mean = ceil_f64_to_u64(u64_to_f64(sum) / u64_to_f64(elements));
        }

        [self.val_95th, self.val_99th, self.val_999th] = vals;
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
        self.buckets.iter().copied().fold(0u64, u64::wrapping_add)
    }

    /// Mean published by the last [`Histogram::compute`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(10);
    /// h.compute();
    /// assert_eq!(h.mean(), 10);
    /// ```
    pub fn mean(&self) -> u64 {
        self.mean
    }

    /// 95th percentile published by the last [`Histogram::compute`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// for v in 0..1_000 { h.record(v); }
    /// h.compute();
    /// assert_eq!(h.val_95th(), 926);
    /// ```
    pub fn val_95th(&self) -> u64 {
        self.val_95th
    }

    /// 99th percentile published by the last [`Histogram::compute`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// for v in 0..1_000 { h.record(v); }
    /// h.compute();
    /// assert_eq!(h.val_99th(), 926);
    /// ```
    pub fn val_99th(&self) -> u64 {
        self.val_99th
    }

    /// 99.9th percentile published by the last [`Histogram::compute`].
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// for v in 0..1_000 { h.record(v); }
    /// h.compute();
    /// assert_eq!(h.val_999th(), 926);
    /// ```
    pub fn val_999th(&self) -> u64 {
        self.val_999th
    }

    /// Maximum observation seen since the last [`Histogram::reset`].
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
        self.val_max
    }

    /// Reset every bucket count and published value to zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::stats::Histogram;
    /// let mut h = Histogram::new();
    /// h.record(7);
    /// h.compute();
    /// h.reset();
    /// assert_eq!((h.count(), h.mean(), h.max()), (0, 0, 0));
    /// ```
    pub fn reset(&mut self) {
        *self = Self::new();
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
        assert_eq!(off[BUCKET_COUNT - 2], 30_130_994);
    }

    #[test]
    fn empty_histogram_computes_first_offset_percentiles() {
        let mut h = Histogram::new();
        h.compute();
        assert_eq!(h.count(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.mean(), 0);
        assert_eq!((h.val_95th(), h.val_99th(), h.val_999th()), (1, 1, 1));
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
    fn record_selects_largest_offset_not_above_value() {
        let off = bucket_offsets();
        let mut h = Histogram::new();
        for v in [0, 1, 11, 12, 13] {
            h.record(v);
        }
        assert_eq!(h.buckets()[0], 2);
        assert_eq!(h.buckets()[9], 1);
        assert_eq!(h.buckets()[10], 2);
        assert_eq!(off[10], 12);
    }

    #[test]
    fn values_past_the_table_never_reach_the_final_bucket() {
        let mut h = Histogram::new();
        h.record(bucket_offsets()[BUCKET_COUNT - 1]);
        h.record(u64::MAX);
        assert_eq!(h.buckets()[BUCKET_COUNT - 1], 0);
        assert_eq!(h.buckets()[BUCKET_COUNT - 2], 2);
        assert_eq!(h.max(), u64::MAX);
        h.compute();
        assert_eq!(h.val_999th(), 30_130_994);
    }

    #[test]
    fn full_final_bucket_skips_compute() {
        let mut h = Histogram::new();
        h.record(10);
        h.compute();
        h.buckets[BUCKET_COUNT - 1] = 1;
        h.record(1_000);
        h.compute();
        // Still the values the first compute published.
        assert_eq!((h.mean(), h.val_99th()), (10, 1));
    }

    #[test]
    fn compute_keeps_mean_when_empty() {
        let mut h = Histogram::new();
        h.mean = 7;
        h.compute();
        assert_eq!(h.mean(), 7);
    }

    #[test]
    fn mean_is_ceiling_over_offsets() {
        let mut h = Histogram::new();
        h.record(10);
        h.record(12);
        h.record(14);
        h.record(14);
        h.compute();
        // (10 + 12 + 14 + 14) / 4 = 12.5
        assert_eq!(h.mean(), 13);
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
        h.compute();
        h.reset();
        assert_eq!(h.count(), 0);
        assert_eq!(h.max(), 0);
        assert_eq!(h.mean(), 0);
        assert_eq!((h.val_95th(), h.val_99th(), h.val_999th()), (0, 0, 0));
    }
}
