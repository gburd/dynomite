//! Standard bloom filter with configurable bit count and hash
//! count.
//!
//! The filter is parameterised by a target false positive rate
//! `fp` and an expected insert count `n`. The constructor
//! computes the optimal bit count and hash count from the
//! standard textbook formulas:
//!
//! ```text
//! m_bits  = ceil( -n * ln(fp) / (ln 2)^2 )
//! k_hashes = ceil( (m_bits / n) * ln 2 )
//! ```
//!
//! # Hashing
//!
//! Each insert / contains call BLAKE3-hashes the key once and
//! synthesises the `k` bit indices via double hashing
//! (`h1 + i * h2 mod m`). This is the standard Kirsch-Mitzenmacher
//! construction; for two independent base hashes the
//! double-hashing scheme reproduces the false positive rate of
//! a true `k`-independent hash family within rounding error,
//! and BLAKE3's first sixteen output bytes split cleanly into
//! two independent `u64` halves.
//!
//! # No false negatives
//!
//! `contains` returns `true` for every key the filter has ever
//! observed via `insert`. Conversely, a `true` return does NOT
//! prove the key was inserted; the false positive rate is
//! determined by the constructor parameters and the actual
//! number of distinct keys inserted.

use serde::{Deserialize, Serialize};

/// Minimum bit count. Provides a usable filter even when the
/// caller's `n` is zero (degenerate corner case).
const MIN_BITS: u64 = 64;

/// Minimum number of hash functions. A single hash is enough to
/// give the filter its no-false-negative guarantee.
const MIN_HASHES: u8 = 1;

/// Maximum number of hash functions. Beyond this point the
/// false-positive rate climbs again and additional hashes only
/// slow inserts and lookups; cap it at a sensible ceiling.
const MAX_HASHES: u8 = 32;

/// Bloom filter over arbitrary byte keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Bit vector packed into `u64` chunks.
    bits: Vec<u64>,
    /// Number of hash functions per insert / lookup.
    hashes: u8,
    /// Total bit count `64 * bits.len()`. Cached so the modulo
    /// computation does not re-multiply on every hash.
    n_bits: u64,
}

impl BloomFilter {
    /// Construct a filter sized for `n` expected inserts at a
    /// target false positive rate of `fp`.
    ///
    /// `fp` must be strictly between 0 and 1; values outside
    /// that range are clamped. `n` of zero produces the minimum
    /// usable filter (64 bits, 1 hash) so callers do not need
    /// to special-case empty corpora.
    ///
    /// # Examples
    ///
    /// ```
    /// use dyntext::bloom::BloomFilter;
    /// let mut bf = BloomFilter::with_size_and_fp_rate(1000, 0.01);
    /// bf.insert(b"hello");
    /// assert!(bf.contains(b"hello"));
    /// ```
    #[must_use]
    pub fn with_size_and_fp_rate(n: usize, fp: f64) -> Self {
        let fp = if fp.is_nan() || fp <= 0.0 {
            0.000_001
        } else if fp >= 1.0 {
            0.999_999
        } else {
            fp
        };

        let (n_bits, hashes) = compute_params(n, fp);
        let chunk_count = (n_bits / 64) as usize;
        Self {
            bits: vec![0_u64; chunk_count],
            hashes,
            n_bits,
        }
    }

    /// Total bit count (multiple of 64).
    #[must_use]
    pub fn n_bits(&self) -> u64 {
        self.n_bits
    }

    /// Number of hash functions per insert / lookup.
    #[must_use]
    pub fn hash_count(&self) -> u8 {
        self.hashes
    }

    /// Insert `key` into the filter.
    ///
    /// After this call, [`Self::contains`] returns `true` for
    /// `key`. Repeated inserts of the same key are idempotent.
    pub fn insert(&mut self, key: &[u8]) {
        for idx in self.indices(key) {
            self.set_bit(idx);
        }
    }

    /// Test whether `key` MAY be present.
    ///
    /// Returns `false` only if `key` has provably never been
    /// inserted (no false negatives). Returns `true` if every
    /// hash bit is set, which can happen either because `key`
    /// was inserted or because the bits were collectively set
    /// by other keys (false positive).
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        for idx in self.indices(key) {
            if !self.test_bit(idx) {
                return false;
            }
        }
        true
    }

    /// Theoretical false positive rate after `n_inserted`
    /// distinct insertions.
    ///
    /// Formula: `(1 - exp(-k * n / m))^k`, where `k` is the
    /// hash count and `m` is the bit count. Returns 0 for an
    /// empty filter (no inserts) and approaches 1 for a
    /// fully saturated filter.
    #[must_use]
    pub fn false_positive_rate(&self, n_inserted: usize) -> f64 {
        if n_inserted == 0 || self.n_bits == 0 {
            return 0.0;
        }
        let k = f64::from(self.hashes);
        let n = usize_to_f64(n_inserted);
        let m = u64_to_f64(self.n_bits);
        let occupancy = 1.0 - (-k * n / m).exp();
        occupancy.powf(k)
    }

    /// Compute the `k` bit indices a key hashes to.
    ///
    /// Uses double hashing (`h1 + i * h2`) over the first
    /// 16 bytes of the BLAKE3 digest split as two `u64`s. This
    /// is the Kirsch-Mitzenmacher construction.
    fn indices(&self, key: &[u8]) -> impl Iterator<Item = u64> {
        let h = blake3::hash(key);
        let bytes = h.as_bytes();
        let mut h1_buf = [0_u8; 8];
        let mut h2_buf = [0_u8; 8];
        h1_buf.copy_from_slice(&bytes[0..8]);
        h2_buf.copy_from_slice(&bytes[8..16]);
        let h1 = u64::from_le_bytes(h1_buf);
        let h2 = u64::from_le_bytes(h2_buf);
        let n_bits = self.n_bits;
        let k = self.hashes;
        (0..u64::from(k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % n_bits)
    }

    fn set_bit(&mut self, idx: u64) {
        let chunk = u64_to_usize(idx / 64);
        let off = idx % 64;
        self.bits[chunk] |= 1_u64 << off;
    }

    fn test_bit(&self, idx: u64) -> bool {
        let chunk = u64_to_usize(idx / 64);
        let off = idx % 64;
        (self.bits[chunk] >> off) & 1 == 1
    }
}

/// Compute `(n_bits, hashes)` for a filter sized for `n` keys at
/// false positive rate `fp` (already clamped to `(0, 1)`).
///
/// Returns `n_bits` rounded up to a multiple of 64, and `hashes`
/// clamped into `[MIN_HASHES, MAX_HASHES]`.
fn compute_params(n: usize, fp: f64) -> (u64, u8) {
    let n_f = usize_to_f64(n);
    let raw_m = if n == 0 {
        u64_to_f64(MIN_BITS)
    } else {
        -n_f * fp.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2)
    };
    let m_rounded = raw_m.ceil().max(u64_to_f64(MIN_BITS));

    // Round up to the next multiple of 64 so the bit vector
    // packs cleanly into u64 chunks.
    let m_as_u64 = f64_to_u64_saturating(m_rounded);
    let m_u64_chunks = m_as_u64.div_ceil(64).max(1);
    let n_bits = m_u64_chunks * 64;

    let k_raw = if n == 0 {
        f64::from(MIN_HASHES)
    } else {
        (u64_to_f64(n_bits) / n_f) * std::f64::consts::LN_2
    };
    let k_rounded = k_raw.ceil().max(f64::from(MIN_HASHES));
    let hashes_u32 = f64_to_u32_saturating(k_rounded).min(u32::from(MAX_HASHES));
    let hashes = u8::try_from(hashes_u32)
        .unwrap_or(MAX_HASHES)
        .max(MIN_HASHES);
    (n_bits, hashes)
}

/// Lossless conversion of a small `u64` (always <= 2^53) into
/// `f64`. Wider values lose precision, which is acceptable for
/// the bloom-dimension formula but is documented at every call
/// site.
#[allow(
    clippy::cast_precision_loss,
    reason = "bloom dimension formula: u64 -> f64 widens by design; values are bounded by the configured bit count and have far fewer than 2^53 significant bits in practice."
)]
fn u64_to_f64(x: u64) -> f64 {
    x as f64
}

/// Lossless-or-rounded conversion of `usize` to `f64`.
#[allow(
    clippy::cast_precision_loss,
    reason = "bloom dimension formula: usize -> f64 may round for n above 2^53; that is the same precision loss the reference Mitzenmacher derivation accepts because the formula uses ln(fp), which dominates the rounding budget."
)]
fn usize_to_f64(x: usize) -> f64 {
    x as f64
}

/// Convert a non-negative finite `f64` to `u64`, saturating at
/// `u64::MAX`. Negative or NaN inputs are mapped to 0.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "bloom dimension formula: f64 -> u64 with explicit guards. Caller has already ceiled the input and the formula's outputs are positive; the saturating cast pins the rare overflow case to u64::MAX."
)]
fn f64_to_u64_saturating(x: f64) -> u64 {
    if !x.is_finite() || x <= 0.0 {
        return 0;
    }
    if x >= u64_to_f64(u64::MAX) {
        return u64::MAX;
    }
    x as u64
}

/// Convert a non-negative finite `f64` to `u32`, saturating at
/// `u32::MAX`. Negative or NaN inputs are mapped to 0.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "bloom dimension formula: f64 -> u32 with explicit guards. The hash count is bounded by MAX_HASHES (32) so the cast is well within u32 range; the saturation arm guards against pathological inputs."
)]
fn f64_to_u32_saturating(x: f64) -> u32 {
    if !x.is_finite() || x <= 0.0 {
        return 0;
    }
    if x >= f64::from(u32::MAX) {
        return u32::MAX;
    }
    x as u32
}

/// Convert a `u64` bit-vector index to `usize`. On 64-bit
/// targets this is lossless; on 32-bit targets values larger
/// than `usize::MAX` are saturated, but the bloom code never
/// produces them because the bit-vector length is itself bounded
/// by the host's heap allocator (which is also `usize`-bounded).
#[allow(
    clippy::cast_possible_truncation,
    reason = "indexing into a Vec<u64> by chunk: idx is already bounded by self.n_bits, which was derived from a Vec capacity (usize-bounded) at construction time."
)]
fn u64_to_usize(x: u64) -> usize {
    x as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_no_false_negatives_on_inserted_keys() {
        let mut bf = BloomFilter::with_size_and_fp_rate(1000, 0.01);
        let keys: Vec<Vec<u8>> = (0..200_u32)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();
        for k in &keys {
            bf.insert(k);
        }
        for k in &keys {
            assert!(bf.contains(k), "false negative on inserted key {k:?}");
        }
    }

    #[test]
    fn bloom_false_positive_rate_under_5pct_at_design_load() {
        let n: u32 = 10_000;
        let fp_target = 0.01;
        let mut bf = BloomFilter::with_size_and_fp_rate(n as usize, fp_target);
        for i in 0..n {
            let k = format!("inserted-{i}");
            bf.insert(k.as_bytes());
        }
        // Probe with disjoint keys; count false positives.
        let probes = 5_000_u32;
        let mut fps: u32 = 0;
        for i in 0..probes {
            let k = format!("probe-{i}-not-in-filter");
            if bf.contains(k.as_bytes()) {
                fps += 1;
            }
        }
        let observed = f64::from(fps) / f64::from(probes);
        assert!(
            observed < 0.05,
            "observed fp rate {observed} exceeded 5%; \
             theoretical={}",
            bf.false_positive_rate(n as usize)
        );
    }

    #[test]
    fn bloom_zero_keys_returns_false_for_anything() {
        let bf = BloomFilter::with_size_and_fp_rate(100, 0.01);
        assert!(!bf.contains(b"nope"));
        assert!(!bf.contains(b""));
        assert!(!bf.contains(b"another"));
    }

    #[test]
    fn bloom_with_zero_n_does_not_panic() {
        let mut bf = BloomFilter::with_size_and_fp_rate(0, 0.01);
        assert_eq!(bf.n_bits(), MIN_BITS);
        bf.insert(b"x");
        assert!(bf.contains(b"x"));
    }

    #[test]
    fn bloom_with_extreme_fp_rates_clamps() {
        let bf_low = BloomFilter::with_size_and_fp_rate(100, 0.0);
        assert!(bf_low.n_bits() >= MIN_BITS);
        let bf_high = BloomFilter::with_size_and_fp_rate(100, 1.0);
        assert!(bf_high.n_bits() >= MIN_BITS);
        let bf_nan = BloomFilter::with_size_and_fp_rate(100, f64::NAN);
        assert!(bf_nan.n_bits() >= MIN_BITS);
    }

    #[test]
    fn bloom_hash_count_is_capped() {
        // Asking for a microscopic fp rate should cap k at
        // MAX_HASHES rather than spending forever per query.
        let bf = BloomFilter::with_size_and_fp_rate(10, 1e-30);
        assert!(bf.hash_count() <= MAX_HASHES);
    }

    #[test]
    fn bloom_false_positive_rate_is_zero_for_empty_filter() {
        let bf = BloomFilter::with_size_and_fp_rate(100, 0.01);
        let rate = bf.false_positive_rate(0);
        assert!(rate.abs() < f64::EPSILON);
    }
}
