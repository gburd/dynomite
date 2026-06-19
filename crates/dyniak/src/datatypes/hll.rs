//! Riak HyperLogLog (probabilistic cardinality estimator).
//!
//! State is a fixed-size register array indexed by the high
//! bits of a 64-bit hash. Each register stores the maximum
//! observed value of `rho(payload)`, where `rho` is the 1-based
//! position of the leading 1-bit in the hash bits left over
//! after the index is sliced off. The cardinality estimate
//! follows the original HyperLogLog formula plus the small-range
//! linear-counting correction; the large-range correction from
//! the 2007 paper is unnecessary because we use a 64-bit hash
//! and the upper bound (`2^57` or so) is far above any practical
//! Riak workload.
//!
//! # Precision
//!
//! [`PRECISION`] = 14 bits, matching Riak's default. The register
//! array has `2^14 = 16384` entries; the standard error envelope
//! is `1.04 / sqrt(m) ~= 0.81%`. Tests assert the merged estimate
//! falls within +/-5% of the true cardinality, which is generous
//! enough that statistical flake is not an issue at 10000 items
//! per replica.
//!
//! # Hash
//!
//! Inputs are hashed through the project's
//! [`dynomite::hashkit::hash`] entry point with
//! [`HashType::Murmur3`]. Murmur3 is a 128-bit hash; we combine
//! the low two `u32` words into the 64-bit value used by HLL.
//! The function is deterministic across replicas, which is what
//! lets `merge` collapse to element-wise max on the register
//! arrays.
//!
//! # Merge
//!
//! Element-wise max on the register arrays. Trivially
//! associative (max is associative), commutative (max is
//! commutative), and idempotent (max of a value with itself is
//! the same value).

use dynomite::hashkit::{hash, HashType};

use crate::datatypes::Crdt;

/// HLL precision parameter `p`, in bits. Riak's default is 14.
pub const PRECISION: u32 = 14;

/// Number of registers `m = 2^p`.
pub const REGISTER_COUNT: usize = 1usize << PRECISION;

/// Maximum value any register can hold: `64 - p + 1 = 51`.
const MAX_RHO: u8 = 51;

/// HyperLogLog cardinality estimator CRDT.
///
/// Each register holds a `u8` (the observed `rho` value, capped
/// at the internal `MAX_RHO` limit). The register array is held as a `Box<[u8]>`
/// so the type stays `Sized` while the 16-KiB array lives on
/// the heap.
///
/// # Examples
///
/// ```
/// use dyniak::datatypes::{Crdt, HyperLogLog};
///
/// let mut h = HyperLogLog::new();
/// for i in 0..1000u32 {
///     h.add(i.to_be_bytes());
/// }
/// let estimate = h.value();
/// // 1.04/sqrt(16384) ~= 0.81% expected error; +/-10% is safe.
/// assert!(estimate >= 900 && estimate <= 1100);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperLogLog {
    registers: Box<[u8]>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    /// Construct an empty HLL with all registers zeroed.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; REGISTER_COUNT].into_boxed_slice(),
        }
    }

    /// Hash an item to a 64-bit value via Murmur3.
    ///
    /// Murmur3 produces a four-word [`dynomite::hashkit::DynToken`];
    /// we splice the first two `u32` words into a `u64`, which
    /// gives a near-uniform 64-bit distribution adequate for
    /// HLL.
    fn hash64(item: &[u8]) -> u64 {
        let token = hash(HashType::Murmur3, item);
        let words = token.mag();
        // Murmur3 always emits 4 words; defensive against shorter
        // tokens by zero-extending.
        let lo = u64::from(words.first().copied().unwrap_or(0));
        let hi = u64::from(words.get(1).copied().unwrap_or(0));
        (hi << 32) | lo
    }

    /// Fold `item` into the register array.
    ///
    /// Concretely:
    ///
    /// 1. `h = murmur3_64(item)`.
    /// 2. `idx = h >> (64 - PRECISION)` -- the top `p` bits.
    /// 3. `payload = (h << PRECISION) | (1 << (PRECISION - 1))`
    ///    -- shift the index off and OR in a sentinel bit so
    ///    `leading_zeros` never exceeds `64 - p`.
    /// 4. `rho = leading_zeros(payload) + 1`.
    /// 5. `registers[idx] = max(registers[idx], rho)`.
    pub fn add(&mut self, item: impl AsRef<[u8]>) {
        let h = Self::hash64(item.as_ref());
        let idx = (h >> (64 - PRECISION)) as usize;
        let payload = (h << PRECISION) | (1u64 << (PRECISION - 1));
        let leading = payload.leading_zeros();
        let rho = u8::try_from(leading + 1).unwrap_or(MAX_RHO).min(MAX_RHO);
        if rho > self.registers[idx] {
            self.registers[idx] = rho;
        }
    }

    /// Read-only view of the register array. Exposed for tests
    /// and operator tooling that needs to dump the raw state.
    #[must_use]
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }
}

impl Crdt for HyperLogLog {
    type Value = u64;

    fn merge(&mut self, other: &Self) {
        for (mine, theirs) in self.registers.iter_mut().zip(other.registers.iter()) {
            if *theirs > *mine {
                *mine = *theirs;
            }
        }
    }

    /// Estimate the number of distinct items folded into the
    /// register array.
    ///
    /// Uses the original HyperLogLog harmonic-mean formula:
    ///
    /// ```text
    ///   E = alpha_m * m^2 / sum(2^(-M[j]) for j in 0..m)
    /// ```
    ///
    /// with the small-range correction (linear counting):
    /// when the number `V` of zero registers is non-zero and
    /// the raw estimate is at most `5/2 * m`, return
    /// `m * ln(m / V)`. The 2007 large-range correction is
    /// dropped: with a 64-bit hash, the breakdown range
    /// (`2^32 / 30`) is unreachable in practice.
    fn value(&self) -> u64 {
        // `REGISTER_COUNT` = 16384 fits in an `f64` exactly
        // (well below the 2^53 mantissa precision threshold);
        // building it via `u32 -> f64` keeps clippy happy.
        let m = f64::from(u32::try_from(REGISTER_COUNT).unwrap_or(u32::MAX));
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for &r in &*self.registers {
            if r == 0 {
                zeros += 1;
            }
            sum += 2.0f64.powi(-i32::from(r));
        }
        // alpha_16384 = 0.7213 / (1 + 1.079 / m) per Flajolet et al.
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;
        let estimate = if zeros > 0 && raw <= 2.5 * m {
            // Linear counting.
            m * (m / f64::from(zeros)).ln()
        } else {
            raw
        };
        f64_to_u64_saturating(estimate)
    }
}

/// Convert a non-negative `f64` cardinality estimate to `u64`,
/// clamping NaN, negatives, and overflow to a sane value.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn f64_to_u64_saturating(x: f64) -> u64 {
    if x.is_nan() || x <= 0.0 {
        return 0;
    }
    let rounded = x.round();
    if !rounded.is_finite() {
        return u64::MAX;
    }
    // `u64::MAX as f64` rounds up to `2^64`; the comparison
    // catches every value that would not fit in a `u64`.
    if rounded >= u64::MAX as f64 {
        return u64::MAX;
    }
    // Rounded is finite, non-negative, and below `2^64`; the
    // `as` cast is exact-or-truncating-to-a-bounded integer.
    rounded as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_hll_estimates_zero() {
        let h = HyperLogLog::new();
        assert_eq!(h.value(), 0);
    }

    #[test]
    fn single_item_estimates_one() {
        let mut h = HyperLogLog::new();
        h.add(b"alpha");
        // Linear counting on a near-empty array gives ~1.
        let v = h.value();
        assert!(
            (1..=2).contains(&v),
            "single-item estimate {v} not in [1,2]"
        );
    }

    #[test]
    fn duplicate_adds_are_idempotent() {
        let mut h = HyperLogLog::new();
        for _ in 0..100 {
            h.add(b"same");
        }
        let v = h.value();
        assert!(v <= 2, "duplicate-adds estimate {v} should be ~1");
    }

    #[test]
    fn small_distinct_set_estimates_correctly() {
        let mut h = HyperLogLog::new();
        for i in 0u32..1000 {
            h.add(i.to_be_bytes());
        }
        let v = h.value();
        assert!(
            (900..=1100).contains(&v),
            "estimate {v} outside +/-10% of 1000"
        );
    }

    #[test]
    fn merge_is_elementwise_max() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        a.registers[0] = 5;
        a.registers[1] = 2;
        b.registers[0] = 3;
        b.registers[1] = 7;
        a.merge(&b);
        assert_eq!(a.registers[0], 5);
        assert_eq!(a.registers[1], 7);
    }

    #[test]
    fn merge_is_commutative() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        for i in 0u32..200 {
            a.add(i.to_be_bytes());
        }
        for i in 100u32..300 {
            b.add(i.to_be_bytes());
        }
        let mut left = a.clone();
        left.merge(&b);
        let mut right = b.clone();
        right.merge(&a);
        assert_eq!(left.registers, right.registers);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = HyperLogLog::new();
        for i in 0u32..500 {
            a.add(i.to_be_bytes());
        }
        let snap = a.clone();
        a.merge(&snap);
        assert_eq!(a.registers, snap.registers);
    }
}
