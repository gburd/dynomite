//! Allocation-free numeric helpers used by the stats subsystem.
//!
//! These helpers convert between `f64` and `u64` without any `as` casts
//! by working directly on the IEEE 754 bit representation, so the crate
//! can compile cleanly under `clippy::pedantic`.

/// Number of representable u32 values, expressed exactly as an `f64`.
const TWO_32: f64 = 4_294_967_296.0_f64;

/// Convert a `u64` to an `f64` losslessly for the upper 53 bits and with
/// rounding for the rest, without using any `as` casts.
pub(crate) fn u64_to_f64(x: u64) -> f64 {
    let lo = u32::try_from(x & 0xFFFF_FFFF).expect("32-bit mask fits in u32");
    let hi = u32::try_from(x >> 32).expect("upper 32 bits fit in u32");
    f64::from(hi) * TWO_32 + f64::from(lo)
}

/// Compute `floor(p * scale)` for `p` in `[0.0, 1.0]` and `scale` in
/// `u64` without any float-to-integer `as` casts.
///
/// Returns `0` for non-finite or negative `p`. For `p >= 1.0` the result
/// is `scale`.
pub(crate) fn floor_p_times_u64(p: f64, scale: u64) -> u64 {
    if !p.is_finite() || p <= 0.0 {
        return 0;
    }
    if p >= 1.0 {
        return scale;
    }
    let bits = p.to_bits();
    let exp = u32::try_from((bits >> 52) & 0x7FF).expect("11-bit field fits in u32");
    let mant = bits & ((1u64 << 52) - 1);
    if exp == 0 {
        // Subnormal value: smaller than 2^-1022, multiplied by any u64
        // floors to zero.
        return 0;
    }
    // For p < 1.0 the biased exponent is at most 1022.
    let shift = 1075u32.saturating_sub(exp);
    if shift >= 128 {
        return 0;
    }
    let m = (1u64 << 52) | mant;
    let prod = u128::from(m) * u128::from(scale);
    let shifted = prod >> shift;
    u64::try_from(shifted).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_zero_or_negative_returns_zero() {
        assert_eq!(floor_p_times_u64(0.0, 1_000), 0);
        assert_eq!(floor_p_times_u64(-1.0, 1_000), 0);
        assert_eq!(floor_p_times_u64(f64::NAN, 1_000), 0);
        assert_eq!(floor_p_times_u64(f64::INFINITY, 1_000), 0);
    }

    #[test]
    fn floor_one_returns_scale() {
        assert_eq!(floor_p_times_u64(1.0, 1_000), 1_000);
        assert_eq!(floor_p_times_u64(2.0, 1_000), 1_000);
    }

    #[test]
    fn floor_half_of_thousand_is_five_hundred() {
        assert_eq!(floor_p_times_u64(0.5, 1_000), 500);
    }

    #[test]
    fn floor_known_quantiles() {
        // 0.95, 0.99, 0.999 are not exactly representable in f64, so the
        // helper produces the same floor-of-product result that the C
        // reference does (one less than the naive arithmetic answer for
        // some inputs).
        assert_eq!(floor_p_times_u64(0.95, 1_000), 949);
        assert_eq!(floor_p_times_u64(0.99, 1_000), 989);
        assert_eq!(floor_p_times_u64(0.999, 10_000), 9_989);
        // Exactly representable fractions match the naive answer.
        assert_eq!(floor_p_times_u64(0.5, 1_000), 500);
        assert_eq!(floor_p_times_u64(0.25, 1_000), 250);
        assert_eq!(floor_p_times_u64(0.125, 1_024), 128);
    }

    #[test]
    fn u64_to_f64_matches_f64_from_for_small_values() {
        let pairs: &[(u64, f64)] = &[
            (0, 0.0),
            (1, 1.0),
            (42, 42.0),
            (4_294_967_295, 4_294_967_295.0),
            (4_294_967_296, 4_294_967_296.0),
            (1_000_000_000_000, 1_000_000_000_000.0),
        ];
        for (x, expected) in pairs.iter().copied() {
            let f = u64_to_f64(x);
            assert!((f - expected).abs() < 1.0, "u64_to_f64({x}) = {f}");
        }
    }

    #[test]
    fn u64_to_f64_zero() {
        assert!(u64_to_f64(0).abs() < f64::EPSILON);
    }
}
