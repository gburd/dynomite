//! Allocation-free numeric helpers used by the stats subsystem.
//!
//! `u64_to_f64` keeps the `f64` <-> `u64` cast off the hot path in the
//! histogram mean computation. `floor_p_times_u64` matches the IEEE 754
//! semantics of the reference percentile expression
//! `(double)count(histo) * percentile`, which the histogram percentile
//! and `from_histogram` summary both depend on.

/// Number of representable u32 values, expressed exactly as an `f64`.
const TWO_32: f64 = 4_294_967_296.0_f64;

/// Convert a `u64` to an `f64` losslessly for the upper 53 bits and with
/// rounding for the rest, without using any `as` casts.
pub(crate) fn u64_to_f64(x: u64) -> f64 {
    let lo = u32::try_from(x & 0xFFFF_FFFF).expect("32-bit mask fits in u32");
    let hi = u32::try_from(x >> 32).expect("upper 32 bits fit in u32");
    f64::from(hi) * TWO_32 + f64::from(lo)
}

/// Compute `floor(p * scale)` using the same IEEE 754 semantics as the
/// reference expression `floor((double)scale * p)`.
///
/// `p` is converted to its `f64` value, multiplied by `scale` rounded to
/// the nearest representable `f64`, and the product is floored. NaN and
/// negative `p` return zero. Saturates at `u64::MAX` for products that
/// overflow the integer range.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub(crate) fn floor_p_times_u64(p: f64, scale: u64) -> u64 {
    // The cast lints are allowed for this helper because the function's
    // contract is to reproduce the C reference's `(double)scale * p`
    // floor exactly. Any alternative arithmetic would diverge from the
    // reference for inputs whose bit pattern rounds across an integer
    // boundary (for example p=0.95 with scale=1000 yields 950 in C and
    // 949 with rational arithmetic). See docs/journal/allowances.md.
    if p.is_nan() || p < 0.0 {
        return 0;
    }
    let product = p * (scale as f64);
    if !product.is_finite() {
        return u64::MAX;
    }
    let floored = product.floor();
    if floored <= 0.0 {
        return 0;
    }
    if floored >= u64::MAX as f64 {
        return u64::MAX;
    }
    floored as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_zero_or_negative_returns_zero() {
        assert_eq!(floor_p_times_u64(0.0, 1_000), 0);
        assert_eq!(floor_p_times_u64(-1.0, 1_000), 0);
        assert_eq!(floor_p_times_u64(f64::NAN, 1_000), 0);
    }

    #[test]
    fn floor_infinite_p_saturates() {
        // The reference f64 expression overflows; saturate at u64::MAX.
        assert_eq!(floor_p_times_u64(f64::INFINITY, 1_000), u64::MAX);
    }

    #[test]
    fn floor_p_one_returns_scale() {
        assert_eq!(floor_p_times_u64(1.0, 1_000), 1_000);
    }

    #[test]
    fn floor_p_above_one_scales_proportionally() {
        // The reference expression does not clamp; floor(2.0 * 1000) = 2000.
        assert_eq!(floor_p_times_u64(2.0, 1_000), 2_000);
    }

    #[test]
    fn floor_half_of_thousand_is_five_hundred() {
        assert_eq!(floor_p_times_u64(0.5, 1_000), 500);
    }

    #[test]
    fn floor_known_quantiles() {
        // The reference percentile expression is
        // floor((double)scale * p) computed in IEEE 754 doubles. The
        // values below match what that expression yields.
        assert_eq!(floor_p_times_u64(0.95, 1_000), 950);
        assert_eq!(floor_p_times_u64(0.99, 1_000), 990);
        assert_eq!(floor_p_times_u64(0.999, 10_000), 9_990);
        assert_eq!(floor_p_times_u64(0.95, 100), 95);
        assert_eq!(floor_p_times_u64(0.95, 200), 190);
        // Exactly representable fractions match the naive answer.
        assert_eq!(floor_p_times_u64(0.5, 1_000), 500);
        assert_eq!(floor_p_times_u64(0.25, 1_000), 250);
        assert_eq!(floor_p_times_u64(0.125, 1_024), 128);
    }

    #[test]
    fn floor_matches_f64_reference_over_known_pairs() {
        // Pairs taken from the histogram's actual percentile call sites:
        // count of recorded observations times one of {0.95, 0.99, 0.999}.
        let cases: &[(f64, u64)] = &[
            (0.95, 19),
            (0.95, 38),
            (0.95, 95),
            (0.95, 190),
            (0.95, 950),
            (0.99, 990),
            (0.999, 9_990),
        ];
        for &(p, scale) in cases {
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let reference = (p * (scale as f64)).floor() as u64;
            assert_eq!(
                floor_p_times_u64(p, scale),
                reference,
                "helper diverged from f64 reference at p={p} scale={scale}"
            );
        }
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

    proptest::proptest! {
        /// For every realistic `(scale, p)` pair the helper must match the
        /// reference IEEE 754 expression `(p * scale as f64).floor() as u64`.
        #[test]
        fn floor_p_times_u64_matches_f64_floor(
            scale in 0u64..=u64::from(u32::MAX),
            p_idx in 0usize..7,
        ) {
            let ps = [0.0f64, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0];
            let p = ps[p_idx];
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let reference = (p * (scale as f64)).floor() as u64;
            proptest::prop_assert_eq!(
                floor_p_times_u64(p, scale),
                reference,
                "helper diverged from f64 reference at p={} scale={}", p, scale
            );
        }
    }
}
