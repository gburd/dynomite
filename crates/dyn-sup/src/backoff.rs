//! Backoff timing for child restarts.

use std::time::Duration;

/// Exponential backoff with jitter.
///
/// The delay before the *n*th consecutive restart of a child is:
///
/// ```text
/// base = min(start * factor.powi(n), max)
/// delay = base * (1 + uniform(-jitter, +jitter))
/// ```
///
/// where `n` starts at 0 for the first restart after a failure. A
/// run that completes successfully resets the consecutive-failure
/// counter to zero. A `factor` of 1.0 disables exponential growth; a
/// `jitter` of 0.0 disables jitter (and yields exactly deterministic
/// timing).
#[derive(Debug, Clone, Copy)]
pub struct BackoffSpec {
    /// Initial delay used for the first restart after a failure.
    pub start: Duration,
    /// Upper bound on the delay (inclusive of jitter, clamped to a
    /// non-negative value).
    pub max: Duration,
    /// Multiplier applied to the delay on each consecutive failure.
    /// Must be finite and `>= 1.0` for the typical exponential shape;
    /// other values are accepted but produce non-monotone sequences.
    pub factor: f64,
    /// Fraction of the computed delay used to randomize the actual
    /// delay. `0.0` disables jitter; `1.0` allows the delay to vary by
    /// up to its full magnitude in either direction (clamped to zero
    /// at the low end).
    pub jitter: f64,
}

impl Default for BackoffSpec {
    /// 100ms start, 30s max, factor 2.0, jitter 0.1.
    fn default() -> Self {
        Self {
            start: Duration::from_millis(100),
            max: Duration::from_secs(30),
            factor: 2.0,
            jitter: 0.1,
        }
    }
}

impl BackoffSpec {
    /// Build a backoff with no jitter and a fixed multiplier. Useful
    /// for tests where deterministic timing matters.
    pub fn fixed(start: Duration, max: Duration, factor: f64) -> Self {
        Self {
            start,
            max,
            factor,
            jitter: 0.0,
        }
    }

    /// Compute the deterministic (un-jittered) delay for the given
    /// consecutive-failure count, clamped to `max`.
    pub(crate) fn base_delay(&self, failures: u32) -> Duration {
        let exp = match i32::try_from(failures) {
            Ok(e) => e,
            Err(_) => i32::MAX,
        };
        let mult = self.factor.powi(exp);
        let secs = self.start.as_secs_f64() * mult;
        let max_secs = self.max.as_secs_f64();
        let capped = if secs.is_finite() {
            secs.min(max_secs).max(0.0)
        } else {
            max_secs
        };
        Duration::from_secs_f64(capped)
    }

    /// Apply jitter to `base` using an already-advanced xorshift64
    /// state value. Pure function: callers are responsible for
    /// advancing the PRNG (sequentially via
    /// [`xorshift64`] or atomically via
    /// [`crate::atomics::BackoffState`]).
    pub(crate) fn apply_jitter(&self, base: Duration, prng_value: u64) -> Duration {
        if self.jitter <= 0.0 {
            return base;
        }
        let r = unit_from_state(prng_value);
        let secs = base.as_secs_f64() * (1.0 + r * self.jitter);
        let max_secs = self.max.as_secs_f64();
        let bounded = if secs.is_finite() {
            secs.clamp(0.0, max_secs)
        } else {
            max_secs
        };
        Duration::from_secs_f64(bounded)
    }
}

/// Advance an xorshift64 PRNG state. Pure function returning the
/// next state. A zero input is replaced with a fixed non-zero seed
/// so the sequence cannot collapse to all-zeros.
pub(crate) fn xorshift64(state: u64) -> u64 {
    let s = if state == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        state
    };
    let mut x = s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Map a 64-bit pseudo-random value to a uniform sample in
/// `[-1.0, 1.0)`.
pub(crate) fn unit_from_state(state: u64) -> f64 {
    // Construct an f64 in [1.0, 2.0) from random bits using the
    // well-known IEEE 754 trick: set sign = 0, exponent = 1023
    // (the bias for 2^0), and stuff 52 random bits into the
    // mantissa. This avoids any narrowing cast.
    let mantissa = (state >> 12) & 0x000F_FFFF_FFFF_FFFF;
    let bits = 0x3FF0_0000_0000_0000_u64 | mantissa;
    let f = f64::from_bits(bits); // f in [1.0, 2.0)
    let unit = f - 1.0; // [0.0, 1.0)
    unit.mul_add(2.0, -1.0) // [-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_delay_doubles_with_factor_two() {
        let b = BackoffSpec::fixed(Duration::from_millis(10), Duration::from_secs(60), 2.0);
        assert_eq!(b.base_delay(0), Duration::from_millis(10));
        assert_eq!(b.base_delay(1), Duration::from_millis(20));
        assert_eq!(b.base_delay(2), Duration::from_millis(40));
        assert_eq!(b.base_delay(3), Duration::from_millis(80));
    }

    #[test]
    fn base_delay_caps_at_max() {
        let b = BackoffSpec::fixed(Duration::from_millis(100), Duration::from_millis(500), 2.0);
        assert_eq!(b.base_delay(0), Duration::from_millis(100));
        assert_eq!(b.base_delay(1), Duration::from_millis(200));
        assert_eq!(b.base_delay(2), Duration::from_millis(400));
        // Capped:
        assert_eq!(b.base_delay(3), Duration::from_millis(500));
        assert_eq!(b.base_delay(20), Duration::from_millis(500));
    }

    #[test]
    fn apply_jitter_stays_within_bounds() {
        let b = BackoffSpec {
            start: Duration::from_millis(100),
            max: Duration::from_secs(60),
            factor: 2.0,
            jitter: 0.5,
        };
        let mut state = 0xdead_beef_u64;
        let base = b.base_delay(3);
        for _ in 0..1024 {
            state = xorshift64(state);
            let d = b.apply_jitter(base, state);
            assert!(d <= Duration::from_secs_f64(base.as_secs_f64() * 1.5));
            assert!(d.as_secs_f64() >= 0.0);
        }
    }

    #[test]
    fn apply_jitter_is_a_noop_when_jitter_is_zero() {
        let b = BackoffSpec::fixed(Duration::from_millis(123), Duration::from_secs(10), 1.5);
        let mut state = 1_u64;
        for f in 0..5 {
            state = xorshift64(state);
            let base = b.base_delay(f);
            assert_eq!(b.apply_jitter(base, state), base);
        }
    }

    #[test]
    fn xorshift64_is_pure() {
        // Same input -> same output; zero is mapped to a fixed seed.
        assert_eq!(xorshift64(0), xorshift64(0));
        assert_ne!(xorshift64(0), 0);
        assert_eq!(xorshift64(42), xorshift64(42));
    }
}
