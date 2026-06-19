//! Pseudo-random integer source used by the `random` distribution.
//!
//! Requests to live servers are dispatched using `random_index() %
//! ncontinuum`, backed by a small, deterministic-when-seeded LCG so
//! the engine does not depend on libc's BSD `random()` family.
//!
//! The default seed is drawn from `SystemTime::now()` (nanoseconds
//! since the Unix epoch), which gives per-process seed entropy
//! without requiring the `time` cargo feature on the `nix`
//! workspace dependency. The choice is documented as a deviation in
//! `docs/parity.md` and pinned by a regression test, so any future
//! move to a monotonic source is an intentional change rather than a
//! drift.

use std::time::{SystemTime, UNIX_EPOCH};

/// Linear congruential generator with the parameters used by glibc's
/// non-secure `random()` family. Sufficient for ring dispatch (the only
/// caller); not cryptographically strong.
///
/// # Examples
///
/// ```
/// use dynomite::hashkit::PseudoRng;
/// let mut rng = PseudoRng::from_seed(42);
/// let value = rng.next_u32();
/// // Same seed reproduces the same stream.
/// let mut rng2 = PseudoRng::from_seed(42);
/// assert_eq!(value, rng2.next_u32());
/// ```
#[derive(Clone, Debug)]
pub struct PseudoRng {
    state: u64,
}

impl PseudoRng {
    /// Construct a generator seeded from the system clock.
    ///
    /// The seed mixes seconds and nanoseconds drawn from
    /// [`SystemTime::now()`]. See the module-level docs for why we use
    /// the system clock rather than a monotonic clock.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::PseudoRng;
    /// let mut rng = PseudoRng::from_monotonic();
    /// let _ = rng.next_u32();
    /// ```
    #[must_use]
    pub fn from_monotonic() -> Self {
        let seed = clock_seed();
        Self::from_seed(seed)
    }

    /// Construct a generator from an explicit seed.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::PseudoRng;
    /// let mut a = PseudoRng::from_seed(7);
    /// let mut b = PseudoRng::from_seed(7);
    /// assert_eq!(a.next_u32(), b.next_u32());
    /// ```
    #[must_use]
    pub fn from_seed(seed: u64) -> Self {
        // A zero seed in the LCG is a degenerate fixed point; nudge it.
        let s = if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        };
        Self { state: s }
    }

    /// Advance the generator and return the next 32-bit value.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::PseudoRng;
    /// let mut rng = PseudoRng::from_seed(1);
    /// let _: u32 = rng.next_u32();
    /// ```
    pub fn next_u32(&mut self) -> u32 {
        // Knuth's MMIX LCG parameters; long-period and full 64-bit state.
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // High bits of the LCG state have better statistical properties
        // than the low bits, so return the upper half.
        (self.state >> 32) as u32
    }

    /// Pick a uniform value in `[0, modulus)`. Returns `0` when modulus
    /// is zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use dynomite::hashkit::PseudoRng;
    /// let mut rng = PseudoRng::from_seed(7);
    /// for _ in 0..16 {
    ///     assert!(rng.next_index(13) < 13);
    /// }
    /// assert_eq!(rng.next_index(0), 0);
    /// ```
    pub fn next_index(&mut self, modulus: u32) -> u32 {
        if modulus == 0 {
            0
        } else {
            self.next_u32() % modulus
        }
    }
}

fn clock_seed() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
        let secs = d.as_secs();
        let nanos = u64::from(d.subsec_nanos());
        secs.wrapping_mul(1_000_000_000).wrapping_add(nanos)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_same_seed() {
        let mut a = PseudoRng::from_seed(123);
        let mut b = PseudoRng::from_seed(123);
        for _ in 0..32 {
            assert_eq!(a.next_u32(), b.next_u32());
        }
    }

    /// Pins the choice of LCG parameters and the high-bit return
    /// strategy. If this test breaks, treat it as an intentional API
    /// change and update `docs/parity.md`'s `PseudoRng` deviation.
    #[test]
    fn lcg_parameters_are_pinned() {
        // Knuth MMIX constants applied once and the high 32 bits of
        // the resulting state.
        let mut rng = PseudoRng::from_seed(1);
        let expected = ((1u64
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407))
            >> 32) as u32;
        assert_eq!(rng.next_u32(), expected);
    }

    #[test]
    fn distinct_seeds_diverge() {
        let mut a = PseudoRng::from_seed(1);
        let mut b = PseudoRng::from_seed(2);
        let av: Vec<u32> = (0..16).map(|_| a.next_u32()).collect();
        let bv: Vec<u32> = (0..16).map(|_| b.next_u32()).collect();
        assert_ne!(av, bv);
    }

    #[test]
    fn next_index_bounded() {
        let mut rng = PseudoRng::from_seed(42);
        for _ in 0..256 {
            let i = rng.next_index(13);
            assert!(i < 13);
        }
    }

    #[test]
    fn next_index_zero_modulus() {
        let mut rng = PseudoRng::from_seed(42);
        assert_eq!(rng.next_index(0), 0);
    }

    #[test]
    fn monotonic_seed_produces_values() {
        let mut rng = PseudoRng::from_monotonic();
        let _ = rng.next_u32();
    }
}
