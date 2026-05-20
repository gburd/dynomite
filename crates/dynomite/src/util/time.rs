//! Wall-clock and digit-counting helpers.
//!
//! The C reference exposes `dn_msec_now`, `dn_usec_now`,
//! `current_timestamp_in_millis`, and `count_digits`. This module
//! gathers them as plain functions over `std::time::SystemTime`.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::types::{Msec, Usec};

/// Microseconds since the UNIX epoch.
///
/// Returns zero if the system clock reports a time before the epoch.
///
/// # Examples
///
/// ```
/// use dynomite::util::time::usec_now;
/// assert!(usec_now() > 0);
/// ```
pub fn usec_now() -> Usec {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| {
            let micros = d.as_micros();
            if micros > u128::from(Usec::MAX) {
                Usec::MAX
            } else {
                #[allow(clippy::cast_possible_truncation)]
                {
                    micros as Usec
                }
            }
        })
        .unwrap_or(0)
}

/// Milliseconds since the UNIX epoch.
///
/// # Examples
///
/// ```
/// use dynomite::util::time::msec_now;
/// let a = msec_now();
/// let b = msec_now();
/// assert!(b >= a);
/// ```
pub fn msec_now() -> Msec {
    usec_now() / 1000
}

/// Number of decimal digits in `arg`, including a leading zero.
///
/// # Examples
///
/// ```
/// use dynomite::util::time::count_digits;
/// assert_eq!(count_digits(0), 1);
/// assert_eq!(count_digits(9), 1);
/// assert_eq!(count_digits(10), 2);
/// assert_eq!(count_digits(u64::MAX), 20);
/// ```
pub fn count_digits(arg: u64) -> u32 {
    if arg == 0 {
        1
    } else {
        arg.ilog10() + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msec_is_monotone_non_decreasing() {
        let a = msec_now();
        for _ in 0..16 {
            let b = msec_now();
            assert!(b >= a);
        }
    }

    #[test]
    fn msec_is_within_usec_to_msec_factor() {
        let m = msec_now();
        let u = usec_now();
        // The two readings happen back-to-back; allow a few ms of skew.
        assert!(u / 1000 >= m);
        assert!(u / 1000 <= m + 50);
    }

    #[test]
    fn digit_table_matches_ten_powers() {
        for d in 1u32..=18 {
            let n = 10u64.pow(d);
            assert_eq!(count_digits(n), d + 1);
            assert_eq!(count_digits(n - 1), d);
        }
    }
}
