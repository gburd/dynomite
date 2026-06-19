//! Numeric parsing helpers for fixed-length ASCII decimal slices.
//!
//! These parsers consume a fixed-length byte slice (no NUL terminator)
//! containing only ASCII decimal digits and return [`None`] on any
//! non-digit byte or empty input so callers can distinguish "the
//! input was zero" from "the input was invalid".

/// Parse a fixed-length ASCII decimal slice as an `i32`.
///
/// Returns [`None`] if the slice is empty or contains a non-digit
/// byte. The accumulator uses `wrapping_mul`/`wrapping_add` and
/// rejects any input whose accumulated value wraps negative, matching
/// the engine's overflow semantics without relying on signed
/// overflow being defined.
///
/// # Examples
///
/// ```
/// use dynomite::util::atoi::dn_atoi;
/// assert_eq!(dn_atoi(b"42"), Some(42));
/// assert_eq!(dn_atoi(b"007"), Some(7));
/// assert_eq!(dn_atoi(b""), None);
/// assert_eq!(dn_atoi(b"4x"), None);
/// ```
pub fn dn_atoi(line: &[u8]) -> Option<i32> {
    if line.is_empty() {
        return None;
    }
    let mut value: i32 = 0;
    for &b in line {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value.wrapping_mul(10).wrapping_add(i32::from(b - b'0'));
    }
    if value < 0 {
        None
    } else {
        Some(value)
    }
}

/// Parse a fixed-length ASCII decimal slice as a `u32`.
///
/// Empty input or non-digit bytes yield [`None`] (rather than `0`)
/// so callers can tell zero-the-input from zero-the-error.
///
/// # Examples
///
/// ```
/// use dynomite::util::atoi::dn_atoui;
/// assert_eq!(dn_atoui(b"42"), Some(42));
/// assert_eq!(dn_atoui(b"0"), Some(0));
/// assert_eq!(dn_atoui(b""), None);
/// assert_eq!(dn_atoui(b"x"), None);
/// ```
pub fn dn_atoui(line: &[u8]) -> Option<u32> {
    if line.is_empty() {
        return None;
    }
    let mut value: u32 = 0;
    for &b in line {
        if !b.is_ascii_digit() {
            return None;
        }
        value = value.wrapping_mul(10).wrapping_add(u32::from(b - b'0'));
    }
    Some(value)
}

/// Whether `n` falls in the closed range `[1, 65535]`.
///
/// # Examples
///
/// ```
/// use dynomite::util::atoi::valid_port;
/// assert!(valid_port(1));
/// assert!(valid_port(65535));
/// assert!(!valid_port(0));
/// assert!(!valid_port(65536));
/// assert!(!valid_port(-1));
/// ```
pub fn valid_port(n: i32) -> bool {
    (1..=65535).contains(&n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atoi_rejects_empty_and_non_digit() {
        assert_eq!(dn_atoi(b""), None);
        assert_eq!(dn_atoi(b" 12"), None);
        assert_eq!(dn_atoi(b"-3"), None);
        assert_eq!(dn_atoi(b"1.0"), None);
    }

    #[test]
    fn atoi_handles_typical_input() {
        assert_eq!(dn_atoi(b"0"), Some(0));
        assert_eq!(dn_atoi(b"123"), Some(123));
        assert_eq!(dn_atoi(b"2147483647"), Some(i32::MAX));
    }

    #[test]
    fn atoui_handles_typical_input() {
        assert_eq!(dn_atoui(b"0"), Some(0));
        assert_eq!(dn_atoui(b"4294967295"), Some(u32::MAX));
    }
}
